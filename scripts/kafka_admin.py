#!/usr/bin/env python3
"""Manage and measure the Kafka topics behind the change stream.

    python scripts/kafka_admin.py describe   declared vs actual topic settings
    python scripts/kafka_admin.py apply      reconcile the cluster with config/kafka.yml
    python scripts/kafka_admin.py lag        consumer group lag, per partition
    python scripts/kafka_admin.py bench      producer throughput and delivery latency

Phase 2 left every topic on the one template Debezium applies to anything it
creates. ``config/kafka.yml`` states what each topic should actually look like
and ``apply`` reconciles the cluster against it, so the settings are reviewable
in git rather than discovered with ``kafka-configs.sh``.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_generator.config import PROJECT_ROOT
from data_generator.logging_setup import configure_logging, get_logger

logger = get_logger(__name__)

KAFKA_CONFIG_PATH = PROJECT_ROOT / "config" / "kafka.yml"
DEFAULT_BOOTSTRAP = "localhost:29092"

#: Consumer groups Kafka and Connect create for their own bookkeeping. They are
#: shown by `lag` but never warned about, because their lag is meaningless.
_INTERNAL_GROUP_PREFIXES = ("connect-", "adtech-connect")


class KafkaAdminError(RuntimeError):
    """Raised when the topic specification or the cluster is not usable."""


# ---------------------------------------------------------------------------
# the declared specification
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    config: dict[str, str] = field(default_factory=dict)


def _as_kafka_value(value: Any) -> str:
    """Kafka takes every topic setting as a string, YAML gives us real types."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def load_topic_specs(path: Path = KAFKA_CONFIG_PATH) -> dict[str, TopicSpec]:
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    try:
        replication_factor = int(document["replication_factor"])
    except KeyError as exc:
        raise KafkaAdminError(f"{path} must set 'replication_factor'") from exc

    default_partitions = int((document.get("defaults") or {}).get("partitions", 1))
    policies: dict[str, dict] = document.get("policies") or {}
    topics: dict[str, dict] = document.get("topics") or {}
    if not topics:
        raise KafkaAdminError(f"{path} declares no topics")

    specs: dict[str, TopicSpec] = {}
    for name, raw in topics.items():
        raw = raw or {}
        policy_name = raw.get("policy")
        if policy_name is not None and policy_name not in policies:
            raise KafkaAdminError(
                f"topic {name!r} references unknown policy {policy_name!r}; "
                f"known policies: {sorted(policies)}"
            )
        settings: dict[str, Any] = dict(policies.get(policy_name, {})) if policy_name else {}
        settings.update(raw.get("config") or {})
        specs[name] = TopicSpec(
            name=name,
            partitions=int(raw.get("partitions", default_partitions)),
            replication_factor=replication_factor,
            config={key: _as_kafka_value(value) for key, value in settings.items()},
        )
    return specs


# ---------------------------------------------------------------------------
# cluster access
# ---------------------------------------------------------------------------


def _admin(bootstrap: str):
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap})


def _cluster_topics(admin, timeout: float = 15.0) -> dict[str, Any]:
    from confluent_kafka import KafkaException

    try:
        return dict(admin.list_topics(timeout=timeout).topics)
    except KafkaException as exc:
        raise KafkaAdminError(
            f"Cannot reach Kafka. Is the stack up? Try: make cdc-up  ({exc})"
        ) from exc


def _actual_configs(admin, names: list[str]) -> dict[str, dict[str, str]]:
    """Current settings for topics that exist, keyed by topic name."""
    from confluent_kafka.admin import ConfigResource

    if not names:
        return {}
    resources = [ConfigResource(ConfigResource.Type.TOPIC, name) for name in names]
    actual: dict[str, dict[str, str]] = {}
    for resource, future in admin.describe_configs(resources).items():
        entries = future.result(timeout=30)
        actual[resource.name] = {key: entry.value for key, entry in entries.items()}
    return actual


def _drifted_keys(spec: TopicSpec, actual: dict[str, str]) -> list[str]:
    """Declared settings whose live value differs. Undeclared settings are ignored."""
    return sorted(key for key, want in spec.config.items() if actual.get(key) != want)


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


def command_describe(args: argparse.Namespace) -> int:
    specs = load_topic_specs(args.config)
    admin = _admin(args.bootstrap)
    cluster = _cluster_topics(admin)

    existing = [name for name in specs if name in cluster]
    actual_configs = _actual_configs(admin, existing)

    print(f"{'topic':<30}{'partitions':>11}  {'cleanup.policy':<16}{'drift'}")
    print("-" * 92)

    missing = 0
    drifting = 0
    for name, spec in specs.items():
        if name not in cluster:
            missing += 1
            print(f"{name:<30}{'-':>4}/{spec.partitions:<6}  {'-':<16}NOT CREATED")
            continue

        actual = actual_configs.get(name, {})
        live_partitions = len(cluster[name].partitions)
        drift = _drifted_keys(spec, actual)
        if drift:
            drifting += 1
        if live_partitions < spec.partitions:
            drift = [f"partitions {live_partitions}->{spec.partitions}", *drift]
        elif live_partitions > spec.partitions:
            drift = [f"partitions {live_partitions}, declared {spec.partitions}", *drift]

        policy = actual.get("cleanup.policy", "?")
        summary = ", ".join(drift) if drift else "ok"
        print(f"{name:<30}{live_partitions:>4}/{spec.partitions:<6}  {policy:<16}{summary}")

    undeclared = sorted(
        name
        for name in cluster
        if name not in specs and (name.startswith("cdc") or name.startswith("__debezium"))
    )
    if undeclared:
        print("\nOn the cluster but not declared in config/kafka.yml:")
        for name in undeclared:
            print(f"  {name}")

    print()
    if missing or drifting:
        print(f"{missing} topic(s) not created, {drifting} with configuration drift.")
        print("Reconcile with:  make kafka-apply")
        return 1
    print("Every declared topic matches its specification.")
    return 0


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


def command_apply(args: argparse.Namespace) -> int:
    from confluent_kafka.admin import (
        AlterConfigOpType,
        ConfigEntry,
        ConfigResource,
        NewPartitions,
        NewTopic,
    )

    specs = load_topic_specs(args.config)
    admin = _admin(args.bootstrap)
    cluster = _cluster_topics(admin)

    existing = [name for name in specs if name in cluster]
    actual_configs = _actual_configs(admin, existing)

    to_create: list[Any] = []
    to_alter: list[Any] = []
    to_grow: list[Any] = []
    shrink_requests: list[tuple[str, int, int]] = []

    for name, spec in specs.items():
        if name not in cluster:
            to_create.append(
                NewTopic(
                    name,
                    num_partitions=spec.partitions,
                    replication_factor=spec.replication_factor,
                    config=dict(spec.config),
                )
            )
            continue

        actual = actual_configs.get(name, {})
        drift = _drifted_keys(spec, actual)
        if drift:
            resource = ConfigResource(ConfigResource.Type.TOPIC, name)
            for key in drift:
                resource.add_incremental_config(
                    ConfigEntry(key, spec.config[key], incremental_operation=AlterConfigOpType.SET)
                )
            to_alter.append((name, drift, resource))

        live_partitions = len(cluster[name].partitions)
        if live_partitions < spec.partitions:
            to_grow.append(NewPartitions(name, spec.partitions))
        elif live_partitions > spec.partitions:
            shrink_requests.append((name, live_partitions, spec.partitions))

    if not (to_create or to_alter or to_grow):
        print("Nothing to do: the cluster already matches config/kafka.yml.")
        _report_shrinks(shrink_requests)
        return 0

    if args.dry_run:
        print("Dry run. These changes would be applied:\n")
        for topic in to_create:
            print(f"  create  {topic.topic}")
        for name, drift, _ in to_alter:
            print(f"  alter   {name}  ({', '.join(drift)})")
        for partitions in to_grow:
            print(f"  grow    {partitions.topic} -> {partitions.total_count} partitions")
        _report_shrinks(shrink_requests)
        return 0

    failures = 0

    if to_create:
        for name, future in admin.create_topics(to_create, request_timeout=30).items():
            try:
                future.result()
                spec = specs[name]
                print(f"created  {name}  ({spec.partitions} partitions)")
            except Exception as exc:
                failures += 1
                print(f"FAILED   create {name}: {exc}")

    if to_alter:
        resources = [resource for _, _, resource in to_alter]
        drift_by_name = {name: drift for name, drift, _ in to_alter}
        for resource, future in admin.incremental_alter_configs(
            resources, request_timeout=30
        ).items():
            try:
                future.result()
                print(f"altered  {resource.name}  ({', '.join(drift_by_name[resource.name])})")
            except Exception as exc:
                failures += 1
                print(f"FAILED   alter {resource.name}: {exc}")

    if to_grow:
        # Growing a topic rehashes key -> partition for everything written from
        # here on, so a key's history is split across the old and new partition
        # and per-key ordering no longer holds across that boundary.
        print(
            "\nWARNING: adding partitions changes which partition a key lands on.\n"
            "         Ordering per key is preserved going forward but not across\n"
            "         the boundary with messages already written.\n"
        )
        for name, future in admin.create_partitions(to_grow, request_timeout=30).items():
            try:
                future.result()
                print(f"grew     {name} -> {specs[name].partitions} partitions")
            except Exception as exc:
                failures += 1
                print(f"FAILED   grow {name}: {exc}")

    _report_shrinks(shrink_requests)

    if failures:
        print(f"\n{failures} change(s) failed.")
        return 1
    print("\nCluster reconciled with config/kafka.yml.")
    return 0


def _report_shrinks(requests: list[tuple[str, int, int]]) -> None:
    """Kafka cannot remove partitions, so these are reported rather than applied."""
    for name, live, declared in requests:
        print(
            f"\nNOTE: {name} has {live} partitions but {declared} are declared.\n"
            f"      Kafka cannot remove partitions. Either raise the declared count\n"
            f"      or delete and recreate the topic, which discards its messages."
        )


# ---------------------------------------------------------------------------
# lag
# ---------------------------------------------------------------------------


def command_lag(args: argparse.Namespace) -> int:
    """How far behind each consumer group is, per partition.

    Lag is the gap between the newest offset in a partition and the offset the
    group has committed. It is the signal that tells you a consumer has stopped
    keeping up before the topic's retention deletes what it has not read.
    """
    from confluent_kafka import Consumer, ConsumerGroupTopicPartitions

    admin = _admin(args.bootstrap)
    listing = admin.list_consumer_groups(request_timeout=30).result()
    group_ids = sorted(group.group_id for group in listing.valid)
    if not group_ids:
        print("No consumer groups. Nothing has read from Kafka yet.")
        return 0

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": "adtech-kafka-lag-inspect",
            "enable.auto.commit": False,
        }
    )

    total_lag = 0
    try:
        for group_id in group_ids:
            request = ConsumerGroupTopicPartitions(group_id)
            offsets = admin.list_consumer_group_offsets([request], request_timeout=30)
            assignment = offsets[group_id].result(timeout=30).topic_partitions or []
            internal = group_id.startswith(_INTERNAL_GROUP_PREFIXES)

            print(f"\n{group_id}" + ("  (internal)" if internal else ""))
            if not assignment:
                print("  no committed offsets")
                continue

            group_lag = 0
            for partition in sorted(assignment, key=lambda p: (p.topic, p.partition)):
                _, high = consumer.get_watermark_offsets(partition, timeout=10, cached=False)
                committed = partition.offset
                if committed < 0:
                    print(f"  {partition.topic}[{partition.partition}]  no commit")
                    continue
                lag = max(high - committed, 0)
                group_lag += lag
                print(
                    f"  {partition.topic}[{partition.partition}]  "
                    f"committed={committed:,}  end={high:,}  lag={lag:,}"
                )
            print(f"  total lag: {group_lag:,}")
            if not internal:
                total_lag += group_lag
    finally:
        consumer.close()

    print(f"\nLag across non-internal groups: {total_lag:,}")
    if total_lag > args.warn_above:
        print(f"WARNING: above the {args.warn_above:,} message threshold.")
        return 1
    return 0


# ---------------------------------------------------------------------------
# bench
# ---------------------------------------------------------------------------

#: Shaped like the Debezium envelope in docs/cdc.md, so the measurement runs on
#: payloads the same size and shape as real change events rather than on filler.
_SAMPLE_ROW = {
    "campaign_id": "00000000-0000-0000-0000-000000000000",
    "advertiser_id": "00000000-0000-0000-0000-000000000000",
    "campaign_name": "Q4 Brand Awareness - Consumer Electronics",
    "campaign_objective": "AWARENESS",
    "campaign_status": "ACTIVE",
    "campaign_budget": "250000.00",
    "daily_budget": "4166.67",
    "start_date": 20390,
    "end_date": 20480,
    "bid_strategy": "TARGET_CPM",
    "created_at": 1790549625218,
    "updated_at": 1790549625218,
}


def _sample_payload() -> bytes:
    envelope = {
        "before": _SAMPLE_ROW,
        "after": {**_SAMPLE_ROW, "campaign_status": "PAUSED"},
        "op": "u",
        "ts_ms": 1790549625218,
        "source": {
            "version": "2.7.3.Final",
            "connector": "postgresql",
            "db": "adtech",
            "schema": "public",
            "table": "campaigns",
            "lsn": 818539667648,
            "txId": 888,
            "ts_ms": 1790549625218,
        },
        "transaction": {"id": "888:818539667648", "total_order": 17},
    }
    return json.dumps(envelope).encode()


@dataclass(frozen=True, slots=True)
class BenchResult:
    compression: str
    messages: int
    payload_bytes: int
    elapsed_s: float
    latency_p50_ms: float
    latency_p99_ms: float
    errors: int

    @property
    def messages_per_second(self) -> float:
        return self.messages / self.elapsed_s if self.elapsed_s else 0.0

    @property
    def megabytes_per_second(self) -> float:
        return (self.messages * self.payload_bytes) / self.elapsed_s / 1_000_000


def _run_bench(args: argparse.Namespace, compression: str) -> BenchResult:
    from confluent_kafka import Producer

    payload = _sample_payload()
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "compression.type": compression,
            "linger.ms": args.linger_ms,
            "batch.size": args.batch_size,
            "acks": args.acks,
            "queue.buffering.max.messages": max(args.messages, 100_000),
        }
    )

    latencies: list[float] = []
    errors = 0
    # Keyed by the key's *value*, not its identity: the callback is handed a
    # fresh bytes object for the same key, so id() would never match.
    sent_at: dict[bytes, float] = {}

    def on_delivery(err, message) -> None:
        nonlocal errors
        if err is not None:
            errors += 1
            return
        started = sent_at.pop(message.key(), None)
        if started is not None:
            latencies.append((time.perf_counter() - started) * 1000)

    started_at = time.perf_counter()
    for _ in range(args.messages):
        key = str(uuid.uuid4()).encode()
        sent_at[key] = time.perf_counter()
        while True:
            try:
                producer.produce(args.topic, key=key, value=payload, on_delivery=on_delivery)
                break
            except BufferError:
                # The local queue is full, which is the producer telling us it is
                # the bottleneck rather than the broker. Drain and retry.
                producer.poll(0.5)
        producer.poll(0)
    producer.flush()
    elapsed = time.perf_counter() - started_at

    return BenchResult(
        compression=compression,
        messages=args.messages,
        payload_bytes=len(payload),
        elapsed_s=elapsed,
        latency_p50_ms=statistics.median(latencies) if latencies else 0.0,
        latency_p99_ms=(
            statistics.quantiles(latencies, n=100)[98] if len(latencies) > 100 else 0.0
        ),
        errors=errors,
    )


def command_bench(args: argparse.Namespace) -> int:
    """Measure producer throughput, so the tuning in config is based on numbers.

    This isolates Kafka from PostgreSQL deliberately: it answers "how fast can
    the broker take writes", not "how fast can Debezium decode the WAL", which
    is a separate and much lower ceiling.
    """
    codecs = ["none", "gzip", "snappy", "lz4", "zstd"] if args.compare else [args.compression]

    payload_bytes = len(_sample_payload())
    print(
        f"Producing {args.messages:,} x {payload_bytes} B to {args.topic!r} "
        f"(acks={args.acks}, linger.ms={args.linger_ms}, batch.size={args.batch_size:,})\n"
    )

    print(f"{'compression':<14}{'msgs/s':>12}{'MB/s':>10}{'p50 ms':>10}{'p99 ms':>10}{'errors':>9}")
    print("-" * 65)

    results: list[BenchResult] = []
    for codec in codecs:
        result = _run_bench(args, codec)
        results.append(result)
        print(
            f"{codec:<14}{result.messages_per_second:>12,.0f}"
            f"{result.megabytes_per_second:>10.2f}"
            f"{result.latency_p50_ms:>10.2f}{result.latency_p99_ms:>10.2f}"
            f"{result.errors:>9}"
        )

    if any(result.errors for result in results):
        print("\nSome messages failed to deliver; the numbers above are not trustworthy.")
        return 1

    if len(results) > 1:
        best = max(results, key=lambda r: r.messages_per_second)
        print(f"\nFastest: {best.compression} at {best.messages_per_second:,.0f} msgs/s")
    print(
        "\nThroughput here is the broker's ceiling, not the pipeline's. Debezium\n"
        "runs one task against a single-threaded replication slot, so end-to-end\n"
        "CDC throughput is bounded well below this."
    )
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafka-admin",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--config", type=Path, default=KAFKA_CONFIG_PATH)
    parser.add_argument("--log-level", default="WARNING")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("describe", help="declared vs actual topic settings").set_defaults(
        func=command_describe
    )

    apply_parser = sub.add_parser("apply", help="reconcile the cluster with config/kafka.yml")
    apply_parser.add_argument("--dry-run", action="store_true", help="show changes, apply none")
    apply_parser.set_defaults(func=command_apply)

    lag = sub.add_parser("lag", help="consumer group lag, per partition")
    lag.add_argument("--warn-above", type=int, default=10_000, help="exit 1 above this lag")
    lag.set_defaults(func=command_lag)

    bench = sub.add_parser("bench", help="producer throughput and delivery latency")
    bench.add_argument("--messages", type=int, default=50_000)
    bench.add_argument("--topic", default="kafka.bench")
    bench.add_argument("--compression", default="lz4")
    bench.add_argument("--compare", action="store_true", help="run every compression codec")
    bench.add_argument("--linger-ms", type=int, default=10)
    bench.add_argument("--batch-size", type=int, default=131_072)
    bench.add_argument("--acks", default="1")
    bench.set_defaults(func=command_bench)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        return args.func(args)
    except KafkaAdminError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
