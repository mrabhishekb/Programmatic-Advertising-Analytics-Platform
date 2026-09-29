#!/usr/bin/env python3
"""Land the change stream in object storage, and inspect what landed.

    python scripts/bronze.py sink    consume the CDC topics into Bronze
    python scripts/bronze.py ls      what is in the bucket, by table and day
    python scripts/bronze.py peek    read change events back out of a file

Bronze is the immutable raw layer: the Debezium envelope exactly as it arrived,
plus provenance, in date-partitioned Parquet. Nothing here interprets the
payload - that is the Silver layer's job in phase 6.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bronze import layout
from bronze.sink import DEFAULT_BOOTSTRAP, DEFAULT_GROUP, DEFAULT_TOPIC_PATTERN, BronzeSink
from bronze.storage import BronzeStorageError, BronzeStore
from data_generator.logging_setup import configure_logging, get_logger

logger = get_logger(__name__)


def _human(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:,.1f} {unit}"
        size /= 1024
    return f"{size:,.1f} GB"


# ---------------------------------------------------------------------------
# sink
# ---------------------------------------------------------------------------


def command_sink(args: argparse.Namespace) -> int:
    sink = BronzeSink(
        BronzeStore(),
        bootstrap=args.bootstrap,
        group_id=args.group,
        topic_pattern=args.topics,
        max_records=args.max_records,
        max_seconds=args.max_seconds,
        from_beginning=not args.latest,
    )
    stats = sink.run(max_messages=args.max_messages, idle_timeout=args.idle_timeout)

    print(f"\n{'table':<24}{'records':>12}")
    print("-" * 36)
    for table, count in stats.by_table.items():
        print(f"{table:<24}{count:>12,}")
    print("-" * 36)
    print(f"{'total':<24}{stats.messages:>12,}")
    print(
        f"\n{stats.objects} object(s), {_human(stats.bytes_written)}, "
        f"{stats.flushes} flush(es), {stats.tombstones} tombstone(s)."
    )
    if stats.messages == 0:
        print("\nNothing to consume. Either the sink is already caught up, or no")
        print("changes have been made yet. Try: make changes")
    return 0


# ---------------------------------------------------------------------------
# ls
# ---------------------------------------------------------------------------


def command_ls(args: argparse.Namespace) -> int:
    store = BronzeStore()
    store.ping()

    if args.keys:
        for key in store.list_keys(args.prefix):
            print(key)
        return 0

    paginator = store.client.get_paginator("list_objects_v2")
    # (kind, table, dt) -> [objects, bytes]
    grouped: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0])
    for page in paginator.paginate(Bucket=store.settings.bucket, Prefix=args.prefix):
        for item in page.get("Contents", []):
            grouped[_describe(item["Key"])][0] += 1
            grouped[_describe(item["Key"])][1] += item["Size"]

    if not grouped:
        print(f"{store.settings.describe()} is empty under prefix {args.prefix!r}.")
        print("Land some change events with:  make bronze-sink")
        return 1

    print(f"{store.settings.describe()}\n")
    print(f"{'layer':<10}{'table':<22}{'partition':<16}{'objects':>9}{'size':>12}")
    print("-" * 69)
    total_objects = 0
    total_bytes = 0
    for (kind, table, partition), (count, size) in sorted(grouped.items()):
        total_objects += count
        total_bytes += size
        print(f"{kind:<10}{table:<22}{partition:<16}{count:>9,}{_human(size):>12}")
    print("-" * 69)
    print(f"{'total':<48}{total_objects:>9,}{_human(total_bytes):>12}")
    return 0


def _describe(key: str) -> tuple[str, str, str]:
    """Split an object key back into (layer, table, partition) for grouping."""
    parts = key.split("/")
    if parts[0] == layout.CDC_PREFIX and len(parts) >= 3:
        return ("cdc", parts[1], parts[2])
    if parts[0] == layout.SNAPSHOT_PREFIX and len(parts) >= 3:
        # snapshot/run_id=.../<table>/part-00000.parquet
        table = parts[2] if not parts[2].startswith("_") else "(manifest)"
        return ("snapshot", table, parts[1])
    return ("other", key, "-")


# ---------------------------------------------------------------------------
# peek
# ---------------------------------------------------------------------------


def command_peek(args: argparse.Namespace) -> int:
    store = BronzeStore()
    store.ping()

    key = args.key
    if key is None:
        candidates = [k for k in store.list_keys(args.prefix) if k.endswith(".parquet")]
        if not candidates:
            print(f"No Parquet objects under {args.prefix!r}.")
            return 1
        key = sorted(candidates)[-1]
        print(f"(newest object under {args.prefix!r})\n")

    table = store.read_table(key)
    print(f"{key}\n{table.num_rows:,} record(s)\n")

    rows = table.to_pylist()[: args.limit]
    for row in rows:
        print(
            f"[{row['op']}] {row['source_schema']}.{row['source_table']}  "
            f"lsn={row['lsn']}  tx={row['tx_id']}  "
            f"{row['kafka_topic']}[{row['kafka_partition']}]@{row['kafka_offset']}"
        )
        if args.payload:
            for label in ("before_json", "after_json"):
                raw = row[label]
                if raw:
                    print(f"    {label[:-5]}: {json.dumps(json.loads(raw))[:160]}")
    if table.num_rows > args.limit:
        print(f"\n... {table.num_rows - args.limit:,} more")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bronze", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)

    sink = sub.add_parser("sink", help="consume the CDC topics into Bronze")
    sink.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    sink.add_argument("--group", default=DEFAULT_GROUP)
    sink.add_argument("--topics", default=DEFAULT_TOPIC_PATTERN)
    sink.add_argument("--max-records", type=int, default=5_000, help="records per Parquet file")
    sink.add_argument("--max-seconds", type=float, default=30.0, help="flush at least this often")
    sink.add_argument("--max-messages", type=int, default=0, help="stop after N (0 = no limit)")
    sink.add_argument(
        "--idle-timeout",
        type=float,
        default=10.0,
        help="stop after this long with nothing new (0 = run forever)",
    )
    sink.add_argument(
        "--latest",
        action="store_true",
        help="skip history and start at the end of each topic",
    )
    sink.set_defaults(func=command_sink)

    ls = sub.add_parser("ls", help="what is in the bucket")
    ls.add_argument("--prefix", default="")
    ls.add_argument("--keys", action="store_true", help="print raw object keys instead")
    ls.set_defaults(func=command_ls)

    peek = sub.add_parser("peek", help="read change events back out of a Bronze file")
    peek.add_argument("--key", help="object key (default: newest under --prefix)")
    peek.add_argument("--prefix", default=f"{layout.CDC_PREFIX}/")
    peek.add_argument("--limit", type=int, default=10)
    peek.add_argument("--payload", action="store_true", help="also show before/after")
    peek.set_defaults(func=command_peek)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        return args.func(args)
    except BronzeStorageError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
