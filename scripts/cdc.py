#!/usr/bin/env python3
"""Manage and inspect the change data capture pipeline.

    python scripts/cdc.py register     register the Debezium connector
    python scripts/cdc.py status       connector and task health
    python scripts/cdc.py slots        replication slot state and WAL retained
    python scripts/cdc.py topics       CDC topics and how many messages each holds
    python scripts/cdc.py watch        stream change events as they happen
    python scripts/cdc.py delete       remove the connector (and optionally the slot)

Registration deliberately runs *before* the bulk export
(``scripts/export_snapshot.py``). Creating the connector creates the replication
slot, and the slot is what makes PostgreSQL retain every change from that moment
on. Exporting first and registering second leaves a window in which a change is
captured by neither, and nothing would ever report the loss.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_generator.config import PROJECT_ROOT
from data_generator.db import DatabaseSettings, connect, load_dotenv_file
from data_generator.logging_setup import configure_logging, get_logger

logger = get_logger(__name__)

CONNECTOR_PATH = PROJECT_ROOT / "debezium" / "connector.json"
DEFAULT_CONNECT_URL = "http://localhost:8083"
DEFAULT_BOOTSTRAP = "localhost:29092"

#: Debezium operation codes.
_OPERATIONS = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT READ", "t": "TRUNCATE"}


class CdcError(RuntimeError):
    """Raised when the connector or its prerequisites are not in a usable state."""


# ---------------------------------------------------------------------------
# Kafka Connect REST API
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ConnectClient:
    base_url: str = DEFAULT_CONNECT_URL

    def _request(self, method: str, path: str, payload: Any = None) -> Any:
        url = f"{self.base_url}{path}"
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(
            url, data=data, method=method, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read().decode()
                return json.loads(body) if body else None
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise CdcError(f"{method} {path} failed with {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise CdcError(
                f"Cannot reach Kafka Connect at {self.base_url}. Is the stack up? "
                f"Try: make cdc-up  ({exc.reason})"
            ) from exc

    def list_connectors(self) -> list[str]:
        return self._request("GET", "/connectors") or []

    def create(self, definition: dict) -> Any:
        return self._request("POST", "/connectors", definition)

    def update_config(self, name: str, config: dict) -> Any:
        return self._request("PUT", f"/connectors/{name}/config", config)

    def status(self, name: str) -> dict:
        return self._request("GET", f"/connectors/{name}/status")

    def delete(self, name: str) -> None:
        self._request("DELETE", f"/connectors/{name}")


def load_connector_definition(path: Path = CONNECTOR_PATH) -> dict:
    definition = json.loads(path.read_text(encoding="utf-8"))
    if "name" not in definition or "config" not in definition:
        raise CdcError(f"{path} must contain 'name' and 'config' keys")
    return definition


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------


def command_register(args: argparse.Namespace) -> int:
    definition = load_connector_definition(args.connector)
    name = definition["name"]

    _ensure_heartbeat_table()

    client = ConnectClient(args.connect_url)
    if name in client.list_connectors():
        if not args.force:
            print(f"Connector {name!r} already exists. Use --force to update its configuration.")
            return 0
        client.update_config(name, definition["config"])
        logger.info("connector configuration updated", extra={"connector": name})
    else:
        client.create(definition)
        logger.info("connector registered", extra={"connector": name})

    config = definition["config"]
    print(f"Connector {name!r} registered against database {config['database.dbname']!r}.")
    print(f"  snapshot.mode : {config.get('snapshot.mode')}")
    print(f"  tables        : {config.get('table.include.list')}")
    print(f"  slot          : {config.get('slot.name')}")
    print("\nThe replication slot now exists, so every change from this moment is retained.")
    print("Next: run the bulk export with  make export-snapshot")
    return 0


def command_status(args: argparse.Namespace) -> int:
    definition = load_connector_definition(args.connector)
    name = definition["name"]
    client = ConnectClient(args.connect_url)

    if name not in client.list_connectors():
        print(f"Connector {name!r} is not registered. Run: make cdc-register")
        return 1

    status = client.status(name)
    connector_state = status["connector"]["state"]
    print(f"Connector {name!r}: {connector_state}")
    for task in status.get("tasks", []):
        print(f"  task {task['id']}: {task['state']}")
        if task.get("trace"):
            print("    " + task["trace"].splitlines()[0])

    healthy = connector_state == "RUNNING" and all(
        task["state"] == "RUNNING" for task in status.get("tasks", [])
    )
    if not healthy:
        print("\nConnector is not healthy. Full status:")
        print(json.dumps(status, indent=2))
    return 0 if healthy else 1


def command_slots(args: argparse.Namespace) -> int:
    """Replication slot health - the number that matters is WAL retained.

    A slot that stops being consumed pins the write-ahead log in place until the
    disk fills. That is the most common way a CDC setup takes a database down, so
    it gets its own command.
    """
    with connect(DatabaseSettings.from_env()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn,
                   pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
            FROM pg_replication_slots ORDER BY slot_name
            """
        )
        slots = cursor.fetchall()
        cursor.execute("SELECT pubname, puballtables FROM pg_publication ORDER BY pubname")
        publications = cursor.fetchall()
        # The age is computed in SQL: the column is a naive TIMESTAMP holding the
        # container's UTC clock, so subtracting it from a local datetime.now()
        # would report the timezone offset as staleness.
        cursor.execute(
            "SELECT beats, EXTRACT(EPOCH FROM (now() - beat_at)) FROM platform.cdc_heartbeat "
            "WHERE id = 1"
        )
        heartbeat = cursor.fetchone()

    if not slots:
        print("No replication slots. The connector has not been registered yet.")
        return 1

    print("Replication slots")
    for slot_name, plugin, slot_type, active, restart, confirmed, retained in slots:
        marker = "active" if active else "INACTIVE - WAL is accumulating"
        print(f"  {slot_name}  [{plugin}/{slot_type}]  {marker}")
        print(f"    restart_lsn={restart}  confirmed_flush_lsn={confirmed}")
        print(f"    WAL retained: {retained}")

    print("\nPublications")
    for pubname, all_tables in publications:
        scope = "ALL TABLES" if all_tables else "filtered to the captured tables"
        print(f"  {pubname}  ({scope})")

    if heartbeat:
        beats, age_seconds = heartbeat
        age = float(age_seconds)
        print(f"\nHeartbeat: {beats:,} beats, last one {age:,.0f}s ago")
        if age > 120:
            print("  WARNING: heartbeat is stale; the connector may not be running.")
    return 0


def command_topics(args: argparse.Namespace) -> int:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": "adtech-cdc-inspect",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(timeout=15)
        topics = sorted(t for t in metadata.topics if t.startswith("cdc") and not t.startswith("_"))
        if not topics:
            print("No CDC topics yet. Register the connector and make a change.")
            return 1

        print(f"{'topic':<44}{'messages':>12}")
        print("-" * 56)
        total = 0
        for topic in topics:
            count = 0
            for partition in metadata.topics[topic].partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=10
                )
                count += high - low
            total += count
            print(f"{topic:<44}{count:>12,}")
        print("-" * 56)
        print(f"{'total':<44}{total:>12,}")
    finally:
        consumer.close()
    return 0


def command_watch(args: argparse.Namespace) -> int:
    """Print change events in a readable form as they arrive."""
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )
    pattern = args.topic or "^cdc\\.public\\..*"
    consumer.subscribe([pattern])

    print(f"Watching {pattern} on {args.bootstrap} (Ctrl-C to stop)\n")
    seen = 0
    try:
        while args.max == 0 or seen < args.max:
            message = consumer.poll(timeout=args.timeout)
            if message is None:
                if args.max:
                    print(f"\nNo further messages within {args.timeout}s.")
                    break
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise CdcError(str(message.error()))
            _print_event(message)
            seen += 1
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        consumer.close()

    print(f"\n{seen} event(s).")
    return 0


def _print_event(message: Any) -> None:
    raw_value = message.value()
    if raw_value is None:
        # Debezium writes a null-valued record after a delete so Kafka log
        # compaction can eventually drop the key entirely.
        print(f"[{message.topic()}] TOMBSTONE  key={_decode(message.key())}")
        return

    event = json.loads(raw_value)
    operation = _OPERATIONS.get(event.get("op", "?"), event.get("op", "?"))
    source = event.get("source", {})
    table = f"{source.get('schema', '?')}.{source.get('table', '?')}"
    when = _format_timestamp(event.get("ts_ms"))

    print(f"[{when}] {operation:<13} {table:<22} lsn={source.get('lsn')} tx={source.get('txId')}")

    before, after = event.get("before"), event.get("after")
    if operation == "UPDATE" and before and after:
        changed = {
            key: (before.get(key), after.get(key))
            for key in after
            if before.get(key) != after.get(key)
        }
        for key, (old, new) in changed.items():
            print(f"      {key}: {_render(key, old)} -> {_render(key, new)}")
        if not changed:
            print("      (no column values changed)")
    elif operation == "INSERT" and after:
        print(f"      {_summarise(after)}")
    elif operation == "DELETE" and before:
        print(f"      deleted: {_summarise(before)}")

    transaction = event.get("transaction")
    if transaction:
        print(f"      transaction {transaction.get('id')} event #{transaction.get('total_order')}")


def _summarise(row: dict, limit: int = 4) -> str:
    items = list(row.items())[:limit]
    rendered = ", ".join(f"{key}={_render(key, value)}" for key, value in items)
    return rendered + (" ..." if len(row) > limit else "")


def _render(column: str, value: Any) -> str:
    """Make a column value readable.

    Temporal columns arrive as epoch milliseconds rather than formatted dates:
    the connector tags them with a logical type, but the JSON converter runs with
    schemas disabled, so the tag is dropped and only the raw integer survives.
    Reading the column name back is a display-layer fix; a consumer that needs
    the types should use Avro with a schema registry instead.
    """
    if isinstance(value, int) and (column.endswith(("_at", "_timestamp", "_date"))):
        try:
            return datetime.fromtimestamp(value / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
        except (OverflowError, OSError, ValueError):
            return repr(value)
    return repr(value)


def _format_timestamp(ts_ms: int | None) -> str:
    if not ts_ms:
        return "-"
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).strftime("%H:%M:%S")


def _decode(value: bytes | None) -> str:
    return value.decode(errors="replace") if value else "None"


def command_delete(args: argparse.Namespace) -> int:
    definition = load_connector_definition(args.connector)
    name = definition["name"]
    client = ConnectClient(args.connect_url)

    if name in client.list_connectors():
        client.delete(name)
        print(f"Connector {name!r} deleted.")
    else:
        print(f"Connector {name!r} was not registered.")

    if args.drop_slot:
        slot = definition["config"].get("slot.name")
        with (
            connect(DatabaseSettings.from_env(), autocommit=True) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = %s AND NOT active",
                (slot,),
            )
            dropped = cursor.rowcount
        if dropped:
            print(f"Replication slot {slot!r} dropped; retained WAL released.")
        else:
            print(f"Slot {slot!r} not dropped (missing, or still active).")
    else:
        print(
            "\nThe replication slot still exists and is still retaining WAL.\n"
            "Re-register the connector soon, or drop it with --drop-slot."
        )
    return 0


def _ensure_heartbeat_table() -> None:
    """The connector's heartbeat query targets this table, so it must exist first."""
    with (
        connect(DatabaseSettings.from_env(), autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            """
                CREATE SCHEMA IF NOT EXISTS platform;
                CREATE TABLE IF NOT EXISTS platform.cdc_heartbeat (
                    id      INTEGER   PRIMARY KEY,
                    beat_at TIMESTAMP NOT NULL DEFAULT now(),
                    beats   BIGINT    NOT NULL DEFAULT 0
                );
                INSERT INTO platform.cdc_heartbeat (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
                """
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cdc", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--connect-url", default=DEFAULT_CONNECT_URL)
    parser.add_argument("--connector", type=Path, default=CONNECTOR_PATH)
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--log-level", default="WARNING")
    sub = parser.add_subparsers(dest="command", required=True)

    register = sub.add_parser("register", help="register the Debezium connector")
    register.add_argument("--force", action="store_true", help="update an existing connector")
    register.set_defaults(func=command_register)

    sub.add_parser("status", help="connector and task health").set_defaults(func=command_status)
    sub.add_parser("slots", help="replication slot state and WAL retained").set_defaults(
        func=command_slots
    )
    sub.add_parser("topics", help="CDC topics and message counts").set_defaults(func=command_topics)

    watch = sub.add_parser("watch", help="stream change events")
    watch.add_argument("--topic", help="topic or ^regex (default: all cdc.public.* topics)")
    watch.add_argument("--from-beginning", action="store_true")
    watch.add_argument("--max", type=int, default=0, help="stop after N events (0 = forever)")
    watch.add_argument("--timeout", type=float, default=10.0)
    watch.add_argument("--group", default="adtech-cdc-watch")
    watch.set_defaults(func=command_watch)

    delete = sub.add_parser("delete", help="remove the connector")
    delete.add_argument("--drop-slot", action="store_true", help="also drop the replication slot")
    delete.set_defaults(func=command_delete)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    load_dotenv_file(PROJECT_ROOT / ".env")
    try:
        return args.func(args)
    except CdcError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
