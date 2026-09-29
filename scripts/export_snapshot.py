#!/usr/bin/env python3
"""Bulk export of the existing data - "job A" of the backfill + CDC tail pattern.

Debezium runs with ``snapshot.mode: no_data``, so it never reads the rows that
already exist; it only streams changes from the moment its replication slot was
created. This script produces the starting state those changes are applied on
top of.

Two properties make the handover safe:

* **The slot must already exist.** The script refuses to run otherwise, because
  exporting before the slot exists leaves a window where a change is captured by
  neither the export nor the stream, and nothing would ever report the loss.
* **The export is a single consistent read.** Every table is read inside one
  REPEATABLE READ transaction, so all the files describe the database as of the
  same instant rather than drifting apart over the hours an export takes.

The recorded WAL position tells the downstream loader which change events are
already reflected in these files. Events at or before it are replays, and
replaying them is harmless because everything downstream merges on primary key.

Since phase 4 the destination is the Bronze bucket rather than local disk, and
the format is typed Parquet rather than gzipped CSV. See docs/bronze.md.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bronze import layout
from bronze.snapshot import DEFAULT_CHUNK_ROWS, export_table
from bronze.storage import BronzeStorageError, BronzeStore
from data_generator.config import PROJECT_ROOT
from data_generator.db import DatabaseSettings, connect, load_dotenv_file
from data_generator.logging_setup import configure_logging, get_logger
from data_generator.models import MASTER_TABLES, TABLE_NAMES

logger = get_logger(__name__)

DEFAULT_SLOT = "adtech_cdc_slot"


class ExportError(RuntimeError):
    """Raised when the export cannot be performed safely."""


def check_slot_exists(connection: Any, slot_name: str) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
            FROM pg_replication_slots WHERE slot_name = %s
            """,
            (slot_name,),
        )
        row = cursor.fetchone()
    if row is None:
        raise ExportError(
            f"Replication slot {slot_name!r} does not exist.\n"
            "Register the Debezium connector first:  make cdc-register\n"
            "Exporting before the slot exists would silently lose any change made "
            "between now and when CDC starts.\n"
            "Pass --allow-no-slot only if you genuinely do not need the CDC tail."
        )
    return {
        "slot_name": row[0],
        "active": row[1],
        "restart_lsn": str(row[2]),
        "confirmed_flush_lsn": str(row[3]) if row[3] else None,
    }


def run_export(
    *,
    tables: list[str],
    slot_name: str,
    allow_no_slot: bool,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    run_id: str | None = None,
) -> dict[str, Any]:
    started_at = datetime.now()
    started = perf_counter()

    from psycopg import IsolationLevel

    settings = DatabaseSettings.from_env()
    store = BronzeStore()
    store.ping()
    run = run_id or layout.new_run_id()
    logger.info(
        "connecting",
        extra={"target": settings.describe(), "bronze": store.settings.describe(), "run_id": run},
    )

    # The slot check runs on its own connection: querying on the export connection
    # would open a transaction, and the isolation level has to be set before that.
    slot = None
    with connect(settings, autocommit=True) as probe:
        try:
            slot = check_slot_exists(probe, slot_name)
        except ExportError:
            if not allow_no_slot:
                raise
            logger.warning("exporting without a replication slot", extra={"slot": slot_name})

    with connect(settings) as connection:
        # One consistent read for every table: all files describe the same instant.
        connection.isolation_level = IsolationLevel.REPEATABLE_READ
        connection.read_only = True

        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_current_wal_lsn(), pg_export_snapshot(), now()")
            wal_lsn, snapshot_id, snapshot_time = cursor.fetchone()

        logger.info(
            "snapshot opened",
            extra={"wal_lsn": str(wal_lsn), "snapshot_id": snapshot_id, "tables": len(tables)},
        )

        exports = []
        for table in tables:
            table_started = perf_counter()
            result = export_table(connection, store, table=table, run_id=run, chunk_rows=chunk_rows)
            exports.append(
                {
                    "table": result.table,
                    "rows": result.rows,
                    "parts": result.parts,
                    "bytes": result.bytes_written,
                    "duration_seconds": round(perf_counter() - table_started, 2),
                }
            )
            logger.info("exported table", extra=exports[-1])
        connection.rollback()  # read-only; release the snapshot

    manifest = {
        "run_id": run,
        "exported_at": started_at.isoformat(timespec="seconds"),
        "database": settings.database,
        "bucket": store.settings.bucket,
        "prefix": layout.snapshot_run_prefix(run),
        "format": "parquet",
        "consistent_snapshot": {
            "wal_lsn": str(wal_lsn),
            "postgres_snapshot_id": snapshot_id,
            "snapshot_time": snapshot_time.isoformat(),
        },
        "replication_slot": slot,
        "tables": exports,
        "total_rows": sum(item["rows"] for item in exports),
        "total_bytes": sum(item["bytes"] for item in exports),
        "duration_seconds": round(perf_counter() - started, 2),
        "note": (
            "Change events at or before consistent_snapshot.wal_lsn are already reflected "
            "in these files. Replaying them is harmless because downstream loads merge on "
            "primary key."
        ),
    }

    store.put_bytes(
        layout.snapshot_manifest_key(run),
        json.dumps(manifest, indent=2).encode(),
        content_type="application/json",
    )
    return manifest


def render_summary(manifest: dict[str, Any]) -> str:
    lines = ["", "=" * 72, " SNAPSHOT EXPORT", "=" * 72]
    lines.append(f" Destination   : s3://{manifest['bucket']}/{manifest['prefix']}")
    lines.append(f" WAL position  : {manifest['consistent_snapshot']['wal_lsn']}")
    slot = manifest["replication_slot"]
    lines.append(
        f" Slot          : {slot['slot_name']} (active={slot['active']})"
        if slot
        else " Slot          : NONE - the CDC tail will have a gap"
    )
    lines.append(f" Duration      : {manifest['duration_seconds']:,.1f}s")
    lines.append("")
    lines.append(f" {'table':<24}{'rows':>14}{'parts':>8}{'size':>14}")
    lines.append(" " + "-" * 60)
    for item in manifest["tables"]:
        size_mb = item["bytes"] / (1024 * 1024)
        lines.append(
            f" {item['table']:<24}{item['rows']:>14,}{item['parts']:>8,}{size_mb:>12,.1f} MB"
        )
    lines.append(" " + "-" * 60)
    total_mb = manifest["total_bytes"] / (1024 * 1024)
    lines.append(f" {'total':<24}{manifest['total_rows']:>14,}{'':>8}{total_mb:>12,.1f} MB")
    lines.append("")
    lines.append(" This is the starting state. Every change after the WAL position above")
    lines.append(" arrives through CDC and lands alongside it:  make bronze-sink")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="export-snapshot",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--tables", nargs="*", default=list(TABLE_NAMES), help="tables to export (default: all)"
    )
    parser.add_argument(
        "--master-only",
        action="store_true",
        help="export only the seven mutable tables, skipping the 100M-row event tables",
    )
    parser.add_argument("--slot", default=DEFAULT_SLOT)
    parser.add_argument(
        "--allow-no-slot",
        action="store_true",
        help="export even though CDC is not running (accepts a gap in change history)",
    )
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=DEFAULT_CHUNK_ROWS,
        help="rows per Parquet part; bounds peak memory on the event tables",
    )
    parser.add_argument("--run-id", help="reuse a run id (default: derived from the clock)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    configure_logging(args.log_level)
    load_dotenv_file(PROJECT_ROOT / ".env")

    tables = args.tables
    if args.master_only:
        # TABLE_ORDER's order, not the set's, so parents still export first.
        tables = [name for name in TABLE_NAMES if name in MASTER_TABLES]

    unknown = set(tables) - set(TABLE_NAMES)
    if unknown:
        print(f"error: unknown tables {sorted(unknown)}", file=sys.stderr)
        return 2

    try:
        manifest = run_export(
            tables=tables,
            slot_name=args.slot,
            allow_no_slot=args.allow_no_slot,
            chunk_rows=args.chunk_rows,
            run_id=args.run_id,
        )
    except (ExportError, BronzeStorageError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(render_summary(manifest))
    print(
        f"\nManifest: s3://{manifest['bucket']}/{layout.snapshot_manifest_key(manifest['run_id'])}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
