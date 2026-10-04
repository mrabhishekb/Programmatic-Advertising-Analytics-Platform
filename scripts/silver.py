#!/usr/bin/env python3
"""Reconcile Bronze into current-state Silver tables, and inspect the result.

    python scripts/silver.py reconcile   snapshot + change stream -> current state
    python scripts/silver.py compact     merge a day's small CDC objects
    python scripts/silver.py ls          what is in the Silver layer
    python scripts/silver.py show        read reconciled rows back out

Needs Java, so it runs in the Spark container rather than the project venv:

    make silver            reconcile the dimension tables
    make silver-compact    merge small Bronze objects
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bronze.storage import BronzeStorageError, BronzeStore
from data_generator.logging_setup import configure_logging, get_logger
from spark import compact, layout
from spark.layout import DEFAULT_ROWS_PER_FILE, RECONCILED_TABLES

# `spark.job` and `spark.session` are imported inside the two commands that need
# them, not here: they pull in PySpark, and `ls` and `compact` are useful from
# the project venv on a machine with no JVM installed.

logger = get_logger(__name__)


def _human(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:,.1f} {unit}"
        size /= 1024
    return f"{size:,.1f} GB"


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------


def command_reconcile(args: argparse.Namespace) -> int:
    from spark.job import render_report, run_reconciliation
    from spark.session import build_session

    store = BronzeStore()
    spark = build_session("adtech-silver-reconcile", shuffle_partitions=args.shuffle_partitions)
    try:
        report = run_reconciliation(
            spark,
            store,
            tables=args.tables or None,
            include_events=args.include_events,
            snapshot_run_id=args.snapshot_run,
            rows_per_file=args.rows_per_file,
            dry_run=args.dry_run,
        )
    finally:
        spark.stop()

    print(render_report(report))
    if args.dry_run:
        print(" Dry run: nothing was written.\n")
    return 0


# ---------------------------------------------------------------------------
# compact
# ---------------------------------------------------------------------------


def command_compact(args: argparse.Namespace) -> int:
    store = BronzeStore()
    store.ping()

    groups = [group for group in compact.plan(store, table=args.table) if not group.redundant]
    if not groups:
        print("Nothing to compact: every CDC partition is already a single object.")
        return 0

    print(f"{'table':<20}{'partition':<14}{'p':>3}{'files':>8}{'offsets':>26}")
    print("-" * 71)
    for group in groups:
        offsets = f"{group.first_offset:,} - {group.last_offset:,}"
        print(
            f"{group.table:<20}dt={group.partition_date:<11}{group.topic_partition:>3}"
            f"{len(group.sources):>8}{offsets:>26}"
        )
    print("-" * 71)
    total_files = sum(len(group.sources) for group in groups)
    print(f"{total_files:,} object(s) in {len(groups)} group(s) -> {len(groups)} object(s)")

    if not args.commit:
        print("\nDry run. Re-run with --commit to rewrite and delete the inputs.")
        return 0

    records = 0
    deleted = 0
    written = 0
    for group in groups:
        result = compact.compact_group(store, group)
        records += result.records
        deleted += result.deleted
        written += result.bytes_written
    print(f"\nMerged {records:,} record(s) into {len(groups)} object(s) ({_human(written)}).")
    print(f"Deleted {deleted:,} source object(s).")
    return 0


# ---------------------------------------------------------------------------
# ls / show
# ---------------------------------------------------------------------------


def command_ls(args: argparse.Namespace) -> int:
    store = BronzeStore()
    store.ping()

    tables: dict[str, list[int]] = {}
    for key in store.list_keys(f"{layout.SILVER_PREFIX}/"):
        parts = key.split("/")
        min_depth = 3
        if len(parts) < min_depth or parts[1].startswith("_"):
            continue
        entry = tables.setdefault(parts[1], [0, 0])
        entry[0] += 1

    if not tables:
        print("Silver is empty. Build it with:  make silver")
        return 1

    for name in tables:
        _, size = store.summarise(f"{layout.SILVER_PREFIX}/{name}/")
        tables[name][1] = size

    print(f"s3://{store.settings.bucket}/{layout.SILVER_PREFIX}/\n")
    print(f"{'table':<24}{'objects':>10}{'size':>14}")
    print("-" * 48)
    for name, (count, size) in sorted(tables.items()):
        print(f"{name:<24}{count:>10,}{_human(size):>14}")
    print("-" * 48)

    runs = sorted(store.list_keys(f"{layout.SILVER_RUNS_PREFIX}/"))
    if runs:
        print(f"\n{len(runs)} run(s), newest: {runs[-1].split('/')[-1].removesuffix('.json')}")
    return 0


def command_show(args: argparse.Namespace) -> int:
    from spark.session import build_session

    store = BronzeStore()
    spark = build_session("adtech-silver-show")
    try:
        url = layout.s3a_url(store.settings.bucket, layout.silver_table_prefix(args.table))
        frame = spark.read.parquet(url)
        if args.deleted_only:
            frame = frame.filter("is_deleted")
        print(f"{url}\n{frame.count():,} row(s)\n")
        frame.show(args.limit, truncate=args.truncate)
    finally:
        spark.stop()
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="silver", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("reconcile", help="merge snapshot and change stream into Silver")
    run.add_argument(
        "--tables",
        nargs="*",
        help=f"tables to reconcile (default: {', '.join(RECONCILED_TABLES)})",
    )
    run.add_argument(
        "--include-events",
        action="store_true",
        help="also copy the append-only event tables through (adds ~7GB and minutes)",
    )
    run.add_argument("--snapshot-run", help="snapshot run id (default: the newest complete one)")
    run.add_argument("--rows-per-file", type=int, default=DEFAULT_ROWS_PER_FILE)
    run.add_argument("--shuffle-partitions", type=int, default=0)
    run.add_argument("--dry-run", action="store_true", help="count everything, write nothing")
    run.set_defaults(func=command_reconcile)

    squash = sub.add_parser(
        "compact", help="merge a day's small CDC objects into one per partition"
    )
    squash.add_argument("--table", help="limit to one table")
    squash.add_argument(
        "--commit", action="store_true", help="actually rewrite and delete (default: dry run)"
    )
    squash.set_defaults(func=command_compact)

    listing = sub.add_parser("ls", help="what is in the Silver layer")
    listing.set_defaults(func=command_ls)

    show = sub.add_parser("show", help="read reconciled rows back out")
    show.add_argument("table")
    show.add_argument("--limit", type=int, default=20)
    show.add_argument("--deleted-only", action="store_true", help="only soft-deleted rows")
    show.add_argument("--truncate", action="store_true", default=False)
    show.set_defaults(func=command_show)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        return args.func(args)
    except (BronzeStorageError, layout.SilverLayoutError, compact.CompactionError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
