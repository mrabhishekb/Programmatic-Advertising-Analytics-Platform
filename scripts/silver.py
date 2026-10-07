#!/usr/bin/env python3
"""Reconcile Bronze into current-state Silver tables, and inspect the result.

    python scripts/silver.py reconcile    snapshot + change stream -> current state
    python scripts/silver.py compact      merge a day's small CDC objects
    python scripts/silver.py ls           what is in the Silver layer
    python scripts/silver.py show         read reconciled rows back out
    python scripts/silver.py history      every version of a table, and its lineage
    python scripts/silver.py drop-legacy  remove phase 5's flat Parquet

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
from data_generator.models import EVENT_TABLES
from spark import catalog, compact, layout
from spark.layout import DEFAULT_ROWS_PER_FILE, RECONCILED_TABLES, human_bytes

# `spark.job` and `spark.session` are imported inside the commands that need
# them, not here: they pull in PySpark, and `ls`, `compact` and `drop-legacy`
# are useful from the project venv on a machine with no JVM installed.

logger = get_logger(__name__)


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
            force_full=args.full,
        )
    finally:
        spark.stop()

    print(render_report(report))
    if args.dry_run:
        print(" Dry run: nothing was written.\n")
    return 0


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def command_status(_args: argparse.Namespace) -> int:
    """What the next run would do, without doing any of it."""
    from spark import incremental
    from spark.session import build_session

    store = BronzeStore()
    store.ping()
    snapshot = layout.latest_snapshot_run(store, None)
    spark = build_session("adtech-silver-status")
    try:
        plans = [
            incremental.plan_table(
                spark,
                store,
                table=table,
                bronze_run=snapshot.run_id,
                bucket=store.settings.bucket,
            )
            for table in [*RECONCILED_TABLES, *sorted(EVENT_TABLES)]
        ]
    finally:
        spark.stop()

    print(f"\n Snapshot export : {snapshot.run_id} @ LSN {snapshot.wal_lsn:,}")
    print(f" {'table':<19}{'next run':>13}{'applied LSN':>16}{'pending':>9}  why")
    print(" " + "-" * 78)
    for plan in plans:
        lsn = f"{plan.state.applied_lsn:,}" if plan.state.applied_lsn else "-"
        print(
            f" {plan.table:<19}{plan.mode.value:>13}{lsn:>16}{len(plan.cdc_urls):>9}  {plan.reason}"
        )
    print()
    print(" 'pending' counts Bronze objects, not changes: one whose offset range")
    print(" straddles the bookmark is read again and filtered out by LSN.")
    print(" Event tables are only attempted with --include-events.")
    print()
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
    print(f"\nMerged {records:,} record(s) into {len(groups)} object(s) ({human_bytes(written)}).")
    print(f"Deleted {deleted:,} source object(s).")
    return 0


# ---------------------------------------------------------------------------
# ls / show
# ---------------------------------------------------------------------------


def command_ls(args: argparse.Namespace) -> int:
    """What the warehouse holds, counted from the object store rather than Spark.

    Deliberately not a catalog query: this is the one command that still answers
    without a JVM, and the figure it reports - bytes actually stored - is the one
    a catalog query cannot give. An Iceberg table's current snapshot is usually
    smaller than its prefix, because superseded files stay until expired.
    """
    store = BronzeStore()
    store.ping()

    prefix = f"{catalog.namespace_prefix()}/"
    tables: dict[str, list[int]] = {}
    for key in store.list_keys(prefix):
        name = key[len(prefix) :].split("/")[0]
        if not name:
            continue
        tables.setdefault(name, [0, 0])[0] += 1

    if not tables:
        print("Silver is empty. Build it with:  make silver")
        return 1

    for name in tables:
        _, size = store.summarise(f"{prefix}{name}/")
        tables[name][1] = size

    print(f"{catalog.CATALOG}.{catalog.NAMESPACE}.*")
    print(f"s3://{store.settings.bucket}/{prefix}\n")
    print(f"{'table':<24}{'objects':>10}{'stored':>14}")
    print("-" * 48)
    for name, (count, size) in sorted(tables.items()):
        print(f"{name:<24}{count:>10,}{human_bytes(size):>14}")
    print("-" * 48)
    print("Stored bytes include metadata and superseded snapshots.")

    runs = sorted(store.list_keys(f"{layout.SILVER_RUNS_PREFIX}/"))
    if runs:
        print(f"\n{len(runs)} run(s), newest: {runs[-1].split('/')[-1].removesuffix('.json')}")
    return 0


def command_show(args: argparse.Namespace) -> int:
    from spark.session import build_session

    spark = build_session("adtech-silver-show")
    identifier = catalog.table_identifier(args.table)
    try:
        if args.as_of:
            # Time travel. The snapshot id comes from `silver.py history`, and
            # the files it names are still in the bucket because nothing has
            # expired them - that retention is what makes this work at all.
            frame = spark.read.option("snapshot-id", args.as_of).table(identifier)
            print(f"{identifier} as of snapshot {args.as_of}")
        else:
            frame = spark.table(identifier)
            print(identifier)
        if args.deleted_only:
            frame = frame.filter("is_deleted")
        print(f"{frame.count():,} row(s)\n")
        frame.show(args.limit, truncate=args.truncate)
    finally:
        spark.stop()
    return 0


def command_history(args: argparse.Namespace) -> int:
    """Every version of a table, with the Bronze run that produced it."""
    from spark.session import build_session

    spark = build_session("adtech-silver-history")
    identifier = catalog.table_identifier(args.table)
    try:
        rows = spark.sql(
            f"SELECT snapshot_id, committed_at, operation, summary "
            f"FROM {identifier}.snapshots ORDER BY committed_at"
        ).collect()
        print(f"{identifier}\n")
        print(f"{'snapshot_id':>20}  {'committed':<20}{'op':<10}{'rows':>14}  bronze run")
        print("-" * 92)
        for row in rows:
            summary = row["summary"] or {}
            print(
                f"{row['snapshot_id']:>20}  "
                f"{row['committed_at'].strftime('%Y-%m-%d %H:%M:%S'):<20}"
                f"{row['operation']:<10}"
                f"{int(summary.get('total-records', 0)):>14,}  "
                f"{summary.get(catalog.BRONZE_RUN_PROPERTY, '-')}"
            )
        print("-" * 92)
        print(f"\nRead an older version:  make silver-show TABLE={args.table} AS_OF=<snapshot_id>")
    finally:
        spark.stop()
    return 0


def command_drop_legacy(args: argparse.Namespace) -> int:
    """Delete phase 5's flat Parquet, which Iceberg superseded."""
    store = BronzeStore()
    store.ping()

    keys = list(store.list_keys(f"{layout.LEGACY_SILVER_PREFIX}/"))
    if not keys:
        print(f"Nothing at s3://{store.settings.bucket}/{layout.LEGACY_SILVER_PREFIX}/.")
        return 0

    _, size = store.summarise(f"{layout.LEGACY_SILVER_PREFIX}/")
    print(f"s3://{store.settings.bucket}/{layout.LEGACY_SILVER_PREFIX}/")
    print(f"{len(keys):,} object(s), {human_bytes(size)}")
    if not args.commit:
        print("\nDry run. Re-run with --commit to delete.")
        return 0

    store.delete_keys(keys)
    print(f"\nDeleted {len(keys):,} object(s).")
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
    run.add_argument(
        "--full",
        action="store_true",
        help="rebuild every table from the snapshot export, ignoring the watermark",
    )
    run.set_defaults(func=command_reconcile)

    status = sub.add_parser("status", help="how far each Silver table has got")
    status.set_defaults(func=command_status)

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
    show.add_argument("--as-of", help="read an older snapshot id (see: silver.py history)")
    show.set_defaults(func=command_show)

    history = sub.add_parser("history", help="every version of a table, and its lineage")
    history.add_argument("table")
    history.set_defaults(func=command_history)

    legacy = sub.add_parser(
        "drop-legacy", help=f"remove phase 5's {layout.LEGACY_SILVER_PREFIX}/ Parquet"
    )
    legacy.add_argument("--commit", action="store_true", help="actually delete (default: dry run)")
    legacy.set_defaults(func=command_drop_legacy)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        return args.func(args)
    except (
        BronzeStorageError,
        catalog.CatalogError,
        layout.SilverLayoutError,
        compact.CompactionError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
