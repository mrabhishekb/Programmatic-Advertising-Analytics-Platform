"""Running reconciliation over every table and recording what happened.

``reconcile.py`` holds the logic and touches no storage, which is what makes it
testable without a bucket. This module is the part that knows where things live,
in what order to do them, and what to write down afterwards.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

from pyspark.sql import functions as F

from bronze import layout as bronze_layout
from bronze.storage import BronzeStore
from data_generator.logging_setup import get_logger
from data_generator.models import EVENT_TABLES, MODEL_BY_TABLE, TABLE_NAMES
from spark import catalog, incremental, layout, reconcile, schemas
from spark.layout import DEFAULT_ROWS_PER_FILE, RECONCILED_TABLES
from spark.schemas import IS_DELETED

logger = get_logger(__name__)

#: Iceberg splits a write at roughly this size. 128MB is large enough that the
#: per-file overhead of opening a footer disappears, and small enough that one
#: task reading one file is still a reasonable unit of parallelism.
TARGET_FILE_BYTES = 128 * 1024 * 1024


@dataclass
class RunReport:
    run_id: str
    snapshot_run_id: str
    snapshot_wal_lsn: int
    bucket: str
    started_at: str
    duration_seconds: float = 0.0
    dry_run: bool = False
    tables: list[dict[str, Any]] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(item["output_rows"] for item in self.tables)

    @property
    def total_deleted(self) -> int:
        return sum(item["deleted"] for item in self.tables)

    @property
    def total_files(self) -> int:
        return sum(item["files"] for item in self.tables)

    @property
    def total_bytes(self) -> int:
        return sum(item["bytes"] for item in self.tables)

    @property
    def total_merged(self) -> int:
        return sum(item["merged_rows"] for item in self.tables)

    @property
    def skipped(self) -> int:
        return sum(1 for item in self.tables if item["mode"] == "skip")

    @property
    def written(self) -> int:
        return len(self.tables) - self.skipped


def run_reconciliation(
    spark: Any,
    store: BronzeStore,
    *,
    tables: list[str] | None = None,
    include_events: bool = False,
    snapshot_run_id: str | None = None,
    rows_per_file: int = DEFAULT_ROWS_PER_FILE,
    dry_run: bool = False,
    force_full: bool = False,
) -> RunReport:
    started = perf_counter()
    store.ping()
    catalog.create_namespace(spark)

    snapshot = layout.latest_snapshot_run(store, snapshot_run_id)
    selected = list(tables or RECONCILED_TABLES)
    if include_events:
        selected += [name for name in TABLE_NAMES if name in EVENT_TABLES]

    missing = [name for name in selected if name not in snapshot.tables]
    if missing:
        raise layout.SilverLayoutError(
            f"snapshot run {snapshot.run_id} does not contain {missing}.\n"
            "It was probably exported with --master-only. Run: make export-snapshot"
        )

    report = RunReport(
        run_id=bronze_layout.new_run_id(),
        snapshot_run_id=snapshot.run_id,
        snapshot_wal_lsn=snapshot.wal_lsn,
        bucket=store.settings.bucket,
        started_at=datetime.now(UTC).isoformat(timespec="seconds"),
        dry_run=dry_run,
    )
    logger.info(
        "reconciling",
        extra={
            "run_id": report.run_id,
            "snapshot": snapshot.run_id,
            "wal_lsn": snapshot.wal_lsn,
            "tables": len(selected),
        },
    )

    for table in selected:
        plan = incremental.plan_table(
            spark,
            store,
            table=table,
            bronze_run=snapshot.run_id,
            bucket=store.settings.bucket,
            force_full=force_full,
        )
        report.tables.append(
            _reconcile_one(
                spark,
                table=table,
                bucket=store.settings.bucket,
                snapshot=snapshot,
                plan=plan,
                rows_per_file=rows_per_file,
                dry_run=dry_run,
            )
        )

    report.duration_seconds = round(perf_counter() - started, 2)
    if not dry_run:
        store.put_bytes(
            layout.silver_run_manifest_key(report.run_id),
            json.dumps(asdict(report), indent=2).encode(),
            content_type="application/json",
        )
    return report


def _reconcile_one(
    spark: Any,
    *,
    table: str,
    bucket: str,
    snapshot: layout.SnapshotRun,
    plan: incremental.TablePlan,
    rows_per_file: int,
    dry_run: bool,
) -> dict[str, Any]:
    table_started = perf_counter()
    model = MODEL_BY_TABLE.get(table)
    if model is None:
        raise layout.SilverLayoutError(f"no model registered for table {table!r}")

    identifier = catalog.table_identifier(table)
    entry: dict[str, Any] = {
        "table": table,
        "primary_key": model.PRIMARY_KEY,
        "identifier": identifier,
        "mode": plan.mode.value,
        "reason": plan.reason,
        "partitioned_by": None,
        "snapshot_rows": 0,
        "change_events": 0,
        "tombstones": 0,
        "merged_rows": 0,
        "output_rows": 0,
        "deleted": 0,
        "files": 0,
        "bytes": 0,
        "iceberg_snapshot_id": None,
    }

    # A table that is not being written still reports its size, so the totals
    # describe the layer as it stands rather than only the part this run
    # touched. Without it a skipped table reads as one that vanished, and a dry
    # run's total mixes real sizes with zeros into a number that is neither the
    # current state nor a projection.
    if (plan.mode is incremental.RunMode.SKIP or dry_run) and plan.state.exists:
        entry["output_rows"], entry["files"], entry["bytes"] = _table_size(spark, identifier)
        entry["iceberg_snapshot_id"] = _current_snapshot_id(spark, identifier)

    if plan.full:
        _run_full(
            spark,
            entry,
            table=table,
            bucket=bucket,
            model=model,
            snapshot=snapshot,
            plan=plan,
            rows_per_file=rows_per_file,
            dry_run=dry_run,
        )
    elif plan.writes:
        _run_incremental(spark, entry, model=model, plan=plan, dry_run=dry_run)

    entry["duration_seconds"] = round(perf_counter() - table_started, 2)
    logger.info("reconciled table", extra=entry)
    return entry


def _run_full(
    spark: Any,
    entry: dict[str, Any],
    *,
    table: str,
    bucket: str,
    model: Any,
    snapshot: layout.SnapshotRun,
    plan: incremental.TablePlan,
    rows_per_file: int,
    dry_run: bool,
) -> None:
    """Build the table from the snapshot export plus its whole change history."""
    identifier = entry["identifier"]
    snapshot_url = layout.snapshot_table_url(bucket, snapshot.run_id, table)
    cdc_urls = list(plan.cdc_urls)

    if cdc_urls:
        entry["change_events"], entry["tombstones"] = reconcile.count_events(
            spark.read.parquet(*cdc_urls)
        )

    # Counted before the write because it also sizes the output: Parquet keeps
    # row counts in the footer, so this reads metadata rather than the 6GB of
    # impressions underneath it.
    entry["snapshot_rows"] = spark.read.parquet(snapshot_url).count()

    silver, _ = reconcile.reconcile_table(
        spark,
        table=table,
        primary_key=model.PRIMARY_KEY,
        snapshot_url=snapshot_url,
        cdc_urls=cdc_urls,
        min_lsn=snapshot.wal_lsn,
    )

    partition_column = catalog.partition_column(table)
    entry["partitioned_by"] = f"days({partition_column})" if partition_column else None

    if dry_run:
        # One pass for both numbers. The phase 5 version cached the frame and
        # counted twice, which is the wrong trade once a 100M-row event table
        # can be in the set: caching that spills to disk and then reads it back.
        totals = silver.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.col(IS_DELETED).cast("long")).alias("deleted"),
        ).first()
        entry["output_rows"] = int(totals["rows"])
        entry["deleted"] = int(totals["deleted"] or 0)
        return

    _write_iceberg(
        silver,
        identifier=identifier,
        partition_column=partition_column,
        snapshot=snapshot,
        offsets=plan.offsets,
        # Only meaningful without partitioning. A partitioned write is
        # redistributed by partition value anyway, so coalescing first
        # would just throw away parallelism ahead of a shuffle.
        coalesce_to=(
            None if partition_column else max(1, -(-entry["snapshot_rows"] // rows_per_file))
        ),
    )
    # merged_rows stays zero: a rebuild writes the table rather than touching
    # rows in it, so "rows this run merged" has no meaning here and the row
    # count is the honest answer.
    _record_sizes(spark, entry, has_changes=bool(cdc_urls))


def _run_incremental(
    spark: Any,
    entry: dict[str, Any],
    *,
    model: Any,
    plan: incremental.TablePlan,
    dry_run: bool,
) -> None:
    """Merge the change events above the watermark into the existing table.

    The snapshot export is never opened. That is the whole saving: the base is
    already in Iceberg, and the only new information is the change events that
    arrived since the last run.
    """
    identifier = entry["identifier"]
    existing = spark.table(identifier)
    target = schemas.source_schema(existing.schema)

    events = spark.read.parquet(*plan.cdc_urls)
    entry["change_events"], entry["tombstones"] = reconcile.count_events(events)

    changes = reconcile.collapse_changes(
        events,
        primary_key=model.PRIMARY_KEY,
        target=target,
        # The watermark, not the export's WAL position: everything up to here
        # is already in the table, and reapplying it would be work for an
        # identical result.
        min_lsn=plan.state.applied_lsn or 0,
    )

    if dry_run:
        entry["merged_rows"] = changes.count()
        entry["output_rows"] = existing.count()
        return

    before = _table_size(spark, identifier)[0]
    changes = changes.cache()
    entry["merged_rows"] = changes.count()
    if entry["merged_rows"]:
        reconcile.merge_into(spark, identifier, changes, primary_key=model.PRIMARY_KEY)
    changes.unpersist()

    # Written after the merge rather than with it: unlike the LSN watermark
    # this one is only a file-pruning hint, so a crash in between costs a
    # re-read, not correctness. See spark.incremental.
    incremental.record_offsets(spark, identifier, plan.offsets)

    _record_sizes(spark, entry, has_changes=True)
    entry["inserted"] = max(0, entry["output_rows"] - before)
    entry["updated"] = entry["merged_rows"] - entry["inserted"]


def _record_sizes(spark: Any, entry: dict[str, Any], *, has_changes: bool) -> None:
    identifier = entry["identifier"]
    entry["output_rows"], entry["files"], entry["bytes"] = _table_size(spark, identifier)
    entry["iceberg_snapshot_id"] = _current_snapshot_id(spark, identifier)
    # A table Debezium does not capture has is_deleted false by construction, so
    # scanning 100M rows to rediscover that would be pure cost.
    entry["deleted"] = (
        spark.table(identifier).filter(F.col(IS_DELETED)).count() if has_changes else 0
    )


def _write_iceberg(
    frame: Any,
    *,
    identifier: str,
    partition_column: str | None,
    snapshot: layout.SnapshotRun,
    offsets: dict[int, int],
    coalesce_to: int | None,
) -> None:
    """Replace a Silver table's contents in a single commit.

    ``createOrReplace`` is not "drop and rewrite": the table keeps its identity
    and its history, and the new set of files becomes current in one atomic
    catalog update. A reader mid-run sees the previous version in full. That is
    the difference from phase 5, where the same operation was an S3 delete
    followed by a write and a reader in between saw a partial table.

    The lineage goes into the snapshot's own summary rather than a side file, so
    ``SELECT * FROM table.snapshots`` answers "which Bronze export produced
    this" for every version that ever existed, not just the current one.
    """
    if coalesce_to:
        frame = frame.coalesce(coalesce_to)

    writer = (
        frame.writeTo(identifier)
        # v2 is what makes row-level deletes possible. Nothing here writes
        # them - a full replace has no need - but phase 7's incremental MERGE
        # does, and the format version cannot be raised in place later without
        # rewriting every file.
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .tableProperty("write.target-file-size-bytes", str(TARGET_FILE_BYTES))
        # Table properties rather than snapshot properties for the two
        # bookmarks a later run reads back: a snapshot property describes one
        # commit, and the merges that follow this write would bury them.
        .tableProperty(incremental.BRONZE_RUN_TABLE_PROPERTY, snapshot.run_id)
        .tableProperty(incremental.APPLIED_OFFSETS_PROPERTY, incremental.encode_offsets(offsets))
        .option(f"snapshot-property.{catalog.BRONZE_RUN_PROPERTY}", snapshot.run_id)
        .option(f"snapshot-property.{catalog.BRONZE_LSN_PROPERTY}", str(snapshot.wal_lsn))
    )

    if partition_column:
        # `days(ts)` is a hidden partition: the stored value is derived from the
        # timestamp by Iceberg, so a query filtering on the timestamp prunes
        # partitions without anyone writing `WHERE dt = ...`. Hive-style layouts
        # need that extra column in the data and in every query that wants
        # pruning, which is how tables end up with a dt that disagrees with the
        # timestamp beside it.
        writer = writer.partitionedBy(F.days(F.col(partition_column))).tableProperty(
            # Redistribute by partition value so each task writes one day.
            # Without it every task writes into every day it happens to hold,
            # turning one write into tasks x days files.
            "write.distribution-mode",
            "hash",
        )
    else:
        writer = writer.tableProperty("write.distribution-mode", "none")

    writer.createOrReplace()


def _table_size(spark: Any, identifier: str) -> tuple[int, int, int]:
    """(rows, files, bytes) for the current snapshot, read from Iceberg metadata.

    Free, because Iceberg already stores a row count and a byte count per file
    in its manifests. Listing the table's prefix in S3 would be both slower and
    wrong: a replaced snapshot's files stay in the bucket until they are expired,
    so the prefix is larger than the table for as long as history is retained.

    ``record_count`` becomes an upper bound once phase 7 starts writing delete
    files, since a row deleted by one is still counted by the manifest that
    added it. There are none at this point - a full replace never writes any.
    """
    row = spark.sql(
        "SELECT coalesce(sum(record_count), 0) AS rows, count(*) AS files, "
        f"coalesce(sum(file_size_in_bytes), 0) AS bytes FROM {identifier}.files"
    ).first()
    return int(row["rows"]), int(row["files"]), int(row["bytes"])


def _current_snapshot_id(spark: Any, identifier: str) -> int | None:
    row = spark.sql(
        f"SELECT snapshot_id FROM {identifier}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()
    return int(row["snapshot_id"]) if row else None


def render_report(report: RunReport) -> str:
    width = 84
    lines = ["", "=" * width, " SILVER RECONCILIATION", "=" * width]
    lines.append(f" Run           : {report.run_id}")
    lines.append(f" Snapshot      : {report.snapshot_run_id} @ LSN {report.snapshot_wal_lsn:,}")
    lines.append(
        f" Destination   : {catalog.CATALOG}.{catalog.NAMESPACE}.*"
        f"   s3://{report.bucket}/{catalog.WAREHOUSE_PREFIX}/"
    )
    lines.append(f" Duration      : {report.duration_seconds:,.1f}s")
    lines.append("")
    rule = " " + "-" * (width - 3)
    lines.append(
        f" {'table':<19}{'mode':>12}{'events':>10}{'merged':>10}"
        f"{'rows':>14}{'files':>7}{'size':>11}"
    )
    lines.append(rule)
    for item in report.tables:
        merged = f"{item['merged_rows']:,}" if item["merged_rows"] else "-"
        lines.append(
            f" {item['table']:<19}{item['mode']:>12}{item['change_events']:>10,}{merged:>10}"
            f"{item['output_rows']:>14,}{item['files']:>7,}"
            f"{layout.human_bytes(item['bytes']):>11}"
        )
    lines.append(rule)
    lines.append(
        f" {'total':<19}{'':>12}{'':>10}{report.total_merged:>10,}"
        f"{report.total_rows:>14,}{report.total_files:>7,}"
        f"{layout.human_bytes(report.total_bytes):>11}"
    )
    lines.append("")
    verb = "would be written" if report.dry_run else "written"
    lines.append(
        f" {report.written} table(s) {verb}, {report.skipped} already current."
        " One row per primary key, deleted rows kept and flagged."
    )
    if report.dry_run:
        # Sizes are what is on disk now, because nothing was written and the
        # output's file layout is not knowable without producing it.
        lines.append(" 'rows' is what the run would produce; files and size are current.")
    for item in report.tables:
        if item["mode"] != "incremental":
            lines.append(f"   {item['table']:<19}{item['mode']:<13}{item['reason']}")
    lines.append(f" History: make silver-history TABLE={report.tables[0]['table']}")
    return "\n".join(lines)
