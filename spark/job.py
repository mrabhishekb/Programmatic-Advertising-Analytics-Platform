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
from spark import layout, reconcile
from spark.layout import DEFAULT_ROWS_PER_FILE, RECONCILED_TABLES
from spark.schemas import IS_DELETED

logger = get_logger(__name__)


@dataclass
class RunReport:
    run_id: str
    snapshot_run_id: str
    snapshot_wal_lsn: int
    bucket: str
    started_at: str
    duration_seconds: float = 0.0
    tables: list[dict[str, Any]] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(item["output_rows"] for item in self.tables)

    @property
    def total_deleted(self) -> int:
        return sum(item["deleted"] for item in self.tables)


def run_reconciliation(
    spark: Any,
    store: BronzeStore,
    *,
    tables: list[str] | None = None,
    include_events: bool = False,
    snapshot_run_id: str | None = None,
    rows_per_file: int = DEFAULT_ROWS_PER_FILE,
    dry_run: bool = False,
) -> RunReport:
    started = perf_counter()
    store.ping()

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
        report.tables.append(
            _reconcile_one(
                spark,
                store,
                table=table,
                snapshot=snapshot,
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
    store: BronzeStore,
    *,
    table: str,
    snapshot: layout.SnapshotRun,
    rows_per_file: int,
    dry_run: bool,
) -> dict[str, Any]:
    table_started = perf_counter()
    bucket = store.settings.bucket
    model = MODEL_BY_TABLE.get(table)
    if model is None:
        raise layout.SilverLayoutError(f"no model registered for table {table!r}")

    snapshot_url = layout.snapshot_table_url(bucket, snapshot.run_id, table)
    has_changes = layout.has_cdc_data(store, table)
    cdc_url = layout.cdc_table_url(bucket, table) if has_changes else None

    change_events = 0
    tombstones = 0
    if has_changes:
        change_events, tombstones = reconcile.count_events(spark.read.parquet(cdc_url))

    silver, _ = reconcile.reconcile_table(
        spark,
        table=table,
        primary_key=model.PRIMARY_KEY,
        snapshot_url=snapshot_url,
        cdc_url=cdc_url,
        min_lsn=snapshot.wal_lsn,
    )

    # Cached because the counts below and the write would otherwise each redo
    # the whole join. On the dimension tables this fits in memory comfortably.
    silver = silver.cache()
    output_rows = silver.count()
    deleted = silver.filter(F.col(IS_DELETED)).count()

    bytes_written = 0
    if not dry_run:
        partitions = max(1, -(-output_rows // rows_per_file))
        (
            silver.coalesce(partitions)
            .write.mode("overwrite")
            .option("compression", "zstd")
            .parquet(layout.s3a_url(bucket, layout.silver_table_prefix(table)))
        )
        _, bytes_written = store.summarise(f"{layout.silver_table_prefix(table)}/")

    snapshot_rows = spark.read.parquet(snapshot_url).count()
    silver.unpersist()

    entry = {
        "table": table,
        "primary_key": model.PRIMARY_KEY,
        "snapshot_rows": snapshot_rows,
        "change_events": change_events,
        "tombstones": tombstones,
        "output_rows": output_rows,
        "deleted": deleted,
        "bytes": bytes_written,
        "duration_seconds": round(perf_counter() - table_started, 2),
    }
    logger.info("reconciled table", extra=entry)
    return entry


def render_report(report: RunReport) -> str:
    lines = ["", "=" * 78, " SILVER RECONCILIATION", "=" * 78]
    lines.append(f" Run           : {report.run_id}")
    lines.append(f" Snapshot      : {report.snapshot_run_id} @ LSN {report.snapshot_wal_lsn:,}")
    lines.append(f" Destination   : s3://{report.bucket}/{layout.SILVER_PREFIX}/")
    lines.append(f" Duration      : {report.duration_seconds:,.1f}s")
    lines.append("")
    header = (
        f" {'table':<20}{'snapshot':>12}{'events':>10}{'applied':>10}{'rows':>12}{'deleted':>9}"
    )
    lines.append(header)
    lines.append(" " + "-" * 73)
    for item in report.tables:
        applied = item["change_events"] - item["tombstones"]
        lines.append(
            f" {item['table']:<20}{item['snapshot_rows']:>12,}{item['change_events']:>10,}"
            f"{applied:>10,}{item['output_rows']:>12,}{item['deleted']:>9,}"
        )
    lines.append(" " + "-" * 73)
    lines.append(f" {'total':<20}{'':>12}{'':>10}{'':>10}{report.total_rows:>12,}")
    lines.append("")
    lines.append(" One row per primary key, deleted rows kept and flagged.")
    return "\n".join(lines)
