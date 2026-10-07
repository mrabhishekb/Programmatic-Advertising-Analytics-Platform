"""Writing Silver out as plain Parquet, for Snowflake to ingest.

Snowflake cannot read this project's Iceberg tables. Its Iceberg support needs
an external volume on real S3, Azure or GCS, and the warehouse here is MinIO on
a laptop with no route in from the outside. So the lakehouse exports and the
warehouse loads, rather than the two sharing storage.

That costs a copy, and it is worth being clear about what it buys back: the
export is a *published* copy, written once per load and immutable afterwards,
rather than Snowflake reading tables that incremental merges are rewriting
underneath it. A load either sees a complete export or none at all.

The export is deliberately dumb. It carries every Silver column through,
including the soft-delete flags and the lineage columns, because the warehouse
has uses for all of them - ``is_deleted`` drives the staging filters and
``_lsn`` is what phase 11's snapshots order history by. Deciding what to keep is
staging's job, and doing it here would push a transformation below the layer
that is supposed to be a faithful landing copy.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from bronze.storage import BronzeStore
from data_generator.logging_setup import get_logger
from spark import catalog, layout

logger = get_logger(__name__)

#: Where exports land. A sibling of ``warehouse/``, not inside it: Iceberg owns
#: every prefix under the warehouse root, and plain Parquet among its metadata
#: is the kind of thing that makes a reader wonder whether it is part of a table.
EXPORT_PREFIX = "export"

#: Written beside the data so a load can check it got everything. Cross-system
#: row counts are the cheapest real check there is, and the one that catches a
#: COPY that silently skipped a file.
MANIFEST_KEY = f"{EXPORT_PREFIX}/_manifest.json"


@dataclass(frozen=True, slots=True)
class ExportedTable:
    table: str
    rows: int
    files: int
    bytes: int


@dataclass(frozen=True, slots=True)
class ExportReport:
    exported_at: str
    tables: tuple[ExportedTable, ...]

    @property
    def total_rows(self) -> int:
        return sum(entry.rows for entry in self.tables)

    @property
    def total_bytes(self) -> int:
        return sum(entry.bytes for entry in self.tables)


def table_prefix(table: str) -> str:
    return f"{EXPORT_PREFIX}/{table}"


def export_url(bucket: str, table: str) -> str:
    return layout.s3a_url(bucket, table_prefix(table))


def _written_size(store: BronzeStore, table: str) -> tuple[int, int]:
    """Files and bytes actually written, read back from the object store.

    Asking the store rather than trusting the writer: Spark reports what it
    intended to write, and the question worth answering is what is there.
    """
    # Counted over the Parquet alone. Spark drops a zero-byte ``_SUCCESS``
    # marker beside the data, and including it would report one more file than
    # Snowflake will ever load.
    files, total = 0, 0
    for key, size in store.summarise_keys(f"{table_prefix(table)}/"):
        if key.endswith(".parquet"):
            files += 1
            total += size
    return files, total


def export_table(
    spark: Any,
    store: BronzeStore,
    *,
    table: str,
    rows_per_file: int = layout.DEFAULT_ROWS_PER_FILE,
) -> ExportedTable:
    """Write one Silver table to the export prefix, replacing what was there."""
    identifier = catalog.table_identifier(table)
    frame = spark.table(identifier)
    rows = frame.count()

    # One file per million rows rather than one per Spark partition: the
    # partitioning that suited the merge has nothing to do with what Snowflake
    # wants to ingest, and COPY parallelises across files.
    partitions = max(1, -(-rows // rows_per_file)) if rows else 1

    (
        frame.repartition(partitions)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(export_url(store.settings.bucket, table))
    )

    files, size = _written_size(store, table)
    logger.info("exported", extra={"table": table, "rows": rows, "files": files, "bytes": size})
    return ExportedTable(table=table, rows=rows, files=files, bytes=size)


def export_tables(
    spark: Any,
    store: BronzeStore,
    *,
    tables: list[str],
    rows_per_file: int = layout.DEFAULT_ROWS_PER_FILE,
) -> ExportReport:
    exported = []
    for table in tables:
        identifier = catalog.table_identifier(table)
        if not spark.catalog.tableExists(identifier):
            logger.warning("skipping, no Silver table", extra={"table": table})
            continue
        exported.append(export_table(spark, store, table=table, rows_per_file=rows_per_file))

    report = ExportReport(
        exported_at=datetime.now(UTC).isoformat(timespec="seconds"),
        tables=tuple(exported),
    )
    store.put_bytes(
        MANIFEST_KEY,
        json.dumps(
            {
                "exported_at": report.exported_at,
                "tables": [asdict(entry) for entry in report.tables],
            },
            indent=2,
        ).encode(),
        content_type="application/json",
    )
    return report


def render(report: ExportReport) -> str:
    width = 62
    lines = [
        "",
        " SILVER EXPORT",
        "=" * width,
        f" {'table':<22}{'rows':>14}{'files':>8}{'size':>14}",
        " " + "-" * (width - 2),
    ]
    for entry in report.tables:
        lines.append(
            f" {entry.table:<22}{entry.rows:>14,}{entry.files:>8,}"
            f"{layout.human_bytes(entry.bytes):>14}"
        )
    lines += [
        " " + "-" * (width - 2),
        f" {'total':<22}{report.total_rows:>14,}"
        f"{sum(e.files for e in report.tables):>8,}"
        f"{layout.human_bytes(report.total_bytes):>14}",
        "",
        " Next: make warehouse-load",
        "",
    ]
    return "\n".join(lines)
