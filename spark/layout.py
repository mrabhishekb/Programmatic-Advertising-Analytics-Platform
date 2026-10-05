"""Where Silver lives, and which Bronze inputs a run reads.

Bronze names objects after the Kafka offsets they contain, so a replay produces
the same key. Silver cannot do that: its whole output depends on every change
seen so far, so there is no stable range to name a file after. Since phase 6 it
does not try - Iceberg names the files and records which ones are the table, and
re-running replaces that set in one commit. Running the job twice over the same
inputs still leaves the same table, by a different route.

What lives here is everything *outside* an Iceberg table: which Bronze snapshot
run feeds a job, which change files it reads, and the run manifests that record
what a run did. Table layout itself is ``spark.catalog``'s business.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from bronze import layout as bronze_layout
from bronze.storage import BronzeStore
from data_generator.models import MASTER_TABLES, TABLE_NAMES
from spark import catalog

#: Phase 5's Silver: flat Parquet, one prefix per table, overwritten in place.
#: Phase 6 replaced it with Iceberg tables under ``warehouse/``, so anything
#: still here is superseded output that nothing reads. ``make silver-drop-legacy``
#: deletes it; it is not removed automatically, because silently deleting a
#: layer during an upgrade is how people lose data they meant to compare against.
LEGACY_SILVER_PREFIX = "silver"

#: Run manifests, newest last by name. Under the warehouse but outside the
#: namespace: Iceberg owns every prefix below ``<namespace>/``, and a file it
#: did not write sitting among its metadata is an invitation to confusion.
SILVER_RUNS_PREFIX = f"{catalog.WAREHOUSE_PREFIX}/_runs"

#: The tables reconciliation applies to: the ones Debezium captures. Event
#: tables are append-only and have no change stream, so for them "reconcile"
#: would mean "copy 7GB", which is why they are opt-in rather than default.
#:
#: Kept here rather than next to the job so the CLI can describe its own
#: defaults without importing PySpark, which would make `silver ls` need a JVM.
RECONCILED_TABLES: tuple[str, ...] = tuple(name for name in TABLE_NAMES if name in MASTER_TABLES)

#: Rows per output file, roughly. Small enough that a reader can skip most of a
#: table, large enough that opening the file is not the dominant cost.
DEFAULT_ROWS_PER_FILE = 1_000_000


class SilverLayoutError(RuntimeError):
    """Raised when the Bronze inputs a run needs are missing or unusable."""


def human_bytes(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:,.1f} {unit}"
        size /= 1024
    return f"{size:,.1f} GB"


@dataclass(frozen=True, slots=True)
class SnapshotRun:
    """A completed snapshot export, and the WAL position it is consistent as of."""

    run_id: str
    wal_lsn: int
    tables: tuple[str, ...]
    manifest_key: str


def silver_run_manifest_key(run_id: str) -> str:
    return f"{SILVER_RUNS_PREFIX}/{run_id}.json"


def s3a_url(bucket: str, key: str = "") -> str:
    """Hadoop addresses the same object store under a different scheme than boto3.

    ``s3a://`` is the Hadoop connector, not a different storage system: the
    bucket and keys are identical to the ones ``bronze.storage`` writes through
    boto3's ``s3://``.
    """
    return f"s3a://{bucket}/{key}" if key else f"s3a://{bucket}"


def parse_lsn(value: Any) -> int:
    """Turn a PostgreSQL LSN into the integer Debezium puts on each event.

    The manifest records ``pg_current_wal_lsn()`` output, which is the
    ``16/B374D848`` text form. Debezium reports the same position as a plain
    integer, so one of them has to be converted before they can be compared -
    and comparing them is the whole point of the manifest.
    """
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if "/" in text:
        high, low = text.split("/", 1)
        return (int(high, 16) << 32) + int(low, 16)
    return int(text)


def find_snapshot_runs(store: BronzeStore) -> list[SnapshotRun]:
    """Every *completed* snapshot export, oldest first.

    A run is completed only if its manifest exists. The manifest is written last,
    after every table has landed, so a run that was interrupted has Parquet files
    but no manifest - and reading those files would silently produce a Silver
    layer missing however many tables the export had not reached yet.
    """
    runs: list[SnapshotRun] = []
    for key in sorted(store.list_keys(f"{bronze_layout.SNAPSHOT_PREFIX}/")):
        if not key.endswith("/_manifest.json"):
            continue
        manifest = json.loads(store.read_bytes(key))
        runs.append(
            SnapshotRun(
                run_id=manifest["run_id"],
                wal_lsn=parse_lsn(manifest["consistent_snapshot"]["wal_lsn"]),
                tables=tuple(item["table"] for item in manifest.get("tables", [])),
                manifest_key=key,
            )
        )
    return runs


def latest_snapshot_run(store: BronzeStore, run_id: str | None = None) -> SnapshotRun:
    """The newest completed snapshot, or a named one.

    Defaulting to the newest rather than reading every run matters: the bucket
    accumulates one prefix per export, and a reader that globbed ``snapshot/*``
    would union them and emit one row per run per primary key.
    """
    runs = find_snapshot_runs(store)
    if not runs:
        raise SilverLayoutError(
            "No completed snapshot export found in Bronze.\n"
            "Run one first:  make export-snapshot   (or make export-snapshot-master)"
        )
    if run_id is None:
        return runs[-1]
    for run in runs:
        if run.run_id == run_id:
            return run
    available = ", ".join(item.run_id for item in runs)
    raise SilverLayoutError(f"snapshot run {run_id!r} not found. Available: {available}")


def snapshot_table_url(bucket: str, run_id: str, table: str) -> str:
    return s3a_url(bucket, f"{bronze_layout.snapshot_run_prefix(run_id)}/{table}/")


def cdc_table_url(bucket: str, table: str) -> str:
    """Every day of changes for one table.

    Deliberately unpartitioned in the path handed to Spark: reconciliation has to
    see a key's whole history to pick its newest version, so reading a single
    ``dt=`` partition would resurrect rows whose later changes live in another.
    """
    return s3a_url(bucket, f"{bronze_layout.CDC_PREFIX}/{table}/")


def has_cdc_data(store: BronzeStore, table: str) -> bool:
    """Whether any change events exist for a table.

    Spark raises on reading a path with no files, and a table nobody has edited
    yet is a normal state rather than an error - every table looks like this
    before its first change.
    """
    prefix = f"{bronze_layout.CDC_PREFIX}/{table}/"
    return any(key.endswith(".parquet") for key in store.list_keys(prefix))
