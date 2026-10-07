"""Merging the snapshot and the change stream into current state.

The shape of the problem
------------------------
Bronze holds two descriptions of the same table. The snapshot is every row as of
one WAL position. The CDC stream is every change after it. Current state is
neither: it is the snapshot with the changes applied, one row per primary key.

Three decisions make that correct rather than approximately correct.

**Order by LSN, not by time.** Every change carries an LSN, which is the position
in PostgreSQL's write-ahead log where it was committed. That is a total order
over the database. ``event_ts_ms`` is not: two changes in one transaction share
a commit timestamp, so ordering by it picks an arbitrary winner among exactly
the rows most likely to disagree.

**Drop what the snapshot already contains.** The export recorded the WAL position
it was consistent as of. Change events at or below it describe edits already
baked into the snapshot rows. Applying them again is *usually* harmless, because
the result is keyed - but not always, since a delete replayed over a row that was
re-inserted afterwards would wrongly remove it. Filtering is both cheaper and
correct.

**A delete marks the row, it does not remove it.** Deleting an audience segment
from Silver orphans every historical impression that referenced it, and the
question "what did that segment target" becomes permanently unanswerable. The row
stays with ``is_deleted`` set. This only works because all seven captured tables
are ``REPLICA IDENTITY FULL``, so a delete event carries the row's final state in
``before`` rather than just its primary key.

Tombstones
----------
Debezium follows each delete with a null-valued message so Kafka's log compaction
can drop the key. It carries no payload and no LSN. The delete immediately before
it says the same thing and carries the row, so tombstones are counted and
discarded rather than reconciled.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from bronze.records import OP_TOMBSTONE
from spark import schemas
from spark.schemas import DELETED_AT, IS_DELETED, SOURCE_EVENT_TS, SOURCE_LSN, SOURCE_OP

#: Debezium's operation codes. ``r`` is a snapshot read, which this connector
#: never emits (``snapshot.mode: no_data``) but which costs nothing to handle.
OP_DELETE = "d"
OP_CREATE = "c"
OP_UPDATE = "u"
OP_READ = "r"

DELETE_OPS = (OP_DELETE, OP_TOMBSTONE)


@dataclass(frozen=True, slots=True)
class TableResult:
    table: str
    snapshot_rows: int
    change_events: int
    tombstones: int
    applied: int
    inserted: int
    deleted: int
    output_rows: int


def primary_key_from_message(key_column: str, primary_key: str) -> Any:
    """Read the primary key out of the Kafka message key.

    Taken from the message key rather than from the payload because a tombstone
    has no payload, and because the message key is what Kafka partitions on - so
    it is the one identifier guaranteed present and consistent on every event for
    a row, whatever happened to it.
    """
    return F.get_json_object(F.col(key_column), f"$.{primary_key}")


def collapse_changes(
    events: DataFrame,
    *,
    primary_key: str,
    target: StructType,
    min_lsn: int | None = None,
) -> DataFrame:
    """One row per primary key: the newest change, typed, with deletes marked.

    ``events`` is the Bronze CDC schema (see ``bronze/records.py``). The result
    is the snapshot's columns plus the lineage columns in ``spark.schemas``.
    """
    wire = schemas.wire_schema(target)

    changes = events.withColumn("_pk", primary_key_from_message("key", primary_key))

    # Tombstones repeat the delete before them and carry neither payload nor
    # LSN, so they cannot order against anything. Dropped here rather than
    # ranked and discarded later, where a key whose delete happened to be
    # filtered out would leave the tombstone to win with an all-null payload.
    changes = changes.filter(F.col("op") != F.lit(OP_TOMBSTONE))

    if min_lsn is not None:
        changes = changes.filter(F.col("lsn") > F.lit(min_lsn))

    # `after` on everything except a delete, where it is null and the final
    # state is in `before` - which is only populated because the source tables
    # are REPLICA IDENTITY FULL.
    payload = F.coalesce(F.col("after_json"), F.col("before_json"))
    changes = changes.withColumn("_payload", F.from_json(payload, wire))

    # kafka_offset breaks ties within a partition. Two events cannot share an
    # LSN, so this only matters for malformed input - but ranking has to be
    # deterministic or a rerun can produce a different row.
    newest = Window.partitionBy("_pk").orderBy(
        F.col("lsn").desc_nulls_last(),
        F.col("kafka_offset").desc(),
    )

    ranked = changes.withColumn("_rank", F.row_number().over(newest)).filter(F.col("_rank") == 1)

    is_deleted = F.col("op").isin(list(DELETE_OPS))
    event_ts = F.timestamp_millis(F.col("event_ts_ms"))

    return ranked.select(
        *schemas.payload_columns("_payload", target),
        is_deleted.alias(IS_DELETED),
        F.when(is_deleted, event_ts).alias(DELETED_AT),
        F.col("lsn").alias(SOURCE_LSN),
        F.col("op").alias(SOURCE_OP),
        event_ts.alias(SOURCE_EVENT_TS),
    )


def snapshot_as_silver(snapshot: DataFrame, target: StructType) -> DataFrame:
    """Snapshot rows in the Silver shape: live, with no change behind them.

    ``_lsn`` is null rather than the export's WAL position: the row was read as
    part of a bulk SELECT, not produced by a change event, and filling in the
    snapshot's LSN would claim a provenance it does not have.
    """
    return snapshot.select(
        *[F.col(field.name) for field in target.fields],
        F.lit(False).alias(IS_DELETED),
        F.lit(None).cast("timestamp").alias(DELETED_AT),
        F.lit(None).cast("long").alias(SOURCE_LSN),
        F.lit(None).cast("string").alias(SOURCE_OP),
        F.lit(None).cast("timestamp").alias(SOURCE_EVENT_TS),
    )


def apply_changes(
    snapshot: DataFrame,
    changes: DataFrame,
    *,
    primary_key: str,
) -> DataFrame:
    """Snapshot rows the changes did not touch, plus the changes themselves.

    An anti-join rather than a full outer join with coalesce per column: the
    collapsed change already holds the row's complete state, so there is nothing
    to merge column-by-column, and the anti-join does not have to enumerate the
    schema. Rows inserted after the snapshot appear only in ``changes`` and come
    through with it.
    """
    untouched = snapshot.join(changes, on=primary_key, how="left_anti")
    return untouched.unionByName(changes)


def reconcile_table(
    spark: Any,
    *,
    table: str,
    primary_key: str,
    snapshot_url: str,
    cdc_urls: list[str],
    min_lsn: int | None,
) -> tuple[DataFrame, StructType]:
    """Build the Silver DataFrame for one table. Nothing is written here.

    The CDC side arrives as explicit object URLs rather than a directory so the
    caller decides what to read. Phase 6 always read everything; a run now opens
    only the objects its plan selected.
    """
    snapshot = spark.read.parquet(snapshot_url)
    target = snapshot.schema

    base = snapshot_as_silver(snapshot, target)
    if not cdc_urls:
        return base, target

    events = spark.read.parquet(*cdc_urls)
    changes = collapse_changes(events, primary_key=primary_key, target=target, min_lsn=min_lsn)
    return apply_changes(base, changes, primary_key=primary_key), target


def count_events(events: DataFrame) -> tuple[int, int]:
    """(change events, tombstones) in one pass over the Bronze CDC files."""
    counts = events.groupBy(F.col("op") == F.lit(OP_TOMBSTONE)).count().collect()
    tombstones = sum(row["count"] for row in counts if row[0])
    total = sum(row["count"] for row in counts)
    return total, tombstones


def highest_lsn(events: DataFrame) -> int | None:
    """The furthest WAL position in a set of change events, or None if empty.

    Taken over everything read rather than only over what survives collapsing:
    a key with three changes contributes one row to the merge but the watermark
    has to clear all three, or the next run reads them again and re-applies an
    older version over a newer one.

    Tombstones carry no LSN and are excluded by the null-safe aggregate - they
    repeat the delete before them, which does carry one.
    """
    row = events.agg(F.max(F.col("lsn")).alias("high")).first()
    return int(row["high"]) if row and row["high"] is not None else None


def merge_into(
    spark: Any,
    identifier: str,
    changes: DataFrame,
    *,
    primary_key: str,
) -> None:
    """Apply collapsed changes to an existing Silver table in one commit.

    The whole point of phase 7, and the reason phase 6 had to come first: this
    rewrites only the files holding rows a change touched, and leaves the rest
    alone. The phase 6 writer could only replace the table, so applying a
    thousand edits meant rewriting two hundred thousand rows.

    ``UPDATE SET *`` and ``INSERT *`` work only because ``collapse_changes``
    emits exactly the Silver schema. They are also better than naming every
    column: a column added upstream would otherwise be silently dropped on
    update, which surfaces months later as "that field is always null on rows
    somebody edited".

    A delete for a key the table has never seen - inserted and deleted between
    two runs - arrives as NOT MATCHED and is inserted with ``is_deleted`` set.
    That is correct under soft deletes: the row did exist, and a historical join
    that still resolves it is what this layer promises.

    Nothing is recorded here about how far the merge got. ``_lsn`` on the merged
    rows is that record, which is why it cannot drift from what committed - see
    ``spark.incremental``.
    """
    view = f"incoming_{identifier.rsplit('.', 1)[-1]}"
    changes.createOrReplaceTempView(view)
    try:
        spark.sql(f"""
            MERGE INTO {identifier} AS target
            USING {view} AS source
            ON target.{primary_key} = source.{primary_key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView(view)
