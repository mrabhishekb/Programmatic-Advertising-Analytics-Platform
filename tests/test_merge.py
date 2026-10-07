"""Incremental merge semantics, against a real Iceberg table.

``test_incremental`` covers the decision of what to read; this covers what
happens when it is applied. The claim being tested is the one phase 7 rests on:
that the watermark does not need storing because ``max(_lsn)`` over the table
already is it. That is only true if the merge always lands the highest-LSN
change in a batch, so it is worth proving rather than asserting.

A local Hadoop catalog under a temp directory - the catalog type is not what is
under test here, and a filesystem one needs no Postgres.

Needs a JVM, so these are marked ``spark`` and run in the Spark container:

    make spark-test
"""

from __future__ import annotations

import json
import shutil
import tempfile
from datetime import datetime

import pytest

pytest.importorskip("pyspark", reason="PySpark is only installed in the Spark image")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark import incremental, reconcile, schemas

pytestmark = pytest.mark.spark

MILLIS = 20468 * 86_400_000

#: The Bronze CDC record, as ``bronze.records`` writes it.
BRONZE_SCHEMA = StructType(
    [
        StructField("op", StringType(), False),
        StructField("source_schema", StringType(), True),
        StructField("source_table", StringType(), False),
        StructField("event_ts_ms", LongType(), True),
        StructField("lsn", LongType(), True),
        StructField("tx_id", LongType(), True),
        StructField("key", StringType(), True),
        StructField("before_json", StringType(), True),
        StructField("after_json", StringType(), True),
        StructField("kafka_topic", StringType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", LongType(), False),
        StructField("ingested_at_ms", LongType(), False),
    ]
)

TARGET = StructType(
    [
        StructField("campaign_id", StringType(), False),
        StructField("campaign_name", StringType(), True),
        StructField("campaign_status", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    warehouse = tempfile.mkdtemp(prefix="iceberg-merge-")
    session = (
        SparkSession.builder.appName("adtech-merge-tests")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.t", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.t.type", "hadoop")
        .config("spark.sql.catalog.t.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


def _event(*, op: str, campaign_id: str, lsn: int, offset: int, status: str = "active") -> dict:
    payload = json.dumps(
        {
            "campaign_id": campaign_id,
            "campaign_name": "Spring Launch",
            "campaign_status": status,
            "updated_at": MILLIS,
        }
    )
    deleted = op == "d"
    return {
        "op": op,
        "source_schema": "public",
        "source_table": "campaigns",
        "event_ts_ms": MILLIS,
        "lsn": lsn,
        "tx_id": 1,
        "key": json.dumps({"campaign_id": campaign_id}),
        "before_json": payload if deleted else None,
        "after_json": None if deleted else payload,
        "kafka_topic": "cdc.public.campaigns",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "ingested_at_ms": MILLIS,
    }


@pytest.fixture
def table(spark, request):
    """A Silver table holding two snapshot-born rows, no changes behind them."""
    name = f"t.db.{request.node.name[:40].replace('-', '_')}"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS t.db")
    spark.sql(f"DROP TABLE IF EXISTS {name}")

    rows = [("c1", "Spring Launch", "active", datetime.utcfromtimestamp(MILLIS / 1000))]
    base = reconcile.snapshot_as_silver(spark.createDataFrame(rows, TARGET), TARGET)
    base.writeTo(name).tableProperty("format-version", "2").createOrReplace()
    return name


def _apply(spark, table, events, *, min_lsn=0):
    """Collapse a batch of Bronze events and merge whatever survives."""
    frame = spark.createDataFrame(events, schema=BRONZE_SCHEMA)
    changes = reconcile.collapse_changes(
        frame, primary_key="campaign_id", target=TARGET, min_lsn=min_lsn
    )
    merged = changes.count()
    if merged:
        reconcile.merge_into(spark, table, changes, primary_key="campaign_id")
    return merged


class TestWhatAMergeDoesToTheTable:
    def test_a_change_to_an_existing_key_updates_that_row(self, spark, table):
        _apply(spark, table, [_event(op="u", campaign_id="c1", lsn=100, offset=1, status="paused")])
        row = spark.table(table).filter("campaign_id = 'c1'").first()
        assert row["campaign_status"] == "paused"
        assert spark.table(table).count() == 1, "an update must not duplicate the key"

    def test_a_change_to_an_unknown_key_inserts_a_row(self, spark, table):
        _apply(spark, table, [_event(op="c", campaign_id="c2", lsn=100, offset=1)])
        assert spark.table(table).count() == 2

    def test_a_delete_marks_the_row_rather_than_removing_it(self, spark, table):
        _apply(spark, table, [_event(op="d", campaign_id="c1", lsn=100, offset=1)])
        row = spark.table(table).filter("campaign_id = 'c1'").first()
        assert row[schemas.IS_DELETED] is True
        assert spark.table(table).count() == 1, "soft deletes keep the row"

    def test_a_delete_for_a_key_the_table_never_saw_is_inserted_flagged(self, spark, table):
        """Inserted and deleted between two runs. The row did exist, and a
        historical join that still resolves it is what Silver promises."""
        _apply(spark, table, [_event(op="d", campaign_id="c9", lsn=100, offset=1)])
        row = spark.table(table).filter("campaign_id = 'c9'").first()
        assert row is not None
        assert row[schemas.IS_DELETED] is True

    def test_rows_no_change_touched_are_left_alone(self, spark, table):
        _apply(spark, table, [_event(op="c", campaign_id="c2", lsn=100, offset=1)])
        row = spark.table(table).filter("campaign_id = 'c1'").first()
        assert row["campaign_status"] == "active"
        assert row[schemas.SOURCE_LSN] is None, "an untouched row keeps its snapshot provenance"


class TestTheWatermarkIsDerivedFromTheRows:
    def test_an_untouched_table_has_no_watermark(self, spark, table):
        """Snapshot-born rows carry no LSN, so the first run has nothing to
        resume from and falls back to the export's own WAL position."""
        assert incremental.applied_lsn(spark, table) is None

    def test_the_watermark_equals_the_highest_lsn_applied(self, spark, table):
        _apply(
            spark,
            table,
            [
                _event(op="u", campaign_id="c1", lsn=100, offset=1, status="paused"),
                _event(op="c", campaign_id="c2", lsn=300, offset=2),
            ],
        )
        assert incremental.applied_lsn(spark, table) == 300

    def test_collapsing_does_not_lose_the_high_water_mark(self, spark, table):
        """Three changes to one key merge to one row, and that row is the
        newest of them - so the watermark still clears all three. This is the
        property that makes a stored bookmark unnecessary."""
        _apply(
            spark,
            table,
            [
                _event(op="u", campaign_id="c1", lsn=100, offset=1, status="paused"),
                _event(op="u", campaign_id="c1", lsn=200, offset=2, status="active"),
                _event(op="u", campaign_id="c1", lsn=300, offset=3, status="archived"),
            ],
        )
        assert spark.table(table).count() == 1
        assert incremental.applied_lsn(spark, table) == 300

    def test_rerunning_the_same_input_above_the_watermark_is_a_no_op(self, spark, table):
        events = [
            _event(op="u", campaign_id="c1", lsn=100, offset=1, status="paused"),
            _event(op="c", campaign_id="c2", lsn=300, offset=2),
        ]
        _apply(spark, table, events)
        watermark = incremental.applied_lsn(spark, table)

        assert _apply(spark, table, events, min_lsn=watermark) == 0

    def test_replaying_from_an_older_watermark_lands_the_same_table(self, spark, table):
        """What a crash between the merge and anything after it costs: the
        changes are seen twice, and collapsing still picks the newest per key,
        so the second pass is redundant rather than damaging."""
        events = [
            _event(op="u", campaign_id="c1", lsn=100, offset=1, status="paused"),
            _event(op="u", campaign_id="c1", lsn=300, offset=2, status="archived"),
        ]
        _apply(spark, table, events)
        first = spark.table(table).collect()

        _apply(spark, table, events, min_lsn=0)
        assert spark.table(table).collect() == first


class TestTheOffsetHintSurvivesInTableProperties:
    def test_offsets_written_after_a_merge_are_read_back(self, spark, table):
        incremental.record_offsets(spark, table, {0: 42, 1: 7})
        state = incremental.read_state(spark, table)
        assert state.offsets == {0: 42, 1: 7}

    def test_offsets_survive_a_later_merge(self, spark, table):
        """A snapshot property would not: it describes one commit, and the next
        merge writes a commit of its own."""
        incremental.record_offsets(spark, table, {0: 42})
        _apply(spark, table, [_event(op="c", campaign_id="c2", lsn=100, offset=1)])
        assert incremental.read_state(spark, table).offsets == {0: 42}
