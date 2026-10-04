"""Reconciliation tests.

The logic in ``spark.reconcile`` takes DataFrames and returns DataFrames, so
these build the Bronze rows by hand rather than reading a bucket. That is the
point of keeping storage out of that module: a wrong answer here is a wrong
answer about merge semantics, not about S3 credentials.

Needs a JVM, so these are marked ``spark`` and run in the Spark container:

    make spark-test
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

import pytest

pytest.importorskip("pyspark", reason="PySpark is only installed in the Spark image")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from bronze.records import OP_TOMBSTONE
from spark import reconcile, schemas

pytestmark = pytest.mark.spark

#: Stands in for `campaigns`: one of every type the wire mapping has to handle.
TARGET = StructType(
    [
        StructField("campaign_id", StringType(), False),
        StructField("campaign_name", StringType(), True),
        StructField("campaign_status", StringType(), True),
        StructField("campaign_budget", DecimalType(14, 4), True),
        StructField("frequency_cap", IntegerType(), True),
        StructField("start_date", DateType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

EPOCH_DAYS_2026_01_15 = 20468
MILLIS_2026_01_15 = 20468 * 86_400_000


def _payload(campaign_id: str, status: str, budget: str = "1000.0000") -> str:
    """A row as Debezium serialises it: decimals as strings, dates as integers."""
    return json.dumps(
        {
            "campaign_id": campaign_id,
            "campaign_name": "Spring Launch",
            "campaign_status": status,
            "campaign_budget": budget,
            "frequency_cap": 3,
            "start_date": EPOCH_DAYS_2026_01_15,
            "updated_at": MILLIS_2026_01_15,
        }
    )


def _event(
    *,
    op: str,
    campaign_id: str,
    lsn: int | None,
    offset: int,
    after: str | None = None,
    before: str | None = None,
) -> dict:
    return {
        "op": op,
        "source_schema": "public",
        "source_table": "campaigns",
        "event_ts_ms": MILLIS_2026_01_15,
        "lsn": lsn,
        "tx_id": 1,
        "key": json.dumps({"campaign_id": campaign_id}),
        "before_json": before,
        "after_json": after,
        "kafka_topic": "cdc.public.campaigns",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "ingested_at_ms": MILLIS_2026_01_15,
    }


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("adtech-silver-tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _bronze_spark_schema() -> StructType:
    from pyspark.sql.types import LongType

    return StructType(
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


def _collapse(spark, events: list[dict], *, min_lsn: int | None = None):
    frame = spark.createDataFrame(events, schema=_bronze_spark_schema())
    return reconcile.collapse_changes(
        frame, primary_key="campaign_id", target=TARGET, min_lsn=min_lsn
    )


# ---------------------------------------------------------------------------
# ordering
# ---------------------------------------------------------------------------


class TestOrdering:
    def test_the_highest_lsn_wins_regardless_of_arrival_order(self, spark):
        events = [
            _event(op="u", campaign_id="c1", lsn=300, offset=2, after=_payload("c1", "ARCHIVED")),
            _event(op="u", campaign_id="c1", lsn=100, offset=0, after=_payload("c1", "ACTIVE")),
            _event(op="u", campaign_id="c1", lsn=200, offset=1, after=_payload("c1", "PAUSED")),
        ]
        rows = _collapse(spark, events).collect()

        assert len(rows) == 1
        assert rows[0]["campaign_status"] == "ARCHIVED"
        assert rows[0][schemas.SOURCE_LSN] == 300

    def test_each_key_collapses_independently(self, spark):
        events = [
            _event(op="u", campaign_id="c1", lsn=100, offset=0, after=_payload("c1", "ACTIVE")),
            _event(op="u", campaign_id="c2", lsn=200, offset=1, after=_payload("c2", "PAUSED")),
            _event(op="u", campaign_id="c1", lsn=300, offset=2, after=_payload("c1", "PAUSED")),
        ]
        rows = {row["campaign_id"]: row for row in _collapse(spark, events).collect()}

        assert set(rows) == {"c1", "c2"}
        assert rows["c1"][schemas.SOURCE_LSN] == 300
        assert rows["c2"][schemas.SOURCE_LSN] == 200

    def test_twenty_changes_to_one_row_produce_one_row(self, spark):
        events = [
            _event(
                op="u",
                campaign_id="c1",
                lsn=100 + index,
                offset=index,
                after=_payload("c1", f"STATE_{index}"),
            )
            for index in range(20)
        ]
        rows = _collapse(spark, events).collect()

        assert len(rows) == 1
        assert rows[0]["campaign_status"] == "STATE_19"


# ---------------------------------------------------------------------------
# deletes
# ---------------------------------------------------------------------------


class TestDeletes:
    def test_a_delete_keeps_the_row_and_marks_it(self, spark):
        events = [
            _event(op="c", campaign_id="c1", lsn=100, offset=0, after=_payload("c1", "ACTIVE")),
            _event(op="d", campaign_id="c1", lsn=200, offset=1, before=_payload("c1", "PAUSED")),
        ]
        rows = _collapse(spark, events).collect()

        assert len(rows) == 1
        assert rows[0][schemas.IS_DELETED] is True
        assert rows[0][schemas.DELETED_AT] is not None

    def test_a_deleted_row_keeps_its_final_values(self, spark):
        """Only possible because the source tables are REPLICA IDENTITY FULL."""
        events = [
            _event(op="d", campaign_id="c1", lsn=200, offset=1, before=_payload("c1", "PAUSED")),
        ]
        rows = _collapse(spark, events).collect()

        assert rows[0]["campaign_name"] == "Spring Launch"
        assert rows[0]["campaign_budget"] == Decimal("1000.0000")

    def test_a_reinsert_after_a_delete_is_live_again(self, spark):
        events = [
            _event(op="d", campaign_id="c1", lsn=100, offset=0, before=_payload("c1", "PAUSED")),
            _event(op="c", campaign_id="c1", lsn=200, offset=1, after=_payload("c1", "ACTIVE")),
        ]
        rows = _collapse(spark, events).collect()

        assert rows[0][schemas.IS_DELETED] is False
        assert rows[0][schemas.DELETED_AT] is None

    def test_a_tombstone_does_not_overwrite_its_delete(self, spark):
        """The tombstone has no payload; losing to the delete is what keeps the row."""
        events = [
            _event(op="d", campaign_id="c1", lsn=200, offset=1, before=_payload("c1", "PAUSED")),
            _event(op=OP_TOMBSTONE, campaign_id="c1", lsn=None, offset=2),
        ]
        rows = _collapse(spark, events).collect()

        assert len(rows) == 1
        assert rows[0][schemas.IS_DELETED] is True
        assert rows[0]["campaign_name"] == "Spring Launch"

    def test_a_lone_tombstone_produces_nothing(self, spark):
        events = [_event(op=OP_TOMBSTONE, campaign_id="c1", lsn=None, offset=0)]

        assert _collapse(spark, events).count() == 0


# ---------------------------------------------------------------------------
# the snapshot boundary
# ---------------------------------------------------------------------------


class TestSnapshotBoundary:
    def test_events_already_in_the_snapshot_are_dropped(self, spark):
        events = [
            _event(op="u", campaign_id="c1", lsn=100, offset=0, after=_payload("c1", "OLD")),
            _event(op="u", campaign_id="c2", lsn=500, offset=1, after=_payload("c2", "NEW")),
        ]
        rows = _collapse(spark, events, min_lsn=200).collect()

        assert [row["campaign_id"] for row in rows] == ["c2"]

    def test_a_delete_below_the_boundary_does_not_remove_a_reinserted_row(self, spark):
        """The case where replaying a known-duplicate event is not harmless."""
        events = [_event(op="d", campaign_id="c1", lsn=100, offset=0, before=_payload("c1", "X"))]

        assert _collapse(spark, events, min_lsn=200).count() == 0


# ---------------------------------------------------------------------------
# merging with the snapshot
# ---------------------------------------------------------------------------


class TestApplyChanges:
    def _snapshot(self, spark, ids: list[str]):
        rows = [
            (
                campaign_id,
                "Spring Launch",
                "ACTIVE",
                Decimal("1000.0000"),
                3,
                date(2026, 1, 15),
                datetime(2026, 1, 15),
            )
            for campaign_id in ids
        ]
        return reconcile.snapshot_as_silver(spark.createDataFrame(rows, schema=TARGET), TARGET)

    def test_untouched_snapshot_rows_survive(self, spark):
        snapshot = self._snapshot(spark, ["c1", "c2"])
        changes = _collapse(
            spark,
            [_event(op="u", campaign_id="c1", lsn=100, offset=0, after=_payload("c1", "PAUSED"))],
        )
        rows = {
            row["campaign_id"]: row
            for row in reconcile.apply_changes(
                snapshot, changes, primary_key="campaign_id"
            ).collect()
        }

        assert set(rows) == {"c1", "c2"}
        assert rows["c1"]["campaign_status"] == "PAUSED"
        assert rows["c2"]["campaign_status"] == "ACTIVE"

    def test_a_row_inserted_after_the_snapshot_appears(self, spark):
        snapshot = self._snapshot(spark, ["c1"])
        changes = _collapse(
            spark,
            [_event(op="c", campaign_id="c9", lsn=100, offset=0, after=_payload("c9", "ACTIVE"))],
        )
        result = reconcile.apply_changes(snapshot, changes, primary_key="campaign_id")

        assert {row["campaign_id"] for row in result.collect()} == {"c1", "c9"}

    def test_the_output_has_one_row_per_key(self, spark):
        snapshot = self._snapshot(spark, ["c1", "c2", "c3"])
        changes = _collapse(
            spark,
            [
                _event(op="u", campaign_id="c1", lsn=100, offset=0, after=_payload("c1", "A")),
                _event(op="u", campaign_id="c1", lsn=200, offset=1, after=_payload("c1", "B")),
                _event(op="d", campaign_id="c2", lsn=300, offset=2, before=_payload("c2", "C")),
            ],
        )
        result = reconcile.apply_changes(snapshot, changes, primary_key="campaign_id")
        ids = [row["campaign_id"] for row in result.collect()]

        assert sorted(ids) == ["c1", "c2", "c3"]
        assert len(ids) == len(set(ids))

    def test_running_it_twice_gives_the_same_answer(self, spark):
        snapshot = self._snapshot(spark, ["c1", "c2"])
        events = [
            _event(op="u", campaign_id="c1", lsn=100, offset=0, after=_payload("c1", "A")),
            _event(op="d", campaign_id="c2", lsn=200, offset=1, before=_payload("c2", "B")),
        ]

        def run():
            changes = _collapse(spark, events)
            result = reconcile.apply_changes(snapshot, changes, primary_key="campaign_id")
            return sorted(result.collect(), key=lambda row: row["campaign_id"])

        assert run() == run()


# ---------------------------------------------------------------------------
# the wire format
# ---------------------------------------------------------------------------


class TestTypes:
    def test_decimals_survive_the_round_trip_exactly(self, spark):
        events = [
            _event(
                op="u",
                campaign_id="c1",
                lsn=100,
                offset=0,
                after=_payload("c1", "ACTIVE", budget="12345.6789"),
            )
        ]
        rows = _collapse(spark, events).collect()

        assert rows[0]["campaign_budget"] == Decimal("12345.6789")

    def test_dates_and_timestamps_decode_from_integers(self, spark):
        events = [
            _event(op="u", campaign_id="c1", lsn=100, offset=0, after=_payload("c1", "ACTIVE"))
        ]
        rows = _collapse(spark, events).collect()

        assert rows[0]["start_date"] == date(2026, 1, 15)
        assert rows[0]["updated_at"] == datetime(2026, 1, 15)

    def test_the_output_matches_the_declared_silver_schema(self, spark):
        events = [
            _event(op="u", campaign_id="c1", lsn=100, offset=0, after=_payload("c1", "ACTIVE"))
        ]
        produced = _collapse(spark, events).schema
        declared = schemas.silver_schema(TARGET)

        assert [field.name for field in produced] == [field.name for field in declared]

    def test_the_wire_schema_maps_every_type(self):
        wire = {field.name: field.dataType for field in schemas.wire_schema(TARGET)}

        assert isinstance(wire["campaign_budget"], StringType)
        assert isinstance(wire["start_date"], IntegerType)
        assert isinstance(wire["campaign_name"], StringType)
