"""Turning Bronze's opaque JSON payloads back into typed columns.

Bronze stores ``before`` and ``after`` as JSON strings so that a column added in
PostgreSQL never invalidates a file written yesterday. The cost is deferred, not
avoided: something has to decide what those strings mean, and this is it.

Two schemas are involved and they are not the same shape.

**The target** is whatever the snapshot export wrote, read back off the Parquet
itself rather than from PostgreSQL. That keeps Silver runnable with the source
database switched off, and it is also the more correct authority - the snapshot
is what Silver actually has to merge against, so if the two ever disagreed, the
snapshot's types are the ones that matter.

**The wire schema** is how Debezium encodes those same columns in JSON, which
depends on two settings in ``debezium/connector.json``:

* ``decimal.handling.mode: string`` - NUMERIC arrives as ``"1234.5600"``, not as
  a float and not as base64 bytes. Money keeps its exact value, at the cost of a
  cast on this side.
* ``time.precision.mode: connect`` - DATE is an integer count of days since the
  epoch and TIMESTAMP an integer count of milliseconds. With
  ``schemas.enable: false`` there is nothing in the message saying so, which is
  precisely why this mapping has to be written down somewhere.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

#: Columns Silver adds that the source has no equivalent for. Prefixed so they
#: cannot collide with a real column, now or after a schema change upstream.
IS_DELETED = "is_deleted"
DELETED_AT = "deleted_at"
SOURCE_LSN = "_lsn"
SOURCE_OP = "_op"
SOURCE_EVENT_TS = "_event_ts"

LINEAGE_COLUMNS = (IS_DELETED, DELETED_AT, SOURCE_LSN, SOURCE_OP, SOURCE_EVENT_TS)


def wire_type(target: DataType) -> DataType:
    """How Debezium serialises a column of this type into JSON."""
    if isinstance(target, DecimalType):
        return StringType()
    if isinstance(target, DateType):
        return IntegerType()
    if isinstance(target, TimestampType | TimestampNTZType):
        return LongType()
    return target


def wire_schema(target: StructType) -> StructType:
    """The schema ``from_json`` needs to parse a Bronze payload."""
    return StructType(
        [StructField(field.name, wire_type(field.dataType), True) for field in target.fields]
    )


def cast_from_wire(column: Column, target: DataType) -> Column:
    """Convert one parsed JSON value back to the type the snapshot holds."""
    if isinstance(target, DecimalType):
        # String to decimal, never through a float: the whole reason the
        # connector is on `string` mode is to avoid that round trip.
        return column.cast(target)
    if isinstance(target, DateType):
        return F.date_from_unix_date(column)
    if isinstance(target, TimestampType | TimestampNTZType):
        # Exact only because the session timezone is pinned to UTC in
        # spark.session; see the comment there.
        return F.timestamp_millis(column).cast(target)
    return column.cast(target)


def payload_columns(payload: str, target: StructType) -> list[Column]:
    """Project a parsed payload struct into the snapshot's columns and types."""
    return [
        cast_from_wire(F.col(f"{payload}.{field.name}"), field.dataType).alias(field.name)
        for field in target.fields
    ]


def silver_schema(target: StructType) -> StructType:
    """The snapshot's columns plus the ones reconciliation adds."""
    return StructType(
        [
            *target.fields,
            StructField(IS_DELETED, BooleanType(), False),
            StructField(DELETED_AT, TimestampType(), True),
            StructField(SOURCE_LSN, LongType(), True),
            StructField(SOURCE_OP, StringType(), True),
            StructField(SOURCE_EVENT_TS, TimestampType(), True),
        ]
    )


def source_schema(silver: StructType) -> StructType:
    """The inverse of :func:`silver_schema`: the source columns, lineage removed.

    Lets an incremental run take its target schema from the Silver table it is
    about to merge into, instead of opening the snapshot Parquet to rediscover
    a shape the table already knows. On ``impressions`` that is the difference
    between touching 6GB of files and touching none.
    """
    return StructType([f for f in silver.fields if f.name not in LINEAGE_COLUMNS])


def read_snapshot_schema(spark: Any, url: str) -> StructType:
    """The target schema, taken from the snapshot Parquet rather than PostgreSQL.

    Reads the footer only - Spark infers a Parquet schema without scanning rows,
    so this costs the same on a 6GB table as on an empty one.
    """
    return spark.read.parquet(url).schema
