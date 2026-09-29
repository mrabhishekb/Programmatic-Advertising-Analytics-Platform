"""The bulk export, landing in Bronze as typed Parquet.

Phase 2 wrote gzipped CSV to local disk, which was fine as a placeholder and
wrong as a destination: CSV has no types, no column pruning and no predicate
pushdown, so every downstream reader would re-parse 100 million rows of text to
answer a question about three columns.

Unlike the CDC side, snapshot rows are stored **typed** rather than as opaque
JSON. The two paths carry genuinely different things - a change envelope is a
before/after pair, a snapshot row is just a row - and the snapshot is read as a
full table scan, which is exactly what columnar Parquet is for. Silver parses
the CDC envelopes into this same shape and merges the two on primary key.

The Arrow schema is read from ``information_schema`` rather than guessed, so a
NUMERIC(12,4) in PostgreSQL becomes a decimal128(12,4) in Parquet and money
keeps its exact value instead of drifting through a float.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from bronze import layout
from bronze.storage import BronzeStore
from data_generator.logging_setup import get_logger

logger = get_logger(__name__)

#: Rows held in memory at once. Bounds peak memory on the 100M-row tables, and
#: sets the size of each part file.
DEFAULT_CHUNK_ROWS = 500_000


class SnapshotError(RuntimeError):
    """Raised when a table cannot be represented or exported."""


@dataclass(frozen=True, slots=True)
class TableExport:
    table: str
    rows: int
    parts: int
    bytes_written: int
    keys: tuple[str, ...]


def arrow_schema_for(connection: Any, table: str) -> pa.Schema:
    """Build the Parquet schema from the live PostgreSQL column types."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name, data_type, numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        columns = cursor.fetchall()

    if not columns:
        raise SnapshotError(f"table {table!r} has no columns in information_schema")

    fields = []
    for name, data_type, precision, scale, nullable in columns:
        fields.append(
            pa.field(name, _arrow_type(data_type, precision, scale), nullable=nullable == "YES")
        )
    return pa.schema(fields)


def _arrow_type(data_type: str, precision: int | None, scale: int | None) -> pa.DataType:
    match data_type:
        case "uuid":
            # Parquet has no UUID logical type that every reader understands, so
            # the canonical hyphenated text form travels instead.
            return pa.string()
        case "integer":
            return pa.int32()
        case "smallint":
            return pa.int16()
        case "bigint":
            return pa.int64()
        case "boolean":
            return pa.bool_()
        case "numeric":
            # Unconstrained NUMERIC has no precision in the catalogue. Parquet
            # decimals must be fixed, so fall back to something wide enough for
            # any money column here rather than silently truncating.
            return pa.decimal128(precision or 38, scale or 9)
        case "double precision":
            return pa.float64()
        case "real":
            return pa.float32()
        case "date":
            return pa.date32()
        case "timestamp without time zone" | "timestamp with time zone":
            return pa.timestamp("us")
        case _:
            return pa.string()


def rows_to_arrow(rows: list[tuple], schema: pa.Schema) -> pa.Table:
    """Columnar-ise a chunk of psycopg rows against a known schema."""
    if not rows:
        return pa.Table.from_pylist([], schema=schema)

    columns = list(zip(*rows, strict=True))
    arrays = []
    for field, column in zip(schema, columns, strict=True):
        values: Any = column
        if pa.types.is_string(field.type):
            # UUIDs, enums and anything else that arrives as a Python object.
            values = [None if value is None else str(value) for value in column]
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def export_table(
    connection: Any,
    store: BronzeStore,
    *,
    table: str,
    run_id: str,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
) -> TableExport:
    """Stream one table into Bronze as one or more Parquet parts."""
    schema = arrow_schema_for(connection, table)
    column_list = ", ".join(f'"{field.name}"' for field in schema)

    keys: list[str] = []
    total_rows = 0
    total_bytes = 0

    # A server-side cursor: without a name psycopg buffers the whole result in
    # the client, which is not an option at 100 million rows.
    with connection.cursor(name=f"bronze_export_{table}") as cursor:
        cursor.itersize = chunk_rows
        cursor.execute(f"SELECT {column_list} FROM {table}")
        while True:
            rows = cursor.fetchmany(chunk_rows)
            if not rows:
                break
            key = layout.snapshot_object_key(run_id, table, part=len(keys))
            total_bytes += store.put_table(key, rows_to_arrow(rows, schema))
            total_rows += len(rows)
            keys.append(key)
            logger.info(
                "exported part",
                extra={"table": table, "part": len(keys) - 1, "rows_so_far": total_rows},
            )

    if not keys:
        # An empty table still gets a file, so a reader can tell "no rows" from
        # "never exported" without consulting the manifest.
        key = layout.snapshot_object_key(run_id, table, part=0)
        total_bytes += store.put_table(key, rows_to_arrow([], schema))
        keys.append(key)

    return TableExport(
        table=table,
        rows=total_rows,
        parts=len(keys),
        bytes_written=total_bytes,
        keys=tuple(keys),
    )
