"""Getting the Silver export into Snowflake RAW.

Three hops, because there is no shorter route from a laptop's MinIO to a cloud
warehouse: download the exported Parquet from the object store, ``PUT`` it to a
Snowflake internal stage, then ``COPY INTO`` a table. An external stage would
remove the middle hop, but it needs a bucket Snowflake can reach, which means
provisioning real cloud storage - a large amount of setup to avoid a copy of a
few gigabytes.

**Types are declared, not inferred.** Snowflake can infer a table from Parquet
with ``INFER_SCHEMA``, and it is one statement instead of the mapping below.
It also reads Parquet's decimals as ``NUMBER`` with whatever precision the file
happens to carry, and this dataset's money columns are decimals for a reason.
Declaring the DDL means ``daily_budget`` is ``NUMBER(14,2)`` because that is
what it is, rather than whatever survived two format conversions.

**RAW is replaced, not merged.** Incremental loading belongs above this layer,
where dbt can see change metadata and decide what it means. A landing schema
that is merged into is a landing schema with history in it, which is a quietly
different thing from a copy of the source.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bronze.storage import BronzeStore
from data_generator.logging_setup import get_logger
from data_generator.models import TABLE_NAMES
from spark import export as silver_export
from warehouse.settings import (
    RAW_SCHEMA,
    SnowflakeSettings,
    WarehouseNotConfigured,
    connect,
)

logger = get_logger(__name__)

STAGE = "SILVER_STAGE"

#: Arrow's type names to Snowflake's. Deliberately small: an unmapped type is an
#: error rather than a guess, because the guess that gets made silently is the
#: one that turns money into a float.
TYPE_MAP: dict[str, str] = {
    "string": "VARCHAR",
    "large_string": "VARCHAR",
    "bool": "BOOLEAN",
    "int8": "NUMBER(3,0)",
    "int16": "NUMBER(5,0)",
    "int32": "NUMBER(10,0)",
    "int64": "NUMBER(19,0)",
    "float": "FLOAT",
    "double": "FLOAT",
    "date32[day]": "DATE",
    "date64[ms]": "DATE",
}


class LoadError(RuntimeError):
    """Raised when the export is missing, or a COPY did not land what it should."""


@dataclass(frozen=True, slots=True)
class LoadedTable:
    table: str
    expected_rows: int
    loaded_rows: int
    files: int

    @property
    def matched(self) -> bool:
        return self.expected_rows == self.loaded_rows


def snowflake_type(arrow_type: Any) -> str:
    """One Arrow field type as Snowflake DDL."""
    import pyarrow as pa

    if pa.types.is_decimal(arrow_type):
        # Carried through exactly. This is the whole reason for not inferring.
        return f"NUMBER({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_timestamp(arrow_type):
        # Silver's timestamps are UTC instants, so NTZ would quietly drop the
        # fact that they are comparable across time zones.
        return "TIMESTAMP_TZ" if arrow_type.tz else "TIMESTAMP_NTZ"

    name = str(arrow_type)
    if name not in TYPE_MAP:
        raise LoadError(
            f"no Snowflake type mapped for Arrow type {name!r}. "
            "Add it to warehouse.load.TYPE_MAP rather than letting it be inferred."
        )
    return TYPE_MAP[name]


def create_table_sql(table: str, schema: Any, *, database: str) -> str:
    """DDL for one RAW table, from the exported Parquet's own schema.

    Taken from the file rather than from ``spark.schemas`` so the warehouse
    cannot disagree with what was actually written. If a column is added in
    PostgreSQL and flows through to Silver, it appears here with no edit.
    """
    columns = ",\n    ".join(
        f'"{field.name.upper()}" {snowflake_type(field.type)}' for field in schema
    )
    return f"CREATE OR REPLACE TABLE {database}.{RAW_SCHEMA}.{table.upper()} (\n    {columns}\n)"


def read_manifest(store: BronzeStore) -> dict[str, int]:
    """Expected row counts per table, written by the export."""
    try:
        payload = json.loads(store.read_bytes(silver_export.MANIFEST_KEY))
    # Missing, unreadable or malformed all mean the same thing to the caller:
    # there is no export to load, and the fix is the same either way.
    except Exception as exc:
        raise LoadError(
            "no Silver export found. Run `make warehouse-export` first.\n"
            f"(looked for s3://{store.settings.bucket}/{silver_export.MANIFEST_KEY}: {exc})"
        ) from exc
    return {entry["table"]: entry["rows"] for entry in payload["tables"]}


def download(store: BronzeStore, table: str, destination: Path) -> list[Path]:
    """Fetch one table's exported Parquet to local disk for PUT."""
    destination.mkdir(parents=True, exist_ok=True)
    paths = []
    for key in store.list_keys(f"{silver_export.table_prefix(table)}/"):
        if not key.endswith(".parquet"):
            continue
        path = destination / Path(key).name
        path.write_bytes(store.read_bytes(key))
        paths.append(path)
    if not paths:
        raise LoadError(f"export for {table} has no Parquet files")
    return paths


#: Any instant outside this window means the load misread the encoding rather
#: than that the data is unusual. The dataset spans roughly two years around
#: now; these bounds are wide enough to never fire on real rows.
PLAUSIBLE_YEARS = (2000, 2100)

#: Rows sampled when checking that temporal columns decoded sensibly. This
#: failure mode is systematic - a file format option applies to every row or
#: none - so a sample catches it, and a full scan of 100M rows would not catch
#: anything more while costing credits to find out.
SANITY_SAMPLE_ROWS = 1000


def temporal_columns(cursor: Any, *, table: str, database: str) -> list[str]:
    cursor.execute(
        f"SELECT column_name FROM {database}.INFORMATION_SCHEMA.COLUMNS "
        "WHERE table_schema = %s AND table_name = %s "
        "AND data_type IN ('DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'TIMESTAMP_LTZ') "
        "ORDER BY ordinal_position",
        (RAW_SCHEMA, table.upper()),
    )
    return [row[0] for row in cursor.fetchall()]


def implausible_dates(cursor: Any, *, table: str, database: str) -> list[str]:
    """Temporal columns whose values did not survive the format conversion.

    The row count matching proves every row arrived, not that any of them mean
    what they should. Snowflake ignores Parquet's timestamp annotations unless
    told otherwise, and the resulting dates are wrong by a factor of a million
    without anything failing - so this asks the one question the count cannot.
    """
    columns = temporal_columns(cursor, table=table, database=database)
    if not columns:
        return []

    low, high = PLAUSIBLE_YEARS
    checks = ", ".join(
        f"count_if(year({column}) NOT BETWEEN {low} AND {high}) AS bad_{index}"
        for index, column in enumerate(columns)
    )
    cursor.execute(
        f"SELECT {checks} FROM ("
        f"  SELECT * FROM {database}.{RAW_SCHEMA}.{table.upper()} LIMIT {SANITY_SAMPLE_ROWS}"
        f")"
    )
    counts = cursor.fetchone()
    return [column for column, bad in zip(columns, counts, strict=True) if bad]


def load_table(
    connection: Any,
    store: BronzeStore,
    *,
    table: str,
    expected_rows: int,
    database: str,
    workspace: Path,
) -> LoadedTable:
    """Download, stage and copy one table, then check the row count survived."""
    import pyarrow.parquet as pq

    local = download(store, table, workspace / table)
    schema = pq.read_schema(local[0])
    stage_path = f"@{database}.{RAW_SCHEMA}.{STAGE}/{table}"

    with connection.cursor() as cursor:
        cursor.execute(create_table_sql(table, schema, database=database))
        # Clearing the stage path first: PUT skips files already there, so a
        # second load after an export with fewer files would copy the leftovers
        # of the first one too.
        cursor.execute(f"REMOVE {stage_path}")

        for path in local:
            # AUTO_COMPRESS off - the Parquet is already Snappy compressed, and
            # gzipping it again costs time to produce a slightly larger file.
            cursor.execute(
                f"PUT 'file://{path}' {stage_path} AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
            )

        cursor.execute(
            f"COPY INTO {database}.{RAW_SCHEMA}.{table.upper()} "
            f"FROM {stage_path} "
            # USE_LOGICAL_TYPE is not optional here, despite defaulting to
            # FALSE. Without it Snowflake ignores Parquet's TIMESTAMP(MICROS)
            # annotation and reads the underlying int64 as epoch *seconds*, so
            # 2026 loads as the year 54,934,202. Dates degrade the same way.
            # Nothing fails - COPY succeeds, the row count matches, and the
            # damage only appears when someone selects the column.
            "FILE_FORMAT = (TYPE = PARQUET, USE_LOGICAL_TYPE = TRUE) "
            "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
            "ON_ERROR = ABORT_STATEMENT"
        )
        cursor.execute(f"SELECT count(*) FROM {database}.{RAW_SCHEMA}.{table.upper()}")
        loaded = int(cursor.fetchone()[0])

        broken = implausible_dates(cursor, table=table, database=database)
        if broken:
            raise LoadError(
                f"{table}: {', '.join(broken)} loaded outside "
                f"{PLAUSIBLE_YEARS[0]}-{PLAUSIBLE_YEARS[1]}.\n"
                "The COPY read the timestamp encoding wrong - check that "
                "FILE_FORMAT still sets USE_LOGICAL_TYPE = TRUE."
            )

    logger.info("loaded", extra={"table": table, "rows": loaded, "expected": expected_rows})
    return LoadedTable(
        table=table, expected_rows=expected_rows, loaded_rows=loaded, files=len(local)
    )


def run(
    *,
    tables: list[str] | None = None,
    settings: SnowflakeSettings | None = None,
    store: BronzeStore | None = None,
) -> list[LoadedTable]:
    resolved = (settings or SnowflakeSettings.from_env()).require()
    bronze = store or BronzeStore()
    manifest = read_manifest(bronze)

    wanted = tables or list(manifest)
    missing = [table for table in wanted if table not in manifest]
    if missing:
        raise LoadError(
            f"not in the export: {', '.join(missing)}. "
            "Re-run `make warehouse-export` with the tables you need."
        )

    connection = connect(resolved, schema=RAW_SCHEMA)
    results = []
    try:
        with tempfile.TemporaryDirectory(prefix="adtech-load-") as workspace:
            for table in wanted:
                results.append(
                    load_table(
                        connection,
                        bronze,
                        table=table,
                        expected_rows=manifest[table],
                        database=resolved.database,
                        workspace=Path(workspace),
                    )
                )
    finally:
        connection.close()

    return results


def render(results: list[LoadedTable], settings: SnowflakeSettings) -> str:
    width = 66
    lines = [
        "",
        " SILVER -> SNOWFLAKE RAW",
        "=" * width,
        f" {settings.describe()}",
        "",
        f" {'table':<22}{'exported':>14}{'loaded':>14}{'files':>8}",
        " " + "-" * (width - 2),
    ]
    for entry in results:
        flag = "" if entry.matched else "  MISMATCH"
        lines.append(
            f" {entry.table:<22}{entry.expected_rows:>14,}{entry.loaded_rows:>14,}"
            f"{entry.files:>8,}{flag}"
        )

    mismatched = [entry for entry in results if not entry.matched]
    lines += [
        " " + "-" * (width - 2),
        f" {'total':<22}{sum(e.expected_rows for e in results):>14,}"
        f"{sum(e.loaded_rows for e in results):>14,}",
        "",
    ]
    if mismatched:
        lines.append(f" {len(mismatched)} table(s) did not land the row count the export recorded.")
    else:
        lines.append(" Every table matched the export row for row.")
        lines.append(" Next: make dbt-run")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load the Silver export into Snowflake RAW.")
    parser.add_argument(
        "--table",
        action="append",
        choices=sorted(TABLE_NAMES),
        help="limit to one table; repeatable (default: everything in the export)",
    )
    args = parser.parse_args(argv)

    try:
        results = run(tables=args.table)
    except (WarehouseNotConfigured, LoadError) as exc:
        print(f"\n{exc}\n", file=sys.stderr)
        return 2

    print(render(results, SnowflakeSettings.from_env()))
    return 0 if all(entry.matched for entry in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
