"""The Iceberg catalog: where table metadata lives and how commits become atomic.

A plain Parquet directory is not a table, it is a pile of files, and "which
files are the table right now" is answered by listing the directory. That is why
phase 5's writer had to overwrite in place: there was nowhere else to record the
answer. A reader that listed mid-write saw whatever had landed so far.

Iceberg moves that answer into a metadata file and moves the pointer to the
current metadata file into a catalog. A commit is then a single compare-and-swap
on that pointer, so a reader sees the table before or after, never during.

**Why JDBC, backed by PostgreSQL.** The catalog is only as atomic as whatever
stores the pointer, which rules out the filesystem catalog here: S3 and MinIO
have no atomic rename, so two concurrent commits can both believe they won. A
row in PostgreSQL updated under a transaction has exactly the semantics Iceberg
needs, and this project already runs a PostgreSQL.

**Why a separate database rather than a schema in ``adtech``.** PostgreSQL
logical decoding is scoped to one database: a replication slot on ``adtech``
never sees changes made in ``iceberg``. Putting the catalog in its own database
therefore keeps every Iceberg commit out of the WAL stream Debezium decodes.
In the same database it would work, but each commit would be decoded and then
discarded by the table filter - wasted effort on the slot, in a pipeline whose
entire subject is that WAL.

Deliberately free of PySpark imports, like ``spark.layout``: the CLI reads
these names to describe itself, and importing PySpark would make listing a
bucket require a JVM.
"""

from __future__ import annotations

import os
from dataclasses import replace
from typing import Any

from bronze.storage import S3Settings
from data_generator.db import DatabaseSettings
from data_generator.logging_setup import get_logger
from data_generator.models import EVENT_TABLES

logger = get_logger(__name__)

#: Spark's name for the catalog. Tables are addressed as
#: ``lake.silver.campaigns`` - catalog, namespace, table.
#:
#: Left out of ``spark.sql.defaultCatalog`` on purpose: the default stays
#: Spark's own, so ``spark.read.parquet`` still reads raw Bronze files. Silver
#: is always named in full, which keeps the two layers impossible to confuse.
CATALOG = "lake"

#: The namespace holding reconciled current-state tables. Gold products in
#: phase 13 become a second namespace in this same catalog.
NAMESPACE = "silver"

#: Warehouse root, a sibling of ``snapshot/`` and ``cdc/``. Iceberg lays tables
#: out beneath it as ``<namespace>/<table>/{data,metadata}/``.
#:
#: Not the phase 5 ``silver/`` prefix: that still holds flat Parquet, and
#: pointing a warehouse at a directory of foreign files invites a reader to
#: treat them as part of a table. ``make silver-drop-legacy`` removes them.
WAREHOUSE_PREFIX = "warehouse"

#: Catalog database on the existing PostgreSQL server.
DEFAULT_CATALOG_DATABASE = "iceberg"

#: Iceberg's own name for the table property holding the Bronze run a Silver
#: table was built from. Written into each snapshot's summary, so the lineage
#: travels with the commit rather than living in a side file that can drift.
BRONZE_RUN_PROPERTY = "bronze-snapshot-run"
BRONZE_LSN_PROPERTY = "bronze-snapshot-lsn"

#: Event tables are partitioned by day on the moment the event happened, not on
#: ``created_at``: queries filter on when the impression occurred, and a
#: partition the planner cannot match is a partition that does not prune.
#:
#: Dimension tables are deliberately absent. At 5k-200k rows they fit in a
#: handful of files, and partitioning them would produce many small files -
#: slower to plan and slower to read than no partitioning at all.
EVENT_PARTITION_COLUMN: dict[str, str] = {
    "impressions": "impression_timestamp",
    "clicks": "click_timestamp",
    "conversions": "conversion_timestamp",
    "spend_transactions": "spend_timestamp",
}


class CatalogError(RuntimeError):
    """Raised when the catalog database is unreachable or cannot be created."""


def table_identifier(table: str, *, catalog: str = CATALOG, namespace: str = NAMESPACE) -> str:
    """The three-part name Spark SQL uses for one Silver table."""
    return f"{catalog}.{namespace}.{table}"


def partition_column(table: str) -> str | None:
    """The column to partition by day on, or ``None`` to leave unpartitioned."""
    return EVENT_PARTITION_COLUMN.get(table)


def warehouse_url(bucket: str) -> str:
    return f"s3a://{bucket}/{WAREHOUSE_PREFIX}"


def namespace_prefix(namespace: str = NAMESPACE) -> str:
    """Where this catalog puts a namespace's tables, as an S3 key prefix.

    A bare namespace, not ``<namespace>.db``. The ``.db`` suffix is Hive's
    convention and HiveCatalog reproduces it; JdbcCatalog does not. Guessing
    wrong here does not break a write - Iceberg records the real location in the
    catalog and keeps working - it breaks anything that goes looking for the
    files directly, which then reports an empty layer that is not empty.
    """
    return f"{WAREHOUSE_PREFIX}/{namespace}"


def table_prefix(table: str, *, namespace: str = NAMESPACE) -> str:
    """Where Iceberg puts this table's files, as an S3 key prefix."""
    return f"{namespace_prefix(namespace)}/{table}"


def catalog_settings(settings: DatabaseSettings | None = None) -> DatabaseSettings:
    """Connection settings for the catalog database, not the source database."""
    base = settings or DatabaseSettings.from_env()
    return replace(base, database=os.environ.get("ICEBERG_CATALOG_DB", DEFAULT_CATALOG_DATABASE))


def jdbc_url(settings: DatabaseSettings) -> str:
    return f"jdbc:postgresql://{settings.host}:{settings.port}/{settings.database}"


def catalog_config(
    s3: S3Settings,
    database: DatabaseSettings,
    *,
    catalog: str = CATALOG,
) -> dict[str, str]:
    """Spark configuration registering the catalog.

    ``io-impl`` is Hadoop's rather than Iceberg's own S3FileIO so that Iceberg
    reads and writes through the S3A connector already configured for MinIO.
    S3FileIO would work too, but it is a second AWS client with a second copy of
    the endpoint, credentials and path-style settings - two places for a bucket
    name to be wrong instead of one.
    """
    prefix = f"spark.sql.catalog.{catalog}"
    return {
        # Supplies MERGE INTO, the time-travel syntax, and the CALL procedures
        # that expire snapshots. Without it the catalog still works, but those
        # are all parse errors.
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "jdbc",
        f"{prefix}.uri": jdbc_url(database),
        f"{prefix}.jdbc.user": database.user,
        f"{prefix}.jdbc.password": database.password,
        # V0 is the older catalog schema, which has nowhere to record a view.
        # Set on a catalog created empty by this project, so the migration it
        # performs has nothing to migrate - this is only a live decision for
        # someone adopting an existing V0 catalog.
        f"{prefix}.jdbc.schema-version": "V1",
        f"{prefix}.warehouse": warehouse_url(s3.bucket),
        f"{prefix}.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
    }


def ensure_catalog_database(settings: DatabaseSettings | None = None) -> bool:
    """Create the catalog database if it is not there yet. True if it created it.

    Iceberg creates its own tables on first use but will not create the database
    to put them in, and ``CREATE DATABASE`` cannot run inside a transaction -
    hence the autocommit connection and the explicit existence check rather than
    ``IF NOT EXISTS`` inside the usual transactional helper.

    Done in code rather than as a ``docker-entrypoint-initdb.d`` script because
    those only run when the data directory is empty. Anyone with an existing
    stack - which is everyone who has reached phase 6 - would never see it.
    """
    import psycopg

    target = catalog_settings(settings)
    admin = replace(target, database=(settings or DatabaseSettings.from_env()).database)

    try:
        with psycopg.connect(admin.conninfo, autocommit=True) as connection:
            exists = connection.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (target.database,)
            ).fetchone()
            if exists:
                return False
            # Not parameterisable: an identifier, not a value.
            connection.execute(f'CREATE DATABASE "{target.database}"')
    except psycopg.Error as exc:
        raise CatalogError(
            f"cannot reach PostgreSQL at {admin.describe()} to set up the Iceberg catalog.\n"
            f"Is the stack up?  make up\n{exc}"
        ) from exc

    logger.info("created catalog database", extra={"database": target.database})
    return True


def create_namespace(spark: Any, *, catalog: str = CATALOG, namespace: str = NAMESPACE) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def is_event_table(table: str) -> bool:
    return table in EVENT_TABLES
