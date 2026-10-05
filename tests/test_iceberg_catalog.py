"""How Silver is addressed and laid out, without starting a JVM.

The things checked here are the ones that fail quietly rather than loudly. A
catalog pointed at the source database still works - until you notice every
Iceberg commit flowing through the replication slot. A partition spec on the
wrong column still queries correctly - it just never prunes. Neither shows up
as an error, so neither can be left to an end-to-end run to catch.
"""

from __future__ import annotations

import pytest
import yaml

from bronze.storage import S3Settings
from data_generator.config import PROJECT_ROOT
from data_generator.db import DatabaseSettings
from data_generator.models import EVENT_TABLES, MASTER_TABLES
from spark import catalog, layout

SOURCE = DatabaseSettings(host="postgres", port=5432, database="adtech", user="u", password="p")
BUCKET = S3Settings(bucket="adtech-bronze")


@pytest.fixture(scope="module")
def spark_service() -> dict:
    compose = yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    return compose["services"]["spark"]


class TestNaming:
    def test_a_table_is_addressed_by_catalog_namespace_and_name(self):
        assert catalog.table_identifier("campaigns") == "lake.silver.campaigns"

    def test_the_warehouse_is_a_sibling_of_bronze_not_a_child_of_it(self):
        """Pointing a warehouse at snapshot/ or cdc/ would put Iceberg metadata
        among files it does not own."""
        assert catalog.WAREHOUSE_PREFIX not in {"snapshot", "cdc"}
        assert catalog.warehouse_url("adtech-bronze") == "s3a://adtech-bronze/warehouse"

    def test_it_does_not_reuse_the_phase_5_prefix(self):
        """That prefix still holds flat Parquet. A warehouse rooted on it would
        invite a reader to treat those files as part of a table."""
        assert catalog.WAREHOUSE_PREFIX != layout.LEGACY_SILVER_PREFIX

    def test_table_files_live_under_a_bare_namespace_directory(self):
        """Not <namespace>.db: that suffix is Hive's convention, which
        HiveCatalog reproduces and JdbcCatalog does not. Getting it wrong does
        not break a write - Iceberg records the real location and carries on -
        it breaks whatever reads the prefix, which then reports an empty layer
        that is not empty."""
        assert catalog.table_prefix("campaigns") == "warehouse/silver/campaigns"

    def test_run_manifests_sit_outside_every_table(self):
        """Iceberg owns everything below the namespace prefix. A manifest of
        ours among its metadata is at best confusing and at worst picked up."""
        assert not layout.SILVER_RUNS_PREFIX.startswith(catalog.namespace_prefix())


class TestCatalogBacking:
    def test_the_catalog_has_its_own_database(self):
        """Logical decoding is per-database, so this is what keeps Iceberg
        commits out of the WAL stream Debezium reads."""
        assert catalog.catalog_settings(SOURCE).database != SOURCE.database

    def test_it_keeps_the_servers_connection_details(self):
        resolved = catalog.catalog_settings(SOURCE)

        assert (resolved.host, resolved.port, resolved.user) == (SOURCE.host, SOURCE.port, "u")

    def test_the_jdbc_url_names_the_catalog_database_not_the_source(self):
        url = catalog.jdbc_url(catalog.catalog_settings(SOURCE))

        assert url == "jdbc:postgresql://postgres:5432/iceberg"

    def test_the_config_registers_a_jdbc_catalog(self):
        config = catalog.catalog_config(BUCKET, catalog.catalog_settings(SOURCE))

        assert config["spark.sql.catalog.lake"] == "org.apache.iceberg.spark.SparkCatalog"
        assert config["spark.sql.catalog.lake.type"] == "jdbc"

    def test_the_sql_extensions_are_registered(self):
        """Without them MERGE INTO and the time-travel syntax are parse errors,
        and phase 7 needs the first of those."""
        config = catalog.catalog_config(BUCKET, catalog.catalog_settings(SOURCE))

        assert "IcebergSparkSessionExtensions" in config["spark.sql.extensions"]

    def test_iceberg_reads_through_the_same_s3a_connector_as_everything_else(self):
        """S3FileIO would be a second AWS client with its own copy of the
        endpoint and credentials - two places for a bucket name to be wrong."""
        config = catalog.catalog_config(BUCKET, catalog.catalog_settings(SOURCE))

        assert config["spark.sql.catalog.lake.io-impl"] == "org.apache.iceberg.hadoop.HadoopFileIO"

    def test_it_does_not_become_the_default_catalog(self):
        """The default stays Spark's own so spark.read.parquet still reads raw
        Bronze files; Silver is always named in full."""
        config = catalog.catalog_config(BUCKET, catalog.catalog_settings(SOURCE))

        assert "spark.sql.defaultCatalog" not in config


class TestPartitioning:
    def test_every_event_table_is_partitioned(self):
        assert set(catalog.EVENT_PARTITION_COLUMN) == set(EVENT_TABLES)

    def test_no_dimension_table_is(self):
        """At 5k-200k rows, partitioning would produce many small files - slower
        to plan and slower to read than leaving them whole."""
        assert not {table for table in MASTER_TABLES if catalog.partition_column(table)}

    def test_it_partitions_on_when_the_event_happened_not_when_it_was_recorded(self):
        """created_at is insertion order. Queries filter on the event time, and
        a partition the planner cannot match is a partition that never prunes."""
        assert catalog.partition_column("impressions") == "impression_timestamp"
        assert catalog.partition_column("clicks") == "click_timestamp"
        assert "created_at" not in set(catalog.EVENT_PARTITION_COLUMN.values())

    def test_the_partition_column_exists_on_the_model(self):
        from data_generator.models import MODEL_BY_TABLE

        for table, column in catalog.EVENT_PARTITION_COLUMN.items():
            assert column in MODEL_BY_TABLE[table].__annotations__, f"{table}.{column}"


class TestTheSparkServiceCanReachItsCatalog:
    """Since phase 6 Spark needs PostgreSQL to read Silver at all - not for the
    source tables, which it never touches, but for the table pointers."""

    def test_it_waits_for_a_healthy_database(self, spark_service):
        assert spark_service["depends_on"]["postgres"]["condition"] == "service_healthy"

    def test_it_reaches_postgres_by_service_name(self, spark_service):
        """localhost inside the container is the container, not the database."""
        assert spark_service["environment"]["POSTGRES_HOST"] == "postgres"

    def test_it_uses_the_container_port_not_the_published_one(self, spark_service):
        """The host mapping can be moved to avoid a clash; inside the compose
        network the database is still on 5432."""
        assert str(spark_service["environment"]["POSTGRES_PORT"]) == "5432"

    def test_the_image_carries_the_jars_the_catalog_needs(self):
        """Baked in, not fetched with --packages: a job that downloads its own
        dependencies fails when Maven Central is slow, which is exactly when a
        batch job is least able to explain itself."""
        dockerfile = (PROJECT_ROOT / "docker/spark.Dockerfile").read_text(encoding="utf-8")

        assert "iceberg-spark-runtime-3.5_2.12" in dockerfile
        assert "org/postgresql/postgresql" in dockerfile

    def test_the_iceberg_runtime_matches_the_spark_it_is_built_for(self):
        """The artifact is per Spark minor and per Scala version. A mismatched
        one resolves fine and then fails at class-load time."""
        dockerfile = (PROJECT_ROOT / "docker/spark.Dockerfile").read_text(encoding="utf-8")
        spark_version = next(
            line.split("=")[1] for line in dockerfile.splitlines() if "ARG SPARK_VERSION" in line
        )

        assert f"iceberg-spark-runtime-{spark_version.rsplit('.', 1)[0]}_2.12" in dockerfile
