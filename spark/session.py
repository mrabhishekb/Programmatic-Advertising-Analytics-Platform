"""The SparkSession, wired to the object store.

Every setting here is either "talk to MinIO instead of AWS" or "do not corrupt
timestamps". Nothing tunes performance; that belongs with the job that knows its
own data volume.
"""

from __future__ import annotations

from typing import Any

from bronze.storage import S3Settings
from data_generator.db import DatabaseSettings
from data_generator.logging_setup import get_logger
from spark import catalog

logger = get_logger(__name__)

DEFAULT_APP_NAME = "adtech-silver"


def s3a_config(settings: S3Settings) -> dict[str, str]:
    """Hadoop's S3A settings, derived from the same env the Bronze writer reads.

    One source of truth on purpose: a Silver job pointed at a different bucket
    than the sink writes would produce an empty, entirely plausible-looking
    result.
    """
    endpoint = settings.endpoint_url or ""
    config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": settings.access_key,
        "spark.hadoop.fs.s3a.secret.key": settings.secret_key,
        "spark.hadoop.fs.s3a.endpoint.region": settings.region,
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        # MinIO serves path-style only. On the default virtual-host style the
        # bucket becomes a subdomain, which does not resolve against a container
        # and surfaces as a DNS error rather than anything naming the real cause.
        "spark.hadoop.fs.s3a.path.style.access": "true",
    }
    if endpoint:
        config["spark.hadoop.fs.s3a.endpoint"] = endpoint
        config["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(
            endpoint.startswith("https")
        ).lower()
    return config


def build_session(
    app_name: str = DEFAULT_APP_NAME,
    *,
    settings: S3Settings | None = None,
    database: DatabaseSettings | None = None,
    shuffle_partitions: int | None = None,
    with_catalog: bool = True,
    extra: dict[str, str] | None = None,
) -> Any:
    from pyspark.sql import SparkSession

    settings = settings or S3Settings.from_env()
    builder = SparkSession.builder.appName(app_name)

    for key, value in s3a_config(settings).items():
        builder = builder.config(key, value)

    if with_catalog:
        # Before the session, not after: Iceberg reads its catalog settings when
        # the catalog is first resolved, and `spark.sql.extensions` is only
        # consulted while the session is being built. Setting either on a live
        # session is accepted silently and does nothing.
        catalog.ensure_catalog_database(database)
        for key, value in catalog.catalog_config(
            settings, catalog.catalog_settings(database)
        ).items():
            builder = builder.config(key, value)

    # The source columns are `timestamp without time zone` on a single
    # simulation clock, and Debezium encodes them as milliseconds since epoch
    # with no zone attached. Pinning the session to UTC makes the integer round
    # trip back to the same wall-clock value it left PostgreSQL with; on any
    # other session zone the snapshot and the CDC path would disagree by the
    # local offset, which is both wrong and very hard to spot.
    builder = builder.config("spark.sql.session.timeZone", "UTC")

    # Rename is a copy on object storage, so the v1 committer's "write to a
    # temp directory, then rename the whole thing" costs a second full write of
    # every output file. v2 commits each task directly. The usual objection is
    # that v2 leaves partial output if a job dies mid-commit - which is fine
    # here, because Silver is derived state that is rebuilt by rerunning.
    #
    # Since phase 6 this no longer governs Silver: Iceberg does not use Hadoop's
    # output committer at all, it writes data files and then commits them by
    # swapping a metadata pointer. Kept because it still applies to any plain
    # Parquet this job writes, and because "v2 is unsafe" is the usual reflex -
    # under Iceberg the question does not arise.
    builder = builder.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

    if shuffle_partitions:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    for key, value in (extra or {}).items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    logger.info(
        "spark session ready",
        extra={
            "app": app_name,
            "bronze": settings.describe(),
            "catalog": catalog.CATALOG if with_catalog else None,
            "parallelism": session.sparkContext.defaultParallelism,
        },
    )
    return session
