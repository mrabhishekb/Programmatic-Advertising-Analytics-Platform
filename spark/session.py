"""The SparkSession, wired to the object store.

Every setting here is either "talk to MinIO instead of AWS" or "do not corrupt
timestamps". Nothing tunes performance; that belongs with the job that knows its
own data volume.
"""

from __future__ import annotations

from typing import Any

from bronze.storage import S3Settings
from data_generator.logging_setup import get_logger

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
    shuffle_partitions: int | None = None,
    extra: dict[str, str] | None = None,
) -> Any:
    from pyspark.sql import SparkSession

    settings = settings or S3Settings.from_env()
    builder = SparkSession.builder.appName(app_name)

    for key, value in s3a_config(settings).items():
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
            "parallelism": session.sparkContext.defaultParallelism,
        },
    )
    return session
