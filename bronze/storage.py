"""S3 access for the Bronze layer.

MinIO stands in for S3 locally. Nothing here is MinIO-specific beyond the
endpoint and path-style addressing, both of which are settings rather than code
paths, so pointing this at real S3 means changing ``.env``.
"""

from __future__ import annotations

import io
import os
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any

from data_generator.config import PROJECT_ROOT
from data_generator.db import load_dotenv_file
from data_generator.logging_setup import get_logger

logger = get_logger(__name__)


class BronzeStorageError(RuntimeError):
    """Raised when the object store is unreachable or rejects a write."""


@dataclass(frozen=True, slots=True)
class S3Settings:
    endpoint_url: str | None = "http://localhost:9010"
    access_key: str = "adtech"
    secret_key: str = "adtechadtech"
    region: str = "us-east-1"
    bucket: str = "adtech-bronze"

    @classmethod
    def from_env(cls, env_file: Path | None = None) -> S3Settings:
        load_dotenv_file(env_file or PROJECT_ROOT / ".env")
        endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9010")
        return cls(
            # Empty means "real AWS": boto3 resolves the regional endpoint itself.
            endpoint_url=endpoint or None,
            access_key=os.environ.get("S3_ACCESS_KEY", "adtech"),
            secret_key=os.environ.get("S3_SECRET_KEY", "adtechadtech"),
            region=os.environ.get("S3_REGION", "us-east-1"),
            bucket=os.environ.get("BRONZE_BUCKET", "adtech-bronze"),
        )

    def describe(self) -> str:
        return f"s3://{self.bucket} via {self.endpoint_url or 'aws'}"


class BronzeStore:
    """Thin wrapper over the S3 API, scoped to one bucket."""

    def __init__(self, settings: S3Settings | None = None) -> None:
        self.settings = settings or S3Settings.from_env()

    @cached_property
    def client(self) -> Any:
        import boto3
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=self.settings.endpoint_url,
            aws_access_key_id=self.settings.access_key,
            aws_secret_access_key=self.settings.secret_key,
            region_name=self.settings.region,
            config=Config(
                # MinIO serves path-style only. Leaving boto3 on its virtual-host
                # default makes it address buckets as subdomains, which does not
                # resolve against a container and fails as a connection error
                # rather than anything that names the real problem.
                s3={"addressing_style": "path"},
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

    # -- reads ------------------------------------------------------------

    def ping(self) -> None:
        """Fail early, with a message that says what to do about it."""
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            self.client.head_bucket(Bucket=self.settings.bucket)
        except (ClientError, BotoCoreError) as exc:
            raise BronzeStorageError(
                f"Cannot reach {self.settings.describe()}. Is the stack up? Try: make up  ({exc})"
            ) from exc

    def list_keys(self, prefix: str = "") -> list[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.settings.bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
        return keys

    def summarise(self, prefix: str = "") -> tuple[int, int]:
        """(object count, total bytes) under a prefix."""
        paginator = self.client.get_paginator("list_objects_v2")
        count = 0
        size = 0
        for page in paginator.paginate(Bucket=self.settings.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                count += 1
                size += item["Size"]
        return count, size

    def read_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.settings.bucket, Key=key)["Body"].read()

    def read_table(self, key: str) -> Any:
        import pyarrow.parquet as pq

        return pq.read_table(io.BytesIO(self.read_bytes(key)))

    # -- writes -----------------------------------------------------------

    def put_bytes(self, key: str, payload: bytes, *, content_type: str) -> None:
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            self.client.put_object(
                Bucket=self.settings.bucket, Key=key, Body=payload, ContentType=content_type
            )
        except (ClientError, BotoCoreError) as exc:
            raise BronzeStorageError(
                f"Writing s3://{self.settings.bucket}/{key} failed: {exc}"
            ) from exc

    def put_table(self, key: str, table: Any) -> int:
        """Write an Arrow table as Parquet. Returns the compressed size.

        Buffered in memory and written as one object rather than streamed:
        an S3 object only becomes visible once the PUT completes, so a crash
        mid-write leaves nothing behind instead of a truncated Parquet file
        that a reader would choke on later.
        """
        import pyarrow.parquet as pq

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="zstd", use_dictionary=True)
        payload = buffer.getvalue()
        self.put_bytes(key, payload, content_type="application/vnd.apache.parquet")
        return len(payload)

    def delete_keys(self, keys: list[str]) -> int:
        """Delete objects in batches. Returns how many were deleted.

        1000 is the S3 API's limit per request, not a tuning choice. Batching
        matters more than it looks: deleting a superseded Silver layer one
        request at a time is thousands of round trips.
        """
        from botocore.exceptions import BotoCoreError, ClientError

        batch_size = 1000
        deleted = 0
        try:
            for start in range(0, len(keys), batch_size):
                chunk = keys[start : start + batch_size]
                self.client.delete_objects(
                    Bucket=self.settings.bucket,
                    Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
                )
                deleted += len(chunk)
        except (ClientError, BotoCoreError) as exc:
            raise BronzeStorageError(
                f"Deleting from s3://{self.settings.bucket} failed after {deleted:,}: {exc}"
            ) from exc
        return deleted
