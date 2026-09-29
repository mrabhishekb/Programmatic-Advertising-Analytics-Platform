"""Where things live in the Bronze bucket, and why.

Two producers write here and they must not collide:

* ``scripts/export_snapshot.py`` - the bulk export of rows that already existed
  when CDC started. One directory per export run.
* ``bronze.sink`` - the CDC tail, one file per flush, partitioned by the date
  the change happened.

Both are append-only. Nothing in Bronze is ever updated or deleted: a correction
arrives as a later change event with a higher LSN, and the Silver layer decides
which one wins. That is what makes Bronze replayable - reprocessing is just
reading the same objects again.

The layout is here rather than inlined at each call site because the sink writes
these paths and Spark reads them in phase 5. A convention split across two
codebases drifts; a convention in one module does not.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime

#: Top-level prefixes. Separate so a reader can take the snapshot alone, the
#: change stream alone, or both, without filtering object names.
SNAPSHOT_PREFIX = "snapshot"
CDC_PREFIX = "cdc"

#: Hive-style partitioning, which is what Spark, Athena and Trino all expect to
#: find. `dt=` on its own (rather than year=/month=/day=) keeps a day's worth of
#: changes in one partition, which matches how the Silver merge reads them.
_PARTITION = "dt={date}"

_SAFE_SEGMENT = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")


class LayoutError(ValueError):
    """Raised when a path segment would produce an unreadable object key."""


def _check(segment: str, label: str) -> str:
    if not _SAFE_SEGMENT.match(segment):
        raise LayoutError(f"{label} {segment!r} is not safe to use in an object key")
    return segment


@dataclass(frozen=True, slots=True)
class BronzeObject:
    """A single object to be written, and everything needed to name it."""

    key: str
    table: str
    partition_date: str
    records: int


def cdc_partition_prefix(table: str, partition_date: str) -> str:
    """Prefix holding every change event for one table on one day."""
    _check(table, "table")
    _check(partition_date, "partition date")
    return f"{CDC_PREFIX}/{table}/{_PARTITION.format(date=partition_date)}"


def cdc_object_key(
    table: str,
    partition_date: str,
    *,
    topic_partition: int,
    first_offset: int,
    last_offset: int,
) -> str:
    """Name a CDC Parquet file after the Kafka offsets it contains.

    Deriving the name from (partition, first offset, last offset) rather than a
    timestamp or a random id makes the write idempotent: replaying the same
    Kafka range after a crash produces the same object key and overwrites
    identical bytes, instead of silently duplicating every record. The sink
    commits offsets only after the write succeeds, so that replay is the
    expected path rather than an edge case.
    """
    prefix = cdc_partition_prefix(table, partition_date)
    return f"{prefix}/part-p{topic_partition:04d}-{first_offset:012d}-{last_offset:012d}.parquet"


def snapshot_run_prefix(run_id: str) -> str:
    _check(run_id, "run id")
    return f"{SNAPSHOT_PREFIX}/run_id={run_id}"


def snapshot_object_key(run_id: str, table: str, *, part: int = 0) -> str:
    _check(table, "table")
    return f"{snapshot_run_prefix(run_id)}/{table}/part-{part:05d}.parquet"


def snapshot_manifest_key(run_id: str) -> str:
    """The manifest records the WAL position the export was consistent as of.

    Phase 5 needs it to know which change events the snapshot already contains;
    without it the two paths cannot be reconciled without guessing.
    """
    return f"{snapshot_run_prefix(run_id)}/_manifest.json"


def partition_date_of(timestamp_ms: int) -> str:
    """Partition on when the change *happened*, not when it was written.

    Using arrival time would scatter a single source transaction across two
    partitions whenever the sink happens to flush near midnight, and would make
    a replay land in different partitions than the original run.
    """
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).strftime("%Y-%m-%d")


def new_run_id(moment: datetime | None = None) -> str:
    """Sortable, collision-free enough for one export per second."""
    return (moment or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
