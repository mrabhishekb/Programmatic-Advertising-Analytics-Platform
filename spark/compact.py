"""Merging a day's small CDC objects into one file per Kafka partition.

Why this exists
---------------
The sink flushes on a timer, and every flush fans out across (table, day, Kafka
partition). At a 15-minute interval that is 96 flushes a day times fifteen
combinations, so a quiet table still accumulates a few hundred objects holding a
few kilobytes each. Reading those costs one S3 request and one Parquet footer
parse per file, which is most of the time a reconciliation run spends.

Why it does not violate Bronze's immutability
---------------------------------------------
Bronze object names are derived from the Kafka offsets they contain, not from
when they were written. Merging the parts covering offsets 100-150 and 151-200
produces the file that *would* have been written had the sink flushed once
instead of twice, under exactly the same naming rule. The records, their order
and their content are untouched; only the number of files changes. Replaying
those offsets still lands on the same key.

That is a physical reorganisation, which is a different thing from editing
history - and it is why the inputs are deleted only after the merged object has
been written and read back.

Deliberately not Spark
----------------------
Rewriting a few hundred megabytes is not a distributed problem, and doing it in
Spark would mean a committer deciding when files appear and disappear. Here the
sequence is explicit: write the merged object, verify it, then delete the inputs.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass

import pyarrow as pa

from bronze import layout
from bronze.storage import BronzeStore
from data_generator.logging_setup import get_logger

logger = get_logger(__name__)

#: ``part-p0002-000000000809-000000000832.parquet``
_PART = re.compile(r"part-p(\d{4})-(\d{12})-(\d{12})\.parquet$")


class CompactionError(RuntimeError):
    """Raised when a merged object does not match the inputs it replaces."""


@dataclass(frozen=True, slots=True)
class CompactionGroup:
    """Every object for one (table, day, Kafka partition), and what replaces them."""

    table: str
    partition_date: str
    topic_partition: int
    sources: tuple[str, ...]
    target_key: str
    first_offset: int
    last_offset: int

    @property
    def redundant(self) -> bool:
        """Already one file, so there is nothing to merge."""
        return len(self.sources) <= 1


@dataclass(frozen=True, slots=True)
class CompactionResult:
    group: CompactionGroup
    records: int
    bytes_written: int
    deleted: int


def parse_part_key(key: str) -> tuple[str, str, int, int, int] | None:
    """``cdc/<table>/dt=<date>/part-pNNNN-<first>-<last>.parquet`` -> its pieces."""
    parts = key.split("/")
    expected_depth = 4
    if len(parts) != expected_depth or parts[0] != layout.CDC_PREFIX:
        return None
    if not parts[2].startswith("dt="):
        return None
    match = _PART.search(parts[3])
    if not match:
        return None
    return (
        parts[1],
        parts[2].removeprefix("dt="),
        int(match.group(1)),
        int(match.group(2)),
        int(match.group(3)),
    )


def plan(store: BronzeStore, *, table: str | None = None) -> list[CompactionGroup]:
    """Group the CDC objects that could be merged, without touching anything."""
    prefix = f"{layout.CDC_PREFIX}/{table}/" if table else f"{layout.CDC_PREFIX}/"
    buckets: dict[tuple[str, str, int], list[tuple[int, int, str]]] = defaultdict(list)

    for key in store.list_keys(prefix):
        parsed = parse_part_key(key)
        if parsed is None:
            continue
        name, partition_date, topic_partition, first, last = parsed
        buckets[(name, partition_date, topic_partition)].append((first, last, key))

    groups = []
    for (name, partition_date, topic_partition), items in sorted(buckets.items()):
        # By first offset, which is also record order: the sink writes one object
        # per flush and offsets only increase.
        items.sort()
        first = items[0][0]
        last = max(item[1] for item in items)
        groups.append(
            CompactionGroup(
                table=name,
                partition_date=partition_date,
                topic_partition=topic_partition,
                sources=tuple(item[2] for item in items),
                target_key=layout.cdc_object_key(
                    name,
                    partition_date,
                    topic_partition=topic_partition,
                    first_offset=first,
                    last_offset=last,
                ),
                first_offset=first,
                last_offset=last,
            )
        )
    return groups


def compact_group(store: BronzeStore, group: CompactionGroup) -> CompactionResult:
    """Write the merged object, confirm it reads back, then delete the inputs."""
    tables = [store.read_table(key) for key in group.sources]
    merged = pa.concat_tables(tables).sort_by([("kafka_offset", "ascending")])
    expected = sum(item.num_rows for item in tables)

    bytes_written = store.put_table(group.target_key, merged)

    # Read it back before deleting anything. A merged object that cannot be
    # parsed is recoverable while its inputs still exist and unrecoverable a
    # moment later, and the cost of checking is one GET.
    verified = store.read_table(group.target_key)
    if verified.num_rows != expected:
        raise CompactionError(
            f"{group.target_key} has {verified.num_rows:,} rows, expected {expected:,}. "
            "Inputs left in place."
        )

    deleted = 0
    for key in group.sources:
        if key == group.target_key:
            # The merged range can coincide with one of its inputs. Deleting it
            # would delete the output.
            continue
        store.client.delete_object(Bucket=store.settings.bucket, Key=key)
        deleted += 1

    logger.info(
        "compacted",
        extra={
            "table": group.table,
            "dt": group.partition_date,
            "partition": group.topic_partition,
            "sources": len(group.sources),
            "records": expected,
            "deleted": deleted,
        },
    )
    return CompactionResult(
        group=group, records=expected, bytes_written=bytes_written, deleted=deleted
    )
