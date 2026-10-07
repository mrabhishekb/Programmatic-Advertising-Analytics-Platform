"""Remembering how far Silver has got, and reading only what came after.

Phase 6 rebuilt every table on every run: read the whole snapshot, read the whole
change history, replace the table. Correct, and increasingly wasteful - the
change history only grows, so the cost of a run climbs while the amount of new
work in it stays flat. Phase 7 makes a run proportional to what changed.

That needs a bookmark, and where to keep it is the whole design.

The watermark is not stored, it is derived
------------------------------------------
The obvious design is a file, or a table property, saying "last processed: 900".
The problem is that writing the rows and writing the bookmark are then two acts,
and a crash can land between them. One order loses work - the bookmark advances
past rows that were never written, and those changes are skipped permanently
with nothing to notice it. The other order merely repeats work.

Silver already records the answer. Every row carries ``_lsn``: the WAL position
of the change that produced it, null for rows that came straight from the
snapshot. So ``max(_lsn)`` over the table *is* the high-water mark, and it is
exact for the same reason the merge is correct - collapsing picks the newest
change per key, so the highest LSN in a batch is always the one that lands.

A derived watermark cannot disagree with the data, because it is the data. A
half-applied run leaves a watermark that matches what actually committed, and
rerunning finishes the job.

Iceberg keeps per-column bounds in its manifests, so this costs a metadata read
rather than a scan.

The offsets are a hint, and are stored
--------------------------------------
Which Bronze *files* to open cannot be derived from Silver, so that one is kept
in table properties - written separately from the merge, and deliberately so.
It is only ever a hint: a stale or missing offset bookmark means opening files
whose changes have already been applied, and the LSN filter discards them. The
two bookmarks have different durability requirements because they do different
jobs, and only one of them can be wrong without being harmful.

Offsets rather than the ``dt=`` partition, which would be the obvious choice and
is wrong: ``bronze.layout.partition_date_of`` dates a file by when the change
*happened*, so a late-arriving event lands in an old partition and pruning by
date would skip it forever. Kafka offsets are arrival order, so a late event
still gets a higher offset than anything already read.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from bronze import layout as bronze_layout
from bronze.storage import BronzeStore
from data_generator.logging_setup import get_logger
from data_generator.models import EVENT_TABLES
from spark import catalog, compact, layout

logger = get_logger(__name__)

#: Which Bronze snapshot export this table was built from. A table property
#: rather than a snapshot property: it describes the table, and has to survive
#: the merges that follow the build. The per-commit copy phase 6 writes into
#: each snapshot summary stays, because that is lineage for one version.
BRONZE_RUN_TABLE_PROPERTY = "adtech.bronze-snapshot-run"

#: ``{kafka_partition: highest offset read}``, JSON. A hint, see above.
APPLIED_OFFSETS_PROPERTY = "adtech.applied-offsets"


class RunMode(StrEnum):
    """What one table needs this run."""

    #: No Iceberg table yet. Read the snapshot and the whole change history.
    BOOTSTRAP = "bootstrap"
    #: Table exists and its base is still current. Merge what is above the
    #: watermark.
    INCREMENTAL = "incremental"
    #: Nothing new. Do not open a writer at all.
    SKIP = "skip"
    #: The base moved under us - a newer snapshot export, or --full.
    REBUILD = "rebuild"


@dataclass(frozen=True, slots=True)
class TableState:
    """How far this table has got, read back from Iceberg."""

    exists: bool = False
    bronze_run: str | None = None
    applied_lsn: int | None = None
    offsets: dict[int, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TablePlan:
    """What to do about one table, and the inputs to do it with."""

    table: str
    mode: RunMode
    state: TableState
    #: Explicit object URLs rather than a directory, so Spark opens only these.
    cdc_urls: tuple[str, ...] = ()
    #: Offsets to record once this run's inputs have been applied.
    offsets: dict[int, int] = field(default_factory=dict)
    reason: str = ""

    @property
    def writes(self) -> bool:
        return self.mode is not RunMode.SKIP

    @property
    def full(self) -> bool:
        return self.mode in (RunMode.BOOTSTRAP, RunMode.REBUILD)


def table_exists(spark: Any, identifier: str) -> bool:
    """Whether the catalog knows this table.

    ``tableExists`` rather than try/except around a read: a missing table and an
    unreadable one are different problems, and only the first is normal.
    """
    return bool(spark.catalog.tableExists(identifier))


def table_properties(spark: Any, identifier: str) -> dict[str, str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {identifier}").collect()
    return {row["key"]: row["value"] for row in rows}


def applied_lsn(spark: Any, identifier: str) -> int | None:
    """The highest WAL position this table reflects, read off the rows.

    Null for snapshot-born rows, so an untouched table correctly reports None
    and falls back to the export's own WAL position as its floor.
    """
    # Imported here so this module stays usable without PySpark, which is what
    # lets the planning logic be unit-tested rather than only run in Docker.
    from spark.schemas import SOURCE_LSN

    row = spark.sql(f"SELECT max({SOURCE_LSN}) AS high FROM {identifier}").first()
    return int(row["high"]) if row and row["high"] is not None else None


def read_state(spark: Any, identifier: str) -> TableState:
    if not table_exists(spark, identifier):
        return TableState(exists=False)

    properties = table_properties(spark, identifier)
    return TableState(
        exists=True,
        bronze_run=properties.get(BRONZE_RUN_TABLE_PROPERTY),
        applied_lsn=applied_lsn(spark, identifier),
        offsets=decode_offsets(properties.get(APPLIED_OFFSETS_PROPERTY)),
    )


def decode_offsets(raw: str | None) -> dict[int, int]:
    if not raw:
        return {}
    try:
        return {int(key): int(value) for key, value in json.loads(raw).items()}
    except (ValueError, TypeError, AttributeError):
        # A hint that cannot be read means opening more files, not wrong
        # answers. Failing the run over it would be the worse trade.
        logger.warning("ignoring unreadable offset bookmark", extra={"raw": raw})
        return {}


def encode_offsets(offsets: dict[int, int]) -> str:
    return json.dumps({str(key): value for key, value in sorted(offsets.items())})


def record_offsets(spark: Any, identifier: str, offsets: dict[int, int]) -> None:
    """Store the file-pruning hint. Separate commit from the data, by design."""
    if not offsets:
        return
    value = encode_offsets(offsets)
    spark.sql(
        f"ALTER TABLE {identifier} SET TBLPROPERTIES ('{APPLIED_OFFSETS_PROPERTY}' = '{value}')"
    )


def new_cdc_objects(
    store: BronzeStore,
    table: str,
    offsets: dict[int, int],
) -> tuple[list[str], dict[int, int]]:
    """Bronze CDC objects not yet read, and the offsets after reading them.

    A file is skipped only when its whole range is at or below what has been
    read from that Kafka partition. Compaction merges files and so can produce a
    range straddling the bookmark; that file is read again and the LSN filter
    discards the part already applied.
    """
    selected: list[str] = []
    high = dict(offsets)

    for key in sorted(store.list_keys(f"{bronze_layout.CDC_PREFIX}/{table}/")):
        parsed = compact.parse_part_key(key)
        if parsed is None:
            continue
        _, _, partition, first_offset, last_offset = parsed
        seen = offsets.get(partition)
        if seen is not None and last_offset <= seen:
            continue
        selected.append(key)
        high[partition] = max(high.get(partition, first_offset), last_offset)

    return selected, high


def plan_table(
    spark: Any,
    store: BronzeStore,
    *,
    table: str,
    bronze_run: str,
    bucket: str,
    force_full: bool = False,
) -> TablePlan:
    """Decide what this table needs, and which Bronze objects feed it."""
    identifier = catalog.table_identifier(table)
    state = read_state(spark, identifier)

    def full(mode: RunMode, reason: str) -> TablePlan:
        # A rebuild starts from the snapshot export again, so it has to replay
        # the whole change history onto it - pruning by the old bookmark would
        # leave the rebuilt table missing every change already applied to the
        # table being replaced.
        keys, offsets = new_cdc_objects(store, table, {})
        urls = tuple(layout.s3a_url(bucket, key) for key in keys)
        return TablePlan(table, mode, state, cdc_urls=urls, offsets=offsets, reason=reason)

    if force_full:
        return full(RunMode.REBUILD, "--full")
    if not state.exists:
        return full(RunMode.BOOTSTRAP, "no Iceberg table yet")
    if state.bronze_run != bronze_run:
        # The export under the table was replaced, so every row's base may have
        # moved. Merging onto the old base would leave rows nothing has edited
        # reflecting an export that is no longer current.
        return full(RunMode.REBUILD, f"snapshot export changed: {state.bronze_run} -> {bronze_run}")
    if table in EVENT_TABLES:
        # Append-only and not captured by Debezium, so the export is the only
        # input and it has not moved. A rerun could only rewrite six gigabytes
        # into an identical table; it rebuilds when a new export appears.
        return TablePlan(table, RunMode.SKIP, state, reason="append-only, export unchanged")

    keys, offsets = new_cdc_objects(store, table, state.offsets)
    if not keys:
        return TablePlan(table, RunMode.SKIP, state, reason="no new change events")

    return TablePlan(
        table,
        RunMode.INCREMENTAL,
        state,
        cdc_urls=tuple(layout.s3a_url(bucket, key) for key in keys),
        offsets=offsets,
        reason=f"{len(keys)} new Bronze object(s)",
    )
