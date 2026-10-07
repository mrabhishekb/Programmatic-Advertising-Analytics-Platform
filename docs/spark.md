# Spark and the Silver layer (phase 5)

Bronze answers *what arrived, and in what order*. It holds two descriptions of
the same tables, each incomplete on its own: a bulk snapshot frozen at one WAL
position, and every change that happened after it. Neither is current state.

Silver answers *what the data is now*. One row per primary key, typed, with
deleted rows kept and flagged.

```
snapshot/run_id=.../campaigns/   ─┐
                                  ├─> reconcile ──> silver/campaigns/
cdc/campaigns/dt=.../            ─┘
```

## Running it

```bash
make silver            # reconcile the seven dimension tables
make silver-plan       # count everything, write nothing
make silver-all        # also copy the four append-only event tables through
make silver-ls         # what landed
make silver-show TABLE=campaigns
```

Spark needs a JVM, which nothing else in this stack does, so it lives in its own
image (`docker/spark.Dockerfile`) behind a Compose profile. `make up` does not
start it: a batch container whose whole purpose is to finish would otherwise sit
in `Exited (0)` next to six healthy services and look like a failure.

`make silver-ls` and `make silver-compact` run from the project venv instead,
because they only read object metadata. That is why `scripts/silver.py` imports
`spark.job` and `spark.session` inside the two commands that need them rather
than at module scope - importing PySpark at the top would make listing a bucket
require a JVM.

## The three decisions that make it correct

### Order by LSN, not by time

Every change event carries an LSN: the position in PostgreSQL's write-ahead log
where it was committed. That is a total order over the database.

`event_ts_ms` is not. Debezium stamps every change in a transaction with the
same commit timestamp, so ordering by it picks an arbitrary winner among exactly
the rows most likely to disagree - the ones edited together. Twenty updates to
one campaign collapse to whichever one sorted last, which is a different answer
on each run.

The window is `partitionBy(pk).orderBy(lsn desc, kafka_offset desc)`. The offset
tiebreak never fires on well-formed input, because two events cannot share an
LSN; it is there so that malformed input produces a *stable* wrong answer rather
than a different one each run.

### Drop what the snapshot already contains

`scripts/export_snapshot.py` records the WAL position its export was consistent
as of, in `_manifest.json`:

```json
"consistent_snapshot": { "wal_lsn": "BE/9C2DEED8" }
```

Change events at or below that position describe edits already baked into the
snapshot rows. Replaying them is *usually* harmless, because the result is keyed
by primary key - but not always. A delete replayed over a row that was
re-inserted afterwards would wrongly remove it.

PostgreSQL prints an LSN as `BE/9C2DEED8`; Debezium reports the same position as
a plain integer. `spark.layout.parse_lsn` converts between them, and
`tests/test_silver_layout.py` checks that the high half is not dropped - a naive
`int(low, 16)` would make every position in segment 0 collide.

### A delete marks the row, it does not remove it

Deleting an audience segment from Silver orphans every historical impression
that referenced it, and "what did that segment target" becomes permanently
unanswerable. The row stays, with `is_deleted` set and `deleted_at` populated
from the event timestamp.

This only works because all seven captured tables are `REPLICA IDENTITY FULL`.
On a delete, Debezium's `after` is null and the row's final state is in `before`
\- and by default PostgreSQL puts only the primary key there. Without FULL,
a soft delete would preserve an ID and seven nulls.

Consumers filter `WHERE NOT is_deleted` for current state. Phase 10 turns the
same information into an SCD Type 2 validity range.

## Types, and why Bronze threw them away

Bronze stores `before` and `after` as JSON strings so a column added in
PostgreSQL never invalidates a file written yesterday. Something has to decide
what those strings mean, and `spark/schemas.py` is it.

The target schema comes from the **snapshot Parquet**, read back off the file
rather than from `information_schema`. Silver therefore runs with the source
database switched off, and the snapshot is the more correct authority anyway -
it is what the changes are being merged against.

The wire schema is how Debezium encodes those same columns, which follows from
two settings in `debezium/connector.json`:

| Source type | Snapshot Parquet | Debezium JSON | Converted with |
|---|---|---|---|
| `numeric(p,s)` | `decimal(p,s)` | string `"1234.5600"` | `cast` |
| `date` | `date` | int, days since epoch | `date_from_unix_date` |
| `timestamp` | `timestamp` | long, millis since epoch | `timestamp_millis` |
| `uuid` | string | string | - |

`decimal.handling.mode: string` is what keeps money exact: the alternative
encodes a NUMERIC as a float and loses the last cents on large budgets.
`time.precision.mode: connect` is what makes dates integers. With
`schemas.enable: false` there is nothing in the message saying any of this,
which is precisely why the mapping is written down rather than inferred.

**The session timezone is pinned to UTC** in `spark/session.py`. The source
columns are `timestamp without time zone` on a single simulation clock, and
Debezium encodes them as milliseconds with no zone attached. On any other
session zone the snapshot and the CDC path would disagree by the local offset -
wrong, and very hard to spot, because every row would look plausible.

## Tombstones

Debezium follows each delete with a null-valued message so Kafka's log
compaction can drop the key (`tombstones.on.delete: true`). It carries no
payload and no LSN.

They are counted and discarded. The delete immediately before says the same
thing and carries the row. They are filtered out *before* ranking rather than
losing on `desc_nulls_last`, because a key whose delete happened to be filtered
by the snapshot boundary would otherwise leave the tombstone to win with an
all-null payload - a row of nulls that looks like real data.

## What reconciliation does not do

No derived metrics. `docs/data_model.md` records that `bid_price`,
`clearing_price`, `target_cpm` and `floor_price` are all **CPM - per thousand
impressions**, while `spend_amount` is an absolute amount; and that
`spend_transactions` has three different grains depending on billing type.
Arithmetic that ignores either is wrong by a factor of 1000 or meaningless.
Silver passes columns through untouched and leaves the business semantics to the
Gold layer in phase 13, which is built for them.

No joins between tables either. Silver is one clean table per source table;
conforming them into a dimensional model is phase 12.

## Which tables, and why not all of them

Reconciliation applies to the seven tables Debezium captures - the mutable
dimensions. The four event tables are append-only and have no change stream, so
for them "reconcile" would mean "copy 7GB". They are opt-in behind
`--include-events` rather than part of a default run, which keeps the normal
cycle fast enough to iterate on.

A table that is captured but has never changed is not a special case: it reads
its snapshot, finds no CDC prefix, and passes through. `spark.layout.has_cdc_data`
exists because Spark raises on reading a path with no files, and "nobody has
edited this yet" is a normal state rather than an error.

## Compaction

```bash
make silver-compact          # show what would be merged
make silver-compact-commit   # merge, then delete the inputs
```

The sink flushes on a timer and fans out across (table, day, Kafka partition),
so a quiet table still accumulates hundreds of objects of a few kilobytes.
Reading them costs one request and one Parquet footer parse each, which was most
of what a reconciliation run spent its time on.

**Measured on this project's Bronze layer: 451 objects merged into 60, holding
15,733 change events in 2.8 MB.** A day of `campaigns` went from 21 objects to 3
\- one per Kafka partition.

### Why this does not violate Bronze's immutability

Bronze object names are derived from the Kafka offsets they contain, not from
when they were written:

```
cdc/campaigns/dt=2026-10-02/part-p0000-000000002441-000000002505.parquet
```

Merging the parts covering 2441-2505 and 2506-2592 produces
`part-p0000-000000002441-000000002592.parquet` - exactly the file the sink *would*
have written had it flushed once instead of twice, under the same naming rule.
The records, their order and their content are untouched; only the file count
changes. Replaying those offsets still lands on the same key.

That is a physical reorganisation, not an edit to history. It is deliberately
not a Spark job: rewriting a few hundred megabytes is not a distributed problem,
and doing it in Spark would put a committer in charge of when files appear and
disappear. Here the sequence is explicit - write the merged object, read it back
and check the row count, and only then delete the inputs. The bucket also has
versioning enabled, so a delete is recoverable.

## Output layout

> Superseded by phase 6. This section describes what phase 5 wrote; Silver is
> now an Iceberg table and the layout is [docs/iceberg.md](iceberg.md). The
> columns below are unchanged.

```
silver/
├── campaigns/part-*.parquet
├── creatives/...
└── _runs/20261004T134500Z.json
```

Each table prefix is overwritten in full. Bronze can name files after offsets;
Silver cannot, because its output depends on every change seen so far, so there
is no stable range to name a part after. Overwriting gives the same property by
a different route: running the job twice over the same inputs leaves the bucket
in the same state.

The run manifest records which snapshot run was used, the WAL position applied,
and per-table counts, so "why does this row look like this" has an answer that
does not require rerunning anything.

Added columns:

| Column | Meaning |
|---|---|
| `is_deleted` | the row was deleted at source |
| `deleted_at` | when, from the change event |
| `_lsn` | WAL position of the change that produced this version, null for untouched snapshot rows |
| `_op` | `c`, `u` or `d` |
| `_event_ts` | source commit time |

`_lsn` is null rather than the export's WAL position for snapshot rows: the row
came from a bulk `SELECT`, not from a change event, and filling in the
snapshot's LSN would claim a provenance it does not have.

## Measured on this project

First full run, seven dimension tables, 16.2 seconds:

| table | snapshot | change events | output rows | soft-deleted |
|---|---|---|---|---|
| advertisers | 5,000 | 0 | 5,000 | 0 |
| publishers | 20,000 | 1,124 | 20,000 | 0 |
| placements | 100,000 | 0 | 100,000 | 0 |
| campaigns | 50,000 | 7,691 | 50,000 | 0 |
| line_items | 150,000 | 2,573 | 150,000 | 0 |
| creatives | 200,000 | 3,066 | 200,000 | 0 |
| audiences | 10,025 | 1,788 | 10,336 | 309 |
| **total** | | | **535,336** | **309** |

`audiences` is the one that exercises every path, because it is the only table
the traffic generator inserts into and deletes from. The arithmetic checks out
against the live database: 10,336 rows minus 309 soft-deleted is 10,027, which
is exactly `SELECT count(*) FROM audiences`.

The tables with zero change events are not a failure. `advertisers` and
`placements` are captured by Debezium but nothing has edited them, so they pass
their snapshot through unchanged.

Of 585 tombstones in Bronze, only 309 produced a deleted row. The rest sit below
the snapshot's WAL position and were filtered - the boundary doing its job.

## Verified behaviour

`tests/test_spark.py` builds Bronze rows by hand and checks the merge semantics
without touching a bucket - a failure there is a wrong answer about merging, not
about S3 credentials. 18 tests covering: the highest LSN wins regardless of
arrival order; keys collapse independently; twenty changes to one row produce one
row; a delete keeps the row and marks it; a deleted row keeps its final values; a
re-insert after a delete is live again; a tombstone does not overwrite its
delete; a lone tombstone produces nothing; events below the snapshot boundary are
dropped; a delete below the boundary does not remove a re-inserted row; untouched
snapshot rows survive; rows inserted after the snapshot appear; the output has
exactly one row per key; running it twice gives the same answer; decimals survive
exactly; dates and timestamps decode from integers.

`tests/test_silver_layout.py` runs without a JVM and covers LSN parsing and the
compaction key convention.

```bash
make spark-test   # the 30, inside the Spark container
make test         # the 9 layout tests, with everything else
```

## Not in this phase

Silver was plain Parquet, overwritten in place, with no ACID commits, no schema
evolution and no time travel - so two jobs writing at once would race. Phase 6
moved it onto Iceberg and fixed all four; see [docs/iceberg.md](iceberg.md).
The reconciliation logic described above was not changed by that, which is what
keeping it storage-free bought.

Reconciliation was a full rebuild: it read the whole snapshot and the whole
change history every time, so its cost grew with the project's age rather than
with how much had changed. Phase 7 added a per-table decision - bootstrap,
merge, skip or rebuild - driven by a watermark that is derived from the rows
rather than stored beside them; see
[docs/incremental.md](incremental.md).
