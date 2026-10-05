# Iceberg and the Silver layer (phase 6)

Phase 5 produced correct Silver tables and wrote them the only way a directory
of Parquet allows: delete the old files, write new ones. This phase keeps the
reconciliation logic exactly as it was and changes what it writes into - from a
prefix in a bucket to an Apache Iceberg table.

The distinction is narrower than it sounds and it matters more than it sounds.
A directory of Parquet files is not a table; it is a pile of files, and "which
files are the table right now" is answered by listing the directory. Everything
below follows from moving that answer somewhere that can be updated atomically.

See [docs/spark.md](spark.md) for the reconciliation itself - LSN ordering, the
snapshot boundary, soft deletes. None of it changed.

## Running it

```
make silver                 reconcile the seven dimension tables
make silver-all             and copy the four event tables through as well
make silver-plan            count everything, write nothing
make silver-ls              what the warehouse holds
make silver-history TABLE=campaigns     every version, and its lineage
make silver-show TABLE=campaigns AS_OF=<snapshot_id>    read an older version
```

`silver-ls` still runs from the project venv rather than the container, because
it only reads object metadata. Everything else needs the JVM.

## What was actually wrong with phase 5

```python
.write.mode("overwrite").parquet("s3a://adtech-bronze/silver/campaigns")
```

Spark deletes the existing files, then writes the new ones. Three consequences,
none of which show up in a successful run:

- **A reader during the write sees a partial table.** Not a stale table - a
  wrong one, with some files from the new version and some missing entirely.
- **A failure halfway leaves the table broken**, with no way to get the old
  version back, because it was deleted first.
- **There is no "old version"** at all. The previous contents are gone, so
  "what did this row look like yesterday" has no answer.

Phase 5 lived with this because Silver is derived state: if it breaks, rerun the
job. That is a real argument and it holds right up until something downstream
reads a table while a job is running.

## How Iceberg fixes it

An Iceberg table is a metadata file listing the data files that constitute the
table, plus a pointer in a catalog naming the current metadata file. Writing
means: write new data files, write a new metadata file, then atomically move the
pointer. Readers resolve the pointer once and read a fixed set of files.

So a commit is a single compare-and-swap. There is no window during which the
table is half-updated, because the table *is* the pointer.

The old metadata file still exists, and so do the data files it names. That is
where time travel comes from - it is not a separate feature, it is the absence
of a delete.

## The catalog is the whole design decision

Iceberg is only as atomic as whatever stores that pointer. Three options:

| Backend | Atomic? | Cost |
|---|---|---|
| Hadoop/filesystem catalog | **No** on S3 | nothing to run |
| JDBC catalog | yes | needs a database |
| REST catalog | yes | needs a service |

The filesystem catalog is tempting because it needs no infrastructure, and it is
wrong here. It relies on atomic rename to swap the pointer, and S3 and MinIO
have no such operation - two concurrent commits can both believe they won. That
would leave this phase with Iceberg's file layout and none of its guarantees,
which is the worst of both.

This project uses the **JDBC catalog against the PostgreSQL it already runs**.
A row updated inside a transaction has exactly the semantics Iceberg needs, and
it costs no new container.

### In its own database, not a schema in `adtech`

```
jdbc:postgresql://postgres:5432/iceberg
```

PostgreSQL logical decoding is scoped to a single database: a replication slot
on `adtech` never sees changes made in `iceberg`. Putting the catalog in its own
database therefore keeps every Iceberg commit out of the WAL stream Debezium
decodes.

In a schema inside `adtech` it would still work - the connector's
`table.include.list` would filter the catalog tables out. But they would be
decoded first and discarded after, which is wasted effort on the slot, in a
pipeline whose entire subject is that WAL.

`spark/catalog.py` creates the database if it is missing. That is done in code
rather than as a `docker-entrypoint-initdb.d` script because those only run
against an empty data directory - anyone who has reached phase 6 has a populated
one, and would never see it.

### Iceberg reads through the same S3A connector as everything else

```python
"spark.sql.catalog.lake.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO"
```

Iceberg's own `S3FileIO` would also work, and is faster in production. It is a
second AWS client with a second copy of the endpoint, the credentials and the
path-style setting - two places for a bucket name to be wrong instead of one.
At this size the speed is not the constraint.

## Naming and layout

Tables are addressed in full: `lake.silver.campaigns` - catalog, namespace,
table. The catalog is deliberately **not** registered as
`spark.sql.defaultCatalog`, so the default stays Spark's own and
`spark.read.parquet` still reads raw Bronze files. Silver always takes three
parts, which makes the two layers impossible to confuse in a query.

```
s3://adtech-bronze/
├── snapshot/     Bronze: the consistent export
├── cdc/          Bronze: the change stream
└── warehouse/
    ├── _runs/            run manifests
    └── silver/
        └── campaigns/
            ├── data/        Parquet
            └── metadata/    manifests, manifest lists, table metadata
```

One trap worth naming: it is `warehouse/silver/`, **not** `warehouse/silver.db/`.
The `.db` suffix is Hive's convention, which `HiveCatalog` reproduces and
`JdbcCatalog` does not. Guessing wrong does not break a write - Iceberg records
the real location in the catalog and carries on - it breaks anything that goes
looking for the files directly, which then cheerfully reports an empty layer
that is not empty. `make silver-ls` did exactly that until it was fixed, and
`tests/test_iceberg_catalog.py` now pins the convention.

The warehouse is a new prefix rather than phase 5's `silver/`, which still held
flat Parquet. Pointing a warehouse at a directory of foreign files invites a
reader to treat them as part of a table. `make silver-drop-legacy` removes the
old layer; it is not automatic, because silently deleting a layer during an
upgrade is how people lose the thing they meant to compare against.

## Partitioning: only the event tables

```python
writer.partitionedBy(F.days(F.col("impression_timestamp")))
```

`days(ts)` is a **hidden partition**: the stored partition value is derived from
the timestamp by Iceberg, so a query filtering on `impression_timestamp` prunes
partitions without anyone writing `WHERE dt = ...`. Hive-style layouts need that
extra column in the data and in every query that wants pruning, which is how
tables end up with a `dt` that disagrees with the timestamp beside it.

Partitioned on when the event happened, not `created_at`, which is insertion
order: queries filter on event time, and a partition the planner cannot match is
a partition that never prunes.

The seven dimension tables are deliberately **not** partitioned. At 5,000 to
200,000 rows they fit in a single file each; partitioning would produce many
small ones, which is slower to plan and slower to read than leaving them whole.

### Distribution mode

```python
.tableProperty("write.distribution-mode", "hash")   # partitioned tables
.tableProperty("write.distribution-mode", "none")   # dimension tables
```

With `hash`, Spark redistributes rows by partition value before writing, so each
task writes one day. Without it every task writes into every day it happens to
hold, turning one write into (tasks x days) files. Impressions produced 118
files across 90 days; ungrouped it would have been thousands.

For the unpartitioned tables there is nothing to group by, and a shuffle would
be pure cost - so `none`, with `coalesce` controlling the file count directly.

## Lineage lives in the snapshot

```python
.option("snapshot-property.bronze-snapshot-run", snapshot.run_id)
.option("snapshot-property.bronze-snapshot-lsn", str(snapshot.wal_lsn))
```

Each commit records which Bronze export produced it and the WAL position that
export was consistent as of. In the snapshot summary rather than a side file, so
the lineage travels with the commit: `make silver-history TABLE=campaigns`
answers "which Bronze run produced this" for every version that ever existed,
not just the current one.

```
         snapshot_id  committed           op                  rows  bronze run
 2960805402178615810  2026-10-05 07:46:55 append             5,000  20260930T052225Z
 4970838561496121903  2026-10-05 07:58:41 append             5,000  20260930T052225Z
 4161860247579063385  2026-10-05 08:14:58 append             5,000  20260930T052225Z
```

## Counting from metadata instead of scanning

Iceberg stores a row count and a byte count per file in its manifests, so the
run report reads them rather than counting rows:

```sql
SELECT sum(record_count), count(*), sum(file_size_in_bytes) FROM lake.silver.impressions.files
```

This is free, and it is also the only correct way to get the number. Listing the
table's prefix in S3 would include superseded snapshots' files, which stay in
the bucket until they are expired - `make silver-ls` reports 632 KB of storage
for a 195 KB `advertisers` table for exactly that reason, and says so.

Phase 5 cached the whole DataFrame and counted it twice. That is the wrong trade
once a 100M-row event table is in the set: caching it spills to disk and reads
it back. Reading the manifests costs nothing at any size.

`record_count` becomes an upper bound once phase 7 writes delete files, since a
row deleted by one is still counted by the manifest that added it. There are
none yet: a full replace never writes any.

## Format version 2

```python
.tableProperty("format-version", "2")
```

Nothing in this phase writes row-level deletes - a full replace has no need for
them. But v2 is what makes them possible, phase 7's incremental `MERGE INTO`
needs them, and the format version cannot be raised in place later without
rewriting every file.

## Measured on this project

All 11 tables, 23 GB / 10 CPU Docker VM, 8 GB Spark driver, `local[*]`:

| table | rows | files | size | partitioned |
|---|---|---|---|---|
| advertisers | 5,000 | 1 | 195.1 KB | |
| publishers | 20,000 | 1 | 897.2 KB | |
| placements | 100,000 | 1 | 4.5 MB | |
| campaigns | 50,000 | 1 | 2.2 MB | |
| line_items | 150,000 | 1 | 7.4 MB | |
| creatives | 200,000 | 1 | 8.8 MB | |
| audiences | 10,336 | 1 | 426.3 KB | |
| impressions | 100,000,000 | 118 | 5.6 GB | `days(impression_timestamp)` |
| clicks | 5,000,000 | 90 | 309.9 MB | `days(click_timestamp)` |
| conversions | 500,000 | 90 | 40.8 MB | `days(conversion_timestamp)` |
| spend_transactions | 19,335,944 | 90 | 589.2 MB | `days(spend_timestamp)` |
| **total** | **125,371,280** | **395** | **6.6 GB** | |

736.8s end to end; impressions alone was 600.5s of it. The dimension tables
together took under 20 seconds, which is why they remain the default and the
event tables stay behind `--include-events`.

## Verified behaviour

- **`audiences` reconciles exactly.** 10,336 rows, 309 flagged deleted, 10,027
  live. Live PostgreSQL holds 10,028 - one row inserted after the Bronze export
  this run read. Silver is as of its inputs, not as of now; closing that gap is
  phase 7.
- **No duplicate keys from the partitioned write.** `count(DISTINCT
  impression_id)` is 100,000,000 across 118 files and 90 partitions.
- **Time travel reads an older version.** `advertisers` as of its first snapshot
  still returns 5,000 rows, two commits later.
- **Day partitions match the generated range**: 90, for a 90-day dataset.

## Not in this phase

**Snapshot expiry.** Every run leaves its predecessor's files in the bucket, so
storage grows with history. Nothing expires them yet. That is deliberate - it is
what made the time-travel check above possible - but a scheduled
`expire_snapshots` belongs with Airflow in phase 15.

**Incremental writes.** This is still a full rebuild: it reads the whole
snapshot and the whole change history every run, and replaces the table. Correct,
and increasingly wasteful. `MERGE INTO` against only new changes is phase 7, and
format version 2 is in place for it.

**Concurrent writers.** The catalog now makes them safe, but nothing in this
project runs two jobs at once to prove it.
