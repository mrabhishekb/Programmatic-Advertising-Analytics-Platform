# S3 Bronze

The raw layer. Two producers write here, neither interprets what it carries, and
nothing is ever updated or deleted.

```
                    ┌─ export_snapshot.py ──> snapshot/run_id=.../<table>/*.parquet
PostgreSQL ─────────┤   (rows that already existed)
                    └─ Debezium → Kafka ────> cdc/<table>/dt=YYYY-MM-DD/*.parquet
                        (every change since)      via bronze.sink
```

MinIO stands in for S3 locally. Nothing in the code is MinIO-specific beyond the
endpoint and path-style addressing, both settings, so pointing this at real S3
means editing `.env`.

## Why two shapes

The snapshot writes **typed** Parquet - `NUMERIC(14,2)` becomes
`decimal128(14,2)`, `DATE` becomes `date32`. The CDC sink writes a **fixed
envelope schema** with the row payload as an opaque JSON string.

That asymmetry is deliberate, because the two carry different things. A snapshot
row is a row, read as a full table scan, which is exactly what columnar Parquet
is for. A change event is a before/after pair plus ordering metadata, and its
*payload* shape is whatever the source table looked like at that moment.

If Bronze stored change payloads as structs, every file would be bound to the
source schema of the day it was written. Add a column in phase 17 and the old
files and the new ones no longer share a schema, so a reader has to merge
schemas across the entire history — expensive, and it fails outright on a type
change — or Bronze has to be rewritten, which contradicts it being immutable.

Keeping the payload opaque moves that problem to where it can be solved. Bronze
answers *what arrived, and in what order*. Silver answers *what it means*, and
parses each file against the schema it needs today.

## The CDC record

Every change event, from every table, has exactly this shape:

| Column | Why it is here |
|---|---|
| `op` | `c`/`u`/`d`/`r` from Debezium, plus `t` for a tombstone |
| `source_schema`, `source_table` | which table, without parsing the topic name |
| `event_ts_ms` | when the change happened; partitioning uses this |
| `lsn`, `tx_id` | the only true ordering across tables |
| `key` | the Kafka message key (the primary key) |
| `before_json`, `after_json` | the payload, opaque |
| `kafka_topic`, `kafka_partition`, `kafka_offset` | provenance for any row |
| `ingested_at_ms` | when the sink wrote it |

`lsn` matters more than it looks. Two changes inside one transaction share a
`tx_id` and can share a millisecond timestamp, so the log sequence number is the
only thing that orders them. Phase 10's SCD Type 2 needs that order to build a
correct history.

Tombstones get their own op code rather than being dropped. Debezium writes a
null-valued message after a delete so log compaction can drop the key (see
[kafka.md](kafka.md)); it carries no payload, so once written it is
indistinguishable from anything else unless it is labelled.

## Layout

```
s3://adtech-bronze/
├── snapshot/
│   └── run_id=20260929T191740Z/
│       ├── _manifest.json
│       ├── campaigns/part-00000.parquet
│       └── impressions/part-00000.parquet, part-00001.parquet, ...
└── cdc/
    └── campaigns/
        └── dt=2026-09-29/
            └── part-p0002-000000000000-000000000379.parquet
```

`dt=` is Hive-style because that is what Spark, Athena and Trino all discover
automatically. One partition per day rather than year/month/day, because a day
is the unit the Silver merge reads.

**Partitioning is on event time, not arrival time.** Using arrival time would
scatter one source transaction across two partitions whenever a flush lands near
midnight, and a replay would file its records somewhere different than the
original run did.

### The object name is the idempotency mechanism

A CDC file is named after the Kafka offsets it contains:
`part-p<partition>-<first offset>-<last offset>.parquet`.

That is not decoration. The sink is at-least-once, so a crash between writing
and committing means the same range is read again on restart. Because the name
is a function of that range, the replay writes the **same key** with the same
content — it overwrites identical bytes instead of silently adding a second copy
of every record under a fresh name.

Offsets are zero-padded so object listings sort chronologically; unpadded,
`part-...-1000` sorts before `part-...-2`.

## Delivery guarantee

At-least-once. The order is always:

1. write the Parquet object
2. **then** commit the Kafka offsets

The reverse — commit, then write — is at-most-once, and a crash in the gap loses
change events permanently with nothing left to detect the loss. For the layer
whose entire purpose is to be the durable record, that trade is the wrong way
round. Duplicates are recoverable; gaps are not.

This is why `enable.auto.commit` is off on the sink's consumer: auto-commit
acknowledges messages that are still sitting in the buffer.

## The handover from the snapshot

The manifest records the WAL position the export was consistent as of:

```json
"consistent_snapshot": {
  "wal_lsn": "BE/97F33150",
  "postgres_snapshot_id": "...",
  "snapshot_time": "2026-09-30T00:47:40"
}
```

Change events at or before that LSN are already reflected in the snapshot files.
Replaying them is harmless because the Silver merge is by primary key, but phase
5 needs the number to know where the two paths meet. Without it the overlap can
only be guessed at.

The export still refuses to run before the replication slot exists, for the
reason in [cdc.md](cdc.md): exporting first leaves a window where a change is
captured by neither path.

## Running it

```bash
make export-snapshot          # the rows that already exist -> Bronze
make export-snapshot-master   # same, skipping the 100M-row event tables
make bronze-sink              # drain the CDC topics into Bronze, then stop
make bronze-sink-forever      # ... or keep running
make bronze-ls                # what is in the bucket, by table and day
make bronze-peek              # read change events back out
```

```
layer     table                 partition         objects        size
---------------------------------------------------------------------
cdc       audiences             dt=2026-09-29           3     38.7 KB
cdc       campaigns             dt=2026-09-29           3    196.8 KB
cdc       creatives             dt=2026-09-29           3    120.4 KB
cdc       line_items            dt=2026-09-29           3     69.5 KB
cdc       publishers            dt=2026-09-29           3     51.2 KB
---------------------------------------------------------------------
total                                                  15    476.6 KB
```

Re-running `make bronze-sink` when it is already caught up writes nothing: the
committed consumer offsets mean there is nothing left to read.

## Measured

Against the loaded database:

* the master-table export wrote **535,011 rows to 26.4 MB of Parquet in 2.9s**,
  compared with 27 MB of gzipped CSV for the same rows in phase 2 — the same
  size, now typed, columnar and pushdown-friendly
* the sink drained **2,571 change events into 15 objects (477 KB)**, including
  104 tombstones
* a 500,000-row table with `--chunk-rows 200000` produced 3 parts, confirming
  peak memory is bounded by the chunk rather than the table

The chunking matters on `impressions`: 100 million rows cannot be buffered, so
the export streams through a server-side cursor and emits a part file per chunk.

## Not in this phase

Nothing reads Bronze yet. Phase 5 brings Spark in to reconcile the two paths by
primary key, and phase 6 writes the result to Iceberg as Silver.

Compaction is also absent. The sink writes one object per flush per partition,
so a busy day at a small flush interval leaves many small files — the classic
small-file problem. It has not bitten yet at these volumes, and the fix belongs
with the Spark work that will actually feel it.
