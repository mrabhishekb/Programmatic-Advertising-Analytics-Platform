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

The sink runs itself. `bronze-sink` is part of the default stack, so `make up`
starts it and `restart: unless-stopped` brings it back after a reboot - the same
arrangement the change-traffic container uses, minus the profile, because this
one only reads Kafka and writes object storage and has no source state it could
damage.

It is not behind a profile for that reason, and because without it the pipeline
would be continuous as far as Kafka and manual from there, which is a strange
place to stop.

```bash
make bronze-logs              # follow the sink container
make bronze-restart           # rebuild and restart it
make bronze-ls                # what is in the bucket, by table and day
make bronze-peek              # read change events back out
```

The snapshot is still a job you run:

```bash
make export-snapshot          # the rows that already exist -> Bronze
make export-snapshot-master   # same, skipping the 100M-row event tables
```

`make bronze-sink` still exists for draining by hand, but the container shares
its consumer group, so it will usually report nothing to consume - the
container has already read it.

Three settings matter to the container, and all three are things that look fine
and fail at runtime if wrong:

* `--bootstrap kafka:9092`, the **internal** listener. 29092 is only advertised
  to the host.
* `S3_ENDPOINT=http://minio:9000`, the port MinIO actually serves on. The 9010
  host mapping does not exist inside the network, and `localhost` is the
  container itself.
* `depends_on: minio-init: service_completed_successfully`. The sink calls
  `head_bucket` at startup and exits if the bucket is missing, so waiting for
  MinIO to be healthy is not enough - the one-shot init container must have
  finished creating it.

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

## Flush interval, and why it is 5 minutes

A flush happens on whichever comes first: `--max-records` (5,000) or
`--max-seconds` (300). Every flush writes **one object per (table, day, Kafka
partition)** present in the buffer, which is what makes the time trigger, not
the record trigger, decide how small a file can be.

Dimension changes arrive at human rates — tens per minute — so 5,000 records is
several hours of traffic and that trigger effectively never fires. The timer is
always in charge. At the 30 seconds this started with, a single 20-change batch
spread over five tables and three partitions produced about ten objects holding
one to three records each. A single-record Parquet file measures around 4 KB,
nearly all of it footer and schema metadata.

| `--max-seconds` | flush cycles/day | objects/day (approx) |
|---|---|---|
| 30 | 2,880 | ~14,000 |
| 300 | 288 | ~1,400 |
| 900 | 96 | ~500 |

Five minutes is the default because Bronze is read by batch Spark in phase 5,
so latency nobody downstream notices buys roughly a tenth of the object count.
`make bronze-sink-forever` goes further and uses 900, since a process running
all day is exactly the case where object count compounds. The record trigger
still caps file size if change volume ever spikes.

Buffering longer is safe: the sink commits Kafka offsets only after a
successful write, so records held in the buffer when a process dies are simply
re-read on restart.

## Not in this phase

Phase 6 writes the reconciled result to Iceberg as Silver. Reading Bronze is now
phase 5's job - see [docs/spark.md](spark.md).

Tuning the flush interval mitigates the small-file problem but does not solve
it: the per-(table, day, partition) fan-out means even a long window is divided
fifteen ways. The real fix is a compaction job that rewrites a day's small
objects into a few large ones, and that arrived with phase 5 as
`make silver-compact` - 451 objects merged into 60 on the first run. It is safe
against the immutability promise because a merged object is named after the
offset range it covers, which is exactly the name the sink would have written
had it flushed once instead of many times.
