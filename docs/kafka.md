# Kafka topic configuration

Phase 2 got change events into Kafka. It did not decide what should happen to
them once they were there.

Every topic Debezium created came from one template in the connector -
3 partitions, delete after 7 days, replication factor 1, for everything. Phase 3
replaces that single default with a decision per topic, writes those decisions
down in `config/kafka.yml`, and measures the throughput assumptions instead of
guessing them.

## The problem with one default

The same template was applied to three kinds of topic that want opposite things:

| Topic | What the default did | What it should do |
|---|---|---|
| `cdc.public.*` | delete change history after 7 days | keep the latest state of every entity forever |
| `cdc.transaction` | delete after 7 days | delete after 7 days (correct by accident) |
| `__debezium-heartbeat.cdc` | keep 8,640 messages/day for a week | discard within the hour |

The first row is the expensive one. A dimension topic on 7-day deletion tells a
new consumer only about the entities that happened to change in the last week.
Every campaign that has been stable for a month simply isn't in the topic.

## Retention

`config/kafka.yml` names three policies and assigns each topic to one, so the
intent is readable without decoding six Kafka settings per topic.

### `state` - the seven dimension topics

```yaml
cleanup.policy: compact
min.compaction.lag.ms: 604800000
delete.retention.ms: 86400000
segment.ms: 86400000
min.cleanable.dirty.ratio: 0.1
retention.ms: -1
```

Compaction keeps the newest message per key **forever**, so replaying the topic
from offset 0 rebuilds the current state of every advertiser, campaign, line
item, creative, publisher, placement and audience. That is what makes the topic
a usable source rather than a rolling window.

The obvious objection is that compaction destroys history, and phase 10 needs
history to build slowly-changing dimensions. That is what
**`min.compaction.lag.ms`** is for: no message becomes eligible for compaction
until it is 7 days old. So the topic holds every individual revision for a week,
and only the newest value survives beyond that. The Bronze layer in phase 4
archives the full history immutably within that window; Kafka is transport, not
the system of record.

`delete.retention.ms` is the one to watch operationally. It bounds how long a
tombstone survives, so a consumer that goes dark for more than 24 hours can come
back, miss the tombstone, and keep a row that was deleted. That is the real
limit on how stale a consumer may be.

`segment.ms` exists because compaction only ever runs on **closed** segments.
These topics are quiet - the busiest has seen 75 messages - so without a forced
daily roll the active segment would stay open for months and the cleaner would
never run at all.

`retention.ms: -1` disarms a trap left over from phase 2. These topics were
created under `cleanup.policy: delete` with a 7-day retention, and switching the
policy to `compact` does not remove that setting - it only makes Kafka stop
consulting it. Anyone later moving a topic to `compact,delete` would silently
re-enable time-based deletion, including of the newest value of a key, which is
exactly what compaction was adopted to prevent. Setting it to "never" makes the
intent explicit rather than leaving a live number that happens to be ignored.

### `transient` - `cdc.transaction`

Deliberately **not** compacted. Debezium writes a BEGIN and an END record per
transaction under the same key. Compaction keeps only the newest value per key,
so every BEGIN would be silently discarded and a consumer would lose the ability
to tell where a transaction started - defeating the point of
`provide.transaction.metadata`.

### `heartbeat` - `__debezium-heartbeat.cdc`

Retention drops from 7 days to 1 hour. At one beat every 10 seconds this topic
accumulates 8,640 messages a day that nothing ever reads twice; it exists purely
so the replication slot has something to advance against (see
[docs/cdc.md](cdc.md)).

## Partitioning

**The key stays the primary key.** Debezium already keys each message by the
row's primary key, and Kafka guarantees ordering within a partition. So every
change to one campaign lands on one partition and arrives in the order it
happened. Nothing about that needed changing, and changing it would break
ordering per entity.

**The count stays at 3, deliberately.** All seven dimension topics are low
churn, and 3 partitions already allow three consumers to work in parallel. There
is no measurement that justifies more.

That restraint matters because partition count is close to a one-way door:

- Kafka cannot **reduce** partitions at all. The only route down is deleting and
  recreating the topic, which discards its messages.
- Increasing them rehashes key → partition, so a key's history is split across
  its old and new partition, and per-key ordering no longer holds across the
  boundary.

`kafka_admin.py apply` reflects both facts. It will grow a topic when the
declared count is higher, printing a warning about the rehash first, and it
refuses to shrink one, reporting the mismatch instead.

The other half of the change is that topics are now created **explicitly**
rather than lazily. Debezium creates a topic on the first change event, so
before this phase, `cdc.public.advertisers` and `cdc.public.placements` did not
exist at all - nothing had edited those tables yet - and their settings would
have depended on whenever something first did. `make kafka-apply` creates all
ten up front.

## Throughput

`make kafka-bench` produces a realistically-shaped Debezium envelope (1,151
bytes, the same structure as a real `campaigns` update) and measures the
producer, isolated from PostgreSQL.

Measured on the local single-broker stack, 50,000 messages per codec:

| compression | msgs/s | MB/s | p50 ms | p99 ms |
|---|---|---|---|---|
| none | 117,425 | 135.16 | 136.48 | 297.34 |
| gzip | 98,165 | 112.99 | 202.39 | 395.60 |
| snappy | 102,801 | 118.32 | 217.18 | 381.33 |
| **lz4** | **376,833** | **433.74** | **16.65** | **36.31** |
| zstd | 382,432 | 440.18 | 20.27 | 31.06 |

lz4 and zstd are roughly **3.2x faster than sending uncompressed**, and cut p99
latency by an order of magnitude. That is not the usual compression trade - it
happens because a verbose JSON CDC envelope compresses extremely well, so the
broker spends far less time on network and disk, and the CPU cost of lz4 is
small enough to disappear next to the saving.

gzip and snappy are both *slower* than no compression here: gzip's CPU cost
exceeds the saving, and snappy's weaker ratio does not buy back enough I/O.

The connector now sets `producer.override.compression.type: lz4`. zstd measured
marginally faster on throughput but the two are within noise of each other, and
lz4 costs less CPU on the broker when decompressing for consumers.

**This number is the broker's ceiling, not the pipeline's.** Debezium runs
`tasks.max: 1` against a single-threaded logical replication slot, so end-to-end
CDC throughput is bounded well below 376k msgs/s. The benchmark answers "can
Kafka keep up", and the answer is comfortably yes.

## Consumer lag

```bash
make kafka-lag
```

```
check-df420dbc-7fc4-487c-8f62-e7166c0ac0c0
  cdc.public.campaigns[0]  committed=17  end=24  lag=7
  cdc.public.campaigns[1]  committed=23  end=29  lag=6
  cdc.public.campaigns[2]  committed=22  end=22  lag=0
  total lag: 13
```

Lag is the gap between the newest offset in a partition and the offset a group
has committed. It is the signal that a consumer has stopped keeping up, and it
matters more than it looks: a consumer whose lag grows past `delete.retention.ms`
starts missing tombstones, and one that falls behind a `delete` topic's
retention loses messages outright.

`make kafka-lag` exits non-zero above a threshold (default 10,000), so it can be
used as a health gate. Connect's own internal groups are listed but excluded
from the total, because their lag is not meaningful.

## Where the broker actually stores data

`docker-compose.yml` declares a named volume for Kafka, but declaring one is
not enough. The `apache/kafka` image ships `log.dirs=/tmp/kraft-combined-logs`
in its own `config/kraft/server.properties`, so without an explicit override the
broker writes to the container's writable layer and ignores the volume entirely.

The failure mode that produces is quiet and delayed. Topics survive a restart,
because the container still exists - so everything looks fine. But `make down`
removes the container, and with it every topic, every message, and Connect's
`_connect_configs`. Losing that last one deregisters the Debezium connector,
which leaves the replication slot **inactive**: still present, still pinning
write-ahead log, with nothing consuming it. That is the exact condition
[docs/cdc.md](cdc.md) warns about, arrived at without anyone doing anything
wrong.

`KAFKA_LOG_DIRS: /var/lib/kafka/data` points the broker at the mounted volume,
and `tests/test_kafka_config.py` asserts the two paths agree so the mount cannot
silently drift away from the setting again.

## One broker

`replication_factor: 1`, because `docker-compose.yml` runs a single broker.
Losing it loses the data - there is no redundancy here at all. That is an
acceptable trade for a laptop, but it is a trade, not a default worth carrying
forward.

Production would change four things together:

| Setting | Local | Production |
|---|---|---|
| brokers | 1 | 3+ |
| `replication_factor` | 1 | 3 |
| `min.insync.replicas` | n/a | 2 |
| producer `acks` | 1 | `all` |

Those four only work as a set. `acks=all` with `replication_factor: 1` still
loses data on a broker failure, and `replication_factor: 3` with `acks=1`
acknowledges writes the followers have not seen yet.

## Running it

```bash
make kafka-describe   # what differs between the cluster and config/kafka.yml
make kafka-plan       # what apply would change
make kafka-apply      # create missing topics, fix retention and compaction
make kafka-lag        # is anything falling behind?
make kafka-bench      # producer throughput across every codec
```

`describe` exits non-zero when anything has drifted, so it works as a check:

```
topic                          partitions  cleanup.policy  drift
--------------------------------------------------------------------------
cdc.public.advertisers           3/3       compact         ok
cdc.public.campaigns             3/3       compact         ok
...
cdc.transaction                  3/3       delete          ok
__debezium-heartbeat.cdc         3/3       delete          ok

Every declared topic matches its specification.
```

## Two places, kept in agreement

Topics get their settings from whichever of these reaches them first:

- **`config/kafka.yml`**, applied by `make kafka-apply`, for topics that already
  exist or can be created up front.
- **`topic.creation.*`** in `debezium/connector.json`, for a topic Debezium
  creates on a change event before anyone ran `kafka-apply`.

The connector now carries three groups rather than one blanket default: the
`default` group is compacted like the dimension topics, and `transaction` and
`heartbeat` groups override it back to deletion for the two topics that need it.

`tests/test_kafka_config.py` asserts the two sources agree. Without that check,
a topic's configuration would depend on the order in which things happened to
run, which is the drift this phase exists to remove.

## Verified behaviour

`tests/test_kafka_config.py` runs without a broker and checks that every
captured table has a declared topic and no event table does; that dimension
topics are compacted with a history window of at least 7 days and a tombstone
window of at least a day; that `cdc.transaction` is *not* compacted; that the
heartbeat is retained far more briefly than the change topics; that the
replication factor matches the single-broker stack; that an unknown policy name
is rejected rather than silently ignored; and that the connector's
`topic.creation` groups agree with the YAML.

## Not in this phase

Serialization is unchanged - still JSON with `schemas.enable: false`, so
timestamps arrive as epoch integers. Avro with a schema registry is the real
fix and belongs with phase 17, which is where schema evolution is handled.

Nothing yet writes these topics to S3 (phase 4) or reads them with Spark
(phase 5). The lag command is the health signal both of those will be measured
against.
