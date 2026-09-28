# Change data capture

Phase 2 streams every change made to the mutable source tables into Kafka, with
the old and new values side by side. Nothing polls the database: Debezium reads
PostgreSQL's write-ahead log directly.

## What runs

```
PostgreSQL ──WAL──> Debezium (Kafka Connect) ──> Kafka topics
   │                                               cdc.public.advertisers
   │                                               cdc.public.campaigns
   └── scripts/export_snapshot.py ──> out/snapshot/*.csv.gz   (the starting state)
```

| Service | Port | Purpose |
|---|---|---|
| `kafka` | 29092 (host) | Holds the change events. Single-node KRaft, no ZooKeeper. |
| `kafka-connect` | 8083 | Runs the Debezium PostgreSQL connector. |
| `kafka-ui` | 8080 | Browse topics and messages in a browser. |

## The backfill + CDC tail pattern

The database already held 100 million rows before CDC existed. Debezium's default
behaviour is to read every one of them before it starts streaming, which would
take hours and produce nothing useful - impressions are never updated after they
are written.

So the work is split in two:

**Job A - bulk export** (`scripts/export_snapshot.py`). One `COPY` per table
straight out of PostgreSQL into gzipped CSV. This is the starting state.

**Job B - CDC tail** (Debezium with `snapshot.mode: no_data`). Only changes from
the moment the replication slot was created.

### Order matters, and the code enforces it

Register the connector **first**, then export. Not the other way round.

Creating the connector creates a replication slot, which is a bookmark in the
write-ahead log. From that instant PostgreSQL retains every change until the
consumer confirms it has read them.

Export first and register second, and you get a hole:

```
10:00  export starts
10:15  export reads campaign X          -> file says budget = 5000
10:30  someone changes campaign X       -> budget becomes 7500
12:00  export finishes
12:01  Debezium starts                  -> never saw the 10:30 change
```

The file says 5000 forever, and nothing ever reports that it is wrong.

Register first and the worst case is an *overlap*: a row appears both in the
export and as a change event. That is harmless, because every downstream load
merges on primary key rather than inserting blindly - applying the same event
twice produces the same result.

`export_snapshot.py` refuses to run when the slot is missing, so the rule is
structural rather than a line in a runbook:

```
error: Replication slot 'adtech_cdc_slot' does not exist.
Register the Debezium connector first:  make cdc-register
Exporting before the slot exists would silently lose any change made
between now and when CDC starts.
```

### The export is one consistent read

All tables are read inside a single REPEATABLE READ transaction, so every file
describes the database as of the same instant rather than drifting apart over the
hours a large export takes. The manifest records the WAL position of that
instant, which tells the downstream loader which change events the files already
contain.

## What is captured, and what is not

Only the seven tables that change:

```
advertisers  campaigns  line_items  creatives  publishers  placements  audiences
```

The four event tables - impressions, clicks, conversions, spend_transactions -
are append-only. A row is written once and never touched again, so there are no
updates or deletes for CDC to capture. They travel through Job A only. A test
asserts no `cdc.public.impressions` topic exists, so this stays true.

## Configuration decisions

Everything lives in `debezium/connector.json`. The settings worth explaining:

**`snapshot.mode: no_data`** - skip the existing rows. The whole point of the
pattern above.

**`plugin.name: pgoutput`** - the logical decoding plugin built into PostgreSQL
10+, so no extension has to be installed in the database.

**`decimal.handling.mode: string`** - by default Debezium encodes `DECIMAL` as
base64-wrapped bytes, which silently mangles money for anyone reading the JSON.
As a string, `192.56` arrives as `"192.56"` exactly.

**`provide.transaction.metadata: true`** - each event carries the transaction it
belonged to and its position within it, and a separate `cdc.transaction` topic
records transaction boundaries. That is what lets a downstream consumer apply a
multi-row change atomically.

**`tombstones.on.delete: true`** - after a delete, a second message with a null
value is written for the same key. Kafka log compaction needs that marker to
actually drop the key.

**`publication.autocreate.mode: filtered`** - PostgreSQL publishes only the seven
captured tables rather than everything.

**`heartbeat.interval.ms` + `heartbeat.action.query`** - see below.

## The heartbeat, and why it is not optional

A replication slot releases write-ahead log only once the consumer confirms how
far it has read, and it can only confirm a position it has actually seen.

If the captured tables sit idle while the rest of the database stays busy - which
is exactly this project, where 100 million impressions are being written and
seven small tables rarely change - the slot never advances. WAL accumulates
until the disk fills and PostgreSQL stops accepting writes.

The fix is a single-row table, `platform.cdc_heartbeat`, that the connector
updates every 10 seconds. That keeps a trickle of activity flowing so the slot
always has something to confirm. The table sits in the `platform` schema, outside
the capture set, so it does not generate change events about itself.

Monitor it with:

```bash
make cdc-slots
```

```
Replication slots
  adtech_cdc_slot  [pgoutput/logical]  active
    restart_lsn=BE/94BDAC88  confirmed_flush_lsn=BE/94BDACC0
    WAL retained: 248 bytes

Publications
  adtech_cdc_pub  (filtered to the captured tables)

Heartbeat: 4 beats, last one 6s ago
```

`WAL retained` is the number to watch. A few hundred bytes is healthy. Gigabytes
means the slot has stopped advancing and something is wrong.

**Deleting a connector does not delete its slot.** A forgotten slot keeps
retaining WAL forever with nothing consuming it. `make cdc-delete` drops both.

## What an event looks like

```
[17:23:48] UPDATE        public.campaigns       lsn=818539353224 tx=888
      campaign_status: 'ACTIVE' -> 'PAUSED'
      updated_at: 2026-08-14 16:44:08 -> 2026-09-27 22:53:45
      transaction 888:818539353224 event #4
```

The underlying message is Debezium's standard envelope:

```json
{
  "before": { "campaign_id": "...", "daily_budget": "192.56", ... },
  "after":  { "campaign_id": "...", "daily_budget": "272.09", ... },
  "op": "u",
  "ts_ms": 1790549625218,
  "source": { "db": "adtech", "schema": "public", "table": "campaigns",
              "lsn": 818539667648, "txId": 888, "ts_ms": ... },
  "transaction": { "id": "888:818539667648", "total_order": 17 }
}
```

`op` is `c` for insert, `u` for update, `d` for delete, `r` for a snapshot read.

The `before` block is only populated because the source tables run with
`REPLICA IDENTITY FULL`, set in `postgres/schema.sql` during phase 1. Without it
PostgreSQL writes only the primary key of the old row into the WAL, and `before`
would arrive nearly empty - which would make slowly-changing dimensions in phase
10 impossible to build.

## Known rough edge: timestamps arrive as integers

The converters run with `schemas.enable: false`, which keeps messages small and
readable. The cost is that logical type information is dropped, so a `TIMESTAMP`
column arrives as raw epoch milliseconds rather than a formatted date. Consumers
have to know the column types from elsewhere.

`scripts/cdc.py` works around it for display by reading the column name, but that
is a cosmetic fix. The production answer is Avro with a schema registry, which
carries the types with the data and is a natural addition when phase 17 tackles
schema evolution.

## Running it

```bash
make cdc-up          # start Kafka, Connect and the UI
make cdc-register    # register the connector (creates the replication slot)
make export-snapshot # bulk export the existing rows
make changes         # apply real UPDATE/INSERT/DELETE traffic
make cdc-watch       # watch the events arrive
```

Useful along the way:

```bash
make cdc-status      # is the connector healthy?
make cdc-slots       # is the slot advancing?
make cdc-topics      # how many messages per topic?
make cdc-delete      # remove the connector and drop its slot
```

The Kafka UI at http://localhost:8080 shows the same topics in a browser.

## Verified behaviour

`tests/test_cdc_config.py` checks the connector configuration without needing
Kafka: the capture list matches the mutable tables exactly, event tables are
excluded, `snapshot.mode` is `no_data`, the heartbeat is configured and points
outside the capture set, and decimals are handled as strings.

`tests/test_cdc_integration.py` runs against the live stack and asserts that an
update arrives with both `before` and `after` populated, that inserts and deletes
are captured, that source and transaction metadata are present, that money
survives exactly, that no event-table topics exist, that the slot is active and
its WAL retention bounded, that the heartbeat is ticking, and that the export
refuses to run without a slot.

Measured on the loaded 100-million-row database: 48 changes produced 51 messages
across 6 topics within a second, and the master-table export wrote 535,002 rows
to 27 MB of gzipped CSV in 2.4 seconds.

## Not in this phase

Kafka was configured minimally here - one broker, three partitions per topic,
seven-day deletion for everything. Partitioning, retention and throughput are
handled properly in phase 3; see [docs/kafka.md](kafka.md), which is also where
the dimension topics switch from deletion to compaction. Nothing is written to
S3 yet (phase 4) and nothing consumes the topics with Spark yet (phase 5).
