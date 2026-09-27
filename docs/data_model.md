# Source data model

The operational schema in `postgres/schema.sql` is the system of record that
every later layer reads from. Eleven business tables plus two reference tables
and two bookkeeping tables.

## Entity relationships

```
advertisers ─┬──< campaigns ──< line_items
             │        │              │
             └──< creatives          │
                      │              │
publishers ──< placements            │
                      │              │
audiences             │              │
    │                 │              │
    └────────┬────────┴──────────────┘
             │
         impressions ──< clicks ──< conversions
             │              │            │
             └──────────────┴────────────┴──< spend_transactions
```

`──<` reads as "one to many".

## Grain of every table

| Table | Grain |
|---|---|
| `advertisers` | one advertiser (brand) account |
| `campaigns` | one campaign, owned by exactly one advertiser |
| `line_items` | one line item (flight) inside a campaign |
| `creatives` | one creative asset, owned by one advertiser |
| `publishers` | one supply-side property (site, app, CTV or audio) |
| `placements` | one ad slot offered by a publisher |
| `audiences` | one targetable audience segment |
| `impressions` | one served ad impression |
| `clicks` | one click, derived from exactly one impression |
| `conversions` | one conversion, attributed last-click to exactly one click |
| `spend_transactions` | one billable spend event (see below) |
| `ref_country` | one supported country, and the currency it bills in |
| `ref_industry` | one advertiser industry vertical |
| `platform.generation_run` | one execution of the data generator |
| `platform.data_quality_result` | one data quality check execution |

`spend_transactions` has a grain that depends on how the campaign is billed:

* **CPM** - one row per (campaign, line item, publisher, hour). `impression_id`
  is NULL because the row aggregates many impressions.
* **CPC** - one row per click, `impression_id` populated.
* **CPA** - one row per conversion, `impression_id` populated.

A `CHECK` constraint enforces that `impression_id` is NULL exactly when the
billing type is CPM, so the two grains can never be confused downstream.

## Units and conventions

| Column | Unit |
|---|---|
| `impressions.bid_price`, `impressions.clearing_price` | CPM - cost per **1000** impressions |
| `placements.floor_price`, `line_items.target_cpm` | CPM |
| `line_items.bid_amount` | the unit the campaign is billed in: CPM, CPC or CPA |
| `spend_transactions.spend_amount` | absolute currency amount |
| `impressions.viewability_score` | fraction in [0, 1] |
| all timestamps | naive local time on a single simulation clock |

The cost of one impression is `clearing_price / 1000`. Every currency column is
the advertiser's billing currency along the demand chain, and the publisher's
country currency on `placements`.

## Design decisions

**UUID primary keys, supplied by the application.** The generator, and later
Debezium and Spark, all need a stable business key that does not depend on a
database sequence. It also means a row can be regenerated deterministically.

**Enumerations as `CHECK` constraints, not PostgreSQL `ENUM` types.** A CHECK
constraint survives the trip through Debezium, Kafka, Spark, Iceberg and
Snowflake as a plain string, and its allowed set can be widened without a table
rewrite. PostgreSQL `ENUM` types would have to be mirrored in four other systems.

**`created_at` / `updated_at` written by the generator, not by database
defaults.** The history has to be reproducible: `DEFAULT now()` would stamp every
row with the load time and destroy the three years of account history the data
depends on.

**Event tables are append-only, master tables are mutable.** This split is what
the rest of the platform is built around: master tables are the CDC surface and
become slowly-changing dimensions; event tables become facts.

**`REPLICA IDENTITY FULL` on the seven mutable tables.** PostgreSQL only writes
the full pre-image of an UPDATE into the WAL when replica identity is FULL, and
that pre-image is what a CDC reader needs to emit a populated `before` payload.
Setting it now means the source database never needs reconfiguring later. It is
deliberately *not* set on the append-only event tables, where the extra WAL
volume would buy nothing.

**Secondary indexes on the event tables live in `postgres/indexes.sql`, applied
after the bulk load.** Maintaining nine B-trees during a multi-million row COPY
is far more expensive than building them once at the end. `schema.sql` keeps the
cheap master-table indexes, so a fresh container is immediately usable.

**Foreign keys stay immediately enforced during the load.** The generator's
emitter flushes tables in dependency order inside one transaction, so a parent
row is always visible before its children are copied. Deferring the constraints
would have surfaced problems as one giant failure at COMMIT instead of at the
offending row.
