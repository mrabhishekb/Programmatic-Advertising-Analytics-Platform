# Programmatic Advertising Data Platform

An end-to-end data engineering project that simulates a programmatic advertising
ecosystem and moves it from an operational PostgreSQL database through CDC,
Kafka, S3, Spark, Iceberg and Snowflake into dimensional models and analytical
data products.

**Phase 1 is complete: the source database and a relationship-aware synthetic
data generator.** Later phases are listed at the bottom and are not implemented
yet.

---

## Why this project exists

Most portfolio data projects generate each table independently with random
values. The moment anyone runs a join, the illusion collapses:

```sql
SELECT COUNT(*) FROM impressions i JOIN campaigns c ON i.campaign_id = c.campaign_id;
-- 0
```

Everything here is built so that cannot happen. The dataset is generated as one
coherent advertising ecosystem: every foreign key is taken from a parent object
that already exists, every click is derived from a real impression, every
conversion from a real click, and every spend transaction from real delivery.

---

## Architecture

```
PostgreSQL → Debezium → Kafka → S3 Bronze → PySpark → Iceberg Silver
   → Snowflake → dimensional model → gold data products → Power BI
```

Airflow orchestrates the stages. See [docs/architecture.md](docs/architecture.md).

Phase 1 delivers the leftmost box: the operational source system and the data
that flows through everything to its right.

---

## Technology choices

| Choice | Why |
|---|---|
| **PostgreSQL 16** | Logical decoding makes it the natural CDC source; strong constraint support means the schema itself is a correctness layer. |
| **Python 3.11** | Type hints, dataclass `slots`, and `zip(..., strict=True)` catch whole classes of bug at the boundaries. |
| **psycopg 3 `COPY`** | An order of magnitude faster than multi-row `INSERT` at a million rows, while keeping foreign keys immediately enforced. |
| **YAML configuration** | Volumes, weight tables and compatibility matrices are data. Reshaping the ecosystem never requires a code change. |
| **Annotated SQL for data quality** | Checks stay readable on their own and get reused by the warehouse layers later, instead of being locked inside Python. |
| **Docker Compose** | One command to a reproducible database that is already configured for the CDC phase. |

---

## Data model

Eleven operational tables. Full detail in [docs/data_model.md](docs/data_model.md).

```
advertisers ─┬──< campaigns ──< line_items
             └──< creatives
publishers ──< placements
audiences
                  │
              impressions ──< clicks ──< conversions
                  └──────────────┴────────────┴──< spend_transactions
```

| Table | Grain |
|---|---|
| `advertisers` | one advertiser account |
| `campaigns` | one campaign, owned by one advertiser |
| `line_items` | one flight inside a campaign |
| `creatives` | one creative asset, owned by one advertiser |
| `publishers` | one supply-side property |
| `placements` | one ad slot on a publisher |
| `audiences` | one targetable segment |
| `impressions` | one served impression |
| `clicks` | one click, derived from one impression |
| `conversions` | one conversion, last-click attributed to one click |
| `spend_transactions` | one billable spend event (CPM hourly roll-up, or per click / per conversion) |

Prices are CPMs: the cost of a single impression is `clearing_price / 1000`.

---

## How relational consistency is guaranteed

Four independent mechanisms, described fully in
[docs/data_generation.md](docs/data_generation.md):

1. **The registry refuses orphans.** `Ecosystem.add_campaign`,
   `add_line_item`, `add_placement` and `add_creative` look the parent up and
   raise `RelationshipError` if it is missing. No code path can register a child
   without a parent.

2. **Events are derived from objects, not identifiers.** An impression is built
   by walking the graph - pick a campaign, then *its* line items, read the
   advertiser off the campaign, pick *that advertiser's* creatives, pick a
   placement compatible with that creative, read the publisher off the
   placement. A click is built from a `GeneratedImpression` object and copies its
   campaign, line item, advertiser and creative. A conversion is built from a
   `GeneratedClick`.

3. **In-process validation before any write.** Around 21 million assertions at
   the SMALL profile, covering referential, temporal and business rules. The
   first violation aborts the run.

4. **SQL validation after the load.** 50 annotated SQL checks plus the
   representative joins, each asserted to return a non-zero row count.

Plus the schema's own `FOREIGN KEY` and `CHECK` constraints, which would reject
bad rows even if all of the above were removed.

---

## Realistic distributions

The data is deliberately not uniform:

| Property | Shape |
|---|---|
| Campaigns per advertiser | Pareto x spend tier: most have a handful, a few have dozens |
| Impressions per campaign | heavy-tailed; the top 10% of campaigns carry a large majority of traffic |
| Impressions per publisher | heavy-tailed, inherited by each publisher's placements |
| CTR per campaign | log-normal, spanning an order of magnitude |
| CTR per creative / placement / device / audience | independent multipliers, so `creative_performance` is a real report |
| CVR per campaign | log-normal x objective (a CONVERSIONS campaign converts ~4x an AWARENESS one) |
| ROAS per campaign | log-normal around a configured median x the industry's ROAS index |
| Traffic by hour and weekday | diurnal curve and weekday/weekend seasonality |

Attribute combinations stay plausible because they are conditioned, not drawn
independently: a CTV publisher sells video inventory on CTV devices, a mobile app
sells banners and interstitials on phones reporting an in-app browser, an audio
creative only runs on inventory whose ad format is `AUDIO_INSTREAM`.

---

## Getting started

### Prerequisites

* Docker (for PostgreSQL)
* Python 3.11+ and [uv](https://docs.astral.sh/uv/) (or use your own venv tooling)

### Install

```bash
make setup          # creates .venv, installs the project, copies .env.example to .env
```

### Start PostgreSQL

```bash
make up             # starts postgres:16, applies postgres/schema.sql and postgres/seed.sql
```

The schema and reference data are applied automatically on first start. To start
over from an empty database:

```bash
make reset
```

### Generate data

```bash
make generate                        # SMALL profile, seed 42
make generate SCALE=tiny             # a few seconds, used by the tests
make generate SCALE=medium SEED=7    # any profile from config/scales.yml
make generate-csv                    # write CSV files instead of loading PostgreSQL
```

A `--target postgres` run truncates the source tables first, so it is repeatable:
the same seed always leaves the database in the same state.

### Validate

The data quality suite runs automatically at the end of a PostgreSQL run. To run
it on its own:

```bash
make validate
```

### Explore

```bash
make queries        # runs scripts/example_queries.sql
make psql           # interactive shell
```

### Test

```bash
make test           # unit, integrity, determinism and (if a database is up) integration tests
make lint
```

### Apply changes to the source tables

```bash
make changes        # realistic UPDATE/INSERT/DELETE traffic against existing rows
```

---

## Configuration

Nothing about dataset size or ecosystem shape is hard-coded.

`config/scales.yml` - row volumes:

| Profile | Advertisers | Campaigns | Line items | Creatives | Publishers | Placements | Audiences | Impressions | Clicks | Conversions |
|---|---|---|---|---|---|---|---|---|---|---|
| tiny | 25 | 150 | 350 | 200 | 40 | 180 | 80 | 25,000 | 900 | 120 |
| small | 100 | 1,000 | 3,000 | 5,000 | 500 | 2,000 | 1,000 | 1,000,000 | 30,000 | 3,000 |
| medium | 1,000 | 10,000 | 30,000 | 50,000 | 5,000 | 20,000 | 5,000 | 10,000,000 | 500,000 | 50,000 |
| large | 5,000 | 50,000 | 150,000 | 200,000 | 20,000 | 100,000 | 10,000 | 100,000,000 | 5,000,000 | 500,000 |

Click and conversion totals are hit **exactly**: they are allocated across
campaigns by each campaign's sampled CTR/CVR profile rather than sampled with a
coin flip, so the configured numbers hold while per-campaign rates still vary
widely.

`config/generation.yml` - behaviour: the simulation clock, how far back history
reaches, the event window, funnel spread, click and conversion delays,
attribution windows, pricing, spend roll-up mode and validation strictness.

`data_generator/reference/*.yml` - business vocabulary and every conditional
weight table (objective → bid strategy, publisher type → placement type, device
→ OS → browser, creative type → compatible inventory).

Environment: `ADTECH_SCALE`, `ADTECH_SEED` and the `POSTGRES_*` settings in
`.env`.

---

## Reproducibility

The generator is deterministic. Every random draw comes from a stream seeded with
`blake2b(master_seed || stream path)`, so:

* the same seed rebuilds a byte-identical dataset (asserted by
  `tests/test_determinism.py`);
* adding a new random stream never shifts the output of existing ones;
* any single campaign-day can be regenerated in isolation.

The simulation clock is a fixed date in `config/generation.yml` rather than
"today", because otherwise the same seed would produce different data tomorrow.

---

## Sample analytical queries

All ten live in [`scripts/example_queries.sql`](scripts/example_queries.sql). The
headline one from the brief:

```sql
SELECT
    a.advertiser_name,
    c.campaign_name,
    COUNT(DISTINCT i.impression_id)  AS impressions,
    COUNT(DISTINCT cl.click_id)      AS clicks,
    COUNT(DISTINCT cv.conversion_id) AS conversions,
    SUM(st.spend_amount)             AS spend
FROM advertisers a
JOIN campaigns c         ON a.advertiser_id = c.advertiser_id
JOIN impressions i       ON c.campaign_id = i.campaign_id
LEFT JOIN clicks cl      ON i.impression_id = cl.impression_id
LEFT JOIN conversions cv ON cl.click_id = cv.click_id
LEFT JOIN spend_transactions st ON i.impression_id = st.impression_id
GROUP BY a.advertiser_name, c.campaign_name;
```

Metric definitions used throughout, all division-by-zero safe (`NULLIF`):

```
CTR  = clicks / impressions
CVR  = conversions / clicks
CPM  = spend / impressions * 1000
CPC  = spend / clicks
CPA  = spend / conversions
ROAS = conversion_value / spend
```

---

## Project structure

```
├── config/                     scale profiles and behavioural configuration
├── data_generator/             the generator, one module per table
│   └── reference/              business vocabulary and weight tables
├── data_quality/
│   └── tests/                  annotated SQL checks
├── postgres/                   schema.sql, seed.sql (generated), indexes.sql
├── scripts/                    example queries, seed renderer
├── tests/                      unit, integrity, determinism, integration
├── docs/                       architecture, data model, generation, data quality
├── docker-compose.yml
└── Makefile
```

---

## Development phases

| # | Phase | Status |
|---|---|---|
| 1 | Project setup, PostgreSQL, realistic synthetic data generator | **complete** |
| 2 | CDC + Debezium | next |
| 3 | Kafka | |
| 4 | S3 Bronze | |
| 5 | Spark ingestion | |
| 6 | Iceberg Silver | |
| 7 | Incremental processing | |
| 8 | Data quality | source-layer checks complete |
| 9 | SCD Type 1 | |
| 10 | SCD Type 2 | |
| 11 | Snowflake | |
| 12 | Dimensional modelling | |
| 13 | Gold data products | |
| 14 | Attribution | |
| 15 | Airflow orchestration | |
| 16 | Late-arriving events | |
| 17 | Schema evolution | |
| 18 | Failure recovery | |
| 19 | Monitoring, documentation, dashboard | |
