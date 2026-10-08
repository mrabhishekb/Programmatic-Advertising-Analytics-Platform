# The warehouse: Snowflake, and dbt on top of it

Phases 1 to 8 built a lakehouse that runs entirely on a laptop. This is the
part that does not, and that is worth stating plainly before anything else:
**Snowflake cannot run locally.** There is no self-hosted edition. Everything
from here needs an account.

The project is arranged so that costs as little as possible. The lakehouse path
through phase 8 stays complete and reproducible on its own; the warehouse is a
layer on top that announces its absence rather than breaking.

```bash
make validate-silver     # works with no Snowflake account at all
make warehouse-load      # "Snowflake is not configured: ... Add them to .env"
```

## Setting it up

```bash
make warehouse-bootstrap   # once: role, warehouse, database, schemas, stage
make warehouse-export      # Silver -> Parquet in MinIO
make warehouse-load        # Parquet -> Snowflake RAW
make dbt-build             # RAW -> STAGING -> CORE, running the tests as it goes
```

`make warehouse-plan` prints the bootstrap SQL with your account's object names
filled in and connects to nothing, which is the safe way to see what the first
command is about to do.

A first run against a trial account takes about a minute in total: the bootstrap
is 22 statements, the load moves 535,585 rows in roughly 30 seconds, and dbt
builds 7 views and 7 tables and runs 107 tests in about 11.

### Pointing at objects that already exist

A trial account arrives with `COMPUTE_WH` and gives you `ACCOUNTADMIN`, and
setting `SNOWFLAKE_ROLE`/`SNOWFLAKE_WAREHOUSE` to those works. The bootstrap
handles both cases rather than assuming its own names:

- **System roles are neither created nor granted.** `CREATE ROLE IF NOT EXISTS
  ACCOUNTADMIN` is an error, and granting it is a privilege change this script
  has no business making.
- **Warehouse settings are applied with an explicit `ALTER`.** `CREATE WAREHOUSE
  IF NOT EXISTS` does nothing at all to a warehouse that already exists, so
  without the `ALTER` an existing `COMPUTE_WH` keeps its original
  `AUTO_SUSPEND` - usually 600, which bills ten idle minutes after every
  two-minute load. On a trial that is the largest credit leak there is.

A dedicated `ADTECH_ENGINEER` role is still the better setup, and is what the
defaults in `.env.example` describe.

### Credentials

All of it comes from `.env`; see the Snowflake section of `.env.example`.
Use key-pair authentication if you can. Snowflake blocks password-only sign-in
for programmatic users unless an authentication policy explicitly allows it, so
a password works during setup and then stops working at an inconvenient moment.
`warehouse/settings.py` prefers a key whenever `SNOWFLAKE_PRIVATE_KEY_PATH` is
set, and the Makefile points dbt at the matching profile target.

Nothing secret is committed. `profiles.yml` lives in the repository rather than
`~/.dbt/` because every value in it is an `env_var()` call, which is asserted by
a test rather than left as a convention.

## Why there is a copy step

Snowflake can query Iceberg directly, which would be the elegant answer here.
It needs an external volume on real S3, Azure or GCS, and the warehouse in this
project is MinIO on a laptop with no route in from the outside. So the lakehouse
exports and the warehouse loads.

```
Iceberg Silver ──(Spark)──> Parquet in MinIO ──(boto3)──> local temp
                                                               │
                                                            PUT │
                                                               ▼
                                              Snowflake internal stage
                                                               │
                                                       COPY INTO │
                                                               ▼
                                                           RAW tables
```

Three hops looks like a lot, and an external stage would remove the middle one.
It would also mean provisioning cloud storage, which is a large amount of setup
to avoid copying a few gigabytes.

The copy buys something back. The export is *published*: written once, immutable
afterwards, and never rewritten while Snowflake is reading it. A load sees a
complete export or none at all, which would not be true of a warehouse reading
tables that incremental merges are rewriting underneath it.

## RAW is a landing copy, and nothing else

Every Silver column arrives, including `is_deleted`, `deleted_at` and the CDC
lineage columns. Nothing is filtered and nothing is renamed, because deciding
what to keep is staging's job and doing it during the load would push a
transformation below the layer that is supposed to be a faithful copy.

RAW is **replaced** on each load rather than merged into. Incremental loading
belongs above this layer, where dbt can see the change metadata and decide what
it means. A landing schema that is merged into is a landing schema with history
in it, which is a quietly different thing from a copy of the source.

### Types are declared, not inferred

Snowflake will infer a table from Parquet with `INFER_SCHEMA`, in one statement
instead of the type map in `warehouse/load.py`. It also reads decimals as
whatever precision survived the format conversions, and this dataset's money
columns are decimals on purpose - `daily_budget` has been `NUMBER(14,2)` since
`postgres/schema.sql`, and the point of carrying it through Debezium, Parquet
and Iceberg without loss is wasted if the warehouse turns it into a float at the
last step.

So the DDL is generated from the exported Parquet's own schema, with an explicit
Arrow-to-Snowflake mapping. The mapping is deliberately small: an unmapped type
raises rather than guesses, because the guess that gets made silently is the one
that costs you.

The schema still comes from the *file* rather than from `spark/schemas.py`, so a
column added in PostgreSQL flows all the way to RAW with no edit here.

### Parquet logical types, and why the COPY says so explicitly

`FILE_FORMAT = (TYPE = PARQUET, USE_LOGICAL_TYPE = TRUE)`. That option defaults
to FALSE, and the default is wrong for anything with a timestamp in it.

Spark writes timestamps as an int64 annotated `TIMESTAMP(MICROS)`. With logical
types off, Snowflake ignores the annotation and reads the integer as epoch
*seconds*, so a 2026 timestamp arrives as **the year 54,934,202**. Dates
degrade the same way.

What makes this worth a section is how it fails. The `COPY` succeeds. The row
count matches the manifest exactly. `dbt build` reports `PASS=65 ERROR=0`,
because none of those tests look at a date. The damage appears only when a
human selects the column and the client refuses to convert the value.

### The load checks itself

The export writes a manifest of row counts; the loader compares what landed
against it and reports a mismatch per table. Cross-system row counts are the
cheapest real check available, and they catch a `COPY` that silently skipped a
file.

They did not catch the timestamp bug above, which is the useful lesson: a row
count proves every row arrived, not that any of them mean what they should. So
the loader also samples each table and checks that every `DATE` and `TIMESTAMP`
column falls within `PLAUSIBLE_YEARS`, failing the load if not.

Sampling rather than scanning is deliberate - a file format option applies to
every row or none, so 1,000 rows catch it as reliably as 100,000,000 would, and
the full scan would only cost credits to learn the same thing.

The guard was confirmed by removing `USE_LOGICAL_TYPE` and watching it fire:

```
advertisers: CREATED_AT, UPDATED_AT loaded outside 2000-2100.
The COPY read the timestamp encoding wrong - check that FILE_FORMAT still
sets USE_LOGICAL_TYPE = TRUE.
```

## The dbt project

Lives in `warehouse/dbt`.

| Layer | Schema | Materialisation | Built in |
|---|---|---|---|
| sources | `RAW` | loaded by `warehouse.load` | phase 9 |
| staging | `STAGING` | view | phase 9 |
| core | `CORE` | table | phases 10-12 |
| analytics | `ANALYTICS` | table | phase 13 |

Staging models are views because staging exists to rename and type. Storing the
result would keep a second copy of RAW to save work that costs nothing. Core is
tables, because everything above it joins and aggregates those models and
recomputing seven unions per query is work paid repeatedly to save storage that
costs nothing.

Each staging model lists its columns rather than selecting `*`, which is
asserted by a test. A column appearing in RAW should not propagate silently
through the whole warehouse before anyone has decided what it means.

`macros/silver_lineage.sql` holds `silver_lineage_columns()`, which renames the
CDC columns, so seven staging models do not each keep their own copy.

### The schema name override

`macros/generate_schema_name.sql` makes dbt use a configured schema as written.
Without it, dbt builds `<target.schema>_<custom schema>`, so models configured
for `STAGING` against a profile whose schema is also `STAGING` land in
`STAGING_STAGING`, beside four correctly-named schemas that stay empty.

This happened on the first run here, and the reason it is worth a section is
that it **fails quietly**: dbt reported `PASS=65 ERROR=0`, the tests genuinely
ran and genuinely passed, and the only symptom was that nothing else could find
the tables. A green dbt run does not mean the models are where you think.

dbt's default is not arbitrary - prefixing with the developer's target schema is
what stops several people sharing one warehouse from overwriting each other.
That is worth giving up here: this project owns its database outright, and the
four layer names appear in `bootstrap.sql`, in this document and in the
architecture diagram. Schemas that depended on who ran dbt last would
contradict all three.

### Tests

107 of them across 14 models. Almost all come free from YAML: `unique` and
`not_null` on every primary key, `relationships` for every foreign key, and
`accepted_values` mirroring the `CHECK` constraints in `postgres/schema.sql`.

That duplication is deliberate. The constraint in PostgreSQL proves the source
was sound; the test in Snowflake proves the value survived CDC, Kafka, Bronze,
Iceberg, Parquet and a `COPY`. They fail for different reasons, which is the
same argument phases 1, 2 and 8 make about their own layers.

The one assertion that is not a column constraint is `min_age <= max_age` on
audiences. An inverted age band targets nobody and would produce zero-row joins
in phase 12 rather than an error.

## The core dimensions (phase 10)

Seven Type 1 dimensions in `CORE`, one per staging model. Type 1 means the
dimension holds current state and overwrites: an advertiser that moves its
billing country shows the new one and keeps no trace of the old. The history
is not lost, it just lives elsewhere - phase 11 snapshots the same staging
models and keeps a row per version.

### Surrogate keys, and why they are hashes

Every dimension carries `<entity>_key` from
`dbt_utils.generate_surrogate_key`, an md5 of the natural key. The alternative
is a Snowflake identity column or a `row_number()`, both of which produce
narrower keys and faster joins.

They also renumber on rebuild. A dimension rebuilt from scratch hands out
different integers for the same entities, and every fact table built against
the previous run now points at the wrong rows - without failing, because the
keys still resolve. A hash of the natural key cannot do that, and the test
that pins it down is a checksum taken either side of a rebuild:

```
advertiser_key checksum before rebuild: 3791077116878899171
advertiser_key checksum after rebuild:  3791077116878899171
```

Determinism buys something else. A model holding `advertiser_id` can produce
`advertiser_key` by hashing it, with no join to `dim_advertiser` at all - which
is how `dim_campaign` and `dim_creative` get theirs. The `relationships` tests
are what prove the independently computed hashes agree.

### The unknown member

Each dimension unions in one extra row keyed `'UNKNOWN'`, which is why every
`CORE` table holds exactly one more row than its staging model.

The sentinel's shape is the point. `generate_surrogate_key` returns md5 hex, so
a sentinel with a non-hex character in it is unreachable by construction. The
tempting `'0' * 32` is a valid md5 output, and a collision there would not
raise - it would quietly attribute a real advertiser's impressions to the
unknown member and still pass every test.

Every foreign key in `postgres/schema.sql` is `NOT NULL`, so the obvious
question is what the unknown member is for. Referential integrity at the source
says nothing about what has arrived *here*: an impression can land in one load
and the audience it targeted in the next. The alternative to a row that absorbs
it is an inner join that drops the impression or an outer join that leaves a
null for every downstream aggregate to trip over.

Its attributes are chosen rather than left null. `'XXX'` is ISO 4217 for "no
currency", so a sum grouped by currency cannot merge the unknown member into a
real one; budgets are zero rather than null, because a null propagating through
a `sum` is harder to notice than a zero that does not move the total; and
`dim_audience`'s unknown member carries the source's own age bounds so the
`min_age <= max_age` assertion holds for it without an exception written
around it.

The natural key stays null, which is why `unique` is asserted on it and
`not_null` is not - staging already proves the source has none. The
`accepted_values` tests carry `where: "<natural key> is not null"` for the same
reason: the unknown member is not a real advertiser and has no real status, and
excluding it keeps the assertion about the source data rather than about the
sentinel.

### Why the dimensions keep deleted members

This changed phase 9. Staging used to filter `is_deleted` out, and the
dimensions would have inherited that.

The change generator deletes audience segments while the 100 million
impressions that targeted them stay exactly where they are - several hundred
segments, in a dimension of about ten thousand. Filtering them out of
`dim_audience` strands every one of those impressions on a key resolving to
nothing: the join routes them to the unknown member and collapses hundreds of
distinct segments into one meaningless bucket. Nothing raises.

So the filter moved up. Staging is now a faithful typed copy of RAW, the
dimensions contain every member a fact has ever referenced, and `is_deleted` is
what a report filters on. "Which segments exist today" is `where not
is_deleted`; "what did this segment ever deliver" is the same query without it.
Both stay askable, which was the whole argument for soft deletes surviving up
from Silver in the first place.

The counts after the change:

| Dimension | `CORE` | `STAGING` | deleted members |
|---|---|---|---|
| `dim_advertiser` | 5,001 | 5,000 | 0 |
| `dim_campaign` | 50,001 | 50,000 | 0 |
| `dim_line_item` | 150,001 | 150,000 | 0 |
| `dim_creative` | 200,001 | 200,000 | 0 |
| `dim_publisher` | 20,001 | 20,000 | 0 |
| `dim_placement` | 100,001 | 100,000 | 0 |
| `dim_audience` | 10,586 | 10,585 | 546 |

### Line items are a separate dimension

Not folded into `dim_campaign`, which is the usual Kimball advice for a
hierarchy this shallow. Two reasons it is wrong here: `impressions` and
`spend_transactions` both carry `line_item_id` and `campaign_id`
independently, so the grain already treats them as separate things, and a line
item's bid and pacing are the levers a trader pulls - attributes nobody wants
repeated across a campaign's worth of rows.

## How this relates to the Silver quality suite

`make validate-silver` checks the lakehouse; `make dbt-test` checks the
warehouse. They do not overlap, and the contrast is the interesting part: one is
hand-written SQL run by a Python executor, the other is declarative YAML a
framework expands. Phases 7 and 8 built by hand what dbt provides - an
incremental strategy, a watermark, a test suite - which is why those phases are
worth keeping hand-built rather than retrofitting dbt over them. Understanding
what a framework does and being able to use it are different claims, and the
project makes both.

## Credits

A trial gives 30 days and $400. This project will not come close to exhausting
that, but two settings in `bootstrap.sql` are there to keep it that way:

- `WAREHOUSE_SIZE = XSMALL`. The dataset is a few gigabytes; a larger warehouse
  finishes the same work in the same wall-clock time and bills proportionally to
  its size.
- `AUTO_SUSPEND = 60`. The default is 600, which bills ten minutes of idle after
  a two-minute load. This is the single largest source of wasted credits on a
  trial account.

`STATEMENT_TIMEOUT_IN_SECONDS = 1200` is insurance rather than economy: nothing
here legitimately runs for twenty minutes, so a query that does should fail
rather than bill until someone notices.

Every dbt statement is tagged `adtech-dbt`, so `QUERY_HISTORY` can separate
transformation cost from load cost when a bill needs explaining.

## When the trial ends

The account is suspended and its data eventually dropped; there is no read-only
archive. What survives is everything through phase 8, which runs locally and
needs no account, plus the dbt project itself as code.

That split is why the dimensional model's *inputs* are built in the lakehouse
and the model itself is built here. A reader who clones this repository without
a Snowflake account still gets a complete, running pipeline from a PostgreSQL
source to an Iceberg lakehouse with its own quality suite. They just do not get
the last four phases.
