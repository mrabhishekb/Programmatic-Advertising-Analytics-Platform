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

Without the event tables, a first run against a trial account takes about a
minute in total: the bootstrap is 25 statements, the load moves 535,648 rows in
roughly 30 seconds, and dbt builds 7 views, 7 snapshots and 15 tables.

With them it is a different exercise. `make warehouse-export-all` writes 15.2 GB
of Parquet and the load uploads it over whatever uplink you have - here, a
little over two hours for 125,371,589 rows at 2 to 4 MB/s. Snowflake's own side
of it is not the slow part: once the files are staged, `dbt build` creates all
four facts, 15 tables, 7 snapshots, 11 views and runs 256 tests in **2 minutes**,
of which `fct_impressions` is 93 seconds for 100 million rows.

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

### The upload resumes (phase 12)

Phase 9's loader cleared the stage and sent each table in one
`PUT ... OVERWRITE = TRUE`. With seven dimensions of two files each, nothing
about that was worth a second thought.

`impressions` is 13.4 GB in 101 files. Two and a half hours into uploading it,
file 27 hit a TLS read error. The Snowflake connector classifies a mid-transfer
failure as non-retryable, so the statement failed - and took with it the ninety
files that had not started yet.

The fix is to make the upload idempotent rather than to make it more reliable.
`OVERWRITE = FALSE` tells PUT to skip files already in the stage, which turns a
retry into a resume: each attempt costs only what the previous one had left.
That is safe because a partial upload never appears in the stage at all -
Snowflake finishes an object in cloud storage atomically, so an interrupted
transfer leaves nothing behind rather than half a Parquet file.

Two details are easy to get wrong here, and both are tested:

- The loop runs until the stage holds **every file the export wrote**, not
  until a PUT returns without raising. A statement can come back successful
  having transferred less than everything, and the COPY would then load a
  table that is quietly short.
- `REMOVE` was there for a real reason - a load following a smaller export
  would otherwise copy the previous one's leftovers into the table - so it
  still runs, but against exactly the files the current export does not
  contain. Deleting all of them is what made resuming impossible.

It earned its place on the retry: one PUT failed partway through and the load
finished 100,000,000 rows without intervention.

The other half of the lesson is not in the code. The upload ran at 2-4 MB/s
when the machine was awake and effectively zero when it was not, so a long load
needs `caffeinate` and mains power as much as it needs retries.

## The dbt project

Lives in `warehouse/dbt`.

| Layer | Schema | Materialisation | Built in |
|---|---|---|---|
| sources | `RAW` | loaded by `warehouse.load` | phase 9 |
| staging | `STAGING` | view | phase 9 |
| snapshots | `SNAPSHOTS` | table (accumulating) | phase 11 |
| core | `CORE` | table; the facts incremental | phases 10-12 |
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
layer names appear in `bootstrap.sql`, in this document and in the
architecture diagram. Schemas that depended on who ran dbt last would
contradict all three.

### Tests

256 of them across 30 models. Almost all come free from YAML: `unique` and
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

The change generator deletes audience segments - several hundred of them, in a
dimension of about ten thousand. It is careful about which ones: the statement
only picks segments no impression references, because a hard delete at source
would otherwise violate a foreign key. So as the data stands today, no fact
points at a deleted member and nothing would actually break.

Relying on that would make the warehouse correct by coincidence. It is a
property of this generator's `WHERE` clause, not of the warehouse, and it is
not a property real sources have - they delete referenced members routinely and
leave the warehouse to cope. Phase 16's late-arriving events make it concrete
even here: an event that arrives after its dimension member was removed has
nowhere to join, and the dimension that already dropped the member cannot get
it back.

The filter would also lose something independent of joins. "What did that
segment target before we stopped using it" is only answerable while the row
exists, and that question is the entire reason soft deletes survive up from
Silver rather than being resolved there.

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

## The Type 2 dimensions (phase 11)

Seven dbt snapshots in `SNAPSHOTS`, and seven `dim_*_history` models in `CORE`
that turn them into validity ranges. A Type 1 dimension answers "what is this
campaign now"; these answer "what was it when that impression was served",
which is the only version of the question that stays correct as attributes
drift.

All seven entities are snapshotted, including the ones that barely change,
because of one asymmetry: **a snapshot started late cannot be backfilled.** The
states it would have recorded are gone from the source by definition.
Capturing a dimension nobody ends up asking about costs a few megabytes.

### Why the check strategy, not timestamp

The timestamp strategy is cheaper and more obvious, and the data cooperates:
every `UPDATE` the change generator issues bumps `updated_at`, with a `CHECK`
constraint in `postgres/schema.sql` to back it up.

It misses deletions completely. A delete at source is a `DELETE`, not an
`UPDATE` - nothing touches `updated_at`. Silver turns it into a flag, so what
reaches staging is `is_deleted` flipping false to true while every timestamp
the row carries stays exactly where it was. A timestamp snapshot compares
`dbt_valid_from < updated_at`, sees nothing move, and records nothing. The
dimension would show 546 deleted audience segments as live forever.

Deriving a timestamp that does move does not rescue it. `coalesce(deleted_at,
updated_at)` is the obvious attempt, and it goes backwards: `updated_at` is a
business timestamp the generator writes in local time, while `deleted_at` comes
from Debezium's event time in UTC. 46 of the 546 deleted segments carry a
`deleted_at` *earlier* than their own `updated_at` for that reason alone.

`check` sidesteps both, comparing the listed columns and noticing anything that
moved whatever the timestamps claim.

`check_cols: 'all'` would be wrong in the other direction: it includes
`source_lsn`, `source_operation` and `source_event_at`, which change on every
CDC event whether or not the row's content did, so a no-op update would record
a version of a change nobody made. `updated_at` is excluded for the same
reason. What is listed is what a historical question would be about, plus
`is_deleted`.

### Business time, not run time

dbt's snapshots carry `dbt_valid_from` and `dbt_valid_to`, and the obvious move
is to rename them. Under the check strategy they hold **the moment the snapshot
ran**, not the moment the change happened.

The first run would therefore stamp every entity with today's date, and a range
join from an impression served three months ago would find no version covering
it - every fact in phase 12 falling outside every range and landing on the
unknown member.

So processing time is kept under a name that says what it is (`captured_at`),
and the ranges are rebuilt from the source's own timestamps in
`macros/scd2.sql`. Two details there are not decoration:

- **A running maximum around the version timestamp.** Because of the UTC/local
  skew above, the raw expression can go backwards between two versions of the
  same row, and a range that starts before the one it replaces means
  overlapping versions - the one thing a Type 2 dimension must never have.
  Clamping to a running maximum makes the sequence non-decreasing whatever the
  timestamps do. Ties collapse to a zero-length window, which a half-open range
  join skips, which is right: a version superseded at the instant it began was
  never true of anything.
- **The first version is backdated to `created_at`.** A snapshot cannot record
  states from before it started running, so without this every fact older than
  the first run falls outside every range. It asserts something slightly false,
  that today's values held since creation, in exchange for a dimension that can
  be joined at all. The alternative is to pretend the history exists.

`valid_to` is `9999-12-31` rather than null, so a range join needs no
`coalesce` - and cannot silently drop every current row by forgetting one.

### Version keys are keyed on capture time

`<entity>_version_key` is md5 of the natural key and `dbt_valid_from`, not of
the natural key and `valid_from`.

The second is the obvious choice and is not unique. Two versions share a
`valid_from` exactly when the timestamps behind them tie - which the skew above
produces regularly on deletions - and the earlier version then occupies a
zero-length window, which is correct, and shares a surrogate key with its
successor, which is not. Two versions collapsing into one key is a silent
merge.

This was found by `unique_dim_audience_history_audience_version_key` failing on
the first run that had real deletions in it, which is the argument for putting
`unique` on a key you are confident about.

### The two tests no column can make

`versions_are_contiguous` and `exactly_one_current_version` are generic tests
in `tests/generic/`. Neither is a property of a single row, so no column-level
test can reach them.

Overlapping ranges make a fact match two versions and double its measures; a
gap makes it match none and lose them. Both pass `unique`, `not_null` and
everything else a dimension carries. Contiguity is asserted rather than mere
non-overlap, because the construction should make a gap impossible - a gap
appearing means the window ordering has drifted from what the ranges assume.

### SNAPSHOTS is its own schema

Every other schema here is derived and can be rebuilt from `RAW` at any time. A
snapshot accumulates, and a dropped one is change history that no rerun
recovers, because the states it recorded no longer exist anywhere to read them
from. The schema boundary makes "do not drop this" something the layout says
rather than something a reader has to already know.

### What it looks like once history exists

After one run of `make changes` through the full pipeline:

| History dimension | rows | entities | with more than one version |
|---|---|---|---|
| `dim_advertiser_history` | 5,001 | 5,001 | 0 |
| `dim_campaign_history` | 50,279 | 50,001 | 278 |
| `dim_line_item_history` | 150,021 | 150,001 | 20 |
| `dim_creative_history` | 200,185 | 200,001 | 184 |
| `dim_publisher_history` | 20,093 | 20,001 | 92 |
| `dim_placement_history` | 100,001 | 100,001 | 0 |
| `dim_audience_history` | 10,665 | 10,646 | 19 |

A campaign that was resumed:

```
PAUSED  budget=26.49  2026-09-23 14:35:37 -> 2026-10-07 20:18:00.961
ACTIVE  budget=26.49  2026-10-07 20:18:00.961 -> 9999-12-31          (current)
```

And a deleted audience segment, which is the case the strategy was chosen for:

```
deleted=False  updated_at=2026-10-05 07:19:54.046  deleted_at=None
  valid 2026-10-05 07:19:54.046 -> 2026-10-07 15:42:55.931
deleted=True   updated_at=2026-10-05 07:19:54.046  deleted_at=2026-10-07 15:42:55.931
  valid 2026-10-07 15:42:55.931 -> 9999-12-31                        (current)
```

`updated_at` is identical in both rows. That is the whole argument for the
check strategy in one line of output: a timestamp snapshot had nothing to
compare, and would have recorded no second version at all.

### Line items are a separate dimension

Not folded into `dim_campaign`, which is the usual Kimball advice for a
hierarchy this shallow. Two reasons it is wrong here: `impressions` and
`spend_transactions` both carry `line_item_id` and `campaign_id`
independently, so the grain already treats them as separate things, and a line
item's bid and pacing are the levers a trader pulls - attributes nobody wants
repeated across a campaign's worth of rows.

## The facts (phase 12)

Four fact tables and a date dimension turn the seven dimensions into a star
schema. This is the phase where the warehouse stops being a tidy copy of
PostgreSQL and starts being something you can ask questions of.

| Fact | Grain | Rows | Measures |
|---|---|---|---|
| `fct_impressions` | one ad served | 100,000,000 | bid price, clearing price, impression cost, viewability |
| `fct_clicks` | one click | 5,000,000 | none - factless |
| `fct_conversions` | one conversion | 500,000 | conversion value |
| `fct_spend` | one billable charge | 19,335,944 | spend amount |

Each one is an event table from `postgres/schema.sql` with four things done to
it: foreign keys replaced by surrogate keys, a `date_key` added, the CDC
lineage columns dropped, and `created_at` dropped. The last two are worth a
sentence each. The lineage columns are null on every row of every event table
because these tables are append-only and never went through Debezium. And
`created_at` is when the generator wrote the row, not when the ad was served -
keeping both invites someone to group by the wrong one.

### What is a measure and what only looks like one

Three columns in these facts are numbers that must not be summed, and each is a
different reason.

`viewability_score` is **non-additive**. It is a ratio between 0 and 1, so
adding two of them produces a number with no meaning. Averaging is defensible
and still only approximately right, because an unweighted average treats a
three-second view and a thirty-second one as equal evidence.

`attribution_window_hours` is **not a measure at all**. It records the rule
that credited a conversion. Averaging it describes how the platform was
configured, which is a real question, but not the one anyone asking about
conversions is asking.

`clearing_price` is **the wrong unit**. It is a CPM - a price per thousand
impressions - so summing it over 100 million rows gives a number a thousand
times too large, and nothing about the column says so. `fct_impressions`
derives `impression_cost = clearing_price / 1000` once, at the fact, so that
the correct answer is the default rather than something every query has to
remember. The CPM stays alongside it, because "what did we bid" is also a
real question.

### Both kinds of key, on the same row

Every fact carries two keys per dimension that has history:

```
campaign_key          -> dim_campaign          what the campaign is now
campaign_version_key  -> dim_campaign_history  what it was at that instant
```

Which one a query wants depends on the question, and the two give different
answers on purpose. *"How much did the Acme account spend?"* wants the Type 1
key, because Acme is Acme regardless of how it was configured at the time.
*"Did this campaign overspend its budget?"* wants the Type 2 key, because the
budget it has today is not the budget it was given then. Carrying only one
would silently decide that for the reader.

The version keys are resolved by a range join against the SCD2 validity
windows, written once in `macros/facts.sql`:

```sql
left join dim_campaign_history as ch
    on  ch.campaign_id = e.campaign_id
    and e.impression_timestamp >= ch.valid_from
    and e.impression_timestamp <  ch.valid_to
```

Three details in four lines, all of which matter at this scale.

**Half-open, not `between`.** `between valid_from and valid_to` is inclusive at
both ends, so an event landing exactly on a boundary matches two versions and
the row doubles - along with every measure on it. A test asserts no fact model
writes this join by hand, because one place to get it right is the only way to
keep it right.

**Left, not inner.** An event timestamped before its dimension member existed
has no version in force. An inner join would delete it from the fact table. A
left join produces a null, which `coalesce` turns into the unknown member and
the `resolves_to_a_version` test then counts.

**The equality is what makes it affordable.** With `campaign_id` matched first,
Snowflake hash-joins 100 million rows against 50,000 versions and gets about
one match per probe; the two range predicates only filter that handful. Drop
the equality and lean on the range alone and the same query becomes a nested
loop over the whole dimension, per row.

Fan-out is the thing that makes range joins dangerous, and the defence is not
vigilance. `versions_are_contiguous` asserts on every build that each version
ends exactly where the next begins, so at most one window contains any instant;
`unique` on each fact's degenerate key then catches it anyway if that ever
stops being true. A fan-out produces a table that loads, joins and looks
entirely normal, with every revenue number multiplied.

### `date_key` is the one key that is not a hash

`20261003`, as an integer. Every other surrogate key here is deliberately
opaque; this one is deliberately not, and the reason is that a date dimension
is the one case where the key's value is information. It sorts, it prunes
micro-partitions, it is readable in a result without a join, and - the part
that matters at 125 million rows - it is derivable from the timestamp by
arithmetic, so no fact ever joins to `dim_date` just to learn its own day.

`dim_date` itself is generated from a spine rather than loaded. There is no
date table in PostgreSQL and there should not be: a calendar is something the
warehouse decides. The bounds live in `dbt_project.yml`, and a test fails a
year before the window runs out, while widening it is still a one-line change.

It is also the only dimension with no unknown member. An unknown member absorbs
keys that are null or point nowhere, and neither is reachable for a date: every
event timestamp is `NOT NULL` at the source and `date_key` is computed, not
looked up. What *can* happen is a date outside the spine, and that should fail
loudly - the fix is to widen the window, not to file the row under Unknown.

### Degenerate dimensions

`impression_id`, `click_id`, `conversion_id` and `spend_transaction_id` sit on
the facts as plain columns. They identify a row but have no dimension behind
them - there is no `dim_impression`, because everything you would put in it is
already on `fct_impressions`.

They are not decoration. `fct_clicks` carries the `impression_id` its click
came from, and `fct_conversions` carries both the click and the impression, so
the whole chain is on one row. That is what phase 14 needs to compare
attribution models, and it is derivable from nothing else in the warehouse.

The dimension UUIDs, by contrast, are dropped once hashed. Keeping
`campaign_id` next to `campaign_key` would add seven 36-character columns to
the largest table in the project to store what a join already answers.

### `fct_spend` has three grains in one table

| `billing_type` | one row per |
|---|---|
| `CPM` | campaign, line item, publisher and hour |
| `CPC` | click |
| `CPA` | conversion |

Mixed grain in a single fact is usually a modelling mistake. It is right here
because the table answers one question - what were we billed - and the billing
system genuinely charges three different ways. Splitting it would make "total
spend" a union every query has to remember to write; keeping it together makes
the sum correct by default, and makes `billing_type` the column you add to a
`GROUP BY` the moment the question is about rates rather than totals.

It is also the one place the nullable foreign key lives. `impression_id` is
`NULL` for every CPM row, because an hourly rollup has no single impression
behind it - the only nullable FK in the entire source schema. It stays a
degenerate dimension rather than becoming a key, so the null stays visible
instead of being quietly filed under Unknown. A dbt test re-asserts the source
`CHECK` that ties the two together: if `(billing_type = 'CPM')` ever stops
meaning `impression_id is null`, the three grains have stopped being
distinguishable and every spend query is suspect.

### Incremental, because a rebuild is not free

The facts are the first models here that are not rebuilt from scratch. A `table`
materialisation would recompute 125 million rows on every run to arrive at the
same answer - correct, and unaffordable.

Each fact reprocesses only events at or after the newest one it already holds:

```sql
where impression_timestamp >= (select max(impression_timestamp) from {{ this }})
```

`>=` rather than `>`, which looks like an off-by-one and is not. At these
volumes events share timestamps, and `>` drops every one that ties with the
boundary except whichever happened to arrive first. Including the boundary
re-reads a few rows instead, which is only safe because the strategy is `merge`
on the degenerate key: the overlap becomes an update rather than a duplicate.

A merge still has to find the rows it might update, and by default that means
scanning the whole target. Each fact passes an `incremental_predicate` limiting
that search to the days the batch can touch, which is what keeps a one-day load
from reading 100 million rows.

Writing that predicate took three attempts, and all three failures were quiet
in different ways - which is the reason it gets its own paragraph.

The obvious version derives the floor from the batch:

```sql
DBT_INTERNAL_DEST.date_key >= (select min(date_key) from DBT_INTERNAL_SOURCE)
```

Snowflake rejects it: `Object 'DBT_INTERNAL_SOURCE' does not exist`. The
merge's source alias is in scope for the `ON` clause but not inside a subquery
within it, so the floor cannot come from the batch at all.

The second version computes it in Jinja with `run_query` and inlines an
integer. That fails *silently*: dbt resolves config while it parses, long
before there is a connection to ask, so the query returns nothing and the
predicate collapses to `1 = 1` - which compiles, runs, reports success, and
scans the whole table.

The third names the target as `{{ this }}`. Also silent, and for a reason
specific to this project: config is resolved before `generate_schema_name`
runs, so `this` still carries the profile's schema. The predicate came out
reading `ADTECH.STAGING.fct_clicks`, a relation that does not exist.

What works is naming the target explicitly, with the schema read from the same
var that configures it:

```sql
DBT_INTERNAL_DEST.date_key >= (
    select coalesce(max(date_key), 0) from ADTECH.CORE.fct_clicks
)
```

Reading the table being written sounds like it should be rejected and is not.
Snowflake evaluates it against one consistent snapshot taken before the merge
begins, so `max(date_key)` is the newest day the fact held on the way in -
exactly where the watermark started the batch. That was checked rather than
assumed: a batch spanning the boundary updates the row it shares and inserts
the one it does not. A test asserts the var and the models config stay equal,
because the whole thing depends on it.

No model declares `cluster_by`. The rows arrive date-ordered from a Silver
export already partitioned by event date, so Snowflake's natural
micro-partitioning prunes on `date_key` without being told to - and declaring a
clustering key switches on the automatic clustering service, which bills
continuously in the background whether or not anything is querying.

### What is tested, and what is deliberately not

The grain is asserted everywhere: `unique` on each degenerate key is the
fan-out test, and it is non-negotiable.

Referential integrity is asserted in full on the three small facts and sampled
on `fct_impressions`. An orphan key is close to impossible by construction -
the key is md5 of a natural key, and phase 10 deliberately kept deleted members
in the dimensions so nothing a fact points at can go missing. The exception is
`dim_audience`, the one dimension the source actually deletes from, which is
the one `fct_impressions` checks. Running all seven would be seven more full
passes over 100 million rows to re-prove a property of md5.

The same reasoning moved identity testing on the event tables to the source and
nowhere else. At 50,000 rows the same `unique` at three layers costs nothing
and catches different failures; at 100 million it is three full aggregations
per build to make one claim.

`resolves_to_a_version` is the one test here that cannot be expressed as a
column constraint. It counts events whose range join found no version in force,
which `relationships` can never see - the fact coalesces those to the unknown
member, so the key is always joinable and referential integrity always passes.
The row is not an orphan; it is unexplained. It warns rather than fails,
because the count is the point: zero means the clocks agree, a handful means
the local-time-versus-UTC skew from phase 11 is real and bounded, and a large
number means the version keys cannot be trusted.

### What it measured

`PASS=293 ERROR=0` in 120 seconds, against the full dataset.

| | |
|---|---|
| `fct_impressions` rows | 100,000,000 |
| distinct `impression_id` | 100,000,000 |
| distinct campaign versions referenced | 38,064 |
| events with no version in force | 0 |
| full build | 93.4s |
| incremental rebuild, nothing new | 3.7s |

The first two lines are the same number, which is the fan-out test passing: two
range joins against Type 2 dimensions resolved 38,064 distinct campaign
versions without duplicating a single row.

The third line is what the Type 2 join is *for*. Had the facts carried only
`campaign_key`, every one of those 100 million impressions would describe its
campaign as the campaign is configured today.

The last two are the argument for incremental materialisation, measured rather
than asserted: the same model, the same answer, 25 times cheaper when there is
nothing new to do.

And the reason `impression_cost` exists:

```
sum(impression_cost)      $416,982.49
sum(clearing_price)   $416,982,494.78
```

Both columns are on the same 100 million rows and both are correct. One of them
is the answer to "what did we spend".

Zero unresolved versions also settles something left open in phase 11. The
local-time-versus-UTC skew that put 46 audience deletions out of order does not
reach the event tables: every one of 125 million events falls inside a validity
window.

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
