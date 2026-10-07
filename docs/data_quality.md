# Data quality

Validation runs in three independent places. All of them have to pass.

```bash
make validate          # layers 1 and 2: the data PostgreSQL holds
make validate-silver   # layer 3: the data that survived the pipeline
```

## Layer 1 - in process, before anything is written

`data_generator/validation.py`. Every batch is checked against the ecosystem it
was generated from *before* it reaches a sink, so a generation bug surfaces as a
precise error naming the check, the table and the offending identifiers, rather
than as a `ForeignKeyViolation` thousands of rows later.

Three families:

* **Referential** - every foreign key resolves in the registry, and derived
  events agree with their parents: a click's campaign, line item, advertiser and
  creative must equal its impression's; a conversion's impression, campaign and
  advertiser must equal its click's.
* **Temporal** - the full chain advertiser → campaign → line item → impression →
  click → conversion never goes backwards, events fall inside the campaign and
  line item flights, nothing is recorded before it happened, and conversions fall
  inside the attribution window they claim.
* **Business rules** - enum domains, currency format, non-negative money,
  `clearing_price <= bid_price`, viewability in [0, 1], `min_age < max_age`,
  `daily_budget <= campaign_budget`, campaign status agreeing with its dates,
  and primary key uniqueness tracked in process.

The first violation aborts the run with a `ValidationError` listing up to 20
offending rows. There is no "log and continue" path.

At the SMALL profile this is roughly 21 million assertions and costs about 5
seconds of the ~19 second generation.

Primary key tracking stores `uuid.int` rather than UUID objects (about 40 bytes
per row). Set `validation.track_primary_keys: false` in
`config/generation.yml` for the LARGE profile and rely on the database's own PK
constraint instead.

## Layer 2 - SQL, after loading

`data_quality/tests/*.sql`, executed by `data_quality/validation.py` and run
automatically at the end of a `--target postgres` generation, or on its own with
`make validate`.

Checks are declared as annotated SQL rather than embedded in Python:

```sql
-- name: orphan_clicks
-- type: referential
-- table: clicks
-- severity: ERROR
-- expect: zero
-- description: every click references an existing impression
SELECT COUNT(*)
FROM clicks cl
LEFT JOIN impressions i ON cl.impression_id = i.impression_id
WHERE i.impression_id IS NULL;
```

Each check returns a single number. `expect: zero` passes when it is 0;
`expect: nonzero` passes when it is greater than 0 and is how the join
validation asserts that a join actually returns rows. `severity: WARNING`
downgrades a failure to a warning that does not fail the run.

| File | Checks |
|---|---|
| `referential_integrity.sql` | orphan rows on every foreign key, plus parent/child agreement on impressions, clicks, conversions and spend |
| `temporal_consistency.sql` | the ordering chain, flight containment, attribution windows, `created_at` ordering |
| `business_rules.sql` | enum domains, nullability, negative money, viewability range, floor/clearing/bid ordering, status vs dates, currency format, billing type vs `impression_id` |
| `uniqueness.sql` | duplicate primary keys, duplicate event ids, at most one click per impression and one conversion per click, unique publisher domains |
| `join_validation.sql` | the representative joins from the brief, each asserted non-zero and reported with its row count |

Results are written to `platform.data_quality_result` with the run id, and
`platform.generation_run.validation_status` is updated. Non-zero exit code on
failure, so the suite is CI-ready.

## Layer 3 - the Silver layer, and whether it still agrees with the source

`data_quality/silver.py`, run with `make validate-silver`. Twenty checks in the
same annotated-SQL format, reusing `Check`, `CheckResult` and
`DataQualityReport` unchanged - only the engine differs, Spark SQL against
Iceberg instead of psycopg against PostgreSQL. They live in
`data_quality/tests/silver/` because `load_checks` does not recurse, so the two
suites cannot accidentally be handed to the wrong engine.

Layers 1 and 2 both ask "is the source data sound?". This asks a different
question: *did it survive the trip?* Debezium, Kafka, Bronze and an arbitrary
number of incremental merges sit between the two, and none of the earlier checks
can see a thing that goes wrong in there.

Three families:

* **Integrity** - invariants that hold whatever the source is doing. No
  duplicate or null primary keys, soft deletes carry a `deleted_at` and live
  rows do not, no duplicate `impression_id` across 90 partitions, and the
  append-only event tables contain no deletions. The duplicate-key check is the
  important one: Iceberg does not enforce primary keys, so nothing but the merge
  logic prevents a key appearing twice, and phase 7's `MERGE INTO` is the first
  thing in the project that could produce one.
* **Lineage** - the columns reconciliation adds tell a coherent story. Rows with
  no `_lsn` carry no `_op`, rows with one have a known operation, `is_deleted`
  agrees with `_op = 'd'`, and no applied change sits at or below the snapshot
  export's WAL position. These are not cosmetic: phase 7 *derives* its watermark
  from `max(_lsn)`, so a row with the wrong `_lsn` moves the watermark, and a
  watermark that is too high skips changes permanently.
* **Reconciliation** - Silver against live PostgreSQL. Row counts, keys in both
  directions, and the columns the change traffic actually mutates.

### Reading PostgreSQL from Spark

Phase 6 put the PostgreSQL JDBC driver in the Spark image so Iceberg could use a
JDBC catalog. The same driver lets a single Spark SQL statement join an Iceberg
table against a live PostgreSQL table, which is what makes reconciliation an
ordinary check rather than a bespoke Python comparison. The source tables appear
as `pg_<table>`.

Only the dimensions are exposed whole, and they rely on the JDBC source's column
pruning - a check selecting two columns has PostgreSQL send two columns. The
event tables reach 100,000,000 rows, so their counts are aggregated inside
PostgreSQL and arrive as the single `pg_counts` view; pulling them over JDBC so
Spark could count them is the kind of thing that works on a test dataset and
takes the cluster down on a real one.

### Why the value checks matter more than the counts

Comparing row counts is cheap and weak: it catches a row that went missing, not
an update that was never applied. A watermark that advanced too far leaves every
count agreeing and the values quietly stale.

So the sharpest checks compare `campaign_status`, `daily_budget`, `bid_amount`,
`creative_status` and `publisher_status` row by row - the columns
`data_generator/change_generator.py` actually edits. A merge that stopped
applying updates shows up there and nowhere else.

### The one thing these checks cannot assume

Reconciliation compares Silver against a source that keeps moving. A mismatch
has two causes - the pipeline is broken, or it is merely behind - so the runner
reports how many Bronze objects are still unmerged alongside the results.

That signal sees one hop. A change that has left PostgreSQL but is still in
Kafka, not yet flushed to Bronze, shows up as nothing pending while PostgreSQL
is already ahead, and the sink flushes on a timer, so that window is minutes
wide. **Zero pending means "nothing left to merge", not "caught up."**
Reconciliation is only conclusive once the source has stopped changing.

This is a real limitation rather than a tidy one, and it is why these checks
stay `ERROR` rather than being softened to warnings: downgrading them would mean
a genuine divergence never fails the suite, which is the whole point of having
it.

## Why all three layers

They fail for different reasons, which is the point:

* Layer 1 catches generator bugs with a precise, actionable message and stops
  before writing anything.
* Layer 2 catches anything that happens *in the database* - a bad load, a partial
  transaction, a schema change.
* Layer 3 catches anything that happens *between* the database and the lakehouse,
  which is the only place a CDC pipeline can go wrong without anyone noticing.
  Every other check can pass on a Silver layer that is internally immaculate and
  no longer reflects reality.

A subtle third layer is the schema itself: the `CHECK` and `FOREIGN KEY`
constraints in `postgres/schema.sql` would reject bad rows even if both
validators were removed.

## Testing the validators

A validation layer that only ever passes is worse than none.
`tests/test_validation.py` deliberately corrupts generated rows - a dangling
foreign key, an invalid enum, reversed flight dates, a duplicate primary key, a
status that contradicts its dates - and asserts that the specific check fires.
`tests/test_data_quality_checks.py` asserts the SQL checks parse, have unique
names and valid metadata, and that the pass/fail/warn logic behaves.
`tests/test_silver_quality.py` does the same for the Silver suite and adds the
part that is specific to it: a check is skipped when a table it reads has not
been built yet, so running the suite before the event tables exist reports
fewer checks rather than a wall of failures about missing tables.

The Silver suite was also confirmed against live data the only way that proves
anything - by breaking something. Changing one campaign's `daily_budget`
directly in PostgreSQL made `reconciliation_campaign_values_match` fail with
`observed 1` and the command exit non-zero; restoring the value cleared it.

## Where this gets consumed

`make validate-silver` exits non-zero when any `ERROR` check fails, but nothing
reads that exit code yet. It is deliberately a separate command: the merge job
and the thing that judges the merge job should be able to run independently
while both are still changing. Phase 15 gives it a caller, where Airflow runs it
as a task downstream of reconciliation and a failure stops the DAG before
Gold-layer aggregates are built on top of data that does not match the source.

A failure writes the same report to `artifacts/silver_quality_report.json`. The
project is mounted read-only inside the Spark container, so a report that cannot
be written logs a warning instead of raising - the exit code should mean "the
data is wrong", never "a file could not be saved".

## Known warnings

None at the SMALL profile. If `multiple_clicks_per_impression` or
`multiple_conversions_per_click` ever warn, the funnel model has changed: this
model emits at most one click per impression and one conversion per click, which
is what makes last-click attribution unambiguous.
