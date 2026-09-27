# Data quality

Validation runs in two independent places. Both have to pass.

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

## Why both layers

They fail for different reasons, which is the point:

* Layer 1 catches generator bugs with a precise, actionable message and stops
  before writing anything.
* Layer 2 catches anything that happens *in the database* - a bad load, a partial
  transaction, a schema change - and is the layer that will be reused against
  the Iceberg and Snowflake copies of the same data in later phases.

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

## Known warnings

None at the SMALL profile. If `multiple_clicks_per_impression` or
`multiple_conversions_per_click` ever warn, the funnel model has changed: this
model emits at most one click per impression and one conversion per click, which
is what makes last-click attribution unambiguous.
