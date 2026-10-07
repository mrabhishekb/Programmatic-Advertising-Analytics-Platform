# Incremental Silver (phase 7)

Phase 6 rebuilt every table on every run. That is correct and it does not scale:
the change history only grows, so each run reads more than the last while the
amount of genuinely new work in it stays flat. Phase 7 makes a run proportional
to what changed.

```bash
make silver-status   # what the next run would do, and why
make silver          # do it
make silver-full     # ignore the bookmarks and rebuild everything
```

## The watermark is derived, not stored

The obvious design is a bookmark somewhere saying "last processed: LSN 900".
The problem is that writing the rows and writing the bookmark become two acts,
and a crash can land between them:

| order | crash in between | result |
| --- | --- | --- |
| bookmark, then rows | rows never written | **changes skipped permanently, silently** |
| rows, then bookmark | bookmark stale | changes reprocessed - wasteful, harmless |

Only one of those is survivable, and even it needs the merge to be idempotent.

Silver already records the answer. Every row carries `_lsn`, the WAL position of
the change that produced it, null for rows that came straight from the snapshot
export. So `max(_lsn)` over the table *is* the high-water mark:

```sql
SELECT max(_lsn) FROM lake.silver.campaigns;
```

A derived watermark cannot disagree with the data, because it is the data. A
half-applied run leaves a watermark describing exactly what committed, and
rerunning finishes the job. Iceberg keeps per-column bounds in its manifests, so
this is a metadata read rather than a scan.

This is exact only because collapsing picks the *newest* change per key, so the
highest LSN in a batch is always one of the rows that lands.
`tests/test_merge.py::test_collapsing_does_not_lose_the_high_water_mark` pins
that property down, because the whole design rests on it.

## The offsets are a hint, and so they are stored

Which Bronze *files* to open cannot be derived from Silver, so that bookmark
does live in table properties (`adtech.applied-offsets`), written after the
merge rather than with it. That is safe because it is only ever a hint: a stale,
missing, or corrupt offset bookmark means opening files whose changes have
already been applied, and the LSN filter discards them. The two bookmarks have
different durability requirements because only one of them can be wrong without
being harmful.

Offsets rather than the `dt=` partition, which is the obvious choice and is
wrong. `bronze.layout.partition_date_of` dates a file by when the change
*happened*, so a late-arriving event lands in an old date partition and pruning
by date would skip it forever. Kafka offsets are arrival order, so a late event
still gets a higher offset than anything already read.

An object whose offset range straddles the bookmark - compaction merges files,
so this happens - is read again rather than skipped. Re-reading costs time; the
alternative skips the half that has not been applied.

## What a run decides, per table

| mode | when | what it reads |
| --- | --- | --- |
| `bootstrap` | no Iceberg table yet | snapshot export + whole change history |
| `incremental` | table exists, same export underneath | only CDC objects above the offset bookmark |
| `skip` | nothing new | nothing |
| `rebuild` | a newer snapshot export appeared, or `--full` | snapshot export + whole change history |

`rebuild` replays the *whole* history rather than the pruned set: it starts from
the export again, so pruning by the old bookmark would leave the rebuilt table
missing every change already applied to the table it replaces.

Event tables (`impressions`, `clicks`, `conversions`, `spend_transactions`) are
append-only and not captured by Debezium, so the export is their only input.
Once built they `skip` until a new export appears, which is what keeps a rerun
from rewriting six gigabytes into an identical table.

## Proof

Baseline, then a rerun with nothing new, then 248 source changes, a Bronze
flush, and another run:

| run | mode | duration | written |
| --- | --- | --- | --- |
| baseline | all `rebuild` | 35.7s | 7 tables, 535,517 rows |
| rerun, no changes | all `skip` | 8.1s | nothing |
| after live traffic | 5 `incremental`, 2 `skip` | 119.0s | 427 rows merged |
| `--include-events`, first time | 4 `rebuild` | 534.0s | 125,371,495 rows, 6.6 GB |
| `--include-events`, rerun | all `skip` | **8.0s** | nothing |

The third run touched only the five tables that had received traffic;
`advertisers` and `placements` were skipped.

The last pair is the clearest result in the phase: with the event tables in
scope, a rerun that has nothing to do costs 8 seconds instead of 534, because
`skip` replaces reading and rewriting 100,000,000 rows. Phase 6 had no way to
express "nothing changed" and paid the full 534 seconds every time.

Every table reconciles exactly against PostgreSQL, with distinct keys equal to
row counts, so the merge neither lost nor duplicated a key. The incremental and
rebuild paths were checked against each other as well: `make silver-full` after
a series of incremental runs lands on the same live counts, so merging does not
drift from rebuilding.

| table | Silver live | PostgreSQL | soft-deleted |
| --- | --- | --- | --- |
| advertisers | 5,000 | 5,000 | 0 |
| publishers | 20,000 | 20,000 | 0 |
| placements | 100,000 | 100,000 | 0 |
| campaigns | 50,000 | 50,000 | 0 |
| line_items | 150,000 | 150,000 | 0 |
| creatives | 200,000 | 200,000 | 0 |
| audiences | 10,037 | 10,037 | 512 |

## Two costs worth being honest about

**The incremental run was slower than the rebuild.** 119s against 35.7s. At half
a million rows, rebuilding seven small tables is simply cheap, and a `MERGE INTO`
pays for a join against the full target plus per-table job overhead. The win
here is structural rather than a speedup: a rebuild's cost grows with the change
history, an incremental run's does not. Where it is already decisive is the
event tables, at 534s against 8.0s.

**Merging grows the file count.** Seven files at 24.5 MB became thirty-three at
29.3 MB to apply 427 changed rows. Iceberg's default `copy-on-write` merge
rewrites every file containing a touched row and writes one output file per
task, so small frequent merges accumulate small files and a table's read cost
drifts upward. Two ways out, neither free:

- `write.merge.mode=merge-on-read` writes small delete files instead of
  rewriting data files. Cheaper writes, slower reads, and it still needs
  compaction eventually.
- Periodic `rewrite_data_files` compaction, which is maintenance this project
  does not yet have. `make silver-full` happens to do the same job as a side
  effect - it took 33 files back down to 7 - but rebuilding a table to tidy its
  files is not a maintenance strategy.

At the current file count `copy-on-write` is the right default - rewriting a
9 MB file is not worth avoiding - but it is a bounded choice, not a permanent
one.
