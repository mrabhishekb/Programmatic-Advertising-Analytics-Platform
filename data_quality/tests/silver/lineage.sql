-- The lineage columns reconciliation adds, and whether they tell a coherent story.
--
-- Phase 7 derives its watermark from `_lsn` rather than storing it, so these
-- columns are not decoration: `max(_lsn)` is what the next run resumes from. A
-- row with the wrong `_lsn` does not just misreport provenance, it moves the
-- watermark - and a watermark that is too high skips changes permanently.

-- Rows read from the bulk export have no change event behind them. Filling in
-- a provenance they do not have is how a snapshot row ends up claiming to be
-- an update, and how it would then contribute to the watermark.
-- name: lineage_snapshot_rows_carry_no_change_metadata
-- type: lineage
-- table: all
-- severity: ERROR
-- expect: zero
-- description: a row with no _lsn carries no _op or _event_ts either
SELECT
    (SELECT count(*) FROM lake.silver.campaigns  WHERE _lsn IS NULL AND (_op IS NOT NULL OR _event_ts IS NOT NULL))
  + (SELECT count(*) FROM lake.silver.creatives  WHERE _lsn IS NULL AND (_op IS NOT NULL OR _event_ts IS NOT NULL))
  + (SELECT count(*) FROM lake.silver.line_items WHERE _lsn IS NULL AND (_op IS NOT NULL OR _event_ts IS NOT NULL))
  + (SELECT count(*) FROM lake.silver.publishers WHERE _lsn IS NULL AND (_op IS NOT NULL OR _event_ts IS NOT NULL))
  + (SELECT count(*) FROM lake.silver.audiences  WHERE _lsn IS NULL AND (_op IS NOT NULL OR _event_ts IS NOT NULL));

-- Anything else means the collapse ranked a record it did not understand -
-- including a tombstone, which carries no payload and would win with an
-- all-null row if it ever survived filtering.
-- name: lineage_changed_rows_carry_a_known_operation
-- type: lineage
-- table: all
-- severity: ERROR
-- expect: zero
-- description: every row with an _lsn has _op in (c, u, d, r)
SELECT
    (SELECT count(*) FROM lake.silver.campaigns  WHERE _lsn IS NOT NULL AND (_op IS NULL OR _op NOT IN ('c','u','d','r')))
  + (SELECT count(*) FROM lake.silver.creatives  WHERE _lsn IS NOT NULL AND (_op IS NULL OR _op NOT IN ('c','u','d','r')))
  + (SELECT count(*) FROM lake.silver.line_items WHERE _lsn IS NOT NULL AND (_op IS NULL OR _op NOT IN ('c','u','d','r')))
  + (SELECT count(*) FROM lake.silver.publishers WHERE _lsn IS NOT NULL AND (_op IS NULL OR _op NOT IN ('c','u','d','r')))
  + (SELECT count(*) FROM lake.silver.audiences  WHERE _lsn IS NOT NULL AND (_op IS NULL OR _op NOT IN ('c','u','d','r')));

-- The flag and the operation are written from the same event, so they cannot
-- disagree unless the collapse picked one row's payload and another's rank.
-- name: lineage_delete_flag_agrees_with_the_operation
-- type: lineage
-- table: all
-- severity: ERROR
-- expect: zero
-- description: is_deleted is set if and only if the newest operation was a delete
SELECT
    (SELECT count(*) FROM lake.silver.campaigns  WHERE _op IS NOT NULL AND is_deleted <> (_op = 'd'))
  + (SELECT count(*) FROM lake.silver.creatives  WHERE _op IS NOT NULL AND is_deleted <> (_op = 'd'))
  + (SELECT count(*) FROM lake.silver.line_items WHERE _op IS NOT NULL AND is_deleted <> (_op = 'd'))
  + (SELECT count(*) FROM lake.silver.publishers WHERE _op IS NOT NULL AND is_deleted <> (_op = 'd'))
  + (SELECT count(*) FROM lake.silver.audiences  WHERE _op IS NOT NULL AND is_deleted <> (_op = 'd'));

-- A deleted row with no WAL position behind it was never deleted by the
-- source; it was produced by the pipeline.
-- name: lineage_soft_deletes_came_from_a_change_event
-- type: lineage
-- table: all
-- severity: ERROR
-- expect: zero
-- description: no row is flagged deleted without an _lsn explaining why
SELECT
    (SELECT count(*) FROM lake.silver.campaigns  WHERE is_deleted AND _lsn IS NULL)
  + (SELECT count(*) FROM lake.silver.creatives  WHERE is_deleted AND _lsn IS NULL)
  + (SELECT count(*) FROM lake.silver.line_items WHERE is_deleted AND _lsn IS NULL)
  + (SELECT count(*) FROM lake.silver.publishers WHERE is_deleted AND _lsn IS NULL)
  + (SELECT count(*) FROM lake.silver.audiences  WHERE is_deleted AND _lsn IS NULL);

-- Reconciliation filters change events to those strictly above the export's
-- position, because anything at or below it is already reflected in the bulk
-- rows. A row below that line means the filter did not run, and re-applying an
-- old change over a newer one is how a table silently goes backwards.
--
-- The boundary is read from the Iceberg snapshot summary phase 6 writes on
-- every full build. `max` over all snapshots rather than the newest, because an
-- incremental merge commits without one - so the newest summary often has no
-- bronze-snapshot-lsn at all, and taking it alone would compare against null.
-- name: lineage_applied_changes_postdate_the_snapshot_export
-- type: lineage
-- table: all
-- severity: ERROR
-- expect: zero
-- description: no applied change sits at or below the export's WAL position
SELECT
    (SELECT count(*) FROM lake.silver.campaigns WHERE _lsn IS NOT NULL AND _lsn <= (
        SELECT max(cast(summary['bronze-snapshot-lsn'] AS bigint)) FROM lake.silver.campaigns.snapshots))
  + (SELECT count(*) FROM lake.silver.creatives WHERE _lsn IS NOT NULL AND _lsn <= (
        SELECT max(cast(summary['bronze-snapshot-lsn'] AS bigint)) FROM lake.silver.creatives.snapshots))
  + (SELECT count(*) FROM lake.silver.line_items WHERE _lsn IS NOT NULL AND _lsn <= (
        SELECT max(cast(summary['bronze-snapshot-lsn'] AS bigint)) FROM lake.silver.line_items.snapshots))
  + (SELECT count(*) FROM lake.silver.publishers WHERE _lsn IS NOT NULL AND _lsn <= (
        SELECT max(cast(summary['bronze-snapshot-lsn'] AS bigint)) FROM lake.silver.publishers.snapshots))
  + (SELECT count(*) FROM lake.silver.audiences WHERE _lsn IS NOT NULL AND _lsn <= (
        SELECT max(cast(summary['bronze-snapshot-lsn'] AS bigint)) FROM lake.silver.audiences.snapshots));

-- Debezium does not capture impressions, so every row should have come from the
-- snapshot export. An _lsn here means a merge ran against a table whose plan
-- should always be skip or rebuild.
-- name: lineage_event_tables_were_never_merged
-- type: lineage
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: append-only tables contain no change-event provenance
SELECT count(*) FROM lake.silver.impressions WHERE _lsn IS NOT NULL;
