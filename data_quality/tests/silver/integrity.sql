-- Invariants that must hold inside Silver, whatever the source is doing.
--
-- These are independent of how far Silver has caught up, which is what
-- separates them from the reconciliation checks: a table lagging the source is
-- normal, a table with two rows for one key is not.

-- The single most important check in the layer. Iceberg does not enforce
-- primary keys, so nothing but the merge logic prevents a key appearing twice -
-- and a MERGE whose ON clause matched more than one target row would do exactly
-- that. Phase 6 could not produce this; phase 7's incremental path can.
-- name: silver_duplicate_primary_keys
-- type: integrity
-- table: all
-- severity: ERROR
-- expect: zero
-- description: no duplicate primary key in any Silver dimension table
SELECT
    (SELECT count(*) FROM (SELECT advertiser_id FROM lake.silver.advertisers GROUP BY 1 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (SELECT publisher_id  FROM lake.silver.publishers  GROUP BY 1 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (SELECT placement_id  FROM lake.silver.placements  GROUP BY 1 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (SELECT campaign_id   FROM lake.silver.campaigns   GROUP BY 1 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (SELECT line_item_id  FROM lake.silver.line_items  GROUP BY 1 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (SELECT creative_id   FROM lake.silver.creatives   GROUP BY 1 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (SELECT audience_id   FROM lake.silver.audiences   GROUP BY 1 HAVING count(*) > 1) d);

-- A null key means the JSON payload did not carry the column the collapse
-- keyed on - schema drift between Debezium and the target, surfacing as rows
-- that can never be matched or updated again.
-- name: silver_null_primary_keys
-- type: integrity
-- table: all
-- severity: ERROR
-- expect: zero
-- description: no null primary key in any Silver dimension table
SELECT
    (SELECT count(*) FROM lake.silver.advertisers WHERE advertiser_id IS NULL)
  + (SELECT count(*) FROM lake.silver.publishers  WHERE publisher_id  IS NULL)
  + (SELECT count(*) FROM lake.silver.placements  WHERE placement_id  IS NULL)
  + (SELECT count(*) FROM lake.silver.campaigns   WHERE campaign_id   IS NULL)
  + (SELECT count(*) FROM lake.silver.line_items  WHERE line_item_id  IS NULL)
  + (SELECT count(*) FROM lake.silver.creatives   WHERE creative_id   IS NULL)
  + (SELECT count(*) FROM lake.silver.audiences   WHERE audience_id   IS NULL);

-- Soft deletes are only useful if the moment is recorded; a flagged row with no
-- timestamp cannot be excluded from an as-of query.
-- name: silver_deleted_rows_have_a_deletion_time
-- type: integrity
-- table: all
-- severity: ERROR
-- expect: zero
-- description: every soft-deleted row carries deleted_at
SELECT
    (SELECT count(*) FROM lake.silver.publishers WHERE is_deleted AND deleted_at IS NULL)
  + (SELECT count(*) FROM lake.silver.campaigns  WHERE is_deleted AND deleted_at IS NULL)
  + (SELECT count(*) FROM lake.silver.line_items WHERE is_deleted AND deleted_at IS NULL)
  + (SELECT count(*) FROM lake.silver.creatives  WHERE is_deleted AND deleted_at IS NULL)
  + (SELECT count(*) FROM lake.silver.audiences  WHERE is_deleted AND deleted_at IS NULL);

-- The inverse, and the one that catches a resurrection handled badly: a key
-- deleted and then re-inserted must come back with deleted_at cleared, not
-- merely with the flag flipped.
-- name: silver_live_rows_have_no_deletion_time
-- type: integrity
-- table: all
-- severity: ERROR
-- expect: zero
-- description: no live row carries deleted_at
SELECT
    (SELECT count(*) FROM lake.silver.publishers WHERE NOT is_deleted AND deleted_at IS NOT NULL)
  + (SELECT count(*) FROM lake.silver.campaigns  WHERE NOT is_deleted AND deleted_at IS NOT NULL)
  + (SELECT count(*) FROM lake.silver.line_items WHERE NOT is_deleted AND deleted_at IS NOT NULL)
  + (SELECT count(*) FROM lake.silver.creatives  WHERE NOT is_deleted AND deleted_at IS NOT NULL)
  + (SELECT count(*) FROM lake.silver.audiences  WHERE NOT is_deleted AND deleted_at IS NOT NULL);

-- The partitioned write is where a duplicate would come from: 100,000,000 rows
-- spread over 90 day-partitions and written by many tasks at once.
-- name: silver_duplicate_impression_ids
-- type: integrity
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: no duplicate impression_id across the partitioned event table
SELECT count(*) FROM (
    SELECT impression_id FROM lake.silver.impressions GROUP BY 1 HAVING count(*) > 1
) duplicates;

-- Debezium does not capture these tables, so the snapshot export is their only
-- input and every row should be live. A flagged row here means something wrote
-- to them through a path that is not supposed to exist.
-- name: silver_event_tables_have_no_deletions
-- type: integrity
-- table: all
-- severity: ERROR
-- expect: zero
-- description: append-only event tables contain no soft-deleted rows
SELECT
    (SELECT count(*) FROM lake.silver.impressions WHERE is_deleted)
  + (SELECT count(*) FROM lake.silver.clicks      WHERE is_deleted)
  + (SELECT count(*) FROM lake.silver.conversions WHERE is_deleted);
