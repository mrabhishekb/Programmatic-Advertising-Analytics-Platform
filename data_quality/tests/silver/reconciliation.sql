-- Does Silver still agree with PostgreSQL?
--
-- This is the suite that justifies the pipeline. Every other check can pass on
-- a Silver layer that is internally immaculate and quietly wrong: a watermark
-- that advanced too far, a change event lost between Kafka and Bronze, or a
-- merge that matched on a stale key all leave a table that is self-consistent
-- and no longer reflects the source.
--
-- `pg_*` are live PostgreSQL tables exposed to Spark over JDBC; `silver_counts`
-- is built from the Iceberg tables. See data_quality/silver.py.
--
-- These assume Silver has caught up. Changes still in flight - sitting in Kafka,
-- or in Bronze but not yet merged - are legitimate drift, not a defect, which is
-- why the runner reports pending work alongside the results.

-- Live rather than total: PostgreSQL removes a deleted row, Silver keeps it
-- flagged, so the two agree only once the flagged ones are excluded. This is
-- the cheapest reconciliation and the weakest - it cannot see an update that
-- was missed, only a row that was.
-- name: reconciliation_live_row_counts_match
-- type: reconciliation
-- table: all
-- severity: ERROR
-- expect: zero
-- description: every table holds the same number of live rows as the source
SELECT count(*)
FROM pg_counts p
JOIN silver_counts s ON s.table_name = p.table_name
WHERE p.row_count <> s.live_rows;

-- An anti-join rather than a count comparison, so it survives the case where
-- one row was dropped and another inserted - the counts would agree and the
-- keys would not.
-- name: reconciliation_rows_missing_from_silver
-- type: reconciliation
-- table: all
-- severity: ERROR
-- expect: zero
-- description: every source row has a live Silver row with the same key
SELECT
    (SELECT count(*) FROM pg_advertisers p LEFT ANTI JOIN (SELECT advertiser_id FROM lake.silver.advertisers WHERE NOT is_deleted) s ON s.advertiser_id = p.advertiser_id)
  + (SELECT count(*) FROM pg_publishers  p LEFT ANTI JOIN (SELECT publisher_id  FROM lake.silver.publishers  WHERE NOT is_deleted) s ON s.publisher_id  = p.publisher_id)
  + (SELECT count(*) FROM pg_placements  p LEFT ANTI JOIN (SELECT placement_id  FROM lake.silver.placements  WHERE NOT is_deleted) s ON s.placement_id  = p.placement_id)
  + (SELECT count(*) FROM pg_campaigns   p LEFT ANTI JOIN (SELECT campaign_id   FROM lake.silver.campaigns   WHERE NOT is_deleted) s ON s.campaign_id   = p.campaign_id)
  + (SELECT count(*) FROM pg_line_items  p LEFT ANTI JOIN (SELECT line_item_id  FROM lake.silver.line_items  WHERE NOT is_deleted) s ON s.line_item_id  = p.line_item_id)
  + (SELECT count(*) FROM pg_creatives   p LEFT ANTI JOIN (SELECT creative_id   FROM lake.silver.creatives   WHERE NOT is_deleted) s ON s.creative_id   = p.creative_id)
  + (SELECT count(*) FROM pg_audiences   p LEFT ANTI JOIN (SELECT audience_id   FROM lake.silver.audiences   WHERE NOT is_deleted) s ON s.audience_id   = p.audience_id);

-- The other direction, and the one that catches a delete that never arrived:
-- the row is gone from PostgreSQL but still live in Silver, so every downstream
-- join keeps resolving it.
-- name: reconciliation_rows_missing_from_source
-- type: reconciliation
-- table: all
-- severity: ERROR
-- expect: zero
-- description: no live Silver row refers to a key the source no longer has
SELECT
    (SELECT count(*) FROM (SELECT advertiser_id FROM lake.silver.advertisers WHERE NOT is_deleted) s LEFT ANTI JOIN pg_advertisers p ON s.advertiser_id = p.advertiser_id)
  + (SELECT count(*) FROM (SELECT publisher_id  FROM lake.silver.publishers  WHERE NOT is_deleted) s LEFT ANTI JOIN pg_publishers  p ON s.publisher_id  = p.publisher_id)
  + (SELECT count(*) FROM (SELECT placement_id  FROM lake.silver.placements  WHERE NOT is_deleted) s LEFT ANTI JOIN pg_placements  p ON s.placement_id  = p.placement_id)
  + (SELECT count(*) FROM (SELECT campaign_id   FROM lake.silver.campaigns   WHERE NOT is_deleted) s LEFT ANTI JOIN pg_campaigns   p ON s.campaign_id   = p.campaign_id)
  + (SELECT count(*) FROM (SELECT line_item_id  FROM lake.silver.line_items  WHERE NOT is_deleted) s LEFT ANTI JOIN pg_line_items  p ON s.line_item_id  = p.line_item_id)
  + (SELECT count(*) FROM (SELECT creative_id   FROM lake.silver.creatives   WHERE NOT is_deleted) s LEFT ANTI JOIN pg_creatives   p ON s.creative_id   = p.creative_id)
  + (SELECT count(*) FROM (SELECT audience_id   FROM lake.silver.audiences   WHERE NOT is_deleted) s LEFT ANTI JOIN pg_audiences   p ON s.audience_id   = p.audience_id);

-- The check that earns its keep. These are the columns the change traffic
-- actually mutates, so a merge that stopped applying updates - a watermark too
-- high, a dropped event - shows up here while every count still agrees.
--
-- `<=>` is null-safe: a plain `<>` would treat a genuine null-to-null match as
-- unknown and never count it, which is the failure mode where a check reports
-- zero violations because it compared nothing.
-- name: reconciliation_campaign_values_match
-- type: reconciliation
-- table: campaigns
-- severity: ERROR
-- expect: zero
-- description: campaign_status and daily_budget agree with the source row by row
SELECT count(*)
FROM lake.silver.campaigns s
JOIN pg_campaigns p ON p.campaign_id = s.campaign_id
WHERE NOT s.is_deleted
  AND (NOT (s.campaign_status <=> p.campaign_status)
       OR NOT (cast(s.daily_budget AS decimal(18,4)) <=> cast(p.daily_budget AS decimal(18,4))));

-- name: reconciliation_line_item_bids_match
-- type: reconciliation
-- table: line_items
-- severity: ERROR
-- expect: zero
-- description: bid_amount agrees with the source row by row
SELECT count(*)
FROM lake.silver.line_items s
JOIN pg_line_items p ON p.line_item_id = s.line_item_id
WHERE NOT s.is_deleted
  AND NOT (cast(s.bid_amount AS decimal(18,4)) <=> cast(p.bid_amount AS decimal(18,4)));

-- name: reconciliation_creative_status_matches
-- type: reconciliation
-- table: creatives
-- severity: ERROR
-- expect: zero
-- description: creative_status agrees with the source row by row
SELECT count(*)
FROM lake.silver.creatives s
JOIN pg_creatives p ON p.creative_id = s.creative_id
WHERE NOT s.is_deleted AND NOT (s.creative_status <=> p.creative_status);

-- name: reconciliation_publisher_status_matches
-- type: reconciliation
-- table: publishers
-- severity: ERROR
-- expect: zero
-- description: publisher_status agrees with the source row by row
SELECT count(*)
FROM lake.silver.publishers s
JOIN pg_publishers p ON p.publisher_id = s.publisher_id
WHERE NOT s.is_deleted AND NOT (s.publisher_status <=> p.publisher_status);

-- Catches the opposite mistake to a missed delete: a row flagged on the
-- strength of a tombstone or a misread operation, while the source still has
-- it. Downstream that row simply disappears, which is harder to notice than a
-- row that lingers.
-- name: reconciliation_soft_deletes_are_gone_from_the_source
-- type: reconciliation
-- table: all
-- severity: ERROR
-- expect: zero
-- description: no row flagged deleted in Silver still exists in the source
SELECT
    (SELECT count(*) FROM (SELECT campaign_id  FROM lake.silver.campaigns  WHERE is_deleted) s JOIN pg_campaigns  p ON s.campaign_id  = p.campaign_id)
  + (SELECT count(*) FROM (SELECT creative_id  FROM lake.silver.creatives  WHERE is_deleted) s JOIN pg_creatives  p ON s.creative_id  = p.creative_id)
  + (SELECT count(*) FROM (SELECT line_item_id FROM lake.silver.line_items WHERE is_deleted) s JOIN pg_line_items p ON s.line_item_id = p.line_item_id)
  + (SELECT count(*) FROM (SELECT publisher_id FROM lake.silver.publishers WHERE is_deleted) s JOIN pg_publishers p ON s.publisher_id = p.publisher_id)
  + (SELECT count(*) FROM (SELECT audience_id  FROM lake.silver.audiences  WHERE is_deleted) s JOIN pg_audiences  p ON s.audience_id  = p.audience_id);
