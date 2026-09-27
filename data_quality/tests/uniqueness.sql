-- Uniqueness checks.
--
-- Primary keys are enforced by the database, so these are an independent audit:
-- they would also catch a duplicate that arrived through a future CDC replay.

-- name: duplicate_primary_keys
-- type: uniqueness
-- table: all
-- severity: ERROR
-- expect: zero
-- description: no duplicate primary key on any table
SELECT
    (SELECT COUNT(*) FROM (SELECT advertiser_id FROM advertisers GROUP BY 1 HAVING COUNT(*) > 1) d)
  + (SELECT COUNT(*) FROM (SELECT campaign_id FROM campaigns GROUP BY 1 HAVING COUNT(*) > 1) d)
  + (SELECT COUNT(*) FROM (SELECT line_item_id FROM line_items GROUP BY 1 HAVING COUNT(*) > 1) d)
  + (SELECT COUNT(*) FROM (SELECT creative_id FROM creatives GROUP BY 1 HAVING COUNT(*) > 1) d)
  + (SELECT COUNT(*) FROM (SELECT publisher_id FROM publishers GROUP BY 1 HAVING COUNT(*) > 1) d)
  + (SELECT COUNT(*) FROM (SELECT placement_id FROM placements GROUP BY 1 HAVING COUNT(*) > 1) d)
  + (SELECT COUNT(*) FROM (SELECT audience_id FROM audiences GROUP BY 1 HAVING COUNT(*) > 1) d);

-- name: duplicate_impression_ids
-- type: uniqueness
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: no duplicate impression_id
SELECT COUNT(*) FROM (
    SELECT impression_id FROM impressions GROUP BY 1 HAVING COUNT(*) > 1
) duplicates;

-- name: duplicate_click_ids
-- type: uniqueness
-- table: clicks
-- severity: ERROR
-- expect: zero
-- description: no duplicate click_id
SELECT COUNT(*) FROM (
    SELECT click_id FROM clicks GROUP BY 1 HAVING COUNT(*) > 1
) duplicates;

-- name: duplicate_conversion_ids
-- type: uniqueness
-- table: conversions
-- severity: ERROR
-- expect: zero
-- description: no duplicate conversion_id
SELECT COUNT(*) FROM (
    SELECT conversion_id FROM conversions GROUP BY 1 HAVING COUNT(*) > 1
) duplicates;

-- name: multiple_clicks_per_impression
-- type: uniqueness
-- table: clicks
-- severity: WARNING
-- expect: zero
-- description: this model emits at most one click per impression
SELECT COUNT(*) FROM (
    SELECT impression_id FROM clicks GROUP BY 1 HAVING COUNT(*) > 1
) duplicates;

-- name: multiple_conversions_per_click
-- type: uniqueness
-- table: conversions
-- severity: WARNING
-- expect: zero
-- description: last-click attribution assigns at most one conversion per click
SELECT COUNT(*) FROM (
    SELECT click_id FROM conversions GROUP BY 1 HAVING COUNT(*) > 1
) duplicates;

-- name: duplicate_publisher_domains
-- type: uniqueness
-- table: publishers
-- severity: ERROR
-- expect: zero
-- description: publisher domains are unique
SELECT COUNT(*) FROM (
    SELECT domain FROM publishers GROUP BY 1 HAVING COUNT(*) > 1
) duplicates;
