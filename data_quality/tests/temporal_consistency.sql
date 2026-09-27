-- Temporal consistency checks.
--
-- The chain advertiser -> campaign -> line item -> impression -> click ->
-- conversion must never travel backwards in time. These are the checks that
-- protect point-in-time correctness in the warehouse layers built later.

-- name: campaign_created_before_advertiser
-- type: temporal
-- table: campaigns
-- severity: ERROR
-- expect: zero
-- description: a campaign is never created before the advertiser that owns it
SELECT COUNT(*)
FROM campaigns c
JOIN advertisers a ON c.advertiser_id = a.advertiser_id
WHERE c.created_at < a.created_at;

-- name: campaign_created_after_flight_start
-- type: temporal
-- table: campaigns
-- severity: ERROR
-- expect: zero
-- description: a campaign is set up before its flight opens
SELECT COUNT(*)
FROM campaigns
WHERE created_at > start_date::timestamp;

-- name: campaign_start_after_end
-- type: temporal
-- table: campaigns
-- severity: ERROR
-- expect: zero
-- description: campaign start_date precedes end_date
SELECT COUNT(*) FROM campaigns WHERE start_date >= end_date;

-- name: line_item_created_before_campaign
-- type: temporal
-- table: line_items
-- severity: ERROR
-- expect: zero
-- description: a line item is never created before its campaign
SELECT COUNT(*)
FROM line_items li
JOIN campaigns c ON li.campaign_id = c.campaign_id
WHERE li.created_at < c.created_at;

-- name: line_item_flight_outside_campaign
-- type: temporal
-- table: line_items
-- severity: ERROR
-- expect: zero
-- description: line item flight dates sit inside the campaign flight
SELECT COUNT(*)
FROM line_items li
JOIN campaigns c ON li.campaign_id = c.campaign_id
WHERE li.start_date < c.start_date OR li.end_date > c.end_date;

-- name: impression_outside_campaign_flight
-- type: temporal
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: impressions only occur while their campaign is flighted
SELECT COUNT(*)
FROM impressions i
JOIN campaigns c ON i.campaign_id = c.campaign_id
WHERE i.impression_timestamp::date < c.start_date
   OR i.impression_timestamp::date > c.end_date;

-- name: impression_outside_line_item_flight
-- type: temporal
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: impressions only occur while their line item is flighted
SELECT COUNT(*)
FROM impressions i
JOIN line_items li ON i.line_item_id = li.line_item_id
WHERE i.impression_timestamp::date < li.start_date
   OR i.impression_timestamp::date > li.end_date;

-- name: impression_before_parent_creation
-- type: temporal
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: an impression never predates the line item, creative or placement it used
SELECT COUNT(*)
FROM impressions i
JOIN line_items li ON i.line_item_id = li.line_item_id
JOIN creatives cr  ON i.creative_id  = cr.creative_id
JOIN placements pl ON i.placement_id = pl.placement_id
WHERE i.impression_timestamp < li.created_at
   OR i.impression_timestamp < cr.created_at
   OR i.impression_timestamp < pl.created_at;

-- name: click_before_impression
-- type: temporal
-- table: clicks
-- severity: ERROR
-- expect: zero
-- description: click_timestamp >= impression_timestamp
SELECT COUNT(*)
FROM clicks cl
JOIN impressions i ON cl.impression_id = i.impression_id
WHERE cl.click_timestamp < i.impression_timestamp;

-- name: conversion_before_click
-- type: temporal
-- table: conversions
-- severity: ERROR
-- expect: zero
-- description: conversion_timestamp >= click_timestamp
SELECT COUNT(*)
FROM conversions cv
JOIN clicks cl ON cv.click_id = cl.click_id
WHERE cv.conversion_timestamp < cl.click_timestamp;

-- name: conversion_outside_attribution_window
-- type: temporal
-- table: conversions
-- severity: ERROR
-- expect: zero
-- description: a conversion falls inside the attribution window it claims
SELECT COUNT(*)
FROM conversions cv
JOIN clicks cl ON cv.click_id = cl.click_id
WHERE cv.conversion_timestamp
      > cl.click_timestamp + make_interval(hours => cv.attribution_window_hours);

-- name: event_created_before_it_happened
-- type: temporal
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: created_at is never earlier than the event it records
SELECT
    (SELECT COUNT(*) FROM impressions WHERE created_at < impression_timestamp)
  + (SELECT COUNT(*) FROM clicks      WHERE created_at < click_timestamp)
  + (SELECT COUNT(*) FROM conversions WHERE created_at < conversion_timestamp)
  + (SELECT COUNT(*) FROM spend_transactions WHERE created_at < spend_timestamp);

-- name: updated_before_created
-- type: temporal
-- table: all_master
-- severity: ERROR
-- expect: zero
-- description: updated_at is never earlier than created_at on any master table
SELECT
    (SELECT COUNT(*) FROM advertisers WHERE updated_at < created_at)
  + (SELECT COUNT(*) FROM campaigns   WHERE updated_at < created_at)
  + (SELECT COUNT(*) FROM line_items  WHERE updated_at < created_at)
  + (SELECT COUNT(*) FROM creatives   WHERE updated_at < created_at)
  + (SELECT COUNT(*) FROM publishers  WHERE updated_at < created_at)
  + (SELECT COUNT(*) FROM placements  WHERE updated_at < created_at)
  + (SELECT COUNT(*) FROM audiences   WHERE updated_at < created_at);
