-- Join validation.
--
-- The point of the whole generator is that these joins return meaningful data.
-- Each check reports the number of rows surviving the join; every one of them
-- must be greater than zero, and the row counts are printed in the run report so
-- a reader can see the funnel hold together across tables.

-- name: join_campaigns_to_advertisers
-- type: join
-- table: campaigns
-- severity: ERROR
-- expect: nonzero
-- description: campaigns joined to their advertisers
SELECT COUNT(*)
FROM campaigns c
JOIN advertisers a ON c.advertiser_id = a.advertiser_id;

-- name: join_line_items_to_campaigns
-- type: join
-- table: line_items
-- severity: ERROR
-- expect: nonzero
-- description: line items joined to their campaigns
SELECT COUNT(*)
FROM line_items li
JOIN campaigns c ON li.campaign_id = c.campaign_id;

-- name: join_creatives_to_advertisers
-- type: join
-- table: creatives
-- severity: ERROR
-- expect: nonzero
-- description: creatives joined to their advertisers
SELECT COUNT(*)
FROM creatives cr
JOIN advertisers a ON cr.advertiser_id = a.advertiser_id;

-- name: join_placements_to_publishers
-- type: join
-- table: placements
-- severity: ERROR
-- expect: nonzero
-- description: placements joined to their publishers
SELECT COUNT(*)
FROM placements p
JOIN publishers pub ON p.publisher_id = pub.publisher_id;

-- name: join_impressions_to_campaigns
-- type: join
-- table: impressions
-- severity: ERROR
-- expect: nonzero
-- description: impressions joined to their campaigns
SELECT COUNT(*)
FROM impressions i
JOIN campaigns c ON i.campaign_id = c.campaign_id;

-- name: join_impressions_to_clicks
-- type: join
-- table: clicks
-- severity: ERROR
-- expect: nonzero
-- description: impressions joined to their clicks
SELECT COUNT(*)
FROM impressions i
JOIN clicks cl ON i.impression_id = cl.impression_id;

-- name: join_clicks_to_conversions
-- type: join
-- table: conversions
-- severity: ERROR
-- expect: nonzero
-- description: clicks joined to their conversions
SELECT COUNT(*)
FROM clicks cl
JOIN conversions cv ON cl.click_id = cv.click_id;

-- name: join_impressions_to_supply
-- type: join
-- table: impressions
-- severity: ERROR
-- expect: nonzero
-- description: impressions joined to publisher and placement
SELECT COUNT(*)
FROM impressions i
JOIN publishers p  ON i.publisher_id = p.publisher_id
JOIN placements pl ON i.placement_id = pl.placement_id;

-- name: join_impressions_to_audiences
-- type: join
-- table: impressions
-- severity: ERROR
-- expect: nonzero
-- description: impressions joined to the audience they targeted
SELECT COUNT(*)
FROM impressions i
JOIN audiences au ON i.audience_id = au.audience_id;

-- name: join_spend_to_impressions
-- type: join
-- table: spend_transactions
-- severity: ERROR
-- expect: nonzero
-- description: per-event spend joined back to the impression that caused it
SELECT COUNT(*)
FROM spend_transactions st
JOIN impressions i ON st.impression_id = i.impression_id;

-- name: join_full_funnel
-- type: join
-- table: impressions
-- severity: ERROR
-- expect: nonzero
-- description: the end-to-end advertiser -> campaign -> impression -> click -> conversion chain
SELECT COUNT(*)
FROM advertisers a
JOIN campaigns c    ON a.advertiser_id = c.advertiser_id
JOIN impressions i  ON c.campaign_id = i.campaign_id
JOIN clicks cl      ON i.impression_id = cl.impression_id
JOIN conversions cv ON cl.click_id = cv.click_id;

-- name: campaigns_with_delivery
-- type: join
-- table: campaigns
-- severity: ERROR
-- expect: nonzero
-- description: campaigns that actually received impressions
SELECT COUNT(DISTINCT i.campaign_id) FROM impressions i;

-- name: publishers_with_delivery
-- type: join
-- table: publishers
-- severity: ERROR
-- expect: nonzero
-- description: publishers that actually received impressions
SELECT COUNT(DISTINCT i.publisher_id) FROM impressions i;
