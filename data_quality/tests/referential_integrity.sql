-- Referential integrity checks.
--
-- Foreign keys already enforce existence at write time; these checks verify it
-- independently from the outside, and go further by asserting that derived
-- events *agree* with their parents rather than merely pointing at something
-- that exists.
--
-- Every check returns a single numeric column: the number of violating rows.

-- name: orphan_campaigns
-- type: referential
-- table: campaigns
-- severity: ERROR
-- expect: zero
-- description: every campaign references an existing advertiser
SELECT COUNT(*)
FROM campaigns c
LEFT JOIN advertisers a ON c.advertiser_id = a.advertiser_id
WHERE a.advertiser_id IS NULL;

-- name: orphan_line_items
-- type: referential
-- table: line_items
-- severity: ERROR
-- expect: zero
-- description: every line item references an existing campaign
SELECT COUNT(*)
FROM line_items li
LEFT JOIN campaigns c ON li.campaign_id = c.campaign_id
WHERE c.campaign_id IS NULL;

-- name: orphan_creatives
-- type: referential
-- table: creatives
-- severity: ERROR
-- expect: zero
-- description: every creative references an existing advertiser
SELECT COUNT(*)
FROM creatives cr
LEFT JOIN advertisers a ON cr.advertiser_id = a.advertiser_id
WHERE a.advertiser_id IS NULL;

-- name: orphan_placements
-- type: referential
-- table: placements
-- severity: ERROR
-- expect: zero
-- description: every placement references an existing publisher
SELECT COUNT(*)
FROM placements p
LEFT JOIN publishers pub ON p.publisher_id = pub.publisher_id
WHERE pub.publisher_id IS NULL;

-- name: orphan_impression_foreign_keys
-- type: referential
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: all seven impression foreign keys resolve
SELECT COUNT(*)
FROM impressions i
LEFT JOIN campaigns   c   ON i.campaign_id   = c.campaign_id
LEFT JOIN line_items  li  ON i.line_item_id  = li.line_item_id
LEFT JOIN advertisers a   ON i.advertiser_id = a.advertiser_id
LEFT JOIN creatives   cr  ON i.creative_id   = cr.creative_id
LEFT JOIN publishers  pub ON i.publisher_id  = pub.publisher_id
LEFT JOIN placements  pl  ON i.placement_id  = pl.placement_id
LEFT JOIN audiences   au  ON i.audience_id   = au.audience_id
WHERE c.campaign_id   IS NULL
   OR li.line_item_id IS NULL
   OR a.advertiser_id IS NULL
   OR cr.creative_id  IS NULL
   OR pub.publisher_id IS NULL
   OR pl.placement_id IS NULL
   OR au.audience_id  IS NULL;

-- name: impression_line_item_belongs_to_campaign
-- type: referential
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: the line item on an impression belongs to the campaign on that impression
SELECT COUNT(*)
FROM impressions i
JOIN line_items li ON i.line_item_id = li.line_item_id
WHERE li.campaign_id <> i.campaign_id;

-- name: impression_advertiser_matches_campaign
-- type: referential
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: the advertiser on an impression owns the campaign on that impression
SELECT COUNT(*)
FROM impressions i
JOIN campaigns c ON i.campaign_id = c.campaign_id
WHERE c.advertiser_id <> i.advertiser_id;

-- name: impression_creative_belongs_to_advertiser
-- type: referential
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: the creative on an impression belongs to the advertiser on that impression
SELECT COUNT(*)
FROM impressions i
JOIN creatives cr ON i.creative_id = cr.creative_id
WHERE cr.advertiser_id <> i.advertiser_id;

-- name: impression_placement_belongs_to_publisher
-- type: referential
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: the placement on an impression belongs to the publisher on that impression
SELECT COUNT(*)
FROM impressions i
JOIN placements pl ON i.placement_id = pl.placement_id
WHERE pl.publisher_id <> i.publisher_id;

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

-- name: click_attributes_match_impression
-- type: referential
-- table: clicks
-- severity: ERROR
-- expect: zero
-- description: campaign, line item, advertiser and creative on a click equal those on its impression
SELECT COUNT(*)
FROM clicks cl
JOIN impressions i ON cl.impression_id = i.impression_id
WHERE cl.campaign_id   <> i.campaign_id
   OR cl.line_item_id  <> i.line_item_id
   OR cl.advertiser_id <> i.advertiser_id
   OR cl.creative_id   <> i.creative_id;

-- name: orphan_conversions
-- type: referential
-- table: conversions
-- severity: ERROR
-- expect: zero
-- description: every conversion references an existing click and impression
SELECT COUNT(*)
FROM conversions cv
LEFT JOIN clicks cl      ON cv.click_id = cl.click_id
LEFT JOIN impressions i  ON cv.impression_id = i.impression_id
WHERE cl.click_id IS NULL OR i.impression_id IS NULL;

-- name: conversion_attributes_match_click
-- type: referential
-- table: conversions
-- severity: ERROR
-- expect: zero
-- description: impression, campaign and advertiser on a conversion equal those on its click
SELECT COUNT(*)
FROM conversions cv
JOIN clicks cl ON cv.click_id = cl.click_id
WHERE cv.impression_id <> cl.impression_id
   OR cv.campaign_id   <> cl.campaign_id
   OR cv.advertiser_id <> cl.advertiser_id;

-- name: orphan_spend_transactions
-- type: referential
-- table: spend_transactions
-- severity: ERROR
-- expect: zero
-- description: spend references an existing campaign, line item, advertiser and publisher
SELECT COUNT(*)
FROM spend_transactions st
LEFT JOIN campaigns   c   ON st.campaign_id   = c.campaign_id
LEFT JOIN line_items  li  ON st.line_item_id  = li.line_item_id
LEFT JOIN advertisers a   ON st.advertiser_id = a.advertiser_id
LEFT JOIN publishers  pub ON st.publisher_id  = pub.publisher_id
WHERE c.campaign_id    IS NULL
   OR li.line_item_id  IS NULL
   OR a.advertiser_id  IS NULL
   OR pub.publisher_id IS NULL;

-- name: spend_line_item_belongs_to_campaign
-- type: referential
-- table: spend_transactions
-- severity: ERROR
-- expect: zero
-- description: the line item billed belongs to the campaign billed
SELECT COUNT(*)
FROM spend_transactions st
JOIN line_items li ON st.line_item_id = li.line_item_id
WHERE li.campaign_id <> st.campaign_id;

-- name: spend_impression_reference_valid
-- type: referential
-- table: spend_transactions
-- severity: ERROR
-- expect: zero
-- description: when spend names an impression, that impression exists and belongs to the same campaign
SELECT COUNT(*)
FROM spend_transactions st
LEFT JOIN impressions i ON st.impression_id = i.impression_id
WHERE st.impression_id IS NOT NULL
  AND (i.impression_id IS NULL OR i.campaign_id <> st.campaign_id);
