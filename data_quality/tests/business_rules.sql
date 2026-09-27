-- Business rule and column-level checks: enum domains, nullability, value ranges.

-- name: invalid_enum_values
-- type: business_rule
-- table: all
-- severity: ERROR
-- expect: zero
-- description: every enumerated column holds one of its allowed values
SELECT
    (SELECT COUNT(*) FROM advertisers WHERE account_status NOT IN ('ACTIVE','SUSPENDED','CLOSED'))
  + (SELECT COUNT(*) FROM campaigns WHERE campaign_objective NOT IN ('AWARENESS','TRAFFIC','ENGAGEMENT','CONVERSIONS','APP_INSTALLS'))
  + (SELECT COUNT(*) FROM campaigns WHERE campaign_status NOT IN ('DRAFT','ACTIVE','PAUSED','COMPLETED','CANCELLED'))
  + (SELECT COUNT(*) FROM campaigns WHERE bid_strategy NOT IN ('CPC','CPM','CPA','TARGET_ROAS','MAX_CONVERSIONS'))
  + (SELECT COUNT(*) FROM line_items WHERE optimization_goal NOT IN ('CLICKS','CONVERSIONS','REVENUE','REACH','IMPRESSIONS'))
  + (SELECT COUNT(*) FROM creatives WHERE creative_type NOT IN ('BANNER','VIDEO','NATIVE','AUDIO'))
  + (SELECT COUNT(*) FROM creatives WHERE creative_status NOT IN ('ACTIVE','PAUSED','REJECTED','EXPIRED'))
  + (SELECT COUNT(*) FROM publishers WHERE publisher_type NOT IN ('WEBSITE','MOBILE_APP','CTV','AUDIO'))
  + (SELECT COUNT(*) FROM placements WHERE placement_type NOT IN ('HEADER','SIDEBAR','IN_FEED','VIDEO_PRE_ROLL','VIDEO_MID_ROLL','APP_BANNER','APP_INTERSTITIAL'))
  + (SELECT COUNT(*) FROM audiences WHERE audience_type NOT IN ('DEMOGRAPHIC','INTEREST','BEHAVIORAL','LOOKALIKE','RETARGETING'))
  + (SELECT COUNT(*) FROM impressions WHERE device_type NOT IN ('DESKTOP','MOBILE','TABLET','CTV','SMART_SPEAKER'))
  + (SELECT COUNT(*) FROM conversions WHERE conversion_type NOT IN ('PURCHASE','SIGNUP','LEAD','APP_INSTALL','SUBSCRIPTION'))
  + (SELECT COUNT(*) FROM spend_transactions WHERE billing_type NOT IN ('CPM','CPC','CPA'));

-- name: unexpected_nulls
-- type: business_rule
-- table: all
-- severity: ERROR
-- expect: zero
-- description: business-critical columns are populated
SELECT
    (SELECT COUNT(*) FROM advertisers WHERE advertiser_name IS NULL OR billing_currency IS NULL)
  + (SELECT COUNT(*) FROM campaigns WHERE campaign_name IS NULL OR start_date IS NULL OR end_date IS NULL)
  + (SELECT COUNT(*) FROM impressions WHERE country IS NULL OR city IS NULL OR currency IS NULL)
  + (SELECT COUNT(*) FROM clicks WHERE country IS NULL OR device_type IS NULL)
  + (SELECT COUNT(*) FROM conversions WHERE conversion_value IS NULL OR currency IS NULL);

-- name: negative_money
-- type: business_rule
-- table: all
-- severity: ERROR
-- expect: zero
-- description: no negative spend, prices, budgets or conversion values
SELECT
    (SELECT COUNT(*) FROM spend_transactions WHERE spend_amount < 0)
  + (SELECT COUNT(*) FROM impressions WHERE bid_price < 0 OR clearing_price < 0)
  + (SELECT COUNT(*) FROM placements WHERE floor_price < 0)
  + (SELECT COUNT(*) FROM line_items WHERE bid_amount <= 0 OR target_cpm <= 0)
  + (SELECT COUNT(*) FROM campaigns WHERE campaign_budget <= 0 OR daily_budget <= 0)
  + (SELECT COUNT(*) FROM conversions WHERE conversion_value < 0);

-- name: viewability_out_of_range
-- type: business_rule
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: viewability_score is a fraction between 0 and 1
SELECT COUNT(*) FROM impressions WHERE viewability_score < 0 OR viewability_score > 1;

-- name: clearing_price_above_bid
-- type: business_rule
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: a second-price auction never clears above the bid
SELECT COUNT(*) FROM impressions WHERE clearing_price > bid_price;

-- name: clearing_price_below_floor
-- type: business_rule
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: a winning impression never clears below the placement floor price
SELECT COUNT(*)
FROM impressions i
JOIN placements pl ON i.placement_id = pl.placement_id
WHERE i.clearing_price < pl.floor_price;

-- name: audience_age_range_invalid
-- type: business_rule
-- table: audiences
-- severity: ERROR
-- expect: zero
-- description: min_age is strictly below max_age
SELECT COUNT(*) FROM audiences WHERE min_age >= max_age;

-- name: daily_budget_above_total
-- type: business_rule
-- table: campaigns
-- severity: ERROR
-- expect: zero
-- description: daily budget never exceeds the total campaign budget
SELECT COUNT(*) FROM campaigns WHERE daily_budget > campaign_budget;

-- name: campaign_status_contradicts_dates
-- type: business_rule
-- table: campaigns
-- severity: ERROR
-- expect: zero
-- description: ACTIVE brackets today, COMPLETED/CANCELLED ended, DRAFT has not started
SELECT COUNT(*)
FROM campaigns c
WHERE (c.campaign_status = 'ACTIVE'
       AND NOT (c.start_date <= %(simulation_end)s AND %(simulation_end)s <= c.end_date))
   OR (c.campaign_status IN ('COMPLETED', 'CANCELLED') AND c.end_date >= %(simulation_end)s)
   OR (c.campaign_status = 'DRAFT' AND c.start_date <= %(simulation_end)s);

-- name: currency_not_iso_format
-- type: business_rule
-- table: all
-- severity: ERROR
-- expect: zero
-- description: currency codes are three uppercase letters
SELECT
    (SELECT COUNT(*) FROM advertisers WHERE billing_currency !~ '^[A-Z]{3}$')
  + (SELECT COUNT(*) FROM line_items  WHERE bid_currency    !~ '^[A-Z]{3}$')
  + (SELECT COUNT(*) FROM placements  WHERE currency        !~ '^[A-Z]{3}$')
  + (SELECT COUNT(*) FROM impressions WHERE currency        !~ '^[A-Z]{3}$')
  + (SELECT COUNT(*) FROM conversions WHERE currency        !~ '^[A-Z]{3}$')
  + (SELECT COUNT(*) FROM spend_transactions WHERE currency !~ '^[A-Z]{3}$');

-- name: spend_impression_reference_inconsistent_with_billing_type
-- type: business_rule
-- table: spend_transactions
-- severity: ERROR
-- expect: zero
-- description: CPM spend is an hourly roll-up (no impression), CPC/CPA spend names its impression
SELECT COUNT(*)
FROM spend_transactions
WHERE (billing_type = 'CPM' AND impression_id IS NOT NULL)
   OR (billing_type IN ('CPC','CPA') AND impression_id IS NULL);

-- name: draft_campaign_delivered_impressions
-- type: business_rule
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: a campaign that never launched cannot have served impressions
SELECT COUNT(*)
FROM impressions i
JOIN campaigns c ON i.campaign_id = c.campaign_id
WHERE c.campaign_status = 'DRAFT';

-- name: impression_country_unknown
-- type: business_rule
-- table: impressions
-- severity: ERROR
-- expect: zero
-- description: impression geography resolves to a known country
SELECT COUNT(*)
FROM impressions i
LEFT JOIN ref_country rc ON i.country = rc.country_name
WHERE rc.country_code IS NULL;
