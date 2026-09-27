-- =============================================================================
-- Example analytical queries
-- =============================================================================
-- These are the queries the generator is designed to make meaningful. Run them
-- with `make queries`, or paste them into psql.
-- =============================================================================

\echo '--- 1. The headline join: advertiser -> campaign -> impression -> click -> conversion -> spend'
-- Note on grain: joining spend on impression_id only picks up CPC and CPA spend,
-- because CPM spend is stored as an hourly roll-up with impression_id NULL (see
-- docs/data_model.md). CPM-billed campaigns therefore show 0.00 here. Query 2
-- aggregates spend at the campaign grain, which is the correct way to report it.
SELECT
    a.advertiser_name,
    c.campaign_name,
    COUNT(DISTINCT i.impression_id) AS impressions,
    COUNT(DISTINCT cl.click_id)     AS clicks,
    COUNT(DISTINCT cv.conversion_id) AS conversions,
    ROUND(COALESCE(SUM(st.spend_amount), 0), 2) AS spend
FROM advertisers a
JOIN campaigns c        ON a.advertiser_id = c.advertiser_id
JOIN impressions i      ON c.campaign_id = i.campaign_id
LEFT JOIN clicks cl     ON i.impression_id = cl.impression_id
LEFT JOIN conversions cv ON cl.click_id = cv.click_id
LEFT JOIN spend_transactions st ON i.impression_id = st.impression_id
GROUP BY a.advertiser_name, c.campaign_name
ORDER BY impressions DESC
LIMIT 10;

\echo '--- 2. Campaign performance with every headline metric, division by zero handled'
WITH delivery AS (
    SELECT
        i.campaign_id,
        COUNT(*)                                   AS impressions,
        COUNT(cl.click_id)                         AS clicks,
        COUNT(cv.conversion_id)                    AS conversions,
        COALESCE(SUM(cv.conversion_value), 0)      AS conversion_value
    FROM impressions i
    LEFT JOIN clicks cl      ON i.impression_id = cl.impression_id
    LEFT JOIN conversions cv ON cl.click_id = cv.click_id
    GROUP BY i.campaign_id
), spend AS (
    SELECT campaign_id, SUM(spend_amount) AS spend
    FROM spend_transactions
    GROUP BY campaign_id
)
SELECT
    c.campaign_name,
    c.campaign_objective,
    c.bid_strategy,
    d.impressions,
    d.clicks,
    d.conversions,
    ROUND(s.spend, 2)                                              AS spend,
    ROUND(100.0 * d.clicks      / NULLIF(d.impressions, 0), 4)     AS ctr_pct,
    ROUND(100.0 * d.conversions / NULLIF(d.clicks, 0), 4)          AS cvr_pct,
    ROUND(1000 * s.spend        / NULLIF(d.impressions, 0), 4)     AS cpm,
    ROUND(s.spend               / NULLIF(d.clicks, 0), 4)          AS cpc,
    ROUND(s.spend               / NULLIF(d.conversions, 0), 4)     AS cpa,
    ROUND(d.conversion_value    / NULLIF(s.spend, 0), 2)           AS roas
FROM delivery d
JOIN campaigns c ON d.campaign_id = c.campaign_id
JOIN spend s     ON d.campaign_id = s.campaign_id
WHERE d.impressions > 1000
ORDER BY d.impressions DESC
LIMIT 15;

\echo '--- 3. Creative performance: proof that some creatives genuinely beat others'
SELECT
    cr.creative_type,
    cr.creative_format,
    COUNT(*)                                                    AS impressions,
    COUNT(cl.click_id)                                          AS clicks,
    ROUND(100.0 * COUNT(cl.click_id) / COUNT(*), 4)             AS ctr_pct
FROM impressions i
JOIN creatives cr    ON i.creative_id = cr.creative_id
LEFT JOIN clicks cl  ON i.impression_id = cl.impression_id
GROUP BY cr.creative_type, cr.creative_format
HAVING COUNT(*) > 500
ORDER BY ctr_pct DESC
LIMIT 15;

\echo '--- 4. Publisher performance and the concentration of supply'
SELECT
    p.publisher_name,
    p.publisher_type,
    p.country,
    COUNT(*)                                            AS impressions,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3)  AS share_of_all_impressions_pct,
    ROUND(AVG(i.clearing_price), 4)                     AS avg_clearing_cpm,
    ROUND(AVG(i.viewability_score), 4)                  AS avg_viewability
FROM impressions i
JOIN publishers p ON i.publisher_id = p.publisher_id
GROUP BY p.publisher_name, p.publisher_type, p.country
ORDER BY impressions DESC
LIMIT 10;

\echo '--- 5. Audience performance: retargeting should convert best'
SELECT
    au.audience_type,
    COUNT(*)                                                       AS impressions,
    COUNT(cl.click_id)                                             AS clicks,
    COUNT(cv.conversion_id)                                        AS conversions,
    ROUND(100.0 * COUNT(cl.click_id) / COUNT(*), 4)                AS ctr_pct,
    ROUND(100.0 * COUNT(cv.conversion_id)
          / NULLIF(COUNT(cl.click_id), 0), 4)                      AS cvr_pct
FROM impressions i
JOIN audiences au        ON i.audience_id = au.audience_id
LEFT JOIN clicks cl      ON i.impression_id = cl.impression_id
LEFT JOIN conversions cv ON cl.click_id = cv.click_id
GROUP BY au.audience_type
ORDER BY cvr_pct DESC NULLS LAST;

\echo '--- 6. Device and geography performance'
SELECT
    i.device_type,
    i.country,
    COUNT(*)                                        AS impressions,
    COUNT(cl.click_id)                              AS clicks,
    ROUND(100.0 * COUNT(cl.click_id) / COUNT(*), 4) AS ctr_pct
FROM impressions i
LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
GROUP BY i.device_type, i.country
HAVING COUNT(*) > 1000
ORDER BY impressions DESC
LIMIT 12;

\echo '--- 7. Daily delivery trend, showing weekday seasonality'
SELECT
    i.impression_timestamp::date        AS day,
    TO_CHAR(i.impression_timestamp, 'Dy') AS weekday,
    COUNT(*)                            AS impressions,
    COUNT(cl.click_id)                  AS clicks
FROM impressions i
LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
GROUP BY 1, 2
ORDER BY day DESC
LIMIT 14;

\echo '--- 8. Long tail: how concentrated is campaign delivery?'
WITH per_campaign AS (
    SELECT campaign_id, COUNT(*) AS impressions
    FROM impressions
    GROUP BY campaign_id
), ranked AS (
    SELECT
        NTILE(10) OVER (ORDER BY impressions DESC) AS decile,
        impressions
    FROM per_campaign
)
SELECT
    decile,
    COUNT(*)                                             AS campaigns,
    SUM(impressions)                                     AS impressions,
    ROUND(100.0 * SUM(impressions) / SUM(SUM(impressions)) OVER (), 2) AS share_pct
FROM ranked
GROUP BY decile
ORDER BY decile;

\echo '--- 9. Last-click attribution: conversions traced back to the impression that started them'
SELECT
    cv.conversion_type,
    COUNT(*)                                                             AS conversions,
    ROUND(AVG(cv.conversion_value), 2)                                   AS avg_value,
    ROUND(AVG(EXTRACT(EPOCH FROM (cv.conversion_timestamp - cl.click_timestamp)) / 3600), 2)
                                                                         AS avg_hours_click_to_conversion,
    ROUND(AVG(EXTRACT(EPOCH FROM (cl.click_timestamp - i.impression_timestamp))), 1)
                                                                         AS avg_seconds_impression_to_click
FROM conversions cv
JOIN clicks cl     ON cv.click_id = cl.click_id
JOIN impressions i ON cl.impression_id = i.impression_id
GROUP BY cv.conversion_type
ORDER BY conversions DESC;

\echo '--- 10. Spend reconciliation by billing type'
SELECT
    st.billing_type,
    COUNT(*)                              AS transactions,
    COUNT(st.impression_id)               AS with_impression_reference,
    ROUND(SUM(st.spend_amount), 2)        AS spend
FROM spend_transactions st
GROUP BY st.billing_type
ORDER BY spend DESC;
