-- One row per ad served. 100 million of them.
--
-- The lineage columns come through like everywhere else, and are null on every
-- row here: these tables are append-only and bypass Debezium entirely, so
-- there is no change event behind any of them. Carrying them anyway keeps all
-- eleven staging models the same shape, which is worth more than saving five
-- columns on a view that stores nothing.

select
    impression_id,
    campaign_id,
    line_item_id,
    advertiser_id,
    creative_id,
    publisher_id,
    placement_id,
    audience_id,
    impression_timestamp,
    device_type,
    operating_system,
    browser,
    country,
    city,
    bid_price,
    clearing_price,
    currency,
    viewability_score,
    created_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'IMPRESSIONS') }}
