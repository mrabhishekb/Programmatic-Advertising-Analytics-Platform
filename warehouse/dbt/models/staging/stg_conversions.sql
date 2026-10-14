-- One row per conversion, attributed last-click.
--
-- Fewer dimensions than the other three events, which is faithful to the
-- source rather than an omission: a conversion is attributed to a campaign,
-- and reaching back to the creative or the placement means walking the chain
-- through click_id and impression_id.

select
    conversion_id,
    click_id,
    impression_id,
    campaign_id,
    advertiser_id,
    conversion_type,
    conversion_timestamp,
    conversion_value,
    currency,
    attribution_window_hours,
    created_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'CONVERSIONS') }}
