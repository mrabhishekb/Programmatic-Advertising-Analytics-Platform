-- One row per click.
--
-- impression_id is carried rather than dropped as redundant. Every click comes
-- from an impression, and that pointer is the only route from a click to where
-- it actually happened - the source records no publisher, placement or
-- audience against a click, because the ad server already has them against the
-- impression. int_clicks_resolved is what walks it.

select
    click_id,
    impression_id,
    campaign_id,
    line_item_id,
    advertiser_id,
    creative_id,
    click_timestamp,
    device_type,
    country,
    created_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'CLICKS') }}
