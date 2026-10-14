-- One row per click.
--
-- impression_id is carried rather than dropped as redundant. Every click comes
-- from an impression, and that pointer is what makes attribution possible in
-- phase 14 - without it you can count clicks but never connect one to the ad
-- that earned it.

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
