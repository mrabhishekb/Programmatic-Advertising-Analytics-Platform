-- A specific ad slot on a publisher's property, with the floor it will accept.

select
    placement_id,
    publisher_id,
    placement_name,
    placement_type,
    ad_format,
    floor_price,
    currency,
    device_type,
    created_at,
    updated_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'PLACEMENTS') }}
