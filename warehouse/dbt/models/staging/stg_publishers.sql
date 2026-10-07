-- Sites and apps selling inventory.

select
    publisher_id,
    publisher_name,
    publisher_type,
    country,
    domain,
    publisher_status,
    created_at,
    updated_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'PUBLISHERS') }}
{{ only_live_rows() }}
