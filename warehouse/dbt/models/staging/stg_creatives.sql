-- The advertisement itself. Owned by an advertiser, not by a campaign, so the
-- same creative can run across several of them.

select
    creative_id,
    advertiser_id,
    creative_name,
    creative_type,
    creative_format,
    landing_page_url,
    creative_status,
    created_at,
    updated_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'CREATIVES') }}
