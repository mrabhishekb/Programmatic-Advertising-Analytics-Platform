-- Companies buying advertising, one row per advertiser.
--
-- Columns are listed rather than selected with *, so a column appearing in
-- RAW does not silently propagate through the whole warehouse before anyone
-- decides what it means.

select
    advertiser_id,
    advertiser_name,
    industry,
    billing_country,
    billing_currency,
    account_status,
    created_at,
    updated_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'ADVERTISERS') }}
{{ only_live_rows() }}
