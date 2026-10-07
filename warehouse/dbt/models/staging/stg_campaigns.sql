-- An advertiser's spending plan over a flight window.
--
-- campaign_status and daily_budget are the two columns the change generator
-- actually edits, which makes this the model that proves the CDC path works
-- end to end.

select
    campaign_id,
    advertiser_id,
    campaign_name,
    campaign_objective,
    campaign_status,
    campaign_budget,
    daily_budget,
    start_date,
    end_date,
    bid_strategy,
    created_at,
    updated_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'CAMPAIGNS') }}
{{ only_live_rows() }}
