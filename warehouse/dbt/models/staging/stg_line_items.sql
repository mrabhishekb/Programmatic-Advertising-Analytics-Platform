-- The bidding unit within a campaign: what actually competes in an auction.

select
    line_item_id,
    campaign_id,
    line_item_name,
    line_item_status,
    bid_amount,
    bid_currency,
    optimization_goal,
    target_cpm,
    frequency_cap,
    start_date,
    end_date,
    created_at,
    updated_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'LINE_ITEMS') }}
