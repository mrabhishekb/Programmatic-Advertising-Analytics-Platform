-- One row per billable charge.
--
-- Three grains share this table and billing_type is what tells them apart: CPM
-- is one row per campaign, line item, publisher and hour; CPC is one row per
-- click; CPA is one row per conversion. A sum across all three is a correct
-- total made of incomparable parts, which is worth knowing before writing one.
--
-- impression_id is the only nullable foreign key in the source schema, for the
-- same reason: CPM spend is an hourly rollup with no single impression behind
-- it, while CPC and CPA spend always traces to the one that started the chain.

select
    spend_transaction_id,
    campaign_id,
    line_item_id,
    advertiser_id,
    publisher_id,
    impression_id,
    spend_timestamp,
    spend_amount,
    currency,
    billing_type,
    created_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'SPEND_TRANSACTIONS') }}
