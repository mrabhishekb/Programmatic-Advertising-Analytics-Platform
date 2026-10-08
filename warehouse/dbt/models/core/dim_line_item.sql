-- The bidding unit inside a campaign: what actually competes in an auction.
--
-- Kept as its own dimension rather than folded into dim_campaign. Collapsing
-- the two would denormalise campaign attributes onto every line item, which is
-- the usual Kimball advice for a shallow hierarchy and wrong here for two
-- reasons: spend_transactions and impressions both carry line_item_id and
-- campaign_id independently, so the grain already treats them as separate
-- things, and a line item's bid and pacing are the levers a trader pulls -
-- attributes nobody wants to find repeated across a campaign's worth of rows.

with members as (

    select
        {{ dbt_utils.generate_surrogate_key(['line_item_id']) }} as line_item_key,
        line_item_id,
        {{ dbt_utils.generate_surrogate_key(['campaign_id']) }}  as campaign_key,
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
        is_deleted,
        deleted_at,
        created_at,
        updated_at
    from {{ ref('stg_line_items') }}

),

unknown as (

    select
        {{ unknown_key() }}             as line_item_key,
        cast(null as varchar)           as line_item_id,
        {{ unknown_key() }}             as campaign_key,
        cast(null as varchar)           as campaign_id,
        'Unknown line item'             as line_item_name,
        'Unknown'                       as line_item_status,
        cast(0 as number(12,4))         as bid_amount,
        'XXX'                           as bid_currency,
        'Unknown'                       as optimization_goal,
        cast(0 as number(12,4))         as target_cpm,
        cast(0 as number(10,0))         as frequency_cap,
        cast(null as date)              as start_date,
        cast(null as date)              as end_date,
        false                           as is_deleted,
        cast(null as timestamp_ntz)     as deleted_at,
        cast(null as timestamp_ntz)     as created_at,
        cast(null as timestamp_ntz)     as updated_at

)

select * from members
union all
select * from unknown
