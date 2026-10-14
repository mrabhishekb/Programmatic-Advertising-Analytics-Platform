-- Grain: three of them, in one table, told apart by billing_type.
--
--   CPM - one row per campaign, line item, publisher and hour
--   CPC - one row per click
--   CPA - one row per conversion
--
-- Mixed grain in a single fact is normally a modelling mistake. It is right
-- here because the table answers one question - what were we billed - and the
-- billing system genuinely charges at three different granularities. Splitting
-- it into three facts would make "total spend" a union that every query has to
-- remember to write. Keeping it together makes the sum correct by default and
-- makes billing_type the column you add to a GROUP BY the moment you want to
-- compare rates rather than totals. A sum across all three is a real number
-- made of incomparable parts, which is the thing to know before writing one.
--
-- This is also the table the unknown member was built for. impression_id is
-- the only nullable foreign key in the whole source schema: CPM spend is an
-- hourly rollup with no single impression behind it. It stays a degenerate
-- dimension rather than becoming a key, so the null is visible instead of
-- being filed under Unknown.

{{ config(
    materialized='incremental',
    unique_key='spend_transaction_id',
    incremental_strategy='merge',
    incremental_predicates=[merge_floor()]
) }}

with events as (
    select *
    from {{ ref('stg_spend_transactions') }}
    {{ incremental_watermark('spend_timestamp') }}
),

keyed as (
    select
        {{ date_key('e.spend_timestamp') }}                                 as date_key,
        e.spend_timestamp,

        -- Degenerate dimensions. impression_id is null for every CPM row.
        e.spend_transaction_id,
        e.impression_id,

        -- Type 1 keys.
        {{ dbt_utils.generate_surrogate_key(['e.advertiser_id']) }}         as advertiser_key,
        {{ dbt_utils.generate_surrogate_key(['e.campaign_id']) }}           as campaign_key,
        {{ dbt_utils.generate_surrogate_key(['e.line_item_id']) }}          as line_item_key,
        {{ dbt_utils.generate_surrogate_key(['e.publisher_id']) }}          as publisher_key,

        -- Type 2 keys. Campaign budget and line item bid are exactly the
        -- attributes a spend question asks about - whether a campaign
        -- overspent depends on the budget that was in force at the time, not
        -- the one it has today.
        coalesce(ch.campaign_version_key,  {{ unknown_key() }})             as campaign_version_key,
        coalesce(lh.line_item_version_key, {{ unknown_key() }})             as line_item_version_key,

        -- Measure. Additive, and already in currency units rather than a CPM,
        -- so unlike fct_impressions nothing needs dividing.
        e.spend_amount,
        e.currency,

        -- Not context and not a measure: the column that says which grain
        -- this row is at.
        e.billing_type
    from events as e
    {{ join_version('dim_campaign_history',  'ch', 'campaign_id',  'spend_timestamp') }}
    {{ join_version('dim_line_item_history', 'lh', 'line_item_id', 'spend_timestamp') }}
)

select * from keyed
