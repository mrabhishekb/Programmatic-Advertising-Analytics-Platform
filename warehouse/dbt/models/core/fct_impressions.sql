-- Grain: one row per ad served. 100 million of them, and the reason almost
-- every decision in this file is a cost decision.
--
-- No surrogate key of its own. A fact table does not need one: impression_id
-- already identifies the row, and it has to be carried anyway as a degenerate
-- dimension - the handle that lets a click in fct_clicks point back at the ad
-- that earned it. Hashing 100 million rows to produce a second unique column
-- would buy nothing.
--
-- The dimension UUIDs are dropped and only the surrogate keys kept. Carrying
-- both would add seven 36-character columns to the largest table in the
-- warehouse to store something a join already answers. The degenerate keys
-- stay because nothing to join to exists for them.
--
-- Two version keys, not seven. A version key is worth its storage when the
-- dimension's attributes change in ways that change how the measure reads:
-- a campaign's budget and status and a line item's bid price all move, and all
-- of them explain why an impression cost what it did. Advertiser, publisher,
-- placement and audience attributes are effectively static here - phase 11
-- measured zero new versions for advertisers and placements - so three more
-- columns on 100 million rows would be paid for and never read.

{{ config(
    materialized='incremental',
    unique_key='impression_id',
    incremental_strategy='merge',
    incremental_predicates=[merge_floor()]
) }}

-- No cluster_by. The rows arrive date-ordered out of a Silver export that is
-- already partitioned by event date, so Snowflake's natural micro-partitioning
-- prunes on date_key without being told to. Declaring a clustering key would
-- switch on the automatic clustering service, which bills continuously in the
-- background whether or not anything is querying - a real cost for an ordering
-- the load already provides. merge_floor() above is what keeps the merge from
-- rewriting history: without it, merging a day of new impressions scans all
-- 100 million rows looking for keys to update.

with events as (
    select *
    from {{ ref('stg_impressions') }}
    {{ incremental_watermark('impression_timestamp') }}
),

keyed as (
    select
        -- When the ad was served.
        {{ date_key('e.impression_timestamp') }}                            as date_key,
        e.impression_timestamp,

        -- Degenerate dimension: an identifier with no dimension behind it.
        e.impression_id,

        -- Type 1 keys: what the member is now.
        {{ dbt_utils.generate_surrogate_key(['e.advertiser_id']) }}         as advertiser_key,
        {{ dbt_utils.generate_surrogate_key(['e.campaign_id']) }}           as campaign_key,
        {{ dbt_utils.generate_surrogate_key(['e.line_item_id']) }}          as line_item_key,
        {{ dbt_utils.generate_surrogate_key(['e.creative_id']) }}           as creative_key,
        {{ dbt_utils.generate_surrogate_key(['e.publisher_id']) }}          as publisher_key,
        {{ dbt_utils.generate_surrogate_key(['e.placement_id']) }}          as placement_key,
        {{ dbt_utils.generate_surrogate_key(['e.audience_id']) }}           as audience_key,

        -- Type 2 keys: what the member was at the moment of the impression.
        -- Coalesced so the column is always joinable. A null here means the
        -- impression is timestamped before its campaign existed, which the
        -- relationships tests would otherwise report as a broken key rather
        -- than as the clock problem it actually is.
        coalesce(ch.campaign_version_key,  {{ unknown_key() }})             as campaign_version_key,
        coalesce(lh.line_item_version_key, {{ unknown_key() }})             as line_item_version_key,

        -- Context the event carries itself. These describe the impression,
        -- not a member, so they belong on the fact rather than in a dimension.
        e.device_type,
        e.operating_system,
        e.browser,
        e.country,
        e.city,

        -- Measures.
        e.bid_price,
        e.clearing_price,
        e.currency,

        -- Non-additive. Summing viewability is meaningless; even averaging it
        -- unweighted is only approximately right.
        e.viewability_score,

        -- bid_price and clearing_price are CPMs - prices per thousand - so
        -- neither one is the cost of this impression and summing either gives
        -- a number a thousand times too large. Doing the division once, here,
        -- means every query downstream gets it right by default instead of by
        -- remembering.
        e.clearing_price / 1000                                             as impression_cost
    from events as e
    {{ join_version('dim_campaign_history',  'ch', 'campaign_id',  'impression_timestamp') }}
    {{ join_version('dim_line_item_history', 'lh', 'line_item_id', 'impression_timestamp') }}
)

select * from keyed
