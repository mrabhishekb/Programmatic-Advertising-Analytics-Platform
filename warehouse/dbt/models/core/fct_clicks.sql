-- Grain: one row per click.
--
-- A factless fact table. There is no measure here, because a click has no
-- amount - the event either happened or it did not, and the thing you count is
-- the rows themselves. Kimball's name for this is unhelpfully literal; what it
-- really means is that the whole point of the table is the combination of
-- dimensions it records.
--
-- Two degenerate dimensions instead of one. impression_id is what makes this
-- table more than a counter: it is the pointer that lets int_clicks_resolved
-- ask which ad a click came from, how long the gap was, and which publisher,
-- placement and audience were behind it. None of those are recorded against
-- the click, so dropping impression_id as "derivable" would make them
-- unrecoverable - it is derivable from nothing else here.
--
-- Three version keys, where impressions get two. Volume is the reason: at 5
-- million rows a third range join costs almost nothing, and creative_status is
-- the attribute that explains why a creative stopped earning clicks.

{{ config(
    materialized='incremental',
    unique_key='click_id',
    incremental_strategy='merge',
    incremental_predicates=[merge_floor()]
) }}

with events as (
    select *
    from {{ ref('stg_clicks') }}
    {{ incremental_watermark('click_timestamp') }}
),

keyed as (
    select
        {{ date_key('e.click_timestamp') }}                                 as date_key,
        e.click_timestamp,

        -- Degenerate dimensions.
        e.click_id,
        e.impression_id,

        -- Type 1 keys. No publisher, placement or audience: the source does
        -- not record them on a click, and inventing them by looking through
        -- impression_id would make this fact table quietly depend on another
        -- one being complete.
        {{ dbt_utils.generate_surrogate_key(['e.advertiser_id']) }}         as advertiser_key,
        {{ dbt_utils.generate_surrogate_key(['e.campaign_id']) }}           as campaign_key,
        {{ dbt_utils.generate_surrogate_key(['e.line_item_id']) }}          as line_item_key,
        {{ dbt_utils.generate_surrogate_key(['e.creative_id']) }}           as creative_key,

        -- Type 2 keys.
        coalesce(ch.campaign_version_key,  {{ unknown_key() }})             as campaign_version_key,
        coalesce(lh.line_item_version_key, {{ unknown_key() }})             as line_item_version_key,
        coalesce(rh.creative_version_key,  {{ unknown_key() }})             as creative_version_key,

        -- Context the event carries itself.
        e.device_type,
        e.country
    from events as e
    {{ join_version('dim_campaign_history',  'ch', 'campaign_id',  'click_timestamp') }}
    {{ join_version('dim_line_item_history', 'lh', 'line_item_id', 'click_timestamp') }}
    {{ join_version('dim_creative_history',  'rh', 'creative_id',  'click_timestamp') }}
)

select * from keyed
