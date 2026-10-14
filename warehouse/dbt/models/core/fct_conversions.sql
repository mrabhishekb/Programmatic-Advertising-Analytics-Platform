-- Grain: one row per conversion, attributed last-click.
--
-- The narrowest fact in the warehouse, and deliberately so. The source records
-- a conversion against a campaign and an advertiser and nothing else - no
-- creative, no publisher, no placement - because by the time someone converts,
-- the attribution has already been decided upstream. Adding those keys here by
-- walking click_id back to an impression would look like richer data and would
-- really be this table asserting an attribution model it does not own. Phase
-- 14 is where that chain gets walked, explicitly, with the model named.
--
-- Three degenerate dimensions, and the whole chain is in them: conversion_id,
-- the click that preceded it, and the impression that preceded that.

{{ config(
    materialized='incremental',
    unique_key='conversion_id',
    incremental_strategy='merge',
    incremental_predicates=[merge_floor()]
) }}

with events as (
    select *
    from {{ ref('stg_conversions') }}
    {{ incremental_watermark('conversion_timestamp') }}
),

keyed as (
    select
        {{ date_key('e.conversion_timestamp') }}                            as date_key,
        e.conversion_timestamp,

        -- Degenerate dimensions: conversion, click, impression.
        e.conversion_id,
        e.click_id,
        e.impression_id,

        -- Type 1 keys.
        {{ dbt_utils.generate_surrogate_key(['e.advertiser_id']) }}         as advertiser_key,
        {{ dbt_utils.generate_surrogate_key(['e.campaign_id']) }}           as campaign_key,

        -- Type 2 key.
        coalesce(ch.campaign_version_key, {{ unknown_key() }})              as campaign_version_key,

        -- Context the event carries itself.
        e.conversion_type,

        -- Measure. Fully additive across campaign, advertiser and time.
        e.conversion_value,
        e.currency,

        -- A number that is not a measure. It records the attribution setting
        -- in force when this conversion was credited, so summing it is
        -- nonsense and averaging it describes the configuration rather than
        -- the business. Kept because it is the only record of what rule
        -- produced the row.
        e.attribution_window_hours
    from events as e
    {{ join_version('dim_campaign_history', 'ch', 'campaign_id', 'conversion_timestamp') }}
)

select * from keyed
