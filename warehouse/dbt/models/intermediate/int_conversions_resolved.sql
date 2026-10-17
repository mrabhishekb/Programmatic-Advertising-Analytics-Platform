{#-
    A conversion, plus where the impression that led to it happened.

    fct_conversions is the narrowest fact in the warehouse: advertiser and
    campaign, and nothing else. That is all the source records, because by the
    time someone buys something the ad server has long since stopped caring
    which creative they saw. What it does keep is the whole chain of
    identifiers on one row - conversion_id, click_id, impression_id - and that
    chain is what makes the rest recoverable.

    So this model recovers it: line item, creative, publisher, placement and
    audience, all read off the impression. It is what lets the creative mart
    report revenue and the audience mart report a conversion rate, neither of
    which the fact alone can answer.

    The lag here is real, unlike the click's. A conversion can land two weeks
    after the impression that caused it, and the source says exactly how long
    it is willing to wait: attribution_window_hours, which is 24, 72, 168 or
    336. The resolution window is built from the largest of those, and
    resolves_within_window asserts every row honours its own.

    This is deliberately not attribution. It resolves the conversion to the
    impression the source already says it came from, and asks no questions
    about whether that impression deserves the credit.

    Nor could it. Attribution distributes credit across the several touchpoints
    one person had, and this source has no person: there is no user, cookie or
    device identifier anywhere in the schema, and the generator emits one click
    per impression and one conversion per click. Every conversion has exactly
    one touchpoint, so last-click is not a choice made here - it is the only
    thing the data can mean.
-#}

{{ config(
    materialized='incremental',
    unique_key='conversion_id',
    incremental_strategy='merge',
    incremental_predicates=[merge_floor(var('intermediate_schema'))]
) }}

with conversions as (

    select *
    from {{ ref('fct_conversions') }}
    {{ incremental_watermark('conversion_timestamp') }}

),

impressions as (

    select
        impression_id,
        impression_timestamp,
        line_item_key,
        creative_key,
        publisher_key,
        placement_key,
        audience_key
    from {{ ref('fct_impressions') }}
    {{ resolution_window('conversions', var('conversion_resolution_days')) }}

),

resolved as (

    select
        v.date_key,
        v.conversion_timestamp,

        -- The whole chain, still on one row.
        v.conversion_id,
        v.click_id,
        v.impression_id,

        -- Observed.
        v.advertiser_key,
        v.campaign_key,
        v.campaign_version_key,

        -- Inherited from the impression.
        coalesce(i.line_item_key, {{ unknown_key() }})   as line_item_key,
        coalesce(i.creative_key,  {{ unknown_key() }})   as creative_key,
        coalesce(i.publisher_key, {{ unknown_key() }})   as publisher_key,
        coalesce(i.placement_key, {{ unknown_key() }})   as placement_key,
        coalesce(i.audience_key,  {{ unknown_key() }})   as audience_key,

        i.impression_timestamp,
        datediff(hour, i.impression_timestamp, v.conversion_timestamp) as hours_to_conversion,

        v.conversion_type,

        -- Still the conversion's own currency, not the impression's. The two
        -- agree today because both follow the advertiser, and asserting that
        -- is cheaper than assuming it, so _models.yml does.
        v.conversion_value,
        v.currency,

        -- Carried so resolves_within_window has something to compare against.
        -- It is a number and it is not a measure: summing it says nothing.
        v.attribution_window_hours

    from conversions as v
    left join impressions as i
        on i.impression_id = v.impression_id

)

select * from resolved
