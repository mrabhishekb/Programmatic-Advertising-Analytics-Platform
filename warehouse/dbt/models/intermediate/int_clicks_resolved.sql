{#-
    A click, plus where it happened.

    fct_clicks knows which advertiser, campaign, line item and creative a
    click belongs to, because the source clicks table carries those ids. It
    does not know which publisher served the ad, which placement it sat in, or
    which audience segment was targeted - the source does not repeat them,
    because the ad server already recorded all of that against the impression.

    That is not an oversight in the source and it was not one in phase 12
    either. A fact carries the keys its grain has, and the grain of a click is
    the click. What it carries instead is impression_id, a degenerate
    dimension: an identifier with no dimension table behind it, kept precisely
    so the chain back to the impression stays walkable.

    This model walks it. Afterwards a click can be counted by publisher and by
    audience, which is what two of phase 13's four marts are for, and which is
    otherwise unanswerable.

    Why this is a model and not a CTE inside each mart: the publisher mart and
    the audience mart both need it, and inlining it would scan a hundred
    million impressions once per mart. Why it is not a wider fct_clicks: the
    keys are inherited rather than observed, and a fact that mixes the two
    stops being readable as a record of what happened.

    The join is an equality on impression_id against 100M rows. Snowflake
    hash-joins it - 5.1 million probes, one match each - and the window below
    only exists so an incremental run does not have to build the probe side
    out of the whole table.
-#}

{{ config(
    materialized='incremental',
    unique_key='click_id',
    incremental_strategy='merge',
    incremental_predicates=[merge_floor(var('intermediate_schema'))]
) }}

with clicks as (

    select *
    from {{ ref('fct_clicks') }}
    {{ incremental_watermark('click_timestamp') }}

),

impressions as (

    select
        impression_id,
        impression_timestamp,
        publisher_key,
        placement_key,
        audience_key,
        currency
    from {{ ref('fct_impressions') }}
    {{ resolution_window('clicks', var('click_resolution_days')) }}

),

resolved as (

    select
        c.date_key,
        c.click_timestamp,

        -- Degenerate, and now load-bearing: this is the column the join below
        -- runs on, so phase 16 breaking it would be visible immediately.
        c.click_id,
        c.impression_id,

        -- Observed: the source recorded these against the click itself.
        c.advertiser_key,
        c.campaign_key,
        c.line_item_key,
        c.creative_key,
        c.campaign_version_key,
        c.line_item_version_key,
        c.creative_version_key,

        -- Inherited: these come from the impression, not from the click.
        --
        -- coalesce to the unknown member rather than leaving nulls, for the
        -- same reason the facts do: a null key cannot be joined, grouped or
        -- counted, so it turns one missing row into a missing row everywhere
        -- downstream. The unknown member keeps the click countable and keeps
        -- the gap visible, and resolves_to_a_version fails if there is one.
        coalesce(i.publisher_key, {{ unknown_key() }})   as publisher_key,
        coalesce(i.placement_key, {{ unknown_key() }})   as placement_key,
        coalesce(i.audience_key,  {{ unknown_key() }})   as audience_key,
        i.currency,

        -- Kept so the window above can be checked rather than believed.
        i.impression_timestamp,
        datediff(second, i.impression_timestamp, c.click_timestamp) as seconds_to_click,

        c.device_type,
        c.country

    from clicks as c
    left join impressions as i
        on i.impression_id = c.impression_id

)

select * from resolved
