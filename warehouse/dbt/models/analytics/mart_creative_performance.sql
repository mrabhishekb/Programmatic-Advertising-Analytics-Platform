{#-
    Which ad is actually earning its impressions.

    Grain: one row per creative per day per currency. As with campaigns, a
    creative belongs to one advertiser and so bills in one currency, and the
    unique test on (date_key, creative_key) is what checks that rather than
    taking it on trust.

    This is the mart creative rotation decisions come out of. Two creatives in
    the same line item get roughly the same impressions; the one with the
    better click-through rate is the one worth keeping, and the one with the
    better conversion rate is the one worth copying.

    NO SPEND COLUMN, AND WHY NOT

    fct_spend has no creative_key, and unlike clicks and conversions the chain
    cannot be walked to get one. It does carry impression_id - but only for
    CPC and CPA rows. Every CPM row has a null there, because a CPM
    transaction bills a thousand impressions rather than one, so there is no
    single impression to point at.

    Resolving spend through impression_id would therefore pick up 1.6 million
    of 19.3 million spend rows. Those rows happen to carry most of the money,
    which is exactly what makes it dangerous: the total would look plausible
    and would be missing 12% of it, silently, forever.

    So this mart reports clearing_cost instead - the sum of what the auctions
    actually cleared at, which exists on every impression and is therefore
    complete at this grain. It is a different number from spend and answers a
    different question: what the inventory cost, rather than what the
    advertiser was billed. Mixing the two under one column name would be the
    real mistake, so they keep separate names everywhere.
-#}

{{ config(
    materialized='incremental',
    unique_key=['date_key', 'creative_key'],
    incremental_strategy='merge',
    incremental_predicates=[merge_floor(var('analytics_schema'), var('mart_reaggregation_days'))]
) }}

with impressions as (

    select
        date_key,
        creative_key,
        currency,
        count(*)                                                        as impressions,
        count_if(viewability_score >= {{ var('viewable_score_threshold') }}) as viewable_impressions,
        sum(impression_cost)                                            as clearing_cost
    from {{ ref('fct_impressions') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

clicks as (

    select
        date_key,
        creative_key,
        currency,
        count(*) as clicks
    from {{ ref('int_clicks_resolved') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

conversions as (

    -- creative_key here is inherited from the impression, not recorded on the
    -- conversion. See int_conversions_resolved.
    select
        date_key,
        creative_key,
        currency,
        count(*)                as conversions,
        sum(conversion_value)   as conversion_value
    from {{ ref('int_conversions_resolved') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

grain as (

    {{ grain_of(
        ['impressions', 'clicks', 'conversions'],
        ['date_key', 'creative_key', 'currency']
    ) }}

),

combined as (

    select
        g.date_key,
        g.creative_key,
        g.currency,
        coalesce(i.impressions, 0)          as impressions,
        coalesce(i.viewable_impressions, 0) as viewable_impressions,
        coalesce(i.clearing_cost, 0)        as clearing_cost,
        coalesce(c.clicks, 0)               as clicks,
        coalesce(v.conversions, 0)          as conversions,
        coalesce(v.conversion_value, 0)     as conversion_value

    from grain as g
    left join impressions as i
        on  i.date_key     = g.date_key
        and i.creative_key = g.creative_key
        and i.currency     = g.currency
    left join clicks as c
        on  c.date_key     = g.date_key
        and c.creative_key = g.creative_key
        and c.currency     = g.currency
    left join conversions as v
        on  v.date_key     = g.date_key
        and v.creative_key = g.creative_key
        and v.currency     = g.currency

)

select
    b.date_key,
    d.date_day,
    b.creative_key,

    dc.creative_id,
    dc.creative_name,
    dc.creative_type,
    dc.creative_format,
    dc.creative_status,
    dc.is_deleted       as creative_is_deleted,
    da.advertiser_id,
    da.advertiser_name,
    da.industry,
    b.currency,

    b.impressions,
    b.viewable_impressions,
    b.clicks,
    b.conversions,
    b.clearing_cost,
    b.conversion_value,

    {{ safe_divide('b.viewable_impressions', 'b.impressions') }} as viewability_rate,
    {{ safe_divide('b.clicks', 'b.impressions') }}               as click_through_rate,
    {{ safe_divide('b.conversions', 'b.clicks') }}               as conversion_rate,
    {{ safe_divide('b.clearing_cost', 'b.impressions') }} * 1000 as average_clearing_cpm,
    -- Not called ROAS. The denominator is the auction price, not the billed
    -- amount, and the two differ enough that one name for both would be a lie.
    {{ safe_divide('b.conversion_value', 'b.clearing_cost') }}   as return_on_clearing_cost

from combined as b
left join {{ ref('dim_creative') }} as dc
    on dc.creative_key = b.creative_key
left join {{ ref('dim_advertiser') }} as da
    on da.advertiser_id = dc.advertiser_id
left join {{ ref('dim_date') }} as d
    on d.date_key = b.date_key
