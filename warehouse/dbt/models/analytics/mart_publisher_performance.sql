{#-
    What each publisher's inventory was worth.

    Grain: one row per publisher per day per currency - and here the currency
    is load-bearing rather than redundant. Every one of the 20,000 publishers
    in this warehouse serves advertisers that bill in several currencies,
    because a publisher sells to whoever bids, and bidders come from
    everywhere.

    That is why currency is in the grain of this mart and not merely carried
    on it. There is no FX rate anywhere in this project, and inventing one
    would be worse than not having it, so the alternative to splitting the
    grain is a spend column that adds yen to dollars and reports the sum as a
    number. Splitting is not a limitation being worked around; it is the only
    honest shape for money that has not been converted.

    The counts are unaffected. An impression is an impression regardless of
    what it was paid for in, so summing impressions across the currency rows
    of one publisher is correct, and summing spend across them is not. The
    column names do not distinguish those two cases and cannot - what does is
    the grain itself, which forces anyone aggregating to at least see the
    currency column sitting there.

    This is the supply-side view, so it reads the other way round from the
    campaign mart: spend here is revenue to the publisher and cost to the
    advertiser, and viewability is the quality of what was sold rather than
    the luck of what was bought.
-#}

{{ config(
    materialized='incremental',
    unique_key=['date_key', 'publisher_key', 'currency'],
    incremental_strategy='merge',
    incremental_predicates=[merge_floor(var('analytics_schema'), var('mart_reaggregation_days'))]
) }}

with impressions as (

    select
        date_key,
        publisher_key,
        currency,
        count(*)                                                        as impressions,
        count_if(viewability_score >= {{ var('viewable_score_threshold') }}) as viewable_impressions,
        sum(impression_cost)                                            as clearing_cost,
        -- How many distinct placements of this publisher won anything that
        -- day. Not additive across days and labelled so in _models.yml -
        -- a publisher with the same 4 placements active all week has 4
        -- active placements that week, not 28.
        count(distinct placement_key)                                   as active_placements
    from {{ ref('fct_impressions') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

clicks as (

    -- publisher_key inherited from the impression. fct_clicks does not have
    -- one; see int_clicks_resolved.
    select
        date_key,
        publisher_key,
        currency,
        count(*) as clicks
    from {{ ref('int_clicks_resolved') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

conversions as (

    select
        date_key,
        publisher_key,
        currency,
        count(*)                as conversions,
        sum(conversion_value)   as conversion_value
    from {{ ref('int_conversions_resolved') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

spend as (

    -- fct_spend carries publisher_key directly on every row, CPM included, so
    -- unlike the creative and audience marts this one gets the complete
    -- billed amount rather than the auction's clearing price.
    select
        date_key,
        publisher_key,
        currency,
        sum(spend_amount)                                           as spend,
        sum(case when billing_type = 'CPM' then spend_amount else 0 end) as spend_cpm,
        sum(case when billing_type = 'CPC' then spend_amount else 0 end) as spend_cpc,
        sum(case when billing_type = 'CPA' then spend_amount else 0 end) as spend_cpa
    from {{ ref('fct_spend') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

grain as (

    {{ grain_of(
        ['impressions', 'clicks', 'conversions', 'spend'],
        ['date_key', 'publisher_key', 'currency']
    ) }}

),

combined as (

    select
        g.date_key,
        g.publisher_key,
        g.currency,
        coalesce(i.impressions, 0)          as impressions,
        coalesce(i.viewable_impressions, 0) as viewable_impressions,
        coalesce(i.clearing_cost, 0)        as clearing_cost,
        coalesce(i.active_placements, 0)    as active_placements,
        coalesce(c.clicks, 0)               as clicks,
        coalesce(v.conversions, 0)          as conversions,
        coalesce(v.conversion_value, 0)     as conversion_value,
        coalesce(s.spend, 0)                as spend,
        coalesce(s.spend_cpm, 0)            as spend_cpm,
        coalesce(s.spend_cpc, 0)            as spend_cpc,
        coalesce(s.spend_cpa, 0)            as spend_cpa

    from grain as g
    left join impressions as i
        on  i.date_key      = g.date_key
        and i.publisher_key = g.publisher_key
        and i.currency      = g.currency
    left join clicks as c
        on  c.date_key      = g.date_key
        and c.publisher_key = g.publisher_key
        and c.currency      = g.currency
    left join conversions as v
        on  v.date_key      = g.date_key
        and v.publisher_key = g.publisher_key
        and v.currency      = g.currency
    left join spend as s
        on  s.date_key      = g.date_key
        and s.publisher_key = g.publisher_key
        and s.currency      = g.currency

)

select
    b.date_key,
    d.date_day,
    b.publisher_key,

    dp.publisher_id,
    dp.publisher_name,
    dp.publisher_type,
    dp.country         as publisher_country,
    dp.domain,
    dp.publisher_status,
    dp.is_deleted      as publisher_is_deleted,
    b.currency,

    b.impressions,
    b.viewable_impressions,
    b.clicks,
    b.conversions,
    b.active_placements,
    b.clearing_cost,
    b.conversion_value,
    b.spend,
    b.spend_cpm,
    b.spend_cpc,
    b.spend_cpa,

    {{ safe_divide('b.viewable_impressions', 'b.impressions') }} as viewability_rate,
    {{ safe_divide('b.clicks', 'b.impressions') }}               as click_through_rate,
    {{ safe_divide('b.conversions', 'b.clicks') }}               as conversion_rate,
    {{ safe_divide('b.spend', 'b.impressions') }} * 1000         as effective_cpm,
    {{ safe_divide('b.spend', 'b.clicks') }}                     as cost_per_click,
    {{ safe_divide('b.spend', 'b.conversions') }}                as cost_per_acquisition,
    {{ safe_divide('b.conversion_value', 'b.spend') }}           as return_on_ad_spend,
    {{ safe_divide('b.clearing_cost', 'b.impressions') }} * 1000 as average_clearing_cpm

from combined as b
left join {{ ref('dim_publisher') }} as dp
    on dp.publisher_key = b.publisher_key
left join {{ ref('dim_date') }} as d
    on d.date_key = b.date_key
