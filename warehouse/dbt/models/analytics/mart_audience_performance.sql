{#-
    Which targeting is worth paying for.

    Grain: one row per audience segment per day per currency. Currency is in
    the grain for the same reason as the publisher mart - every one of the
    10,000 segments is targeted by advertisers billing in several currencies,
    because a segment describes people, not buyers.

    This mart exists because targeting is the lever that is actually
    adjustable. A campaign's budget is set by someone else and a creative
    takes a week to replace, but which segments a line item bids on can change
    this afternoon. The question it answers is which of them convert, and the
    only way to ask it is through the impression.

    THE ONLY FACT THAT KNOWS

    audience_key appears on exactly one fact - fct_impressions - because the
    segment is a property of the bid request. By the time a click or a
    conversion is recorded, nobody is carrying the targeting decision around
    any more. Without int_clicks_resolved and int_conversions_resolved this
    mart could only report delivery, which is to say it could report what was
    bought and never whether it worked.

    No spend column, for the reason given in mart_creative_performance:
    fct_spend has no audience_key, and resolving it through impression_id
    would pick up CPC and CPA rows and drop every CPM one. clearing_cost is
    complete at this grain and is what gets reported instead.

    Deleted segments are kept. A segment that was retired last month still
    spent money the month before, and a performance history that disappears
    when somebody tidies up is not a history. is_deleted is on the row so a
    current-state report can filter it out deliberately.
-#}

{{ config(
    materialized='incremental',
    unique_key=['date_key', 'audience_key', 'currency'],
    incremental_strategy='merge',
    incremental_predicates=[merge_floor(var('analytics_schema'), var('mart_reaggregation_days'))]
) }}

with impressions as (

    select
        date_key,
        audience_key,
        currency,
        count(*)                                                        as impressions,
        count_if(viewability_score >= {{ var('viewable_score_threshold') }}) as viewable_impressions,
        sum(impression_cost)                                            as clearing_cost,
        -- How many campaigns bid on this segment that day. Like
        -- active_placements, a distinct count and therefore not additive
        -- across days.
        count(distinct campaign_key)                                    as active_campaigns
    from {{ ref('fct_impressions') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

clicks as (

    select
        date_key,
        audience_key,
        currency,
        count(*) as clicks
    from {{ ref('int_clicks_resolved') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

conversions as (

    select
        date_key,
        audience_key,
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
        ['date_key', 'audience_key', 'currency']
    ) }}

),

combined as (

    select
        g.date_key,
        g.audience_key,
        g.currency,
        coalesce(i.impressions, 0)          as impressions,
        coalesce(i.viewable_impressions, 0) as viewable_impressions,
        coalesce(i.clearing_cost, 0)        as clearing_cost,
        coalesce(i.active_campaigns, 0)     as active_campaigns,
        coalesce(c.clicks, 0)               as clicks,
        coalesce(v.conversions, 0)          as conversions,
        coalesce(v.conversion_value, 0)     as conversion_value

    from grain as g
    left join impressions as i
        on  i.date_key     = g.date_key
        and i.audience_key = g.audience_key
        and i.currency     = g.currency
    left join clicks as c
        on  c.date_key     = g.date_key
        and c.audience_key = g.audience_key
        and c.currency     = g.currency
    left join conversions as v
        on  v.date_key     = g.date_key
        and v.audience_key = g.audience_key
        and v.currency     = g.currency

)

select
    b.date_key,
    d.date_day,
    b.audience_key,

    dau.audience_id,
    dau.audience_name,
    dau.audience_type,
    dau.interest_category,
    dau.gender,
    dau.min_age,
    dau.max_age,
    dau.country        as audience_country,
    dau.is_deleted     as audience_is_deleted,
    b.currency,

    b.impressions,
    b.viewable_impressions,
    b.clicks,
    b.conversions,
    b.active_campaigns,
    b.clearing_cost,
    b.conversion_value,

    {{ safe_divide('b.viewable_impressions', 'b.impressions') }} as viewability_rate,
    {{ safe_divide('b.clicks', 'b.impressions') }}               as click_through_rate,
    {{ safe_divide('b.conversions', 'b.clicks') }}               as conversion_rate,
    {{ safe_divide('b.clearing_cost', 'b.impressions') }} * 1000 as average_clearing_cpm,
    {{ safe_divide('b.conversion_value', 'b.clearing_cost') }}   as return_on_clearing_cost

from combined as b
left join {{ ref('dim_audience') }} as dau
    on dau.audience_key = b.audience_key
left join {{ ref('dim_date') }} as d
    on d.date_key = b.date_key
