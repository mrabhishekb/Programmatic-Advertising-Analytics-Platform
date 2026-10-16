{#-
    What every campaign did, every day.

    Grain: one row per campaign per day per currency. Currency is in the grain
    of all four marts for consistency, and here it is provably redundant - no
    campaign in this warehouse ever bills in two currencies, which is what the
    unique test on (date_key, campaign_key) asserts rather than assumes. If
    that ever stops being true the test fails and the mart splits honestly,
    instead of quietly adding dollars to yen.

    This is the mart with everything in it, because campaign_key is the one
    key all four facts carry. The other three marts are narrower for reasons
    the facts decide, not reasons anyone chose.

    THE JOIN THAT IS NOT HERE

    The obvious way to build this is to join impressions to clicks and count
    both. It is also the single most expensive mistake in dimensional
    modelling, and it is worth naming: the chasm trap.

    Take a campaign with 50,000 impressions and 400 clicks on one day. Joining
    the two facts on campaign_key matches every impression against every click
    - 20 million rows - and then counts 20 million impressions, 20 million
    clicks, and sums the spend 400 times over. Nothing errors. The report
    renders. The numbers are wrong by a factor that changes per campaign per
    day, which is what makes it so hard to spot: there is no constant to
    divide back out.

    The fix is structural, not clever. Each fact is aggregated to the grain
    first, alone, with no other fact in scope. Only then are the results
    joined, and by then each side has at most one row per key, so there is
    nothing left to multiply.

    WHY THE RATES ARE STORED ANYWAY

    A rate is not additive and cannot be re-aggregated. The click-through rate
    of a week is not the average of seven daily rates - it is the week's
    clicks over the week's impressions, and the two disagree whenever the days
    differ in size, which is every week.

    So the components are stored and are the real content of this mart. The
    rates are stored next to them for convenience, and are correct for the row
    they sit on and for nothing else. Anything summarising across rows must
    divide the summed components. _models.yml says so on every rate column,
    and phase 19's semantic model is where that stops being a convention and
    starts being enforced.
-#}

{{ config(
    materialized='incremental',
    unique_key=['date_key', 'campaign_key'],
    incremental_strategy='merge',
    incremental_predicates=[merge_floor(var('analytics_schema'), var('mart_reaggregation_days'))]
) }}

with impressions as (

    select
        date_key,
        campaign_key,
        currency,
        count(*)                                                        as impressions,
        -- Viewability is a score per impression and averaging it would be a
        -- non-additive measure stored as if it were additive. Counting the
        -- impressions that clear the MRC threshold gives a numerator that
        -- can be summed over any period, which is the whole point.
        count_if(viewability_score >= {{ var('viewable_score_threshold') }}) as viewable_impressions,
        sum(impression_cost)                                            as clearing_cost
    from {{ ref('fct_impressions') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

clicks as (

    select
        date_key,
        campaign_key,
        currency,
        count(*) as clicks
    from {{ ref('int_clicks_resolved') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

conversions as (

    select
        date_key,
        campaign_key,
        currency,
        count(*)                as conversions,
        sum(conversion_value)   as conversion_value
    from {{ ref('int_conversions_resolved') }}
    {{ reaggregation_window() }}
    group by 1, 2, 3

),

spend as (

    -- fct_spend holds three grains in one table and they are not
    -- interchangeable: a CPM row bills a thousand impressions, a CPC row
    -- bills one click, a CPA row bills one conversion. The total is still
    -- meaningful - it is what the advertiser paid - but a total that hides
    -- which of the three moved is a total nobody can act on, so the split is
    -- carried alongside it.
    select
        date_key,
        campaign_key,
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
        ['date_key', 'campaign_key', 'currency']
    ) }}

),

combined as (

    select
        g.date_key,
        g.campaign_key,
        g.currency,

        -- Zero rather than null, because every one of these is a count of
        -- things that happened and nothing happening is a real answer. A
        -- campaign that spent money and served no impressions served zero,
        -- and the row says so instead of going blank.
        coalesce(i.impressions, 0)          as impressions,
        coalesce(i.viewable_impressions, 0) as viewable_impressions,
        coalesce(i.clearing_cost, 0)        as clearing_cost,
        coalesce(c.clicks, 0)               as clicks,
        coalesce(v.conversions, 0)          as conversions,
        coalesce(v.conversion_value, 0)     as conversion_value,
        coalesce(s.spend, 0)                as spend,
        coalesce(s.spend_cpm, 0)            as spend_cpm,
        coalesce(s.spend_cpc, 0)            as spend_cpc,
        coalesce(s.spend_cpa, 0)            as spend_cpa

    from grain as g
    left join impressions as i
        on  i.date_key    = g.date_key
        and i.campaign_key = g.campaign_key
        and i.currency    = g.currency
    left join clicks as c
        on  c.date_key    = g.date_key
        and c.campaign_key = g.campaign_key
        and c.currency    = g.currency
    left join conversions as v
        on  v.date_key    = g.date_key
        and v.campaign_key = g.campaign_key
        and v.currency    = g.currency
    left join spend as s
        on  s.date_key    = g.date_key
        and s.campaign_key = g.campaign_key
        and s.currency    = g.currency

)

select
    b.date_key,
    d.date_day,
    b.campaign_key,

    -- Denormalised on purpose. A mart is a product, and a product that
    -- cannot be read without three joins is a product nobody uses. The cost
    -- is that a renamed campaign only shows its new name after the next run,
    -- which for a Type 1 dimension was already the deal.
    --
    -- Type 1 keys, not Type 2. Rolling up on a version key would split a
    -- campaign into two rows the day somebody edited its budget, and a
    -- performance report that fragments when a field is edited is useless.
    -- The version keys are still on the facts for the questions that need
    -- them, which is what phase 11 built them for.
    dc.campaign_id,
    dc.campaign_name,
    dc.campaign_objective,
    dc.campaign_status,
    dc.bid_strategy,
    dc.is_deleted                   as campaign_is_deleted,
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
    b.spend,
    b.spend_cpm,
    b.spend_cpc,
    b.spend_cpa,

    -- Everything below is derived from the four lines above it and is true of
    -- this row only. See the header.
    {{ safe_divide('b.viewable_impressions', 'b.impressions') }} as viewability_rate,
    {{ safe_divide('b.clicks', 'b.impressions') }}               as click_through_rate,
    {{ safe_divide('b.conversions', 'b.clicks') }}               as conversion_rate,
    {{ safe_divide('b.spend', 'b.impressions') }} * 1000         as effective_cpm,
    {{ safe_divide('b.spend', 'b.clicks') }}                     as cost_per_click,
    {{ safe_divide('b.spend', 'b.conversions') }}                as cost_per_acquisition,
    {{ safe_divide('b.conversion_value', 'b.spend') }}           as return_on_ad_spend,
    {{ safe_divide('b.clearing_cost', 'b.impressions') }} * 1000 as average_clearing_cpm

from combined as b

-- Left, not inner, everywhere below. An inner join here would silently drop
-- any row whose key failed to resolve, which is the one case worth seeing.
-- The dimensions all carry an unknown member, so an unresolved key lands on
-- it and the not_null tests in _models.yml catch anything that does not.
left join {{ ref('dim_campaign') }} as dc
    on dc.campaign_key = b.campaign_key
left join {{ ref('dim_advertiser') }} as da
    on da.advertiser_id = dc.advertiser_id
left join {{ ref('dim_date') }} as d
    on d.date_key = b.date_key
