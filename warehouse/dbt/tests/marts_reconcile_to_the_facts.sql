{#-
    The one test that would catch the chasm trap.

    Every mart aggregates the same facts to a different grain, so every mart
    must sum back to the same totals. 100 million impressions by campaign and
    100 million by publisher are the same 100 million impressions; if either
    number drifts, the aggregation lost rows or invented them.

    This is worth asserting separately from the grain tests because the two
    failures look nothing alike. A broken grain produces duplicate keys and
    unique_combination_of_columns catches it immediately. A chasm trap does
    not: it produces exactly one row per key, perfectly unique, with every
    measure multiplied by however many rows the other fact happened to have
    for that key that day. On the campaign-day measured while building this,
    joining fct_impressions to fct_clicks directly turned 559,210 impressions
    into 14 billion rows - a factor of 25,157, which is neither constant nor
    guessable, which is why nobody ever spots it by eye.

    The protection against it is structural - each fact is aggregated alone
    before anything is joined - and structure is exactly the kind of thing a
    later refactor quietly undoes. So the invariant is asserted rather than
    trusted, and it is asserted against the facts rather than against a
    number written down here, so it keeps working when the data changes.

    Money is rounded to the cent before comparing. The sums are fixed-point
    and therefore exact regardless of order, but the two sides reach that
    scale by different routes and there is no reason to make the test depend
    on which one Snowflake picks.

    Cost: four scans of the marts and four aggregates over the facts. It is
    the most expensive test in the project and the only one whose failure
    would mean every number in the gold layer is wrong.
-#}

with facts as (

    select
        (select count(*)                      from {{ ref('fct_impressions') }}) as impressions,
        (select count(*)                      from {{ ref('fct_clicks') }})      as clicks,
        (select count(*)                      from {{ ref('fct_conversions') }}) as conversions,
        (select round(sum(impression_cost), 2) from {{ ref('fct_impressions') }}) as clearing_cost,
        (select round(sum(conversion_value), 2) from {{ ref('fct_conversions') }}) as conversion_value,
        (select round(sum(spend_amount), 2)   from {{ ref('fct_spend') }})       as spend

),

marts as (

    select
        'mart_campaign_performance'  as mart,
        sum(impressions)             as impressions,
        sum(clicks)                  as clicks,
        sum(conversions)             as conversions,
        round(sum(clearing_cost), 2) as clearing_cost,
        round(sum(conversion_value), 2) as conversion_value,
        -- Only two marts can carry spend, so the other two opt out of that
        -- comparison rather than failing it. See analytics/_models.yml.
        round(sum(spend), 2)         as spend
    from {{ ref('mart_campaign_performance') }}

    union all

    select
        'mart_publisher_performance',
        sum(impressions),
        sum(clicks),
        sum(conversions),
        round(sum(clearing_cost), 2),
        round(sum(conversion_value), 2),
        round(sum(spend), 2)
    from {{ ref('mart_publisher_performance') }}

    union all

    select
        'mart_creative_performance',
        sum(impressions),
        sum(clicks),
        sum(conversions),
        round(sum(clearing_cost), 2),
        round(sum(conversion_value), 2),
        null
    from {{ ref('mart_creative_performance') }}

    union all

    select
        'mart_audience_performance',
        sum(impressions),
        sum(clicks),
        sum(conversions),
        round(sum(clearing_cost), 2),
        round(sum(conversion_value), 2),
        null
    from {{ ref('mart_audience_performance') }}

)

select
    m.mart,
    m.impressions       as mart_impressions,
    f.impressions       as fact_impressions,
    m.clicks            as mart_clicks,
    f.clicks            as fact_clicks,
    m.conversions       as mart_conversions,
    f.conversions       as fact_conversions,
    m.clearing_cost     as mart_clearing_cost,
    f.clearing_cost     as fact_clearing_cost,
    m.conversion_value  as mart_conversion_value,
    f.conversion_value  as fact_conversion_value,
    m.spend             as mart_spend,
    f.spend             as fact_spend
from marts as m
cross join facts as f
where m.impressions      <> f.impressions
   or m.clicks           <> f.clicks
   or m.conversions      <> f.conversions
   or m.clearing_cost    <> f.clearing_cost
   or m.conversion_value <> f.conversion_value
   or (m.spend is not null and m.spend <> f.spend)
