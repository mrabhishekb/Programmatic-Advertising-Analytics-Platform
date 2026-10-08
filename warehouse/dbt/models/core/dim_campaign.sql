-- An advertiser's spending plan over a flight window.
--
-- Carries `advertiser_key` as well as `advertiser_id`, and computes it by
-- hashing rather than by joining to dim_advertiser. That is the whole point of
-- a deterministic key: any model holding the natural key can produce the
-- surrogate without reading the dimension, so the build has no edge here to
-- wait on. The relationships test in _models.yml is what proves the two
-- hashes agree.
--
-- campaign_status and daily_budget are the columns the change generator edits,
-- which makes this the dimension where Type 1's overwrite is visible: run the
-- generator, reload, rebuild, and the budget here changes with no record of
-- what it was.

with members as (

    select
        {{ dbt_utils.generate_surrogate_key(['campaign_id']) }}   as campaign_key,
        campaign_id,
        {{ dbt_utils.generate_surrogate_key(['advertiser_id']) }} as advertiser_key,
        advertiser_id,
        campaign_name,
        campaign_objective,
        campaign_status,
        campaign_budget,
        daily_budget,
        start_date,
        end_date,
        bid_strategy,
        is_deleted,
        deleted_at,
        created_at,
        updated_at
    from {{ ref('stg_campaigns') }}

),

unknown as (

    select
        {{ unknown_key() }}             as campaign_key,
        cast(null as varchar)           as campaign_id,
        {{ unknown_key() }}             as advertiser_key,
        cast(null as varchar)           as advertiser_id,
        'Unknown campaign'              as campaign_name,
        'Unknown'                       as campaign_objective,
        'Unknown'                       as campaign_status,
        -- Zero rather than null: budgets are summed, and a null that
        -- propagates through a sum is harder to notice than a zero that does
        -- not move the total.
        cast(0 as number(14,2))         as campaign_budget,
        cast(0 as number(14,2))         as daily_budget,
        cast(null as date)              as start_date,
        cast(null as date)              as end_date,
        'Unknown'                       as bid_strategy,
        false                           as is_deleted,
        cast(null as timestamp_ntz)     as deleted_at,
        cast(null as timestamp_ntz)     as created_at,
        cast(null as timestamp_ntz)     as updated_at

)

select * from members
union all
select * from unknown
