-- Every version of every campaign.
--
-- The one that earns its keep. The change generator pauses campaigns and moves
-- daily_budget on every run, so this is where versions actually accumulate -
-- and "what was the budget on the day we overspent" is a question dim_campaign
-- cannot answer at all, because the overwrite already happened.

with versions as (

    select
        campaign_id,
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
        updated_at,
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snap_campaigns') }}

),

timed as (

    select
        *,
        {{ version_started_at('campaign_id') }} as version_at
    from versions

),

bounded as (

    select
        *,
        -- The first version is backdated to the entity's creation. A snapshot's
        -- first run can only record current state, so without this every fact
        -- older than that run falls outside every range and resolves to the
        -- unknown member - which here would be most of the fact table. It
        -- asserts something slightly false, that today's values held since
        -- creation, in exchange for a dimension that can be joined at all.
        case
            when dbt_valid_from = min(dbt_valid_from) over (partition by campaign_id)
                then least(created_at, version_at)
            else version_at
        end as valid_from,
        {{ version_ended_at('campaign_id') }} as valid_to
    from timed

),

history as (

    select
        -- Keyed on capture time rather than on valid_from, which is the more
        -- obvious choice and not unique. Two versions tie when the business
        -- timestamps behind them tie - a deletion whose deleted_at lands
        -- before the updated_at it supersedes, which the UTC/local skew
        -- between CDC and generator timestamps produces regularly. The earlier
        -- version then occupies a zero-length window, which is correct, and
        -- would share a surrogate key with its successor, which is not: two
        -- versions collapsing into one key is a silent merge, not an error.
        {{ dbt_utils.generate_surrogate_key(['campaign_id', 'dbt_valid_from']) }} as campaign_version_key,
        {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as campaign_key,
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
        updated_at,
        valid_from,
        valid_to,
        dbt_valid_to is null as is_current,
        dbt_valid_from as captured_at
    from bounded

),

-- Spans all of time, so a fact that cannot resolve its key still matches
-- exactly one version whatever its timestamp.
unknown as (

    select
        {{ unknown_key() }}              as campaign_version_key,
        {{ unknown_key() }}              as campaign_key,
        cast(null as varchar)            as campaign_id,
        {{ unknown_key() }}              as advertiser_key,
        cast(null as varchar)            as advertiser_id,
        'Unknown campaign'               as campaign_name,
        'Unknown'                        as campaign_objective,
        'Unknown'                        as campaign_status,
        cast(0 as number(14,2))          as campaign_budget,
        cast(0 as number(14,2))          as daily_budget,
        cast(null as date)               as start_date,
        cast(null as date)               as end_date,
        'Unknown'                        as bid_strategy,
        false                            as is_deleted,
        cast(null as timestamp_ntz)      as deleted_at,
        cast(null as timestamp_ntz)      as created_at,
        cast(null as timestamp_ntz)      as updated_at,
        {{ beginning_of_time() }}        as valid_from,
        {{ end_of_time() }}              as valid_to,
        true                             as is_current,
        cast(null as timestamp_ntz)      as captured_at

)

select * from history
union all
select * from unknown
