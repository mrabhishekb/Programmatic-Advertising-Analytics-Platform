-- Every version of every line item, including every bid change.
--
-- bid_amount is the attribute that makes this worth storing: comparing what
-- was bid against what cleared is only meaningful against the bid that was
-- live at the time, not the one that happens to be live now.

with versions as (

    select
        line_item_id,
        campaign_id,
        line_item_name,
        line_item_status,
        bid_amount,
        bid_currency,
        optimization_goal,
        target_cpm,
        frequency_cap,
        start_date,
        end_date,
        is_deleted,
        deleted_at,
        created_at,
        updated_at,
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snap_line_items') }}

),

timed as (

    select
        *,
        {{ version_started_at('line_item_id') }} as version_at
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
            when dbt_valid_from = min(dbt_valid_from) over (partition by line_item_id)
                then least(created_at, version_at)
            else version_at
        end as valid_from,
        {{ version_ended_at('line_item_id') }} as valid_to
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
        {{ dbt_utils.generate_surrogate_key(['line_item_id', 'dbt_valid_from']) }} as line_item_version_key,
        {{ dbt_utils.generate_surrogate_key(['line_item_id']) }} as line_item_key,
        line_item_id,
        {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as campaign_key,
        campaign_id,
        line_item_name,
        line_item_status,
        bid_amount,
        bid_currency,
        optimization_goal,
        target_cpm,
        frequency_cap,
        start_date,
        end_date,
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
        {{ unknown_key() }}              as line_item_version_key,
        {{ unknown_key() }}              as line_item_key,
        cast(null as varchar)            as line_item_id,
        {{ unknown_key() }}              as campaign_key,
        cast(null as varchar)            as campaign_id,
        'Unknown line item'              as line_item_name,
        'Unknown'                        as line_item_status,
        cast(0 as number(12,4))          as bid_amount,
        'XXX'                            as bid_currency,
        'Unknown'                        as optimization_goal,
        cast(0 as number(12,4))          as target_cpm,
        cast(0 as number(10,0))          as frequency_cap,
        cast(null as date)               as start_date,
        cast(null as date)               as end_date,
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
