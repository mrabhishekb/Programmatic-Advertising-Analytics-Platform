-- Every version of every targeting segment, deletions included.
--
-- The dimension the `check` strategy was chosen for. A deletion here moves no
-- timestamp at all - it flips is_deleted and nothing else - so a timestamp
-- snapshot would record 546 removals as nothing whatsoever, and this model
-- would show every one of those segments as live in perpetuity.

with versions as (

    select
        audience_id,
        audience_name,
        audience_type,
        min_age,
        max_age,
        gender,
        country,
        interest_category,
        is_deleted,
        deleted_at,
        created_at,
        updated_at,
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snap_audiences') }}

),

timed as (

    select
        *,
        {{ version_started_at('audience_id') }} as version_at
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
            when dbt_valid_from = min(dbt_valid_from) over (partition by audience_id)
                then least(created_at, version_at)
            else version_at
        end as valid_from,
        {{ version_ended_at('audience_id') }} as valid_to
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
        {{ dbt_utils.generate_surrogate_key(['audience_id', 'dbt_valid_from']) }} as audience_version_key,
        {{ dbt_utils.generate_surrogate_key(['audience_id']) }} as audience_key,
        audience_id,
        audience_name,
        audience_type,
        min_age,
        max_age,
        gender,
        country,
        interest_category,
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
        {{ unknown_key() }}              as audience_version_key,
        {{ unknown_key() }}              as audience_key,
        cast(null as varchar)            as audience_id,
        'Unknown audience'               as audience_name,
        'Unknown'                        as audience_type,
        cast(13 as number(10,0))         as min_age,
        cast(99 as number(10,0))         as max_age,
        'Unknown'                        as gender,
        'Unknown'                        as country,
        'Unknown'                        as interest_category,
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
