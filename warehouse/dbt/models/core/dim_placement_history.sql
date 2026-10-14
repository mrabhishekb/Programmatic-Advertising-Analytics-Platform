-- Every version of every ad slot.
--
-- floor_price moves, and a floor is only comparable with a clearing price from
-- the same moment. Joined to the current floor, every historical auction looks
-- mispriced by exactly the amount the floor has changed since.

with versions as (

    select
        placement_id,
        publisher_id,
        placement_name,
        placement_type,
        ad_format,
        floor_price,
        currency,
        device_type,
        is_deleted,
        deleted_at,
        created_at,
        updated_at,
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snap_placements') }}

),

timed as (

    select
        *,
        {{ version_started_at('placement_id') }} as version_at
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
            when dbt_valid_from = min(dbt_valid_from) over (partition by placement_id)
                then least(created_at, version_at)
            else version_at
        end as valid_from,
        {{ version_ended_at('placement_id') }} as valid_to
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
        {{ dbt_utils.generate_surrogate_key(['placement_id', 'dbt_valid_from']) }} as placement_version_key,
        {{ dbt_utils.generate_surrogate_key(['placement_id']) }} as placement_key,
        placement_id,
        {{ dbt_utils.generate_surrogate_key(['publisher_id']) }} as publisher_key,
        publisher_id,
        placement_name,
        placement_type,
        ad_format,
        floor_price,
        currency,
        device_type,
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
        {{ unknown_key() }}              as placement_version_key,
        {{ unknown_key() }}              as placement_key,
        cast(null as varchar)            as placement_id,
        {{ unknown_key() }}              as publisher_key,
        cast(null as varchar)            as publisher_id,
        'Unknown placement'              as placement_name,
        'Unknown'                        as placement_type,
        'Unknown'                        as ad_format,
        cast(0 as number(12,4))          as floor_price,
        'XXX'                            as currency,
        'Unknown'                        as device_type,
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
