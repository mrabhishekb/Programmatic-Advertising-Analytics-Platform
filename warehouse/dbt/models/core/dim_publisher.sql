-- Sites and apps selling inventory: the supply side of the marketplace.

with members as (

    select
        {{ dbt_utils.generate_surrogate_key(['publisher_id']) }} as publisher_key,
        publisher_id,
        publisher_name,
        publisher_type,
        country,
        domain,
        publisher_status,
        is_deleted,
        deleted_at,
        created_at,
        updated_at
    from {{ ref('stg_publishers') }}

),

unknown as (

    select
        {{ unknown_key() }}             as publisher_key,
        cast(null as varchar)           as publisher_id,
        'Unknown publisher'             as publisher_name,
        'Unknown'                       as publisher_type,
        'Unknown'                       as country,
        cast(null as varchar)           as domain,
        'Unknown'                       as publisher_status,
        false                           as is_deleted,
        cast(null as timestamp_ntz)     as deleted_at,
        cast(null as timestamp_ntz)     as created_at,
        cast(null as timestamp_ntz)     as updated_at

)

select * from members
union all
select * from unknown
