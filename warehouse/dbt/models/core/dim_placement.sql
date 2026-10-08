-- A specific ad slot on a publisher's property, with the floor it will accept.
--
-- floor_price is a CPM, which is worth saying in the model rather than only in
-- the docs: it is the column most likely to be averaged against clearing_price
-- in phase 12, and the two are only comparable because both are per thousand.

with members as (

    select
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
        updated_at
    from {{ ref('stg_placements') }}

),

unknown as (

    select
        {{ unknown_key() }}             as placement_key,
        cast(null as varchar)           as placement_id,
        {{ unknown_key() }}             as publisher_key,
        cast(null as varchar)           as publisher_id,
        'Unknown placement'             as placement_name,
        'Unknown'                       as placement_type,
        'Unknown'                       as ad_format,
        cast(0 as number(12,4))         as floor_price,
        'XXX'                           as currency,
        'Unknown'                       as device_type,
        false                           as is_deleted,
        cast(null as timestamp_ntz)     as deleted_at,
        cast(null as timestamp_ntz)     as created_at,
        cast(null as timestamp_ntz)     as updated_at

)

select * from members
union all
select * from unknown
