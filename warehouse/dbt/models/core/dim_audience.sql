-- Targeting segments, and the dimension that makes the case for keeping
-- deleted members.
--
-- The change generator deletes segments while the 100M impressions that
-- targeted them stay exactly where they are. Filtering `is_deleted` out here
-- would strand every one of those impressions on a key that resolves to
-- nothing - the join would route them to the unknown member and collapse
-- several hundred distinct segments into one meaningless bucket.
--
-- So the member stays, flagged. "Which segments exist today" is answered with
-- `where not is_deleted`; "what did this segment ever deliver" is answered
-- without it. Both questions stay askable, which is the whole argument for
-- soft deletes surviving all the way up from Silver.

with members as (

    select
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
        updated_at
    from {{ ref('stg_audiences') }}

),

unknown as (

    select
        {{ unknown_key() }}             as audience_key,
        cast(null as varchar)           as audience_id,
        'Unknown audience'              as audience_name,
        'Unknown'                       as audience_type,
        -- The source's own bounds rather than zeros, so the min_age <= max_age
        -- assertion carried up from postgres/schema.sql holds for this row too
        -- and does not need an exception written around it.
        cast(13 as number(10,0))        as min_age,
        cast(99 as number(10,0))        as max_age,
        'Unknown'                       as gender,
        'Unknown'                       as country,
        'Unknown'                       as interest_category,
        false                           as is_deleted,
        cast(null as timestamp_ntz)     as deleted_at,
        cast(null as timestamp_ntz)     as created_at,
        cast(null as timestamp_ntz)     as updated_at

)

select * from members
union all
select * from unknown
