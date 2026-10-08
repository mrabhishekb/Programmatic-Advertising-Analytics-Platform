-- The advertisement itself.
--
-- Owned by an advertiser rather than by a campaign, so the same creative can
-- run across several of them. That ownership is why the key here points at
-- dim_advertiser and not at dim_campaign: hanging it off the campaign would
-- duplicate the asset once per campaign that used it and make "how did this
-- creative perform overall" a question requiring a deduplication.

with members as (

    select
        {{ dbt_utils.generate_surrogate_key(['creative_id']) }}   as creative_key,
        creative_id,
        {{ dbt_utils.generate_surrogate_key(['advertiser_id']) }} as advertiser_key,
        advertiser_id,
        creative_name,
        creative_type,
        creative_format,
        landing_page_url,
        creative_status,
        is_deleted,
        deleted_at,
        created_at,
        updated_at
    from {{ ref('stg_creatives') }}

),

unknown as (

    select
        {{ unknown_key() }}             as creative_key,
        cast(null as varchar)           as creative_id,
        {{ unknown_key() }}             as advertiser_key,
        cast(null as varchar)           as advertiser_id,
        'Unknown creative'              as creative_name,
        'Unknown'                       as creative_type,
        'Unknown'                       as creative_format,
        cast(null as varchar)           as landing_page_url,
        'Unknown'                       as creative_status,
        false                           as is_deleted,
        cast(null as timestamp_ntz)     as deleted_at,
        cast(null as timestamp_ntz)     as created_at,
        cast(null as timestamp_ntz)     as updated_at

)

select * from members
union all
select * from unknown
