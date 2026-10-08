-- Companies buying advertising. The top of the hierarchy and the one dimension
-- every fact reaches, directly or through campaign.
--
-- Type 1: an advertiser that moves its billing country shows the new country
-- and keeps no trace of the old one. The history of that change is not lost,
-- it just lives somewhere else - phase 11 snapshots the same staging model and
-- keeps a row per version.
--
-- Deleted members are kept rather than filtered. A dimension has to contain
-- every member a fact has ever referenced, and `is_deleted` is how a report
-- asks for current state without the facts losing their join.

with members as (

    select
        {{ dbt_utils.generate_surrogate_key(['advertiser_id']) }} as advertiser_key,
        advertiser_id,
        advertiser_name,
        industry,
        billing_country,
        billing_currency,
        account_status,
        is_deleted,
        deleted_at,
        created_at,
        updated_at
    from {{ ref('stg_advertisers') }}

),

-- The nulls are cast rather than written bare because a UNION takes the wider
-- of the two types, and an uncast null arrives as NUMBER(38,0) or VARCHAR(16M)
-- and widens the column it lands in. The declared types from the load are
-- worth keeping all the way through.
unknown as (

    select
        {{ unknown_key() }}             as advertiser_key,
        cast(null as varchar)           as advertiser_id,
        'Unknown advertiser'            as advertiser_name,
        'Unknown'                       as industry,
        'Unknown'                       as billing_country,
        -- XXX is ISO 4217 for "no currency", so a sum grouped by currency
        -- cannot quietly merge the unknown member into a real one.
        'XXX'                           as billing_currency,
        'Unknown'                       as account_status,
        false                           as is_deleted,
        cast(null as timestamp_ntz)     as deleted_at,
        cast(null as timestamp_ntz)     as created_at,
        cast(null as timestamp_ntz)     as updated_at

)

select * from members
union all
select * from unknown
