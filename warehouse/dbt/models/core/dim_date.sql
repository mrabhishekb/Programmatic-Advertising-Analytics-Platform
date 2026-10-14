-- The conformed date dimension. Every fact in phase 12 joins to this one.
--
-- Generated rather than loaded. There is no date table in Postgres and there
-- should not be - a calendar is not something the application knows, it is
-- something the warehouse decides. The bounds come from dbt_project.yml so
-- that widening the window is a config change rather than an edit to SQL.
--
-- Deliberately has no unknown member, unlike every other dimension here.
-- An unknown member absorbs foreign keys that are null or point nowhere, and
-- neither is reachable for a date: every event timestamp is NOT NULL in the
-- source schema, and date_key is computed from it by arithmetic rather than
-- looked up, so there is no join that could fail to find a row. What can
-- happen is a date outside the spine, and the relationships test from each
-- fact is what catches that - loudly, which is the right outcome, because the
-- fix is to widen the window rather than to file the row under Unknown.

{{ config(materialized='table') }}

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ var('date_spine_start') ~ "')",
        end_date="to_date('" ~ var('date_spine_end') ~ "')"
    ) }}
),

days as (
    select cast(date_day as date) as date_day
    from spine
)

select
    {{ date_key('date_day') }}          as date_key,
    date_day,
    year(date_day)                      as calendar_year,
    quarter(date_day)                   as calendar_quarter,
    month(date_day)                     as calendar_month,
    day(date_day)                       as day_of_month,
    monthname(date_day)                 as month_name,
    dayname(date_day)                   as day_name,

    -- ISO numbering, so Monday is 1 and the week belongs to the year that
    -- holds its Thursday. Snowflake's defaults for both are session-dependent
    -- and would make this column mean different things to different readers.
    dayofweekiso(date_day)              as day_of_week,
    weekiso(date_day)                   as week_of_year,
    yearofweekiso(date_day)             as iso_year,

    dayofweekiso(date_day) in (6, 7)    as is_weekend,
    date_trunc('month', date_day)       as first_day_of_month,
    last_day(date_day)                  as last_day_of_month,
    date_trunc('quarter', date_day)     as first_day_of_quarter,
    date_trunc('year', date_day)        as first_day_of_year
from days
