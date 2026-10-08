-- Targeting segments, and the one dimension that is routinely deleted.
--
-- Worth knowing when reading counts from this model: the source holds ~10,000
-- live segments but this holds more than that, because deletions are kept and
-- flagged rather than removed. Nothing here filters them out - dim_audience
-- needs every member the 100M impressions ever referenced, and `is_deleted` is
-- what a current-state report filters on instead.

select
    audience_id,
    audience_name,
    audience_type,
    min_age,
    max_age,
    gender,
    country,
    interest_category,
    created_at,
    updated_at,
    {{ silver_lineage_columns() }}
from {{ source('raw', 'AUDIENCES') }}
