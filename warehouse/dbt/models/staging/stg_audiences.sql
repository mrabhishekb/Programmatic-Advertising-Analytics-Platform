-- Targeting segments, and the one dimension that is routinely deleted.
--
-- Worth knowing when reading counts from this model: the source holds ~10,000
-- live segments but Silver holds more rows than that, because deletions are
-- kept and flagged. The live/total gap here is larger than in any other
-- dimension, which makes it the best test of the soft-delete filter.

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
{{ only_live_rows() }}
