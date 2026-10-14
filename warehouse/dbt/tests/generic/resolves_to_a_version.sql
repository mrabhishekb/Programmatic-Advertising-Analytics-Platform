{% test resolves_to_a_version(model, column_name) %}

-- Counts events that found no version of their dimension member in force at
-- the moment they happened.
--
-- A relationships test cannot catch this. The fact coalesces an unmatched
-- range join to the unknown member, so the key is always joinable and
-- referential integrity always passes - the row is not an orphan, it is
-- unexplained. That distinction is the whole reason this test exists.
--
-- There is only one way to get here: an event timestamped before the first
-- version of its dimension member begins, which is before that member's
-- created_at. In a world with one clock that is impossible. In this project it
-- is the visible edge of a real bug - the change generator writes business
-- timestamps in local time while the CDC path derives them from Debezium event
-- time in UTC, and both land in TIMESTAMP_NTZ columns that cannot tell the
-- difference. Phase 11 found it on deletions; this is the same skew measured
-- against 125 million events instead of a few hundred.
--
-- Warns rather than fails, because the count is the point. Zero means the
-- clocks agree. A handful means the skew is real and bounded. A large number
-- means something upstream has moved and the version keys cannot be trusted.

select
    {{ column_name }} as version_key,
    count(*)          as unresolved_events
from {{ model }}
where {{ column_name }} = {{ unknown_key() }}
group by 1

{% endtest %}
