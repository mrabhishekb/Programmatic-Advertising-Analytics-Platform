{#
    Asserts a Type 2 dimension's versions tile the timeline without gaps or
    overlaps, per entity.

    The two failures this catches look identical in a row count and could not be
    more different in a join. Overlapping ranges make a fact match two versions
    and double its measures. A gap makes it match none and lose them. Both pass
    `unique`, `not_null` and every other column-level test a dimension carries,
    because neither is a property of any single row.

    Contiguity is asserted rather than just non-overlap because the construction
    in `version_ended_at` should make a gap impossible - so a gap appearing here
    means the window ordering has drifted from what the validity ranges assume,
    which is worth knowing before phase 12 joins against it.
#}
{% test versions_are_contiguous(model, column_name, valid_from='valid_from', valid_to='valid_to') %}

with ordered as (

    select
        {{ column_name }} as durable_key,
        {{ valid_from }}  as starts_at,
        {{ valid_to }}    as ends_at,
        lead({{ valid_from }}) over (
            partition by {{ column_name }}
            order by {{ valid_from }}
        ) as next_starts_at
    from {{ model }}

)

select durable_key, starts_at, ends_at, next_starts_at
from ordered
where next_starts_at is not null
  and ends_at != next_starts_at

{% endtest %}
