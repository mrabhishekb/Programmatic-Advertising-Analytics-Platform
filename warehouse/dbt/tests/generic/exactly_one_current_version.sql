{#
    Asserts every entity in a Type 2 dimension has exactly one current version.

    Two is the dangerous one: a report that filters `where is_current` and
    aggregates would count the entity twice, which looks like growth rather
    than like a bug. Zero is louder but just as wrong - the entity vanishes
    from every current-state report while still appearing in historical ones.

    Checking both directions matters because they come from opposite mistakes.
    More than one means versions were recorded without closing the previous;
    none means a close was applied to the last version too.
#}
{% test exactly_one_current_version(model, column_name) %}

select
    {{ column_name }} as durable_key,
    count_if(is_current) as current_versions
from {{ model }}
group by 1
having count_if(is_current) != 1

{% endtest %}
