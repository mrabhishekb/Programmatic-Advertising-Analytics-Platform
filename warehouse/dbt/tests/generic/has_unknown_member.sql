{#
    Asserts a dimension holds exactly one unknown member.

    Zero means a fact with an unresolvable reference has nowhere to land, and
    phase 12 will either drop the row or carry a null. More than one means the
    union picked up a real member whose key collided with the sentinel, which
    is the failure the sentinel's shape is chosen to prevent - so if this ever
    fires with a count above one, the key strategy is what to look at.
#}
{% test has_unknown_member(model, column_name) %}

select count(*) as unknown_members
from {{ model }}
where {{ column_name }} = {{ unknown_key() }}
having count(*) != 1

{% endtest %}
