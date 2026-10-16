{% test resolves_within_window(model, column_name, window_column) %}
{#-
    Asserts that a measured lag never exceeds the window the source itself
    declared for that row.

    This exists to guard an optimisation. int_conversions_resolved prunes the
    impressions it scans to the last fourteen days, because 336 hours is the
    largest attribution_window_hours the source ever sets. That is a sound
    reason to prune and a terrible thing to leave unchecked: if the source
    ever starts issuing a 30-day window, the join stops finding impressions
    that are genuinely there, the keys quietly fall back to the unknown
    member, and the audience mart starts under-reporting conversions without
    anything failing.

    Comparing each row against its own window rather than against a constant
    is what makes the test worth having. A constant would only catch the
    windows getting longer than the var; this catches any row whose
    impression is older than that row was willing to look back, which is the
    actual invariant.

    resolves_to_a_version catches the consequence - a key that landed on the
    unknown member. This catches the cause, and does it one step earlier.
-#}

select
    {{ column_name }}  as measured_lag,
    {{ window_column }} as declared_window,
    count(*)           as rows_outside_window
from {{ model }}
where {{ column_name }} > {{ window_column }}
group by 1, 2

{% endtest %}
