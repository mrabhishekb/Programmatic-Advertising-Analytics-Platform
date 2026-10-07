{#
    The columns Silver adds to every row, and the filter that hides the ones
    reconciliation kept but current-state analysis does not want.

    Written once here because seven staging models would otherwise each carry
    their own copy, and the day the soft-delete rule changes is the day six of
    them get updated.
#}

{% macro silver_lineage_columns() %}
    is_deleted,
    deleted_at,
    -- The WAL position of the change that produced this row, null for rows that
    -- came straight from the snapshot export. Phase 11's snapshots order
    -- history by it, so it is carried through staging rather than dropped.
    _lsn    as source_lsn,
    _op     as source_operation,
    _event_ts as source_event_at
{% endmacro %}


{#
    Current state unless asked otherwise.

    Silver keeps deleted rows with `is_deleted` set, because deleting an
    audience segment outright would orphan every historical impression that
    referenced it. Analysis of what exists *now* still has to exclude them, and
    forgetting to is the easiest way to overstate a count.
#}
{% macro only_live_rows() %}
    {% if not var('include_deleted', false) %}
        where not is_deleted
    {% endif %}
{% endmacro %}
