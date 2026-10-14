{#
    The columns Silver adds to every row.

    Written once here because seven staging models would otherwise each carry
    their own copy, and the day the rename changes is the day six of them get
    updated.

    Note what this macro does *not* do: filter anything. Silver carries
    soft-deleted rows and staging carries them onward, because a dimension has
    to contain every member a fact might reference and because "what did that
    segment target before we removed it" stops being answerable the moment the
    row goes. Current-state filtering is a question for whoever is reporting,
    answered against `is_deleted` in the core dimensions.
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
