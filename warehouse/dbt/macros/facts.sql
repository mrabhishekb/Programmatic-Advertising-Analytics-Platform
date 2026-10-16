-- Macros shared by the fact models in phase 12.


{% macro date_key(timestamp_column) -%}
    {#-
        The one key in the warehouse that is not a hash.

        A date dimension is the exception to "surrogate keys should be opaque".
        20261003 is sortable, range-prunable, readable in a query result
        without a join, and derivable from the timestamp by arithmetic - so a
        fact never has to join to dim_date just to know its own day. An md5 of
        the date would give up all four of those for a consistency nobody
        benefits from.

        Computed rather than looked up for the same reason. At 100 million rows
        a join that returns a value the row already contains is pure cost.
    -#}
    year({{ timestamp_column }}) * 10000 + month({{ timestamp_column }}) * 100 + day({{ timestamp_column }})
{%- endmacro %}


{% macro join_version(history_model, alias, natural_key, event_timestamp, event_alias='e') -%}
    {#-
        Attach the version of a dimension member that was in force when an
        event happened.

        This is the join a Type 2 dimension exists for, and the one that is
        easy to get wrong in two directions.

        Fan-out is the first. A range join whose windows overlap quietly
        multiplies the fact and every measure on it. That cannot happen here,
        because versions_are_contiguous asserts on every build that each
        version ends exactly where the next begins - so at most one window
        contains any instant.

        Cost is the second. The equality on the natural key is what makes this
        affordable: Snowflake hash-joins 100 million rows against 50,000
        versions, gets roughly one match per probe, and the two range
        predicates only filter that handful. Dropping the equality and leaning
        on the range alone would turn the same query into a nested loop.

        Left, not inner, because an event timestamped before its dimension
        member existed has no version in force. That should produce a null to
        be counted, not a row to be silently dropped from a fact table.
    -#}
    left join {{ ref(history_model) }} as {{ alias }}
        on  {{ alias }}.{{ natural_key }} = {{ event_alias }}.{{ natural_key }}
        and {{ event_alias }}.{{ event_timestamp }} >= {{ alias }}.valid_from
        and {{ event_alias }}.{{ event_timestamp }} <  {{ alias }}.valid_to
{%- endmacro %}


{% macro merge_floor(schema=none, lookback_days=0) -%}
    {#-
        The predicate that keeps an incremental merge from scanning the whole
        fact table.

        A merge has to find the rows it might update, and by default that means
        every row in the target. Restricting it to the days the batch actually
        touches is the difference between a one-day load reading one day and a
        one-day load reading 100 million rows.

        The obvious way to write it does not work:

            DBT_INTERNAL_DEST.date_key >= (select min(date_key) from DBT_INTERNAL_SOURCE)

        Snowflake compiles that to "Object 'DBT_INTERNAL_SOURCE' does not
        exist". The merge's source alias is in scope for the ON clause itself
        but not inside a subquery within it, so the floor cannot be derived
        from the batch at all - it has to arrive as a literal.

        Which is what this does: one cheap max() against the target before the
        merge is built, inlined as an integer. The target's newest day is
        exactly the floor we want, because incremental_watermark starts the
        batch there.

        Computing the floor in Jinja instead does not work either, for a
        subtler reason: dbt resolves config while it parses, long before there
        is a warehouse connection to ask, so a `run_query` here returns nothing
        and the predicate silently becomes `1 = 1` - which compiles, runs, and
        scans the whole table.

        So the floor comes from the target itself, named explicitly. `{{ this }}`
        would be the obvious way to name it and is wrong for the third version
        of the same reason: config is resolved before generate_schema_name
        runs, so at this point `this` still says STAGING, and the predicate
        reads an empty relation - or worse, a real one - without complaining.
        The schema is read from the var that configures it instead, so the two
        cannot drift apart.

        Snowflake reads the target from one consistent snapshot taken before
        the merge begins, so `max(date_key)` is the newest day the fact held on
        the way in - exactly where incremental_watermark started the batch. It
        is also close to free: Snowflake answers max() from micro-partition
        metadata without reading the column.

        Reading the table being written sounds like it should be rejected and
        is not. The semantics were checked rather than assumed: a batch
        spanning the boundary updates the row it shares and inserts the one it
        does not.

        Note for phase 16: this prunes the merge, it does not define the batch.
        An event arriving late enough to predate the watermark is never
        selected out of staging in the first place, so late arrivals are a
        problem for incremental_watermark to solve, not this.

        `lookback_days` exists for the marts in phase 13, whose batch is not
        everything after the newest row. A daily aggregate has to revisit the
        days it already summarised, because the newest one was partial when it
        was summarised. The floor has to move back with the batch or the merge
        prunes away the very rows it is supposed to update, and the stale
        totals stay exactly where they are.
    -#}
    {%- set schema = schema or var('core_schema') -%}
    DBT_INTERNAL_DEST.date_key >= (
        select {{ date_key_lookback('max(date_key)', lookback_days) }}
        from {{ target.database }}.{{ schema }}.{{ this.identifier }}
    )
{%- endmacro %}


{% macro date_key_lookback(date_key_expression, days) -%}
    {#-
        `date_key` minus some number of days, still as a date_key.

        The integer is readable precisely because it is not a number you can do
        arithmetic on: 20260301 minus 3 is 20260298, which is not a date. So it
        goes back through DATE and returns, which is the cost of the format and
        the reason this is a macro rather than four copies of it.
    -#}
    {%- if days == 0 -%}
        coalesce({{ date_key_expression }}, 0)
    {%- else -%}
        coalesce(
            to_number(to_char(
                dateadd(day, -{{ days }}, to_date(to_char({{ date_key_expression }}), 'YYYYMMDD'))
            , 'YYYYMMDD')),
            0
        )
    {%- endif -%}
{%- endmacro %}


{% macro incremental_watermark(timestamp_column) -%}
    {#-
        The predicate that keeps a rebuild from being a reload.

        `>=` rather than `>`, paired with a unique_key so the overlap merges
        instead of duplicating. Events sharing the boundary timestamp are
        common at these volumes, and `>` would drop every one of them except
        whichever arrived first.
    -#}
    {% if is_incremental() %}
        where {{ timestamp_column }} >= (
            select coalesce(max({{ timestamp_column }}), {{ beginning_of_time() }})
            from {{ this }}
        )
    {% endif %}
{%- endmacro %}
