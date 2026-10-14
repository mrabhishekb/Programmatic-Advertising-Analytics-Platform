{#
    Validity ranges for the Type 2 dimensions.

    dbt's snapshots already carry `dbt_valid_from` and `dbt_valid_to`, and the
    obvious move is to rename those and stop. They record the wrong thing.
    Under the `check` strategy they hold the moment the *snapshot ran*, not the
    moment the change happened - so the first run stamps every entity with
    today's date, and a range join from an impression served three months ago
    finds no version covering it. Every fact in phase 12 would fall outside
    every range and land on the unknown member.

    So processing time is kept, under a name that says what it is
    (`captured_at`), and the validity range is rebuilt from the source's own
    timestamps. The macros below are that rebuild, written once because seven
    dimensions would otherwise each carry a copy of a window function nobody
    would want to re-derive.
#}

{#
    When a version's content became true, in business time.

    `coalesce(greatest(updated_at, deleted_at), updated_at)` is the raw answer:
    the last edit, or the deletion if that came later. `greatest` returns null
    in Snowflake when any argument is null, which is what makes the coalesce do
    the work for live rows.

    The running maximum around it is not decoration. `updated_at` is written by
    the generator in local time while `deleted_at` is derived from Debezium's
    event time in UTC, so the raw expression can go *backwards* between two
    versions of the same row - and a validity range that starts before the one
    it replaces produces overlapping versions, which is the one thing a Type 2
    dimension must never have. Clamping to the running maximum makes the
    sequence non-decreasing whatever the timestamps do.

    Ties collapse to a zero-length interval, which a half-open range join
    skips. That is the correct outcome: a version that was superseded at the
    same instant it began was never true of anything.
#}
{% macro version_started_at(unique_key) -%}
    max(coalesce(greatest(updated_at, deleted_at), updated_at)) over (
        partition by {{ unique_key }}
        order by dbt_valid_from
        rows between unbounded preceding and current row
    )
{%- endmacro %}

{#
    When it stopped being true: the moment the next version began.

    Reads `version_at` rather than `valid_from` so it can be computed in the
    same select, and because the two differ only for the first version, whose
    backdating must not pull the previous version's end backwards - there being
    no previous version to pull.
#}
{% macro version_ended_at(unique_key) -%}
    coalesce(
        lead(version_at) over (
            partition by {{ unique_key }}
            order by dbt_valid_from
        ),
        {{ end_of_time() }}
    )
{%- endmacro %}

{#
    The open end of a current version, as a date rather than null.

    A null would need `coalesce` or `is null` in every range join written
    against these dimensions from here on, and the one place it gets forgotten
    is the place that silently drops every current row.
#}
{% macro end_of_time() -%}
    to_timestamp_ntz('9999-12-31 00:00:00')
{%- endmacro %}

{#
    The open start of the unknown member, for the same reason: a fact that
    cannot resolve its dimension has to land somewhere whatever its timestamp.
#}
{% macro beginning_of_time() -%}
    to_timestamp_ntz('1900-01-01 00:00:00')
{%- endmacro %}
