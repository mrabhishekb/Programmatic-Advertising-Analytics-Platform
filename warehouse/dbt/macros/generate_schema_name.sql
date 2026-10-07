{#
    Use the configured schema as written, instead of appending it to the one in
    the profile.

    dbt's default builds `<target.schema>_<custom schema>`, so a model
    configured with `+schema: STAGING` against a profile whose schema is also
    STAGING lands in STAGING_STAGING - beside the four schemas bootstrap
    created, none of which get used. It is the single most common surprise in a
    new dbt project, and it fails quietly: the run succeeds, the tests pass, and
    the tables are simply not where anything expects them.

    The default exists to stop developers sharing one warehouse from
    overwriting each other's work, since each person's target schema prefixes
    everything they build. That is worth giving up here: this project owns its
    database outright, and the four layers are named in bootstrap.sql,
    docs/warehouse.md and the architecture diagram. A warehouse whose schemas
    depend on who ran dbt last would contradict all three.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
