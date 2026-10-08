{#
    The key a fact uses when its dimension reference cannot be resolved.

    Surrogate keys here are md5 hex from `dbt_utils.generate_surrogate_key`, so
    a sentinel containing a non-hex character cannot collide with a real one.
    That is worth more than the tidiness of the conventional -1: a collision
    would not raise, it would quietly attribute somebody's impressions to the
    unknown member and still pass every test.

    Why an unknown member exists at all, given that every foreign key in
    postgres/schema.sql is NOT NULL: referential integrity in the source says
    nothing about what arrives here. An impression can reach the warehouse in
    one load and the audience it targeted in the next, and the alternative to a
    row that absorbs it is an inner join that drops it or an outer join that
    leaves a null to trip over in every downstream aggregate.
#}
{% macro unknown_key() -%}
    'UNKNOWN'
{%- endmacro %}
