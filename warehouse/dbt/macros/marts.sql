{% macro safe_divide(numerator, denominator) -%}
    {#-
        Division that returns null instead of failing on a zero denominator.

        Every rate in every mart needs this, because the denominator is a
        count and a count can legitimately be zero. A campaign that served
        impressions and got no clicks has no conversion rate - not a
        conversion rate of zero, which would be a claim that nobody converted
        out of the people who clicked, when nobody clicked.

        `nullif` is what makes that distinction. Null means the question does
        not apply here, and every aggregate downstream ignores it, so a week
        containing a day with no clicks still averages over the days that had
        some rather than being dragged toward zero by the days that did not.

        The numerator is cast to double so the result is not quietly rounded.
        Snowflake divides two integers into a fixed-point number with six
        decimal places, which is fine for a conversion rate and is not fine
        for a click-through rate of 0.0045 measured against a competitor's.
    -#}
    cast({{ numerator }} as double) / nullif({{ denominator }}, 0)
{%- endmacro %}


{% macro reaggregation_window(days=none) -%}
    {#-
        The incremental filter for a mart, as opposed to for a fact.

        incremental_watermark is wrong here and the difference is the whole
        reason this exists. A fact is a log: a row written yesterday is the
        same row today, so starting at the newest row and taking everything
        after it is complete. A daily aggregate is a summary, and the summary
        of today is wrong the moment one more event for today arrives.

        So the batch is not everything after the newest day, it is the newest
        day again plus the days behind it. Those days get recomputed from the
        fact and merged over whatever the last run concluded, which is also
        why the marts merge on the grain rather than insert: the second run
        has to be allowed to overwrite the first run's answer.

        Recomputing is cheap because the facts cluster naturally on date_key
        and the window becomes a partition-pruning predicate. Snowflake reads
        three days of impressions, not a hundred million.
    -#}
    {%- set days = var('mart_reaggregation_days') if days is none else days -%}
    {% if is_incremental() %}
        where date_key >= (
            select {{ date_key_lookback('max(date_key)', days) }}
            from {{ this }}
        )
    {% endif %}
{%- endmacro %}


{% macro resolution_window(batch_relation, days) -%}
    {#-
        Prunes the impression scan when resolving a click or a conversion back
        to the impression it belongs to.

        The join itself is an equality on impression_id and is correct without
        this. What this buys is that an incremental run resolving a few
        thousand new clicks reads the handful of days those clicks could
        possibly point back to, rather than every impression ever served.

        `min` and not `max`, because the impression comes before the event, so
        the question is how far back the oldest row in the batch can reach.
    -#}
    {% if is_incremental() %}
        where date_key >= (
            select {{ date_key_lookback('min(date_key)', days) }}
            from {{ batch_relation }}
        )
    {% endif %}
{%- endmacro %}


{% macro grain_of(relations, columns) -%}
    {#-
        The set of grain keys present in any of the aggregates being combined,
        as a union of selects.

        This is the alternative to chaining full outer joins, and it is here
        because the obvious thing to write is an inner join and the obvious
        thing is wrong. A campaign that served impressions and got no clicks
        has no row in the clicks aggregate. Inner-join them and the campaign
        vanishes from its own performance report - not with a zero, which
        someone would notice, but with nothing at all, which nobody does.

        Full outer joins fix that and cost a coalesce over every key in every
        join condition, which has to be got right four times and reads worse
        each time. Collecting the keys first and left-joining each aggregate
        onto them says the same thing once: these are the rows that exist, and
        here is what each source knew about them.

        `union` rather than `union all` - the point is the distinct set.
    -#}
    {% for relation in relations %}
        select {{ columns | join(', ') }} from {{ relation }}
        {%- if not loop.last %}
        union
        {% endif %}
    {%- endfor %}
{%- endmacro %}
