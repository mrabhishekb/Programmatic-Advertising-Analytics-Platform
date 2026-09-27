"""Audience segment generation.

Audience type drives the shape of the segment: a RETARGETING pool is small,
broadly aged and converts far better than a DEMOGRAPHIC pool. Those performance
indices are read by the click and conversion generators, so "retargeting
converts best" shows up in the analytics rather than being asserted in a README.

Segments are onboarded before the event window opens so that every impression can
reference a segment that already existed - see docs/data_generation.md.
"""

from __future__ import annotations

from datetime import timedelta

from data_generator.context import GeneratorContext
from data_generator.distributions import lognormal_around
from data_generator.models import Audience
from data_generator.relationships import AudienceNode
from data_generator.timeline import business_datetime_between, start_of_day


def generate_audiences(ctx: GeneratorContext) -> list[Audience]:
    count = ctx.config.scale.entities.audiences
    reference = ctx.reference
    timeline = ctx.config.timeline

    earliest = ctx.simulation_end - timedelta(days=timeline.advertiser_history_days)
    latest = start_of_day(timeline.event_window_start_date) - timedelta(days=1)
    if latest < earliest:
        latest = earliest

    rows: list[Audience] = []
    for index in range(count):
        rng = ctx.stream("audiences", index)

        audience_type = reference.audience_type_sampler.pick(rng)
        bracket = reference.age_bracket_sampler.pick(rng)
        country = reference.country_sampler.pick(rng)
        interest = rng.choice(reference.interest_categories)
        # Retargeting pools are built from site visitors, so they are rarely
        # narrowed by age or gender.
        gender = (
            "ALL" if audience_type == "RETARGETING" else reference.audience_gender_sampler.pick(rng)
        )

        created_at = business_datetime_between(rng, earliest, latest, recency_bias=1.5)
        updated_at = (
            business_datetime_between(rng, created_at, ctx.simulation_end, recency_bias=2.0)
            if rng.random() < 0.5
            else created_at
        )

        row = Audience(
            audience_id=rng.uuid4(),
            audience_name=ctx.names.audience_name(
                rng, audience_type, interest, bracket.min, bracket.max
            ),
            audience_type=audience_type,
            min_age=bracket.min,
            max_age=bracket.max,
            gender=gender,
            country=country.name,
            interest_category=interest,
            created_at=created_at,
            updated_at=updated_at,
        )

        ctx.ecosystem.add_audience(
            AudienceNode(
                row=row,
                profile=reference.audience_type_profile[audience_type],
                # Targeting share: a few segments are used far more than the rest.
                weight=lognormal_around(rng, 1.0, 0.9),
            )
        )
        rows.append(row)

    return rows
