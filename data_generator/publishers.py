"""Publisher generation - the root of the supply-side hierarchy.

Traffic is heavily concentrated: a handful of publishers carry a large share of
all impressions while most carry very little. That shape comes from a Pareto
draw on ``traffic_weight``, which the impression generator uses when it picks
inventory.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from data_generator.context import GeneratorContext
from data_generator.models import Publisher
from data_generator.relationships import PublisherNode
from data_generator.rng import RandomStream
from data_generator.timeline import business_datetime_between, start_of_day

_MUTATED_STATUSES = frozenset({"SUSPENDED", "CLOSED"})


def generate_publishers(ctx: GeneratorContext) -> list[Publisher]:
    count = ctx.config.scale.entities.publishers
    reference = ctx.reference
    timeline = ctx.config.timeline
    simulation_end = ctx.simulation_end
    alpha = ctx.config.entities.publisher_traffic_alpha

    earliest = simulation_end - timedelta(days=timeline.advertiser_history_days)
    # Supply is onboarded before the event window opens, so every impression can
    # reference a publisher and placement that already existed at the time.
    latest = max(earliest, start_of_day(timeline.event_window_start_date) - timedelta(days=1))

    rows: list[Publisher] = []
    for index in range(count):
        rng = ctx.stream("publishers", index)

        publisher_type = reference.publisher_type_sampler.pick(rng)
        country = reference.country_sampler.pick(rng)
        status = reference.publisher_status_sampler.pick(rng)
        name = ctx.names.publisher_name(rng, publisher_type)

        created_at = business_datetime_between(rng, earliest, latest, recency_bias=1.4)
        updated_at = _updated_at(ctx, rng, created_at, status)

        row = Publisher(
            publisher_id=rng.uuid4(),
            publisher_name=name,
            publisher_type=publisher_type,
            country=country.name,
            domain=ctx.names.publisher_domain(rng, name, country, index),
            publisher_status=status,
            created_at=created_at,
            updated_at=updated_at,
        )

        ctx.ecosystem.add_publisher(
            PublisherNode(
                row=row,
                country=country,
                # Pareto: a few properties carry most of the inventory.
                traffic_weight=rng.paretovariate(alpha),
            )
        )
        rows.append(row)

    return rows


def _updated_at(
    ctx: GeneratorContext, rng: RandomStream, created_at: datetime, status: str
) -> datetime:
    must_have_changed = status in _MUTATED_STATUSES
    if not must_have_changed and rng.random() > 0.4:
        return created_at
    return business_datetime_between(rng, created_at, ctx.simulation_end, recency_bias=2.0)
