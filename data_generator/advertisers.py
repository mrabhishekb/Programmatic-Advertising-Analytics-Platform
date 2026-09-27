"""Advertiser generation - the root of the demand-side hierarchy.

Every advertiser is assigned a spend tier. The tier is not stored in the source
table (a real CRM would not expose it) but it drives how many campaigns the
advertiser runs, how large its budgets are and how much traffic it buys, which is
what creates the whale/long-tail shape that analytics work is actually about.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from data_generator.context import GeneratorContext
from data_generator.distributions import lognormal_around
from data_generator.models import Advertiser
from data_generator.relationships import AdvertiserNode
from data_generator.rng import RandomStream
from data_generator.timeline import business_datetime_between

#: Statuses that imply the record was touched after it was created.
_MUTATED_STATUSES = frozenset({"SUSPENDED", "CLOSED"})


def generate_advertisers(ctx: GeneratorContext) -> list[Advertiser]:
    count = ctx.config.scale.entities.advertisers
    reference = ctx.reference
    timeline = ctx.config.timeline
    simulation_end = ctx.simulation_end

    earliest = simulation_end - timedelta(days=timeline.advertiser_history_days)
    latest = simulation_end - timedelta(days=3)

    rows: list[Advertiser] = []
    for index in range(count):
        rng = ctx.stream("advertisers", index)

        industry = reference.industry_sampler.pick(rng)
        country = reference.country_sampler.pick(rng)
        tier = reference.advertiser_tier_sampler.pick(rng)
        status = reference.advertiser_status_sampler.pick(rng)

        # Recency bias: the platform has been signing up advertisers faster over time.
        created_at = business_datetime_between(rng, earliest, latest, recency_bias=1.6)
        updated_at = _updated_at(ctx, rng, created_at, status)

        row = Advertiser(
            advertiser_id=rng.uuid4(),
            advertiser_name=ctx.names.advertiser_name(rng, industry.name),
            industry=industry.name,
            billing_country=country.name,
            billing_currency=country.currency,
            account_status=status,
            created_at=created_at,
            updated_at=updated_at,
        )

        ctx.ecosystem.add_advertiser(
            AdvertiserNode(
                row=row,
                tier=tier,
                industry=industry,
                country=country,
                # Traffic appetite: tier sets the level, the log-normal draw makes
                # advertisers inside a tier differ from each other.
                volume_weight=tier.volume_index * lognormal_around(rng, 1.0, 0.85),
                budget_scale=tier.budget_index * lognormal_around(rng, 1.0, 0.55),
            )
        )
        rows.append(row)

    return rows


def _updated_at(
    ctx: GeneratorContext, rng: RandomStream, created_at: datetime, status: str
) -> datetime:
    """A suspended or closed account must have been modified after creation."""
    must_have_changed = status in _MUTATED_STATUSES
    if not must_have_changed and rng.random() > 0.45:
        return created_at
    return business_datetime_between(rng, created_at, ctx.simulation_end, recency_bias=2.0)
