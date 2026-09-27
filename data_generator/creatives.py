"""Creative generation.

Creatives are only given types that the generated inventory can actually serve -
an AUDIO creative is pointless if no audio placement exists - and each one gets a
CTR multiplier so some assets genuinely outperform others.

The first creative of every advertiser is created within a day of the account and
is always ACTIVE. That is not cosmetic: the impression generator only selects
creatives that existed at the time of the impression, and this invariant
guarantees that the "as of" pool is never empty.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from data_generator.context import GeneratorContext
from data_generator.distributions import (
    WeightedSampler,
    allocate_children,
    lognormal_around,
    lognormal_multiplier,
    pareto_weights,
)
from data_generator.models import Creative
from data_generator.relationships import AdvertiserNode, CreativeNode
from data_generator.rng import RandomStream
from data_generator.timeline import business_datetime_between


def generate_creatives(ctx: GeneratorContext) -> list[Creative]:
    ecosystem = ctx.ecosystem
    total = ctx.config.scale.entities.creatives
    advertiser_nodes = list(ecosystem.advertisers.values())
    if not advertiser_nodes:
        return []

    # Only offer creative types that have compatible inventory.
    available_types = ecosystem.creative_types_with_inventory()
    type_sampler = WeightedSampler.from_mapping(
        ctx.reference.creative_type_weights_for(available_types)
    )

    allocation_rng = ctx.stream("creatives", "allocation")
    weights = pareto_weights(
        allocation_rng, len(advertiser_nodes), ctx.config.entities.creatives_per_advertiser_alpha
    )
    counts = allocate_children(total, weights)

    rows: list[Creative] = []
    for advertiser_index, (advertiser, creative_count) in enumerate(
        zip(advertiser_nodes, counts, strict=True)
    ):
        for ordinal in range(creative_count):
            rng = ctx.stream("creatives", advertiser_index, ordinal)
            rows.append(_build_creative(ctx, rng, advertiser, ordinal, type_sampler))
    return rows


def _build_creative(
    ctx: GeneratorContext,
    rng: RandomStream,
    advertiser: AdvertiserNode,
    ordinal: int,
    type_sampler: WeightedSampler[str],
) -> Creative:
    reference = ctx.reference
    creative_type = type_sampler.pick(rng)
    creative_format = reference.creative_format_samplers[creative_type].pick(rng)
    status = "ACTIVE" if ordinal == 0 else reference.creative_status_sampler.pick(rng)

    created_at = _created_at(ctx, rng, advertiser, ordinal)
    updated_at = (
        business_datetime_between(rng, created_at, ctx.simulation_end, recency_bias=2.0)
        if status != "ACTIVE" or rng.random() < 0.4
        else created_at
    )

    brand = advertiser.row.advertiser_name
    row = Creative(
        creative_id=rng.uuid4(),
        advertiser_id=advertiser.advertiser_id,
        creative_name=ctx.names.creative_name(rng, brand, creative_type, creative_format),
        creative_type=creative_type,
        creative_format=creative_format,
        landing_page_url=ctx.names.landing_page_url(rng, brand, f"{creative_type}-{ordinal + 1}"),
        creative_status=status,
        created_at=created_at,
        updated_at=updated_at,
    )

    ctx.ecosystem.add_creative(
        CreativeNode(
            row=row,
            advertiser_id=advertiser.advertiser_id,
            # Creative quality: this is what makes creative_performance a report
            # worth looking at rather than uniform noise.
            ctr_multiplier=lognormal_multiplier(rng, ctx.config.funnel.creative_ctr_sigma),
            serving_weight=lognormal_around(rng, 1.0, 0.7),
        )
    )
    return row


def _created_at(
    ctx: GeneratorContext, rng: RandomStream, advertiser: AdvertiserNode, ordinal: int
) -> datetime:
    account_created = advertiser.row.created_at
    if ordinal == 0:
        upper = min(account_created + timedelta(days=1), ctx.simulation_end)
        return business_datetime_between(rng, account_created, max(account_created, upper))
    return business_datetime_between(rng, account_created, ctx.simulation_end, recency_bias=1.5)
