"""Placement generation.

Placements are allocated to publishers with Pareto weights (large properties sell
far more inventory than small ones) and every attribute is conditioned on the
parent publisher: a CTV property sells pre/mid-roll video on CTV devices, a
mobile app sells banners and interstitials on phones. Floor prices combine the
country's price level with the placement type's price level.
"""

from __future__ import annotations

from datetime import timedelta

from data_generator.context import GeneratorContext
from data_generator.distributions import allocate_children, lognormal_around, pareto_weights
from data_generator.models import Placement
from data_generator.numeric import dec4
from data_generator.relationships import PlacementNode
from data_generator.timeline import business_datetime_between, start_of_day


def generate_placements(ctx: GeneratorContext) -> list[Placement]:
    ecosystem = ctx.ecosystem
    reference = ctx.reference
    total = ctx.config.scale.entities.placements
    publisher_nodes = list(ecosystem.publishers.values())
    if not publisher_nodes:
        return []

    allocation_rng = ctx.stream("placements", "allocation")
    weights = pareto_weights(
        allocation_rng, len(publisher_nodes), ctx.config.entities.placements_per_publisher_alpha
    )
    counts = allocate_children(total, weights)

    # All inventory exists before the first impression can be served.
    inventory_deadline = start_of_day(ctx.config.timeline.event_window_start_date) - timedelta(
        days=1
    )

    rows: list[Placement] = []
    for publisher_index, (publisher, placement_count) in enumerate(
        zip(publisher_nodes, counts, strict=True)
    ):
        publisher_type = publisher.row.publisher_type
        inventory_ready_by = max(publisher.row.created_at, inventory_deadline)
        placement_type_sampler = reference.placement_type_samplers[publisher_type]
        device_sampler = reference.device_samplers[publisher_type]
        country = publisher.country

        for ordinal in range(placement_count):
            rng = ctx.stream("placements", publisher_index, ordinal)

            placement_type = placement_type_sampler.pick(rng)
            ad_format = reference.ad_format_sampler(publisher_type, placement_type).pick(rng)
            profile = reference.placement_type_profile[placement_type]

            # Floor price is a CPM: regional price level x inventory quality level.
            floor_price = lognormal_around(
                rng,
                ctx.config.pricing.floor_price_median * country.cpm_index * profile.cpm_index,
                ctx.config.pricing.floor_price_sigma,
            )

            created_at = business_datetime_between(
                rng, publisher.row.created_at, inventory_ready_by, recency_bias=1.3
            )
            updated_at = (
                created_at
                if rng.random() > 0.35
                else business_datetime_between(rng, created_at, ctx.simulation_end)
            )

            row = Placement(
                placement_id=rng.uuid4(),
                publisher_id=publisher.publisher_id,
                placement_name=(
                    f"{publisher.row.publisher_name} - "
                    f"{placement_type.replace('_', ' ').title()} {ordinal + 1:02d}"
                ),
                placement_type=placement_type,
                ad_format=ad_format,
                floor_price=dec4(floor_price),
                currency=country.currency,
                device_type=device_sampler.pick(rng),
                created_at=created_at,
                updated_at=updated_at,
            )

            ecosystem.add_placement(
                PlacementNode(
                    row=row,
                    publisher=publisher,
                    profile=profile,
                    # Inherit the publisher's traffic level, then vary per slot.
                    traffic_weight=publisher.traffic_weight * lognormal_around(rng, 1.0, 0.7),
                )
            )
            rows.append(row)

    return rows
