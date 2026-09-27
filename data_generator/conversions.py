"""Conversion generation (last-click attribution).

Conversions are selected from the campaign's actual clicks. ``impression_id``,
``campaign_id`` and ``advertiser_id`` are inherited through the click, so the
attribution chain conversion -> click -> impression -> campaign is true by
construction rather than by convention.

Conversion value is anchored to what the campaign actually cost: the value of a
conversion is the campaign's realised cost per conversion multiplied by the
campaign's target ROAS. That is what makes ROAS a meaningful metric in the gold
layer instead of a random number.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from data_generator.clicks import GeneratedClick
from data_generator.context import GeneratorContext
from data_generator.distributions import (
    lognormal_around,
    weighted_sample_without_replacement,
)
from data_generator.impressions import CampaignPlan
from data_generator.models import Conversion
from data_generator.numeric import dec2
from data_generator.rng import RandomStream
from data_generator.timeline import apply_ingestion_lag

_VALUE_NOISE_SIGMA = 0.38


@dataclass(slots=True)
class GeneratedConversion:
    row: Conversion
    click: GeneratedClick


def select_conversions(
    ctx: GeneratorContext,
    plan: CampaignPlan,
    clicks: list[GeneratedClick],
    campaign_cost: float,
) -> list[GeneratedConversion]:
    """Pick exactly ``plan.conversions`` of the campaign's clicks and convert them."""
    count = min(plan.conversions, len(clicks))
    if count <= 0:
        return []

    campaign = plan.campaign
    rng = ctx.stream("conversions", campaign.campaign_id)
    cutoff = ctx.simulation_end
    attribution = ctx.config.attribution
    lag_seconds = ctx.config.timeline.ingestion_lag_seconds_max
    conversion_type_sampler = ctx.reference.conversion_type_samplers[
        campaign.row.campaign_objective
    ]
    currency = campaign.advertiser.row.billing_currency

    # Cost per conversion x target ROAS = average conversion value.
    value_anchor = (campaign_cost / count) * campaign.target_roas if campaign_cost > 0 else 0.0

    chosen = weighted_sample_without_replacement(
        rng, clicks, [item.impression.conversion_weight for item in clicks], count
    )

    conversions: list[GeneratedConversion] = []
    for source in chosen:
        click = source.row
        window_hours = rng.choices(
            attribution.window_hours_choices, weights=attribution.window_hours_weights, k=1
        )[0]
        delay_hours = _conversion_delay_hours(ctx, rng, click.click_timestamp, window_hours, cutoff)
        conversion_timestamp = click.click_timestamp + timedelta(hours=delay_hours)
        value = value_anchor * lognormal_around(rng, 1.0, _VALUE_NOISE_SIGMA)

        conversions.append(
            GeneratedConversion(
                row=Conversion(
                    conversion_id=rng.uuid4(),
                    click_id=click.click_id,
                    # Inherited through the click, never re-drawn.
                    impression_id=click.impression_id,
                    campaign_id=click.campaign_id,
                    advertiser_id=click.advertiser_id,
                    conversion_type=conversion_type_sampler.pick(rng),
                    conversion_timestamp=conversion_timestamp,
                    conversion_value=dec2(value),
                    currency=currency,
                    attribution_window_hours=window_hours,
                    created_at=min(
                        apply_ingestion_lag(rng, conversion_timestamp, lag_seconds), cutoff
                    ),
                ),
                click=source,
            )
        )
    return conversions


def _conversion_delay_hours(
    ctx: GeneratorContext, rng: RandomStream, click_time, window_hours: int, cutoff
) -> float:
    """Hours between click and conversion, inside the attribution window.

    Bounded by two things: the conversion must fall inside the window it claims
    to be attributed within, and it cannot happen after the observation cutoff.
    """
    funnel = ctx.config.funnel
    available_hours = (cutoff - click_time).total_seconds() / 3600.0
    limit = min(float(window_hours), max(available_hours, 0.0))
    if limit <= 0:
        return 0.0
    delay = lognormal_around(
        rng, funnel.conversion_delay_hours_median, funnel.conversion_delay_sigma
    )
    if delay > limit:
        delay = rng.random() * limit
    return delay
