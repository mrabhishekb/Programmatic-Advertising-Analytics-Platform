"""Click generation.

Clicks are selected *from impressions that were just generated*, never
constructed from scratch. The campaign, line item, advertiser and creative on a
click row are copied off the impression, so a click can never disagree with the
impression it belongs to.

Which impressions get clicked is not uniform: the selection is weighted by the
creative's quality, the placement type, the device, the audience and how
viewable the impression was.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from data_generator.context import GeneratorContext
from data_generator.distributions import (
    lognormal_around,
    weighted_sample_without_replacement,
)
from data_generator.impressions import CampaignPlan, GeneratedImpression
from data_generator.models import Click
from data_generator.rng import RandomStream
from data_generator.timeline import apply_ingestion_lag


@dataclass(slots=True)
class GeneratedClick:
    row: Click
    impression: GeneratedImpression


def select_clicks(
    ctx: GeneratorContext,
    plan: CampaignPlan,
    impressions: list[GeneratedImpression],
    day_index: int,
) -> list[GeneratedClick]:
    """Pick exactly ``plan.clicks_per_day[day_index]`` of the day's impressions."""
    count = plan.clicks_per_day[day_index]
    if count <= 0 or not impressions:
        return []

    campaign = plan.campaign
    day = plan.days[day_index]
    rng = ctx.stream("clicks", campaign.campaign_id, day.toordinal())
    cutoff = ctx.simulation_end
    lag_seconds = ctx.config.timeline.ingestion_lag_seconds_max

    chosen = weighted_sample_without_replacement(
        rng, impressions, [item.click_weight for item in impressions], count
    )

    clicks: list[GeneratedClick] = []
    for source in chosen:
        impression = source.row
        delay = _click_delay_seconds(ctx, rng, impression.impression_timestamp, cutoff)
        click_timestamp = impression.impression_timestamp + timedelta(seconds=delay)
        clicks.append(
            GeneratedClick(
                row=Click(
                    click_id=rng.uuid4(),
                    # Everything below is inherited, never re-drawn.
                    impression_id=impression.impression_id,
                    campaign_id=impression.campaign_id,
                    line_item_id=impression.line_item_id,
                    advertiser_id=impression.advertiser_id,
                    creative_id=impression.creative_id,
                    click_timestamp=click_timestamp,
                    device_type=impression.device_type,
                    country=impression.country,
                    created_at=min(apply_ingestion_lag(rng, click_timestamp, lag_seconds), cutoff),
                ),
                impression=source,
            )
        )
    return clicks


def _click_delay_seconds(
    ctx: GeneratorContext, rng: RandomStream, impression_time, cutoff
) -> float:
    """Seconds between the impression and the click.

    Log-normal, but truncated so the click cannot land after the observation
    cutoff. Truncation redraws uniformly inside the remaining time instead of
    clamping, which would otherwise pile clicks up on the cutoff instant.
    """
    funnel = ctx.config.funnel
    available = (cutoff - impression_time).total_seconds()
    limit = min(float(funnel.click_delay_seconds_max), max(available, 0.0))
    if limit <= 0:
        return 0.0
    delay = lognormal_around(rng, funnel.click_delay_seconds_median, funnel.click_delay_sigma)
    if delay > limit:
        delay = rng.random() * limit
    return delay
