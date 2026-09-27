"""Campaign generation.

Two things make this module more than a row factory:

1. **Flight dates agree with the status.** An ACTIVE campaign brackets "now", a
   COMPLETED one ended in the past, a DRAFT one has not started. Dates are also
   clamped so a campaign can never predate the advertiser that owns it.
2. **Each campaign gets a performance profile** (CTR multiplier, CVR multiplier,
   traffic weight, target ROAS). These never reach the database; they are what
   the event generator samples against, so campaign-level CTR and ROAS vary the
   way they do in a real account.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from data_generator.context import GeneratorContext
from data_generator.distributions import (
    allocate_children,
    lognormal_around,
    lognormal_multiplier,
    pareto_weights,
)
from data_generator.models import Campaign
from data_generator.numeric import dec2
from data_generator.relationships import AdvertiserNode, CampaignNode
from data_generator.rng import RandomStream
from data_generator.timeline import business_datetime_between, date_between, start_of_day

_DURATION_DAYS = (7, 14, 21, 30, 45, 60, 90, 120, 180, 365)
_DURATION_WEIGHTS = (6.0, 11.0, 8.0, 21.0, 10.0, 14.0, 12.0, 7.0, 7.0, 4.0)

#: A campaign may not start until its advertiser has existed for two days. The
#: two-day gap (rather than one) guarantees there is always room to place a
#: created_at strictly between the advertiser's creation and the flight start.
_MIN_ADVERTISER_AGE_DAYS = 2

#: Objectives that are explicitly optimised for conversions convert better.
_CVR_INDEX_BY_OBJECTIVE = {
    "AWARENESS": 0.45,
    "TRAFFIC": 0.75,
    "ENGAGEMENT": 0.85,
    "CONVERSIONS": 1.75,
    "APP_INSTALLS": 1.45,
}


@dataclass(frozen=True, slots=True)
class Flight:
    status: str
    start_date: date
    end_date: date


def generate_campaigns(ctx: GeneratorContext) -> list[Campaign]:
    ecosystem = ctx.ecosystem
    total = ctx.config.scale.entities.campaigns
    advertiser_nodes = list(ecosystem.advertisers.values())
    if not advertiser_nodes:
        return []

    allocation_rng = ctx.stream("campaigns", "allocation")
    # Campaign count per advertiser: tier sets the level, Pareto adds the tail.
    weights = [
        node.tier.campaign_index * weight
        for node, weight in zip(
            advertiser_nodes,
            pareto_weights(
                allocation_rng,
                len(advertiser_nodes),
                ctx.config.entities.campaigns_per_advertiser_alpha,
            ),
            strict=True,
        )
    ]
    counts = allocate_children(total, weights)

    rows: list[Campaign] = []
    for advertiser_index, (advertiser, campaign_count) in enumerate(
        zip(advertiser_nodes, counts, strict=True)
    ):
        for ordinal in range(campaign_count):
            rng = ctx.stream("campaigns", advertiser_index, ordinal)
            rows.append(_build_campaign(ctx, rng, advertiser))

    return rows


def _build_campaign(
    ctx: GeneratorContext, rng: RandomStream, advertiser: AdvertiserNode
) -> Campaign:
    reference = ctx.reference
    config = ctx.config
    simulation_end_date = ctx.simulation_end_date

    objective = reference.campaign_objective_sampler.pick(rng)
    requested_status = reference.campaign_status_sampler.pick(rng)
    earliest_start = advertiser.row.created_at.date() + timedelta(days=_MIN_ADVERTISER_AGE_DAYS)

    flight = _choose_flight(
        rng,
        requested_status,
        earliest_start=earliest_start,
        simulation_end_date=simulation_end_date,
        max_lookback_days=config.timeline.campaign_history_days,
    )

    created_at = _created_at(ctx, rng, advertiser, flight)
    updated_at = _updated_at(ctx, rng, created_at, flight.status)

    duration_days = max((flight.end_date - flight.start_date).days, 1)
    country = advertiser.country
    daily_budget = max(
        10.0,
        lognormal_around(rng, 130.0 * advertiser.budget_scale * country.cpm_index, 0.62),
    )
    campaign_budget = daily_budget * duration_days * rng.uniform(0.85, 1.15)

    row = Campaign(
        campaign_id=rng.uuid4(),
        advertiser_id=advertiser.advertiser_id,
        campaign_name=ctx.names.campaign_name(rng, objective, flight.start_date.year),
        campaign_objective=objective,
        campaign_status=flight.status,
        campaign_budget=dec2(campaign_budget),
        daily_budget=dec2(daily_budget),
        start_date=flight.start_date,
        end_date=flight.end_date,
        bid_strategy=reference.bid_strategy_samplers[objective].pick(rng),
        created_at=created_at,
        updated_at=updated_at,
    )

    delivery_start, delivery_end = _delivery_window(ctx, flight)

    ctx.ecosystem.add_campaign(
        CampaignNode(
            row=row,
            advertiser=advertiser,
            ctr_multiplier=(
                advertiser.industry.ctr_index
                * lognormal_multiplier(rng, config.funnel.campaign_ctr_sigma)
            ),
            cvr_multiplier=(
                _CVR_INDEX_BY_OBJECTIVE[objective]
                * lognormal_multiplier(rng, config.funnel.campaign_cvr_sigma)
            ),
            # Traffic appetite: the advertiser's appetite, the campaign's own
            # Pareto draw, and the size of the budget all pull in the same direction.
            volume_weight=(
                advertiser.volume_weight
                * rng.paretovariate(config.entities.campaign_volume_alpha)
                * (daily_budget / 130.0) ** 0.5
            ),
            target_roas=lognormal_around(
                rng,
                config.pricing.roas_median * advertiser.industry.roas_index,
                config.pricing.roas_sigma,
            ),
            delivery_start=delivery_start,
            delivery_end=delivery_end,
        )
    )
    return row


# ---------------------------------------------------------------------------
# flight dates
# ---------------------------------------------------------------------------


def _choose_flight(
    rng: RandomStream,
    status: str,
    *,
    earliest_start: date,
    simulation_end_date: date,
    max_lookback_days: int,
) -> Flight:
    """Pick start/end dates that are consistent with the campaign status.

    When the advertiser is too young for the requested status (you cannot have a
    campaign that completed before the account existed) the status is downgraded
    rather than the dates being bent.
    """
    duration = rng.choices(_DURATION_DAYS, weights=_DURATION_WEIGHTS, k=1)[0]
    days_available = (simulation_end_date - earliest_start).days

    if status == "DRAFT" or days_available < 2:
        # A brand new advertiser cannot own a campaign that already ran, so the
        # status is downgraded to DRAFT instead of inventing impossible dates.
        start = simulation_end_date + timedelta(days=rng.randint(1, 30))
        return Flight("DRAFT", start, start + timedelta(days=duration))

    if status == "PAUSED":
        # A paused campaign is usually still inside its flight, occasionally one
        # that was paused and never resumed before the end date passed.
        status_shape = "ACTIVE" if rng.random() < 0.65 else "COMPLETED"
    else:
        status_shape = status

    if status_shape == "ACTIVE":
        elapsed = rng.randint(1, min(duration - 1, days_available))
        start = simulation_end_date - timedelta(days=elapsed)
        return Flight(status, start, start + timedelta(days=duration))

    # COMPLETED / CANCELLED: the flight has to fit entirely in the past.
    latest_end = simulation_end_date - timedelta(days=1)
    duration = min(duration, max((latest_end - earliest_start).days, 1))
    if duration < 3:
        elapsed = rng.randint(1, max(days_available, 1))
        start = simulation_end_date - timedelta(days=elapsed)
        return Flight("ACTIVE", start, start + timedelta(days=max(elapsed + 1, 7)))

    earliest_end = earliest_start + timedelta(days=duration)
    lookback_floor = simulation_end_date - timedelta(days=max_lookback_days)
    window_start = max(earliest_end, lookback_floor)
    if window_start > latest_end:
        window_start = earliest_end
    end = date_between(rng, window_start, latest_end, recency_bias=1.9)
    return Flight(status, end - timedelta(days=duration), end)


def _created_at(
    ctx: GeneratorContext, rng: RandomStream, advertiser: AdvertiserNode, flight: Flight
) -> datetime:
    """Set up happens before the flight starts, and never before the advertiser existed."""
    flight_start = start_of_day(flight.start_date)
    upper = min(flight_start - timedelta(hours=1), ctx.simulation_end)
    lower = max(advertiser.row.created_at, upper - timedelta(days=21))
    if lower > upper:  # defensive: only reachable if the clamps ever change
        return upper
    return business_datetime_between(rng, lower, upper, recency_bias=1.5)


def _updated_at(
    ctx: GeneratorContext, rng: RandomStream, created_at: datetime, status: str
) -> datetime:
    # Statuses that can only be reached by editing the campaign after creation.
    if status in {"PAUSED", "COMPLETED", "CANCELLED"} or rng.random() < 0.5:
        return business_datetime_between(rng, created_at, ctx.simulation_end, recency_bias=2.2)
    return created_at


def _delivery_window(ctx: GeneratorContext, flight: Flight) -> tuple[date | None, date | None]:
    """The days on which this campaign may legitimately produce impressions.

    The intersection of the campaign flight with the configured event window.
    DRAFT campaigns never deliver.
    """
    if flight.status == "DRAFT":
        return None, None
    timeline = ctx.config.timeline
    start = max(flight.start_date, timeline.event_window_start_date)
    end = min(flight.end_date, timeline.event_window_end_date)
    if start > end:
        return None, None
    return start, end
