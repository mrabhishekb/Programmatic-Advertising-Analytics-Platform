"""Line item generation.

Every line item belongs to a campaign that already exists and flies inside that
campaign's dates. The first line item of each campaign deliberately spans the
whole flight so that every delivery day has at least one eligible line item -
the impression generator filters line items by date, and this invariant is what
guarantees the filter can never come back empty.

Bids are derived from the campaign's own expected performance: a campaign with a
high expected CTR can afford a lower CPC for the same CPM, so ``bid_amount`` is
computed from the effective CPM rather than drawn independently.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from data_generator.context import GeneratorContext
from data_generator.distributions import (
    allocate_children,
    clamp,
    lognormal_around,
    pareto_weights,
)
from data_generator.models import LineItem
from data_generator.numeric import dec4
from data_generator.relationships import CampaignNode, LineItemNode
from data_generator.rng import RandomStream
from data_generator.timeline import business_datetime_between, start_of_day

_FREQUENCY_CAPS = (1, 2, 3, 4, 5, 8, 10, 20)
_FREQUENCY_CAP_WEIGHTS = (8.0, 14.0, 22.0, 14.0, 16.0, 10.0, 10.0, 6.0)

#: Current line item status given the parent campaign's status. Historical
#: delivery is governed by the flight dates, not by these values - a COMPLETED
#: line item still served while it was running.
_STATUS_WEIGHTS_BY_CAMPAIGN_STATUS = {
    "DRAFT": {"DRAFT": 100.0},
    "ACTIVE": {"ACTIVE": 82.0, "PAUSED": 12.0, "DRAFT": 4.0, "COMPLETED": 2.0},
    "PAUSED": {"PAUSED": 74.0, "ACTIVE": 18.0, "DRAFT": 4.0, "COMPLETED": 4.0},
    "COMPLETED": {"COMPLETED": 86.0, "PAUSED": 8.0, "CANCELLED": 6.0},
    "CANCELLED": {"CANCELLED": 82.0, "COMPLETED": 10.0, "PAUSED": 8.0},
}


def generate_line_items(ctx: GeneratorContext) -> list[LineItem]:
    ecosystem = ctx.ecosystem
    total = ctx.config.scale.entities.line_items
    campaign_nodes = list(ecosystem.campaigns.values())
    if not campaign_nodes:
        return []

    allocation_rng = ctx.stream("line_items", "allocation")
    weights = pareto_weights(
        allocation_rng, len(campaign_nodes), ctx.config.entities.line_items_per_campaign_alpha
    )
    counts = allocate_children(total, weights)

    base_ctr, base_cvr = _baseline_rates(ctx)

    rows: list[LineItem] = []
    for campaign_index, (campaign, line_item_count) in enumerate(
        zip(campaign_nodes, counts, strict=True)
    ):
        for ordinal in range(line_item_count):
            rng = ctx.stream("line_items", campaign_index, ordinal)
            rows.append(_build_line_item(ctx, rng, campaign, ordinal, base_ctr, base_cvr))
    return rows


def _build_line_item(
    ctx: GeneratorContext,
    rng: RandomStream,
    campaign: CampaignNode,
    ordinal: int,
    base_ctr: float,
    base_cvr: float,
) -> LineItem:
    config = ctx.config
    advertiser = campaign.advertiser
    objective = campaign.row.campaign_objective

    start_date, end_date = _flight_dates(rng, campaign, ordinal)
    status = _status(ctx, rng, campaign, ordinal)
    created_at = _created_at(ctx, rng, campaign)
    updated_at = (
        business_datetime_between(rng, created_at, ctx.simulation_end, recency_bias=2.0)
        if status in {"PAUSED", "COMPLETED", "CANCELLED"} or rng.random() < 0.45
        else created_at
    )

    target_cpm = lognormal_around(
        rng,
        config.pricing.base_cpm_median * advertiser.country.cpm_index,
        config.pricing.base_cpm_sigma,
    )
    expected_ctr = clamp(
        base_ctr * campaign.ctr_multiplier, config.funnel.min_ctr, config.funnel.max_ctr
    )
    expected_cvr = clamp(
        base_cvr * campaign.cvr_multiplier, config.funnel.min_cvr, config.funnel.max_cvr
    )

    row = LineItem(
        line_item_id=rng.uuid4(),
        campaign_id=campaign.campaign_id,
        line_item_name=ctx.names.line_item_name(rng, campaign.row.campaign_name, ordinal + 1),
        line_item_status=status,
        bid_amount=dec4(
            _bid_amount(campaign.row.bid_strategy, target_cpm, expected_ctr, expected_cvr)
        ),
        bid_currency=advertiser.row.billing_currency,
        optimization_goal=ctx.reference.optimization_goal_samplers[objective].pick(rng),
        target_cpm=dec4(target_cpm),
        frequency_cap=rng.choices(_FREQUENCY_CAPS, weights=_FREQUENCY_CAP_WEIGHTS, k=1)[0],
        start_date=start_date,
        end_date=end_date,
        created_at=created_at,
        updated_at=updated_at,
    )

    ctx.ecosystem.add_line_item(
        LineItemNode(
            row=row,
            campaign_id=campaign.campaign_id,
            # Delivery share inside the campaign: the first line item usually
            # carries the bulk of the budget.
            weight=lognormal_around(rng, 2.0 if ordinal == 0 else 1.0, 0.6),
        )
    )
    return row


def _bid_amount(
    bid_strategy: str, target_cpm: float, expected_ctr: float, expected_cvr: float
) -> float:
    """Express the CPM bid in the unit the bid strategy is billed in.

    Keeping all three units anchored to the same effective CPM is what makes
    total spend comparable across billing types later on.
    """
    cost_per_impression = target_cpm / 1000.0
    if bid_strategy == "CPM":
        return target_cpm
    if bid_strategy == "CPC":
        return cost_per_impression / expected_ctr
    # CPA, TARGET_ROAS and MAX_CONVERSIONS are all billed per conversion.
    return cost_per_impression / (expected_ctr * expected_cvr)


def _flight_dates(rng: RandomStream, campaign: CampaignNode, ordinal: int) -> tuple[date, date]:
    """Line item dates always sit inside the campaign flight."""
    campaign_start = campaign.row.start_date
    campaign_end = campaign.row.end_date
    if ordinal == 0:
        # Guarantees full coverage of the campaign flight.
        return campaign_start, campaign_end

    span = (campaign_end - campaign_start).days
    if span <= 1:
        return campaign_start, campaign_end
    offset = rng.randint(0, max(span // 2, 1))
    start = campaign_start + timedelta(days=offset)
    end = start + timedelta(days=rng.randint(1, max(span - offset, 1)))
    return start, min(end, campaign_end)


def _status(ctx: GeneratorContext, rng: RandomStream, campaign: CampaignNode, ordinal: int) -> str:
    weights = _STATUS_WEIGHTS_BY_CAMPAIGN_STATUS[campaign.row.campaign_status]
    status = rng.choices(list(weights), weights=list(weights.values()), k=1)[0]
    if ordinal == 0 and campaign.row.campaign_status != "DRAFT" and status == "DRAFT":
        # The spanning line item must be servable for a campaign that delivers.
        return "ACTIVE"
    return status


def _created_at(ctx: GeneratorContext, rng: RandomStream, campaign: CampaignNode) -> datetime:
    """Set up after the campaign exists and before the flight opens."""
    upper = min(start_of_day(campaign.row.start_date), ctx.simulation_end)
    lower = campaign.row.created_at
    if upper <= lower:
        return lower
    return business_datetime_between(rng, lower, upper, recency_bias=1.4)


def _baseline_rates(ctx: GeneratorContext) -> tuple[float, float]:
    """Platform-wide average CTR and CVR implied by the configured event volumes."""
    events = ctx.config.scale.events
    base_ctr = events.clicks / events.impressions if events.impressions else 0.0
    base_cvr = events.conversions / events.clicks if events.clicks else 0.0
    return (
        clamp(base_ctr, ctx.config.funnel.min_ctr, ctx.config.funnel.max_ctr),
        clamp(base_cvr, ctx.config.funnel.min_cvr, ctx.config.funnel.max_cvr),
    )
