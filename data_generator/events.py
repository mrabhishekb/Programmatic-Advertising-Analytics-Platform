"""Streaming event generation.

The funnel is generated one campaign-day at a time:

    impressions for (campaign, day)
        -> clicks selected from those impressions
        -> CPM/CPC spend derived from those impressions and clicks
    ... then, once the campaign's days are done:
        -> conversions selected from that campaign's clicks
        -> CPA spend derived from those conversions

Working campaign by campaign keeps peak memory proportional to a single
campaign's traffic rather than the whole dataset, which is what makes the
million-row SMALL profile (and the 100M-row LARGE profile) practical. It also
means a click can only ever be derived from an impression object that exists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter

from data_generator.clicks import select_clicks
from data_generator.context import GeneratorContext
from data_generator.conversions import select_conversions
from data_generator.impressions import (
    CampaignPlan,
    generate_impressions_for_day,
    plan_delivery,
)
from data_generator.logging_setup import get_logger
from data_generator.sinks import BatchedEmitter
from data_generator.spend_transactions import (
    billing_type_for,
    spend_for_clicks,
    spend_for_conversions,
    spend_for_impressions,
)
from data_generator.validation import EcosystemValidator

logger = get_logger(__name__)


@dataclass(slots=True)
class EventTotals:
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    spend_transactions: int = 0
    media_cost: float = 0.0
    billed_spend: float = 0.0
    conversion_value: float = 0.0
    delivering_campaigns: int = 0
    duration_seconds: float = 0.0
    spend_by_billing_type: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "impressions": self.impressions,
            "clicks": self.clicks,
            "conversions": self.conversions,
            "spend_transactions": self.spend_transactions,
            "media_cost": round(self.media_cost, 2),
            "billed_spend": round(self.billed_spend, 2),
            "conversion_value": round(self.conversion_value, 2),
            "delivering_campaigns": self.delivering_campaigns,
            "duration_seconds": round(self.duration_seconds, 3),
            "spend_by_billing_type": dict(self.spend_by_billing_type),
        }


def generate_events(
    ctx: GeneratorContext,
    emitter: BatchedEmitter,
    validator: EcosystemValidator | None = None,
    *,
    log_every: int = 250_000,
) -> EventTotals:
    started = perf_counter()
    totals = EventTotals()
    plans = plan_delivery(ctx)
    totals.delivering_campaigns = sum(1 for plan in plans if plan.total_impressions > 0)

    logger.info(
        "planned delivery",
        extra={
            "campaigns": len(plans),
            "delivering": totals.delivering_campaigns,
            "impressions": sum(plan.total_impressions for plan in plans),
        },
    )

    next_log = log_every
    for plan in plans:
        _generate_campaign(ctx, plan, emitter, validator, totals)
        if log_every and totals.impressions >= next_log:
            logger.info(
                "generating events",
                extra={
                    "impressions": totals.impressions,
                    "clicks": totals.clicks,
                    "conversions": totals.conversions,
                    "elapsed_s": round(perf_counter() - started, 1),
                },
            )
            next_log = totals.impressions + log_every

    emitter.flush()
    totals.duration_seconds = perf_counter() - started
    return totals


def _generate_campaign(
    ctx: GeneratorContext,
    plan: CampaignPlan,
    emitter: BatchedEmitter,
    validator: EcosystemValidator | None,
    totals: EventTotals,
) -> None:
    campaign = plan.campaign
    billing_type = billing_type_for(campaign.row.bid_strategy)
    campaign_clicks = []
    media_cost = 0.0
    billed_spend = 0.0

    for day_index, day in enumerate(plan.days):
        impressions = generate_impressions_for_day(ctx, plan, day_index)
        if not impressions:
            continue
        clicks = select_clicks(ctx, plan, impressions, day_index)

        day_ordinal = day.toordinal()
        spend_rows = spend_for_impressions(ctx, campaign, impressions, day_ordinal)
        spend_rows += spend_for_clicks(ctx, campaign, clicks, day_ordinal)

        if validator is not None:
            validator.validate_impressions(impressions)
            validator.validate_clicks(clicks)
            validator.validate_spend(spend_rows)

        emitter.emit([item.row for item in impressions])
        emitter.emit([item.row for item in clicks])
        emitter.emit(spend_rows)

        media_cost += sum(float(item.row.clearing_price) for item in impressions) / 1000.0
        billed_spend += sum(float(row.spend_amount) for row in spend_rows)
        totals.impressions += len(impressions)
        totals.clicks += len(clicks)
        totals.spend_transactions += len(spend_rows)
        if spend_rows:
            totals.spend_by_billing_type[billing_type] = totals.spend_by_billing_type.get(
                billing_type, 0
            ) + len(spend_rows)
        campaign_clicks.extend(clicks)

    conversions = select_conversions(
        ctx, plan, campaign_clicks, _conversion_cost_anchor(ctx, plan, billing_type, billed_spend)
    )
    if conversions:
        conversion_spend = spend_for_conversions(ctx, campaign, conversions)
        if validator is not None:
            validator.validate_conversions(conversions)
            validator.validate_spend(conversion_spend)
        emitter.emit([item.row for item in conversions])
        emitter.emit(conversion_spend)

        totals.conversions += len(conversions)
        totals.spend_transactions += len(conversion_spend)
        totals.conversion_value += sum(float(item.row.conversion_value) for item in conversions)
        billed_spend += sum(float(row.spend_amount) for row in conversion_spend)
        if conversion_spend:
            totals.spend_by_billing_type[billing_type] = totals.spend_by_billing_type.get(
                billing_type, 0
            ) + len(conversion_spend)

    totals.media_cost += media_cost
    totals.billed_spend += billed_spend


def _conversion_cost_anchor(
    ctx: GeneratorContext, plan: CampaignPlan, billing_type: str, billed_spend: float
) -> float:
    """The spend figure that conversion value is priced against.

    For CPM and CPC campaigns the spend is already known by the time conversions
    are generated. For CPA campaigns it is not - the spend *is* the conversions -
    so it is projected from the line item CPA bids that will be charged.
    """
    if billing_type != "CPA":
        return billed_spend
    if plan.conversions <= 0:
        return 0.0
    line_items = ctx.ecosystem.line_item_nodes(plan.campaign.campaign_id)
    if not line_items:
        return billed_spend
    mean_cpa = sum(float(node.row.bid_amount) for node in line_items) / len(line_items)
    return mean_cpa * plan.conversions
