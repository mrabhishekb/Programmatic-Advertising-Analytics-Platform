"""Spend generation.

Spend is derived from delivery, never invented. How it is recorded depends on how
the campaign is billed:

* ``CPM``  - one transaction per (campaign, line item, publisher, hour), summing
  the clearing price of the impressions in that bucket. ``impression_id`` is NULL
  because the row covers many impressions, which is also how real billing systems
  aggregate high-volume display spend.
* ``CPC``  - one transaction per click, priced at the line item's CPC bid.
* ``CPA``  - one transaction per conversion, priced at the line item's CPA bid.

Because the CPC and CPA bids were themselves derived from the same effective CPM
(see ``line_items._bid_amount``), total spend stays comparable across billing
types instead of jumping by orders of magnitude.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, time, timedelta

from data_generator.clicks import GeneratedClick
from data_generator.context import GeneratorContext
from data_generator.conversions import GeneratedConversion
from data_generator.impressions import GeneratedImpression
from data_generator.models import SpendTransaction
from data_generator.numeric import dec6
from data_generator.relationships import CampaignNode
from data_generator.rng import RandomStream

_BILLING_TYPE_BY_BID_STRATEGY = {
    "CPM": "CPM",
    "CPC": "CPC",
    "CPA": "CPA",
    "TARGET_ROAS": "CPA",
    "MAX_CONVERSIONS": "CPA",
}

#: Billing systems close an hourly bucket shortly after the hour ends.
_BILLING_CLOSE_LAG = timedelta(hours=1)

_PRICE_NOISE = (0.92, 1.08)


def billing_type_for(bid_strategy: str) -> str:
    return _BILLING_TYPE_BY_BID_STRATEGY[bid_strategy]


def spend_for_impressions(
    ctx: GeneratorContext,
    campaign: CampaignNode,
    impressions: list[GeneratedImpression],
    day_ordinal: int,
) -> list[SpendTransaction]:
    """Hourly CPM roll-up for a campaign-day. Empty for CPC/CPA campaigns."""
    if billing_type_for(campaign.row.bid_strategy) != "CPM" or not impressions:
        return []

    rng = ctx.stream("spend", "cpm", campaign.campaign_id, day_ordinal)
    cutoff = ctx.simulation_end
    per_impression = ctx.config.spend.cpm_rollup == "per_impression"

    if per_impression:
        return [
            _transaction(
                rng=rng,
                campaign=campaign,
                line_item_id=item.line_item.line_item_id,
                publisher_id=item.placement.publisher.publisher_id,
                impression_id=item.row.impression_id,
                spend_timestamp=item.row.impression_timestamp,
                amount=float(item.row.clearing_price) / 1000.0,
                billing_type="CPM",
                created_at=item.row.created_at,
            )
            for item in impressions
        ]

    buckets: dict[tuple, float] = defaultdict(float)
    for item in impressions:
        timestamp = item.row.impression_timestamp
        key = (
            item.line_item.line_item_id,
            item.placement.publisher.publisher_id,
            datetime.combine(timestamp.date(), time(hour=timestamp.hour)),
        )
        buckets[key] += float(item.row.clearing_price) / 1000.0

    rows: list[SpendTransaction] = []
    # Sorted so the output is stable regardless of dict iteration order.
    for (line_item_id, publisher_id, hour_start), amount in sorted(
        buckets.items(), key=lambda entry: (entry[0][2], entry[0][0].int, entry[0][1].int)
    ):
        rows.append(
            _transaction(
                rng=rng,
                campaign=campaign,
                line_item_id=line_item_id,
                publisher_id=publisher_id,
                impression_id=None,
                spend_timestamp=hour_start,
                amount=amount,
                billing_type="CPM",
                created_at=min(hour_start + _BILLING_CLOSE_LAG, cutoff),
            )
        )
    return rows


def spend_for_clicks(
    ctx: GeneratorContext,
    campaign: CampaignNode,
    clicks: list[GeneratedClick],
    day_ordinal: int,
) -> list[SpendTransaction]:
    """One transaction per click. Empty for CPM/CPA campaigns."""
    if billing_type_for(campaign.row.bid_strategy) != "CPC" or not clicks:
        return []

    rng = ctx.stream("spend", "cpc", campaign.campaign_id, day_ordinal)
    return [
        _transaction(
            rng=rng,
            campaign=campaign,
            line_item_id=click.impression.line_item.line_item_id,
            publisher_id=click.impression.placement.publisher.publisher_id,
            impression_id=click.row.impression_id,
            spend_timestamp=click.row.click_timestamp,
            amount=float(click.impression.line_item.row.bid_amount) * rng.uniform(*_PRICE_NOISE),
            billing_type="CPC",
            created_at=click.row.created_at,
        )
        for click in clicks
    ]


def spend_for_conversions(
    ctx: GeneratorContext,
    campaign: CampaignNode,
    conversions: list[GeneratedConversion],
) -> list[SpendTransaction]:
    """One transaction per conversion. Empty for CPM/CPC campaigns."""
    if billing_type_for(campaign.row.bid_strategy) != "CPA" or not conversions:
        return []

    rng = ctx.stream("spend", "cpa", campaign.campaign_id)
    rows: list[SpendTransaction] = []
    for conversion in conversions:
        impression = conversion.click.impression
        rows.append(
            _transaction(
                rng=rng,
                campaign=campaign,
                line_item_id=impression.line_item.line_item_id,
                publisher_id=impression.placement.publisher.publisher_id,
                impression_id=conversion.row.impression_id,
                spend_timestamp=conversion.row.conversion_timestamp,
                amount=float(impression.line_item.row.bid_amount) * rng.uniform(*_PRICE_NOISE),
                billing_type="CPA",
                created_at=conversion.row.created_at,
            )
        )
    return rows


def _transaction(
    *,
    rng: RandomStream,
    campaign: CampaignNode,
    line_item_id,
    publisher_id,
    impression_id,
    spend_timestamp: datetime,
    amount: float,
    billing_type: str,
    created_at: datetime,
) -> SpendTransaction:
    return SpendTransaction(
        spend_transaction_id=rng.uuid4(),
        campaign_id=campaign.campaign_id,
        line_item_id=line_item_id,
        advertiser_id=campaign.advertiser_id,
        publisher_id=publisher_id,
        impression_id=impression_id,
        spend_timestamp=spend_timestamp,
        spend_amount=dec6(max(amount, 0.0)),
        currency=campaign.advertiser.row.billing_currency,
        billing_type=billing_type,
        created_at=max(created_at, spend_timestamp),
    )
