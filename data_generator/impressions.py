"""Impression generation.

An impression is never assembled from independently drawn identifiers. It is
built by walking the ecosystem graph:

    campaign        -> chosen from the campaigns that can deliver on this day
    line item       -> chosen from that campaign's line items whose flight covers the day
    advertiser      -> read off the campaign (never re-drawn)
    creative        -> chosen from that advertiser's creatives that existed on the day
    placement       -> chosen from inventory compatible with that creative
    publisher       -> read off the placement (never re-drawn)
    audience        -> chosen from segments in the publisher's market

so every foreign key combination on the row is one that could really have
happened. The module also plans how many impressions each campaign and each day
receives, which is what produces the heavy-tailed traffic distribution.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta

from data_generator.context import GeneratorContext
from data_generator.distributions import (
    WeightedSampler,
    allocate_counts,
    allocate_counts_capped,
    lognormal_around,
)
from data_generator.models import Impression
from data_generator.numeric import dec4, dec6
from data_generator.relationships import CampaignNode, LineItemNode, PlacementNode
from data_generator.rng import RandomStream
from data_generator.timeline import apply_ingestion_lag, datetime_within_day, start_of_day

#: Share of buying that happens in the advertiser's own region.
_HOME_REGION_SHARE = 0.72
#: Share of impressions whose geo matches the publisher's country.
_PUBLISHER_GEO_SHARE = 0.88
#: Share of impressions targeted at a segment in the publisher's market.
_LOCAL_AUDIENCE_SHARE = 0.75

_STATUSES_THAT_NEVER_SERVED = frozenset({"DRAFT"})


@dataclass(slots=True)
class GeneratedImpression:
    """An impression row plus the context the downstream funnel needs.

    Carrying the parent nodes here means clicks, conversions and spend can be
    derived without a single lookup by identifier - and without any opportunity
    to attach the wrong parent.
    """

    row: Impression
    line_item: LineItemNode
    placement: PlacementNode
    click_weight: float
    conversion_weight: float


@dataclass(slots=True)
class CampaignPlan:
    """How much traffic a campaign receives, and when."""

    campaign: CampaignNode
    days: list[date]
    impressions_per_day: list[int]
    clicks_per_day: list[int]
    conversions: int

    @property
    def total_impressions(self) -> int:
        return sum(self.impressions_per_day)

    @property
    def total_clicks(self) -> int:
        return sum(self.clicks_per_day)


def plan_delivery(ctx: GeneratorContext) -> list[CampaignPlan]:
    """Distribute the configured event volumes across campaigns and days.

    Campaign totals are allocated with the largest-remainder method, so the
    configured impression/click/conversion counts are met exactly while the
    per-campaign CTR and CVR still vary by an order of magnitude.
    """
    events = ctx.config.scale.events
    campaigns = ctx.ecosystem.deliverable_campaigns()
    if not campaigns or events.impressions == 0:
        return []

    # Stable ordering: campaign_id keeps the plan independent of dict insertion order.
    campaigns.sort(key=lambda node: node.campaign_id.int)

    delivery_days = [_delivery_days(node) for node in campaigns]
    rng = ctx.stream("delivery", "plan")

    # Traffic weight: appetite x how long the campaign was live in the window.
    campaign_weights = [
        node.volume_weight * len(days) for node, days in zip(campaigns, delivery_days, strict=True)
    ]
    impressions_per_campaign = allocate_counts(events.impressions, campaign_weights)

    click_weights = [
        impressions * node.ctr_multiplier
        for node, impressions in zip(campaigns, impressions_per_campaign, strict=True)
    ]
    clicks_per_campaign = allocate_counts_capped(
        events.clicks, click_weights, impressions_per_campaign
    )

    conversion_weights = [
        clicks * node.cvr_multiplier
        for node, clicks in zip(campaigns, clicks_per_campaign, strict=True)
    ]
    conversions_per_campaign = allocate_counts_capped(
        events.conversions, conversion_weights, clicks_per_campaign
    )

    plans: list[CampaignPlan] = []
    for index, node in enumerate(campaigns):
        days = delivery_days[index]
        impressions_per_day = _spread_over_days(
            ctx, rng, node, days, impressions_per_campaign[index]
        )
        clicks_per_day = allocate_counts_capped(
            clicks_per_campaign[index], impressions_per_day, impressions_per_day
        )
        plans.append(
            CampaignPlan(
                campaign=node,
                days=days,
                impressions_per_day=impressions_per_day,
                clicks_per_day=clicks_per_day,
                conversions=conversions_per_campaign[index],
            )
        )
    return plans


def _delivery_days(node: CampaignNode) -> list[date]:
    assert node.delivery_start is not None and node.delivery_end is not None
    span = (node.delivery_end - node.delivery_start).days
    return [node.delivery_start + timedelta(days=offset) for offset in range(span + 1)]


def _spread_over_days(
    ctx: GeneratorContext,
    rng: RandomStream,
    node: CampaignNode,
    days: list[date],
    total: int,
) -> list[int]:
    """Weekday seasonality plus per-day noise: real delivery is never flat."""
    dow_weights = ctx.reference.day_of_week_weights
    day_rng = rng.substream(node.campaign_id)
    weights = [dow_weights[day.weekday()] * lognormal_around(day_rng, 1.0, 0.35) for day in days]
    return allocate_counts(total, weights)


def generate_impressions_for_day(
    ctx: GeneratorContext,
    plan: CampaignPlan,
    day_index: int,
) -> list[GeneratedImpression]:
    """Build one campaign-day worth of impressions."""
    count = plan.impressions_per_day[day_index]
    if count == 0:
        return []

    day = plan.days[day_index]
    campaign = plan.campaign
    ecosystem = ctx.ecosystem
    reference = ctx.reference
    rng = ctx.stream("impressions", campaign.campaign_id, day.toordinal())

    line_item_sampler = _line_items_active_on(ctx, campaign, day)
    creative_sampler = ecosystem.creative_sampler_as_of(campaign.advertiser_id, start_of_day(day))
    home_region = campaign.advertiser.country.region
    currency = campaign.advertiser.row.billing_currency
    lag_seconds = ctx.config.timeline.ingestion_lag_seconds_max
    cutoff = ctx.simulation_end

    # Draw the independent dimensions in bulk: same distributions, far fewer
    # Python-level calls than sampling one impression at a time.
    line_items = line_item_sampler.pick_many(rng, count)
    creatives = creative_sampler.pick_many(rng, count)
    hours = reference.hour_of_day_sampler.pick_many(rng, count)
    placements = _draw_placements(ctx, rng, creatives, home_region, count)

    impressions: list[GeneratedImpression] = []
    for index in range(count):
        line_item = line_items[index]
        creative = creatives[index]
        placement = placements[index]
        publisher = placement.publisher
        profile = placement.profile

        audience_country = publisher.country.name if rng.random() < _LOCAL_AUDIENCE_SHARE else None
        audience = ecosystem.audience_sampler(audience_country).pick(rng)

        country = _impression_country(ctx, rng, placement)
        city = rng.choice(country.cities)

        device_type = placement.row.device_type
        operating_system = reference.operating_system_samplers[device_type].pick(rng)
        browser = (
            "In-App"
            if placement.row.placement_type in reference.in_app_placement_types
            else reference.browser_samplers[operating_system].pick(rng)
        )

        bid_price, clearing_price = _auction(ctx, rng, line_item, placement)
        viewability = rng.betavariate(profile.view_alpha, profile.view_beta)

        impression_timestamp = datetime_within_day(rng, day, hours[index])
        created_at = min(apply_ingestion_lag(rng, impression_timestamp, lag_seconds), cutoff)

        row = Impression(
            impression_id=rng.uuid4(),
            campaign_id=campaign.campaign_id,
            line_item_id=line_item.line_item_id,
            advertiser_id=campaign.advertiser_id,
            creative_id=creative.creative_id,
            publisher_id=publisher.publisher_id,
            placement_id=placement.placement_id,
            audience_id=audience.audience_id,
            impression_timestamp=impression_timestamp,
            device_type=device_type,
            operating_system=operating_system,
            browser=browser,
            country=country.name,
            city=city,
            bid_price=dec6(bid_price),
            clearing_price=dec6(clearing_price),
            currency=currency,
            viewability_score=dec4(viewability),
            created_at=created_at,
        )

        device_profile = reference.device_profile[device_type]
        impressions.append(
            GeneratedImpression(
                row=row,
                line_item=line_item,
                placement=placement,
                # Which impressions get clicked: good creative, good slot, good
                # device, engaged audience, and actually being seen.
                click_weight=(
                    creative.ctr_multiplier
                    * profile.ctr_index
                    * device_profile.ctr_index
                    * audience.profile.ctr_index
                    * (0.4 + 1.2 * viewability)
                ),
                conversion_weight=device_profile.cvr_index * audience.profile.cvr_index,
            )
        )

    return impressions


def _line_items_active_on(
    ctx: GeneratorContext, campaign: CampaignNode, day: date
) -> WeightedSampler[LineItemNode]:
    """Line items of this campaign whose own flight covers the day.

    The campaign's first line item always spans the full flight, so this can
    never return an empty set for a campaign that is allowed to deliver.
    """
    eligible = [
        node
        for node in ctx.ecosystem.line_item_nodes(campaign.campaign_id)
        if node.row.start_date <= day <= node.row.end_date
        and node.row.line_item_status not in _STATUSES_THAT_NEVER_SERVED
    ]
    if not eligible:
        eligible = ctx.ecosystem.line_item_nodes(campaign.campaign_id)
    return WeightedSampler(eligible, [node.weight for node in eligible])


def _draw_placements(
    ctx: GeneratorContext,
    rng: RandomStream,
    creatives: list,
    home_region: str,
    count: int,
) -> list[PlacementNode]:
    """Pick compatible inventory for each creative, biased towards the home region.

    Placements are drawn per (creative type, region) pool in bulk rather than one
    at a time: the pools are reused across the whole run and bulk sampling keeps
    the inner loop cheap.
    """
    keys: list[tuple[str, str | None]] = []
    for index in range(count):
        region = home_region if rng.random() < _HOME_REGION_SHARE else None
        keys.append((creatives[index].creative_type, region))

    drawn: dict[tuple[str, str | None], list[PlacementNode]] = {}
    for key, needed in Counter(keys).items():
        creative_type, region = key
        drawn[key] = ctx.ecosystem.placement_pool(creative_type, region).pick_many(rng, needed)

    cursors: Counter[tuple[str, str | None]] = Counter()
    placements: list[PlacementNode] = []
    for key in keys:
        position = cursors[key]
        cursors[key] += 1
        placements.append(drawn[key][position])
    return placements


def _impression_country(ctx: GeneratorContext, rng: RandomStream, placement: PlacementNode):
    """Most traffic on a property is domestic; the rest comes from the same region."""
    publisher_country = placement.publisher.country
    if rng.random() < _PUBLISHER_GEO_SHARE:
        return publisher_country
    same_region = [
        country
        for country in ctx.reference.countries
        if country.region == publisher_country.region and country.code != publisher_country.code
    ]
    if not same_region:
        return publisher_country
    return rng.choice(same_region)


def _auction(
    ctx: GeneratorContext,
    rng: RandomStream,
    line_item: LineItemNode,
    placement: PlacementNode,
) -> tuple[float, float]:
    """Second-price auction: floor <= clearing_price <= bid_price.

    Both prices are CPMs. The cost of the single impression is
    ``clearing_price / 1000``, which is what the CPM spend roll-up sums.
    """
    pricing = ctx.config.pricing
    floor = float(placement.row.floor_price)
    bid = (
        float(line_item.row.target_cpm)
        * placement.profile.cpm_index
        * lognormal_around(rng, 1.0, 0.22)
    )
    if bid < floor:
        # The buyer has to meet the floor to win the slot at all.
        bid = floor * rng.uniform(1.01, 1.25)
    clearing = floor + (bid - floor) * rng.betavariate(
        pricing.clearing_price_beta_a, pricing.clearing_price_beta_b
    )
    return bid, clearing
