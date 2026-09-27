"""The in-memory ecosystem graph.

This module is the reason the generated data is relationally coherent. Every
entity is registered here together with its parent, and the ``add_*`` methods
refuse to register a child whose parent is unknown. Downstream generators never
invent an identifier: they ask the ecosystem for a parent and read the foreign
key off the object they were handed.

Nodes also carry *generation metadata* that never reaches the database - traffic
weights, CTR/CVR multipliers, delivery windows. Keeping that here rather than on
the row models is what lets the event generators produce correlated behaviour
(some campaigns big, some tiny; some creatives good, some bad) while the rows
themselves stay a clean mirror of the source schema.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date, datetime
from uuid import UUID

from data_generator.distributions import WeightedSampler
from data_generator.models import (
    Advertiser,
    Audience,
    Campaign,
    Creative,
    LineItem,
    Placement,
    Publisher,
)
from data_generator.reference import (
    AdvertiserTier,
    AudienceTypeProfile,
    Country,
    Industry,
    PlacementTypeProfile,
    ReferenceData,
)


class RelationshipError(RuntimeError):
    """Raised when a child entity is registered against a non-existent parent."""


@dataclass(slots=True)
class AdvertiserNode:
    row: Advertiser
    tier: AdvertiserTier
    industry: Industry
    country: Country
    volume_weight: float
    budget_scale: float
    campaign_ids: list[UUID] = field(default_factory=list)
    creative_ids: list[UUID] = field(default_factory=list)
    servable_creative_ids: list[UUID] = field(default_factory=list)

    @property
    def advertiser_id(self) -> UUID:
        return self.row.advertiser_id


@dataclass(slots=True)
class CampaignNode:
    row: Campaign
    advertiser: AdvertiserNode
    ctr_multiplier: float
    cvr_multiplier: float
    volume_weight: float
    target_roas: float
    #: Trailing window in which this campaign may deliver, clipped to the
    #: campaign flight dates and the configured event window. None => no delivery.
    delivery_start: date | None
    delivery_end: date | None
    line_item_ids: list[UUID] = field(default_factory=list)

    @property
    def campaign_id(self) -> UUID:
        return self.row.campaign_id

    @property
    def advertiser_id(self) -> UUID:
        return self.row.advertiser_id

    @property
    def can_deliver(self) -> bool:
        return (
            self.delivery_start is not None
            and self.delivery_end is not None
            and bool(self.line_item_ids)
            and bool(self.advertiser.servable_creative_ids)
        )


@dataclass(slots=True)
class LineItemNode:
    row: LineItem
    campaign_id: UUID
    weight: float

    @property
    def line_item_id(self) -> UUID:
        return self.row.line_item_id


@dataclass(slots=True)
class CreativeNode:
    row: Creative
    advertiser_id: UUID
    ctr_multiplier: float
    serving_weight: float

    @property
    def creative_id(self) -> UUID:
        return self.row.creative_id

    @property
    def creative_type(self) -> str:
        return self.row.creative_type


@dataclass(slots=True)
class PublisherNode:
    row: Publisher
    country: Country
    traffic_weight: float
    placement_ids: list[UUID] = field(default_factory=list)

    @property
    def publisher_id(self) -> UUID:
        return self.row.publisher_id


@dataclass(slots=True)
class PlacementNode:
    row: Placement
    publisher: PublisherNode
    profile: PlacementTypeProfile
    traffic_weight: float

    @property
    def placement_id(self) -> UUID:
        return self.row.placement_id


@dataclass(slots=True)
class AudienceNode:
    row: Audience
    profile: AudienceTypeProfile
    weight: float

    @property
    def audience_id(self) -> UUID:
        return self.row.audience_id


class Ecosystem:
    """Registry of every generated entity and the edges between them."""

    def __init__(self, reference: ReferenceData) -> None:
        self.reference = reference
        self.advertisers: dict[UUID, AdvertiserNode] = {}
        self.campaigns: dict[UUID, CampaignNode] = {}
        self.line_items: dict[UUID, LineItemNode] = {}
        self.creatives: dict[UUID, CreativeNode] = {}
        self.publishers: dict[UUID, PublisherNode] = {}
        self.placements: dict[UUID, PlacementNode] = {}
        self.audiences: dict[UUID, AudienceNode] = {}
        self._placement_pools: dict[tuple[str, str | None], WeightedSampler[PlacementNode]] = {}
        self._audience_sampler: WeightedSampler[AudienceNode] | None = None
        self._audience_samplers_by_country: dict[str, WeightedSampler[AudienceNode]] = {}
        self._line_item_samplers: dict[UUID, WeightedSampler[LineItemNode]] = {}
        self._creative_samplers: dict[tuple[UUID, int], WeightedSampler[CreativeNode]] = {}
        self._sorted_creatives: dict[UUID, tuple[list[CreativeNode], list[datetime]]] = {}

    # -- registration -----------------------------------------------------

    def add_advertiser(self, node: AdvertiserNode) -> AdvertiserNode:
        self.advertisers[node.advertiser_id] = node
        return node

    def add_publisher(self, node: PublisherNode) -> PublisherNode:
        self.publishers[node.publisher_id] = node
        return node

    def add_placement(self, node: PlacementNode) -> PlacementNode:
        publisher = self.publishers.get(node.row.publisher_id)
        if publisher is None:
            raise RelationshipError(
                f"placement {node.placement_id} references unknown publisher {node.row.publisher_id}"
            )
        self.placements[node.placement_id] = node
        publisher.placement_ids.append(node.placement_id)
        return node

    def add_campaign(self, node: CampaignNode) -> CampaignNode:
        advertiser = self.advertisers.get(node.advertiser_id)
        if advertiser is None:
            raise RelationshipError(
                f"campaign {node.campaign_id} references unknown advertiser {node.advertiser_id}"
            )
        self.campaigns[node.campaign_id] = node
        advertiser.campaign_ids.append(node.campaign_id)
        return node

    def add_line_item(self, node: LineItemNode) -> LineItemNode:
        campaign = self.campaigns.get(node.campaign_id)
        if campaign is None:
            raise RelationshipError(
                f"line item {node.line_item_id} references unknown campaign {node.campaign_id}"
            )
        self.line_items[node.line_item_id] = node
        campaign.line_item_ids.append(node.line_item_id)
        return node

    def add_creative(self, node: CreativeNode) -> CreativeNode:
        advertiser = self.advertisers.get(node.advertiser_id)
        if advertiser is None:
            raise RelationshipError(
                f"creative {node.creative_id} references unknown advertiser {node.advertiser_id}"
            )
        self.creatives[node.creative_id] = node
        advertiser.creative_ids.append(node.creative_id)
        if node.row.creative_status in self.reference.creative_servable_statuses:
            advertiser.servable_creative_ids.append(node.creative_id)
        return node

    def add_audience(self, node: AudienceNode) -> AudienceNode:
        self.audiences[node.audience_id] = node
        return node

    # -- lookups ----------------------------------------------------------

    def line_item_nodes(self, campaign_id: UUID) -> list[LineItemNode]:
        campaign = self.campaigns[campaign_id]
        return [self.line_items[line_item_id] for line_item_id in campaign.line_item_ids]

    def servable_creative_nodes(self, advertiser_id: UUID) -> list[CreativeNode]:
        advertiser = self.advertisers[advertiser_id]
        return [self.creatives[creative_id] for creative_id in advertiser.servable_creative_ids]

    def line_item_sampler(self, campaign_id: UUID) -> WeightedSampler[LineItemNode]:
        sampler = self._line_item_samplers.get(campaign_id)
        if sampler is None:
            nodes = self.line_item_nodes(campaign_id)
            sampler = WeightedSampler(nodes, [node.weight for node in nodes])
            self._line_item_samplers[campaign_id] = sampler
        return sampler

    def creative_sampler_as_of(
        self, advertiser_id: UUID, as_of: datetime
    ) -> WeightedSampler[CreativeNode]:
        """Servable creatives that already existed at ``as_of``.

        A creative uploaded last week cannot have served an impression two months
        ago. The advertiser's creatives are kept sorted by ``created_at`` so the
        eligible set is a prefix, found with a binary search and cached per
        prefix length rather than per timestamp.
        """
        nodes, created_ats = self._sorted_servable_creatives(advertiser_id)
        if not nodes:
            raise RelationshipError(
                f"advertiser {advertiser_id} has no servable creative; "
                "campaigns for this advertiser cannot deliver"
            )
        cutoff = bisect_right(created_ats, as_of)
        if cutoff == 0:
            # Guarded by the creative generator: every advertiser's first creative
            # is created within a day of the account, before any delivery starts.
            raise RelationshipError(
                f"advertiser {advertiser_id} had no creative at {as_of.isoformat()}"
            )

        key = (advertiser_id, cutoff)
        sampler = self._creative_samplers.get(key)
        if sampler is None:
            eligible = nodes[:cutoff]
            sampler = WeightedSampler(eligible, [node.serving_weight for node in eligible])
            self._creative_samplers[key] = sampler
        return sampler

    def _sorted_servable_creatives(
        self, advertiser_id: UUID
    ) -> tuple[list[CreativeNode], list[datetime]]:
        cached = self._sorted_creatives.get(advertiser_id)
        if cached is None:
            nodes = sorted(
                self.servable_creative_nodes(advertiser_id), key=lambda node: node.row.created_at
            )
            cached = (nodes, [node.row.created_at for node in nodes])
            self._sorted_creatives[advertiser_id] = cached
        return cached

    def audience_sampler(self, country: str | None = None) -> WeightedSampler[AudienceNode]:
        """Audience segments, optionally restricted to one country.

        Buyers mostly target segments in the market they are buying inventory in,
        so the impression generator asks for the publisher's country first and
        falls back to the global pool when that country has no segments.
        """
        if country is not None:
            sampler = self._audience_samplers_by_country.get(country)
            if sampler is None:
                nodes = [node for node in self.audiences.values() if node.row.country == country]
                sampler = (
                    WeightedSampler(nodes, [node.weight for node in nodes])
                    if nodes
                    else self.audience_sampler(None)
                )
                self._audience_samplers_by_country[country] = sampler
            return sampler

        if self._audience_sampler is None:
            nodes = list(self.audiences.values())
            if not nodes:
                raise RelationshipError("no audiences have been generated")
            self._audience_sampler = WeightedSampler(nodes, [node.weight for node in nodes])
        return self._audience_sampler

    # -- inventory matching ----------------------------------------------

    def compatible_placements(self, creative_type: str) -> list[PlacementNode]:
        """Placements a creative of this type can physically run on."""
        allowed_types = self.reference.placement_types_by_creative_type.get(
            creative_type, frozenset()
        )
        allowed_formats = self.reference.ad_formats_by_creative_type.get(creative_type, frozenset())
        return [
            node
            for node in self.placements.values()
            if node.row.placement_type in allowed_types and node.row.ad_format in allowed_formats
        ]

    def creative_types_with_inventory(self) -> set[str]:
        """Creative types that have at least one compatible placement."""
        available: set[str] = set()
        for creative_type in self.reference.placement_types_by_creative_type:
            if self.compatible_placements(creative_type):
                available.add(creative_type)
        return available

    def placement_pool(
        self, creative_type: str, region: str | None = None
    ) -> WeightedSampler[PlacementNode]:
        """Weighted placement sampler for a creative type, optionally biased to a region.

        Advertisers buy most of their inventory in their own market, so the event
        generator asks for a regional pool first and falls back to the global pool
        when a region has no compatible inventory.
        """
        key = (creative_type, region)
        sampler = self._placement_pools.get(key)
        if sampler is not None:
            return sampler

        candidates = self.compatible_placements(creative_type)
        if region is not None:
            regional = [node for node in candidates if node.publisher.country.region == region]
            if regional:
                candidates = regional
            else:
                return self.placement_pool(creative_type, None)
        if not candidates:
            raise RelationshipError(
                f"no placement is compatible with creative type {creative_type!r}; "
                "increase the publisher/placement counts or relax the compatibility matrix"
            )
        sampler = WeightedSampler(candidates, [node.traffic_weight for node in candidates])
        self._placement_pools[key] = sampler
        return sampler

    # -- summary ----------------------------------------------------------

    def entity_counts(self) -> dict[str, int]:
        return {
            "advertisers": len(self.advertisers),
            "publishers": len(self.publishers),
            "placements": len(self.placements),
            "campaigns": len(self.campaigns),
            "line_items": len(self.line_items),
            "creatives": len(self.creatives),
            "audiences": len(self.audiences),
        }

    def deliverable_campaigns(self) -> list[CampaignNode]:
        """Campaigns that can legitimately produce impressions in the event window."""
        return [node for node in self.campaigns.values() if node.can_deliver]
