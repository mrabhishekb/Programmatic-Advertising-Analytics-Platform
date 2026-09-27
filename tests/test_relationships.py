"""The ecosystem registry refuses to create dangling references."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from data_generator.models import Campaign, LineItem, Placement
from data_generator.reference import ReferenceData
from data_generator.relationships import (
    CampaignNode,
    Ecosystem,
    LineItemNode,
    PlacementNode,
    RelationshipError,
)

NOW = datetime(2026, 1, 1, 12, 0, 0)


@pytest.fixture
def ecosystem() -> Ecosystem:
    return Ecosystem(ReferenceData.load())


def _campaign_node(advertiser_id) -> CampaignNode:
    row = Campaign(
        campaign_id=uuid4(),
        advertiser_id=advertiser_id,
        campaign_name="Orphan",
        campaign_objective="TRAFFIC",
        campaign_status="ACTIVE",
        campaign_budget=Decimal("1000.00"),
        daily_budget=Decimal("100.00"),
        start_date=NOW.date(),
        end_date=NOW.date(),
        bid_strategy="CPC",
        created_at=NOW,
        updated_at=NOW,
    )
    return CampaignNode(
        row=row,
        advertiser=None,  # type: ignore[arg-type]
        ctr_multiplier=1.0,
        cvr_multiplier=1.0,
        volume_weight=1.0,
        target_roas=2.0,
        delivery_start=None,
        delivery_end=None,
    )


class TestGuardrails:
    def test_campaign_for_an_unknown_advertiser_is_rejected(self, ecosystem):
        with pytest.raises(RelationshipError, match="unknown advertiser"):
            ecosystem.add_campaign(_campaign_node(uuid4()))

    def test_line_item_for_an_unknown_campaign_is_rejected(self, ecosystem):
        row = LineItem(
            line_item_id=uuid4(),
            campaign_id=uuid4(),
            line_item_name="Orphan",
            line_item_status="ACTIVE",
            bid_amount=Decimal("1.0000"),
            bid_currency="USD",
            optimization_goal="CLICKS",
            target_cpm=Decimal("5.0000"),
            frequency_cap=3,
            start_date=NOW.date(),
            end_date=NOW.date(),
            created_at=NOW,
            updated_at=NOW,
        )
        with pytest.raises(RelationshipError, match="unknown campaign"):
            ecosystem.add_line_item(LineItemNode(row=row, campaign_id=row.campaign_id, weight=1.0))

    def test_placement_for_an_unknown_publisher_is_rejected(self, ecosystem):
        row = Placement(
            placement_id=uuid4(),
            publisher_id=uuid4(),
            placement_name="Orphan",
            placement_type="HEADER",
            ad_format="DISPLAY_LEADERBOARD",
            floor_price=Decimal("1.0000"),
            currency="USD",
            device_type="DESKTOP",
            created_at=NOW,
            updated_at=NOW,
        )
        with pytest.raises(RelationshipError, match="unknown publisher"):
            ecosystem.add_placement(
                PlacementNode(
                    row=row,
                    publisher=None,  # type: ignore[arg-type]
                    profile=ecosystem.reference.placement_type_profile["HEADER"],
                    traffic_weight=1.0,
                )
            )

    def test_placement_pool_for_an_empty_inventory_fails_loudly(self, ecosystem):
        with pytest.raises(RelationshipError, match="no placement is compatible"):
            ecosystem.placement_pool("BANNER")

    def test_audience_sampler_without_audiences_fails_loudly(self, ecosystem):
        with pytest.raises(RelationshipError, match="no audiences"):
            ecosystem.audience_sampler()


class TestRegistryViews:
    def test_deliverable_campaigns_excludes_campaigns_without_a_window(self, tables, result):
        # Generated ecosystems always contain campaigns that cannot deliver
        # (drafts, flights that ended before the event window).
        assert result.events.delivering_campaigns < len(tables["campaigns"])
        assert result.events.delivering_campaigns > 0
