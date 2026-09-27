"""End-to-end integrity of a generated ecosystem.

These tests verify the generated rows independently of the generator: they walk
the emitted data and check that every relationship, timestamp and funnel total
holds. This is the suite that would catch "the IDs were drawn independently".
"""

from __future__ import annotations

from collections import Counter

import pytest


def index_by(rows, key):
    return {row[key]: row for row in rows}


@pytest.fixture(scope="module")
def indexes(tables):
    return {
        "advertisers": index_by(tables["advertisers"], "advertiser_id"),
        "campaigns": index_by(tables["campaigns"], "campaign_id"),
        "line_items": index_by(tables["line_items"], "line_item_id"),
        "creatives": index_by(tables["creatives"], "creative_id"),
        "publishers": index_by(tables["publishers"], "publisher_id"),
        "placements": index_by(tables["placements"], "placement_id"),
        "audiences": index_by(tables["audiences"], "audience_id"),
        "impressions": index_by(tables["impressions"], "impression_id"),
        "clicks": index_by(tables["clicks"], "click_id"),
    }


class TestRowCounts:
    def test_entity_counts_match_the_configuration(self, tables, config):
        expected = config.scale.entities.as_dict()
        for table, count in expected.items():
            assert len(tables[table]) == count, table

    def test_event_counts_match_the_configuration_exactly(self, tables, config):
        """Clicks and conversions are allocated, not sampled, so totals are exact."""
        assert len(tables["impressions"]) == config.scale.events.impressions
        assert len(tables["clicks"]) == config.scale.events.clicks
        assert len(tables["conversions"]) == config.scale.events.conversions

    def test_spend_transactions_were_produced(self, tables):
        assert len(tables["spend_transactions"]) > 0


class TestReferentialIntegrity:
    """Every foreign key resolves - the core promise of the generator."""

    def test_campaigns_reference_real_advertisers(self, tables, indexes):
        for row in tables["campaigns"]:
            assert row["advertiser_id"] in indexes["advertisers"]

    def test_line_items_reference_real_campaigns(self, tables, indexes):
        for row in tables["line_items"]:
            assert row["campaign_id"] in indexes["campaigns"]

    def test_creatives_reference_real_advertisers(self, tables, indexes):
        for row in tables["creatives"]:
            assert row["advertiser_id"] in indexes["advertisers"]

    def test_placements_reference_real_publishers(self, tables, indexes):
        for row in tables["placements"]:
            assert row["publisher_id"] in indexes["publishers"]

    def test_all_seven_impression_foreign_keys_resolve(self, tables, indexes):
        for row in tables["impressions"]:
            assert row["campaign_id"] in indexes["campaigns"]
            assert row["line_item_id"] in indexes["line_items"]
            assert row["advertiser_id"] in indexes["advertisers"]
            assert row["creative_id"] in indexes["creatives"]
            assert row["publisher_id"] in indexes["publishers"]
            assert row["placement_id"] in indexes["placements"]
            assert row["audience_id"] in indexes["audiences"]

    def test_clicks_reference_real_impressions(self, tables, indexes):
        for row in tables["clicks"]:
            assert row["impression_id"] in indexes["impressions"]

    def test_conversions_reference_real_clicks_and_impressions(self, tables, indexes):
        for row in tables["conversions"]:
            assert row["click_id"] in indexes["clicks"]
            assert row["impression_id"] in indexes["impressions"]

    def test_spend_references_resolve(self, tables, indexes):
        for row in tables["spend_transactions"]:
            assert row["campaign_id"] in indexes["campaigns"]
            assert row["line_item_id"] in indexes["line_items"]
            assert row["advertiser_id"] in indexes["advertisers"]
            assert row["publisher_id"] in indexes["publishers"]
            if row["impression_id"] is not None:
                assert row["impression_id"] in indexes["impressions"]


class TestCombinationsAreCoherent:
    """Resolving is not enough: the combination has to be one that could occur."""

    def test_impression_line_item_belongs_to_its_campaign(self, tables, indexes):
        for row in tables["impressions"]:
            assert indexes["line_items"][row["line_item_id"]]["campaign_id"] == row["campaign_id"]

    def test_impression_advertiser_owns_its_campaign(self, tables, indexes):
        for row in tables["impressions"]:
            assert indexes["campaigns"][row["campaign_id"]]["advertiser_id"] == row["advertiser_id"]

    def test_impression_creative_belongs_to_its_advertiser(self, tables, indexes):
        for row in tables["impressions"]:
            assert indexes["creatives"][row["creative_id"]]["advertiser_id"] == row["advertiser_id"]

    def test_impression_placement_belongs_to_its_publisher(self, tables, indexes):
        for row in tables["impressions"]:
            assert indexes["placements"][row["placement_id"]]["publisher_id"] == row["publisher_id"]

    def test_impression_creative_can_run_on_its_placement(self, tables, indexes, config):
        from data_generator.reference import ReferenceData

        reference = ReferenceData.load(config.reference_dir)
        for row in tables["impressions"]:
            creative_type = indexes["creatives"][row["creative_id"]]["creative_type"]
            placement = indexes["placements"][row["placement_id"]]
            assert (
                placement["placement_type"]
                in reference.placement_types_by_creative_type[creative_type]
            )
            assert placement["ad_format"] in reference.ad_formats_by_creative_type[creative_type]

    def test_clicks_inherit_every_attribute_from_their_impression(self, tables, indexes):
        for row in tables["clicks"]:
            impression = indexes["impressions"][row["impression_id"]]
            for column in ("campaign_id", "line_item_id", "advertiser_id", "creative_id"):
                assert row[column] == impression[column]
            assert row["device_type"] == impression["device_type"]
            assert row["country"] == impression["country"]

    def test_conversions_inherit_from_their_click(self, tables, indexes):
        for row in tables["conversions"]:
            click = indexes["clicks"][row["click_id"]]
            assert row["impression_id"] == click["impression_id"]
            assert row["campaign_id"] == click["campaign_id"]
            assert row["advertiser_id"] == click["advertiser_id"]

    def test_spend_line_item_belongs_to_the_billed_campaign(self, tables, indexes):
        for row in tables["spend_transactions"]:
            assert indexes["line_items"][row["line_item_id"]]["campaign_id"] == row["campaign_id"]

    def test_placement_device_matches_publisher_medium(self, tables, indexes):
        for row in tables["placements"]:
            publisher_type = indexes["publishers"][row["publisher_id"]]["publisher_type"]
            if publisher_type == "CTV":
                assert row["device_type"] == "CTV"
            if publisher_type == "MOBILE_APP":
                assert row["device_type"] in {"MOBILE", "TABLET"}


class TestTemporalConsistency:
    def test_campaign_never_predates_its_advertiser(self, tables, indexes):
        for row in tables["campaigns"]:
            assert row["created_at"] >= indexes["advertisers"][row["advertiser_id"]]["created_at"]

    def test_campaign_is_set_up_before_it_flies(self, tables):
        for row in tables["campaigns"]:
            assert row["created_at"].date() <= row["start_date"]
            assert row["start_date"] < row["end_date"]

    def test_line_item_flight_sits_inside_its_campaign(self, tables, indexes):
        for row in tables["line_items"]:
            campaign = indexes["campaigns"][row["campaign_id"]]
            assert (
                campaign["start_date"]
                <= row["start_date"]
                <= row["end_date"]
                <= campaign["end_date"]
            )
            assert row["created_at"] >= campaign["created_at"]

    def test_impressions_occur_inside_the_campaign_flight(self, tables, indexes):
        for row in tables["impressions"]:
            campaign = indexes["campaigns"][row["campaign_id"]]
            assert (
                campaign["start_date"] <= row["impression_timestamp"].date() <= campaign["end_date"]
            )

    def test_impressions_never_predate_the_entities_they_used(self, tables, indexes):
        for row in tables["impressions"]:
            moment = row["impression_timestamp"]
            assert moment >= indexes["line_items"][row["line_item_id"]]["created_at"]
            assert moment >= indexes["creatives"][row["creative_id"]]["created_at"]
            assert moment >= indexes["placements"][row["placement_id"]]["created_at"]

    def test_clicks_never_precede_their_impression(self, tables, indexes):
        for row in tables["clicks"]:
            assert (
                row["click_timestamp"]
                >= indexes["impressions"][row["impression_id"]]["impression_timestamp"]
            )

    def test_conversions_never_precede_their_click(self, tables, indexes):
        for row in tables["conversions"]:
            assert (
                row["conversion_timestamp"] >= indexes["clicks"][row["click_id"]]["click_timestamp"]
            )

    def test_conversions_fall_inside_their_attribution_window(self, tables, indexes):
        for row in tables["conversions"]:
            click = indexes["clicks"][row["click_id"]]
            elapsed = (
                row["conversion_timestamp"] - click["click_timestamp"]
            ).total_seconds() / 3600
            assert elapsed <= row["attribution_window_hours"]

    def test_nothing_is_recorded_before_it_happened(self, tables):
        for row in tables["impressions"]:
            assert row["created_at"] >= row["impression_timestamp"]
        for row in tables["clicks"]:
            assert row["created_at"] >= row["click_timestamp"]
        for row in tables["conversions"]:
            assert row["created_at"] >= row["conversion_timestamp"]

    def test_no_event_happens_after_the_simulation_clock(self, tables, config):
        cutoff = config.timeline.simulation_end
        for row in tables["impressions"]:
            assert row["impression_timestamp"] <= cutoff
        for row in tables["conversions"]:
            assert row["conversion_timestamp"] <= cutoff


class TestUniqueness:
    @pytest.mark.parametrize(
        ("table", "key"),
        [
            ("advertisers", "advertiser_id"),
            ("campaigns", "campaign_id"),
            ("line_items", "line_item_id"),
            ("creatives", "creative_id"),
            ("publishers", "publisher_id"),
            ("placements", "placement_id"),
            ("audiences", "audience_id"),
            ("impressions", "impression_id"),
            ("clicks", "click_id"),
            ("conversions", "conversion_id"),
            ("spend_transactions", "spend_transaction_id"),
        ],
    )
    def test_primary_keys_are_unique(self, tables, table, key):
        identifiers = [row[key] for row in tables[table]]
        assert len(set(identifiers)) == len(identifiers)

    def test_publisher_domains_are_unique(self, tables):
        domains = [row["domain"] for row in tables["publishers"]]
        assert len(set(domains)) == len(domains)

    def test_at_most_one_click_per_impression(self, tables):
        impression_ids = [row["impression_id"] for row in tables["clicks"]]
        assert len(set(impression_ids)) == len(impression_ids)

    def test_at_most_one_conversion_per_click(self, tables):
        click_ids = [row["click_id"] for row in tables["conversions"]]
        assert len(set(click_ids)) == len(click_ids)


class TestBusinessRules:
    def test_no_negative_money_anywhere(self, tables):
        assert all(row["spend_amount"] >= 0 for row in tables["spend_transactions"])
        assert all(row["conversion_value"] >= 0 for row in tables["conversions"])
        assert all(row["floor_price"] >= 0 for row in tables["placements"])
        assert all(row["bid_price"] >= 0 for row in tables["impressions"])

    def test_viewability_is_a_fraction(self, tables):
        assert all(0 <= row["viewability_score"] <= 1 for row in tables["impressions"])

    def test_auction_clears_between_floor_and_bid(self, tables, indexes):
        for row in tables["impressions"]:
            floor = indexes["placements"][row["placement_id"]]["floor_price"]
            assert floor <= row["clearing_price"] <= row["bid_price"]

    def test_audience_age_ranges_are_valid(self, tables):
        for row in tables["audiences"]:
            assert 13 <= row["min_age"] < row["max_age"] <= 99

    def test_daily_budget_fits_within_the_total(self, tables):
        for row in tables["campaigns"]:
            assert 0 < row["daily_budget"] <= row["campaign_budget"]

    def test_draft_campaigns_never_delivered(self, tables, indexes):
        for row in tables["impressions"]:
            assert indexes["campaigns"][row["campaign_id"]]["campaign_status"] != "DRAFT"

    def test_campaign_status_agrees_with_its_dates(self, tables, config):
        today = config.timeline.simulation_end_date
        for row in tables["campaigns"]:
            status = row["campaign_status"]
            if status == "ACTIVE":
                assert row["start_date"] <= today <= row["end_date"]
            elif status in {"COMPLETED", "CANCELLED"}:
                assert row["end_date"] < today
            elif status == "DRAFT":
                assert row["start_date"] > today

    def test_currency_is_consistent_along_the_demand_chain(self, tables, indexes):
        for row in tables["impressions"]:
            advertiser = indexes["advertisers"][row["advertiser_id"]]
            assert row["currency"] == advertiser["billing_currency"]

    def test_spend_impression_reference_matches_billing_type(self, tables):
        for row in tables["spend_transactions"]:
            if row["billing_type"] == "CPM":
                assert row["impression_id"] is None
            else:
                assert row["impression_id"] is not None

    def test_in_app_traffic_reports_an_in_app_browser(self, tables, indexes):
        for row in tables["impressions"]:
            placement_type = indexes["placements"][row["placement_id"]]["placement_type"]
            if placement_type in {"APP_BANNER", "APP_INTERSTITIAL"}:
                assert row["browser"] == "In-App"


class TestDistributionsAreRealistic:
    """The data must not be uniform: analytics on uniform data says nothing."""

    def test_campaigns_per_advertiser_vary(self, tables):
        per_advertiser = Counter(row["advertiser_id"] for row in tables["campaigns"])
        counts = sorted(per_advertiser.values())
        assert counts[-1] >= counts[0] * 3

    def test_traffic_is_concentrated_in_a_few_campaigns(self, tables):
        per_campaign = Counter(row["campaign_id"] for row in tables["impressions"])
        volumes = sorted(per_campaign.values(), reverse=True)
        top_decile = max(len(volumes) // 10, 1)
        share = sum(volumes[:top_decile]) / sum(volumes)
        assert share > 0.25, f"top 10% of campaigns only carry {share:.0%} of impressions"

    def test_traffic_is_concentrated_in_a_few_publishers(self, tables):
        per_publisher = Counter(row["publisher_id"] for row in tables["impressions"])
        volumes = sorted(per_publisher.values(), reverse=True)
        top_decile = max(len(volumes) // 10, 1)
        assert sum(volumes[:top_decile]) / sum(volumes) > 0.2

    def test_click_through_rate_varies_across_campaigns(self, tables):
        impressions = Counter(row["campaign_id"] for row in tables["impressions"])
        clicks = Counter(row["campaign_id"] for row in tables["clicks"])
        rates = [
            clicks[campaign_id] / total
            for campaign_id, total in impressions.items()
            if total >= 50 and clicks[campaign_id] > 0
        ]
        assert len(rates) >= 5
        assert max(rates) > min(rates) * 2

    def test_several_devices_countries_and_creative_types_appear(self, tables):
        assert len({row["device_type"] for row in tables["impressions"]}) >= 3
        assert len({row["country"] for row in tables["impressions"]}) >= 5
        assert len({row["creative_type"] for row in tables["creatives"]}) >= 2

    def test_traffic_follows_a_daily_rhythm(self, tables):
        hours = Counter(row["impression_timestamp"].hour for row in tables["impressions"])
        busiest = max(hours.values())
        quietest = min(hours.values())
        assert busiest > quietest * 2, "impression volume should vary by time of day"


class TestSpendCorrelatesWithDelivery:
    def test_cpm_spend_matches_the_clearing_prices_it_rolls_up(self, tables, indexes):
        """A CPM roll-up row must equal the media cost of the hour it covers."""
        by_campaign_media_cost: dict = {}
        for row in tables["impressions"]:
            campaign = indexes["campaigns"][row["campaign_id"]]
            if campaign["bid_strategy"] != "CPM":
                continue
            by_campaign_media_cost[row["campaign_id"]] = (
                by_campaign_media_cost.get(row["campaign_id"], 0)
                + float(row["clearing_price"]) / 1000
            )

        by_campaign_spend: dict = {}
        for row in tables["spend_transactions"]:
            if row["billing_type"] != "CPM":
                continue
            by_campaign_spend[row["campaign_id"]] = by_campaign_spend.get(
                row["campaign_id"], 0
            ) + float(row["spend_amount"])

        assert by_campaign_spend.keys() == by_campaign_media_cost.keys()
        for campaign_id, media_cost in by_campaign_media_cost.items():
            # Roll-up rows are stored as DECIMAL(14,6), so the sum of the rounded
            # buckets differs from the exact media cost by a few ulps per row.
            assert by_campaign_spend[campaign_id] == pytest.approx(media_cost, rel=1e-4, abs=1e-6)

    def test_cpc_spend_has_one_row_per_click(self, tables, indexes):
        cpc_campaigns = {
            row["campaign_id"] for row in tables["campaigns"] if row["bid_strategy"] == "CPC"
        }
        clicks = sum(1 for row in tables["clicks"] if row["campaign_id"] in cpc_campaigns)
        spend_rows = sum(1 for row in tables["spend_transactions"] if row["billing_type"] == "CPC")
        assert spend_rows == clicks

    def test_cpa_spend_has_one_row_per_conversion(self, tables):
        cpa_campaigns = {
            row["campaign_id"]
            for row in tables["campaigns"]
            if row["bid_strategy"] in {"CPA", "TARGET_ROAS", "MAX_CONVERSIONS"}
        }
        conversions = sum(1 for row in tables["conversions"] if row["campaign_id"] in cpa_campaigns)
        spend_rows = sum(1 for row in tables["spend_transactions"] if row["billing_type"] == "CPA")
        assert spend_rows == conversions


class TestAnalyticalQueryShape:
    def test_the_headline_analytical_join_returns_meaningful_rows(self, tables, indexes):
        """The join from the brief: advertiser -> campaign -> impression -> click -> conversion."""
        clicks_by_impression = {row["impression_id"]: row for row in tables["clicks"]}
        conversions_by_click = {row["click_id"]: row for row in tables["conversions"]}

        joined = 0
        for impression in tables["impressions"]:
            campaign = indexes["campaigns"][impression["campaign_id"]]
            advertiser = indexes["advertisers"][campaign["advertiser_id"]]
            assert advertiser is not None
            click = clicks_by_impression.get(impression["impression_id"])
            if click and conversions_by_click.get(click["click_id"]):
                joined += 1
        assert joined > 0, "no impression -> click -> conversion chain survived the join"
