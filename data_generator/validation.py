"""In-process validation, run before anything is written.

The database's own constraints are the last line of defence, not the first. This
module checks every row against the ecosystem it was generated from *before* it
reaches a sink, so a generation bug surfaces as a precise error naming the check,
the table and the offending identifiers - rather than as a generic
``ForeignKeyViolation`` thousands of rows later.

Three families of checks run here:

* **Referential** - every foreign key resolves, and derived events agree with
  their parents (a click's campaign must be the campaign of its impression).
* **Temporal** - the chain advertiser -> campaign -> line item -> impression ->
  click -> conversion never goes backwards in time.
* **Business rules** - enum domains, non-negative money, viewability in [0, 1],
  min_age < max_age, attribution windows actually containing the conversion.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from data_generator.clicks import GeneratedClick
from data_generator.config import GenerationConfig
from data_generator.conversions import GeneratedConversion
from data_generator.impressions import GeneratedImpression
from data_generator.models import (
    Advertiser,
    Audience,
    Campaign,
    Creative,
    LineItem,
    Placement,
    Publisher,
    SpendTransaction,
)
from data_generator.relationships import Ecosystem
from data_generator.timeline import start_of_day

MAX_REPORTED_VIOLATIONS = 20

ACCOUNT_STATUSES = frozenset({"ACTIVE", "SUSPENDED", "CLOSED"})
CAMPAIGN_OBJECTIVES = frozenset(
    {"AWARENESS", "TRAFFIC", "ENGAGEMENT", "CONVERSIONS", "APP_INSTALLS"}
)
CAMPAIGN_STATUSES = frozenset({"DRAFT", "ACTIVE", "PAUSED", "COMPLETED", "CANCELLED"})
BID_STRATEGIES = frozenset({"CPC", "CPM", "CPA", "TARGET_ROAS", "MAX_CONVERSIONS"})
OPTIMIZATION_GOALS = frozenset({"CLICKS", "CONVERSIONS", "REVENUE", "REACH", "IMPRESSIONS"})
CREATIVE_TYPES = frozenset({"BANNER", "VIDEO", "NATIVE", "AUDIO"})
CREATIVE_STATUSES = frozenset({"ACTIVE", "PAUSED", "REJECTED", "EXPIRED"})
PUBLISHER_TYPES = frozenset({"WEBSITE", "MOBILE_APP", "CTV", "AUDIO"})
PLACEMENT_TYPES = frozenset(
    {
        "HEADER",
        "SIDEBAR",
        "IN_FEED",
        "VIDEO_PRE_ROLL",
        "VIDEO_MID_ROLL",
        "APP_BANNER",
        "APP_INTERSTITIAL",
    }
)
DEVICE_TYPES = frozenset({"DESKTOP", "MOBILE", "TABLET", "CTV", "SMART_SPEAKER"})
AUDIENCE_TYPES = frozenset({"DEMOGRAPHIC", "INTEREST", "BEHAVIORAL", "LOOKALIKE", "RETARGETING"})
GENDERS = frozenset({"MALE", "FEMALE", "ALL"})
CONVERSION_TYPES = frozenset({"PURCHASE", "SIGNUP", "LEAD", "APP_INSTALL", "SUBSCRIPTION"})
BILLING_TYPES = frozenset({"CPM", "CPC", "CPA"})


@dataclass(frozen=True, slots=True)
class Violation:
    check: str
    table: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.table}] {self.check}: {self.detail}"


class ValidationError(RuntimeError):
    """Raised as soon as generated data breaks an invariant."""

    def __init__(self, violations: Sequence[Violation]) -> None:
        self.violations = list(violations)
        shown = self.violations[:MAX_REPORTED_VIOLATIONS]
        summary = "\n  - ".join(str(violation) for violation in shown)
        suffix = (
            f"\n  ... and {len(self.violations) - len(shown)} more"
            if len(self.violations) > len(shown)
            else ""
        )
        super().__init__(f"{len(self.violations)} validation violation(s):\n  - {summary}{suffix}")


@dataclass(slots=True)
class ValidationStats:
    rows_validated: dict[str, int] = field(default_factory=dict)
    referential_checks: int = 0
    temporal_checks: int = 0
    business_rule_checks: int = 0
    duplicate_keys_found: int = 0

    @property
    def total_checks(self) -> int:
        return self.referential_checks + self.temporal_checks + self.business_rule_checks

    def rows(self, table: str, count: int) -> None:
        self.rows_validated[table] = self.rows_validated.get(table, 0) + count

    def as_dict(self) -> dict[str, object]:
        return {
            "rows_validated": dict(self.rows_validated),
            "referential_checks": self.referential_checks,
            "temporal_checks": self.temporal_checks,
            "business_rule_checks": self.business_rule_checks,
            "total_checks": self.total_checks,
            "duplicate_keys_found": self.duplicate_keys_found,
        }


class EcosystemValidator:
    """Validates generated rows against the ecosystem that produced them."""

    def __init__(
        self,
        ecosystem: Ecosystem,
        config: GenerationConfig,
        *,
        track_primary_keys: bool | None = None,
    ) -> None:
        self.ecosystem = ecosystem
        self.config = config
        self.stats = ValidationStats()
        self._track_keys = (
            config.validation.track_primary_keys
            if track_primary_keys is None
            else track_primary_keys
        )
        self._seen_keys: dict[str, set[int]] = {}

    # -- master tables ----------------------------------------------------

    def validate_advertisers(self, rows: Sequence[Advertiser]) -> None:
        violations: list[Violation] = []
        for row in rows:
            self._track(violations, "advertisers", row.advertiser_id)
            self._enum(
                violations,
                "advertisers",
                "account_status",
                row.account_status,
                ACCOUNT_STATUSES,
                row.advertiser_id,
            )
            self._rule(
                violations,
                len(row.billing_currency) == 3 and row.billing_currency.isupper(),
                "currency_format",
                "advertisers",
                f"advertiser {row.advertiser_id} currency {row.billing_currency!r}",
            )
            self._rule(
                violations,
                bool(row.advertiser_name.strip()),
                "not_null",
                "advertisers",
                f"advertiser {row.advertiser_id} has an empty name",
            )
            self._ordered(
                violations,
                "advertisers",
                "created_at <= updated_at",
                row.created_at,
                row.updated_at,
                row.advertiser_id,
            )
        self.stats.rows("advertisers", len(rows))
        _raise(violations)

    def validate_publishers(self, rows: Sequence[Publisher]) -> None:
        violations: list[Violation] = []
        domains: set[str] = set()
        for row in rows:
            self._track(violations, "publishers", row.publisher_id)
            self._enum(
                violations,
                "publishers",
                "publisher_type",
                row.publisher_type,
                PUBLISHER_TYPES,
                row.publisher_id,
            )
            self._enum(
                violations,
                "publishers",
                "publisher_status",
                row.publisher_status,
                ACCOUNT_STATUSES,
                row.publisher_id,
            )
            self._rule(
                violations,
                row.domain not in domains,
                "unique_domain",
                "publishers",
                f"duplicate domain {row.domain!r}",
            )
            domains.add(row.domain)
            self._ordered(
                violations,
                "publishers",
                "created_at <= updated_at",
                row.created_at,
                row.updated_at,
                row.publisher_id,
            )
        self.stats.rows("publishers", len(rows))
        _raise(violations)

    def validate_placements(self, rows: Sequence[Placement]) -> None:
        violations: list[Violation] = []
        for row in rows:
            self._track(violations, "placements", row.placement_id)
            publisher = self._parent(
                violations,
                "placements",
                "publisher_id",
                row.publisher_id,
                self.ecosystem.publishers,
                row.placement_id,
            )
            self._enum(
                violations,
                "placements",
                "placement_type",
                row.placement_type,
                PLACEMENT_TYPES,
                row.placement_id,
            )
            self._enum(
                violations,
                "placements",
                "device_type",
                row.device_type,
                DEVICE_TYPES,
                row.placement_id,
            )
            self._rule(
                violations,
                row.floor_price >= 0,
                "non_negative_price",
                "placements",
                f"placement {row.placement_id} floor_price {row.floor_price}",
            )
            if publisher is not None:
                self._ordered(
                    violations,
                    "placements",
                    "publisher.created_at <= placement.created_at",
                    publisher.row.created_at,
                    row.created_at,
                    row.placement_id,
                )
            self._ordered(
                violations,
                "placements",
                "created_at <= updated_at",
                row.created_at,
                row.updated_at,
                row.placement_id,
            )
        self.stats.rows("placements", len(rows))
        _raise(violations)

    def validate_campaigns(self, rows: Sequence[Campaign]) -> None:
        violations: list[Violation] = []
        for row in rows:
            self._track(violations, "campaigns", row.campaign_id)
            advertiser = self._parent(
                violations,
                "campaigns",
                "advertiser_id",
                row.advertiser_id,
                self.ecosystem.advertisers,
                row.campaign_id,
            )
            self._enum(
                violations,
                "campaigns",
                "campaign_objective",
                row.campaign_objective,
                CAMPAIGN_OBJECTIVES,
                row.campaign_id,
            )
            self._enum(
                violations,
                "campaigns",
                "campaign_status",
                row.campaign_status,
                CAMPAIGN_STATUSES,
                row.campaign_id,
            )
            self._enum(
                violations,
                "campaigns",
                "bid_strategy",
                row.bid_strategy,
                BID_STRATEGIES,
                row.campaign_id,
            )
            self._rule(
                violations,
                row.start_date < row.end_date,
                "start_before_end",
                "campaigns",
                f"campaign {row.campaign_id} {row.start_date}..{row.end_date}",
            )
            self._rule(
                violations,
                row.campaign_budget > 0 and row.daily_budget > 0,
                "positive_budget",
                "campaigns",
                f"campaign {row.campaign_id} budget {row.campaign_budget}/{row.daily_budget}",
            )
            self._rule(
                violations,
                row.daily_budget <= row.campaign_budget,
                "daily_budget_within_total",
                "campaigns",
                f"campaign {row.campaign_id} daily {row.daily_budget} > total {row.campaign_budget}",
            )
            self._rule(
                violations,
                self._status_matches_dates(row),
                "status_matches_dates",
                "campaigns",
                f"campaign {row.campaign_id} is {row.campaign_status} but runs "
                f"{row.start_date}..{row.end_date}",
            )
            if advertiser is not None:
                self._ordered(
                    violations,
                    "campaigns",
                    "advertiser.created_at <= campaign.created_at",
                    advertiser.row.created_at,
                    row.created_at,
                    row.campaign_id,
                )
            self._ordered(
                violations,
                "campaigns",
                "campaign.created_at < flight start",
                row.created_at,
                start_of_day(row.start_date),
                row.campaign_id,
            )
            self._ordered(
                violations,
                "campaigns",
                "created_at <= updated_at",
                row.created_at,
                row.updated_at,
                row.campaign_id,
            )
        self.stats.rows("campaigns", len(rows))
        _raise(violations)

    def validate_line_items(self, rows: Sequence[LineItem]) -> None:
        violations: list[Violation] = []
        for row in rows:
            self._track(violations, "line_items", row.line_item_id)
            campaign = self._parent(
                violations,
                "line_items",
                "campaign_id",
                row.campaign_id,
                self.ecosystem.campaigns,
                row.line_item_id,
            )
            self._enum(
                violations,
                "line_items",
                "line_item_status",
                row.line_item_status,
                CAMPAIGN_STATUSES,
                row.line_item_id,
            )
            self._enum(
                violations,
                "line_items",
                "optimization_goal",
                row.optimization_goal,
                OPTIMIZATION_GOALS,
                row.line_item_id,
            )
            self._rule(
                violations,
                row.start_date <= row.end_date,
                "start_before_end",
                "line_items",
                f"line item {row.line_item_id} {row.start_date}..{row.end_date}",
            )
            self._rule(
                violations,
                row.bid_amount > 0 and row.target_cpm > 0 and row.frequency_cap > 0,
                "positive_bid",
                "line_items",
                f"line item {row.line_item_id} bid {row.bid_amount} cpm {row.target_cpm}",
            )
            if campaign is not None:
                self._rule(
                    violations,
                    campaign.row.start_date <= row.start_date
                    and row.end_date <= campaign.row.end_date,
                    "flight_within_campaign",
                    "line_items",
                    f"line item {row.line_item_id} {row.start_date}..{row.end_date} outside "
                    f"campaign {row.campaign_id} {campaign.row.start_date}..{campaign.row.end_date}",
                )
                self._ordered(
                    violations,
                    "line_items",
                    "campaign.created_at <= line_item.created_at",
                    campaign.row.created_at,
                    row.created_at,
                    row.line_item_id,
                )
                self._ordered(
                    violations,
                    "line_items",
                    "line_item.created_at <= flight start",
                    row.created_at,
                    start_of_day(campaign.row.start_date),
                    row.line_item_id,
                )
            self._ordered(
                violations,
                "line_items",
                "created_at <= updated_at",
                row.created_at,
                row.updated_at,
                row.line_item_id,
            )
        self.stats.rows("line_items", len(rows))
        _raise(violations)

    def validate_creatives(self, rows: Sequence[Creative]) -> None:
        violations: list[Violation] = []
        for row in rows:
            self._track(violations, "creatives", row.creative_id)
            advertiser = self._parent(
                violations,
                "creatives",
                "advertiser_id",
                row.advertiser_id,
                self.ecosystem.advertisers,
                row.creative_id,
            )
            self._enum(
                violations,
                "creatives",
                "creative_type",
                row.creative_type,
                CREATIVE_TYPES,
                row.creative_id,
            )
            self._enum(
                violations,
                "creatives",
                "creative_status",
                row.creative_status,
                CREATIVE_STATUSES,
                row.creative_id,
            )
            self._rule(
                violations,
                row.landing_page_url.startswith("https://"),
                "https_landing_page",
                "creatives",
                f"creative {row.creative_id} url {row.landing_page_url!r}",
            )
            if advertiser is not None:
                self._ordered(
                    violations,
                    "creatives",
                    "advertiser.created_at <= creative.created_at",
                    advertiser.row.created_at,
                    row.created_at,
                    row.creative_id,
                )
            self._ordered(
                violations,
                "creatives",
                "created_at <= updated_at",
                row.created_at,
                row.updated_at,
                row.creative_id,
            )
        self.stats.rows("creatives", len(rows))
        _raise(violations)

    def validate_audiences(self, rows: Sequence[Audience]) -> None:
        violations: list[Violation] = []
        for row in rows:
            self._track(violations, "audiences", row.audience_id)
            self._enum(
                violations,
                "audiences",
                "audience_type",
                row.audience_type,
                AUDIENCE_TYPES,
                row.audience_id,
            )
            self._enum(violations, "audiences", "gender", row.gender, GENDERS, row.audience_id)
            self._rule(
                violations,
                row.min_age < row.max_age,
                "age_range",
                "audiences",
                f"audience {row.audience_id} ages {row.min_age}..{row.max_age}",
            )
            self._rule(
                violations,
                row.min_age >= 13 and row.max_age <= 99,
                "age_bounds",
                "audiences",
                f"audience {row.audience_id} ages {row.min_age}..{row.max_age}",
            )
            self._ordered(
                violations,
                "audiences",
                "created_at <= updated_at",
                row.created_at,
                row.updated_at,
                row.audience_id,
            )
        self.stats.rows("audiences", len(rows))
        _raise(violations)

    # -- event tables -----------------------------------------------------

    def validate_impressions(self, items: Sequence[GeneratedImpression]) -> None:
        violations: list[Violation] = []
        ecosystem = self.ecosystem
        for item in items:
            row = item.row
            self._track(violations, "impressions", row.impression_id)

            campaign = self._parent(
                violations,
                "impressions",
                "campaign_id",
                row.campaign_id,
                ecosystem.campaigns,
                row.impression_id,
            )
            line_item = self._parent(
                violations,
                "impressions",
                "line_item_id",
                row.line_item_id,
                ecosystem.line_items,
                row.impression_id,
            )
            creative = self._parent(
                violations,
                "impressions",
                "creative_id",
                row.creative_id,
                ecosystem.creatives,
                row.impression_id,
            )
            placement = self._parent(
                violations,
                "impressions",
                "placement_id",
                row.placement_id,
                ecosystem.placements,
                row.impression_id,
            )
            self._parent(
                violations,
                "impressions",
                "advertiser_id",
                row.advertiser_id,
                ecosystem.advertisers,
                row.impression_id,
            )
            self._parent(
                violations,
                "impressions",
                "publisher_id",
                row.publisher_id,
                ecosystem.publishers,
                row.impression_id,
            )
            self._parent(
                violations,
                "impressions",
                "audience_id",
                row.audience_id,
                ecosystem.audiences,
                row.impression_id,
            )

            # The combination has to be internally consistent, not merely resolvable.
            if line_item is not None:
                self._rule(
                    violations,
                    line_item.campaign_id == row.campaign_id,
                    "line_item_belongs_to_campaign",
                    "impressions",
                    f"impression {row.impression_id} line item {row.line_item_id} belongs to "
                    f"campaign {line_item.campaign_id}, not {row.campaign_id}",
                )
            if campaign is not None:
                self._rule(
                    violations,
                    campaign.advertiser_id == row.advertiser_id,
                    "advertiser_matches_campaign",
                    "impressions",
                    f"impression {row.impression_id} advertiser {row.advertiser_id} != campaign "
                    f"advertiser {campaign.advertiser_id}",
                )
                self._rule(
                    violations,
                    campaign.row.start_date
                    <= row.impression_timestamp.date()
                    <= campaign.row.end_date,
                    "event_within_campaign_flight",
                    "impressions",
                    f"impression {row.impression_id} at {row.impression_timestamp} outside "
                    f"{campaign.row.start_date}..{campaign.row.end_date}",
                )
            if creative is not None:
                self._rule(
                    violations,
                    creative.advertiser_id == row.advertiser_id,
                    "creative_belongs_to_advertiser",
                    "impressions",
                    f"impression {row.impression_id} creative {row.creative_id} belongs to "
                    f"advertiser {creative.advertiser_id}",
                )
                self._ordered(
                    violations,
                    "impressions",
                    "creative.created_at <= impression",
                    creative.row.created_at,
                    row.impression_timestamp,
                    row.impression_id,
                )
            if placement is not None:
                self._rule(
                    violations,
                    placement.row.publisher_id == row.publisher_id,
                    "placement_belongs_to_publisher",
                    "impressions",
                    f"impression {row.impression_id} placement {row.placement_id} belongs to "
                    f"publisher {placement.row.publisher_id}",
                )
                self._ordered(
                    violations,
                    "impressions",
                    "placement.created_at <= impression",
                    placement.row.created_at,
                    row.impression_timestamp,
                    row.impression_id,
                )
            if line_item is not None:
                self._ordered(
                    violations,
                    "impressions",
                    "line_item.created_at <= impression",
                    line_item.row.created_at,
                    row.impression_timestamp,
                    row.impression_id,
                )

            self._enum(
                violations,
                "impressions",
                "device_type",
                row.device_type,
                DEVICE_TYPES,
                row.impression_id,
            )
            self._rule(
                violations,
                0 <= row.viewability_score <= 1,
                "viewability_in_range",
                "impressions",
                f"impression {row.impression_id} viewability {row.viewability_score}",
            )
            self._rule(
                violations,
                row.bid_price >= 0 and row.clearing_price >= 0,
                "non_negative_price",
                "impressions",
                f"impression {row.impression_id} bid {row.bid_price} clearing {row.clearing_price}",
            )
            self._rule(
                violations,
                row.clearing_price <= row.bid_price,
                "clearing_at_or_below_bid",
                "impressions",
                f"impression {row.impression_id} clearing {row.clearing_price} > bid {row.bid_price}",
            )
            self._ordered(
                violations,
                "impressions",
                "impression <= created_at",
                row.impression_timestamp,
                row.created_at,
                row.impression_id,
            )
        self.stats.rows("impressions", len(items))
        _raise(violations)

    def validate_clicks(self, items: Sequence[GeneratedClick]) -> None:
        violations: list[Violation] = []
        for item in items:
            row = item.row
            source = item.impression.row
            self._track(violations, "clicks", row.click_id)
            self.stats.referential_checks += 1
            self._rule(
                violations,
                row.impression_id == source.impression_id,
                "click_references_its_impression",
                "clicks",
                f"click {row.click_id} points at {row.impression_id}, generated from "
                f"{source.impression_id}",
            )
            for column in ("campaign_id", "line_item_id", "advertiser_id", "creative_id"):
                self.stats.referential_checks += 1
                self._rule(
                    violations,
                    getattr(row, column) == getattr(source, column),
                    f"click_{column}_matches_impression",
                    "clicks",
                    f"click {row.click_id} {column}={getattr(row, column)} but impression has "
                    f"{getattr(source, column)}",
                )
            self._rule(
                violations,
                row.device_type == source.device_type and row.country == source.country,
                "click_context_matches_impression",
                "clicks",
                f"click {row.click_id} context differs from impression {source.impression_id}",
            )
            self._ordered(
                violations,
                "clicks",
                "impression <= click",
                source.impression_timestamp,
                row.click_timestamp,
                row.click_id,
            )
            self._ordered(
                violations,
                "clicks",
                "click <= created_at",
                row.click_timestamp,
                row.created_at,
                row.click_id,
            )
        self.stats.rows("clicks", len(items))
        _raise(violations)

    def validate_conversions(self, items: Sequence[GeneratedConversion]) -> None:
        violations: list[Violation] = []
        for item in items:
            row = item.row
            click = item.click.row
            self._track(violations, "conversions", row.conversion_id)
            self.stats.referential_checks += 2
            self._rule(
                violations,
                row.click_id == click.click_id,
                "conversion_references_its_click",
                "conversions",
                f"conversion {row.conversion_id} points at {row.click_id}, generated from "
                f"{click.click_id}",
            )
            self._rule(
                violations,
                row.impression_id == click.impression_id,
                "conversion_impression_matches_click",
                "conversions",
                f"conversion {row.conversion_id} impression {row.impression_id} != click "
                f"impression {click.impression_id}",
            )
            for column in ("campaign_id", "advertiser_id"):
                self.stats.referential_checks += 1
                self._rule(
                    violations,
                    getattr(row, column) == getattr(click, column),
                    f"conversion_{column}_matches_click",
                    "conversions",
                    f"conversion {row.conversion_id} {column} differs from its click",
                )
            self._enum(
                violations,
                "conversions",
                "conversion_type",
                row.conversion_type,
                CONVERSION_TYPES,
                row.conversion_id,
            )
            self._rule(
                violations,
                row.conversion_value >= 0,
                "non_negative_value",
                "conversions",
                f"conversion {row.conversion_id} value {row.conversion_value}",
            )
            self._ordered(
                violations,
                "conversions",
                "click <= conversion",
                click.click_timestamp,
                row.conversion_timestamp,
                row.conversion_id,
            )
            self._ordered(
                violations,
                "conversions",
                "conversion <= created_at",
                row.conversion_timestamp,
                row.created_at,
                row.conversion_id,
            )
            elapsed_hours = (
                row.conversion_timestamp - click.click_timestamp
            ).total_seconds() / 3600.0
            self._rule(
                violations,
                elapsed_hours <= row.attribution_window_hours + 1e-6,
                "conversion_within_attribution_window",
                "conversions",
                f"conversion {row.conversion_id} is {elapsed_hours:.2f}h after the click but "
                f"claims a {row.attribution_window_hours}h window",
            )
        self.stats.rows("conversions", len(items))
        _raise(violations)

    def validate_spend(self, rows: Sequence[SpendTransaction]) -> None:
        violations: list[Violation] = []
        ecosystem = self.ecosystem
        for row in rows:
            self._track(violations, "spend_transactions", row.spend_transaction_id)
            self._parent(
                violations,
                "spend_transactions",
                "campaign_id",
                row.campaign_id,
                ecosystem.campaigns,
                row.spend_transaction_id,
            )
            line_item = self._parent(
                violations,
                "spend_transactions",
                "line_item_id",
                row.line_item_id,
                ecosystem.line_items,
                row.spend_transaction_id,
            )
            self._parent(
                violations,
                "spend_transactions",
                "publisher_id",
                row.publisher_id,
                ecosystem.publishers,
                row.spend_transaction_id,
            )
            if line_item is not None:
                self._rule(
                    violations,
                    line_item.campaign_id == row.campaign_id,
                    "line_item_belongs_to_campaign",
                    "spend_transactions",
                    f"spend {row.spend_transaction_id} line item belongs to "
                    f"{line_item.campaign_id}, not {row.campaign_id}",
                )
            self._enum(
                violations,
                "spend_transactions",
                "billing_type",
                row.billing_type,
                BILLING_TYPES,
                row.spend_transaction_id,
            )
            self._rule(
                violations,
                row.spend_amount >= 0,
                "non_negative_spend",
                "spend_transactions",
                f"spend {row.spend_transaction_id} amount {row.spend_amount}",
            )
            self._rule(
                violations,
                (row.billing_type == "CPM") == (row.impression_id is None),
                "impression_reference_matches_billing_type",
                "spend_transactions",
                f"spend {row.spend_transaction_id} billing {row.billing_type} with "
                f"impression_id={row.impression_id}",
            )
            self._ordered(
                violations,
                "spend_transactions",
                "spend <= created_at",
                row.spend_timestamp,
                row.created_at,
                row.spend_transaction_id,
            )
        self.stats.rows("spend_transactions", len(rows))
        _raise(violations)

    # -- primitives -------------------------------------------------------

    def _rule(
        self, violations: list[Violation], condition: bool, check: str, table: str, detail: str
    ) -> None:
        self.stats.business_rule_checks += 1
        if not condition:
            violations.append(Violation(check, table, detail))

    def _enum(
        self,
        violations: list[Violation],
        table: str,
        column: str,
        value: str,
        allowed: Iterable[str],
        key: UUID,
    ) -> None:
        self.stats.business_rule_checks += 1
        if value not in allowed:
            violations.append(Violation(f"valid_{column}", table, f"{key} has {column}={value!r}"))

    def _ordered(
        self,
        violations: list[Violation],
        table: str,
        check: str,
        earlier: datetime,
        later: datetime,
        key: UUID,
    ) -> None:
        self.stats.temporal_checks += 1
        if earlier > later:
            violations.append(
                Violation(check, table, f"{key}: {earlier.isoformat()} > {later.isoformat()}")
            )

    def _parent(
        self,
        violations: list[Violation],
        table: str,
        column: str,
        value: UUID,
        registry: dict,
        key: UUID,
    ):
        self.stats.referential_checks += 1
        parent = registry.get(value)
        if parent is None:
            violations.append(
                Violation(f"fk_{column}", table, f"{key} references missing {column}={value}")
            )
        return parent

    def _track(self, violations: list[Violation], table: str, key: UUID) -> None:
        if not self._track_keys:
            return
        seen = self._seen_keys.setdefault(table, set())
        # Store the int rather than the UUID object: same guarantee, less memory.
        marker = key.int
        if marker in seen:
            self.stats.duplicate_keys_found += 1
            violations.append(Violation("primary_key_unique", table, f"duplicate key {key}"))
        else:
            seen.add(marker)

    def _status_matches_dates(self, row: Campaign) -> bool:
        """A campaign's status has to agree with where "now" sits in its flight."""
        today = self.config.timeline.simulation_end_date
        status = row.campaign_status
        if status == "DRAFT":
            return row.start_date > today
        if status == "ACTIVE":
            return row.start_date <= today <= row.end_date
        if status in {"COMPLETED", "CANCELLED"}:
            return row.end_date < today
        # PAUSED can sit anywhere inside or after its flight, but it must have started.
        return row.start_date <= today


def _raise(violations: list[Violation]) -> None:
    if violations:
        raise ValidationError(violations)
