"""Row-level models.

Each dataclass mirrors exactly one PostgreSQL table: same fields, same order,
nothing extra. Generation-time metadata (CTR profiles, traffic weights, delivery
windows) deliberately lives in ``relationships.py`` instead, so these objects
stay a faithful description of the source system.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date, datetime
from decimal import Decimal
from typing import ClassVar
from uuid import UUID


@dataclass(slots=True)
class Advertiser:
    advertiser_id: UUID
    advertiser_name: str
    industry: str
    billing_country: str
    billing_currency: str
    account_status: str
    created_at: datetime
    updated_at: datetime

    TABLE: ClassVar[str] = "advertisers"
    PRIMARY_KEY: ClassVar[str] = "advertiser_id"


@dataclass(slots=True)
class Campaign:
    campaign_id: UUID
    advertiser_id: UUID
    campaign_name: str
    campaign_objective: str
    campaign_status: str
    campaign_budget: Decimal
    daily_budget: Decimal
    start_date: date
    end_date: date
    bid_strategy: str
    created_at: datetime
    updated_at: datetime

    TABLE: ClassVar[str] = "campaigns"
    PRIMARY_KEY: ClassVar[str] = "campaign_id"


@dataclass(slots=True)
class LineItem:
    line_item_id: UUID
    campaign_id: UUID
    line_item_name: str
    line_item_status: str
    bid_amount: Decimal
    bid_currency: str
    optimization_goal: str
    target_cpm: Decimal
    frequency_cap: int
    start_date: date
    end_date: date
    created_at: datetime
    updated_at: datetime

    TABLE: ClassVar[str] = "line_items"
    PRIMARY_KEY: ClassVar[str] = "line_item_id"


@dataclass(slots=True)
class Creative:
    creative_id: UUID
    advertiser_id: UUID
    creative_name: str
    creative_type: str
    creative_format: str
    landing_page_url: str
    creative_status: str
    created_at: datetime
    updated_at: datetime

    TABLE: ClassVar[str] = "creatives"
    PRIMARY_KEY: ClassVar[str] = "creative_id"


@dataclass(slots=True)
class Publisher:
    publisher_id: UUID
    publisher_name: str
    publisher_type: str
    country: str
    domain: str
    publisher_status: str
    created_at: datetime
    updated_at: datetime

    TABLE: ClassVar[str] = "publishers"
    PRIMARY_KEY: ClassVar[str] = "publisher_id"


@dataclass(slots=True)
class Placement:
    placement_id: UUID
    publisher_id: UUID
    placement_name: str
    placement_type: str
    ad_format: str
    floor_price: Decimal
    currency: str
    device_type: str
    created_at: datetime
    updated_at: datetime

    TABLE: ClassVar[str] = "placements"
    PRIMARY_KEY: ClassVar[str] = "placement_id"


@dataclass(slots=True)
class Audience:
    audience_id: UUID
    audience_name: str
    audience_type: str
    min_age: int
    max_age: int
    gender: str
    country: str
    interest_category: str
    created_at: datetime
    updated_at: datetime

    TABLE: ClassVar[str] = "audiences"
    PRIMARY_KEY: ClassVar[str] = "audience_id"


@dataclass(slots=True)
class Impression:
    impression_id: UUID
    campaign_id: UUID
    line_item_id: UUID
    advertiser_id: UUID
    creative_id: UUID
    publisher_id: UUID
    placement_id: UUID
    audience_id: UUID
    impression_timestamp: datetime
    device_type: str
    operating_system: str
    browser: str
    country: str
    city: str
    bid_price: Decimal
    clearing_price: Decimal
    currency: str
    viewability_score: Decimal
    created_at: datetime

    TABLE: ClassVar[str] = "impressions"
    PRIMARY_KEY: ClassVar[str] = "impression_id"


@dataclass(slots=True)
class Click:
    click_id: UUID
    impression_id: UUID
    campaign_id: UUID
    line_item_id: UUID
    advertiser_id: UUID
    creative_id: UUID
    click_timestamp: datetime
    device_type: str
    country: str
    created_at: datetime

    TABLE: ClassVar[str] = "clicks"
    PRIMARY_KEY: ClassVar[str] = "click_id"


@dataclass(slots=True)
class Conversion:
    conversion_id: UUID
    click_id: UUID
    impression_id: UUID
    campaign_id: UUID
    advertiser_id: UUID
    conversion_type: str
    conversion_timestamp: datetime
    conversion_value: Decimal
    currency: str
    attribution_window_hours: int
    created_at: datetime

    TABLE: ClassVar[str] = "conversions"
    PRIMARY_KEY: ClassVar[str] = "conversion_id"


@dataclass(slots=True)
class SpendTransaction:
    spend_transaction_id: UUID
    campaign_id: UUID
    line_item_id: UUID
    advertiser_id: UUID
    publisher_id: UUID
    impression_id: UUID | None
    spend_timestamp: datetime
    spend_amount: Decimal
    currency: str
    billing_type: str
    created_at: datetime

    TABLE: ClassVar[str] = "spend_transactions"
    PRIMARY_KEY: ClassVar[str] = "spend_transaction_id"


#: Tables in foreign-key dependency order. Loading and validating in this order
#: means a child row is never written before its parent exists.
TABLE_ORDER: tuple[type, ...] = (
    Advertiser,
    Publisher,
    Placement,
    Campaign,
    LineItem,
    Creative,
    Audience,
    Impression,
    Click,
    Conversion,
    SpendTransaction,
)

TABLE_NAMES: tuple[str, ...] = tuple(model.TABLE for model in TABLE_ORDER)

MASTER_TABLES: frozenset[str] = frozenset(
    {"advertisers", "publishers", "placements", "campaigns", "line_items", "creatives", "audiences"}
)

EVENT_TABLES: frozenset[str] = frozenset(
    {"impressions", "clicks", "conversions", "spend_transactions"}
)

_COLUMN_CACHE: dict[type, tuple[str, ...]] = {}


def columns_of(model: type) -> tuple[str, ...]:
    """Column names in DDL order for a model class."""
    cached = _COLUMN_CACHE.get(model)
    if cached is None:
        cached = tuple(field.name for field in fields(model))
        _COLUMN_CACHE[model] = cached
    return cached


def row_of(instance: object) -> tuple:
    """Flatten a model instance into a tuple ordered like the table columns."""
    return tuple(getattr(instance, name) for name in columns_of(type(instance)))


MODEL_BY_TABLE: dict[str, type] = {model.TABLE: model for model in TABLE_ORDER}
