"""Typed access to the reference vocabulary in ``data_generator/reference/``.

All conditional weight tables (objective -> bid strategy, publisher type ->
placement type, device -> OS -> browser, ...) are loaded once and turned into
samplers. Keeping them as data means attribute combinations stay plausible
without a single hard-coded choice in the generator modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any

import yaml

from data_generator.config import REFERENCE_DIR
from data_generator.distributions import WeightedSampler


@dataclass(frozen=True, slots=True)
class Country:
    code: str
    name: str
    currency: str
    region: str
    weight: float
    cpm_index: float
    cities: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Industry:
    name: str
    group: str
    weight: float
    roas_index: float
    ctr_index: float


@dataclass(frozen=True, slots=True)
class AdvertiserTier:
    name: str
    weight: float
    budget_index: float
    volume_index: float
    campaign_index: float


@dataclass(frozen=True, slots=True)
class PlacementTypeProfile:
    ctr_index: float
    view_alpha: float
    view_beta: float
    cpm_index: float


@dataclass(frozen=True, slots=True)
class DeviceProfile:
    ctr_index: float
    cvr_index: float


@dataclass(frozen=True, slots=True)
class AudienceTypeProfile:
    ctr_index: float
    cvr_index: float


@dataclass(frozen=True, slots=True)
class AgeBracket:
    min: int
    max: int
    weight: float


class ReferenceData:
    """Loaded reference vocabulary plus the samplers built on top of it."""

    def __init__(self, geography: dict[str, Any], business: dict[str, Any], naming: dict[str, Any]):
        self._geography = geography
        self._business = business
        self.naming = naming

        self.countries: tuple[Country, ...] = tuple(
            Country(
                code=item["code"],
                name=item["name"],
                currency=item["currency"],
                region=item["region"],
                weight=float(item["weight"]),
                cpm_index=float(item["cpm_index"]),
                cities=tuple(item["cities"]),
            )
            for item in geography["countries"]
        )
        self.country_by_name: dict[str, Country] = {c.name: c for c in self.countries}
        self.country_by_code: dict[str, Country] = {c.code: c for c in self.countries}

        self.industries: tuple[Industry, ...] = tuple(
            Industry(
                name=item["name"],
                group=item["group"],
                weight=float(item["weight"]),
                roas_index=float(item["roas_index"]),
                ctr_index=float(item["ctr_index"]),
            )
            for item in business["industries"]
        )
        self.industry_by_name: dict[str, Industry] = {i.name: i for i in self.industries}

        self.advertiser_tiers: tuple[AdvertiserTier, ...] = tuple(
            AdvertiserTier(
                name=item["name"],
                weight=float(item["weight"]),
                budget_index=float(item["budget_index"]),
                volume_index=float(item["volume_index"]),
                campaign_index=float(item["campaign_index"]),
            )
            for item in business["advertiser_tiers"]
        )

        self.placement_type_profile: dict[str, PlacementTypeProfile] = {
            key: PlacementTypeProfile(**value)
            for key, value in business["placement_type_profile"].items()
        }
        self.device_profile: dict[str, DeviceProfile] = {
            key: DeviceProfile(**value) for key, value in business["device_profile"].items()
        }
        self.audience_type_profile: dict[str, AudienceTypeProfile] = {
            key: AudienceTypeProfile(**value)
            for key, value in business["audience_type_profile"].items()
        }
        self.age_brackets: tuple[AgeBracket, ...] = tuple(
            AgeBracket(min=int(item["min"]), max=int(item["max"]), weight=float(item["weight"]))
            for item in business["audience_age_brackets"]
        )

        self.interest_categories: tuple[str, ...] = tuple(business["interest_categories"])
        self.in_app_placement_types: frozenset[str] = frozenset(business["in_app_placement_types"])
        self.creative_servable_statuses: frozenset[str] = frozenset(
            business["creative_servable_statuses"]
        )
        self.placement_types_by_creative_type: dict[str, frozenset[str]] = {
            key: frozenset(value)
            for key, value in business["placement_types_by_creative_type"].items()
        }
        self.ad_formats_by_creative_type: dict[str, frozenset[str]] = {
            key: frozenset(value) for key, value in business["ad_formats_by_creative_type"].items()
        }
        self.day_of_week_weights: tuple[float, ...] = tuple(
            float(v) for v in business["day_of_week_weights"]
        )
        self.hour_of_day_weights: tuple[float, ...] = tuple(
            float(v) for v in business["hour_of_day_weights"]
        )

    # -- loading ----------------------------------------------------------

    @classmethod
    def load(cls, directory: Path = REFERENCE_DIR) -> ReferenceData:
        return cls(
            geography=_read_yaml(directory / "geography.yml"),
            business=_read_yaml(directory / "business.yml"),
            naming=_read_yaml(directory / "naming.yml"),
        )

    # -- samplers ---------------------------------------------------------

    @cached_property
    def country_sampler(self) -> WeightedSampler[Country]:
        return WeightedSampler(self.countries, [c.weight for c in self.countries])

    @cached_property
    def industry_sampler(self) -> WeightedSampler[Industry]:
        return WeightedSampler(self.industries, [i.weight for i in self.industries])

    @cached_property
    def advertiser_tier_sampler(self) -> WeightedSampler[AdvertiserTier]:
        return WeightedSampler(self.advertiser_tiers, [t.weight for t in self.advertiser_tiers])

    @cached_property
    def advertiser_status_sampler(self) -> WeightedSampler[str]:
        return WeightedSampler.from_mapping(self._business["advertiser_status_weights"])

    @cached_property
    def campaign_objective_sampler(self) -> WeightedSampler[str]:
        return WeightedSampler.from_mapping(self._business["campaign_objective_weights"])

    @cached_property
    def campaign_status_sampler(self) -> WeightedSampler[str]:
        return WeightedSampler.from_mapping(self._business["campaign_status_weights"])

    @cached_property
    def line_item_status_sampler(self) -> WeightedSampler[str]:
        return WeightedSampler.from_mapping(self._business["line_item_status_weights"])

    @cached_property
    def creative_status_sampler(self) -> WeightedSampler[str]:
        return WeightedSampler.from_mapping(self._business["creative_status_weights"])

    @cached_property
    def publisher_type_sampler(self) -> WeightedSampler[str]:
        return WeightedSampler.from_mapping(self._business["publisher_type_weights"])

    @cached_property
    def publisher_status_sampler(self) -> WeightedSampler[str]:
        return WeightedSampler.from_mapping(self._business["publisher_status_weights"])

    @cached_property
    def audience_type_sampler(self) -> WeightedSampler[str]:
        return WeightedSampler.from_mapping(self._business["audience_type_weights"])

    @cached_property
    def audience_gender_sampler(self) -> WeightedSampler[str]:
        return WeightedSampler.from_mapping(self._business["audience_gender_weights"])

    @cached_property
    def age_bracket_sampler(self) -> WeightedSampler[AgeBracket]:
        return WeightedSampler(self.age_brackets, [b.weight for b in self.age_brackets])

    @cached_property
    def hour_of_day_sampler(self) -> WeightedSampler[int]:
        return WeightedSampler(tuple(range(24)), self.hour_of_day_weights)

    @cached_property
    def bid_strategy_samplers(self) -> dict[str, WeightedSampler[str]]:
        return _mapping_of_samplers(self._business["bid_strategy_by_objective"])

    @cached_property
    def optimization_goal_samplers(self) -> dict[str, WeightedSampler[str]]:
        return _mapping_of_samplers(self._business["optimization_goal_by_objective"])

    @cached_property
    def conversion_type_samplers(self) -> dict[str, WeightedSampler[str]]:
        return _mapping_of_samplers(self._business["conversion_type_by_objective"])

    @cached_property
    def creative_type_sampler(self) -> WeightedSampler[str]:
        return WeightedSampler.from_mapping(self._business["creative_type_weights"])

    @cached_property
    def creative_format_samplers(self) -> dict[str, WeightedSampler[str]]:
        return _mapping_of_samplers(self._business["creative_format_by_type"])

    @cached_property
    def placement_type_samplers(self) -> dict[str, WeightedSampler[str]]:
        return _mapping_of_samplers(self._business["placement_type_by_publisher_type"])

    @cached_property
    def device_samplers(self) -> dict[str, WeightedSampler[str]]:
        return _mapping_of_samplers(self._business["device_by_publisher_type"])

    @cached_property
    def operating_system_samplers(self) -> dict[str, WeightedSampler[str]]:
        return _mapping_of_samplers(self._business["operating_system_by_device"])

    @cached_property
    def browser_samplers(self) -> dict[str, WeightedSampler[str]]:
        return _mapping_of_samplers(self._business["browser_by_operating_system"])

    @cached_property
    def _ad_format_samplers(self) -> dict[str, WeightedSampler[str]]:
        return _mapping_of_samplers(self._business["ad_format_by_placement_type"])

    @cached_property
    def _ad_format_override_samplers(self) -> dict[str, dict[str, WeightedSampler[str]]]:
        return {
            publisher_type: _mapping_of_samplers(by_placement)
            for publisher_type, by_placement in self._business[
                "ad_format_overrides_by_publisher_type"
            ].items()
        }

    def ad_format_sampler(self, publisher_type: str, placement_type: str) -> WeightedSampler[str]:
        """Ad format depends on the placement, narrowed by the publisher's medium."""
        override = self._ad_format_override_samplers.get(publisher_type, {}).get(placement_type)
        if override is not None:
            return override
        sampler = self._ad_format_samplers.get(placement_type)
        if sampler is None:
            raise KeyError(f"No ad format weights configured for placement type {placement_type!r}")
        return sampler

    def creative_type_weights_for(self, allowed_types: set[str]) -> dict[str, float]:
        """Creative type weights restricted to types that have servable inventory."""
        weights = {
            key: float(value)
            for key, value in self._business["creative_type_weights"].items()
            if key in allowed_types
        }
        if not weights:
            raise ValueError(
                "No creative type has compatible inventory; generate more publishers/placements"
            )
        return weights


def _mapping_of_samplers(raw: dict[str, dict[str, float]]) -> dict[str, WeightedSampler[str]]:
    return {key: WeightedSampler.from_mapping(value) for key, value in raw.items()}


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    if not isinstance(document, dict):
        raise ValueError(f"Reference file {path} must contain a mapping")
    return document
