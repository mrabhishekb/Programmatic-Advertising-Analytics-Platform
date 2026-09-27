"""Deterministic, readable names.

Names are built from the reference vocabulary and the entity's own random stream.
Where the schema requires uniqueness (``publishers.domain``) the entity index is
folded into the value, so uniqueness is structural rather than hoped for.
"""

from __future__ import annotations

import re

from data_generator.reference import Country, ReferenceData
from data_generator.rng import RandomStream

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def slug(value: str) -> str:
    return _NON_ALNUM.sub("", value.lower())


class NameFactory:
    """Builds entity names from the reference vocabulary."""

    def __init__(self, reference: ReferenceData) -> None:
        naming = reference.naming
        self._brand_prefixes: list[str] = list(naming["brand_prefixes"])
        self._brand_suffixes: list[str] = list(naming["brand_suffixes"])
        self._publisher_prefixes: list[str] = list(naming["publisher_prefixes"])
        self._publisher_roots: list[str] = list(naming["publisher_roots"])
        self._publisher_topics: list[str] = list(naming["publisher_topics"])
        self._app_suffixes: list[str] = list(naming["app_suffixes"])
        self._ctv_suffixes: list[str] = list(naming["ctv_suffixes"])
        self._audio_suffixes: list[str] = list(naming["audio_suffixes"])
        self._tlds_by_region: dict[str, list[str]] = {
            key: list(value) for key, value in naming["domain_tlds_by_region"].items()
        }
        self._campaign_themes: list[str] = list(naming["campaign_themes"])
        self._line_item_qualifiers: list[str] = list(naming["line_item_qualifiers"])
        self._creative_themes: list[str] = list(naming["creative_themes"])
        self._audience_descriptors: list[str] = list(naming["audience_descriptors"])

    def advertiser_name(self, rng: RandomStream, industry: str) -> str:
        prefix = rng.choice(self._brand_prefixes)
        suffix = rng.choice(self._brand_suffixes)
        if rng.random() < 0.25:
            return f"{prefix} {industry.split()[0]} {suffix}"
        return f"{prefix} {suffix}"

    def publisher_name(self, rng: RandomStream, publisher_type: str) -> str:
        topic = rng.choice(self._publisher_topics)
        if publisher_type == "MOBILE_APP":
            return f"{topic}{rng.choice(self._app_suffixes)}"
        if publisher_type == "CTV":
            return f"{topic} {rng.choice(self._ctv_suffixes)}"
        if publisher_type == "AUDIO":
            return f"{topic} {rng.choice(self._audio_suffixes)}"
        return f"{rng.choice(self._publisher_prefixes)} {topic} {rng.choice(self._publisher_roots)}"

    def publisher_domain(
        self, rng: RandomStream, publisher_name: str, country: Country, index: int
    ) -> str:
        tld = rng.choice(self._tlds_by_region.get(country.region, [".com"]))
        # The index guarantees the UNIQUE constraint on publishers.domain holds.
        return f"{slug(publisher_name)}{index}{tld}"

    def campaign_name(self, rng: RandomStream, objective: str, year: int) -> str:
        theme = rng.choice(self._campaign_themes)
        objective_label = objective.replace("_", " ").title()
        return f"{theme} {year} - {objective_label}"

    def line_item_name(self, rng: RandomStream, campaign_name: str, ordinal: int) -> str:
        qualifier = rng.choice(self._line_item_qualifiers)
        short = campaign_name.split(" - ")[0]
        return f"{short} | {qualifier} {ordinal:02d}"

    def creative_name(
        self, rng: RandomStream, brand: str, creative_type: str, creative_format: str
    ) -> str:
        theme = rng.choice(self._creative_themes)
        return f"{brand} {theme} {creative_type.title()} {creative_format}"

    def landing_page_url(self, rng: RandomStream, brand: str, campaign_theme: str) -> str:
        return f"https://www.{slug(brand)}.com/{slug(campaign_theme)}?utm_source=programmatic"

    def audience_name(
        self, rng: RandomStream, audience_type: str, interest: str, min_age: int, max_age: int
    ) -> str:
        descriptor = rng.choice(self._audience_descriptors)
        label = audience_type.title()
        return f"{descriptor} {interest} {label} {min_age}-{max_age}"
