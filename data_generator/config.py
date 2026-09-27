"""Configuration objects for the generator.

Row counts come from ``config/scales.yml`` and behaviour from
``config/generation.yml``. Nothing about dataset size or ecosystem shape is
hard-coded in the generator itself.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCALES_PATH = PROJECT_ROOT / "config" / "scales.yml"
DEFAULT_GENERATION_PATH = PROJECT_ROOT / "config" / "generation.yml"
REFERENCE_DIR = Path(__file__).resolve().parent / "reference"


class ConfigError(ValueError):
    """Raised when configuration is missing, malformed or internally inconsistent."""


@dataclass(frozen=True, slots=True)
class EntityVolumes:
    advertisers: int
    campaigns: int
    line_items: int
    creatives: int
    publishers: int
    placements: int
    audiences: int

    def as_dict(self) -> dict[str, int]:
        return {
            "advertisers": self.advertisers,
            "campaigns": self.campaigns,
            "line_items": self.line_items,
            "creatives": self.creatives,
            "publishers": self.publishers,
            "placements": self.placements,
            "audiences": self.audiences,
        }


@dataclass(frozen=True, slots=True)
class EventVolumes:
    impressions: int
    clicks: int
    conversions: int

    def as_dict(self) -> dict[str, int]:
        return {
            "impressions": self.impressions,
            "clicks": self.clicks,
            "conversions": self.conversions,
        }


@dataclass(frozen=True, slots=True)
class ScaleProfile:
    name: str
    entities: EntityVolumes
    events: EventVolumes


@dataclass(frozen=True, slots=True)
class TimelineConfig:
    simulation_end_date: date
    advertiser_history_days: int
    campaign_history_days: int
    event_window_days: int
    ingestion_lag_seconds_max: int

    @property
    def simulation_end(self) -> datetime:
        """The simulation "now": midnight at the start of ``simulation_end_date``."""
        return datetime.combine(self.simulation_end_date, datetime.min.time())

    @property
    def event_window_end_date(self) -> date:
        """Last day that can contain events - the last *complete* day before "now"."""
        return self.simulation_end_date - _days(1)

    @property
    def event_window_start_date(self) -> date:
        return self.event_window_end_date - _days(self.event_window_days - 1)


@dataclass(frozen=True, slots=True)
class BatchConfig:
    event_batch_size: int


@dataclass(frozen=True, slots=True)
class EntityShapeConfig:
    campaigns_per_advertiser_alpha: float
    creatives_per_advertiser_alpha: float
    line_items_per_campaign_alpha: float
    placements_per_publisher_alpha: float
    publisher_traffic_alpha: float
    campaign_volume_alpha: float


@dataclass(frozen=True, slots=True)
class FunnelConfig:
    campaign_ctr_sigma: float
    campaign_cvr_sigma: float
    creative_ctr_sigma: float
    min_ctr: float
    max_ctr: float
    min_cvr: float
    max_cvr: float
    click_delay_seconds_median: float
    click_delay_sigma: float
    click_delay_seconds_max: int
    conversion_delay_hours_median: float
    conversion_delay_sigma: float


@dataclass(frozen=True, slots=True)
class AttributionConfig:
    window_hours_choices: tuple[int, ...]
    window_hours_weights: tuple[float, ...]

    @property
    def max_window_hours(self) -> int:
        return max(self.window_hours_choices)


@dataclass(frozen=True, slots=True)
class PricingConfig:
    base_cpm_median: float
    base_cpm_sigma: float
    clearing_price_beta_a: float
    clearing_price_beta_b: float
    floor_price_median: float
    floor_price_sigma: float
    roas_median: float
    roas_sigma: float


@dataclass(frozen=True, slots=True)
class SpendConfig:
    cpm_rollup: str


@dataclass(frozen=True, slots=True)
class ValidationConfig:
    validate_batches: bool
    track_primary_keys: bool
    funnel_tolerance_pct: float


@dataclass(frozen=True, slots=True)
class GenerationConfig:
    seed: int
    scale: ScaleProfile
    timeline: TimelineConfig
    batch: BatchConfig
    entities: EntityShapeConfig
    funnel: FunnelConfig
    attribution: AttributionConfig
    pricing: PricingConfig
    spend: SpendConfig
    validation: ValidationConfig
    reference_dir: Path = REFERENCE_DIR

    # -- loading ----------------------------------------------------------

    @classmethod
    def load(
        cls,
        *,
        scale: str | None = None,
        seed: int | None = None,
        scales_path: Path = DEFAULT_SCALES_PATH,
        generation_path: Path = DEFAULT_GENERATION_PATH,
        overrides: dict[str, Any] | None = None,
    ) -> GenerationConfig:
        scales_doc = _read_yaml(scales_path)
        generation_doc = _read_yaml(generation_path)

        scale_name = scale or os.environ.get("ADTECH_SCALE") or scales_doc.get("default_profile")
        if not scale_name:
            raise ConfigError("No scale profile selected and no default_profile configured")

        profiles = scales_doc.get("profiles") or {}
        if scale_name not in profiles:
            raise ConfigError(
                f"Unknown scale profile {scale_name!r}. Available: {sorted(profiles)}"
            )
        profile = _build_scale_profile(scale_name, profiles[scale_name])

        env_seed = os.environ.get("ADTECH_SEED")
        resolved_seed = seed if seed is not None else int(env_seed) if env_seed else None
        if resolved_seed is None:
            resolved_seed = int(generation_doc.get("seed", 42))

        config = cls(
            seed=resolved_seed,
            scale=profile,
            timeline=_build_timeline(generation_doc.get("timeline", {})),
            batch=BatchConfig(
                event_batch_size=int(
                    _require(generation_doc, "batch", {}).get("event_batch_size", 50_000)
                )
            ),
            entities=_build_dataclass(EntityShapeConfig, generation_doc.get("entities", {})),
            funnel=_build_dataclass(FunnelConfig, generation_doc.get("funnel", {})),
            attribution=_build_attribution(generation_doc.get("attribution", {})),
            pricing=_build_dataclass(PricingConfig, generation_doc.get("pricing", {})),
            spend=_build_dataclass(SpendConfig, generation_doc.get("spend", {})),
            validation=_build_dataclass(ValidationConfig, generation_doc.get("validation", {})),
        )
        if overrides:
            config = replace(config, **overrides)
        config.validate()
        return config

    # -- invariants -------------------------------------------------------

    def validate(self) -> None:
        entities, events = self.scale.entities, self.scale.events
        problems: list[str] = []

        for name, value in {**entities.as_dict(), **events.as_dict()}.items():
            if value < 0:
                problems.append(f"{name} must be non-negative, got {value}")

        if events.clicks > events.impressions:
            problems.append(
                f"clicks ({events.clicks:,}) cannot exceed impressions ({events.impressions:,})"
            )
        if events.conversions > events.clicks:
            problems.append(
                f"conversions ({events.conversions:,}) cannot exceed clicks ({events.clicks:,})"
            )
        if entities.campaigns and not entities.advertisers:
            problems.append("campaigns require at least one advertiser")
        if entities.line_items and not entities.campaigns:
            problems.append("line items require at least one campaign")
        if entities.creatives and not entities.advertisers:
            problems.append("creatives require at least one advertiser")
        if entities.placements and not entities.publishers:
            problems.append("placements require at least one publisher")
        if events.impressions and not (
            entities.campaigns
            and entities.line_items
            and entities.creatives
            and entities.placements
            and entities.audiences
        ):
            problems.append(
                "impressions require campaigns, line items, creatives, placements and audiences"
            )
        if self.timeline.event_window_days < 1:
            problems.append("timeline.event_window_days must be >= 1")
        if self.batch.event_batch_size < 1:
            problems.append("batch.event_batch_size must be >= 1")
        if self.spend.cpm_rollup not in {"hourly", "per_impression"}:
            problems.append(
                f"spend.cpm_rollup must be 'hourly' or 'per_impression', got {self.spend.cpm_rollup!r}"
            )
        if not (0 < self.funnel.min_ctr < self.funnel.max_ctr <= 1):
            problems.append("funnel requires 0 < min_ctr < max_ctr <= 1")
        if not (0 < self.funnel.min_cvr < self.funnel.max_cvr <= 1):
            problems.append("funnel requires 0 < min_cvr < max_cvr <= 1")

        if problems:
            raise ConfigError("Invalid configuration:\n  - " + "\n  - ".join(problems))

    def summary(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "scale": self.scale.name,
            "simulation_end_date": self.timeline.simulation_end_date.isoformat(),
            "event_window_days": self.timeline.event_window_days,
            **self.scale.entities.as_dict(),
            **self.scale.events.as_dict(),
        }


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _days(count: int):
    from datetime import timedelta

    return timedelta(days=count)


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    if not isinstance(document, dict):
        raise ConfigError(f"Configuration file {path} must contain a mapping")
    return document


def _require(document: dict[str, Any], key: str, default: Any) -> Any:
    value = document.get(key, default)
    return default if value is None else value


def _build_scale_profile(name: str, raw: Any) -> ScaleProfile:
    if not isinstance(raw, dict):
        raise ConfigError(f"Scale profile {name!r} must be a mapping")
    try:
        entities = EntityVolumes(**{k: int(v) for k, v in raw["entities"].items()})
        events = EventVolumes(**{k: int(v) for k, v in raw["events"].items()})
    except KeyError as exc:
        raise ConfigError(f"Scale profile {name!r} is missing section {exc}") from exc
    except TypeError as exc:
        raise ConfigError(f"Scale profile {name!r} has unexpected keys: {exc}") from exc
    return ScaleProfile(name=name, entities=entities, events=events)


def _build_timeline(raw: dict[str, Any]) -> TimelineConfig:
    raw_end = raw.get("simulation_end_date")
    if raw_end is None:
        simulation_end = date.today()
    elif isinstance(raw_end, date):
        simulation_end = raw_end
    else:
        simulation_end = date.fromisoformat(str(raw_end))
    return TimelineConfig(
        simulation_end_date=simulation_end,
        advertiser_history_days=int(raw.get("advertiser_history_days", 1095)),
        campaign_history_days=int(raw.get("campaign_history_days", 540)),
        event_window_days=int(raw.get("event_window_days", 90)),
        ingestion_lag_seconds_max=int(raw.get("ingestion_lag_seconds_max", 90)),
    )


def _build_attribution(raw: dict[str, Any]) -> AttributionConfig:
    choices = tuple(int(v) for v in raw.get("window_hours_choices", [168]))
    weights = tuple(float(v) for v in raw.get("window_hours_weights", [1.0] * len(choices)))
    if len(choices) != len(weights):
        raise ConfigError(
            "attribution.window_hours_choices and window_hours_weights differ in length"
        )
    if not choices:
        raise ConfigError("attribution.window_hours_choices must not be empty")
    return AttributionConfig(window_hours_choices=choices, window_hours_weights=weights)


def _build_dataclass(cls: type, raw: dict[str, Any]) -> Any:
    """Instantiate a frozen config dataclass, coercing YAML scalars to field types."""
    import dataclasses

    field_types = {f.name: f.type for f in dataclasses.fields(cls)}
    unknown = set(raw) - set(field_types)
    if unknown:
        raise ConfigError(f"Unknown keys for {cls.__name__}: {sorted(unknown)}")
    missing = set(field_types) - set(raw)
    if missing:
        raise ConfigError(f"Missing keys for {cls.__name__}: {sorted(missing)}")

    coerced: dict[str, Any] = {}
    for name, type_name in field_types.items():
        value = raw[name]
        text = type_name if isinstance(type_name, str) else getattr(type_name, "__name__", "")
        if text == "int":
            coerced[name] = int(value)
        elif text == "float":
            coerced[name] = float(value)
        elif text == "bool":
            coerced[name] = bool(value)
        else:
            coerced[name] = value
    return cls(**coerced)
