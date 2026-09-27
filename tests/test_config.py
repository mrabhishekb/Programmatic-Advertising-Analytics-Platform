"""Configuration loading and validation."""

from __future__ import annotations

from dataclasses import replace

import pytest
import yaml

from data_generator.config import (
    ConfigError,
    EntityVolumes,
    EventVolumes,
    GenerationConfig,
    ScaleProfile,
)


class TestLoading:
    def test_loads_the_documented_profiles(self):
        for name in ("tiny", "small", "medium", "large"):
            config = GenerationConfig.load(scale=name, seed=1)
            assert config.scale.name == name
            assert config.scale.entities.advertisers > 0

    def test_small_profile_matches_the_specification(self):
        entities = GenerationConfig.load(scale="small", seed=1).scale.entities
        assert entities.advertisers == 100
        assert entities.campaigns == 1_000
        assert entities.line_items == 3_000
        assert entities.creatives == 5_000
        assert entities.publishers == 500
        assert entities.placements == 2_000
        assert entities.audiences == 1_000

    def test_unknown_profile_is_rejected(self):
        with pytest.raises(ConfigError, match="Unknown scale profile"):
            GenerationConfig.load(scale="enormous")

    def test_explicit_seed_wins_over_the_file(self):
        assert GenerationConfig.load(scale="tiny", seed=999).seed == 999

    def test_scale_can_be_changed_without_touching_code(self, tmp_path):
        """The whole point of config/scales.yml: new sizes need no code change."""
        path = tmp_path / "scales.yml"
        path.write_text(
            yaml.safe_dump(
                {
                    "default_profile": "custom",
                    "profiles": {
                        "custom": {
                            "entities": {
                                "advertisers": 3,
                                "campaigns": 6,
                                "line_items": 9,
                                "creatives": 7,
                                "publishers": 4,
                                "placements": 12,
                                "audiences": 5,
                            },
                            "events": {"impressions": 100, "clicks": 10, "conversions": 2},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        config = GenerationConfig.load(scale="custom", seed=1, scales_path=path)
        assert config.scale.entities.campaigns == 6
        assert config.scale.events.impressions == 100

    def test_event_window_is_the_last_complete_days(self):
        timeline = GenerationConfig.load(scale="tiny", seed=1).timeline
        span = (timeline.event_window_end_date - timeline.event_window_start_date).days + 1
        assert span == timeline.event_window_days
        assert timeline.event_window_end_date < timeline.simulation_end_date


class TestInvariants:
    def _profile(self, **events) -> ScaleProfile:
        defaults = {"impressions": 100, "clicks": 10, "conversions": 1}
        defaults.update(events)
        return ScaleProfile(
            name="broken",
            entities=EntityVolumes(1, 1, 1, 1, 1, 1, 1),
            events=EventVolumes(**defaults),
        )

    def test_more_clicks_than_impressions_is_rejected(self):
        config = GenerationConfig.load(scale="tiny", seed=1)
        broken = replace(config, scale=self._profile(clicks=500))
        with pytest.raises(ConfigError, match="cannot exceed impressions"):
            broken.validate()

    def test_more_conversions_than_clicks_is_rejected(self):
        config = GenerationConfig.load(scale="tiny", seed=1)
        broken = replace(config, scale=self._profile(clicks=10, conversions=50))
        with pytest.raises(ConfigError, match="cannot exceed clicks"):
            broken.validate()

    def test_invalid_funnel_bounds_are_rejected(self):
        config = GenerationConfig.load(scale="tiny", seed=1)
        broken = replace(config, funnel=replace(config.funnel, min_ctr=0.9, max_ctr=0.1))
        with pytest.raises(ConfigError, match="min_ctr < max_ctr"):
            broken.validate()

    def test_unknown_spend_mode_is_rejected(self):
        config = GenerationConfig.load(scale="tiny", seed=1)
        broken = replace(config, spend=replace(config.spend, cpm_rollup="daily"))
        with pytest.raises(ConfigError, match="cpm_rollup"):
            broken.validate()
