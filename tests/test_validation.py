"""The validator must actually reject bad data.

A validation layer that only ever passes is worse than none, so each test
deliberately corrupts a generated row and asserts the specific check fires.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from data_generator.advertisers import generate_advertisers
from data_generator.campaigns import generate_campaigns
from data_generator.context import GeneratorContext
from data_generator.placements import generate_placements
from data_generator.publishers import generate_publishers
from data_generator.validation import EcosystemValidator, ValidationError
from tests.conftest import build_config


@pytest.fixture(scope="module")
def master_ctx() -> GeneratorContext:
    ctx = GeneratorContext.create(build_config())
    generate_advertisers(ctx)
    generate_publishers(ctx)
    generate_placements(ctx)
    generate_campaigns(ctx)
    return ctx


@pytest.fixture
def validator(master_ctx) -> EcosystemValidator:
    return EcosystemValidator(master_ctx.ecosystem, master_ctx.config)


@pytest.fixture
def key_tracking_validator(master_ctx) -> EcosystemValidator:
    """A validator with primary key tracking forced on.

    The corresponding setting in config/generation.yml is expected to be turned
    off for large runs, so a test of the duplicate check must not depend on it.
    """
    return EcosystemValidator(master_ctx.ecosystem, master_ctx.config, track_primary_keys=True)


def _campaign(master_ctx):
    node = next(iter(master_ctx.ecosystem.campaigns.values()))
    return replace(node.row)


class TestValidatorRejectsCorruptData:
    def test_dangling_foreign_key(self, master_ctx, validator):
        row = _campaign(master_ctx)
        row.advertiser_id = uuid4()
        with pytest.raises(ValidationError, match="fk_advertiser_id"):
            validator.validate_campaigns([row])

    def test_invalid_enum_value(self, master_ctx, validator):
        row = _campaign(master_ctx)
        row.campaign_status = "ON_FIRE"
        with pytest.raises(ValidationError, match="valid_campaign_status"):
            validator.validate_campaigns([row])

    def test_reversed_flight_dates(self, master_ctx, validator):
        row = _campaign(master_ctx)
        row.start_date, row.end_date = row.end_date, row.start_date
        with pytest.raises(ValidationError, match="start_before_end"):
            validator.validate_campaigns([row])

    def test_daily_budget_larger_than_total(self, master_ctx, validator):
        row = _campaign(master_ctx)
        row.daily_budget = row.campaign_budget + Decimal("1.00")
        with pytest.raises(ValidationError, match="daily_budget_within_total"):
            validator.validate_campaigns([row])

    def test_campaign_created_after_its_flight_started(self, master_ctx, validator):
        row = _campaign(master_ctx)
        row.created_at = row.created_at + timedelta(days=3650)
        row.updated_at = row.created_at
        with pytest.raises(ValidationError, match="created_at"):
            validator.validate_campaigns([row])

    def test_duplicate_primary_key(self, master_ctx, key_tracking_validator):
        row = _campaign(master_ctx)
        with pytest.raises(ValidationError, match="primary_key_unique"):
            key_tracking_validator.validate_campaigns([row, replace(row)])

    def test_duplicate_primary_key_is_ignored_when_tracking_is_disabled(self, master_ctx):
        """Disabling the tracker must disable only the tracker.

        Large runs turn this off to save memory and rely on the database's own
        primary key constraint instead, so the other checks have to keep working.
        """
        validator = EcosystemValidator(
            master_ctx.ecosystem, master_ctx.config, track_primary_keys=False
        )
        row = _campaign(master_ctx)
        validator.validate_campaigns([row, replace(row)])
        assert validator.stats.duplicate_keys_found == 0

        # Referential checking is unaffected.
        orphan = _campaign(master_ctx)
        orphan.advertiser_id = uuid4()
        with pytest.raises(ValidationError, match="fk_advertiser_id"):
            validator.validate_campaigns([orphan])

    def test_status_that_contradicts_the_dates(self, master_ctx, validator):
        row = _campaign(master_ctx)
        row.campaign_status = "DRAFT"  # but the flight is in the past
        row.start_date = master_ctx.simulation_end_date - timedelta(days=30)
        row.end_date = master_ctx.simulation_end_date - timedelta(days=1)
        with pytest.raises(ValidationError, match="status_matches_dates"):
            validator.validate_campaigns([row])


class TestValidatorAcceptsGoodData:
    def test_generated_master_data_passes(self, master_ctx):
        validator = EcosystemValidator(master_ctx.ecosystem, master_ctx.config)
        validator.validate_campaigns([node.row for node in master_ctx.ecosystem.campaigns.values()])
        assert validator.stats.rows_validated["campaigns"] == len(master_ctx.ecosystem.campaigns)

    def test_statistics_are_recorded(self, master_ctx):
        validator = EcosystemValidator(master_ctx.ecosystem, master_ctx.config)
        validator.validate_advertisers(
            [node.row for node in master_ctx.ecosystem.advertisers.values()]
        )
        stats = validator.stats.as_dict()
        assert stats["total_checks"] > 0
        assert stats["duplicate_keys_found"] == 0


class TestFullRunIsValidated:
    def test_every_table_was_validated(self, result, config):
        validated = result.validation_stats["rows_validated"]
        for table in ("advertisers", "campaigns", "impressions", "clicks", "conversions"):
            assert validated[table] > 0
        assert validated["impressions"] == config.scale.events.impressions

    def test_millions_of_assertions_run_without_a_violation(self, result):
        assert result.validation_stats["total_checks"] > 10_000
        assert result.validation_stats["duplicate_keys_found"] == 0
