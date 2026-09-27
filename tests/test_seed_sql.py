"""The checked-in SQL seed must stay in step with the reference YAML."""

from __future__ import annotations

import re

from data_generator.config import PROJECT_ROOT
from data_generator.reference import ReferenceData
from scripts.render_seed_sql import SEED_PATH, render

SCHEMA_PATH = PROJECT_ROOT / "postgres" / "schema.sql"


def test_seed_sql_is_up_to_date():
    assert SEED_PATH.read_text(encoding="utf-8") == render(), (
        "postgres/seed.sql is stale; run python scripts/render_seed_sql.py"
    )


def test_every_reference_country_is_seeded():
    seed = SEED_PATH.read_text(encoding="utf-8")
    for country in ReferenceData.load().countries:
        assert f"'{country.code}'" in seed
        assert country.name in seed


def test_schema_declares_every_generated_table():
    schema = SCHEMA_PATH.read_text(encoding="utf-8").lower()
    from data_generator.models import TABLE_NAMES

    for table in TABLE_NAMES:
        assert f"create table if not exists {table}" in schema, table


def test_indexes_file_only_touches_event_tables():
    """Master table indexes belong in schema.sql; indexes.sql is applied after load."""
    indexes = (PROJECT_ROOT / "postgres" / "indexes.sql").read_text(encoding="utf-8")
    targets = set(re.findall(r"ON (\w+) \(", indexes))
    assert targets == {"impressions", "clicks", "conversions", "spend_transactions"}
