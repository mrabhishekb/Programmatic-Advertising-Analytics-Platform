"""Integration tests against a real PostgreSQL instance.

Skipped automatically when no database is reachable, so `make test` works
without Docker. Run `make up` first to include them.
"""

from __future__ import annotations

import pytest

from data_generator.config import GenerationConfig
from data_generator.db import DatabaseSettings, connect
from data_generator.models import TABLE_NAMES

pytestmark = pytest.mark.postgres


@pytest.fixture(scope="module")
def connection():
    settings = DatabaseSettings.from_env()
    try:
        conn = connect(settings)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"PostgreSQL not reachable at {settings.describe()}: {exc}")
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def loaded(connection):
    """Skip unless a dataset has actually been generated into the database."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM impressions")
        if cursor.fetchone()[0] == 0:
            pytest.skip("No data loaded; run `make generate` first")
    return connection


class TestSchema:
    def test_every_table_exists(self, connection):
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            present = {row[0] for row in cursor.fetchall()}
        assert set(TABLE_NAMES) <= present

    def test_reference_data_is_seeded(self, connection):
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM ref_country")
            assert cursor.fetchone()[0] > 10

    def test_mutable_tables_are_ready_for_cdc(self, connection):
        """REPLICA IDENTITY FULL on the tables a CDC reader will need a before-image for."""
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT relname, relreplident
                FROM pg_class
                WHERE relname IN ('advertisers','campaigns','line_items','creatives',
                                  'publishers','placements','audiences')
                """
            )
            identities = dict(cursor.fetchall())
        assert identities and all(value == "f" for value in identities.values())

    def test_foreign_keys_are_declared(self, connection):
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.table_constraints
                WHERE constraint_type = 'FOREIGN KEY' AND table_schema = 'public'
                """
            )
            assert cursor.fetchone()[0] >= 20


class TestLoadedData:
    def test_the_data_quality_suite_passes(self, loaded):
        from data_quality.validation import run_data_quality_suite

        report = run_data_quality_suite(loaded, None, GenerationConfig.load())
        failures = [result.check.name for result in report.failures]
        assert not failures, f"failing checks: {failures}"

    def test_the_headline_analytical_join_returns_rows(self, loaded):
        with loaded.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT a.advertiser_name, c.campaign_name
                    FROM advertisers a
                    JOIN campaigns c         ON a.advertiser_id = c.advertiser_id
                    JOIN impressions i       ON c.campaign_id = i.campaign_id
                    LEFT JOIN clicks cl      ON i.impression_id = cl.impression_id
                    LEFT JOIN conversions cv ON cl.click_id = cv.click_id
                    GROUP BY a.advertiser_name, c.campaign_name
                ) grouped
                """
            )
            assert cursor.fetchone()[0] > 0

    def test_the_run_was_recorded(self, loaded):
        with loaded.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM platform.generation_run")
            assert cursor.fetchone()[0] >= 1

    def test_event_indexes_were_applied(self, loaded):
        with loaded.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'impressions'")
            # Primary key plus the analytical indexes from postgres/indexes.sql.
            assert cursor.fetchone()[0] >= 9


class TestConstraintsRejectBadData:
    """The schema is the last line of defence and it has to actually hold."""

    def test_foreign_key_violation_is_rejected(self, loaded):
        from uuid import uuid4

        with (
            pytest.raises(Exception, match=r"foreign key|violates"),
            loaded.transaction(force_rollback=True),
            loaded.cursor() as cursor,
        ):
            cursor.execute(
                "INSERT INTO campaigns (campaign_id, advertiser_id, campaign_name,"
                " campaign_objective, campaign_status, campaign_budget, daily_budget,"
                " start_date, end_date, bid_strategy, created_at, updated_at)"
                " VALUES (%s, %s, 'x', 'TRAFFIC', 'ACTIVE', 10, 1,"
                " '2026-01-01', '2026-02-01', 'CPC', now(), now())",
                (uuid4(), uuid4()),
            )

    def test_invalid_enum_is_rejected(self, loaded):
        from uuid import uuid4

        with (
            pytest.raises(Exception, match=r"check constraint|violates"),
            loaded.transaction(force_rollback=True),
            loaded.cursor() as cursor,
        ):
            cursor.execute(
                "INSERT INTO publishers (publisher_id, publisher_name, publisher_type,"
                " country, domain, publisher_status, created_at, updated_at)"
                " VALUES (%s, 'x', 'CARRIER_PIGEON', 'United States', %s, 'ACTIVE',"
                " now(), now())",
                (uuid4(), f"{uuid4()}.example"),
            )

    def test_cpm_spend_with_an_impression_is_rejected(self, loaded):
        """The billing-type/grain rule is enforced by the database, not just by code."""
        from uuid import uuid4

        with loaded.cursor() as cursor:
            cursor.execute(
                "SELECT impression_id, campaign_id, line_item_id, advertiser_id,"
                " publisher_id FROM impressions LIMIT 1"
            )
            impression = cursor.fetchone()

        with (
            pytest.raises(Exception, match=r"check constraint|violates"),
            loaded.transaction(force_rollback=True),
            loaded.cursor() as cursor,
        ):
            cursor.execute(
                "INSERT INTO spend_transactions (spend_transaction_id, campaign_id,"
                " line_item_id, advertiser_id, publisher_id, impression_id,"
                " spend_timestamp, spend_amount, currency, billing_type, created_at)"
                " VALUES (%s, %s, %s, %s, %s, %s, now(), 1.0, 'USD', 'CPM', now())",
                (
                    uuid4(),
                    impression[1],
                    impression[2],
                    impression[3],
                    impression[4],
                    impression[0],
                ),
            )
