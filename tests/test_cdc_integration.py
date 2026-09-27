"""End-to-end CDC tests against a running Kafka + Debezium stack.

Skipped automatically when the stack is not up, so `make test` still works with
only PostgreSQL. Bring it up with `make cdc-up && make cdc-register` to include
these.

These tests prove the two claims the phase rests on: that a change made in
PostgreSQL turns up in Kafka carrying both its old and new values, and that the
replication slot is advancing rather than quietly hoarding write-ahead log.
"""

from __future__ import annotations

import json
import time
import uuid
from decimal import Decimal

import pytest

from data_generator.db import DatabaseSettings, connect
from scripts.cdc import CONNECTOR_PATH, CdcError, ConnectClient, load_connector_definition

pytestmark = pytest.mark.cdc

BOOTSTRAP = "localhost:29092"
POLL_TIMEOUT_SECONDS = 30


@pytest.fixture(scope="module")
def definition() -> dict:
    return load_connector_definition(CONNECTOR_PATH)


@pytest.fixture(scope="module")
def connect_client(definition) -> ConnectClient:
    client = ConnectClient()
    try:
        registered = client.list_connectors()
    except CdcError as exc:
        pytest.skip(f"Kafka Connect not reachable: {exc}")
    if definition["name"] not in registered:
        pytest.skip("Connector not registered; run `make cdc-register`")
    return client


@pytest.fixture(scope="module")
def database():
    try:
        connection = connect(DatabaseSettings.from_env(), autocommit=True)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    yield connection
    connection.close()


def consume_until(topic: str, predicate, *, timeout: float = POLL_TIMEOUT_SECONDS):
    """Read `topic` from the beginning until `predicate` matches an event."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(timeout=1.0)
            if message is None or message.error() or message.value() is None:
                continue
            event = json.loads(message.value())
            if predicate(event):
                return event
    finally:
        consumer.close()
    return None


class TestConnectorIsHealthy:
    def test_connector_and_task_are_running(self, connect_client, definition):
        status = connect_client.status(definition["name"])
        assert status["connector"]["state"] == "RUNNING"
        assert status["tasks"], "connector has no tasks"
        for task in status["tasks"]:
            assert task["state"] == "RUNNING", task.get("trace", "")

    def test_replication_slot_exists_and_is_attached(self, database, definition):
        slot_name = definition["config"]["slot.name"]
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot_name,)
            )
            row = cursor.fetchone()
        assert row is not None, f"slot {slot_name} missing"
        assert row[0] is True, "slot exists but nothing is consuming it"

    def test_publication_covers_only_the_captured_tables(self, database, definition):
        expected = {
            name.split(".", 1)[1] for name in definition["config"]["table.include.list"].split(",")
        }
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT tablename FROM pg_publication_tables WHERE pubname = %s",
                (definition["config"]["publication.name"],),
            )
            published = {row[0] for row in cursor.fetchall()}
        assert published == expected

    def test_wal_retention_is_bounded(self, database, definition):
        """A slot that stops advancing will eventually fill the disk."""
        with database.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                FROM pg_replication_slots WHERE slot_name = %s
                """,
                (definition["config"]["slot.name"],),
            )
            retained_bytes = int(cursor.fetchone()[0])
        assert retained_bytes < 2 * 1024**3, (
            f"{retained_bytes / 1024**3:.1f} GB of WAL retained - the slot is not advancing"
        )

    def test_heartbeat_is_ticking(self, database):
        """The heartbeat is what keeps the slot advancing while tables are idle."""
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT beats, EXTRACT(EPOCH FROM (now() - beat_at)) "
                "FROM platform.cdc_heartbeat WHERE id = 1"
            )
            beats, age_seconds = cursor.fetchone()
        assert beats > 0, "the connector has never written a heartbeat"
        assert float(age_seconds) < 300, f"heartbeat is {age_seconds}s stale"


class TestChangesReachKafka:
    def test_update_is_captured_with_before_and_after(self, database, connect_client):
        """The headline claim of the phase: old value and new value, both present."""
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT campaign_id, daily_budget FROM campaigns "
                "WHERE campaign_status = 'ACTIVE' ORDER BY campaign_id LIMIT 1"
            )
            campaign_id, original_budget = cursor.fetchone()

        new_budget = (Decimal(original_budget) + Decimal("1234.56")).quantize(Decimal("0.01"))
        with database.cursor() as cursor:
            cursor.execute(
                "UPDATE campaigns SET daily_budget = %s, updated_at = now() WHERE campaign_id = %s",
                (new_budget, campaign_id),
            )

        event = consume_until(
            "cdc.public.campaigns",
            lambda e: (
                e.get("op") == "u"
                and (e.get("after") or {}).get("campaign_id") == str(campaign_id)
                and Decimal(str((e.get("after") or {}).get("daily_budget", "0"))) == new_budget
            ),
        )
        assert event is not None, "the update never arrived in cdc.public.campaigns"

        assert event["before"] is not None, "REPLICA IDENTITY FULL is not in effect"
        assert Decimal(event["before"]["daily_budget"]) == Decimal(original_budget)
        assert Decimal(event["after"]["daily_budget"]) == new_budget

        with database.cursor() as cursor:
            cursor.execute(
                "UPDATE campaigns SET daily_budget = %s WHERE campaign_id = %s",
                (original_budget, campaign_id),
            )

    def test_event_carries_source_and_transaction_metadata(self, database, connect_client):
        marker = f"CDC probe {uuid.uuid4().hex[:8]}"
        with database.cursor() as cursor:
            cursor.execute("SELECT audience_id FROM audiences ORDER BY audience_id LIMIT 1")
            audience_id = cursor.fetchone()[0]
            cursor.execute(
                "UPDATE audiences SET audience_name = %s, updated_at = now() WHERE audience_id = %s",
                (marker, audience_id),
            )

        event = consume_until(
            "cdc.public.audiences",
            lambda e: (e.get("after") or {}).get("audience_name") == marker,
        )
        assert event is not None

        source = event["source"]
        for field in ("db", "schema", "table", "lsn", "ts_ms", "connector"):
            assert field in source, f"source metadata is missing {field}"
        assert source["table"] == "audiences"
        assert source["schema"] == "public"

        assert event.get("transaction"), "transaction metadata was not emitted"
        assert event["transaction"]["id"]

    def test_insert_is_captured(self, database):
        audience_id = uuid.uuid4()
        with database.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO audiences (audience_id, audience_name, audience_type, min_age,
                                       max_age, gender, country, interest_category,
                                       created_at, updated_at)
                VALUES (%s, 'CDC insert probe', 'INTEREST', 25, 34, 'ALL',
                        'United States', 'Technology and Computing', now(), now())
                """,
                (audience_id,),
            )

        event = consume_until(
            "cdc.public.audiences",
            lambda e: e.get("op") == "c"
            and (e.get("after") or {}).get("audience_id") == str(audience_id),
        )
        assert event is not None, "the insert never arrived"
        assert event["before"] is None
        assert event["after"]["audience_name"] == "CDC insert probe"

        with database.cursor() as cursor:
            cursor.execute("DELETE FROM audiences WHERE audience_id = %s", (audience_id,))

    def test_delete_is_captured_with_the_deleted_row(self, database):
        audience_id = uuid.uuid4()
        with database.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO audiences (audience_id, audience_name, audience_type, min_age,
                                       max_age, gender, country, interest_category,
                                       created_at, updated_at)
                VALUES (%s, 'CDC delete probe', 'INTEREST', 25, 34, 'ALL',
                        'United States', 'Gaming', now(), now())
                """,
                (audience_id,),
            )
            cursor.execute("DELETE FROM audiences WHERE audience_id = %s", (audience_id,))

        event = consume_until(
            "cdc.public.audiences",
            lambda e: e.get("op") == "d"
            and (e.get("before") or {}).get("audience_id") == str(audience_id),
        )
        assert event is not None, "the delete never arrived"
        assert event["after"] is None
        assert event["before"]["audience_name"] == "CDC delete probe"


class TestPayloadFidelity:
    def test_money_survives_the_round_trip_exactly(self, database):
        """decimal.handling.mode=string; the default would base64-encode this."""
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT campaign_id, campaign_budget FROM campaigns ORDER BY campaign_id LIMIT 1"
            )
            campaign_id, budget = cursor.fetchone()
            probe = (Decimal(budget) + Decimal("0.07")).quantize(Decimal("0.01"))
            cursor.execute(
                "UPDATE campaigns SET campaign_budget = %s WHERE campaign_id = %s",
                (probe, campaign_id),
            )

        event = consume_until(
            "cdc.public.campaigns",
            lambda e: (e.get("after") or {}).get("campaign_id") == str(campaign_id)
            and Decimal(str((e.get("after") or {}).get("campaign_budget", "0"))) == probe,
        )
        assert event is not None
        # An exact decimal string, not a float and not base64 bytes.
        assert Decimal(event["after"]["campaign_budget"]) == probe

        with database.cursor() as cursor:
            cursor.execute(
                "UPDATE campaigns SET campaign_budget = %s WHERE campaign_id = %s",
                (budget, campaign_id),
            )

    def test_event_tables_are_not_captured(self):
        """Impressions are append-only; capturing 100M of them would be pointless."""
        from confluent_kafka import Consumer

        consumer = Consumer(
            {"bootstrap.servers": BOOTSTRAP, "group.id": f"test-{uuid.uuid4()}"}
        )
        try:
            topics = set(consumer.list_topics(timeout=15).topics)
        finally:
            consumer.close()
        for table in ("impressions", "clicks", "conversions", "spend_transactions"):
            assert f"cdc.public.{table}" not in topics


class TestBackfillHandover:
    def test_export_refuses_to_run_without_a_slot(self, tmp_path):
        """The ordering rule is enforced in code, not just documented."""
        from scripts.export_snapshot import ExportError, run_export

        with pytest.raises(ExportError, match="does not exist"):
            run_export(
                tables=["advertisers"],
                output_dir=tmp_path,
                slot_name="no_such_slot",
                allow_no_slot=False,
            )

    def test_export_records_the_wal_position_for_the_handover(self, tmp_path, definition):
        """The loader needs to know which change events the files already contain."""
        from scripts.export_snapshot import run_export

        manifest = run_export(
            tables=["advertisers"],
            output_dir=tmp_path,
            slot_name=definition["config"]["slot.name"],
            allow_no_slot=False,
        )
        assert manifest["consistent_snapshot"]["wal_lsn"]
        assert manifest["consistent_snapshot"]["postgres_snapshot_id"]
        assert manifest["replication_slot"]["active"] is True
        assert manifest["tables"][0]["rows"] > 0
        assert (tmp_path / "advertisers.csv.gz").exists()
        assert (tmp_path / "_manifest.json").exists()
