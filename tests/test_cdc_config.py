"""The Debezium connector configuration is checked without needing Kafka.

A wrong value here is expensive to discover at runtime: the wrong
``snapshot.mode`` would spend hours re-reading 100 million rows, and a missing
heartbeat would let the replication slot pin the write-ahead log until the disk
fills. Both are cheap to assert here.
"""

from __future__ import annotations

import json
import re

import pytest
import yaml

from data_generator.config import PROJECT_ROOT
from data_generator.models import EVENT_TABLES, MASTER_TABLES
from scripts.cdc import CONNECTOR_PATH, load_connector_definition


@pytest.fixture(scope="module")
def config() -> dict:
    return load_connector_definition(CONNECTOR_PATH)["config"]


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


class TestConnectorDefinition:
    def test_file_is_valid_json_with_the_required_shape(self):
        definition = json.loads(CONNECTOR_PATH.read_text(encoding="utf-8"))
        assert definition["name"]
        assert isinstance(definition["config"], dict)

    def test_every_config_value_is_a_string(self, config):
        """Kafka Connect rejects non-string values in a connector config."""
        wrong = {key: value for key, value in config.items() if not isinstance(value, str)}
        assert not wrong, f"these must be quoted strings: {sorted(wrong)}"

    def test_uses_the_postgres_connector_with_the_builtin_plugin(self, config):
        assert config["connector.class"].endswith("PostgresConnector")
        # pgoutput ships with PostgreSQL 10+, so no extension has to be installed.
        assert config["plugin.name"] == "pgoutput"


class TestCaptureScope:
    def test_only_the_mutable_tables_are_captured(self, config):
        captured = {name.split(".", 1)[1] for name in config["table.include.list"].split(",")}
        assert captured == MASTER_TABLES

    def test_append_only_event_tables_are_excluded(self, config):
        """Impressions and clicks are never updated, so CDC would add nothing."""
        captured = config["table.include.list"]
        for table in EVENT_TABLES:
            assert f"public.{table}" not in captured

    def test_publication_is_filtered_to_the_captured_tables(self, config):
        assert config["publication.autocreate.mode"] == "filtered"


class TestBackfillHandover:
    def test_existing_rows_are_not_snapshotted(self, config):
        """The 100M existing rows come from scripts/export_snapshot.py instead."""
        assert config["snapshot.mode"] == "no_data"

    def test_topic_names_match_the_documented_scheme(self, config):
        # topic.prefix 'cdc' yields cdc.public.campaigns, cdc.public.advertisers, ...
        assert config["topic.prefix"] == "cdc"
        assert config["schema.include.list"] == "public"

    def test_a_named_slot_is_used_so_it_can_be_monitored_and_dropped(self, config):
        assert config["slot.name"]
        assert config["publication.name"]


class TestOperationalSafety:
    def test_heartbeat_is_enabled(self, config):
        """Without it an idle capture set lets the slot pin WAL until the disk fills."""
        assert int(config["heartbeat.interval.ms"]) > 0

    def test_heartbeat_query_targets_the_heartbeat_table(self, config):
        assert "platform.cdc_heartbeat" in config["heartbeat.action.query"]

    def test_heartbeat_table_is_outside_the_capture_set(self, config):
        """Otherwise the heartbeat would generate change events about itself."""
        assert "cdc_heartbeat" not in config["table.include.list"]


class TestPayloadFidelity:
    def test_decimals_keep_their_exact_value(self, config):
        """The default encodes DECIMAL as base64 bytes, which loses money silently."""
        assert config["decimal.handling.mode"] == "string"

    def test_timestamps_are_readable(self, config):
        assert config["time.precision.mode"] == "connect"

    def test_transaction_metadata_is_emitted(self, config):
        assert config["provide.transaction.metadata"] == "true"

    def test_deletes_emit_a_tombstone(self, config):
        assert config["tombstones.on.delete"] == "true"


class TestSchemaSupportsTheConnector:
    def test_heartbeat_table_exists_in_the_schema(self):
        schema = (PROJECT_ROOT / "postgres" / "schema.sql").read_text(encoding="utf-8")
        assert "platform.cdc_heartbeat" in schema

    def test_captured_tables_have_full_replica_identity(self):
        """REPLICA IDENTITY FULL is what populates the 'before' image of an update."""
        schema = (PROJECT_ROOT / "postgres" / "schema.sql").read_text(encoding="utf-8").lower()
        for table in MASTER_TABLES:
            pattern = rf"alter\s+table\s+{table}\s+replica\s+identity\s+full"
            assert re.search(pattern, schema), table


class TestComposeStack:
    def test_the_cdc_services_are_defined(self, compose):
        assert {"kafka", "kafka-connect", "kafka-ui"} <= set(compose["services"])

    def test_connect_reaches_postgres_by_its_service_name(self, compose, config):
        assert config["database.hostname"] in compose["services"]

    def test_logical_decoding_is_enabled_on_postgres(self, compose):
        """Without wal_level=logical there is nothing for Debezium to read."""
        command = " ".join(str(part) for part in compose["services"]["postgres"]["command"])
        assert "wal_level=logical" in command

    def test_connect_stores_its_offsets_in_kafka(self, compose):
        """Those offsets are what let it resume from the right place after a restart."""
        environment = compose["services"]["kafka-connect"]["environment"]
        for key in ("CONFIG_STORAGE_TOPIC", "OFFSET_STORAGE_TOPIC", "STATUS_STORAGE_TOPIC"):
            assert environment.get(key)
