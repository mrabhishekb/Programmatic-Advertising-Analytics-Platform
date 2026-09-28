"""The topic specification is checked without needing a broker.

Topic settings are the kind of thing that is wrong for months before anyone
notices: a compacted transaction topic silently drops every BEGIN record, and a
dimension topic left on 7-day deletion loses change history that nothing will
ever reconstruct. Both are cheap to assert here.

Topics are configured in two places - ``config/kafka.yml`` for topics that
already exist, and ``topic.creation.*`` in the connector for topics Debezium
creates on first write - so these also assert the two agree.
"""

from __future__ import annotations

import pytest

from data_generator.config import PROJECT_ROOT
from data_generator.models import EVENT_TABLES, MASTER_TABLES
from scripts.cdc import CONNECTOR_PATH, load_connector_definition
from scripts.kafka_admin import KAFKA_CONFIG_PATH, KafkaAdminError, load_topic_specs

ONE_DAY_MS = 86_400_000
SEVEN_DAYS_MS = 604_800_000


@pytest.fixture(scope="module")
def specs() -> dict:
    return load_topic_specs(KAFKA_CONFIG_PATH)


@pytest.fixture(scope="module")
def connector() -> dict:
    return load_connector_definition(CONNECTOR_PATH)["config"]


@pytest.fixture(scope="module")
def dimension_specs(specs) -> dict:
    return {name: spec for name, spec in specs.items() if name.startswith("cdc.public.")}


class TestSpecificationLoads:
    def test_the_file_parses_and_declares_topics(self, specs):
        assert specs

    def test_every_topic_has_at_least_one_partition(self, specs):
        assert all(spec.partitions >= 1 for spec in specs.values())

    def test_single_broker_means_nothing_can_be_replicated(self, specs):
        """docker-compose.yml runs one broker, so anything above 1 fails at creation."""
        assert all(spec.replication_factor == 1 for spec in specs.values())

    def test_an_unknown_policy_is_rejected_rather_than_silently_ignored(self, tmp_path):
        path = tmp_path / "kafka.yml"
        path.write_text(
            "replication_factor: 1\npolicies:\n  real: {}\ntopics:\n  t:\n    policy: typo\n",
            encoding="utf-8",
        )
        with pytest.raises(KafkaAdminError, match="unknown policy"):
            load_topic_specs(path)


class TestCaptureScope:
    def test_every_captured_table_has_a_declared_topic(self, specs):
        """Otherwise the topic is created lazily with whatever defaults apply."""
        for table in MASTER_TABLES:
            assert f"cdc.public.{table}" in specs

    def test_no_event_table_has_a_topic(self, specs):
        """The append-only tables travel through the bulk export, not Kafka."""
        for table in EVENT_TABLES:
            assert f"cdc.public.{table}" not in specs


class TestDimensionTopicsKeepState:
    def test_they_are_compacted(self, dimension_specs):
        """Deletion would discard the current state of anything not edited lately."""
        assert dimension_specs
        for name, spec in dimension_specs.items():
            assert spec.config["cleanup.policy"] == "compact", name

    def test_compaction_cannot_touch_recent_history(self, dimension_specs):
        """Phase 10 needs intermediate revisions, which compaction would collapse.

        min.compaction.lag.ms holds every message uncompacted for a window, so
        the full revision history stays readable long enough to be archived.
        """
        for name, spec in dimension_specs.items():
            assert int(spec.config["min.compaction.lag.ms"]) >= SEVEN_DAYS_MS, name

    def test_tombstones_outlive_a_slow_consumer(self, dimension_specs):
        """A consumer that misses the tombstone keeps a row that no longer exists."""
        for name, spec in dimension_specs.items():
            assert int(spec.config["delete.retention.ms"]) >= ONE_DAY_MS, name

    def test_segments_roll_so_the_cleaner_has_something_to_compact(self, dimension_specs):
        """Compaction only runs on closed segments; these topics are quiet enough
        that the active segment would otherwise stay open indefinitely."""
        for name, spec in dimension_specs.items():
            assert 0 < int(spec.config["segment.ms"]) <= SEVEN_DAYS_MS, name

    def test_time_based_deletion_is_disarmed(self, dimension_specs):
        """These topics were created under `delete` and kept their retention.ms.

        It is inert while the policy is pure compaction, but anyone moving them
        to `compact,delete` later would silently re-enable time-based deletion
        of the newest value of a key. -1 means never.
        """
        for name, spec in dimension_specs.items():
            assert int(spec.config["retention.ms"]) == -1, name


class TestTransactionTopicIsNotCompacted:
    def test_it_uses_deletion(self, specs):
        """Debezium writes BEGIN and END under the same key. Compaction keeps only
        the newest, so every BEGIN would vanish and a consumer could no longer
        tell where a transaction started."""
        assert specs["cdc.transaction"].config["cleanup.policy"] == "delete"

    def test_it_retains_at_least_as_long_as_it_takes_to_read_the_changes(self, specs):
        assert int(specs["cdc.transaction"].config["retention.ms"]) >= SEVEN_DAYS_MS


class TestHeartbeatIsCheap:
    def test_it_is_deleted_quickly(self, specs):
        """A beat every 10s is 8,640 messages a day that nothing reads twice."""
        spec = specs["__debezium-heartbeat.cdc"]
        assert spec.config["cleanup.policy"] == "delete"
        assert int(spec.config["retention.ms"]) <= ONE_DAY_MS

    def test_it_is_kept_far_shorter_than_the_change_topics(self, specs):
        heartbeat = int(specs["__debezium-heartbeat.cdc"].config["retention.ms"])
        transaction = int(specs["cdc.transaction"].config["retention.ms"])
        assert heartbeat < transaction


class TestConnectorAgreesWithTheSpecification:
    """Debezium creates a topic on first write, before `kafka-apply` ever sees it.

    If the two disagree, a topic's settings depend on which of them got there
    first, which is exactly the kind of drift this phase exists to remove.
    """

    def test_topic_creation_is_enabled(self, connector):
        assert connector["topic.creation.enable"] == "true"

    def test_the_default_group_matches_the_dimension_policy(self, connector, dimension_specs):
        sample = next(iter(dimension_specs.values()))
        for key in (
            "cleanup.policy",
            "min.compaction.lag.ms",
            "delete.retention.ms",
            "segment.ms",
            "retention.ms",
        ):
            assert connector[f"topic.creation.default.{key}"] == sample.config[key], key

    def test_the_default_partition_count_matches(self, connector, dimension_specs):
        sample = next(iter(dimension_specs.values()))
        assert int(connector["topic.creation.default.partitions"]) == sample.partitions

    def test_the_transaction_group_overrides_the_compacted_default(self, connector, specs):
        groups = connector["topic.creation.groups"].split(",")
        assert "transaction" in groups
        assert connector["topic.creation.transaction.include"] == r"cdc\.transaction"
        assert (
            connector["topic.creation.transaction.cleanup.policy"]
            == specs["cdc.transaction"].config["cleanup.policy"]
        )

    def test_the_heartbeat_group_overrides_the_compacted_default(self, connector, specs):
        groups = connector["topic.creation.groups"].split(",")
        assert "heartbeat" in groups
        spec = specs["__debezium-heartbeat.cdc"]
        assert connector["topic.creation.heartbeat.cleanup.policy"] == spec.config["cleanup.policy"]
        assert connector["topic.creation.heartbeat.retention.ms"] == spec.config["retention.ms"]

    def test_the_producer_compresses(self, connector):
        """Measured with `make kafka-bench`: lz4 and zstd roughly triple producer
        throughput on this payload and cut p99 latency by an order of magnitude."""
        assert connector["producer.override.compression.type"] in {"lz4", "zstd", "snappy", "gzip"}


class TestDocumented:
    def test_the_phase_has_a_document(self):
        assert (PROJECT_ROOT / "docs" / "kafka.md").exists()
