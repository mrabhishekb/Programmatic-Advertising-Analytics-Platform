"""The Bronze layer: object layout, record shape, and the sink's guarantees.

Most of this runs without an object store. The parts that need one are marked
``bronze`` and skip when it is not reachable, in the same way the CDC and
PostgreSQL integration tests do.
"""

from __future__ import annotations

import inspect
import json
import time

import pyarrow as pa
import pytest
import yaml

from bronze import layout, records
from bronze import sink as sink_module
from bronze.sink import BronzeSink
from bronze.snapshot import _arrow_type, rows_to_arrow
from bronze.storage import S3Settings
from data_generator.config import PROJECT_ROOT
from data_generator.models import MASTER_TABLES
from scripts.bronze import build_parser


def make_envelope(
    *,
    op: str = "u",
    table: str = "campaigns",
    lsn: int = 818539667648,
    ts_ms: int = 1790549625218,
) -> bytes:
    return json.dumps(
        {
            "before": {"campaign_status": "ACTIVE"},
            "after": {"campaign_status": "PAUSED"},
            "op": op,
            "ts_ms": ts_ms,
            "source": {"schema": "public", "table": table, "lsn": lsn, "txId": 888},
            "transaction": {"id": f"888:{lsn}", "total_order": 17},
        }
    ).encode()


# ---------------------------------------------------------------------------
# layout
# ---------------------------------------------------------------------------


class TestObjectKeysAreDerivedFromOffsets:
    """The key is the idempotency mechanism, not decoration."""

    def test_the_same_kafka_range_always_produces_the_same_key(self):
        """A crash between writing and committing replays the range. Because the
        key is a function of that range, the replay overwrites identical bytes
        instead of adding a second copy under a new name."""
        first = layout.cdc_object_key(
            "campaigns", "2026-09-29", topic_partition=2, first_offset=100, last_offset=199
        )
        second = layout.cdc_object_key(
            "campaigns", "2026-09-29", topic_partition=2, first_offset=100, last_offset=199
        )
        assert first == second

    def test_different_ranges_do_not_collide(self):
        keys = {
            layout.cdc_object_key(
                "campaigns", "2026-09-29", topic_partition=p, first_offset=f, last_offset=f + 9
            )
            for p in (0, 1)
            for f in (0, 10)
        }
        assert len(keys) == 4

    def test_offsets_are_zero_padded_so_keys_sort_chronologically(self):
        """Object stores list lexicographically; unpadded offsets would put
        part-...-1000 before part-...-2."""
        early = layout.cdc_object_key(
            "campaigns", "2026-09-29", topic_partition=0, first_offset=2, last_offset=2
        )
        later = layout.cdc_object_key(
            "campaigns", "2026-09-29", topic_partition=0, first_offset=1000, last_offset=1000
        )
        assert early < later

    def test_partitions_are_hive_style(self):
        """Spark, Athena and Trino all discover partitions from this form."""
        prefix = layout.cdc_partition_prefix("campaigns", "2026-09-29")
        assert prefix == "cdc/campaigns/dt=2026-09-29"

    def test_the_two_producers_live_under_separate_prefixes(self):
        """So a reader can take the snapshot alone or the tail alone."""
        assert layout.CDC_PREFIX != layout.SNAPSHOT_PREFIX
        assert layout.snapshot_object_key("r1", "campaigns").startswith(layout.SNAPSHOT_PREFIX)
        assert layout.cdc_partition_prefix("campaigns", "2026-09-29").startswith(layout.CDC_PREFIX)

    def test_a_hostile_table_name_is_rejected(self):
        with pytest.raises(layout.LayoutError):
            layout.cdc_partition_prefix("../../etc/passwd", "2026-09-29")

    def test_partitioning_uses_event_time_not_arrival_time(self):
        """Otherwise a flush near midnight splits one transaction across two
        partitions, and a replay lands somewhere different again."""
        # 2026-09-27T22:53:45Z
        assert layout.partition_date_of(1790549625218) == "2026-09-27"


# ---------------------------------------------------------------------------
# records
# ---------------------------------------------------------------------------


class TestRecordShapeIsStable:
    def test_payloads_are_stored_as_opaque_json(self):
        """Structs would bind every Bronze file to the source schema of the day
        it was written, so a column added in phase 17 would split the history
        into two incompatible halves."""
        assert records.SCHEMA.field("before_json").type == pa.string()
        assert records.SCHEMA.field("after_json").type == pa.string()

    def test_every_table_produces_the_identical_schema(self):
        built = [
            records.record_from_message(
                topic=f"cdc.public.{table}",
                partition=0,
                offset=1,
                key=b'{"id":"x"}',
                value=make_envelope(table=table),
                ingested_at_ms=1,
            )
            for table in sorted(MASTER_TABLES)
        ]
        assert records.to_arrow(built).schema == records.SCHEMA

    def test_ordering_metadata_survives(self):
        """The LSN is the only total order across tables; losing it would make
        the Silver merge unable to tell which of two updates came last."""
        record = records.record_from_message(
            topic="cdc.public.campaigns",
            partition=0,
            offset=7,
            key=b'{"id":"x"}',
            value=make_envelope(lsn=999),
            ingested_at_ms=1,
        )
        assert record["lsn"] == 999
        assert record["tx_id"] == 888

    def test_provenance_points_back_at_the_exact_message(self):
        record = records.record_from_message(
            topic="cdc.public.campaigns",
            partition=3,
            offset=42,
            key=b'{"id":"x"}',
            value=make_envelope(),
            ingested_at_ms=5,
        )
        assert (record["kafka_topic"], record["kafka_partition"], record["kafka_offset"]) == (
            "cdc.public.campaigns",
            3,
            42,
        )

    def test_a_tombstone_is_recorded_rather_than_dropped(self):
        """Debezium writes a null value after a delete. It carries no payload,
        so it needs its own op code or it becomes indistinguishable noise."""
        record = records.record_from_message(
            topic="cdc.public.audiences",
            partition=0,
            offset=9,
            key=b'{"audience_id":"x"}',
            value=None,
            ingested_at_ms=5,
        )
        assert record["op"] == records.OP_TOMBSTONE
        assert record["source_table"] == "audiences"
        assert record["after_json"] is None

    def test_a_delete_keeps_its_before_image(self):
        """REPLICA IDENTITY FULL exists so phase 10 can see what was removed."""
        envelope = json.loads(make_envelope(op="d"))
        envelope["after"] = None
        record = records.record_from_message(
            topic="cdc.public.campaigns",
            partition=0,
            offset=1,
            key=b'{"id":"x"}',
            value=json.dumps(envelope).encode(),
            ingested_at_ms=1,
        )
        assert record["op"] == "d"
        assert json.loads(record["before_json"]) == {"campaign_status": "ACTIVE"}
        assert record["after_json"] is None

    def test_json_is_canonicalised_so_equal_payloads_compare_equal(self):
        record = records.record_from_message(
            topic="cdc.public.campaigns",
            partition=0,
            offset=1,
            key=None,
            value=b'{"after":{"b":2,"a":1},"op":"c","source":{"table":"campaigns"}}',
            ingested_at_ms=1,
        )
        assert record["after_json"] == '{"a":1,"b":2}'


# ---------------------------------------------------------------------------
# sink buffering
# ---------------------------------------------------------------------------


class TestFlushDefaults:
    """Every flush writes one object per (table, day, Kafka partition) in the
    buffer, so the time trigger sets the floor on how small a file can be."""

    def test_the_cli_and_the_class_agree(self):
        """Two defaults for the same knob drift, and the drift is invisible:
        both values are plausible, so nothing looks wrong."""
        parser = build_parser()
        args = parser.parse_args(["sink"])
        assert args.max_seconds == sink_module.DEFAULT_MAX_SECONDS
        assert args.max_records == sink_module.DEFAULT_MAX_RECORDS

        inspected = inspect.signature(BronzeSink.__init__).parameters
        assert inspected["max_seconds"].default == sink_module.DEFAULT_MAX_SECONDS
        assert inspected["max_records"].default == sink_module.DEFAULT_MAX_RECORDS

    def test_the_interval_is_long_enough_to_produce_usable_files(self):
        """Dimension changes arrive at tens per minute, so the record trigger
        effectively never fires and the timer decides file size. At 30s this
        wrote objects holding one or two records, nearly all footer."""
        assert sink_module.DEFAULT_MAX_SECONDS >= 60

    def test_the_record_trigger_still_caps_a_spike(self):
        """Whichever comes first: a burst must not buffer for the full interval."""
        assert 0 < sink_module.DEFAULT_MAX_RECORDS <= 50_000


class TestSinkGrouping:
    """Buffering needs neither Kafka nor S3, so a placeholder store is enough."""

    def test_records_are_grouped_by_table_date_and_kafka_partition(self):
        sink = BronzeSink(store=object())  # type: ignore[arg-type]
        for offset, (table, ts, part) in enumerate(
            [
                ("campaigns", 1790549625218, 0),  # 2026-09-27
                ("campaigns", 1790549625218, 1),  # same day, other partition
                ("campaigns", 1790722425218, 0),  # different day
                ("audiences", 1790549625218, 0),  # different table
            ]
        ):
            sink.add(
                records.record_from_message(
                    topic=f"cdc.public.{table}",
                    partition=part,
                    offset=offset,
                    key=None,
                    value=make_envelope(table=table, ts_ms=ts),
                    ingested_at_ms=ts,
                )
            )
        # Four inputs that must not share a file, because each would need a
        # different object key.
        assert len(sink._buffer) == 4
        assert sink.buffered == 4

    def test_a_tombstone_falls_back_to_ingestion_time(self):
        """It has no event time, and holding it back to find one would stall
        the flush behind a message that will never arrive."""
        sink = BronzeSink(store=object())  # type: ignore[arg-type]
        sink.add(
            records.record_from_message(
                topic="cdc.public.audiences",
                partition=0,
                offset=1,
                key=b"k",
                value=None,
                ingested_at_ms=1790549625218,
            )
        )
        assert next(iter(sink._buffer))[1] == "2026-09-27"


# ---------------------------------------------------------------------------
# snapshot typing
# ---------------------------------------------------------------------------


class TestSnapshotKeepsTypes:
    def test_money_becomes_an_exact_decimal(self):
        """A float would silently drift; the CDC side keeps decimals as strings
        for the same reason."""
        assert _arrow_type("numeric", 14, 2) == pa.decimal128(14, 2)

    def test_uuids_travel_as_text(self):
        """Parquet has no UUID logical type every reader understands."""
        assert _arrow_type("uuid", None, None) == pa.string()

    def test_timestamps_keep_microseconds(self):
        assert _arrow_type("timestamp without time zone", None, None) == pa.timestamp("us")

    def test_unconstrained_numeric_gets_a_wide_fallback(self):
        """information_schema reports no precision for bare NUMERIC, and Parquet
        decimals must be fixed, so the fallback has to be wide enough."""
        assert _arrow_type("numeric", None, None) == pa.decimal128(38, 9)

    def test_an_empty_table_still_produces_a_typed_file(self):
        """So a reader can tell "no rows" from "never exported"."""
        schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        table = rows_to_arrow([], schema)
        assert table.num_rows == 0
        assert table.schema == schema

    def test_object_columns_are_stringified_against_the_schema(self):
        import uuid as uuid_module

        schema = pa.schema([pa.field("id", pa.string())])
        table = rows_to_arrow([(uuid_module.UUID(int=1),)], schema)
        assert table.column("id")[0].as_py() == str(uuid_module.UUID(int=1))


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


class TestComposeProvidesTheObjectStore:
    def test_the_object_store_is_part_of_the_default_stack(self, compose):
        """Unlike the traffic container, Bronze is not optional."""
        assert "minio" in compose["services"]
        assert not compose["services"]["minio"].get("profiles")

    def test_bronze_data_survives_make_down(self, compose):
        """The Kafka volume was mounted at a path the broker ignored, which is
        exactly the mistake worth not repeating here."""
        mounts = {
            entry.split(":")[1] for entry in compose["services"]["minio"]["volumes"] if ":" in entry
        }
        assert "/data" in mounts, "MinIO serves from /data; anything else is not persisted"
        source = next(
            entry.split(":")[0]
            for entry in compose["services"]["minio"]["volumes"]
            if entry.split(":")[1] == "/data"
        )
        assert source in compose["volumes"]

    def test_the_bucket_is_created_on_startup(self, compose):
        assert "minio-init" in compose["services"]
        entrypoint = compose["services"]["minio-init"]["entrypoint"]
        assert "mb" in entrypoint

    def test_settings_come_from_the_environment(self):
        settings = S3Settings.from_env()
        assert settings.bucket
        assert settings.describe()


class TestTheSinkRunsContinuously:
    """Without a container the pipeline is continuous as far as Kafka and manual
    from there, which is a strange place to stop."""

    def test_it_starts_with_the_rest_of_the_stack(self, compose):
        """Not behind a profile: it only reads Kafka and writes object storage,
        so there is no state it can damage by running."""
        assert not compose["services"]["bronze-sink"].get("profiles")

    def test_it_comes_back_after_a_reboot(self, compose):
        assert compose["services"]["bronze-sink"]["restart"] == "unless-stopped"

    def test_it_never_stops_when_caught_up(self, compose):
        """--idle-timeout 0. The one-shot default would exit after 10s idle,
        leaving the container in a restart loop doing nothing."""
        command = [str(part) for part in compose["services"]["bronze-sink"]["command"]]
        assert "--idle-timeout" in command
        assert command[command.index("--idle-timeout") + 1] == "0"

    def test_it_waits_for_the_bucket_to_exist(self, compose):
        """The sink calls head_bucket on startup and exits if it is missing, so
        "minio is healthy" is not enough - the init container must have run."""
        depends = compose["services"]["bronze-sink"]["depends_on"]
        assert depends["minio-init"]["condition"] == "service_completed_successfully"
        assert depends["kafka"]["condition"] == "service_healthy"

    def test_it_uses_the_internal_kafka_listener(self, compose):
        """29092 is only advertised to the host; inside the network it is 9092."""
        command = [str(part) for part in compose["services"]["bronze-sink"]["command"]]
        bootstrap = command[command.index("--bootstrap") + 1]
        host, _, port = bootstrap.partition(":")
        assert host in compose["services"]
        assert port == "9092"

    def test_it_reaches_the_object_store_by_service_name(self, compose):
        """localhost inside the container is the container, and the remapped
        host port does not exist on the internal network."""
        endpoint = compose["services"]["bronze-sink"]["environment"]["S3_ENDPOINT"]
        assert "localhost" not in endpoint
        assert endpoint == "http://minio:9000"

    def test_it_cannot_write_to_the_project(self, compose):
        mounts = compose["services"]["bronze-sink"]["volumes"]
        assert all(str(mount).endswith(":ro") for mount in mounts), mounts


class TestTheSharedRuntimeImage:
    """Both long-running Python services build from one Dockerfile."""

    def _python_services(self, compose) -> dict:
        return {
            name: service
            for name, service in compose["services"].items()
            if isinstance(service.get("build"), dict)
        }

    def test_they_share_one_dockerfile(self, compose):
        dockerfiles = {
            service["build"]["dockerfile"] for service in self._python_services(compose).values()
        }
        assert len(dockerfiles) == 1, f"drifted into separate images: {dockerfiles}"
        assert (PROJECT_ROOT / next(iter(dockerfiles))).exists()

    def test_every_service_supplies_its_own_full_command(self, compose):
        """The shared image deliberately has no ENTRYPOINT, so a command that
        starts with a bare flag would be executed as a binary and fail at
        startup rather than at build time."""
        for name, service in self._python_services(compose).items():
            command = [str(part) for part in service["command"]]
            assert command[0] == "python", f"{name} relies on an ENTRYPOINT that is not there"


# ---------------------------------------------------------------------------
# against a real object store
# ---------------------------------------------------------------------------


@pytest.mark.bronze
class TestAgainstTheObjectStore:
    @pytest.fixture(scope="module")
    def store(self):
        from bronze.storage import BronzeStorageError, BronzeStore

        candidate = BronzeStore()
        try:
            candidate.ping()
        except BronzeStorageError as exc:  # pragma: no cover - environment dependent
            pytest.skip(f"object store not reachable: {exc}")
        return candidate

    @pytest.fixture
    def scratch_prefix(self, store):
        prefix = f"cdc/_test_{int(time.time() * 1000)}"
        yield prefix
        for key in store.list_keys(prefix):
            store.client.delete_object(Bucket=store.settings.bucket, Key=key)

    def test_a_written_object_reads_back_identically(self, store, scratch_prefix):
        record = records.record_from_message(
            topic="cdc.public.campaigns",
            partition=0,
            offset=1,
            key=b'{"id":"x"}',
            value=make_envelope(),
            ingested_at_ms=1,
        )
        key = f"{scratch_prefix}/part-00000.parquet"
        store.put_table(key, records.to_arrow([record]))

        back = store.read_table(key).to_pylist()
        assert len(back) == 1
        assert back[0]["lsn"] == record["lsn"]
        assert json.loads(back[0]["after_json"]) == {"campaign_status": "PAUSED"}

    def test_rewriting_the_same_key_does_not_duplicate_records(self, store, scratch_prefix):
        """The at-least-once replay path: same Kafka range, same key, same bytes."""
        record = records.record_from_message(
            topic="cdc.public.campaigns",
            partition=0,
            offset=1,
            key=None,
            value=make_envelope(),
            ingested_at_ms=1,
        )
        key = f"{scratch_prefix}/part-00000.parquet"
        store.put_table(key, records.to_arrow([record]))
        store.put_table(key, records.to_arrow([record]))

        assert len(store.list_keys(scratch_prefix)) == 1
        assert store.read_table(key).num_rows == 1
