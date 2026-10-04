"""The parts of the Silver layer that need neither a JVM nor a bucket.

Separate from ``test_spark.py`` so they run in the normal suite: LSN arithmetic
and object-key parsing are where a quiet off-by-one would silently reprocess or
silently skip, and those are the last things that should only be checked when
somebody remembers to start a container.
"""

from __future__ import annotations

from bronze import layout as bronze_layout
from spark import compact, layout


class TestLsnParsing:
    def test_the_postgres_text_form_becomes_the_integer_debezium_reports(self):
        # pg_current_wal_lsn() prints 'BE/9C2DEED8'; the connector reports the
        # same position as a plain integer, and the two have to be comparable.
        assert layout.parse_lsn("BE/9C2DEED8") == (0xBE << 32) + 0x9C2DEED8

    def test_an_integer_passes_through(self):
        assert layout.parse_lsn(818648776096) == 818648776096

    def test_the_high_half_is_not_dropped(self):
        """A naive int(low, 16) would make every position in segment 0 collide."""
        assert layout.parse_lsn("1/0") == 4294967296
        assert layout.parse_lsn("0/1") == 1


class TestReconciledTables:
    def test_it_covers_the_tables_debezium_captures(self):
        from data_generator.models import MASTER_TABLES

        assert set(layout.RECONCILED_TABLES) == set(MASTER_TABLES)

    def test_it_excludes_the_append_only_event_tables(self):
        from data_generator.models import EVENT_TABLES

        assert not set(layout.RECONCILED_TABLES) & set(EVENT_TABLES)


class TestCompactionPlanning:
    def test_a_part_key_round_trips(self):
        key = "cdc/campaigns/dt=2026-10-02/part-p0002-000000000809-000000000832.parquet"

        assert compact.parse_part_key(key) == ("campaigns", "2026-10-02", 2, 809, 832)

    def test_snapshot_keys_are_not_compaction_candidates(self):
        assert compact.parse_part_key("snapshot/run_id=X/campaigns/part-00000.parquet") is None

    def test_the_manifest_is_not_a_compaction_candidate(self):
        assert compact.parse_part_key("snapshot/run_id=X/_manifest.json") is None

    def test_the_merged_name_follows_the_same_convention_as_its_inputs(self):
        """Why compaction does not break Bronze's naming contract: the merged
        object is the one the sink would have written had it flushed once."""
        merged = bronze_layout.cdc_object_key(
            "campaigns", "2026-10-02", topic_partition=0, first_offset=100, last_offset=300
        )

        assert compact.parse_part_key(merged) == ("campaigns", "2026-10-02", 0, 100, 300)
