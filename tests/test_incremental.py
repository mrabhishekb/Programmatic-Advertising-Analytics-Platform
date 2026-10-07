"""What the next run decides to do, and what it reads to do it.

No Spark here. The decision logic takes a state object and a key listing, both
of which are cheap to fabricate, so the interesting cases - a bookmark that
straddles a compacted file, an export that moved underneath a built table - are
ordinary unit tests rather than things that only show up against a live stack.
"""

from __future__ import annotations

import pytest

from spark import incremental
from spark.incremental import RunMode


class FakeStore:
    """Just enough of BronzeStore for the planner: a sorted key listing."""

    def __init__(self, keys: list[str]) -> None:
        self.keys = keys

    def list_keys(self, prefix: str) -> list[str]:
        return [key for key in self.keys if key.startswith(prefix)]


def part(table: str, date: str, partition: int, first: int, last: int) -> str:
    """A CDC object key, built the way the Bronze sink builds them."""
    return f"cdc/{table}/dt={date}/part-p{partition:04d}-{first:012d}-{last:012d}.parquet"


class TestTheOffsetBookmarkRoundTrips:
    def test_a_bookmark_survives_being_written_and_read_back(self):
        offsets = {0: 120, 3: 9}
        assert incremental.decode_offsets(incremental.encode_offsets(offsets)) == offsets

    def test_no_bookmark_means_an_empty_one_not_an_error(self):
        assert incremental.decode_offsets(None) == {}
        assert incremental.decode_offsets("") == {}

    @pytest.mark.parametrize("raw", ["not json", "[1,2]", '{"a":"b"}', "null"])
    def test_an_unreadable_bookmark_degrades_to_reading_everything(self, raw):
        """It is a hint, so losing it costs a re-read. Failing the run would be
        the worse trade: the LSN filter makes the extra reads harmless."""
        assert incremental.decode_offsets(raw) == {}


class TestChoosingWhichBronzeObjectsToRead:
    def test_with_no_bookmark_everything_is_new(self):
        store = FakeStore([part("campaigns", "2026-01-01", 0, 0, 99)])
        keys, offsets = incremental.new_cdc_objects(store, "campaigns", {})
        assert len(keys) == 1
        assert offsets == {0: 99}

    def test_objects_already_read_are_not_read_again(self):
        store = FakeStore(
            [
                part("campaigns", "2026-01-01", 0, 0, 99),
                part("campaigns", "2026-01-02", 0, 100, 199),
            ]
        )
        keys, offsets = incremental.new_cdc_objects(store, "campaigns", {0: 99})
        assert keys == [part("campaigns", "2026-01-02", 0, 100, 199)]
        assert offsets == {0: 199}

    def test_a_file_straddling_the_bookmark_is_read_again(self):
        """Compaction merges small files, so a group can span the bookmark. Read
        it and let the LSN filter drop the part already applied - the
        alternative is skipping the half that has not been."""
        store = FakeStore([part("campaigns", "2026-01-01", 0, 50, 250)])
        keys, _ = incremental.new_cdc_objects(store, "campaigns", {0: 99})
        assert keys == [part("campaigns", "2026-01-01", 0, 50, 250)]

    def test_each_kafka_partition_is_bookmarked_separately(self):
        store = FakeStore(
            [
                part("campaigns", "2026-01-01", 0, 0, 99),
                part("campaigns", "2026-01-01", 1, 0, 99),
            ]
        )
        keys, offsets = incremental.new_cdc_objects(store, "campaigns", {0: 99})
        assert keys == [part("campaigns", "2026-01-01", 1, 0, 99)]
        assert offsets == {0: 99, 1: 99}

    def test_another_tables_changes_are_not_picked_up(self):
        store = FakeStore(
            [
                part("campaigns", "2026-01-01", 0, 0, 99),
                part("advertisers", "2026-01-01", 0, 0, 99),
            ]
        )
        keys, _ = incremental.new_cdc_objects(store, "campaigns", {})
        assert keys == [part("campaigns", "2026-01-01", 0, 0, 99)]

    def test_a_late_arriving_event_is_found_in_an_old_date_partition(self):
        """The reason offsets are the pruning key and ``dt=`` is not: Bronze
        dates a file by when the change happened, so a late event lands behind
        the dates already processed. Its offset is still ahead of them."""
        store = FakeStore(
            [
                part("campaigns", "2026-03-01", 0, 0, 99),
                part("campaigns", "2026-01-01", 0, 100, 109),
            ]
        )
        keys, _ = incremental.new_cdc_objects(store, "campaigns", {0: 99})
        assert keys == [part("campaigns", "2026-01-01", 0, 100, 109)]

    def test_keys_that_are_not_cdc_parts_are_ignored(self):
        store = FakeStore(["cdc/campaigns/dt=2026-01-01/_SUCCESS"])
        keys, offsets = incremental.new_cdc_objects(store, "campaigns", {})
        assert keys == []
        assert offsets == {}


class FakeSpark:
    """A catalog that knows which tables exist and what they remember."""

    def __init__(self, state: dict[str, incremental.TableState]) -> None:
        self._state = state
        self.catalog = self

    def tableExists(self, identifier: str) -> bool:
        return identifier in self._state


@pytest.fixture
def planner(monkeypatch):
    """plan_table with its two Iceberg reads stubbed out."""

    def build(state: dict[str, incremental.TableState]):
        spark = FakeSpark(state)
        monkeypatch.setattr(
            incremental, "read_state", lambda _s, ident: state.get(ident, incremental.TableState())
        )
        return spark

    return build


BUILT = "lake.silver.campaigns"


class TestDecidingWhatOneTableNeeds:
    def test_a_table_that_does_not_exist_is_bootstrapped(self, planner):
        spark = planner({})
        store = FakeStore([part("campaigns", "2026-01-01", 0, 0, 99)])
        plan = incremental.plan_table(
            spark, store, table="campaigns", bronze_run="run-1", bucket="b"
        )
        assert plan.mode is RunMode.BOOTSTRAP
        assert plan.full
        assert len(plan.cdc_urls) == 1

    def test_a_built_table_with_new_changes_is_merged(self, planner):
        state = incremental.TableState(
            exists=True, bronze_run="run-1", applied_lsn=500, offsets={0: 99}
        )
        spark = planner({BUILT: state})
        store = FakeStore(
            [
                part("campaigns", "2026-01-01", 0, 0, 99),
                part("campaigns", "2026-01-02", 0, 100, 199),
            ]
        )
        plan = incremental.plan_table(
            spark, store, table="campaigns", bronze_run="run-1", bucket="b"
        )
        assert plan.mode is RunMode.INCREMENTAL
        assert not plan.full
        assert len(plan.cdc_urls) == 1

    def test_a_built_table_with_nothing_new_is_skipped(self, planner):
        state = incremental.TableState(
            exists=True, bronze_run="run-1", applied_lsn=500, offsets={0: 99}
        )
        spark = planner({BUILT: state})
        store = FakeStore([part("campaigns", "2026-01-01", 0, 0, 99)])
        plan = incremental.plan_table(
            spark, store, table="campaigns", bronze_run="run-1", bucket="b"
        )
        assert plan.mode is RunMode.SKIP
        assert not plan.writes

    def test_a_new_snapshot_export_forces_a_rebuild(self, planner):
        """Merging onto the old base would leave every row nothing has edited
        reflecting an export that is no longer current."""
        state = incremental.TableState(
            exists=True, bronze_run="run-1", applied_lsn=500, offsets={0: 99}
        )
        spark = planner({BUILT: state})
        store = FakeStore([part("campaigns", "2026-01-01", 0, 0, 99)])
        plan = incremental.plan_table(
            spark, store, table="campaigns", bronze_run="run-2", bucket="b"
        )
        assert plan.mode is RunMode.REBUILD
        assert "run-1 -> run-2" in plan.reason

    def test_a_rebuild_replays_the_whole_change_history_not_just_the_new_part(self, planner):
        """It starts from the export again, so pruning by the bookmark would
        drop changes that were already applied to the table being replaced."""
        state = incremental.TableState(
            exists=True, bronze_run="run-1", applied_lsn=500, offsets={0: 99}
        )
        spark = planner({BUILT: state})
        store = FakeStore(
            [
                part("campaigns", "2026-01-01", 0, 0, 99),
                part("campaigns", "2026-01-02", 0, 100, 199),
            ]
        )
        plan = incremental.plan_table(
            spark, store, table="campaigns", bronze_run="run-2", bucket="b"
        )
        assert len(plan.cdc_urls) == 2

    def test_full_overrides_a_table_that_would_otherwise_be_skipped(self, planner):
        state = incremental.TableState(
            exists=True, bronze_run="run-1", applied_lsn=500, offsets={0: 99}
        )
        spark = planner({BUILT: state})
        store = FakeStore([part("campaigns", "2026-01-01", 0, 0, 99)])
        plan = incremental.plan_table(
            spark, store, table="campaigns", bronze_run="run-1", bucket="b", force_full=True
        )
        assert plan.mode is RunMode.REBUILD

    def test_an_event_table_is_skipped_while_its_export_holds(self, planner):
        """Append-only and not captured by Debezium, so a rerun could only
        rewrite six gigabytes into an identical table."""
        state = incremental.TableState(exists=True, bronze_run="run-1")
        spark = planner({"lake.silver.impressions": state})
        plan = incremental.plan_table(
            spark, FakeStore([]), table="impressions", bronze_run="run-1", bucket="b"
        )
        assert plan.mode is RunMode.SKIP

    def test_an_event_table_rebuilds_when_a_new_export_appears(self, planner):
        state = incremental.TableState(exists=True, bronze_run="run-1")
        spark = planner({"lake.silver.impressions": state})
        plan = incremental.plan_table(
            spark, FakeStore([]), table="impressions", bronze_run="run-2", bucket="b"
        )
        assert plan.mode is RunMode.REBUILD
