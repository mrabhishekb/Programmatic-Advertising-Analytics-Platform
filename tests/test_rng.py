"""Determinism guarantees of the random streams."""

from __future__ import annotations

from data_generator.rng import RandomStream, derive_seed


class TestDerivedSeeds:
    def test_same_inputs_give_the_same_seed(self):
        assert derive_seed(42, "campaigns", 7) == derive_seed(42, "campaigns", 7)

    def test_different_streams_give_different_seeds(self):
        assert derive_seed(42, "campaigns", 7) != derive_seed(42, "campaigns", 8)
        assert derive_seed(42, "campaigns", 7) != derive_seed(42, "clicks", 7)

    def test_master_seed_changes_everything(self):
        assert derive_seed(42, "campaigns", 7) != derive_seed(43, "campaigns", 7)


class TestRandomStream:
    def test_streams_are_reproducible(self):
        first = [RandomStream(42, "a", 1).random() for _ in range(3)]
        second = [RandomStream(42, "a", 1).random() for _ in range(3)]
        assert first == second

    def test_streams_are_independent_of_each_other(self):
        """Adding a new stream must not shift an existing one."""
        before = RandomStream(42, "advertisers", 5)
        values_before = [before.random() for _ in range(5)]

        unrelated = RandomStream(42, "some-new-feature")
        [unrelated.random() for _ in range(100)]

        after = RandomStream(42, "advertisers", 5)
        assert [after.random() for _ in range(5)] == values_before

    def test_uuids_are_valid_v4_and_reproducible(self):
        first = RandomStream(42, "ids").uuid4()
        second = RandomStream(42, "ids").uuid4()
        assert first == second
        assert first.version == 4
        assert first.variant == "specified in RFC 4122"

    def test_uuids_do_not_repeat_within_a_stream(self):
        stream = RandomStream(42, "ids")
        generated = {stream.uuid4() for _ in range(10_000)}
        assert len(generated) == 10_000

    def test_substreams_are_path_dependent(self):
        parent = RandomStream(42, "campaigns")
        assert parent.substream("a").random() != parent.substream("b").random()
        assert parent.substream("a").random() == parent.substream("a").random()

    def test_substream_is_not_affected_by_parent_draw_order(self):
        parent = RandomStream(42, "campaigns")
        expected = parent.substream("child").random()
        noisy = RandomStream(42, "campaigns")
        [noisy.random() for _ in range(50)]
        assert noisy.substream("child").random() == expected
