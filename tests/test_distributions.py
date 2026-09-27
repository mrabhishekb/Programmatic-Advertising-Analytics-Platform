"""Tests for the sampling primitives everything else is built on."""

from __future__ import annotations

import pytest

from data_generator.distributions import (
    WeightedSampler,
    allocate_children,
    allocate_counts,
    allocate_counts_capped,
    clamp,
    lognormal_multiplier,
    normalised_index,
    weighted_sample_without_replacement,
)
from data_generator.rng import RandomStream


class TestAllocateCounts:
    def test_sums_to_exactly_the_total(self):
        counts = allocate_counts(1000, [3.0, 1.0, 0.5, 9.25])
        assert sum(counts) == 1000

    def test_respects_weight_ordering(self):
        counts = allocate_counts(1000, [10.0, 1.0])
        assert counts[0] > counts[1]

    def test_honours_a_minimum_per_bucket(self):
        counts = allocate_counts(100, [100.0, 0.0001, 0.0001], minimum=1)
        assert sum(counts) == 100
        assert all(count >= 1 for count in counts)

    def test_rejects_an_impossible_minimum(self):
        with pytest.raises(ValueError, match="minimum"):
            allocate_counts(2, [1.0, 1.0, 1.0], minimum=1)

    def test_handles_zero_total(self):
        assert allocate_counts(0, [1.0, 2.0]) == [0, 0]

    def test_is_deterministic(self):
        weights = [1.5, 2.5, 0.25, 7.0, 3.0]
        assert allocate_counts(997, weights) == allocate_counts(997, weights)

    def test_allocate_children_gives_every_parent_one_when_possible(self):
        counts = allocate_children(10, [5.0, 1.0, 1.0])
        assert sum(counts) == 10
        assert min(counts) >= 1

    def test_allocate_children_allows_zero_when_children_are_scarce(self):
        counts = allocate_children(2, [5.0, 1.0, 1.0])
        assert sum(counts) == 2


class TestAllocateCountsCapped:
    def test_never_exceeds_a_cap(self):
        counts = allocate_counts_capped(100, [100.0, 1.0, 1.0], [10, 50, 50])
        assert sum(counts) == 100
        assert counts[0] <= 10

    def test_redistributes_overflow(self):
        # The first bucket has all the weight but almost no capacity.
        counts = allocate_counts_capped(30, [1000.0, 1.0], [5, 100])
        assert counts == [5, 25]

    def test_rejects_more_than_total_capacity(self):
        with pytest.raises(ValueError, match="capacity"):
            allocate_counts_capped(10, [1.0, 1.0], [2, 3])

    def test_fills_exactly_at_capacity(self):
        counts = allocate_counts_capped(5, [1.0, 1.0], [2, 3])
        assert counts == [2, 3]


class TestWeightedSampler:
    def test_rejects_an_empty_population(self):
        with pytest.raises(ValueError, match="empty population"):
            WeightedSampler([], [])

    def test_rejects_mismatched_lengths(self):
        with pytest.raises(ValueError, match="same length"):
            WeightedSampler(["a"], [1.0, 2.0])

    def test_favours_heavier_items(self):
        sampler = WeightedSampler(["heavy", "light"], [95.0, 5.0])
        rng = RandomStream(1, "sampler-test")
        draws = sampler.pick_many(rng, 2000)
        assert draws.count("heavy") > draws.count("light") * 5

    def test_is_reproducible_for_the_same_stream(self):
        sampler = WeightedSampler(list(range(20)), [float(i + 1) for i in range(20)])
        first = sampler.pick_many(RandomStream(7, "s"), 50)
        second = sampler.pick_many(RandomStream(7, "s"), 50)
        assert first == second

    def test_from_mapping_drops_zero_weights(self):
        sampler = WeightedSampler.from_mapping({"a": 1.0, "b": 0.0})
        assert sampler.population == ["a"]


class TestWeightedSampleWithoutReplacement:
    def test_returns_exactly_k_distinct_items(self):
        population = list(range(100))
        weights = [1.0] * 100
        chosen = weighted_sample_without_replacement(
            RandomStream(3, "wsr"), population, weights, 20
        )
        assert len(chosen) == 20
        assert len(set(chosen)) == 20

    def test_returns_everything_when_k_exceeds_the_population(self):
        chosen = weighted_sample_without_replacement(
            RandomStream(3, "wsr"), [1, 2, 3], [1.0, 1.0, 1.0], 10
        )
        assert sorted(chosen) == [1, 2, 3]

    def test_prefers_heavier_items(self):
        # Ten heavy items among a hundred; picking ten should mostly find them.
        population = list(range(100))
        weights = [100.0 if index < 10 else 1.0 for index in range(100)]
        hits = 0
        for trial in range(20):
            chosen = weighted_sample_without_replacement(
                RandomStream(trial, "wsr"), population, weights, 10
            )
            hits += sum(1 for item in chosen if item < 10)
        assert hits > 150  # out of 200; uniform selection would give ~20

    def test_ignores_zero_weight_items_when_possible(self):
        population = ["a", "b", "c"]
        weights = [1.0, 1.0, 0.0]
        chosen = weighted_sample_without_replacement(RandomStream(5, "wsr"), population, weights, 2)
        assert "c" not in chosen


class TestMultipliers:
    def test_lognormal_multiplier_has_unit_mean(self):
        rng = RandomStream(11, "multiplier")
        draws = [lognormal_multiplier(rng, 0.6) for _ in range(20_000)]
        assert 0.95 < sum(draws) / len(draws) < 1.05

    def test_lognormal_multiplier_actually_varies(self):
        rng = RandomStream(11, "multiplier")
        draws = sorted(lognormal_multiplier(rng, 0.6) for _ in range(1000))
        assert draws[950] / draws[50] > 4  # a wide spread, not noise around 1

    def test_zero_sigma_disables_variation(self):
        assert lognormal_multiplier(RandomStream(1, "x"), 0.0) == 1.0

    def test_normalised_index_has_weighted_mean_of_one(self):
        index = {"a": 2.0, "b": 0.5}
        weights = {"a": 25.0, "b": 75.0}
        normalised = normalised_index(index, weights)
        weighted_mean = (normalised["a"] * 25 + normalised["b"] * 75) / 100
        assert weighted_mean == pytest.approx(1.0)

    def test_clamp(self):
        assert clamp(5.0, 0.0, 1.0) == 1.0
        assert clamp(-5.0, 0.0, 1.0) == 0.0
        assert clamp(0.5, 0.0, 1.0) == 0.5
