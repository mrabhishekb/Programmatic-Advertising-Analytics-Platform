"""Reproducibility: the same seed must rebuild the same ecosystem."""

from __future__ import annotations

from tests.conftest import run_generation


def test_same_seed_produces_an_identical_dataset():
    _, first = run_generation(seed=42)
    _, second = run_generation(seed=42)
    assert first.digest() == second.digest()


def test_different_seeds_produce_different_datasets():
    _, first = run_generation(seed=42)
    _, second = run_generation(seed=43)
    assert first.digest() != second.digest()


def test_identifiers_are_stable_across_runs():
    _, first = run_generation(seed=7)
    _, second = run_generation(seed=7)
    for table in ("advertisers", "campaigns", "impressions", "clicks", "conversions"):
        left = [tuple(sorted(row.items())) for row in first.tables[table]]
        right = [tuple(sorted(row.items())) for row in second.tables[table]]
        assert left == right, table


def test_row_counts_are_stable_across_seeds():
    """Volumes are configured, so only the content changes when the seed changes."""
    first_result, _ = run_generation(seed=42)
    second_result, _ = run_generation(seed=99)
    assert first_result.row_counts["impressions"] == second_result.row_counts["impressions"]
    assert first_result.row_counts["clicks"] == second_result.row_counts["clicks"]
    assert first_result.row_counts["conversions"] == second_result.row_counts["conversions"]
