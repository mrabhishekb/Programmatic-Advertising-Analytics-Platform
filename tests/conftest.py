"""Shared test fixtures.

The full ecosystem is generated once per session at the ``tiny`` scale and
reused, so the integrity tests all assert against the same dataset.
"""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from data_generator.config import GenerationConfig
from data_generator.generate import GenerationResult, generate
from data_generator.sinks import RowWriter


class CollectingWriter(RowWriter):
    """Keeps every row in memory as a dict, for assertions."""

    def __init__(self) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = {}
        self.order: list[tuple[str, int]] = []

    def write_rows(self, table: str, columns: tuple[str, ...], rows: list[tuple]) -> None:
        bucket = self.tables.setdefault(table, [])
        bucket.extend(dict(zip(columns, row, strict=True)) for row in rows)
        self.order.append((table, len(rows)))

    def digest(self) -> str:
        """Stable fingerprint of the whole dataset, used by the determinism test."""
        hasher = hashlib.sha256()
        for table in sorted(self.tables):
            hasher.update(table.encode())
            for row in self.tables[table]:
                hasher.update(repr(sorted(row.items())).encode())
        return hasher.hexdigest()


def build_config(scale: str = "tiny", seed: int = 42, **overrides: Any) -> GenerationConfig:
    return GenerationConfig.load(scale=scale, seed=seed, overrides=overrides or None)


def run_generation(
    scale: str = "tiny", seed: int = 42
) -> tuple[GenerationResult, CollectingWriter]:
    writer = CollectingWriter()
    result = generate(build_config(scale, seed), writer, validate=True)
    return result, writer


@pytest.fixture(scope="session")
def generated() -> tuple[GenerationResult, CollectingWriter]:
    return run_generation()


@pytest.fixture(scope="session")
def tables(generated) -> dict[str, list[dict[str, Any]]]:
    _, writer = generated
    return writer.tables


@pytest.fixture(scope="session")
def result(generated) -> GenerationResult:
    generation_result, _ = generated
    return generation_result


@pytest.fixture(scope="session")
def config() -> GenerationConfig:
    return build_config()
