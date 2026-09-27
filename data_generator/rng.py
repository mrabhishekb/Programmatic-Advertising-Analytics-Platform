"""Deterministic random streams.

Every part of the generator draws from a named stream whose seed is derived from
the master seed and the stream name. Two consequences matter:

* Adding a new stream later never shifts the output of existing streams, so a
  generator change does not silently invalidate a previously published dataset.
* A campaign's events can be regenerated in isolation (stream name includes the
  campaign index), which is what makes batch-level reproducibility possible.
"""

from __future__ import annotations

import hashlib
import random
import uuid
from collections.abc import Sequence
from typing import TypeVar

T = TypeVar("T")

_SEPARATOR = b"\x1f"


def derive_seed(master_seed: int, *parts: object) -> int:
    """Derive a stable 64-bit sub-seed from the master seed and a stream path."""
    digest = hashlib.blake2b(digest_size=8)
    digest.update(str(master_seed).encode("utf-8"))
    for part in parts:
        digest.update(_SEPARATOR)
        digest.update(str(part).encode("utf-8"))
    return int.from_bytes(digest.digest(), "big")


class RandomStream:
    """A named, independently seeded `random.Random` wrapper."""

    __slots__ = ("_master_seed", "_path", "_rng")

    def __init__(self, master_seed: int, *path: object) -> None:
        self._master_seed = master_seed
        self._path = tuple(str(p) for p in path)
        self._rng = random.Random(derive_seed(master_seed, *path))

    @property
    def path(self) -> tuple[str, ...]:
        return self._path

    def substream(self, *path: object) -> RandomStream:
        """Create a child stream. Independent of draw order in the parent."""
        return RandomStream(self._master_seed, *self._path, *path)

    # -- primitives -------------------------------------------------------

    def random(self) -> float:
        return self._rng.random()

    def randint(self, low: int, high: int) -> int:
        return self._rng.randint(low, high)

    def uniform(self, low: float, high: float) -> float:
        return self._rng.uniform(low, high)

    def gauss(self, mu: float, sigma: float) -> float:
        return self._rng.gauss(mu, sigma)

    def lognormvariate(self, mu: float, sigma: float) -> float:
        return self._rng.lognormvariate(mu, sigma)

    def betavariate(self, alpha: float, beta: float) -> float:
        return self._rng.betavariate(alpha, beta)

    def paretovariate(self, alpha: float) -> float:
        return self._rng.paretovariate(alpha)

    def choice(self, population: Sequence[T]) -> T:
        return self._rng.choice(population)

    def choices(
        self,
        population: Sequence[T],
        weights: Sequence[float] | None = None,
        *,
        cum_weights: Sequence[float] | None = None,
        k: int = 1,
    ) -> list[T]:
        return self._rng.choices(population, weights=weights, cum_weights=cum_weights, k=k)

    def sample(self, population: Sequence[T], k: int) -> list[T]:
        return self._rng.sample(population, k)

    def shuffle(self, population: list[T]) -> None:
        self._rng.shuffle(population)

    def uuid4(self) -> uuid.UUID:
        """A seeded RFC-4122 v4 UUID. Reproducible, unlike `uuid.uuid4()`."""
        return uuid.UUID(int=self._rng.getrandbits(128), version=4)

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"RandomStream(path={'/'.join(self._path)})"
