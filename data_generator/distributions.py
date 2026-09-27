"""Sampling helpers that give the ecosystem its skew.

Uniform randomness produces data that looks synthetic the moment anyone groups by
anything. These helpers provide the two shapes that real advertising data has:

* Pareto-style weights for "how much" questions (traffic, budget, children per
  parent) - a small number of advertisers, campaigns and publishers dominate.
* Mean-normalised log-normal multipliers for "how well" questions (CTR, CVR) -
  performance varies widely around a controllable average.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from itertools import accumulate
from typing import Generic, TypeVar

from data_generator.rng import RandomStream

T = TypeVar("T")


class WeightedSampler(Generic[T]):
    """O(log n) weighted sampling with replacement from a fixed population."""

    __slots__ = ("_cum_weights", "_population", "_total")

    def __init__(self, population: Sequence[T], weights: Sequence[float]) -> None:
        if len(population) != len(weights):
            raise ValueError("population and weights must have the same length")
        if not population:
            raise ValueError("cannot build a sampler over an empty population")
        if any(w < 0 for w in weights):
            raise ValueError("weights must be non-negative")
        total = float(sum(weights))
        if total <= 0:
            raise ValueError("weights must sum to a positive value")
        self._population = list(population)
        self._cum_weights = list(accumulate(float(w) for w in weights))
        self._total = total

    @classmethod
    def from_mapping(cls, mapping: Mapping[T, float]) -> WeightedSampler[T]:
        items = [(key, weight) for key, weight in mapping.items() if weight > 0]
        if not items:
            raise ValueError("mapping contains no positive weights")
        return cls([key for key, _ in items], [weight for _, weight in items])

    @property
    def population(self) -> list[T]:
        return self._population

    def pick(self, rng: RandomStream) -> T:
        return rng.choices(self._population, cum_weights=self._cum_weights, k=1)[0]

    def pick_many(self, rng: RandomStream, k: int) -> list[T]:
        if k <= 0:
            return []
        return rng.choices(self._population, cum_weights=self._cum_weights, k=k)

    def __len__(self) -> int:
        return len(self._population)


def allocate_counts(
    total: int,
    weights: Sequence[float],
    *,
    minimum: int = 0,
) -> list[int]:
    """Split `total` across len(weights) buckets proportionally to `weights`.

    Uses the largest-remainder method so the result sums to exactly `total` and is
    fully deterministic (ties broken by index). This is how every parent/child
    fan-out is sized: it guarantees the configured row counts are hit exactly
    while still letting the weights create a long tail.
    """
    n = len(weights)
    if n == 0:
        if total != 0:
            raise ValueError(f"cannot allocate {total} across zero buckets")
        return []
    if total < 0:
        raise ValueError("total must be non-negative")
    if minimum * n > total:
        raise ValueError(
            f"cannot give every one of {n} buckets a minimum of {minimum} from a total of {total}"
        )

    remaining = total - minimum * n
    weight_sum = float(sum(weights))
    if weight_sum <= 0:
        exact = [remaining / n] * n
    else:
        exact = [remaining * (float(w) / weight_sum) for w in weights]

    counts = [int(value) for value in exact]
    shortfall = remaining - sum(counts)
    if shortfall:
        order = sorted(range(n), key=lambda i: (-(exact[i] - counts[i]), i))
        for index in order[:shortfall]:
            counts[index] += 1
    return [count + minimum for count in counts]


def allocate_counts_capped(
    total: int,
    weights: Sequence[float],
    caps: Sequence[int],
) -> list[int]:
    """Weighted allocation that never exceeds a per-bucket ceiling.

    Clicks are allocated across campaigns proportionally to each campaign's CTR
    profile, but a campaign can never receive more clicks than it had
    impressions. Overflow from a capped bucket is redistributed across the
    buckets that still have room, so the total is still met exactly.
    """
    n = len(weights)
    if len(caps) != n:
        raise ValueError("weights and caps must have the same length")
    capacity = sum(caps)
    if total > capacity:
        raise ValueError(f"cannot allocate {total} into buckets with capacity {capacity}")

    counts = [0] * n
    remaining = total
    while remaining > 0:
        open_buckets = [i for i in range(n) if counts[i] < caps[i] and weights[i] > 0]
        if not open_buckets:
            break
        share = allocate_counts(remaining, [weights[i] for i in open_buckets])
        progressed = False
        for position, bucket in enumerate(open_buckets):
            addition = min(share[position], caps[bucket] - counts[bucket])
            if addition > 0:
                counts[bucket] += addition
                remaining -= addition
                progressed = True
        if not progressed:
            # Rounding left nothing to give; hand out single units instead.
            for bucket in open_buckets:
                if remaining == 0:
                    break
                counts[bucket] += 1
                remaining -= 1

    if remaining > 0:
        # Only reachable when every bucket with spare capacity has zero weight.
        for bucket in range(n):
            if remaining == 0:
                break
            addition = min(remaining, caps[bucket] - counts[bucket])
            counts[bucket] += addition
            remaining -= addition

    return counts


def allocate_children(total: int, weights: Sequence[float]) -> list[int]:
    """Allocate children to parents, giving every parent at least one when possible."""
    minimum = 1 if total >= len(weights) else 0
    return allocate_counts(total, weights, minimum=minimum)


def pareto_weights(rng: RandomStream, n: int, alpha: float) -> list[float]:
    """Heavy-tailed weights: a few very large values, many small ones."""
    if n <= 0:
        return []
    return [rng.paretovariate(alpha) for _ in range(n)]


def lognormal_multiplier(rng: RandomStream, sigma: float) -> float:
    """A positive multiplier with an expected value of exactly 1.0.

    Choosing mu = -sigma^2 / 2 keeps E[X] = 1, which is what lets the generator
    vary per-campaign CTR widely without moving the global funnel target.
    """
    if sigma <= 0:
        return 1.0
    return rng.lognormvariate(-0.5 * sigma * sigma, sigma)


def lognormal_around(rng: RandomStream, median: float, sigma: float) -> float:
    """Log-normal draw whose *median* is `median`."""
    if median <= 0:
        raise ValueError("median must be positive")
    if sigma <= 0:
        return median
    return median * math.exp(rng.gauss(0.0, sigma))


def clamp(value: float, low: float, high: float) -> float:
    return low if value < low else high if value > high else value


def beta_in_range(rng: RandomStream, alpha: float, beta: float, low: float, high: float) -> float:
    return low + (high - low) * rng.betavariate(alpha, beta)


def normalised_index(
    mapping: Mapping[str, float], weights: Mapping[str, float]
) -> dict[str, float]:
    """Rescale an index map so its weighted mean is 1.0.

    Placement types have different CTR indices and are not equally common. Without
    this normalisation the blended CTR would drift away from the configured target
    whenever the inventory mix changed.
    """
    total_weight = sum(weights.get(key, 0.0) for key in mapping)
    if total_weight <= 0:
        return dict(mapping)
    weighted_mean = sum(mapping[key] * weights.get(key, 0.0) for key in mapping) / total_weight
    if weighted_mean <= 0:
        return dict(mapping)
    return {key: value / weighted_mean for key, value in mapping.items()}


def weighted_sample_without_replacement(
    rng: RandomStream,
    population: Sequence[T],
    weights: Sequence[float],
    k: int,
) -> list[T]:
    """Pick exactly `k` distinct items, favouring higher weights.

    Implements the Efraimidis-Spirakis algorithm: each item draws a key
    ``u ** (1 / w)`` and the top `k` keys win. This is how clicks are selected from
    a day's impressions - the count is exact (so the configured funnel totals are
    met) while high-performing creatives and placements still win more often.
    """
    n = len(population)
    if k <= 0:
        return []
    if k >= n:
        return list(population)

    keys: list[tuple[float, int]] = []
    for index in range(n):
        weight = weights[index]
        if weight <= 0:
            key = -math.inf
        else:
            u = rng.random()
            # log(u) / w is monotonic in u ** (1 / w) and avoids underflow.
            key = math.log(u) / weight if u > 0.0 else -math.inf
        keys.append((key, index))

    keys.sort(key=lambda item: (-item[0], item[1]))
    return [population[index] for _, index in keys[:k]]


def mean(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0
