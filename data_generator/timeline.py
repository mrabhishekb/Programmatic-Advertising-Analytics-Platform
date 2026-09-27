"""Timestamp helpers.

Chronology is a first-class correctness concern in this project: an impression
that predates the campaign it belongs to would quietly poison every downstream
point-in-time join. These helpers make the legal interval explicit at every call
site instead of relying on luck.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

from data_generator.rng import RandomStream

_BUSINESS_HOUR_WEIGHTS = (
    # 00-07 rare, 08-18 the working day, 19-23 tapering off
    0.2,
    0.1,
    0.1,
    0.1,
    0.2,
    0.4,
    1.0,
    2.5,
    6.0,
    9.0,
    10.0,
    9.5,
    7.0,
    8.5,
    9.5,
    9.0,
    7.5,
    5.0,
    3.0,
    1.8,
    1.2,
    0.9,
    0.6,
    0.3,
)


def datetime_between(
    rng: RandomStream,
    start: datetime,
    end: datetime,
    *,
    recency_bias: float = 1.0,
) -> datetime:
    """Uniform (or recency-biased) timestamp in ``[start, end]``.

    ``recency_bias`` > 1 pushes the draw towards ``end``; it is how the generator
    models a platform that has been acquiring advertisers faster over time.
    """
    if end < start:
        raise ValueError(f"end {end} precedes start {start}")
    span = (end - start).total_seconds()
    if span <= 0:
        return start
    u = rng.random()
    if recency_bias != 1.0:
        u = u ** (1.0 / recency_bias)
    return start + timedelta(seconds=span * u)


def date_between(rng: RandomStream, start: date, end: date, *, recency_bias: float = 1.0) -> date:
    if end < start:
        raise ValueError(f"end {end} precedes start {start}")
    span = (end - start).days
    if span <= 0:
        return start
    u = rng.random()
    if recency_bias != 1.0:
        u = u ** (1.0 / recency_bias)
    return start + timedelta(days=int(u * (span + 1)) if u < 1.0 else span)


def business_datetime_between(
    rng: RandomStream,
    start: datetime,
    end: datetime,
    *,
    recency_bias: float = 1.0,
) -> datetime:
    """Like :func:`datetime_between` but with a working-hours time of day.

    Configuration objects (campaigns, line items, creatives) are created by people
    at work, so their ``created_at`` should not be uniformly spread over midnight.
    """
    moment = datetime_between(rng, start, end, recency_bias=recency_bias)
    hour = rng.choices(range(24), weights=_BUSINESS_HOUR_WEIGHTS, k=1)[0]
    candidate = datetime.combine(
        moment.date(), time(hour=hour, minute=rng.randint(0, 59), second=rng.randint(0, 59))
    )
    # Re-clamp: replacing the time of day can push the value outside the interval.
    if candidate < start:
        return start
    if candidate > end:
        return end
    return candidate


def start_of_day(value: date) -> datetime:
    return datetime.combine(value, time.min)


def end_of_day(value: date) -> datetime:
    return datetime.combine(value, time.max).replace(microsecond=0)


def datetime_within_day(rng: RandomStream, day: date, hour: int) -> datetime:
    return datetime.combine(
        day, time(hour=hour, minute=rng.randint(0, 59), second=rng.randint(0, 59))
    ).replace(microsecond=rng.randint(0, 999) * 1000)


def apply_ingestion_lag(rng: RandomStream, event_time: datetime, max_seconds: int) -> datetime:
    """``created_at`` for an event: the moment the platform recorded it."""
    if max_seconds <= 0:
        return event_time
    return event_time + timedelta(seconds=rng.randint(0, max_seconds))


def days_between(start: date, end: date) -> int:
    return (end - start).days
