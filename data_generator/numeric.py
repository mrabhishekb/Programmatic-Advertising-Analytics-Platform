"""Decimal helpers.

Money and rates are stored as ``DECIMAL`` in PostgreSQL. Generating them as
floats and letting the driver round would make the same seed produce slightly
different stored values depending on platform float formatting, so every value
that lands in a DECIMAL column is quantised here first.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

_Q2 = Decimal("0.01")
_Q4 = Decimal("0.0001")
_Q6 = Decimal("0.000001")


def dec2(value: float | Decimal) -> Decimal:
    """DECIMAL(_,2) - budgets, conversion values."""
    return Decimal(str(value)).quantize(_Q2, rounding=ROUND_HALF_UP)


def dec4(value: float | Decimal) -> Decimal:
    """DECIMAL(_,4) - bids, floor prices, viewability."""
    return Decimal(str(value)).quantize(_Q4, rounding=ROUND_HALF_UP)


def dec6(value: float | Decimal) -> Decimal:
    """DECIMAL(_,6) - auction prices and spend amounts."""
    return Decimal(str(value)).quantize(_Q6, rounding=ROUND_HALF_UP)
