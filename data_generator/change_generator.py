"""Mutation driver for the operational tables.

Applies realistic edits to *existing* rows - pausing a campaign, raising a daily
budget, changing a bid, suspending a publisher - as genuine PostgreSQL
``UPDATE``/``INSERT``/``DELETE`` statements. Every change bumps ``updated_at``.

Phase 1 uses this to prove the schema tolerates mutation and to produce the
change history that slowly-changing dimensions will need. The statements are
also exactly what a WAL-based CDC reader would capture when that part of the
platform is built, which is why the mutable tables already run with
``REPLICA IDENTITY FULL``.

Deletes are restricted to leaf rows that nothing references, so referential
integrity is never broken by a change run.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from data_generator.config import PROJECT_ROOT, GenerationConfig
from data_generator.db import DatabaseSettings, connect
from data_generator.logging_setup import configure_logging, get_logger
from data_generator.rng import RandomStream

logger = get_logger(__name__)


@dataclass(slots=True)
class ChangeSummary:
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    by_table: dict[str, int] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.by_table is None:
            self.by_table = {}

    def record(self, table: str, operation: str, count: int = 1) -> None:
        if count <= 0:
            return
        self.by_table[f"{table}.{operation}"] = self.by_table.get(f"{table}.{operation}", 0) + count
        if operation == "insert":
            self.inserts += count
        elif operation == "update":
            self.updates += count
        else:
            self.deletes += count

    def as_dict(self) -> dict[str, Any]:
        return {
            "inserts": self.inserts,
            "updates": self.updates,
            "deletes": self.deletes,
            "by_table": dict(sorted(self.by_table.items())),
        }


def apply_changes(
    connection: Any,
    config: GenerationConfig,
    *,
    batches: int = 1,
    changes_per_batch: int = 50,
) -> ChangeSummary:
    """Apply ``batches`` rounds of ``changes_per_batch`` mutations."""
    summary = ChangeSummary()
    for batch in range(batches):
        rng = RandomStream(config.seed, "changes", batch)
        now = datetime.now()
        _pause_or_resume_campaigns(connection, rng, now, changes_per_batch // 4, summary)
        _adjust_daily_budgets(connection, rng, now, changes_per_batch // 4, summary)
        _retune_line_item_bids(connection, rng, now, changes_per_batch // 4, summary)
        _rotate_creative_status(connection, rng, now, changes_per_batch // 6, summary)
        _suspend_publishers(connection, rng, now, max(changes_per_batch // 12, 1), summary)
        _add_audience_segments(connection, rng, now, max(changes_per_batch // 12, 1), summary)
        _delete_unused_audiences(connection, rng, max(changes_per_batch // 25, 1), summary)
        connection.commit()
        logger.info("applied change batch", extra={"batch": batch, **summary.as_dict()})
    return summary


# ---------------------------------------------------------------------------
# individual change types
# ---------------------------------------------------------------------------


def _pause_or_resume_campaigns(
    connection: Any, rng: RandomStream, now: datetime, count: int, summary: ChangeSummary
) -> None:
    """ACTIVE <-> PAUSED, the most common campaign edit there is."""
    ids = _sample_ids(
        connection, "SELECT campaign_id FROM campaigns WHERE campaign_status = 'ACTIVE'", rng, count
    )
    if ids:
        _execute(
            connection,
            """
            UPDATE campaigns
               SET campaign_status = 'PAUSED', updated_at = %s
             WHERE campaign_id = ANY(%s)
            """,
            (now, ids),
        )
        summary.record("campaigns", "update", len(ids))

    resume = _sample_ids(
        connection,
        "SELECT campaign_id FROM campaigns WHERE campaign_status = 'PAUSED' AND end_date > CURRENT_DATE",
        rng,
        max(count // 2, 1),
    )
    if resume:
        _execute(
            connection,
            "UPDATE campaigns SET campaign_status = 'ACTIVE', updated_at = %s WHERE campaign_id = ANY(%s)",
            (now, resume),
        )
        summary.record("campaigns", "update", len(resume))


def _adjust_daily_budgets(
    connection: Any, rng: RandomStream, now: datetime, count: int, summary: ChangeSummary
) -> None:
    ids = _sample_ids(
        connection,
        "SELECT campaign_id FROM campaigns WHERE campaign_status IN ('ACTIVE','PAUSED')",
        rng,
        count,
    )
    for campaign_id in ids:
        factor = Decimal(str(round(rng.uniform(1.1, 1.8), 3)))
        _execute(
            connection,
            """
            UPDATE campaigns
               SET daily_budget = LEAST(ROUND(daily_budget * %s, 2), campaign_budget),
                   updated_at = %s
             WHERE campaign_id = %s
            """,
            (factor, now, campaign_id),
        )
        summary.record("campaigns", "update")


def _retune_line_item_bids(
    connection: Any, rng: RandomStream, now: datetime, count: int, summary: ChangeSummary
) -> None:
    ids = _sample_ids(
        connection,
        "SELECT line_item_id FROM line_items WHERE line_item_status = 'ACTIVE'",
        rng,
        count,
    )
    for line_item_id in ids:
        factor = Decimal(str(round(rng.uniform(0.8, 1.4), 3)))
        _execute(
            connection,
            """
            UPDATE line_items
               SET bid_amount = GREATEST(ROUND(bid_amount * %s, 4), 0.0001),
                   updated_at = %s
             WHERE line_item_id = %s
            """,
            (factor, now, line_item_id),
        )
        summary.record("line_items", "update")


def _rotate_creative_status(
    connection: Any, rng: RandomStream, now: datetime, count: int, summary: ChangeSummary
) -> None:
    """Pause tired creatives, but never the advertiser's last servable one."""
    ids = _sample_ids(
        connection,
        """
        SELECT c.creative_id
        FROM creatives c
        WHERE c.creative_status = 'ACTIVE'
          AND (SELECT COUNT(*) FROM creatives o
                WHERE o.advertiser_id = c.advertiser_id AND o.creative_status = 'ACTIVE') > 1
        """,
        rng,
        count,
    )
    if ids:
        _execute(
            connection,
            "UPDATE creatives SET creative_status = 'PAUSED', updated_at = %s WHERE creative_id = ANY(%s)",
            (now, ids),
        )
        summary.record("creatives", "update", len(ids))


def _suspend_publishers(
    connection: Any, rng: RandomStream, now: datetime, count: int, summary: ChangeSummary
) -> None:
    ids = _sample_ids(
        connection,
        "SELECT publisher_id FROM publishers WHERE publisher_status = 'ACTIVE'",
        rng,
        count,
    )
    if ids:
        _execute(
            connection,
            "UPDATE publishers SET publisher_status = 'SUSPENDED', updated_at = %s WHERE publisher_id = ANY(%s)",
            (now, ids),
        )
        summary.record("publishers", "update", len(ids))


def _add_audience_segments(
    connection: Any, rng: RandomStream, now: datetime, count: int, summary: ChangeSummary
) -> None:
    """INSERTs, so the change stream is not updates only."""
    from data_generator.reference import ReferenceData

    reference = ReferenceData.load()
    for index in range(count):
        item_rng = rng.substream("audience", index)
        bracket = reference.age_bracket_sampler.pick(item_rng)
        country = reference.country_sampler.pick(item_rng)
        interest = item_rng.choice(reference.interest_categories)
        audience_type = reference.audience_type_sampler.pick(item_rng)
        _execute(
            connection,
            """
            INSERT INTO audiences (
                audience_id, audience_name, audience_type, min_age, max_age, gender,
                country, interest_category, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                item_rng.uuid4(),
                f"New {interest} {audience_type.title()} {bracket.min}-{bracket.max}",
                audience_type,
                bracket.min,
                bracket.max,
                reference.audience_gender_sampler.pick(item_rng),
                country.name,
                interest,
                now,
                now,
            ),
        )
        summary.record("audiences", "insert")


def _delete_unused_audiences(
    connection: Any, rng: RandomStream, count: int, summary: ChangeSummary
) -> None:
    """Only segments that no impression references: deletes must not break FKs."""
    ids = _sample_ids(
        connection,
        """
        SELECT a.audience_id
        FROM audiences a
        LEFT JOIN impressions i ON i.audience_id = a.audience_id
        WHERE i.audience_id IS NULL
        """,
        rng,
        count,
    )
    if ids:
        _execute(connection, "DELETE FROM audiences WHERE audience_id = ANY(%s)", (ids,))
        summary.record("audiences", "delete", len(ids))


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _sample_ids(connection: Any, query: str, rng: RandomStream, count: int) -> list[UUID]:
    if count <= 0:
        return []
    with connection.cursor() as cursor:
        cursor.execute(query)
        candidates = [row[0] for row in cursor.fetchall()]
    if not candidates:
        return []
    # Deterministic selection: sort, then sample with the seeded stream.
    candidates.sort(key=lambda value: value.int if isinstance(value, UUID) else str(value))
    return rng.sample(candidates, min(count, len(candidates)))


def _execute(connection: Any, statement: str, parameters: Sequence[Any]) -> None:
    with connection.cursor() as cursor:
        cursor.execute(statement, parameters)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="adtech-changes",
        description="Apply realistic UPDATE/INSERT/DELETE traffic to the source tables.",
    )
    parser.add_argument("--batches", type=int, default=1)
    parser.add_argument("--changes-per-batch", type=int, default=50)
    parser.add_argument("--scale", help="Scale profile (only used to resolve the seed)")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--report", type=Path, default=PROJECT_ROOT / "artifacts" / "change_report.json"
    )
    args = parser.parse_args(argv)

    configure_logging(args.log_level)
    config = GenerationConfig.load(scale=args.scale, seed=args.seed)

    with connect(DatabaseSettings.from_env()) as connection:
        summary = apply_changes(
            connection, config, batches=args.batches, changes_per_batch=args.changes_per_batch
        )

    import json

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(summary.as_dict(), indent=2), encoding="utf-8")
    print(json.dumps(summary.as_dict(), indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
