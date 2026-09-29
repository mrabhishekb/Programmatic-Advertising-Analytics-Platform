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

``--loop`` keeps applying batches until interrupted, which is what the optional
`traffic` service in docker-compose runs. Continuous runs are always balanced:
every state change is paired with its reverse, because the default profile is
one-way by design and would otherwise end up pausing every campaign and
suspending every publisher.
"""

from __future__ import annotations

import argparse
import math
import signal
import sys
import threading
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

    def absorb(self, other: ChangeSummary) -> None:
        """Fold another summary into this one, for running totals across batches."""
        self.inserts += other.inserts
        self.updates += other.updates
        self.deletes += other.deletes
        for key, value in other.by_table.items():
            self.by_table[key] = self.by_table.get(key, 0) + value

    @property
    def total(self) -> int:
        return self.inserts + self.updates + self.deletes

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
    run_token: str | None = None,
    balanced: bool = False,
    batch_offset: int = 0,
) -> ChangeSummary:
    """Apply ``batches`` rounds of ``changes_per_batch`` mutations.

    ``run_token`` distinguishes one invocation from the next. Updates and deletes
    pick from rows that already exist, so replaying them is harmless, but an
    INSERT mints a new primary key - and a key derived purely from the seed and
    batch number would be identical on every run, so the second run would collide
    with the first. The token is mixed into the insert stream to keep new rows
    new. Pass an explicit value to make a run reproducible.

    ``balanced`` pairs every state change with its reverse, which is what makes
    the generator safe to run continuously. One-shot runs leave it off: the
    default profile is deliberately one-way (campaigns only ever drift towards
    PAUSED, publishers towards SUSPENDED, budgets upwards) because a single
    batch reads as a realistic slice of a working day. Repeat that a few
    thousand times and every publisher ends up suspended and every budget
    pinned at its cap, which is neither realistic nor useful.

    ``batch_offset`` continues the batch numbering across calls, so a long
    running loop keeps drawing from fresh random streams instead of replaying
    batch 0 forever.
    """
    total = ChangeSummary()
    token = run_token or datetime.now().strftime("%Y%m%d%H%M%S%f")
    for batch in range(batch_offset, batch_offset + batches):
        summary = ChangeSummary()
        rng = RandomStream(config.seed, "changes", batch)
        insert_rng = RandomStream(config.seed, "changes", "inserts", token, batch)
        now = datetime.now()
        audience_inserts = max(changes_per_batch // 12, 1)
        # Balanced runs delete as many segments as they add; the one-way profile
        # adds roughly twice what it removes.
        audience_deletes = audience_inserts if balanced else max(changes_per_batch // 25, 1)

        _pause_or_resume_campaigns(
            connection, rng, now, changes_per_batch // 4, summary, balanced=balanced
        )
        _adjust_daily_budgets(
            connection, rng, now, changes_per_batch // 4, summary, balanced=balanced
        )
        _retune_line_item_bids(
            connection, rng, now, changes_per_batch // 4, summary, balanced=balanced
        )
        _rotate_creative_status(connection, rng, now, changes_per_batch // 6, summary)
        _suspend_publishers(connection, rng, now, max(changes_per_batch // 12, 1), summary)
        _add_audience_segments(connection, insert_rng, now, audience_inserts, summary)
        _delete_unused_audiences(connection, rng, audience_deletes, summary)

        if balanced:
            _reactivate_creatives(connection, rng, now, changes_per_batch // 6, summary)
            _reinstate_publishers(connection, rng, now, max(changes_per_batch // 12, 1), summary)

        connection.commit()
        total.absorb(summary)
        logger.info("applied change batch", extra={"batch": batch, **summary.as_dict()})
    return total


# ---------------------------------------------------------------------------
# individual change types
# ---------------------------------------------------------------------------


def _pause_or_resume_campaigns(
    connection: Any,
    rng: RandomStream,
    now: datetime,
    count: int,
    summary: ChangeSummary,
    *,
    balanced: bool = False,
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

    # Resume as many as were paused when balanced, otherwise half - the latter
    # is what makes a one-shot batch drift towards PAUSED.
    resume_count = len(ids) if balanced else max(count // 2, 1)
    resume = _sample_ids(
        connection,
        "SELECT campaign_id FROM campaigns WHERE campaign_status = 'PAUSED' AND end_date > CURRENT_DATE",
        rng,
        resume_count,
    )
    if resume:
        _execute(
            connection,
            "UPDATE campaigns SET campaign_status = 'ACTIVE', updated_at = %s WHERE campaign_id = ANY(%s)",
            (now, resume),
        )
        summary.record("campaigns", "update", len(resume))


def _symmetric_factor(rng: RandomStream, spread: float) -> Decimal:
    """A multiplier centred on 1 in log space, so repeated draws do not ratchet.

    Sampling uniformly from 0.8-1.4 looks balanced but is not: the mean is 1.1,
    so a value multiplied a few thousand times only ever climbs. Drawing the
    *exponent* symmetrically makes a halving exactly as likely as a doubling.
    """
    return Decimal(str(round(math.exp(rng.uniform(-spread, spread)), 3)))


def _adjust_daily_budgets(
    connection: Any,
    rng: RandomStream,
    now: datetime,
    count: int,
    summary: ChangeSummary,
    *,
    balanced: bool = False,
) -> None:
    ids = _sample_ids(
        connection,
        "SELECT campaign_id FROM campaigns WHERE campaign_status IN ('ACTIVE','PAUSED')",
        rng,
        count,
    )
    # A symmetric factor has no drift but its *variance* still grows without
    # bound, so an unclamped walk eventually wanders somewhere absurd. Holding
    # the result inside a band expressed against campaign_budget keeps it
    # stationary. The band brackets the ratio the generator itself produces
    # (daily budgets average around a sixty-sixth of the campaign total), so a
    # value inside it is one the dataset could have started with.
    balanced_statement = """
        UPDATE campaigns
           SET daily_budget = LEAST(
                   GREATEST(ROUND(daily_budget * %s, 2), ROUND(campaign_budget / 200.0, 2)),
                   ROUND(campaign_budget / 20.0, 2)
               ),
               updated_at = %s
         WHERE campaign_id = %s
    """
    one_way_statement = """
        UPDATE campaigns
           SET daily_budget = LEAST(ROUND(daily_budget * %s, 2), campaign_budget),
               updated_at = %s
         WHERE campaign_id = %s
    """

    for campaign_id in ids:
        if balanced:
            factor = _symmetric_factor(rng, 0.3)
            statement = balanced_statement
        else:
            factor = Decimal(str(round(rng.uniform(1.1, 1.8), 3)))
            statement = one_way_statement
        _execute(connection, statement, (factor, now, campaign_id))
        summary.record("campaigns", "update")


def _retune_line_item_bids(
    connection: Any,
    rng: RandomStream,
    now: datetime,
    count: int,
    summary: ChangeSummary,
    *,
    balanced: bool = False,
) -> None:
    ids = _sample_ids(
        connection,
        "SELECT line_item_id FROM line_items WHERE line_item_status = 'ACTIVE'",
        rng,
        count,
    )
    # Clamped for the same reason as daily_budget above, against each line
    # item's own target_cpm. Bids average a little under half the target CPM in
    # the generated data, so 0.2x-0.8x is a band a real bid would sit in.
    balanced_statement = """
        UPDATE line_items
           SET bid_amount = LEAST(
                   GREATEST(ROUND(bid_amount * %s, 4), ROUND(target_cpm * 0.2, 4)),
                   ROUND(target_cpm * 0.8, 4)
               ),
               updated_at = %s
         WHERE line_item_id = %s
    """
    one_way_statement = """
        UPDATE line_items
           SET bid_amount = GREATEST(ROUND(bid_amount * %s, 4), 0.0001),
               updated_at = %s
         WHERE line_item_id = %s
    """

    for line_item_id in ids:
        if balanced:
            factor = _symmetric_factor(rng, 0.25)
            statement = balanced_statement
        else:
            factor = Decimal(str(round(rng.uniform(0.8, 1.4), 3)))
            statement = one_way_statement
        _execute(connection, statement, (factor, now, line_item_id))
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


# ---------------------------------------------------------------------------
# reversals - only used by balanced runs
# ---------------------------------------------------------------------------
# `_rotate_creative_status` and `_suspend_publishers` have no way back on their
# own, so a loop running them would eventually pause every creative and suspend
# every publisher. CLOSED publishers are left alone: that is a terminal state a
# real platform would not silently undo.


def _reinstate_publishers(
    connection: Any, rng: RandomStream, now: datetime, count: int, summary: ChangeSummary
) -> None:
    ids = _sample_ids(
        connection,
        "SELECT publisher_id FROM publishers WHERE publisher_status = 'SUSPENDED'",
        rng,
        count,
    )
    if ids:
        _execute(
            connection,
            "UPDATE publishers SET publisher_status = 'ACTIVE', updated_at = %s WHERE publisher_id = ANY(%s)",
            (now, ids),
        )
        summary.record("publishers", "update", len(ids))


def _reactivate_creatives(
    connection: Any, rng: RandomStream, now: datetime, count: int, summary: ChangeSummary
) -> None:
    ids = _sample_ids(
        connection,
        "SELECT creative_id FROM creatives WHERE creative_status = 'PAUSED'",
        rng,
        count,
    )
    if ids:
        _execute(
            connection,
            "UPDATE creatives SET creative_status = 'ACTIVE', updated_at = %s WHERE creative_id = ANY(%s)",
            (now, ids),
        )
        summary.record("creatives", "update", len(ids))


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


# ---------------------------------------------------------------------------
# continuous mode
# ---------------------------------------------------------------------------

#: Consecutive failed batches before the loop stops trying. A container with a
#: restart policy will bring it back, which also re-resolves DNS and settings -
#: better than spinning forever against a database that has moved.
_MAX_CONSECUTIVE_FAILURES = 5


def run_loop(
    config: GenerationConfig,
    *,
    interval: float,
    changes_per_batch: int,
    run_token: str | None = None,
    max_batches: int = 0,
) -> ChangeSummary:
    """Apply balanced batches until asked to stop.

    Always balanced: an unbalanced loop would pause every campaign and suspend
    every publisher given enough time.

    This runs as the optional `traffic` service in docker-compose, so stopping
    cleanly on SIGTERM matters - Docker sends that first and only kills after a
    grace period. Each batch commits on its own, so a stop between batches
    leaves the database in a consistent state either way.
    """
    stop = threading.Event()

    def request_stop(signum: int, _frame: Any) -> None:
        logger.info("stop requested", extra={"signal": signal.Signals(signum).name})
        stop.set()

    for received in (signal.SIGINT, signal.SIGTERM):
        signal.signal(received, request_stop)

    total = ChangeSummary()
    batch = 0
    failures = 0
    logger.info(
        "traffic loop started",
        extra={"interval_seconds": interval, "changes_per_batch": changes_per_batch},
    )

    def more_to_do() -> bool:
        return not stop.is_set() and (max_batches == 0 or batch < max_batches)

    while more_to_do():
        try:
            with connect(DatabaseSettings.from_env()) as connection:
                while more_to_do():
                    total.absorb(
                        apply_changes(
                            connection,
                            config,
                            batches=1,
                            changes_per_batch=changes_per_batch,
                            run_token=run_token,
                            balanced=True,
                            batch_offset=batch,
                        )
                    )
                    batch += 1
                    failures = 0
                    stop.wait(interval)
        # Broad on purpose: a transient database blip must not end the run.
        except Exception as exc:
            failures += 1
            if failures >= _MAX_CONSECUTIVE_FAILURES:
                logger.error("giving up", extra={"failures": failures, "error": str(exc)})
                raise
            backoff = min(interval, 5.0) * failures
            logger.warning(
                "batch failed, retrying", extra={"error": str(exc), "retry_in_seconds": backoff}
            )
            stop.wait(backoff)

    logger.info("traffic loop stopped", extra={"batches": batch, **total.as_dict()})
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="adtech-changes",
        description="Apply realistic UPDATE/INSERT/DELETE traffic to the source tables.",
    )
    parser.add_argument("--batches", type=int, default=1)
    parser.add_argument("--changes-per-batch", type=int, default=50)
    parser.add_argument(
        "--balanced",
        action="store_true",
        help="pair every state change with its reverse, so repeated runs do not "
        "drift the dataset towards paused campaigns and suspended publishers",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="keep applying batches until interrupted (always balanced)",
    )
    parser.add_argument(
        "--interval", type=float, default=60.0, help="seconds between batches under --loop"
    )
    parser.add_argument(
        "--max-batches", type=int, default=0, help="stop after N batches (0 = until interrupted)"
    )
    parser.add_argument(
        "--run-token",
        help="fixes the identifiers of inserted rows, making the run reproducible "
        "(and therefore only runnable once)",
    )
    parser.add_argument("--scale", help="Scale profile (only used to resolve the seed)")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--report", type=Path, default=PROJECT_ROOT / "artifacts" / "change_report.json"
    )
    parser.add_argument("--no-report", action="store_true", help="do not write the report file")
    args = parser.parse_args(argv)

    configure_logging(args.log_level)
    config = GenerationConfig.load(scale=args.scale, seed=args.seed)

    if args.loop:
        summary = run_loop(
            config,
            interval=args.interval,
            changes_per_batch=args.changes_per_batch,
            run_token=args.run_token,
            max_batches=args.max_batches,
        )
    else:
        with connect(DatabaseSettings.from_env()) as connection:
            summary = apply_changes(
                connection,
                config,
                batches=args.batches,
                changes_per_batch=args.changes_per_batch,
                run_token=args.run_token,
                balanced=args.balanced,
            )

    import json

    payload = json.dumps(summary.as_dict(), indent=2)
    # A loop has no single run to report on, and the traffic container mounts
    # the project read-only, so writing the file would fail there anyway.
    if not (args.no_report or args.loop):
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(payload, encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
