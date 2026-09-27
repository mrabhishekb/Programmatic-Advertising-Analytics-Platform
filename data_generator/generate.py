"""Generator entry point.

Runs the full pipeline: build the ecosystem in dependency order, validate every
batch in process, stream it into the chosen sink, then run the SQL-level
referential, join and data quality checks against the loaded database.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import UUID

from data_generator import __version__
from data_generator.advertisers import generate_advertisers
from data_generator.audiences import generate_audiences
from data_generator.campaigns import generate_campaigns
from data_generator.config import PROJECT_ROOT, GenerationConfig
from data_generator.context import GeneratorContext
from data_generator.creatives import generate_creatives
from data_generator.db import DatabaseSettings, connect
from data_generator.events import EventTotals, generate_events
from data_generator.line_items import generate_line_items
from data_generator.logging_setup import configure_logging, get_logger
from data_generator.models import TABLE_NAMES
from data_generator.placements import generate_placements
from data_generator.publishers import generate_publishers
from data_generator.reporting import render_generation_report
from data_generator.rng import RandomStream
from data_generator.sinks import (
    BatchedEmitter,
    CsvWriter,
    NullWriter,
    PostgresCopyWriter,
    RowWriter,
)
from data_generator.validation import EcosystemValidator

logger = get_logger(__name__)

#: Master entities in dependency order. Publishers and placements come before
#: campaigns because creative types are restricted to inventory that exists.
_MASTER_STAGES = (
    ("advertisers", generate_advertisers, "validate_advertisers"),
    ("publishers", generate_publishers, "validate_publishers"),
    ("placements", generate_placements, "validate_placements"),
    ("campaigns", generate_campaigns, "validate_campaigns"),
    ("line_items", generate_line_items, "validate_line_items"),
    ("creatives", generate_creatives, "validate_creatives"),
    ("audiences", generate_audiences, "validate_audiences"),
)


@dataclass(slots=True)
class GenerationResult:
    run_id: UUID
    config: GenerationConfig
    row_counts: dict[str, int]
    events: EventTotals
    validation_stats: dict[str, Any]
    stage_durations: dict[str, float] = field(default_factory=dict)
    duration_seconds: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id),
            "generator_version": __version__,
            "config": self.config.summary(),
            "row_counts": self.row_counts,
            "events": self.events.as_dict(),
            "validation": self.validation_stats,
            "stage_durations_seconds": {
                key: round(value, 3) for key, value in self.stage_durations.items()
            },
            "duration_seconds": round(self.duration_seconds, 3),
        }


def generate(
    config: GenerationConfig,
    writer: RowWriter,
    *,
    validate: bool = True,
) -> GenerationResult:
    """Generate one complete ecosystem into ``writer``."""
    started = perf_counter()
    ctx = GeneratorContext.create(config)
    validator = EcosystemValidator(ctx.ecosystem, config) if validate else None
    emitter = BatchedEmitter(writer, config.batch.event_batch_size)
    run_id = RandomStream(config.seed, "run", config.scale.name).uuid4()

    logger.info("starting generation", extra={"run_id": str(run_id), **config.summary()})

    stage_durations: dict[str, float] = {}
    for table, generate_stage, validator_method in _MASTER_STAGES:
        stage_started = perf_counter()
        rows = generate_stage(ctx)
        if validator is not None:
            getattr(validator, validator_method)(rows)
        emitter.emit(rows)
        emitter.flush()
        stage_durations[table] = perf_counter() - stage_started
        logger.info(
            "generated master table",
            extra={"table": table, "rows": len(rows), "seconds": round(stage_durations[table], 2)},
        )

    events = generate_events(ctx, emitter, validator)
    stage_durations["events"] = events.duration_seconds
    emitter.close()

    row_counts = {table: emitter.counts[table] for table in TABLE_NAMES}
    result = GenerationResult(
        run_id=run_id,
        config=config,
        row_counts=row_counts,
        events=events,
        validation_stats=validator.stats.as_dict() if validator else {"skipped": True},
        stage_durations=stage_durations,
        duration_seconds=perf_counter() - started,
    )
    logger.info(
        "generation complete", extra={"seconds": round(result.duration_seconds, 2), **row_counts}
    )
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="adtech-generate",
        description="Generate a relationally coherent synthetic AdTech ecosystem.",
    )
    parser.add_argument(
        "--scale",
        help="Dataset size profile from config/scales.yml (default: ADTECH_SCALE or the file default)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Master random seed. The same seed reproduces the same ecosystem.",
    )
    parser.add_argument(
        "--target",
        choices=("postgres", "csv", "none"),
        default="postgres",
        help="Where to write the generated rows (default: postgres)",
    )
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=PROJECT_ROOT / "out" / "csv",
        help="Output directory when --target csv",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Keep existing rows. By default a postgres run truncates first so it is repeatable.",
    )
    parser.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Do not apply postgres/indexes.sql after loading",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip in-process validation (not recommended; the database still enforces its constraints)",
    )
    parser.add_argument(
        "--skip-data-quality",
        action="store_true",
        help="Skip the SQL data quality and join validation suite",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=PROJECT_ROOT / "artifacts" / "generation_report.json",
        help="Where to write the machine-readable run report",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)

    config = GenerationConfig.load(scale=args.scale, seed=args.seed)

    if args.target == "postgres":
        return _run_postgres(args, config)
    if args.target == "csv":
        with CsvWriter(args.csv_dir) as writer:
            result = generate(config, writer, validate=not args.skip_validation)
        _finish(args, result, dq_report=None)
        return 0

    result = generate(config, NullWriter(), validate=not args.skip_validation)
    _finish(args, result, dq_report=None)
    return 0


def _run_postgres(args: argparse.Namespace, config: GenerationConfig) -> int:
    from data_quality.validation import run_data_quality_suite

    settings = DatabaseSettings.from_env()
    logger.info("connecting to postgres", extra={"target": settings.describe()})

    with connect(settings) as connection:
        if not args.no_truncate:
            _truncate(connection)
        writer = PostgresCopyWriter(connection)
        result = generate(config, writer, validate=not args.skip_validation)

        if not args.skip_indexes:
            _apply_sql_file(connection, PROJECT_ROOT / "postgres" / "indexes.sql")
        _record_run(connection, result)
        connection.commit()

        report = None
        if not args.skip_data_quality:
            report = run_data_quality_suite(connection, result.run_id, config)
            connection.commit()

    _finish(args, result, report)
    return 0 if report is None or report.passed else 1


def _truncate(connection: Any) -> None:
    """Make a postgres run repeatable: same seed in, same database state out."""
    statement = "TRUNCATE TABLE " + ", ".join(TABLE_NAMES) + " RESTART IDENTITY CASCADE"
    with connection.cursor() as cursor:
        cursor.execute(statement)
    logger.info("truncated source tables", extra={"tables": len(TABLE_NAMES)})


def _apply_sql_file(connection: Any, path: Path) -> None:
    started = perf_counter()
    with connection.cursor() as cursor:
        cursor.execute(path.read_text(encoding="utf-8"))
    connection.commit()
    logger.info(
        "applied sql file", extra={"file": path.name, "seconds": round(perf_counter() - started, 2)}
    )


def _record_run(connection: Any, result: GenerationResult) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO platform.generation_run (
                run_id, scale_profile, seed, simulation_end, started_at, finished_at,
                duration_seconds, row_counts, validation_status, generator_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                finished_at = EXCLUDED.finished_at,
                duration_seconds = EXCLUDED.duration_seconds,
                row_counts = EXCLUDED.row_counts,
                validation_status = EXCLUDED.validation_status
            """,
            (
                result.run_id,
                result.config.scale.name,
                result.config.seed,
                result.config.timeline.simulation_end_date,
                datetime.now(),
                datetime.now(),
                result.duration_seconds,
                json.dumps(result.row_counts),
                "PASS",
                __version__,
            ),
        )


def _finish(args: argparse.Namespace, result: GenerationResult, dq_report: Any) -> None:
    payload = result.as_dict()
    if dq_report is not None:
        payload["data_quality"] = dq_report.as_dict()

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    print(render_generation_report(result, dq_report))
    print(f"\nMachine-readable report written to {args.report}")


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
