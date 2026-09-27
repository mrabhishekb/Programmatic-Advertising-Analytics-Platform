"""Data quality suite.

Checks are declared as annotated SQL in ``data_quality/tests/*.sql`` rather than
embedded in Python. That keeps them readable on their own, reviewable by anyone
who reads SQL, and reusable by the warehouse layers added in later phases.

Each check returns a single number - the count of violating rows for a check
that expects zero, or a row count for a join check that expects a non-zero
result. Results are printed and persisted to ``platform.data_quality_result``.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import UUID

from data_generator.config import PROJECT_ROOT, GenerationConfig
from data_generator.db import DatabaseSettings, connect
from data_generator.logging_setup import configure_logging, get_logger
from data_generator.reporting import safe_divide

logger = get_logger(__name__)

TESTS_DIR = Path(__file__).resolve().parent / "tests"
_METADATA_LINE = re.compile(r"^--\s*([a-z_]+):\s*(.+?)\s*$")
_REQUIRED_KEYS = ("name", "type", "table", "severity", "expect", "description")
_WIDTH = 72


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    type: str
    table: str
    severity: str
    expect: str
    description: str
    sql: str
    source: str

    def evaluate(self, observed: int) -> str:
        satisfied = observed == 0 if self.expect == "zero" else observed > 0
        if satisfied:
            return "PASS"
        return "FAIL" if self.severity == "ERROR" else "WARN"


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: Check
    observed: int
    status: str
    duration_seconds: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.check.name,
            "type": self.check.type,
            "table": self.check.table,
            "severity": self.check.severity,
            "expect": self.check.expect,
            "description": self.check.description,
            "observed": self.observed,
            "status": self.status,
            "duration_seconds": round(self.duration_seconds, 4),
        }


@dataclass(slots=True)
class DataQualityReport:
    results: list[CheckResult] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)
    funnel: dict[str, float] = field(default_factory=dict)
    distribution: dict[str, Any] = field(default_factory=dict)
    duration_seconds: float = 0.0

    @property
    def failures(self) -> list[CheckResult]:
        return [result for result in self.results if result.status == "FAIL"]

    @property
    def warnings(self) -> list[CheckResult]:
        return [result for result in self.results if result.status == "WARN"]

    @property
    def passed(self) -> bool:
        return not self.failures

    def by_type(self, check_type: str) -> list[CheckResult]:
        return [result for result in self.results if result.check.type == check_type]

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": "PASS" if self.passed else "FAIL",
            "checks_run": len(self.results),
            "failures": len(self.failures),
            "warnings": len(self.warnings),
            "duration_seconds": round(self.duration_seconds, 3),
            "row_counts": self.row_counts,
            "funnel": self.funnel,
            "distribution": self.distribution,
            "results": [result.as_dict() for result in self.results],
        }

    def render(self) -> str:
        lines: list[str] = []
        lines.append(" SQL DATA QUALITY AND JOIN VALIDATION")
        lines.append(" " + "-" * (_WIDTH - 2))

        for check_type, title in (
            ("referential", "Referential integrity"),
            ("temporal", "Temporal consistency"),
            ("business_rule", "Business rules"),
            ("uniqueness", "Uniqueness"),
        ):
            results = self.by_type(check_type)
            if not results:
                continue
            failed = [r for r in results if r.status != "PASS"]
            lines.append(f" {title:<28} {len(results):>3} checks, {len(failed):>2} not passing")
            for result in failed:
                lines.append(
                    f"     {result.status}  {result.check.name}: observed {result.observed:,}"
                    f" ({result.check.description})"
                )

        join_results = self.by_type("join")
        if join_results:
            lines.append("")
            lines.append(" Representative joins (row counts must be non-zero)")
            for result in join_results:
                marker = " " if result.status == "PASS" else "!"
                lines.append(f"   {marker} {result.check.name:<38} {result.observed:>14,}")

        if self.funnel:
            lines.append("")
            lines.append(" Funnel measured in the database")
            lines.append(
                f"   impressions {int(self.funnel['impressions']):>12,}"
                f"   clicks {int(self.funnel['clicks']):>10,}"
                f"   conversions {int(self.funnel['conversions']):>8,}"
            )
            lines.append(
                f"   CTR {self.funnel['ctr'] * 100:>8.4f}%"
                f"   CVR {self.funnel['cvr'] * 100:>8.4f}%"
                f"   CPM {self.funnel['cpm']:>8.4f}"
                f"   CPC {self.funnel['cpc']:>7.4f}"
                f"   CPA {self.funnel['cpa']:>9.4f}"
                f"   ROAS {self.funnel['roas']:>6.2f}x"
            )

        if self.distribution:
            lines.append("")
            lines.append(" Distribution spot checks (proof the data is not uniform)")
            for label, value in self.distribution.items():
                lines.append(f"   {label:<48} {value}")

        lines.append("")
        lines.append(
            f" Checks: {len(self.results)}   Failures: {len(self.failures)}   "
            f"Warnings: {len(self.warnings)}   ({self.duration_seconds:,.1f}s)"
        )
        lines.append(f" Status: {'PASS' if self.passed else 'FAIL'}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# check loading
# ---------------------------------------------------------------------------


def load_checks(directory: Path = TESTS_DIR) -> list[Check]:
    checks: list[Check] = []
    for path in sorted(directory.glob("*.sql")):
        checks.extend(_parse_checks(path))
    if not checks:
        raise FileNotFoundError(f"No data quality checks found in {directory}")
    names = [check.name for check in checks]
    duplicates = {name for name in names if names.count(name) > 1}
    if duplicates:
        raise ValueError(f"Duplicate data quality check names: {sorted(duplicates)}")
    return checks


def _parse_checks(path: Path) -> list[Check]:
    checks: list[Check] = []
    metadata: dict[str, str] | None = None
    statement: list[str] = []

    def flush() -> None:
        if metadata is None:
            return
        missing = [key for key in _REQUIRED_KEYS if key not in metadata]
        if missing:
            raise ValueError(
                f"Check {metadata.get('name', '?')} in {path.name} is missing {missing}"
            )
        sql = "\n".join(statement).strip()
        if not sql:
            raise ValueError(f"Check {metadata['name']} in {path.name} has no SQL body")
        checks.append(
            Check(sql=sql, source=path.name, **{key: metadata[key] for key in _REQUIRED_KEYS})
        )

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        match = _METADATA_LINE.match(raw_line.strip())
        if match and match.group(1) == "name":
            flush()
            metadata = {"name": match.group(2)}
            statement = []
            continue
        if match and metadata is not None and not statement and match.group(1) in _REQUIRED_KEYS:
            metadata[match.group(1)] = match.group(2)
            continue
        if metadata is None:
            continue  # file-level header comment
        if not statement and not raw_line.strip():
            continue
        statement.append(raw_line)

    flush()
    return checks


# ---------------------------------------------------------------------------
# execution
# ---------------------------------------------------------------------------


def run_data_quality_suite(
    connection: Any,
    run_id: UUID | None,
    config: GenerationConfig,
    *,
    checks: list[Check] | None = None,
) -> DataQualityReport:
    started = perf_counter()
    report = DataQualityReport()
    parameters = {"simulation_end": config.timeline.simulation_end_date}

    for check in checks or load_checks():
        check_started = perf_counter()
        with connection.cursor() as cursor:
            cursor.execute(check.sql, parameters)
            row = cursor.fetchone()
        observed = int(row[0]) if row and row[0] is not None else 0
        result = CheckResult(
            check=check,
            observed=observed,
            status=check.evaluate(observed),
            duration_seconds=perf_counter() - check_started,
        )
        report.results.append(result)
        if result.status != "PASS":
            logger.error(
                "data quality check did not pass",
                extra={
                    "check": check.name,
                    "status": result.status,
                    "observed": observed,
                    "expect": check.expect,
                },
            )

    report.row_counts = _row_counts(connection)
    report.funnel = _funnel_metrics(connection)
    report.distribution = _distribution_metrics(connection)
    report.duration_seconds = perf_counter() - started

    if run_id is not None:
        _persist(connection, run_id, report)
    return report


def _row_counts(connection: Any) -> dict[str, int]:
    from data_generator.models import TABLE_NAMES

    counts: dict[str, int] = {}
    with connection.cursor() as cursor:
        for table in TABLE_NAMES:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = int(cursor.fetchone()[0])
    return counts


def _funnel_metrics(connection: Any) -> dict[str, float]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM impressions),
                (SELECT COUNT(*) FROM clicks),
                (SELECT COUNT(*) FROM conversions),
                (SELECT COALESCE(SUM(spend_amount), 0) FROM spend_transactions),
                (SELECT COALESCE(SUM(conversion_value), 0) FROM conversions)
            """
        )
        impressions, clicks, conversions, spend, value = cursor.fetchone()

    spend = float(spend)
    value = float(value)
    return {
        "impressions": float(impressions),
        "clicks": float(clicks),
        "conversions": float(conversions),
        "spend": round(spend, 2),
        "conversion_value": round(value, 2),
        "ctr": safe_divide(clicks, impressions),
        "cvr": safe_divide(conversions, clicks),
        "cpm": safe_divide(spend, impressions) * 1000,
        "cpc": safe_divide(spend, clicks),
        "cpa": safe_divide(spend, conversions),
        "roas": safe_divide(value, spend),
    }


def _distribution_metrics(connection: Any) -> dict[str, Any]:
    """Evidence that the ecosystem is skewed rather than uniform.

    A uniform generator would put every campaign's share of traffic and CTR in
    the same place; these numbers are what show it does not.
    """
    metrics: dict[str, Any] = {}
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH per_campaign AS (
                SELECT campaign_id, COUNT(*) AS impressions
                FROM impressions GROUP BY campaign_id
            ), ranked AS (
                SELECT impressions,
                       SUM(impressions) OVER () AS total,
                       ROW_NUMBER() OVER (ORDER BY impressions DESC) AS position,
                       COUNT(*) OVER () AS campaigns
                FROM per_campaign
            )
            SELECT COALESCE(ROUND(100.0 * SUM(impressions) FILTER (
                       WHERE position <= GREATEST(campaigns / 10, 1)
                   ) / NULLIF(MAX(total), 0), 1), 0)
            FROM ranked
            """
        )
        metrics["impression share held by the top 10% of campaigns"] = f"{cursor.fetchone()[0]}%"

        cursor.execute(
            """
            WITH per_publisher AS (
                SELECT publisher_id, COUNT(*) AS impressions
                FROM impressions GROUP BY publisher_id
            ), ranked AS (
                SELECT impressions,
                       SUM(impressions) OVER () AS total,
                       ROW_NUMBER() OVER (ORDER BY impressions DESC) AS position,
                       COUNT(*) OVER () AS publishers
                FROM per_publisher
            )
            SELECT COALESCE(ROUND(100.0 * SUM(impressions) FILTER (
                       WHERE position <= GREATEST(publishers / 10, 1)
                   ) / NULLIF(MAX(total), 0), 1), 0)
            FROM ranked
            """
        )
        metrics["impression share held by the top 10% of publishers"] = f"{cursor.fetchone()[0]}%"

        cursor.execute(
            """
            WITH campaign_ctr AS (
                SELECT i.campaign_id,
                       COUNT(*) AS impressions,
                       COUNT(cl.click_id)::numeric / COUNT(*) AS ctr
                FROM impressions i
                LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
                GROUP BY i.campaign_id
                HAVING COUNT(*) >= 100
            )
            SELECT ROUND(MIN(ctr) * 100, 4), ROUND(MAX(ctr) * 100, 4),
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ctr)::numeric * 100, 4),
                   COUNT(*)
            FROM campaign_ctr
            """
        )
        low, high, median, sample = cursor.fetchone()
        metrics["campaign CTR min / median / max (campaigns >= 100 impressions)"] = (
            f"{low}% / {median}% / {high}%  across {sample:,} campaigns"
        )

        cursor.execute(
            """
            SELECT COUNT(*), MIN(campaigns), MAX(campaigns), ROUND(AVG(campaigns), 1)
            FROM (
                SELECT advertiser_id, COUNT(*) AS campaigns
                FROM campaigns GROUP BY advertiser_id
            ) per_advertiser
            """
        )
        advertisers, min_campaigns, max_campaigns, avg_campaigns = cursor.fetchone()
        metrics["campaigns per advertiser min / avg / max"] = (
            f"{min_campaigns} / {avg_campaigns} / {max_campaigns}  across {advertisers:,} advertisers"
        )
    return metrics


def _persist(connection: Any, run_id: UUID, report: DataQualityReport) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM platform.generation_run WHERE run_id = %s", (run_id,))
        if cursor.fetchone() is None:
            logger.warning(
                "skipping result persistence: unknown run", extra={"run_id": str(run_id)}
            )
            return
        cursor.executemany(
            """
            INSERT INTO platform.data_quality_result (
                run_id, check_name, check_type, target_table, severity, status, observed,
                threshold, details
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    run_id,
                    result.check.name,
                    result.check.type,
                    result.check.table,
                    result.check.severity,
                    result.status,
                    result.observed,
                    0 if result.check.expect == "zero" else None,
                    result.check.description,
                )
                for result in report.results
            ],
        )
        cursor.execute(
            "UPDATE platform.generation_run SET validation_status = %s WHERE run_id = %s",
            ("PASS" if report.passed else "FAIL", run_id),
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="adtech-validate",
        description="Run the SQL data quality and join validation suite against PostgreSQL.",
    )
    parser.add_argument("--scale", help="Scale profile, only used to resolve the simulation date")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--report", type=Path, default=PROJECT_ROOT / "artifacts" / "data_quality_report.json"
    )
    args = parser.parse_args(argv)

    configure_logging(args.log_level)
    config = GenerationConfig.load(scale=args.scale)
    settings = DatabaseSettings.from_env()

    with connect(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT run_id FROM platform.generation_run ORDER BY started_at DESC LIMIT 1"
            )
            row = cursor.fetchone()
        report = run_data_quality_suite(connection, row[0] if row else None, config)
        connection.commit()

    import json

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report.as_dict(), indent=2, default=str), encoding="utf-8")
    print(report.render())
    print(f"\nMachine-readable report written to {args.report}")
    return 0 if report.passed else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
