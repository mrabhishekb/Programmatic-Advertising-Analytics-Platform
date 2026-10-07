"""Data quality for the Silver layer, and its agreement with the source.

``validation.py`` checks the data PostgreSQL holds. This checks what survived
the trip through Debezium, Kafka, Bronze and reconciliation - which is a
different question, and the one the pipeline can actually get wrong.

The checks are the same annotated-SQL format and reuse ``Check``,
``CheckResult`` and ``DataQualityReport`` unchanged. Only the engine differs:
Spark SQL against Iceberg instead of psycopg against PostgreSQL. They live in
``tests/silver/`` rather than beside the source checks because ``load_checks``
does not recurse, so the two suites stay independent and neither can
accidentally run against the wrong engine.

Why the source is readable from Spark
-------------------------------------
Phase 6 put the PostgreSQL JDBC driver in the Spark image so Iceberg could use
a JDBC catalog. That driver is also what lets a single Spark SQL statement join
an Iceberg table against a live PostgreSQL table, which is what makes
reconciliation expressible as an ordinary check rather than a bespoke Python
comparison.

The source tables are exposed as ``pg_<table>``. Only the dimensions are exposed
whole; the event tables reach 100,000,000 rows and pulling them over JDBC to
count them would be absurd, so their counts are aggregated inside PostgreSQL
first and arrive as the single ``pg_counts`` view.

What reconciliation is actually testing
---------------------------------------
That Silver still agrees with the source after an arbitrary number of
incremental merges. A watermark that advanced too far, a change event dropped
between Kafka and Bronze, or a merge that matched on the wrong key all show up
here and nowhere else - the Silver table stays internally consistent and simply
holds a value that is quietly out of date. Comparing row counts would not catch
it; comparing the columns the change traffic actually mutates does.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from time import perf_counter
from typing import Any

from data_generator.config import PROJECT_ROOT
from data_generator.db import DatabaseSettings
from data_generator.logging_setup import configure_logging, get_logger
from data_generator.models import EVENT_TABLES, TABLE_NAMES
from data_quality.validation import Check, CheckResult, DataQualityReport, load_checks

logger = get_logger(__name__)

SILVER_TESTS_DIR = Path(__file__).resolve().parent / "tests" / "silver"

#: Deliberately disjoint from the source suite's types. A check written for one
#: engine would not survive being run by the other, and distinct vocabularies
#: make that obvious in the file rather than at the point of failure.
CHECK_TYPES = frozenset({"integrity", "lineage", "reconciliation"})

_WIDTH = 78


def source_view_name(table: str) -> str:
    return f"pg_{table}"


def present_tables(spark: Any) -> list[str]:
    """The Silver tables that exist, in a stable order.

    Event tables are only built with ``--include-events``, so a run where they
    are absent is normal rather than a failure. Checking a table that is not
    there would report a violation that says nothing about data quality.
    """
    from spark import catalog

    return [
        table for table in TABLE_NAMES if spark.catalog.tableExists(catalog.table_identifier(table))
    ]


def register_source_views(
    spark: Any,
    tables: list[str],
    settings: DatabaseSettings | None = None,
) -> None:
    """Expose the PostgreSQL side to Spark SQL.

    Dimensions are registered whole and rely on the JDBC source's column
    pruning: a check selecting two columns has PostgreSQL send two columns.
    Hardcoding a column list here instead would put a second copy of the schema
    in this file, to be forgotten the next time one changes.
    """
    from spark import catalog as iceberg_catalog

    settings = settings or DatabaseSettings.from_env()
    reader = (
        spark.read.format("jdbc")
        .option("url", iceberg_catalog.jdbc_url(settings))
        .option("user", settings.user)
        .option("password", settings.password)
        .option("driver", "org.postgresql.Driver")
    )

    for table in tables:
        if table in EVENT_TABLES:
            continue
        reader.option("dbtable", table).load().createOrReplaceTempView(source_view_name(table))

    # One row per table, counted inside PostgreSQL. The alternative - reading
    # 100,000,000 impressions over JDBC so Spark can count them - is the kind of
    # thing that works on the test dataset and takes the cluster down on the
    # real one.
    union = " UNION ALL ".join(
        f"SELECT '{table}' AS table_name, count(*) AS row_count FROM {table}" for table in tables
    )
    reader.option("dbtable", f"({union}) AS counts").load().createOrReplaceTempView("pg_counts")


def register_silver_views(spark: Any, tables: list[str]) -> None:
    """``silver_counts``: live and soft-deleted rows per Silver table.

    Live rather than total, because that is what the source can be compared
    against - PostgreSQL deletes a row, Silver keeps it flagged, and the two
    agree only once the flagged ones are excluded.
    """
    from spark import catalog

    union = " UNION ALL ".join(
        f"SELECT '{table}' AS table_name, "
        f"count(*) AS total_rows, "
        f"count_if(NOT is_deleted) AS live_rows, "
        f"count_if(is_deleted) AS deleted_rows "
        f"FROM {catalog.table_identifier(table)}"
        for table in tables
    )
    spark.sql(union).createOrReplaceTempView("silver_counts")


def load_silver_checks(directory: Path = SILVER_TESTS_DIR) -> list[Check]:
    checks = load_checks(directory)
    unknown = {check.type for check in checks} - CHECK_TYPES
    if unknown:
        raise ValueError(f"Unknown Silver check type(s): {sorted(unknown)}")
    return checks


def referenced_tables(sql: str) -> set[str]:
    """The Silver tables a check reads, taken from the SQL rather than metadata.

    The ``table:`` annotation is a label - it says ``all`` for checks spanning
    the layer - so it cannot answer "is everything this needs built?". Reading
    the identifiers out of the statement can, and it cannot drift from the
    query the way a hand-maintained list would.
    """
    from spark import catalog

    prefix = f"{catalog.CATALOG}.{catalog.NAMESPACE}."
    return set(re.findall(rf"{re.escape(prefix)}(\w+)", sql))


def applicable(check: Check, tables: list[str]) -> bool:
    """Whether every Silver table this check reads has been built.

    Event tables only exist after ``--include-events``, so a suite run without
    them skips their checks rather than reporting violations about tables that
    were never created.
    """
    return referenced_tables(check.sql) <= set(tables)


def pending_work(spark: Any) -> dict[str, int]:
    """Tables with Bronze changes that have not been merged yet, and how many.

    Reconciliation compares Silver against a source that keeps moving, so a
    mismatch has two possible causes: the pipeline is broken, or it is simply
    behind. Reporting the second makes the first diagnosable - without it, a
    failure during live traffic looks identical to a genuine defect, which is
    how a suite ends up being ignored.

    This sees one hop only. A change that has left PostgreSQL but is still in
    Kafka, not yet flushed to Bronze, shows up here as nothing pending while
    PostgreSQL is already ahead - the sink flushes on a timer, so that window is
    minutes wide. Zero pending therefore means "nothing left to merge", not
    "caught up", and reconciliation is only conclusive once the source has
    stopped changing.
    """
    from bronze.storage import BronzeStore
    from spark import incremental, layout

    store = BronzeStore()
    snapshot = layout.latest_snapshot_run(store, None)
    pending: dict[str, int] = {}
    for table in layout.RECONCILED_TABLES:
        plan = incremental.plan_table(
            spark,
            store,
            table=table,
            bronze_run=snapshot.run_id,
            bucket=store.settings.bucket,
        )
        if plan.cdc_urls:
            pending[table] = len(plan.cdc_urls)
    return pending


def run_silver_suite(
    spark: Any,
    *,
    checks: list[Check] | None = None,
    settings: DatabaseSettings | None = None,
) -> DataQualityReport:
    started = perf_counter()
    report = DataQualityReport()

    tables = present_tables(spark)
    if not tables:
        raise RuntimeError(
            "No Silver tables found. Run `make silver` before validating the Silver layer."
        )

    register_silver_views(spark, tables)
    register_source_views(spark, tables, settings)
    report.row_counts = {
        row["table_name"]: int(row["live_rows"])
        for row in spark.sql("SELECT * FROM silver_counts").collect()
    }

    for check in checks or load_silver_checks():
        if not applicable(check, tables):
            logger.info(
                "skipping check for a table that is not built",
                extra={"check": check.name, "table": check.table},
            )
            continue
        check_started = perf_counter()
        row = spark.sql(check.sql).first()
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
                "silver data quality check did not pass",
                extra={
                    "check": check.name,
                    "status": result.status,
                    "observed": observed,
                    "expect": check.expect,
                },
            )

    report.duration_seconds = perf_counter() - started
    return report


def render(report: DataQualityReport, pending: dict[str, int] | None = None) -> str:
    lines = ["", "=" * _WIDTH, " SILVER DATA QUALITY", "=" * _WIDTH]

    for check_type, title in (
        ("integrity", "Integrity within Silver"),
        ("lineage", "Lineage and watermark"),
        ("reconciliation", "Reconciliation against PostgreSQL"),
    ):
        results = report.by_type(check_type)
        if not results:
            continue
        failed = [result for result in results if result.status != "PASS"]
        lines.append(f" {title:<36} {len(results):>3} checks, {len(failed):>2} not passing")
        for result in failed:
            lines.append(
                f"     {result.status}  {result.check.name}: observed {result.observed:,}"
                f" ({result.check.description})"
            )

    if report.row_counts:
        lines.append("")
        lines.append(" Live rows in Silver")
        for table, count in report.row_counts.items():
            lines.append(f"   {table:<22}{count:>14,}")

    if pending:
        lines.append("")
        lines.append(" Unmerged Bronze changes - the source is ahead of Silver here:")
        for table, count in sorted(pending.items()):
            lines.append(f"   {table:<22}{count:>4} object(s) pending")
        lines.append(" Reconciliation differences are expected until `make silver` is run.")

    lines.append("")
    lines.append(
        f" Checks: {len(report.results)}   Failures: {len(report.failures)}   "
        f"Warnings: {len(report.warnings)}   ({report.duration_seconds:,.1f}s)"
    )
    lines.append(f" Status: {'PASS' if report.passed else 'FAIL'}")
    return "\n".join(lines)


def _write_report(path: Path, payload: dict[str, Any]) -> bool:
    """Persist the machine-readable report, if the filesystem allows it.

    This suite runs inside the Spark container, where the project is mounted
    read-only. Failing the run over an artifact nobody is waiting for would
    turn a passing suite into a red one, and would make the exit code mean
    "could not write a file" rather than "the data is wrong".
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    except OSError as error:
        logger.warning(
            "could not write the report; console output is the only record",
            extra={"path": str(path), "error": str(error)},
        )
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="adtech-validate-silver",
        description="Run the Silver data quality and reconciliation suite.",
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--report",
        type=Path,
        default=PROJECT_ROOT / "artifacts" / "silver_quality_report.json",
    )
    args = parser.parse_args(argv)

    configure_logging(args.log_level)

    from spark.session import build_session

    # The console progress bar interleaves with the report and makes it
    # unreadable. This command exists to be read, so it is turned off.
    spark = build_session("adtech-silver-quality", extra={"spark.ui.showConsoleProgress": "false"})
    try:
        report = run_silver_suite(spark)
        pending = pending_work(spark)
    finally:
        spark.stop()

    payload = report.as_dict() | {"pending_bronze_objects": pending}
    print(render(report, pending))
    if _write_report(args.report, payload):
        print(f"\n Machine-readable report written to {args.report}")
    # Non-zero on failure so a scheduler can gate on it. Nothing consumes this
    # yet; Airflow in phase 15 is what will.
    return 0 if report.passed else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
