"""Human-readable run report."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from data_generator.models import EVENT_TABLES, TABLE_NAMES

if TYPE_CHECKING:  # pragma: no cover
    from data_generator.generate import GenerationResult

_WIDTH = 72


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Every rate in this project goes through here: no division by zero, ever."""
    return numerator / denominator if denominator else default


def render_generation_report(result: GenerationResult, dq_report: Any = None) -> str:
    config = result.config
    events = result.events
    counts = result.row_counts

    lines: list[str] = []
    lines.append("=" * _WIDTH)
    lines.append(" DATA GENERATION REPORT")
    lines.append("=" * _WIDTH)
    lines.append(f" Run id          : {result.run_id}")
    lines.append(f" Scale profile   : {config.scale.name}")
    lines.append(f" Seed            : {config.seed}")
    lines.append(f" Simulation end  : {config.timeline.simulation_end_date}")
    lines.append(
        f" Event window    : {config.timeline.event_window_start_date} .. "
        f"{config.timeline.event_window_end_date} "
        f"({config.timeline.event_window_days} days)"
    )
    lines.append(f" Duration        : {result.duration_seconds:,.1f}s")

    lines.append("")
    lines.append(" ROW COUNTS")
    lines.append(" " + "-" * (_WIDTH - 2))
    for table in TABLE_NAMES:
        marker = "*" if table in EVENT_TABLES else " "
        lines.append(f" {marker} {table:<24} {counts.get(table, 0):>14,}")

    ctr = safe_divide(events.clicks, events.impressions)
    cvr = safe_divide(events.conversions, events.clicks)
    cpm = safe_divide(events.billed_spend, events.impressions) * 1000
    cpc = safe_divide(events.billed_spend, events.clicks)
    cpa = safe_divide(events.billed_spend, events.conversions)
    roas = safe_divide(events.conversion_value, events.billed_spend)

    lines.append("")
    lines.append(" FUNNEL AND BLENDED METRICS")
    lines.append(" " + "-" * (_WIDTH - 2))
    lines.append(f" Delivering campaigns     {events.delivering_campaigns:>14,}")
    lines.append(f" Impressions              {events.impressions:>14,}")
    lines.append(f" Clicks                   {events.clicks:>14,}   CTR  {ctr * 100:>8.4f}%")
    lines.append(f" Conversions              {events.conversions:>14,}   CVR  {cvr * 100:>8.4f}%")
    lines.append(f" Spend transactions       {events.spend_transactions:>14,}")
    lines.append(f" Billed spend             {events.billed_spend:>14,.2f}")
    lines.append(f" Media cost (CPM basis)   {events.media_cost:>14,.2f}")
    lines.append(f" Conversion value         {events.conversion_value:>14,.2f}")
    lines.append(f" CPM {cpm:>10,.4f}   CPC {cpc:>8,.4f}   CPA {cpa:>10,.4f}   ROAS {roas:>7,.2f}x")
    if events.spend_by_billing_type:
        mix = "  ".join(
            f"{billing}={count:,}"
            for billing, count in sorted(events.spend_by_billing_type.items())
        )
        lines.append(f" Spend rows by billing type: {mix}")

    lines.append("")
    lines.append(" IN-PROCESS VALIDATION (before any row was written)")
    lines.append(" " + "-" * (_WIDTH - 2))
    stats = result.validation_stats
    if stats.get("skipped"):
        lines.append(" SKIPPED (--skip-validation)")
    else:
        lines.append(f" Referential checks       {stats['referential_checks']:>14,}")
        lines.append(f" Temporal checks          {stats['temporal_checks']:>14,}")
        lines.append(f" Business rule checks     {stats['business_rule_checks']:>14,}")
        lines.append(f" Total assertions         {stats['total_checks']:>14,}")
        lines.append(f" Duplicate primary keys   {stats['duplicate_keys_found']:>14,}")
        lines.append(" Violations               " + f"{0:>14,}  (any violation aborts the run)")

    if dq_report is not None:
        lines.append("")
        lines.append(dq_report.render())
    else:
        lines.append("")
        lines.append(" Status: PASS (in-process validation)")

    return "\n".join(lines)
