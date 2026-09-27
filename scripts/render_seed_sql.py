#!/usr/bin/env python3
"""Render ``postgres/seed.sql`` from the reference YAML.

The generator and the database must agree on the country -> currency mapping and
the industry list. Rather than maintaining the same facts twice, the SQL seed is
generated from the YAML the generator already reads, and ``tests/test_seed_sql.py``
fails if the checked-in file drifts from the source.

Usage:
    python scripts/render_seed_sql.py           # write postgres/seed.sql
    python scripts/render_seed_sql.py --check   # exit 1 if the file is stale
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_generator.config import PROJECT_ROOT
from data_generator.reference import ReferenceData

SEED_PATH = PROJECT_ROOT / "postgres" / "seed.sql"

_HEADER = """-- =============================================================================
-- Reference data seed
-- =============================================================================
-- GENERATED FILE - do not edit by hand.
-- Source: data_generator/reference/geography.yml, data_generator/reference/business.yml
-- Regenerate with: python scripts/render_seed_sql.py
--
-- Seeding these tables from the same YAML the generator reads means the database
-- and the generator can never disagree about which countries exist or which
-- currency a country bills in. The data quality suite uses ref_country to verify
-- that every impression resolves to a known market.
-- =============================================================================
"""


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def render() -> str:
    reference = ReferenceData.load()

    lines = [
        _HEADER,
        "",
        "INSERT INTO ref_country (country_code, country_name, currency_code, region) VALUES",
    ]
    country_values = [
        f"    ({_quote(country.code)}, {_quote(country.name)}, "
        f"{_quote(country.currency)}, {_quote(country.region)})"
        for country in reference.countries
    ]
    lines.append(",\n".join(country_values))
    lines.append("ON CONFLICT (country_code) DO UPDATE SET")
    lines.append("    country_name  = EXCLUDED.country_name,")
    lines.append("    currency_code = EXCLUDED.currency_code,")
    lines.append("    region        = EXCLUDED.region;")
    lines.append("")
    lines.append("INSERT INTO ref_industry (industry, industry_group) VALUES")
    industry_values = [
        f"    ({_quote(industry.name)}, {_quote(industry.group)})"
        for industry in reference.industries
    ]
    lines.append(",\n".join(industry_values))
    lines.append("ON CONFLICT (industry) DO UPDATE SET")
    lines.append("    industry_group = EXCLUDED.industry_group;")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify the file is up to date")
    args = parser.parse_args(argv)

    rendered = render()
    if args.check:
        current = SEED_PATH.read_text(encoding="utf-8") if SEED_PATH.exists() else ""
        if current != rendered:
            print(f"{SEED_PATH} is out of date. Run: python scripts/render_seed_sql.py")
            return 1
        print(f"{SEED_PATH} is up to date.")
        return 0

    SEED_PATH.write_text(rendered, encoding="utf-8")
    print(f"Wrote {SEED_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
