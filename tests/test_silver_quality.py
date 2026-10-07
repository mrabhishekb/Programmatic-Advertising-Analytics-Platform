"""The Silver quality suite is well-formed, and its skip logic is right.

No Spark and no database: these parse the check files and exercise the decision
about which checks can run. Whether the SQL returns the right number is a
question for the live suite, not for a unit test that would have to stand up an
Iceberg catalog to ask it.
"""

from __future__ import annotations

import pytest

from data_quality import silver
from data_quality.validation import Check


@pytest.fixture(scope="module")
def checks() -> list[Check]:
    return silver.load_silver_checks()


class TestCheckDefinitions:
    def test_checks_are_discovered(self, checks):
        assert len(checks) >= 15

    def test_names_are_unique(self, checks):
        names = [check.name for check in checks]
        assert len(set(names)) == len(names)

    def test_names_do_not_collide_with_the_source_suite(self, checks):
        """Both suites persist results by check name, so a collision would make
        two different checks indistinguishable in the history."""
        from data_quality.validation import load_checks

        assert not {c.name for c in checks} & {c.name for c in load_checks()}

    def test_metadata_uses_known_values(self, checks):
        for check in checks:
            assert check.severity in {"ERROR", "WARNING"}, check.name
            assert check.expect in {"zero", "nonzero"}, check.name
            assert check.type in silver.CHECK_TYPES, check.name
            assert check.description, check.name

    def test_every_check_selects_something(self, checks):
        for check in checks:
            assert check.sql.lower().lstrip().startswith(("select", "with")), check.name

    def test_all_three_families_are_present(self, checks):
        assert {check.type for check in checks} == set(silver.CHECK_TYPES)

    def test_an_unknown_check_type_is_rejected(self, tmp_path):
        """The two suites run on different engines, so a check landing in the
        wrong directory has to fail loudly rather than be handed to Spark."""
        (tmp_path / "bad.sql").write_text(
            "-- name: wrong_family\n"
            "-- type: referential\n"
            "-- table: all\n"
            "-- severity: ERROR\n"
            "-- expect: zero\n"
            "-- description: belongs to the PostgreSQL suite\n"
            "SELECT 0;\n"
        )
        with pytest.raises(ValueError, match="Unknown Silver check type"):
            silver.load_silver_checks(tmp_path)

    def test_reconciliation_checks_exist_for_the_mutated_columns(self, checks):
        """The change generator only ever edits these, so a merge that stopped
        applying updates would be invisible to a check on any other column."""
        names = {check.name for check in checks}
        assert {
            "reconciliation_campaign_values_match",
            "reconciliation_line_item_bids_match",
            "reconciliation_creative_status_matches",
            "reconciliation_publisher_status_matches",
        } <= names


class TestWorkingOutWhatACheckReads:
    def test_a_silver_table_is_recognised(self):
        assert silver.referenced_tables("SELECT * FROM lake.silver.campaigns") == {"campaigns"}

    def test_a_metadata_table_resolves_to_its_parent(self):
        """`.snapshots` is not a table anyone builds; the thing that has to
        exist is the table it hangs off."""
        assert silver.referenced_tables("SELECT max(x) FROM lake.silver.campaigns.snapshots") == {
            "campaigns"
        }

    def test_several_tables_in_one_statement(self):
        sql = "SELECT (SELECT count(*) FROM lake.silver.clicks) + (SELECT count(*) FROM lake.silver.impressions)"
        assert silver.referenced_tables(sql) == {"clicks", "impressions"}

    def test_jdbc_views_are_not_silver_tables(self):
        """`pg_campaigns` is a temp view over PostgreSQL, so it says nothing
        about whether the Iceberg table has been built."""
        assert silver.referenced_tables("SELECT * FROM pg_campaigns") == set()


class TestSkippingChecksForTablesThatAreNotBuilt:
    def _check(self, sql: str) -> Check:
        return Check(
            name="x",
            type="integrity",
            table="all",
            severity="ERROR",
            expect="zero",
            description="d",
            sql=sql,
            source="t.sql",
        )

    def test_a_check_runs_when_its_tables_exist(self):
        check = self._check("SELECT count(*) FROM lake.silver.campaigns")
        assert silver.applicable(check, ["campaigns", "clicks"])

    def test_a_check_is_skipped_when_a_table_is_missing(self):
        """Event tables only exist after --include-events. Running their checks
        anyway would report violations about tables nobody created."""
        check = self._check("SELECT count(*) FROM lake.silver.impressions")
        assert not silver.applicable(check, ["campaigns"])

    def test_a_check_needs_every_table_it_reads(self):
        sql = "SELECT (SELECT count(*) FROM lake.silver.campaigns) + (SELECT count(*) FROM lake.silver.impressions)"
        assert not silver.applicable(self._check(sql), ["campaigns"])

    def test_a_check_over_views_only_always_runs(self):
        """The counts views are built over whatever is present, so they are
        self-limiting and need no gate."""
        check = self._check("SELECT count(*) FROM pg_counts JOIN silver_counts USING (table_name)")
        assert silver.applicable(check, [])
