"""The SQL data quality suite is well-formed and its pass/fail logic is right."""

from __future__ import annotations

import pytest

from data_quality.validation import Check, CheckResult, DataQualityReport, load_checks


@pytest.fixture(scope="module")
def checks() -> list[Check]:
    return load_checks()


class TestCheckDefinitions:
    def test_checks_are_discovered(self, checks):
        assert len(checks) >= 40

    def test_names_are_unique(self, checks):
        names = [check.name for check in checks]
        assert len(set(names)) == len(names)

    def test_metadata_uses_known_values(self, checks):
        for check in checks:
            assert check.severity in {"ERROR", "WARNING"}
            assert check.expect in {"zero", "nonzero"}
            assert check.type in {"referential", "temporal", "business_rule", "uniqueness", "join"}
            assert check.description

    def test_every_check_selects_something(self, checks):
        for check in checks:
            assert check.sql.lower().lstrip().startswith(("select", "with")), check.name

    def test_all_check_families_are_present(self, checks):
        families = {check.type for check in checks}
        assert families == {"referential", "temporal", "business_rule", "uniqueness", "join"}

    def test_the_required_checks_from_the_brief_exist(self, checks):
        names = {check.name for check in checks}
        required = {
            "orphan_clicks",
            "orphan_conversions",
            "click_attributes_match_impression",
            "conversion_attributes_match_click",
            "duplicate_impression_ids",
            "duplicate_click_ids",
            "duplicate_conversion_ids",
            "viewability_out_of_range",
            "audience_age_range_invalid",
            "click_before_impression",
            "conversion_before_click",
            "impression_outside_campaign_flight",
            "join_impressions_to_campaigns",
            "join_clicks_to_conversions",
        }
        assert required <= names


class TestEvaluationLogic:
    def _check(self, expect: str, severity: str = "ERROR") -> Check:
        return Check(
            name="example",
            type="referential",
            table="clicks",
            severity=severity,
            expect=expect,
            description="example",
            sql="SELECT 0",
            source="test",
        )

    def test_zero_expectation(self):
        assert self._check("zero").evaluate(0) == "PASS"
        assert self._check("zero").evaluate(3) == "FAIL"

    def test_nonzero_expectation(self):
        assert self._check("nonzero").evaluate(5) == "PASS"
        assert self._check("nonzero").evaluate(0) == "FAIL"

    def test_warnings_do_not_fail_the_run(self):
        assert self._check("zero", severity="WARNING").evaluate(1) == "WARN"

    def test_report_status_reflects_failures(self):
        report = DataQualityReport()
        report.results.append(CheckResult(self._check("zero"), 0, "PASS", 0.1))
        assert report.passed
        report.results.append(CheckResult(self._check("zero"), 9, "FAIL", 0.1))
        assert not report.passed
        assert len(report.failures) == 1

    def test_warnings_are_reported_separately(self):
        report = DataQualityReport()
        report.results.append(CheckResult(self._check("zero", "WARNING"), 2, "WARN", 0.1))
        assert report.passed
        assert len(report.warnings) == 1
