"""The continuous change generator and the optional container that runs it.

A loop that mutates the source tables forever is only safe if every state
change has a reverse. Without that, the one-way default profile - campaigns
drifting to PAUSED, publishers to SUSPENDED, budgets ratcheting to their cap -
eventually turns a carefully shaped dataset into a uniform one, slowly enough
that nobody notices until the analytics look wrong.
"""

from __future__ import annotations

import math
import statistics

import pytest
import yaml

from data_generator.change_generator import ChangeSummary, _symmetric_factor, apply_changes
from data_generator.config import PROJECT_ROOT, GenerationConfig
from data_generator.db import DatabaseSettings, connect
from data_generator.rng import RandomStream


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


class TestMultipliersDoNotRatchet:
    """Budget and bid factors are applied over and over by a long-running loop."""

    def test_the_factor_is_centred_on_one_in_log_space(self):
        """Uniform(0.8, 1.4) looks balanced but has a mean of 1.1, so repeated
        application only ever climbs. Drawing the exponent symmetrically makes a
        halving exactly as likely as a doubling."""
        rng = RandomStream(42, "test", "factors")
        logs = [math.log(float(_symmetric_factor(rng, 0.3))) for _ in range(4000)]
        assert abs(statistics.fmean(logs)) < 0.01

    def test_the_factor_stays_within_the_requested_spread(self):
        rng = RandomStream(42, "test", "bounds")
        factors = [float(_symmetric_factor(rng, 0.3)) for _ in range(1000)]
        assert min(factors) >= math.exp(-0.3) - 0.01
        assert max(factors) <= math.exp(0.3) + 0.01

    def test_an_unclamped_walk_does_wander(self):
        """Zero drift is not the same as bounded.

        The factor has no bias, but the variance of a product of many draws
        grows without limit, so a long enough loop takes a value anywhere. This
        is why the SQL clamps the result into a band rather than relying on the
        factor alone.
        """
        rng = RandomStream(7, "test", "walk")
        product = 1.0
        for _ in range(2000):
            product *= float(_symmetric_factor(rng, 0.3))
        assert not 0.5 < product < 2.0

    def test_clamping_to_a_band_keeps_it_stationary(self):
        """Mirrors what the balanced UPDATE does: multiply, then clamp between
        campaign_budget/200 and campaign_budget/20."""
        rng = RandomStream(7, "test", "clamped")
        campaign_budget = 60_000.0
        low, high = campaign_budget / 200, campaign_budget / 20
        value = campaign_budget / 66

        for _ in range(5000):
            value = min(max(value * float(_symmetric_factor(rng, 0.3)), low), high)
            assert low <= value <= high


class TestChangeSummary:
    def test_absorb_accumulates_totals_and_per_table_counts(self):
        first = ChangeSummary()
        first.record("campaigns", "update", 3)
        first.record("audiences", "insert", 1)

        second = ChangeSummary()
        second.record("campaigns", "update", 2)
        second.record("audiences", "delete", 1)

        first.absorb(second)
        assert first.updates == 5
        assert first.inserts == 1
        assert first.deletes == 1
        assert first.by_table["campaigns.update"] == 5
        assert first.total == 7


class TestTrafficServiceIsOptional:
    """The container mutates the source database, so it must never start by
    accident. A profile keeps it out of a plain `docker compose up`."""

    def test_it_sits_behind_a_profile(self, compose):
        assert compose["services"]["traffic"]["profiles"] == ["traffic"]

    def test_no_other_service_is_gated(self, compose):
        """If anything else grew a profile, `make up` would silently skip it."""
        gated = {name for name, service in compose["services"].items() if service.get("profiles")}
        assert gated == {"traffic"}

    def test_it_waits_for_a_healthy_database(self, compose):
        condition = compose["services"]["traffic"]["depends_on"]["postgres"]["condition"]
        assert condition == "service_healthy"

    def test_it_comes_back_after_a_reboot(self, compose):
        assert compose["services"]["traffic"]["restart"] == "unless-stopped"

    def test_it_cannot_write_to_the_project(self, compose):
        """It drives the database; it has no business editing its own source."""
        mounts = compose["services"]["traffic"]["volumes"]
        assert all(str(mount).endswith(":ro") for mount in mounts), mounts

    def test_it_reaches_postgres_by_service_name(self, compose):
        """localhost inside the container is the container, not the database."""
        environment = compose["services"]["traffic"]["environment"]
        assert environment["POSTGRES_HOST"] in compose["services"]

    def test_it_runs_the_loop(self, compose):
        assert "--loop" in [str(part) for part in compose["services"]["traffic"]["command"]]

    def test_its_dockerfile_exists(self, compose):
        dockerfile = compose["services"]["traffic"]["build"]["dockerfile"]
        assert (PROJECT_ROOT / dockerfile).exists()


@pytest.mark.postgres
class TestBalanceAgainstARealDatabase:
    # Named away from the session-scoped `config` fixture in conftest.py, which
    # builds a whole generated ecosystem none of this needs.
    @pytest.fixture(scope="module")
    def source_db(self):
        settings = DatabaseSettings.from_env()
        try:
            conn = connect(settings)
        except Exception as exc:  # pragma: no cover - environment dependent
            pytest.skip(f"PostgreSQL not reachable at {settings.describe()}: {exc}")
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM campaigns")
            if cursor.fetchone()[0] == 0:
                conn.close()
                pytest.skip("No data loaded; run `make generate` first")
        yield conn
        conn.close()

    @pytest.fixture(scope="module")
    def generation_config(self) -> GenerationConfig:
        return GenerationConfig.load(scale="small", seed=42)

    def test_a_balanced_batch_adds_and_removes_the_same_audiences(
        self, source_db, generation_config
    ):
        summary = apply_changes(
            source_db, generation_config, batches=1, changes_per_batch=24, balanced=True
        )
        assert summary.by_table.get("audiences.insert") == summary.by_table.get("audiences.delete")

    def test_the_default_profile_is_deliberately_one_way(self, source_db, generation_config):
        """Documents the asymmetry rather than treating it as a bug: a single
        one-shot batch should read like a slice of a working day."""
        summary = apply_changes(
            source_db, generation_config, batches=1, changes_per_batch=24, balanced=False
        )
        assert summary.by_table.get("audiences.insert", 0) > summary.by_table.get(
            "audiences.delete", 0
        )

    def test_a_balanced_batch_still_produces_change_events_to_capture(
        self, source_db, generation_config
    ):
        """Balance must not mean a no-op: CDC needs something to stream."""
        summary = apply_changes(
            source_db, generation_config, batches=1, changes_per_batch=24, balanced=True
        )
        assert summary.total > 0

    def test_the_balanced_update_never_pushes_a_budget_above_its_band(
        self, source_db, generation_config
    ):
        """The clamp lives in SQL, so this is the only place it gets exercised."""
        with source_db.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM campaigns "
                "WHERE campaign_budget > 0 AND daily_budget > campaign_budget / 20.0 + 0.01"
            )
            before = cursor.fetchone()[0]

        for _ in range(3):
            apply_changes(
                source_db, generation_config, batches=1, changes_per_batch=24, balanced=True
            )

        with source_db.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM campaigns "
                "WHERE campaign_budget > 0 AND daily_budget > campaign_budget / 20.0 + 0.01"
            )
            after = cursor.fetchone()[0]

        # Rows the generator created may already sit outside the band and are
        # only pulled in when touched, so this asserts the count never grows.
        assert after <= before
