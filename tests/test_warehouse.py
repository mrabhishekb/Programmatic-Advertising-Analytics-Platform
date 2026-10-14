"""The warehouse layer's logic, tested without a Snowflake account.

Almost everything worth asserting here happens before a connection is opened:
which statements the bootstrap will run, what DDL a Parquet schema produces,
and whether the dbt project is internally consistent. Those are the parts that
fail silently - a decimal mapped to a float still loads, and a source declared
with the wrong name only fails on the day someone runs dbt.

The connection itself is not mocked. A mock of a database driver asserts that
the code calls the mock the way the test expects, which is a different claim
from the code working.
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import pytest
import yaml

from data_generator.config import PROJECT_ROOT
from data_generator.models import EVENT_TABLES
from warehouse import load
from warehouse.bootstrap import (
    BOOTSTRAP_SQL,
    InvalidIdentifier,
    check_identifier,
    render,
    statements,
)
from warehouse.settings import (
    SCHEMAS,
    SNAPSHOT_SCHEMA,
    SnowflakeSettings,
    WarehouseNotConfigured,
)

DBT_DIR = PROJECT_ROOT / "warehouse" / "dbt"


@pytest.fixture
def settings() -> SnowflakeSettings:
    return SnowflakeSettings(
        account="xy12345.eu-west-1",
        user="ADTECH_LOADER",
        password="unused-in-these-tests",
        role="ADTECH_ENGINEER",
        warehouse="ADTECH_WH",
        database="ADTECH",
    )


class TestSettings:
    def test_blank_settings_are_not_configured(self) -> None:
        assert not SnowflakeSettings().configured

    def test_an_account_alone_is_not_enough(self) -> None:
        assert not SnowflakeSettings(account="xy12345").configured

    def test_a_private_key_counts_as_credentials(self) -> None:
        resolved = SnowflakeSettings(account="xy12345", user="u", private_key_path="/k.p8")
        assert resolved.configured
        assert resolved.uses_key_pair

    def test_key_pair_is_preferred_when_both_are_present(self) -> None:
        resolved = SnowflakeSettings(
            account="xy12345", user="u", password="p", private_key_path="/k.p8"
        )
        assert resolved.uses_key_pair

    def test_require_names_what_is_missing(self) -> None:
        with pytest.raises(WarehouseNotConfigured, match="SNOWFLAKE_ACCOUNT"):
            SnowflakeSettings().require()

    def test_describe_never_leaks_the_password(self, settings: SnowflakeSettings) -> None:
        assert settings.password not in settings.describe()


class TestBootstrapStatements:
    def test_every_schema_is_created(self, settings: SnowflakeSettings) -> None:
        script = "\n".join(render(settings))
        for schema in SCHEMAS:
            assert f"CREATE SCHEMA IF NOT EXISTS ADTECH.{schema}" in script

    def test_nothing_is_left_unsubstituted(self, settings: SnowflakeSettings) -> None:
        # A stray {database} would reach Snowflake as a syntax error, and the
        # message would name a brace rather than the setting that is missing.
        for statement in render(settings):
            assert "{" not in statement and "}" not in statement

    def test_nothing_in_bootstrap_destroys_anything(self, settings: SnowflakeSettings) -> None:
        # Rerunning bootstrap must be safe: it is the command people run when
        # they are unsure what state the account is in, and it may be pointed
        # at an account that holds more than this project.
        for statement in render(settings):
            assert not statement.startswith(("DROP", "TRUNCATE", "DELETE"))
            assert "OR REPLACE" not in statement

    def test_warehouse_settings_are_applied_even_when_it_already_exists(
        self, settings: SnowflakeSettings
    ) -> None:
        # CREATE WAREHOUSE IF NOT EXISTS does nothing to an existing warehouse,
        # so pointing this at a trial's COMPUTE_WH would leave AUTO_SUSPEND at
        # its default and bill ten idle minutes after every load.
        altered = [s for s in render(settings) if s.startswith("ALTER WAREHOUSE")]
        assert len(altered) == 1
        assert "AUTO_SUSPEND = 60" in altered[0]

    def test_system_roles_are_neither_created_nor_granted(self) -> None:
        # A trial hands you ACCOUNTADMIN. Creating it is an error and granting
        # it is a privilege change this script has no business making.
        as_admin = SnowflakeSettings(account="a", user="U", password="p", role="ACCOUNTADMIN")
        rendered = render(as_admin)
        assert not any(s.startswith(("CREATE ROLE", "GRANT ROLE")) for s in rendered)
        # The rest of the script still runs: schemas and the stage are the point.
        assert any(s.startswith("CREATE SCHEMA") for s in rendered)

    def test_a_custom_role_is_still_created_and_granted(self) -> None:
        rendered = render(
            SnowflakeSettings(account="a", user="U", password="p", role="ADTECH_ENGINEER")
        )
        assert any(s.startswith("CREATE ROLE") for s in rendered)
        assert any(s.startswith("GRANT ROLE") for s in rendered)

    def test_comments_do_not_become_statements(self) -> None:
        rendered = render(SnowflakeSettings(account="a", user="U", password="p"))
        assert not any(statement.startswith("--") for statement in rendered)

    def test_splitting_ignores_trailing_whitespace(self) -> None:
        assert statements("SELECT 1;\n\n  ;\nSELECT 2;") == ["SELECT 1", "SELECT 2"]

    def test_the_sql_file_has_no_placeholder_we_do_not_fill(self) -> None:
        import re

        placeholders = set(re.findall(r"\{(\w+)\}", BOOTSTRAP_SQL.read_text(encoding="utf-8")))
        assert placeholders <= {"role", "warehouse", "database", "user"}


class TestIdentifierChecking:
    @pytest.mark.parametrize("name", ["ADTECH", "adtech_wh", "A1_$x"])
    def test_accepts_valid_identifiers(self, name: str) -> None:
        assert check_identifier(name, setting="SNOWFLAKE_DATABASE") == name

    @pytest.mark.parametrize(
        "name",
        [
            "",
            "1ADTECH",
            "ad-tech",
            "ADTECH; DROP DATABASE OTHER",
            "ADTECH WH",
        ],
    )
    def test_rejects_anything_that_is_not_one(self, name: str) -> None:
        # These names are interpolated into SQL because an identifier cannot be
        # a bind parameter, so this check is the only thing standing between a
        # typo in .env and a statement that means something else.
        with pytest.raises(InvalidIdentifier):
            check_identifier(name, setting="SNOWFLAKE_DATABASE")


class TestTypeMapping:
    def test_decimals_keep_their_precision_and_scale(self) -> None:
        pa = pytest.importorskip("pyarrow")
        assert load.snowflake_type(pa.decimal128(14, 2)) == "NUMBER(14,2)"

    def test_money_never_becomes_a_float(self) -> None:
        pa = pytest.importorskip("pyarrow")
        assert "FLOAT" not in load.snowflake_type(pa.decimal128(18, 4))

    def test_timestamps_keep_their_zone_awareness(self) -> None:
        pa = pytest.importorskip("pyarrow")
        assert load.snowflake_type(pa.timestamp("us", tz="UTC")) == "TIMESTAMP_TZ"
        assert load.snowflake_type(pa.timestamp("us")) == "TIMESTAMP_NTZ"

    def test_an_unmapped_type_is_an_error_rather_than_a_guess(self) -> None:
        pa = pytest.importorskip("pyarrow")
        with pytest.raises(load.LoadError, match="no Snowflake type mapped"):
            load.snowflake_type(pa.list_(pa.string()))

    def test_the_copy_reads_parquet_logical_types(self) -> None:
        """`USE_LOGICAL_TYPE = TRUE` must stay on the COPY.

        It defaults to FALSE, and with it off Snowflake ignores Parquet's
        TIMESTAMP(MICROS) annotation and reads the raw int64 as epoch seconds:
        2026 lands as the year 54,934,202. Nothing fails while this happens -
        the COPY succeeds and the row count matches - so the only protection is
        that the option is there.
        """
        source = (PROJECT_ROOT / "warehouse" / "load.py").read_text(encoding="utf-8")
        assert "USE_LOGICAL_TYPE = TRUE" in source

    def test_the_plausible_window_covers_the_dataset(self) -> None:
        # The generator produces flights a year or two either side of now. A
        # window that clipped them would fail every load on correct data.
        low, high = load.PLAUSIBLE_YEARS
        assert low < 2024 and high > 2030

    def test_ddl_quotes_and_uppercases_column_names(self) -> None:
        pa = pytest.importorskip("pyarrow")
        schema = pa.schema(
            [
                pa.field("campaign_id", pa.string()),
                pa.field("daily_budget", pa.decimal128(14, 2)),
                pa.field("_lsn", pa.int64()),
            ]
        )
        sql = load.create_table_sql("campaigns", schema, database="ADTECH")
        assert sql.startswith("CREATE OR REPLACE TABLE ADTECH.RAW.CAMPAIGNS")
        assert '"DAILY_BUDGET" NUMBER(14,2)' in sql
        # Leading-underscore names are legal in Snowflake only when quoted.
        assert '"_LSN" NUMBER(19,0)' in sql


class TestDbtProject:
    """The dbt project parses and refers only to things that exist.

    `dbt parse` would check this properly, but it needs credentials to resolve
    a profile. These assertions cover the failures that are actually likely:
    a model naming a source that is not declared, or a schema config drifting
    away from the schemas bootstrap creates.
    """

    @staticmethod
    def _yaml(name: str) -> dict:
        return yaml.safe_load((DBT_DIR / name).read_text(encoding="utf-8"))

    def test_the_project_file_parses(self) -> None:
        project = self._yaml("dbt_project.yml")
        assert project["profile"] == "adtech"

    def test_model_schemas_match_the_ones_bootstrap_creates(self) -> None:
        layers = self._yaml("dbt_project.yml")["models"]["adtech"]
        configured = {
            config["+schema"]
            for config in layers.values()
            if isinstance(config, dict) and "+schema" in config
        }
        assert configured, "no layer declares a schema"
        assert configured <= set(SCHEMAS)

    def test_staging_is_views_and_core_is_tables(self) -> None:
        # Staging renames and types; materialising it would store a second copy
        # of RAW to save work that costs nothing. Core is joined and aggregated
        # by everything above it, so its hashes are computed once instead.
        layers = self._yaml("dbt_project.yml")["models"]["adtech"]
        assert layers["staging"]["+materialized"] == "view"
        assert layers["core"]["+materialized"] == "table"

    def test_no_layer_is_configured_before_it_has_models(self) -> None:
        # dbt warns about configured paths with no resources, and a warning on
        # every run is one people stop reading.
        layers = self._yaml("dbt_project.yml")["models"]["adtech"]
        configured = {key for key in layers if not key.startswith("+")}
        on_disk = {path.name for path in (DBT_DIR / "models").iterdir() if path.is_dir()}
        assert configured <= on_disk

    def test_sources_parse_and_declare_the_raw_schema(self) -> None:
        sources = yaml.safe_load(
            (DBT_DIR / "models" / "staging" / "_sources.yml").read_text(encoding="utf-8")
        )
        raw = sources["sources"][0]
        assert raw["name"] == "raw"
        assert raw["schema"] == "RAW"

    def test_every_staging_model_reads_a_declared_source(self) -> None:
        sources = yaml.safe_load(
            (DBT_DIR / "models" / "staging" / "_sources.yml").read_text(encoding="utf-8")
        )
        declared = {table["name"] for table in sources["sources"][0]["tables"]}

        import re

        for model in sorted((DBT_DIR / "models" / "staging").glob("stg_*.sql")):
            referenced = set(
                re.findall(r"source\(\s*'raw'\s*,\s*'(\w+)'\s*\)", model.read_text("utf-8"))
            )
            assert referenced, f"{model.name} reads no source"
            assert referenced <= declared, f"{model.name} reads undeclared {referenced - declared}"

    def test_every_declared_source_has_a_staging_model(self) -> None:
        # The other direction: a table loaded into RAW that nothing stages is a
        # table paying storage for nothing.
        sources = yaml.safe_load(
            (DBT_DIR / "models" / "staging" / "_sources.yml").read_text(encoding="utf-8")
        )
        declared = {table["name"].lower() for table in sources["sources"][0]["tables"]}
        staged = {
            path.stem[len("stg_") :] for path in (DBT_DIR / "models" / "staging").glob("stg_*.sql")
        }
        assert declared == staged

    def test_no_staging_model_selects_star(self) -> None:
        for model in (DBT_DIR / "models" / "staging").glob("stg_*.sql"):
            assert "select *" not in model.read_text("utf-8").lower()

    def test_documented_models_all_exist(self) -> None:
        documented = {
            model["name"]
            for model in yaml.safe_load(
                (DBT_DIR / "models" / "staging" / "_models.yml").read_text(encoding="utf-8")
            )["models"]
        }
        on_disk = {path.stem for path in (DBT_DIR / "models" / "staging").glob("stg_*.sql")}
        assert documented == on_disk

    def test_the_profile_holds_no_literal_credentials(self) -> None:
        """Every connection value is read from the environment.

        This is what makes profiles.yml safe to commit, so it is worth an
        assertion rather than a convention - the file is one careless edit away
        from carrying a real account identifier into git history.
        """
        secretish = {"account", "user", "password", "private_key_path", "private_key_passphrase"}
        outputs = yaml.safe_load((DBT_DIR / "profiles.yml").read_text(encoding="utf-8"))
        for name, output in outputs["adtech"]["outputs"].items():
            for key in secretish & set(output):
                assert "env_var(" in str(output[key]), f"{name}.{key} is a literal"

    def test_custom_schemas_are_used_as_written(self) -> None:
        """The `generate_schema_name` override is present and does its job.

        Without it dbt builds `<target.schema>_<custom schema>`, so everything
        configured for STAGING lands in STAGING_STAGING and the schemas
        bootstrap created stay empty. It fails quietly - the run succeeds and
        the tests pass against tables nothing else can find - which is exactly
        the kind of thing worth pinning down once it has bitten.
        """
        macro = (DBT_DIR / "macros" / "generate_schema_name.sql").read_text(encoding="utf-8")
        assert "macro generate_schema_name" in macro
        assert "custom_schema_name | trim" in macro
        assert "target.schema ~" not in macro and "_{{" not in macro

    def test_both_authentication_targets_exist(self) -> None:
        # The Makefile picks between these by name, so renaming one here would
        # fail at connection time rather than at parse time.
        outputs = yaml.safe_load((DBT_DIR / "profiles.yml").read_text(encoding="utf-8"))
        assert set(outputs["adtech"]["outputs"]) == {"key_pair", "password"}


class TestCoreDimensions:
    """The Type 1 dimensions, and the properties that make them joinable.

    A dimension is wrong in ways a dbt run will not notice: a key built from a
    row number looks fine until the next rebuild renumbers it under the facts,
    and a missing unknown member looks fine until a reference arrives that
    cannot be resolved. Both are cheap to assert here and expensive to discover
    in phase 12.
    """

    CORE = DBT_DIR / "models" / "core"

    #: dim_date is a dimension but not one of these. It is generated from a
    #: spine rather than loaded, so it has no staging model to read, no natural
    #: key to hash and no unknown member to carry - every assertion below is
    #: about dimensions that come from the source, and it is none of them.
    GENERATED: ClassVar[set[str]] = {"dim_date"}

    @classmethod
    def _models(cls) -> list[Path]:
        return sorted(
            path
            for path in cls.CORE.glob("dim_*.sql")
            if not path.stem.endswith("_history") and path.stem not in cls.GENERATED
        )

    @classmethod
    def _documented(cls) -> list[dict]:
        models = yaml.safe_load((cls.CORE / "_models.yml").read_text(encoding="utf-8"))["models"]
        return [
            model
            for model in models
            if not model["name"].endswith("_history")
            and model["name"] not in cls.GENERATED
            and not model["name"].startswith("fct_")
        ]

    def test_every_staging_model_has_a_dimension(self) -> None:
        # Only the master tables. The event tables are staged too, and what
        # they feed is a fact - asserting a dim_impressions exists would be
        # asserting the model is wrong.
        staged = {
            path.stem[len("stg_") :].rstrip("s")
            for path in (DBT_DIR / "models" / "staging").glob("stg_*.sql")
            if path.stem[len("stg_") :] not in EVENT_TABLES
        }
        dimensions = {path.stem[len("dim_") :] for path in self._models()}
        assert dimensions == staged

    def test_every_dimension_reads_its_staging_model(self) -> None:
        # Core reads staging, never a source. A dimension reaching back to RAW
        # would skip the renames and the types that staging exists to apply.
        import re

        for model in self._models():
            body = model.read_text("utf-8")
            assert re.search(r"ref\(\s*'stg_\w+'\s*\)", body), (
                f"{model.name} reads no staging model"
            )
            assert "source(" not in body, f"{model.name} reaches past staging to RAW"

    def test_surrogate_keys_are_hashed_rather_than_numbered(self) -> None:
        """A key has to survive a rebuild.

        `row_number()` and an identity column both produce smaller keys and
        better joins, and both renumber when the dimension is rebuilt - which
        silently repoints every fact built against the previous run. A hash of
        the natural key cannot do that, and the cost is a wider column.
        """
        for model in self._models():
            body = model.read_text("utf-8").lower()
            assert "generate_surrogate_key" in body, f"{model.name} builds no surrogate key"
            assert "row_number()" not in body, f"{model.name} numbers its key"
            assert "autoincrement" not in body and "identity(" not in body

    def test_every_dimension_carries_an_unknown_member(self) -> None:
        for model in self._models():
            body = model.read_text("utf-8")
            assert "unknown_key()" in body, f"{model.name} has no unknown member"
            assert "union all" in body, f"{model.name} never unions it in"

    def test_the_unknown_sentinel_cannot_be_a_real_key(self) -> None:
        """The sentinel's shape is what guarantees it never collides.

        `generate_surrogate_key` returns md5 hex, so any sentinel containing a
        non-hex character is unreachable by construction. Swapping it for
        something hex - '0' * 32 is the tempting one - would reintroduce a
        collision that fails by misattributing rows rather than by raising.
        """
        macro = (DBT_DIR / "macros" / "dimensions.sql").read_text(encoding="utf-8")
        sentinel = macro.split("'")[1]
        assert not all(character in "0123456789abcdefABCDEF" for character in sentinel)

    def test_every_dimension_key_is_asserted_unique_and_present(self) -> None:
        for model in self._documented():
            key = f"{model['name'][len('dim_') :]}_key"
            columns = {column["name"]: column for column in model["columns"]}
            assert key in columns, f"{model['name']} does not document {key}"
            assert set(columns[key]["tests"]) >= {"unique", "not_null", "has_unknown_member"}

    def test_nothing_filters_deleted_members_out(self) -> None:
        """Dimensions keep every member a fact might reference.

        Several hundred audience segments are deleted at source. The generator
        only removes ones no impression references, so filtering them would not
        break today's data - which is the point: that is a property of the
        generator's WHERE clause, not of the warehouse, and a filter added here
        on the strength of it fails the first time a real source deletes
        something that is in use.
        """
        paths = [*self._models(), *(DBT_DIR / "models" / "staging").glob("stg_*.sql")]
        for model in paths:
            # Comments are stripped first, because the models that explain why
            # they do not filter necessarily quote the filter they are not
            # applying.
            body = "\n".join(
                line
                for line in model.read_text("utf-8").lower().splitlines()
                if not line.lstrip().startswith("--")
            )
            assert "not is_deleted" not in body, f"{model.name} filters deleted members"
            assert "is_deleted = false" not in body

    def test_documented_dimensions_all_exist(self) -> None:
        assert {model["name"] for model in self._documented()} == {
            path.stem for path in self._models()
        }


class TestHistoryDimensions:
    """The Type 2 dimensions and the snapshots underneath them.

    These assertions are about the two decisions that would be expensive to
    reverse: which strategy the snapshots detect changes with, and whether the
    validity ranges mean business time or processing time. Both are invisible
    in a dbt run - a snapshot that records nothing still succeeds.
    """

    CORE = DBT_DIR / "models" / "core"
    SNAPSHOTS = DBT_DIR / "snapshots"

    @classmethod
    def _models(cls) -> list[Path]:
        return sorted(cls.CORE.glob("dim_*_history.sql"))

    @classmethod
    def _snapshots(cls) -> list[dict]:
        return yaml.safe_load((cls.SNAPSHOTS / "_snapshots.yml").read_text(encoding="utf-8"))[
            "snapshots"
        ]

    def test_every_master_table_is_snapshotted(self) -> None:
        # Every one, including the entities that rarely change, because a
        # snapshot started late cannot be backfilled: the states it would have
        # recorded no longer exist anywhere to read them from.
        #
        # The event tables are the exception, and not an omission. They are
        # append-only: a row is written once and never updated, so a snapshot
        # of one would record a hundred million first versions and never a
        # second. History is something a dimension has.
        staged = {
            path.stem[len("stg_") :]
            for path in (DBT_DIR / "models" / "staging").glob("*.sql")
            if path.stem[len("stg_") :] not in EVENT_TABLES
        }
        captured = {snapshot["name"][len("snap_") :] for snapshot in self._snapshots()}
        assert captured == staged

    def test_snapshots_detect_changes_by_checking_columns(self) -> None:
        """The check strategy, not timestamp.

        A delete at source is a DELETE, not an UPDATE, so it never moves
        `updated_at`. Silver turns it into `is_deleted` flipping true while
        every timestamp stays put, and a timestamp snapshot compares
        timestamps - it would record the removal of 546 audience segments as
        nothing at all, and raise nothing while doing it.
        """
        for snapshot in self._snapshots():
            config = snapshot["config"]
            assert config["strategy"] == "check", f"{snapshot['name']} is not a check snapshot"
            assert "is_deleted" in config["check_cols"], f"{snapshot['name']} ignores deletions"

    def test_snapshots_ignore_columns_that_move_without_a_change(self) -> None:
        # check_cols: all would include the CDC lineage columns, which change on
        # every event whether or not the row's content did. A no-op update would
        # record a version of a change nobody made.
        noise = {"source_lsn", "source_operation", "source_event_at", "updated_at"}
        for snapshot in self._snapshots():
            checked = set(snapshot["config"]["check_cols"])
            assert checked.isdisjoint(noise), f"{snapshot['name']} checks {checked & noise}"

    def test_snapshots_are_kept_out_of_the_rebuilt_schemas(self) -> None:
        """Snapshots accumulate; every other schema here is derived.

        CORE can be dropped and rebuilt from RAW at any time. A dropped
        snapshot is change history that no rerun recovers, because the states
        it recorded are gone from the source. The schema boundary is what makes
        that difference visible in the layout rather than only in a document.
        """
        project = yaml.safe_load((DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))
        assert project["snapshots"]["adtech"]["+schema"] == SNAPSHOT_SCHEMA
        assert SNAPSHOT_SCHEMA in BOOTSTRAP_SQL.read_text(encoding="utf-8")

    def test_validity_is_business_time_rather_than_run_time(self) -> None:
        """dbt's own valid_from records when the snapshot ran, not when the
        change happened.

        Under the check strategy that is the run timestamp, so renaming
        dbt_valid_from to valid_from would date every version to the first run
        and leave every historical fact outside every range. The models build
        their own ranges and keep dbt's column under a name that says what it
        is.
        """
        for model in self._models():
            body = model.read_text("utf-8")
            assert "dbt_valid_from as captured_at" in body, f"{model.name} hides processing time"
            assert "version_started_at" in body, f"{model.name} does not derive business validity"
            assert "dbt_valid_from as valid_from" not in body

    def test_every_version_window_is_closed(self) -> None:
        # A null valid_to needs a coalesce in every range join written from here
        # on, and the one place it gets forgotten drops every current row.
        for model in self._models():
            assert "end_of_time()" in model.read_text("utf-8"), f"{model.name} leaves ranges open"

    def test_version_keys_are_not_built_from_valid_from(self) -> None:
        """Business validity ties, so it cannot identify a version.

        Two versions share a valid_from whenever the timestamps behind them tie
        - a deletion whose deleted_at precedes the updated_at it supersedes,
        which the UTC/local skew between CDC and generator timestamps produces
        regularly. The earlier version is then zero-length, which is correct,
        and shares a key with its successor, which silently merges them.
        """
        for model in self._models():
            body = model.read_text("utf-8")
            assert "'dbt_valid_from']) }} as" in body.replace("\n", ""), (
                f"{model.name} does not key its versions on capture time"
            )

    def test_the_contiguity_test_rejects_gaps_as_well_as_overlaps(self) -> None:
        # Overlaps double a fact's measures and gaps lose them. Both pass every
        # column-level test, because neither is a property of one row.
        test = (DBT_DIR / "tests" / "generic" / "versions_are_contiguous.sql").read_text("utf-8")
        assert "!=" in test and "lead(" in test


class TestResumableStaging:
    """Uploading 13 GB has to survive the connection dropping.

    The first version of `stage` cleared the stage and sent everything in one
    `PUT ... OVERWRITE = TRUE`. It lost two and a half hours to a TLS read
    error on file 27 of 101: the connector reports a mid-upload failure as
    non-retryable, fails the statement, and the ninety files that had not
    started go with it.

    A fake cursor is enough to assert the behaviour that fixes it, because the
    behaviour is entirely about which statements get issued in which order.
    """

    class FakeCursor:
        """Enough of a Snowflake cursor to answer LIST and fail a PUT on cue."""

        def __init__(self, staged: set[str], fail_times: int = 0, lands: int = 99) -> None:
            self.staged = set(staged)
            self.fail_times = fail_times
            self.lands = lands
            self.pending: list[str] = []
            self.statements: list[str] = []

        def execute(self, sql: str) -> None:
            self.statements.append(sql)
            if sql.startswith("LIST"):
                self.pending = sorted(self.staged)
            elif sql.startswith("REMOVE"):
                self.staged.discard(sql.rsplit("/", 1)[-1])
            elif sql.startswith("PUT"):
                arriving = sorted(self.wanted - self.staged)[: self.lands]
                self.staged |= set(arriving)
                if self.fail_times:
                    self.fail_times -= 1
                    raise RuntimeError("non-retryable TLS error")

        def fetchall(self) -> list[tuple[str, int]]:
            return [(f"stage/impressions/{name}", 0) for name in self.pending]

    @pytest.fixture(autouse=True)
    def _no_backoff(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The pause between resumes is there for a flaky uplink, not for a
        # test suite, and twenty of them is five minutes of waiting to assert
        # something that has nothing to do with time.
        monkeypatch.setattr(load, "PUT_RETRY_SECONDS", 0)

    def _run(self, cursor: FakeCursor, names: list[str]) -> None:
        cursor.wanted = {*names}  # type: ignore[attr-defined]
        load.stage(
            cursor,
            table="impressions",
            stage_path="@ADTECH.RAW.SILVER_STAGE/impressions",
            local=[Path("/tmp/impressions") / name for name in names],
        )

    def test_a_fully_staged_table_is_not_uploaded_again(self) -> None:
        names = ["a.parquet", "b.parquet"]
        cursor = self.FakeCursor(staged=set(names))
        self._run(cursor, names)
        assert not [sql for sql in cursor.statements if sql.startswith("PUT")]

    def test_an_interrupted_upload_resumes_instead_of_restarting(self) -> None:
        # Two files are already up. The first PUT lands one more and then
        # fails; the resume has to finish the rest rather than start over.
        names = [f"part-{index}.parquet" for index in range(6)]
        cursor = self.FakeCursor(staged={names[0], names[1]}, fail_times=1, lands=1)
        self._run(cursor, names)
        assert cursor.staged == set(names)

    def test_only_files_the_export_no_longer_has_are_removed(self) -> None:
        """A blanket REMOVE is what made resuming impossible.

        It was there for a real reason - a load following a smaller export
        would otherwise COPY the previous one's leftovers into the table - so
        the stale files still have to go, just not the ones still wanted.
        """
        names = ["keep.parquet"]
        cursor = self.FakeCursor(staged={"keep.parquet", "leftover.parquet"})
        self._run(cursor, names)

        removed = [sql for sql in cursor.statements if sql.startswith("REMOVE")]
        assert len(removed) == 1
        assert removed[0].endswith("leftover.parquet")

    def test_the_upload_does_not_overwrite_what_is_already_there(self) -> None:
        # OVERWRITE = TRUE is what turns a resume back into a full re-upload,
        # and it does it without failing or saying anything.
        names = ["a.parquet"]
        cursor = self.FakeCursor(staged=set())
        self._run(cursor, names)
        put = next(sql for sql in cursor.statements if sql.startswith("PUT"))
        assert "OVERWRITE = FALSE" in put

    def test_giving_up_says_how_to_carry_on(self) -> None:
        names = ["a.parquet"]
        cursor = self.FakeCursor(staged=set(), fail_times=load.PUT_ATTEMPTS + 1, lands=0)
        with pytest.raises(load.LoadError, match="resumes from here"):
            self._run(cursor, names)

    def test_a_put_that_succeeds_without_sending_everything_is_not_trusted(self) -> None:
        """Returning without raising is not the same as having uploaded.

        Looping until the statement stops throwing would stop here with files
        missing, and the COPY would then load a table that is quietly short.
        """
        names = [f"part-{index}.parquet" for index in range(4)]
        cursor = self.FakeCursor(staged=set(), lands=1)
        self._run(cursor, names)
        assert cursor.staged == set(names)
        assert len([sql for sql in cursor.statements if sql.startswith("PUT")]) == 4


class TestFacts:
    """The four fact tables, and the properties that survive 100 million rows.

    Everything here is a mistake that a successful `dbt build` would not
    report. A fact rebuilt in full instead of incrementally still produces the
    right answer, at several minutes of warehouse time per run. A range join
    against a Type 2 dimension that fans out still produces a table, with every
    measure in it multiplied. Both are cheap to assert from the SQL text and
    expensive to notice from a row count that looks plausible.
    """

    CORE = DBT_DIR / "models" / "core"

    #: fct_spend is the one name that is not its staging model with the prefix
    #: swapped: the source table is spend_transactions, but the fact is about
    #: spend and the transaction is a degenerate dimension on it.
    FACT_FOR_TABLE: ClassVar[dict[str, str]] = {
        "impressions": "fct_impressions",
        "clicks": "fct_clicks",
        "conversions": "fct_conversions",
        "spend_transactions": "fct_spend",
    }

    #: Identifiers with no dimension behind them. Any other `*_id` selected
    #: straight onto a fact is a UUID being stored next to the hash of itself.
    DEGENERATE: ClassVar[set[str]] = {
        "impression_id",
        "click_id",
        "conversion_id",
        "spend_transaction_id",
    }

    @classmethod
    def _models(cls) -> list[Path]:
        return sorted(cls.CORE.glob("fct_*.sql"))

    def test_every_event_table_has_a_fact(self) -> None:
        assert set(EVENT_TABLES) == set(self.FACT_FOR_TABLE)
        on_disk = {path.stem for path in self._models()}
        assert on_disk == set(self.FACT_FOR_TABLE.values())

    def test_every_fact_reads_its_own_staging_model(self) -> None:
        import re

        for table, fact in self.FACT_FOR_TABLE.items():
            body = (self.CORE / f"{fact}.sql").read_text("utf-8")
            assert re.search(rf"ref\(\s*'stg_{table}'\s*\)", body), f"{fact} does not stage {table}"
            assert "source(" not in body, f"{fact} reaches past staging to RAW"

    def test_every_fact_is_incremental(self) -> None:
        """A full rebuild is the default, and the wrong one here.

        `table` would recompute 125 million rows on every run to arrive at the
        same answer. That is correct and unaffordable, and nothing in a dbt run
        distinguishes it from the cheap version.
        """
        for model in self._models():
            body = model.read_text("utf-8")
            assert "materialized='incremental'" in body, f"{model.name} rebuilds in full"
            assert "incremental_watermark(" in body, f"{model.name} reprocesses everything"

        # Incremental materialisation without a predicate is the quiet version
        # of the same mistake: dbt merges the whole source into the target on
        # every run and reports it as an incremental build.
        macro = (DBT_DIR / "macros" / "facts.sql").read_text(encoding="utf-8")
        assert "is_incremental()" in macro

    def test_every_fact_merges_on_its_grain(self) -> None:
        """The watermark overlaps on purpose, so the load has to be idempotent.

        Reprocessing from `>=` the last timestamp re-reads the rows on the
        boundary, which is the price of not dropping the ones that tie with it.
        `append` would turn that overlap into duplicates; a unique_key turns it
        into an update.
        """
        for model in self._models():
            body = model.read_text("utf-8")
            assert "unique_key=" in body, f"{model.name} has no key to merge on"
            assert "incremental_strategy='merge'" in body
            assert "incremental_strategy='append'" not in body
            assert "merge_floor()" in body, f"{model.name} merges against its whole history"

    def test_the_merge_floor_names_core_rather_than_asking_this(self) -> None:
        """`{{ this }}` is the obvious way to name the target and the wrong one.

        dbt resolves config before generate_schema_name runs, so at that point
        `this` still carries the profile's schema - STAGING - and the predicate
        reads an empty relation without complaining. Naming the schema from the
        var is what avoids it, which only works while the var and the models
        config agree.
        """
        macro = (DBT_DIR / "macros" / "facts.sql").read_text(encoding="utf-8")
        # Past the `-#}` that closes the doc comment, which necessarily quotes
        # the expression the macro is explaining why it does not use.
        floor = macro.split("macro merge_floor")[1].split("-#}")[1].split("endmacro")[0]
        assert "var('core_schema')" in floor
        assert "{{ this }}" not in floor

        project = yaml.safe_load((DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))
        assert project["vars"]["core_schema"] == project["models"]["adtech"]["core"]["+schema"]

    def test_the_watermark_includes_its_own_boundary(self) -> None:
        # `>` is the obvious choice and loses every event that shares the
        # maximum timestamp - which at 100 million rows is not a rare case.
        macro = (DBT_DIR / "macros" / "facts.sql").read_text(encoding="utf-8")
        assert ">= (" in macro and "> (\n" not in macro

    def test_every_fact_asserts_its_own_grain(self) -> None:
        """`unique` on the degenerate key is the fan-out test.

        A range join whose validity windows overlap multiplies the fact and
        every measure on it, and the result still loads, still joins and still
        looks like a fact table. This assertion is what notices.
        """
        documented = {
            model["name"]: model
            for model in yaml.safe_load((self.CORE / "_models.yml").read_text("utf-8"))["models"]
            if model["name"].startswith("fct_")
        }
        assert set(documented) == {path.stem for path in self._models()}

        grain = {
            "fct_impressions": "impression_id",
            "fct_clicks": "click_id",
            "fct_conversions": "conversion_id",
            "fct_spend": "spend_transaction_id",
        }
        for fact, key in grain.items():
            columns = {column["name"]: column for column in documented[fact]["columns"]}
            assert key in columns, f"{fact} does not document its grain"
            assert set(columns[key].get("tests", [])) >= {"unique", "not_null"}

    def test_point_in_time_joins_go_through_the_macro(self) -> None:
        """One place where the range join is written, and one place to get it right.

        A hand-written `between valid_from and valid_to` is inclusive at both
        ends, so an event landing exactly on a boundary matches two versions
        and doubles. The macro is half-open for that reason, and the only way
        to keep that guarantee is for nothing to write the join by hand.
        """
        for model in self._models():
            body = model.read_text("utf-8")
            assert "join_version(" in body, f"{model.name} resolves no versions"
            assert "valid_from" not in body, f"{model.name} writes a range join by hand"

    def test_no_fact_joins_to_the_date_dimension(self) -> None:
        # date_key is arithmetic on a timestamp the row already has. Joining
        # dim_date to look it up is 125 million lookups for a known answer.
        for model in self._models():
            assert "ref('dim_date')" not in model.read_text("utf-8")

    def test_no_fact_stores_a_uuid_next_to_the_hash_of_it(self) -> None:
        import re

        for model in self._models():
            selected = set(re.findall(r"^\s+e\.(\w+_id),", model.read_text("utf-8"), re.MULTILINE))
            extra = selected - self.DEGENERATE
            assert not extra, f"{model.name} carries {extra} alongside their surrogate keys"

    def test_version_keys_are_always_joinable(self) -> None:
        """An unresolved version has to become the unknown member, not a null.

        A null key passes `relationships`, because an anti-join ignores nulls -
        so the one state worth noticing would be the one state nothing reports.
        Coalescing makes it a row that can be counted, which is what
        resolves_to_a_version then counts.
        """
        for model in self._models():
            body = model.read_text("utf-8")
            version_keys = body.count("_version_key")
            assert body.count("coalesce(") >= version_keys - body.count("as "), (
                f"{model.name} leaves a version key null"
            )
            for line in body.splitlines():
                if line.strip().endswith("_version_key") and "as " in line:
                    assert "coalesce(" in line, f"{model.name}: {line.strip()} can be null"

    def test_the_date_dimension_is_generated_rather_than_loaded(self) -> None:
        body = (self.CORE / "dim_date.sql").read_text(encoding="utf-8")
        assert "date_spine" in body
        assert "ref(" not in body and "source(" not in body

    def test_the_date_dimension_has_no_unknown_member(self) -> None:
        """Deliberately, and the reason matters.

        An unknown member absorbs foreign keys that are null or point nowhere.
        Neither is reachable for a date: every event timestamp is NOT NULL at
        the source, and date_key is computed rather than looked up. What can
        happen is a date outside the spine, and that should fail loudly - the
        fix is to widen the window, not to file the row under Unknown.
        """
        body = (self.CORE / "dim_date.sql").read_text(encoding="utf-8")
        assert "unknown_key()" not in body

    def test_the_date_spine_still_covers_the_simulation(self) -> None:
        """The generator's clock is today's, so the spine has to stay ahead of it.

        simulation_end_date defaults to the day the generator runs, which means
        a window that was comfortable when it was written stops covering the
        data by simply being left alone. This fails a year before that happens,
        while widening it is still a one-line config change rather than an
        incident.
        """
        from datetime import date, timedelta

        project = yaml.safe_load((DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))
        start = date.fromisoformat(project["vars"]["date_spine_start"])
        end = date.fromisoformat(project["vars"]["date_spine_end"])

        today = date.today()
        assert start < today, "the spine starts after the data does"
        assert end > today + timedelta(days=365), "the spine runs out within a year"


class TestExportLayout:
    def test_the_export_prefix_is_outside_the_iceberg_warehouse(self) -> None:
        from spark import catalog, export

        # Plain Parquet under the warehouse root would sit among Iceberg's own
        # metadata, where a reader has no way to tell it is not part of a table.
        assert not export.EXPORT_PREFIX.startswith(catalog.WAREHOUSE_PREFIX)

    def test_table_prefixes_do_not_collide(self) -> None:
        from spark import export

        prefixes = {export.table_prefix(name) for name in ("campaigns", "campaign")}
        assert len(prefixes) == 2


def test_the_env_template_documents_every_setting_from_env_is_read() -> None:
    """Anything `from_env` reads should be findable in .env.example.

    A setting that exists only in code is a setting nobody knows to set.
    """
    template = (Path(PROJECT_ROOT) / ".env.example").read_text(encoding="utf-8")
    source = (Path(PROJECT_ROOT) / "warehouse" / "settings.py").read_text(encoding="utf-8")

    import re

    for name in sorted(set(re.findall(r'os\.environ\.get\("(SNOWFLAKE_\w+)"', source))):
        assert name in template, f"{name} is read but not documented in .env.example"
