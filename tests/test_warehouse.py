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

import pytest
import yaml

from data_generator.config import PROJECT_ROOT
from warehouse import load
from warehouse.bootstrap import (
    BOOTSTRAP_SQL,
    InvalidIdentifier,
    check_identifier,
    render,
    statements,
)
from warehouse.settings import SCHEMAS, SnowflakeSettings, WarehouseNotConfigured

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

    def test_staging_is_materialised_as_views(self) -> None:
        # Staging renames and types; materialising it would store a second copy
        # of RAW to save work that costs nothing. Core becomes tables in phase
        # 10, when there are models in it to configure.
        layers = self._yaml("dbt_project.yml")["models"]["adtech"]
        assert layers["staging"]["+materialized"] == "view"

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
