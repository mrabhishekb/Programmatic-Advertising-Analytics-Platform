"""The orchestration layer is checked without a scheduler running.

Airflow is not a dependency of this project - it exists only inside the image
in ``docker/airflow.Dockerfile``, so these tests cannot import the DAG and ask
it questions. They read it instead, the same way ``test_warehouse.py`` reads
the dbt models it cannot execute locally.

That turns out to suit what is worth asserting. The properties that matter in
phase 14 are not runtime behaviour - they are decisions written into the DAG
and the Compose file that are invisible until the night they cost something:
a quality gate placed after the load instead of before it, a retry on the one
task that must not retry, catchup left on so unpausing queues hundreds of
Snowflake builds at once. Every test below is a failure that was either hit
while building this phase or is one config edit away.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest
import yaml

from data_generator.config import PROJECT_ROOT

DAG_PATH = PROJECT_ROOT / "airflow" / "dags" / "adtech_pipeline.py"

#: Tasks that reach Snowflake. Nothing here may run before the gate.
WAREHOUSE_TASKS = frozenset({"warehouse_export", "warehouse_load", "dbt_build"})

GATE = "validate_silver"


# ---------------------------------------------------------------------------
# Reading the DAG
# ---------------------------------------------------------------------------


def _literal(node: ast.AST) -> Any:
    """Best-effort value of a keyword argument.

    The DAG is read, not executed, so anything computed - an f-string, a
    ``timedelta`` call - has no value here. Those come back as ``None`` and
    the tests that care about them look at the source text instead.
    """
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError):
        return None


def _calls_named(tree: ast.AST, name: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name
    ]


def _kwargs(call: ast.Call) -> dict[str, ast.AST]:
    return {kw.arg: kw.value for kw in call.keywords if kw.arg}


@pytest.fixture(scope="module")
def dag_source() -> str:
    return DAG_PATH.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def dag_tree(dag_source) -> ast.AST:
    return ast.parse(dag_source)


@pytest.fixture(scope="module")
def dag_kwargs(dag_tree) -> dict[str, ast.AST]:
    calls = _calls_named(dag_tree, "DAG")
    assert len(calls) == 1, "expected exactly one DAG in this file"
    return _kwargs(calls[0])


@pytest.fixture(scope="module")
def tasks(dag_tree) -> dict[str, dict[str, ast.AST]]:
    """Every operator in the file, keyed by task_id."""
    found = {}
    for call in _calls_named(dag_tree, "BashOperator"):
        kwargs = _kwargs(call)
        task_id = _literal(kwargs["task_id"])
        found[task_id] = kwargs
    assert found, "no tasks found - did the operator import change?"
    return found


@pytest.fixture(scope="module")
def chain(dag_tree) -> list[str]:
    """Task ids in dependency order, read off the ``>>`` expression.

    ``a >> b >> c`` parses as a left-nested BinOp, so flattening it recovers
    the order the tasks were written in.
    """

    def flatten(node: ast.AST) -> list[str]:
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
            return flatten(node.left) + flatten(node.right)
        if isinstance(node, ast.Name):
            return [node.id]
        return []

    for node in ast.walk(dag_tree):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.BinOp):
            names = flatten(node.value)
            if len(names) > 1:
                return names
    pytest.fail("no `>>` dependency chain found in the DAG")


@pytest.fixture(scope="module")
def bash_commands(tasks, dag_source) -> dict[str, str]:
    """The ``bash_command`` of each task, as source text.

    Only the commands, deliberately. Asserting against the whole file catches
    the prose: this module's own docstrings name ``--include-events`` while
    explaining why no task uses it, and a test that reads the file top to
    bottom cannot tell the difference between a command and an explanation.
    """
    return {
        task_id: ast.get_source_segment(dag_source, kwargs["bash_command"])
        for task_id, kwargs in tasks.items()
    }


@pytest.fixture(scope="module")
def default_args(dag_tree) -> dict[str, ast.AST]:
    for node in ast.walk(dag_tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "default_args" for t in node.targets
        ):
            assert isinstance(node.value, ast.Dict)
            return {_literal(k): v for k, v in zip(node.value.keys, node.value.values, strict=True)}
    pytest.fail("default_args not found")


# ---------------------------------------------------------------------------
# Reading the Compose file
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def airflow_services(compose) -> dict[str, dict]:
    return {
        name: service
        for name, service in compose["services"].items()
        if name.startswith("airflow-")
    }


@pytest.fixture(scope="module")
def airflow_common(compose) -> dict:
    """The ``x-airflow-common`` anchor every Airflow service merges in."""
    anchor = compose.get("x-airflow-common")
    assert anchor, "x-airflow-common missing: the services would each need their own copy"
    return anchor


class TestTheGate:
    """The reason this phase exists.

    ``data_quality/silver.py`` has exited non-zero on failure since phase 8,
    with a comment saying a scheduler would eventually read it. Nothing did:
    `make silver-reconcile` followed by `make warehouse-export` is two
    commands with nothing between them, so Silver disagreeing with PostgreSQL
    would be exported, loaded and aggregated into the gold marts before anyone
    looked at a report.

    These assert the exit code now stops something, and keeps stopping it.
    """

    def test_the_gate_runs_before_anything_reaches_snowflake(self, chain):
        assert GATE in chain, f"{GATE} is not in the dependency chain at all"
        gate_at = chain.index(GATE)
        for task in WAREHOUSE_TASKS:
            assert task in chain, f"{task} missing from the chain"
            assert gate_at < chain.index(task), (
                f"{task} is ordered before the quality gate, so bad data reaches "
                "Snowflake and the gate only reports on it afterwards"
            )

    def test_the_gate_runs_after_the_data_it_is_checking_is_built(self, chain):
        assert chain.index("silver_reconcile") < chain.index(GATE), (
            "the gate would validate the previous run's Silver, not this one's"
        )

    def test_the_gate_does_not_retry(self, tasks):
        retries = _literal(tasks[GATE].get("retries"))
        assert retries == 0, (
            "the gate inherits retries from default_args unless it sets its own. "
            "Bad data is not a transient fault: three more attempts are three "
            "more identical failures, and the alert arrives twenty minutes late"
        )

    def test_the_gate_is_the_only_task_that_does_not_retry(self, tasks, default_args):
        assert _literal(default_args["retries"]) > 0, (
            "the pipeline has genuinely transient failures - MinIO refusing a "
            "connection while it restarts, Snowflake dropping a TLS session"
        )
        for task_id, kwargs in tasks.items():
            if task_id == GATE:
                continue
            assert _literal(kwargs.get("retries")) != 0, (
                f"{task_id} opted out of retries; only the gate should"
            )

    def test_the_chain_is_linear(self, chain, tasks):
        assert len(chain) == len(tasks) == 5, (
            "a task exists that is not in the chain, which means it runs "
            "unordered against everything else"
        )


class TestTheSchedule:
    """Defaults that are cheap to leave wrong and expensive to leave running."""

    def test_catchup_is_off(self, dag_kwargs):
        assert _literal(dag_kwargs.get("catchup")) is False, (
            "with catchup on, unpausing queues one run per day since the start "
            "date - hundreds of parallel Snowflake builds on a trial account"
        )

    def test_only_one_run_happens_at_a_time(self, dag_kwargs):
        assert _literal(dag_kwargs.get("max_active_runs")) == 1, (
            "two concurrent runs would write the same Iceberg tables and merge "
            "into the same incremental models"
        )

    def test_the_dag_arrives_paused(self, dag_kwargs):
        assert _literal(dag_kwargs.get("is_paused_upon_creation")) is True, (
            "relying on the instance-wide default means one config edit turns "
            "this into a nightly Snowflake build nobody asked for"
        )

    def test_every_task_has_a_timeout(self, tasks, default_args):
        assert "execution_timeout" in default_args, (
            "without one, a task that hangs holds max_active_runs=1 forever and "
            "the pipeline simply stops, with nothing marked failed"
        )


class TestTheTasksShellOut:
    """Each task runs the command a person would, rather than a copy of it.

    A DAG that reconstructs what a Spark job needs - image, network, mounts,
    a dozen environment variables - is a second docker-compose.yml, and the
    two drift the first time either is edited. Compose stays the definition of
    what a run is; the DAG decides when one happens.
    """

    def test_the_spark_tasks_go_through_compose(self, dag_source):
        spark_run = [line for line in dag_source.splitlines() if line.startswith("SPARK_RUN =")]
        assert spark_run, "SPARK_RUN is gone; the Spark tasks no longer use Compose"
        assert "docker compose" in spark_run[0]
        assert "--rm" in spark_run[0], (
            "without --rm a failed task leaves a stopped container for the next run to trip over"
        )

    def test_the_daily_run_does_not_upload_the_event_tables(self, bash_commands):
        for task_id, command in bash_commands.items():
            assert "--include-events" not in command, (
                f"{task_id} uploads the event tables: 100M rows and a multi-hour "
                "job. A daily schedule carrying it either never finishes or "
                "spends three hours a night moving a few thousand changed "
                "dimension rows"
            )

    def test_dbt_uses_build_rather_than_run_then_test(self, bash_commands):
        assert "dbt build" in bash_commands["dbt_build"], (
            "`run` then `test` lets a model whose test failed become the input "
            "to the model above it"
        )

    def test_dbt_picks_its_target_the_same_way_the_rest_of_the_project_does(self, bash_commands):
        command = bash_commands["dbt_build"]
        assert command.count("export DBT_TARGET=") == 2, (
            "these have to be two `export`s joined by &&, not one assignment "
            "prefix: a prefix expands the whole list against the old "
            "environment, so the second never sees the first and key-pair auth "
            "silently falls through to password"
        )
        assert "SNOWFLAKE_PRIVATE_KEY_PATH" in command, (
            "the key path is what selects key_pair, same as warehouse/settings.py"
        )


class TestComposeWiring:
    """The container arrangement, including two things that actually broke."""

    def test_airflow_is_behind_a_profile(self, airflow_services):
        assert airflow_services, "no airflow-* services found"
        for name, service in airflow_services.items():
            assert service.get("profiles") == ["airflow"], (
                f"{name} has no profile, so plain `docker compose up` starts "
                "five more containers for a pipeline most commands drive by hand"
            )

    def test_every_service_signs_and_validates_with_the_same_jwt_secret(self, airflow_common):
        """The bug that made every task fail with "Invalid auth token".

        In Airflow 3 a task does not write to the database - it calls the
        api-server back over the execution API, carrying a JWT the scheduler
        minted. The signing secret defaults to a random value *per process*,
        so unset it leaves the scheduler and the api-server holding different
        keys and every task dies the moment it reports that it started.

        It being on the shared anchor is the whole point, so that is what this
        asserts rather than the value.
        """
        env = airflow_common["environment"]
        assert env.get("AIRFLOW__API_AUTH__JWT_SECRET"), (
            "unset, this defaults to a different random value in each container"
        )

    def test_airflow_does_not_share_the_database_being_replicated(self, compose):
        """Debezium is streaming the other PostgreSQL's WAL into Kafka.

        Pointing Airflow at it would push scheduler bookkeeping through the CDC
        pipeline and into Bronze, where it would be indistinguishable from the
        advertising data it is supposed to be orchestrating.
        """
        assert "airflow-postgres" in compose["services"], (
            "Airflow has no metadata database of its own"
        )
        conn = compose["x-airflow-common"]["environment"]["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
        assert "@airflow-postgres" in conn, (
            "Airflow's metadata is in the CDC source database, so its task "
            "history is being replicated into the lakehouse"
        )

    def test_the_project_is_mounted_at_its_host_path(self, airflow_common):
        """Load-bearing, and it looks like a mistake until it isn't.

        ``docker compose run`` inside the scheduler talks to the *host's*
        daemon, and the daemon resolves the Spark service's ``./:/app:ro`` bind
        against the host filesystem. Mounted anywhere else, Compose hands the
        daemon a path that does not exist and Spark starts with an empty /app.
        """
        mounts = airflow_common["volumes"]
        assert any(m.startswith("${PWD}:${PWD}") for m in mounts), (
            "the project must be mounted at the same path it has on the host"
        )
        assert airflow_common.get("working_dir") == "${PWD}"

    def test_the_project_is_mounted_read_only(self, airflow_common):
        for mount in airflow_common["volumes"]:
            if mount.startswith("${PWD}"):
                assert mount.endswith(":ro"), (
                    f"{mount} is writable: the scheduler can edit the repo it runs"
                )

    def test_dbt_writes_outside_the_read_only_mount(self, airflow_common):
        """dbt opens its log file before it does anything else.

        Both paths default to directories next to dbt_project.yml, which is
        inside the read-only mount, so out of the box `dbt build` dies on
        ``Read-only file system: .../logs/dbt.log`` before it ever reaches
        Snowflake.
        """
        env = airflow_common["environment"]
        for key in ("DBT_LOG_PATH", "DBT_TARGET_PATH"):
            path = env.get(key)
            assert path, f"{key} unset: dbt writes into the read-only project mount"
            assert not path.startswith("${PWD}"), f"{key} still points inside the mount"

        mounted = {m.split(":")[1] for m in airflow_common["volumes"] if ":" in m}
        for key in ("DBT_LOG_PATH", "DBT_TARGET_PATH"):
            assert any(env[key].startswith(target) for target in mounted), (
                f"{key} is not under a mounted volume, so the artifacts live in "
                "the container layer and vanish with it"
            )

    def test_the_ui_avoids_the_port_the_kafka_ui_already_has(self, compose):
        kafka_ui_ports = compose["services"]["kafka-ui"]["ports"]
        airflow_ports = compose["services"]["airflow-apiserver"]["ports"]
        published = {str(p).split(":")[0] for p in airflow_ports}
        taken = {str(p).split(":")[0] for p in kafka_ui_ports}
        assert not published & taken, (
            f"the Airflow UI and the Kafka UI both want {published & taken}"
        )

    def test_the_socket_is_mounted_because_the_tasks_start_containers(self, airflow_common):
        assert "/var/run/docker.sock:/var/run/docker.sock" in airflow_common["volumes"], (
            "without the socket the Spark tasks cannot start sibling containers"
        )

    def test_there_is_no_triggerer(self, airflow_services):
        assert "airflow-triggerer" not in airflow_services, (
            "the triggerer runs deferrable operators and nothing here defers - "
            "it would be a sixth idle container"
        )


class TestTheImage:
    """Two installs that must not be one install."""

    @pytest.fixture(scope="module")
    def dockerfile(self) -> str:
        return (PROJECT_ROOT / "docker" / "airflow.Dockerfile").read_text(encoding="utf-8")

    def test_dbt_lives_in_its_own_interpreter(self, dockerfile, dag_source):
        """Airflow and dbt pin incompatible versions of the same libraries.

        Resolving them together either fails outright or succeeds by
        downgrading something Airflow needs, which is worse because it fails
        later and somewhere unrelated.
        """
        assert "python -m venv /opt/adtech" in dockerfile
        assert "/opt/adtech/bin/pip install" in dockerfile
        assert "/opt/adtech/bin" in dag_source, (
            "tasks must call dbt by absolute path; Airflow's own environment never sees it"
        )

    def test_the_dbt_artifact_directory_exists_in_the_image(self, dockerfile):
        """Docker seeds a named volume from the mount point in the image.

        That includes ownership. With no directory there it creates one owned
        by root, the container runs as the invoking user, and dbt fails on a
        bare mkdir - before it has a logger, so it exits 2 having printed
        nothing at all, which is a genuinely confusing way to fail.
        """
        assert "mkdir -p /opt/airflow/dbt" in dockerfile
        assert "chown airflow:root /opt/airflow/dbt" in dockerfile

    def test_the_image_can_speak_to_the_host_daemon(self, dockerfile):
        assert "docker-ce-cli" in dockerfile
        assert "docker-compose-plugin" in dockerfile, (
            "the CLI alone has no `docker compose`, which is how every Spark task runs"
        )


class TestNoCredentialsAreWrittenDown:
    """Same rule the dbt profile is held to in test_warehouse.py."""

    def test_the_dag_contains_no_literal_credentials(self, dag_source):
        lowered = dag_source.lower()
        for marker in ("snowflake_password", "account=", "iqlriaf", "accountadmin"):
            assert marker not in lowered, f"{marker!r} appears in the DAG"

    def test_the_airflow_services_read_the_same_env_file_as_everything_else(self, airflow_common):
        assert ".env" in airflow_common.get("env_file", []), (
            "the tasks read .env like the rest of the project rather than "
            "keeping a second copy of the credentials in Airflow connections"
        )


def test_the_dag_file_is_where_the_dag_processor_looks(airflow_common):
    dags_folder = airflow_common["environment"]["AIRFLOW__CORE__DAGS_FOLDER"]
    mounts = [m for m in airflow_common["volumes"] if m.endswith(f"{dags_folder}:ro")]
    assert mounts, f"nothing is mounted at {dags_folder}"
    host_side = Path(mounts[0].split(":")[0].replace("${PWD}", str(PROJECT_ROOT)))
    assert (host_side / DAG_PATH.name).exists()
