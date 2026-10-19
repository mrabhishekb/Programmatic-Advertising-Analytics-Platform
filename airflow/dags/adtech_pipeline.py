"""The pipeline, end to end, with the quality suite as a gate.

Every stage below already existed as a Makefile target and already ran
correctly by hand. What phase 14 adds is not new work - it is ordering,
failure handling, and one thing the project has been building toward since
phase 8: an exit code that stops something.

``data_quality/silver.py`` has returned non-zero on failure since it was
written, with a comment saying a scheduler would eventually read it. Nothing
did. Running `make silver-reconcile` and then `make warehouse-export` by hand
is two commands with nothing between them, so a Silver layer that disagreed
with PostgreSQL would be exported, loaded and aggregated into the gold marts
before anyone looked at a report. The DAG is where that stops being a matter
of remembering.

WHY THE TASKS SHELL OUT

Each task runs the same command a person would. `silver-reconcile` is
``docker compose run --rm spark``, which starts a sibling container on the
host's daemon; `dbt build` runs in this image's second virtualenv. Neither is
re-implemented here.

That is a deliberate limit on how much this file is allowed to know. A DAG that
reconstructs what a Spark job needs - image, network, mounts, ten environment
variables - is a second copy of docker-compose.yml, and the two drift the first
time someone edits one. Here the Compose file stays the definition of what a
Spark run is, and the DAG only decides when one happens and what to do if it
fails.

WHAT THIS DAG DOES NOT DO

The 100 million row event tables. `warehouse-export` deliberately excludes
them; `warehouse-export-all` is the one that includes them, and it is a
multi-hour upload that has nothing to do with a daily schedule. Dimensions
change every day and events are a bulk backfill, so they are different jobs
with different cadences, and conflating them would mean either never running
this daily or running a three-hour upload every night to move a few thousand
changed rows.

It also does not drain Kafka into Bronze. The `bronze-sink` service does that
continuously, as a streaming consumer - which is what it should be. Putting a
batch drain in here would mean Bronze only caught up when the scheduler said
so, and the lag would be invisible between runs.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

# Two imports from two places, which is an Airflow 3 detail worth knowing.
# `DAG` is part of the task SDK - the stable authoring surface DAG files are
# meant to use. The operators are not: in Airflow 3 even BashOperator lives in
# the separately versioned `standard` provider, so that operators can be
# released without releasing the scheduler.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

#: Where the project is mounted, which is also where it lives on the host. See
#: the volumes in docker-compose.yml for why those have to be the same path.
PROJECT_DIR = os.environ.get("PROJECT_DIR", os.getcwd())

#: The interpreter holding dbt and the Snowflake driver, kept apart from
#: Airflow's own so their pins cannot collide. See docker/airflow.Dockerfile.
ADTECH_VENV = "/opt/adtech/bin"

#: One Spark run, as Compose already defines it. `--rm` so a failed task does
#: not leave a stopped container behind for the next run to trip over.
#:
#: No `--build`, which the Makefile does use. A rebuild on every task start
#: would add minutes to each run to rebuild an image that changes when someone
#: edits a Dockerfile, not when a schedule fires. `make airflow-up` builds.
SPARK_RUN = "docker compose --profile spark run --rm --no-deps -T spark"

default_args = {
    # Retries are for the failures that are worth retrying, and the pipeline
    # has real ones: MinIO refusing a connection while it restarts, Snowflake
    # dropping a TLS session mid-upload - which is exactly what killed the
    # phase 12 load after two and a half hours.
    #
    # They are not for the failures that are not. A quality gate that fails
    # will fail again in five minutes, because the data is wrong; the retry
    # just delays the alert. That task overrides this to zero below.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="adtech_pipeline",
    description="Bronze to Silver, quality gate, then Snowflake and the dbt marts",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    # Without this, unpausing the DAG would immediately queue one run per day
    # since the start date - hundreds of Snowflake builds, in parallel, on a
    # trial account. catchup is the right default for a warehouse that is
    # rebuilt from current state rather than assembled per-day, and it is
    # emphatically the right default for a laptop.
    catchup=False,
    # Stated rather than inherited. Airflow's own default pauses new DAGs, but
    # it is a config value someone can flip for the whole instance, and the
    # blast radius here is a nightly Snowflake build on a trial account that
    # nobody asked for. Unpausing should be a decision, not a side effect.
    is_paused_upon_creation=True,
    # The pipeline is not safe to run twice at once. Two Spark reconciles would
    # write the same Iceberg tables, and two dbt builds would merge into the
    # same incremental models. One run at a time, always.
    max_active_runs=1,
    tags=["adtech", "phase-14"],
) as dag:
    # ---------------------------------------------------------------- lakehouse

    silver_reconcile = BashOperator(
        task_id="silver_reconcile",
        bash_command=f"cd {PROJECT_DIR} && {SPARK_RUN} python scripts/silver.py reconcile",
        doc_md=(
            "Merge new Bronze change events into the current-state Iceberg "
            "tables. Incremental and idempotent since phase 7: it reads its own "
            "high-water mark, so a re-run after a failure resumes rather than "
            "reprocessing, and a run with nothing new to do is a no-op."
        ),
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=f"cd {PROJECT_DIR} && {SPARK_RUN} python -m data_quality.silver",
        # No retries, on purpose. Everything downstream of this is gated on it,
        # so a retry here is three extra attempts at a question whose answer
        # will not change, followed by the same failure twenty minutes later.
        # Bad data is not a transient fault.
        retries=0,
        doc_md=(
            "**The gate.** Checks Silver's internal integrity and that its row "
            "counts still agree with PostgreSQL. Exits non-zero when any ERROR "
            "check fails, which stops this DAG here.\n\n"
            "This is the task the whole phase is for. Without it, Silver "
            "disagreeing with the source would be exported, loaded and "
            "aggregated into the gold marts, and the first sign of trouble "
            "would be a number in a report that nobody could explain."
        ),
    )

    # ---------------------------------------------------------------- warehouse

    warehouse_export = BashOperator(
        task_id="warehouse_export",
        bash_command=f"cd {PROJECT_DIR} && {SPARK_RUN} python scripts/silver.py export",
        doc_md=(
            "Write Silver out as Parquet for Snowflake to read, into the object "
            "store rather than local disk - which is why the next task can run "
            "in a different container without sharing a volume with this one.\n\n"
            "Dimensions only. The event tables are `--include-events`, a "
            "multi-hour job that does not belong on a daily schedule."
        ),
    )

    warehouse_load = BashOperator(
        task_id="warehouse_load",
        bash_command=f"cd {PROJECT_DIR} && {ADTECH_VENV}/python -m warehouse.load",
        # The upload is the one step here that has failed in practice, and it
        # failed after two and a half hours to a non-retryable TLS error. It
        # was made resumable in phase 12: files already in the stage are
        # skipped, so a retry carries on from where it stopped.
        retries=3,
        execution_timeout=timedelta(hours=4),
        doc_md=(
            "Download the export, `PUT` it to the Snowflake stage and `COPY` it "
            "into RAW, checking the row counts against the export's manifest.\n\n"
            "Runs in this image rather than a Spark container - it needs the "
            "Snowflake driver and boto3, not a JVM. Resumable since phase 12, "
            "which is why it is allowed more retries than anything else here."
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # Two `export`s rather than one assignment prefix, which matters: in a
        # prefix the whole list is expanded against the *old* environment, so
        # the second DBT_TARGET would never see the first and key-pair auth
        # would silently fall through to password. Same precedence as the
        # Makefile and as warehouse/settings.py: a key path wins if set.
        #
        # No `set -a && . .env` the way the Makefile needs - Compose already
        # passed .env in through env_file, so env_var() in profiles.yml
        # resolves. DBT_LOG_PATH and DBT_TARGET_PATH come from there too,
        # because the project is mounted read-only and dbt wants to write.
        bash_command=(
            f"cd {PROJECT_DIR}/warehouse/dbt && "
            f"export DBT_TARGET=${{SNOWFLAKE_PRIVATE_KEY_PATH:+key_pair}} && "
            f"export DBT_TARGET=${{DBT_TARGET:-password}} && "
            f"DBT_PROFILES_DIR=. {ADTECH_VENV}/dbt build"
        ),
        execution_timeout=timedelta(hours=1),
        doc_md=(
            "`build`, not `run` then `test`. One command that interleaves them "
            "in dependency order and stops at the first failure, so a model "
            "whose test fails never becomes the input to the model above it.\n\n"
            "This is also where the snapshots run. They accumulate rather than "
            "rebuild, so a missed one is history nothing recovers - which is "
            "the strongest argument for this DAG existing at all."
        ),
    )

    silver_reconcile >> validate_silver >> warehouse_export >> warehouse_load >> dbt_build
