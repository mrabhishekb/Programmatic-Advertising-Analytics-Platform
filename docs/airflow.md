# Orchestration (phase 14)

Every stage of this pipeline already worked. `make silver-reconcile`,
`make warehouse-export`, `make warehouse-load`, `make dbt-run` - each one ran
correctly, by hand, in that order, because someone remembered the order.

Phase 14 does not add a stage. It adds the thing that was missing between
them: an exit code that stops something.

```bash
make airflow-up        # build the image and start the stack
make airflow-dags      # what Airflow can see, and any parse errors
make airflow-trigger   # run it now
make airflow-logs      # follow the scheduler
make airflow-down      # stop it, keep the history
make airflow-reset     # stop it, destroy the metadata database
```

The UI is on <http://localhost:8090> - not 8080, which the Kafka UI already
has.

## What the DAG is for

`data_quality/silver.py` has returned a non-zero exit code on failure since
phase 8. It has done so into a terminal, where the number is discarded.

So the real sequence was two commands with nothing between them:

```bash
make silver-reconcile
make warehouse-export    # runs whether or not the first one was any good
```

A Silver layer that disagreed with PostgreSQL would be exported, loaded into
Snowflake and aggregated into four gold marts before anyone looked at it. The
first sign of trouble would be a number in a report that nobody could explain,
several days after the cause.

The DAG is where that stops being a matter of remembering:

```
silver_reconcile → validate_silver → warehouse_export → warehouse_load → dbt_build
                        ↑
                   nothing downstream of here runs if this fails
```

One measured run, on the `large` dataset:

| task | time | what it did |
| --- | --- | --- |
| `silver_reconcile` | 7.7s | nothing new to merge, so a no-op - which is the point of phase 7 |
| `validate_silver` | 75.3s | the gate: Silver's integrity, and its row counts against PostgreSQL |
| `warehouse_export` | 12.6s | dimensions out to Parquet |
| `warehouse_load` | 134.3s | `PUT` to the Snowflake stage, `COPY` into RAW |
| `dbt_build` | 180.8s | 10 incremental models, 7 snapshots, 15 tables, 11 views, 370 tests |
| | **410.7s** | `PASS=413 WARN=0 ERROR=0` |

## The gate is the only task that does not retry

Everything else gets two attempts with exponential backoff, because the
pipeline has genuinely transient failures and this project has hit them: MinIO
refusing a connection while it restarts, and the Snowflake upload in phase 12
dying to a dropped TLS session two and a half hours in.

The gate gets zero, deliberately. A quality check that failed will fail again
in five minutes, because the data is wrong - the data does not heal between
attempts. Retrying buys three more identical failures and an alert that
arrives twenty minutes late.

`warehouse_load` gets three rather than two, because phase 12 made it
resumable: files already in the stage are skipped, so a retry carries on
rather than starting over.

## The tasks shell out, and that is the design

Every task runs the command a person would. `silver_reconcile` is
`docker compose run --rm spark python scripts/silver.py reconcile`. Nothing is
reimplemented in Python inside the DAG.

The alternative is `DockerOperator`, which means restating the Spark service's
image, network, bind mounts and a dozen environment variables inside the DAG
file. That is a second copy of `docker-compose.yml`, and the two drift the
first time either is edited - silently, because nothing compares them.

Here the Compose file stays the single definition of what a Spark run *is*,
and the DAG only decides when one happens and what to do when it fails. That
is a deliberate limit on how much this file is allowed to know.

The cost is that the scheduler needs the Docker socket, which is discussed
below and is not free.

## What it deliberately does not do

**The 100 million row event tables.** `warehouse-export` excludes them;
`warehouse-export-all` is a multi-hour upload. Dimensions change daily and
events are a bulk backfill, so they are different jobs with different
cadences. Conflating them means either never running this daily or spending
three hours a night to move a few thousand changed rows.

**Draining Kafka into Bronze.** The `bronze-sink` service does that
continuously, as a streaming consumer, which is what it should be. A batch
drain here would mean Bronze only caught up when the scheduler said so, and
the lag would be invisible between runs.

**Catching up.** `catchup=False`. With it on, unpausing would immediately
queue one run per day since the start date - hundreds of parallel Snowflake
builds on a trial account. This warehouse is rebuilt from current state rather
than assembled per-day, so there is nothing a backfill would reconstruct.

**Running at all, until asked.** `is_paused_upon_creation=True`, stated in the
DAG rather than inherited from the instance default, because the blast radius
is a nightly Snowflake bill.

## Three containers, not one

Airflow 3 splits what 2.x ran in a single process:

| service | what it does |
| --- | --- |
| `airflow-scheduler` | decides what runs, and runs it (`LocalExecutor`) |
| `airflow-dag-processor` | parses DAG files, in its own process |
| `airflow-apiserver` | the UI, and the execution API tasks call back on |
| `airflow-postgres` | metadata, and see below |
| `airflow-init` | runs the schema migration once and exits |

They are listed separately rather than hidden behind `airflow standalone`,
which is a development shortcut that hides exactly the architecture this phase
is about.

There is no triggerer. It exists to run deferrable operators; nothing here
defers, so it would be a sixth idle container.

All five sit behind a Compose profile, so `make up` does not start them.

### Airflow's database is not the pipeline's database

`airflow-postgres` is a second PostgreSQL, and that is not duplication for its
own sake. The `postgres` service is the CDC source - Debezium is streaming its
write-ahead log into Kafka. Putting Airflow's task history there would push
scheduler bookkeeping through the pipeline and into Bronze, where it would sit
alongside the advertising data it is supposed to be orchestrating.

### Tasks authenticate to the api-server

In Airflow 3 a task does not write to the metadata database. It calls the
api-server back over an HTTP execution API, carrying a JWT the scheduler
minted for it.

The secret that signs that token defaults to a **random value per process**.
Leave it unset and the scheduler and the api-server each invent their own, so
every task fails the moment it tries to report that it started:

```
airflow.sdk.api.client.ServerResponseError: Invalid auth token
```

from the scheduler, and on the other side:

```
jwt.exceptions.InvalidSignatureError: Signature verification failed
```

`AIRFLOW__API_AUTH__JWT_SECRET` is therefore set on the shared
`x-airflow-common` anchor, which is what guarantees the two agree.

## Two Pythons in one image

`docker/airflow.Dockerfile` installs dbt and the Snowflake driver into a
separate virtualenv at `/opt/adtech`, and tasks call `/opt/adtech/bin/dbt` by
absolute path.

Not alongside Airflow, because they fight. Airflow pins `jinja2`, `packaging`
and a dozen transitive libraries; `dbt-core` pins its own versions of several
of the same ones. Resolving them together either fails outright or succeeds by
quietly downgrading something Airflow needs - which is worse, because it fails
later and somewhere unrelated.

A second interpreter makes the conflict impossible rather than merely
unlikely. Airflow's own environment never sees dbt.

## The socket, and the host path

The Spark tasks start sibling containers on the host's daemon, which needs
`/var/run/docker.sock` mounted into the scheduler.

**This is root on the host by another name.** A container with the socket can
start any other container, including a privileged one mounting `/`. It is an
acceptable trade on a developer laptop running a project that already builds
its own images. It would not be acceptable on shared infrastructure, where the
answer is a remote executor rather than a socket.

It also forces something that looks like a mistake:

```yaml
volumes:
  - ${PWD}:${PWD}:ro
working_dir: ${PWD}
```

The project is mounted at *the same absolute path it has on the host*. This is
load-bearing. When the scheduler runs `docker compose run spark`, the request
goes to the **host's** daemon, which resolves the Spark service's `./:/app:ro`
bind against the **host's** filesystem. Mounted at `/opt/airflow/project`
instead, Compose would hand the daemon a path that does not exist there, and
Spark would start with an empty `/app`.

### Read-only, which dbt has opinions about

The mount is `:ro`, so a scheduler cannot edit the repository it is running.
Worth keeping - but dbt writes `logs/` and `target/` next to
`dbt_project.yml`, and opens its log file before it does anything else:

```
OSError: [Errno 30] Read-only file system: '.../warehouse/dbt/logs/dbt.log'
```

`DBT_LOG_PATH` and `DBT_TARGET_PATH` redirect both to a named volume. A volume
rather than `/tmp` because `target/` is not scratch: `manifest.json` and
`run_results.json` are how a run is audited afterwards, and they are worth
having from the run *before* the one that broke.

The volume has to exist in the image first. Docker seeds a fresh named volume
from whatever is at the mount point, **including its ownership** - with
nothing there it creates a root-owned directory, the container runs as the
invoking user, and dbt fails on a bare `mkdir` before it has a logger to
report with. It exits 2 having printed nothing at all, which is a genuinely
unpleasant way to spend an afternoon.

## Credentials

The tasks read `.env` the way the rest of the project does, passed in by
Compose as `env_file`. Nothing is stored in an Airflow connection, so there is
no second copy to keep in step and nothing credential-shaped in the metadata
database. `tests/test_airflow.py` asserts the DAG contains no literal
credentials, the same rule `profiles.yml` is held to.

## Tests

Airflow is not a dependency of this project - it exists only inside the image -
so `tests/test_airflow.py` reads the DAG rather than importing it, the same way
`test_warehouse.py` reads dbt models it cannot execute locally.

That suits what is worth asserting. The properties that matter here are not
runtime behaviour; they are decisions that stay invisible until the night they
cost something. The gate ordered before the warehouse tasks rather than after.
Zero retries on the gate and non-zero everywhere else. `catchup` off.
`max_active_runs=1`. The JWT secret on the shared anchor. The project mounted
read-only, at its host path, with dbt writing somewhere else.

Every one of those is a real failure - most of them hit while building this
phase, the rest one config edit away.
