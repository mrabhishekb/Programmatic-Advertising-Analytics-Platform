# Airflow image for the orchestration layer (phase 14).
#
# Two things are added to the official image, and both are here for reasons
# worth stating.
#
# 1. The Docker CLI and the Compose plugin.
#
#    Spark in this project is not a cluster with a master to submit to - it is
#    a throwaway container per job, `docker compose run --rm spark`. So the
#    scheduler has to be able to start sibling containers on the host's daemon,
#    which means the CLI here and /var/run/docker.sock mounted in
#    docker-compose.yml.
#
#    Shelling out to Compose rather than using DockerOperator is deliberate.
#    DockerOperator would need the Spark service's image, network, bind mounts
#    and ten environment variables restated inside the DAG, which is a second
#    copy of docker-compose.yml that nothing keeps in step with the first. The
#    Compose file stays the single definition of what a Spark run is, and the
#    DAG names the service.
#
#    The cost is real and should not be glossed over: a container with the
#    Docker socket can start any container it likes, including a privileged one
#    mounting the host filesystem. That is root on the host by another name. It
#    is an acceptable trade on a developer laptop running a project that
#    already builds its own images, and it would not be acceptable on shared
#    infrastructure, where the answer is a remote executor rather than a socket.
#
# 2. A second virtualenv at /opt/adtech, holding dbt and the Snowflake driver.
#
#    Not installed alongside Airflow, because they fight. Airflow pins jinja2,
#    packaging and a dozen transitive libraries; dbt-core pins its own versions
#    of several of the same ones. Resolving them together either fails outright
#    or succeeds by downgrading something Airflow needs, which is worse,
#    because it fails later and somewhere unrelated.
#
#    Keeping dbt in its own interpreter makes the conflict impossible rather
#    than merely unlikely. Tasks call /opt/adtech/bin/dbt by absolute path, and
#    Airflow's own environment never sees it.
FROM apache/airflow:3.3.2-python3.11

USER root

# Docker CLI and Compose plugin from Docker's own apt repository. The distro
# packages lag far enough behind that `docker compose` is often missing
# entirely, and this image has to speak the same Compose file as the host.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*

# The socket is owned by root:docker on Linux and by root with mode 660 on the
# Docker Desktop VM. Adding the airflow user to a docker group covers the first
# case; the second works because Desktop's socket is world-accessible inside
# the VM. Both are developer-machine assumptions, like the socket mount itself.
RUN groupadd -f -g 999 docker && usermod -aG docker airflow

# The second interpreter. Created as root and handed over, because the airflow
# user cannot write to /opt.
RUN python -m venv /opt/adtech && chown -R airflow:root /opt/adtech

# Somewhere for dbt to write. The project is mounted read-only, so dbt's logs/
# and target/ are redirected here by DBT_LOG_PATH and DBT_TARGET_PATH in
# docker-compose.yml, where a named volume is mounted over this path.
#
# The mkdir is what makes that volume usable: Docker seeds a fresh named volume
# from whatever is at the mount point in the image, *including its ownership*.
# With no directory here it creates one owned by root, and the container runs
# as the invoking user - so dbt would fail on a bare mkdir, before it has a
# logger to report with, and exit 2 having printed nothing at all.
RUN mkdir -p /opt/airflow/dbt && chown airflow:root /opt/airflow/dbt \
    && chmod 775 /opt/airflow/dbt

USER airflow

# Pinned loosely to match pyproject.toml's `warehouse` extra, the same hand-kept
# arrangement docker/tools.Dockerfile uses and for the same reason: the project
# is mounted rather than installed, so editing a model or a loader takes effect
# without a rebuild. The duplication is the price of that, and it is small
# enough to notice when it drifts.
RUN /opt/adtech/bin/pip install --no-cache-dir \
      "snowflake-connector-python>=3.12" \
      "cryptography>=42.0" \
      "dbt-core>=1.8" \
      "dbt-snowflake>=1.8" \
      "boto3>=1.34" \
      "pyarrow>=17.0" \
      "PyYAML>=6.0" \
      "python-dotenv>=1.0"

# Unbuffered so a task's output reaches the Airflow log as it happens rather
# than when the pipe buffer fills - which for a dbt build is at the end, long
# after anyone watching has given up.
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
