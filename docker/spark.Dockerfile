# Spark runtime for the Silver jobs (phase 5).
#
# Separate from docker/tools.Dockerfile rather than folded into it: this image
# carries a JRE and ~600MB of Spark and AWS jars that the sink and the traffic
# generator have no use for, and rebuilding those two every time a Spark jar
# moves would be a poor trade.
#
# No ENTRYPOINT, matching the other image: each service supplies its own full
# command, so what a container runs is visible in docker-compose.yml.
# Pinned to bookworm rather than tracking plain `python:3.11-slim`. That tag has
# moved on to Debian trixie, which dropped the OpenJDK 17 packages entirely and
# offers only 21 - and Spark 3.5 supports Java 8, 11 and 17, not 21. Following
# the floating tag turns an unrelated rebuild into either a build failure or,
# worse, a silent move to a JVM this Spark was never tested against.
FROM python:3.11-slim-bookworm

# Spark is a JVM program that PySpark drives over a socket, so the JRE is not
# optional. Headless because nothing here draws anything.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

# The JRE installs to an architecture-suffixed path and this image is built on
# both arm64 (Apple silicon) and amd64, so resolve it and symlink to a stable
# location rather than hardcoding one architecture's spelling.
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Pinned exactly. S3A is the one part of this stack where the Hadoop version and
# the AWS SDK version have to agree precisely, so a floating install would turn
# a working image into NoSuchMethodError on an unrelated rebuild.
ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

RUN pip install --no-cache-dir \
      "pyspark==${SPARK_VERSION}" \
      "psycopg[binary]>=3.1" \
      "PyYAML>=6.0" \
      "python-dotenv>=1.0" \
      "pyarrow>=17.0" \
      "boto3>=1.34" \
      "pytest>=8.0"

# Baked into the image rather than resolved at runtime with --packages: a job
# that downloads its own dependencies fails whenever Maven Central is slow or
# unreachable, which is exactly when a batch job is least able to explain why.
ENV SPARK_JARS=/usr/local/lib/python3.11/site-packages/pyspark/jars
RUN curl -fsSL -o "${SPARK_JARS}/hadoop-aws-${HADOOP_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
    && curl -fsSL -o "${SPARK_JARS}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
# The project is mounted read-only, so Spark needs somewhere writable for the
# shuffle and spill files it creates for anything larger than memory.
ENV SPARK_LOCAL_DIRS=/tmp/spark
