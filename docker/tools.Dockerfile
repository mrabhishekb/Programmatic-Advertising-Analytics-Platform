# Runtime image for the project's long-running Python services: the Bronze sink
# and the optional change-traffic generator.
#
# One image for both rather than one each. They overlap on most dependencies,
# and a single image means a single build and a single thing to keep current;
# the few megabytes of unused wheels cost less than the drift would.
#
# No ENTRYPOINT: each service supplies its own full command, so what a container
# runs is visible in docker-compose.yml instead of buried in here.
FROM python:3.11-slim

# Pinned loosely to match pyproject.toml. The project itself is mounted at /app
# rather than installed, so editing code takes effect on restart without a
# rebuild - but that also means these have to be listed here by hand.
RUN pip install --no-cache-dir \
      "psycopg[binary]>=3.1" \
      "PyYAML>=6.0" \
      "python-dotenv>=1.0" \
      "confluent-kafka>=2.5" \
      "pyarrow>=17.0" \
      "boto3>=1.34"

WORKDIR /app

# Unbuffered so `docker compose logs -f` shows each batch as it lands, rather
# than in blocks whenever the pipe buffer happens to fill.
ENV PYTHONUNBUFFERED=1
# The project is mounted read-only, so there is nowhere to put .pyc files.
ENV PYTHONDONTWRITEBYTECODE=1
