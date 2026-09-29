# Runs data_generator.change_generator in --loop mode as the optional
# `traffic` service, so the CDC pipeline always has something to capture.
#
# Only the three runtime dependencies are installed. The project itself is
# mounted at /app rather than copied in, so changing the generator takes effect
# on restart instead of requiring a rebuild.
FROM python:3.11-slim

RUN pip install --no-cache-dir \
      "psycopg[binary]>=3.1" \
      "PyYAML>=6.0" \
      "python-dotenv>=1.0"

WORKDIR /app

# Unbuffered so `make traffic-logs` shows each batch as it lands, rather than
# in blocks whenever the pipe buffer happens to fill.
ENV PYTHONUNBUFFERED=1
# The project is mounted read-only, so there is nowhere to put .pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["python", "-m", "data_generator.change_generator"]
CMD ["--loop"]
