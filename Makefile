.DEFAULT_GOAL := help
SHELL := /bin/bash

PYTHON ?= .venv/bin/python
SCALE  ?= small
SEED   ?= 42

# Load .env so psql and docker compose see the same settings the Python code does.
ifneq (,$(wildcard .env))
include .env
export
endif

POSTGRES_USER     ?= adtech
POSTGRES_DB       ?= adtech
POSTGRES_PORT     ?= 5432
CONNECT_HOST_PORT ?= 8083
KAFKA_UI_PORT     ?= 8080

# Mirrors the defaults in docker-compose.yml's traffic service, so `make
# traffic-up` reports the interval it actually started with.
TRAFFIC_INTERVAL_SECONDS  ?= 60
TRAFFIC_CHANGES_PER_BATCH ?= 20

.PHONY: help
help: ## Show the available targets
	@# firstword: including .env adds it to MAKEFILE_LIST, and grep would then
	@# prefix every match with a filename.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(firstword $(MAKEFILE_LIST)) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup: ## Create the virtualenv and install the project
	uv venv --python 3.11 .venv
	uv pip install --python $(PYTHON) -e ".[dev]"
	@test -f .env || cp .env.example .env

.PHONY: up
up: ## Start PostgreSQL (schema and reference data are applied on first start)
	docker compose up -d
	@echo "waiting for postgres..."
	@until docker compose exec -T postgres pg_isready -U $(POSTGRES_USER) -d $(POSTGRES_DB) >/dev/null 2>&1; do sleep 1; done
	@echo "postgres is ready on port $(POSTGRES_PORT)"

.PHONY: down
down: ## Stop every service, keeping the data volumes
	# Every profile named, so the optional containers are torn down too rather
	# than left running against a database that has gone away.
	docker compose --profile traffic --profile spark down

.PHONY: reset
reset: ## Destroy the database volume and start again from a clean schema
	docker compose --profile traffic --profile spark down -v
	$(MAKE) up

.PHONY: generate
generate: ## Generate and load a dataset (make generate SCALE=small SEED=42)
	$(PYTHON) -m data_generator.generate --scale $(SCALE) --seed $(SEED)

.PHONY: generate-csv
generate-csv: ## Generate to CSV files instead of PostgreSQL
	$(PYTHON) -m data_generator.generate --scale $(SCALE) --seed $(SEED) --target csv

.PHONY: validate
validate: ## Run the SQL data quality and join validation suite
	$(PYTHON) -m data_quality.validation

.PHONY: changes
changes: ## Apply UPDATE/INSERT/DELETE traffic to the source tables
	$(PYTHON) -m data_generator.change_generator --batches 1 --changes-per-batch 50

# --- change data capture (phase 2) -------------------------------------------

.PHONY: cdc-up
cdc-up: ## Start Kafka, Kafka Connect (Debezium) and the Kafka UI
	docker compose up -d kafka kafka-connect kafka-ui
	@echo "waiting for Kafka Connect..."
	@until curl -sf http://localhost:$(CONNECT_HOST_PORT)/connectors >/dev/null 2>&1; do sleep 2; done
	@echo "Kafka Connect ready on :$(CONNECT_HOST_PORT)  |  Kafka UI on http://localhost:$(KAFKA_UI_PORT)"

.PHONY: cdc-register
cdc-register: ## Register the Debezium connector (creates the replication slot)
	$(PYTHON) scripts/cdc.py register

.PHONY: cdc-status
cdc-status: ## Connector and task health
	$(PYTHON) scripts/cdc.py status

.PHONY: cdc-slots
cdc-slots: ## Replication slot state and how much WAL is being retained
	$(PYTHON) scripts/cdc.py slots

.PHONY: cdc-topics
cdc-topics: ## List CDC topics and their message counts
	$(PYTHON) scripts/cdc.py topics

.PHONY: cdc-watch
cdc-watch: ## Stream change events as they happen (Ctrl-C to stop)
	$(PYTHON) scripts/cdc.py watch --from-beginning

.PHONY: cdc-delete
cdc-delete: ## Remove the connector and drop its replication slot
	$(PYTHON) scripts/cdc.py delete --drop-slot

# --- kafka topic configuration (phase 3) -------------------------------------

.PHONY: kafka-describe
kafka-describe: ## Compare the live topics against config/kafka.yml
	$(PYTHON) scripts/kafka_admin.py describe

.PHONY: kafka-plan
kafka-plan: ## Show what kafka-apply would change, without changing it
	$(PYTHON) scripts/kafka_admin.py apply --dry-run

.PHONY: kafka-apply
kafka-apply: ## Reconcile topic partitions, retention and compaction with config/kafka.yml
	$(PYTHON) scripts/kafka_admin.py apply

.PHONY: kafka-lag
kafka-lag: ## Consumer group lag, per partition
	$(PYTHON) scripts/kafka_admin.py lag

.PHONY: kafka-bench
kafka-bench: ## Measure producer throughput across every compression codec
	$(PYTHON) scripts/kafka_admin.py bench --compare

# --- continuous change traffic (optional) ------------------------------------

.PHONY: traffic-up
traffic-up: ## Start the optional container applying balanced changes on a loop
	docker compose --profile traffic up -d --build traffic
	@echo ""
	@echo "Traffic running: one balanced batch every $(TRAFFIC_INTERVAL_SECONDS) seconds."
	@echo "It restarts with the stack, so it survives a reboot. Follow it with:"
	@echo "  make traffic-logs"

.PHONY: traffic-down
traffic-down: ## Stop the change traffic container
	docker compose --profile traffic rm -sf traffic

.PHONY: traffic-logs
traffic-logs: ## Follow the change traffic log (Ctrl-C stops watching, not the traffic)
	docker compose --profile traffic logs -f traffic

.PHONY: export-snapshot
export-snapshot: ## Bulk export the existing rows to Bronze (run AFTER cdc-register)
	$(PYTHON) scripts/export_snapshot.py

.PHONY: export-snapshot-master
export-snapshot-master: ## Same, but skip the 100M-row event tables
	$(PYTHON) scripts/export_snapshot.py --master-only

# --- S3 Bronze (phase 4) -----------------------------------------------------

.PHONY: bronze-sink
bronze-sink: ## Drain the CDC topics into Bronze once, from the host
	# The bronze-sink container is normally already doing this, so this usually
	# reports nothing to consume. It exists for running the sink by hand.
	$(PYTHON) scripts/bronze.py sink

.PHONY: bronze-logs
bronze-logs: ## Follow the Bronze sink container (Ctrl-C stops watching, not the sink)
	docker compose logs -f bronze-sink

.PHONY: bronze-restart
bronze-restart: ## Rebuild and restart the Bronze sink container
	docker compose up -d --build bronze-sink

.PHONY: bronze-sink-forever
bronze-sink-forever: ## Same, but keep running instead of stopping when caught up
	# A longer flush interval than the one-shot default: running all day, the
	# time trigger decides how many Parquet files exist, and 15 minutes of
	# latency costs nothing when Bronze is read by batch Spark.
	$(PYTHON) scripts/bronze.py sink --idle-timeout 0 --max-seconds 900

.PHONY: bronze-ls
bronze-ls: ## What is in the Bronze bucket, by table and day
	$(PYTHON) scripts/bronze.py --log-level WARNING ls

.PHONY: bronze-peek
bronze-peek: ## Read change events back out of the newest Bronze file
	$(PYTHON) scripts/bronze.py --log-level WARNING peek --payload

# --- Spark / Silver (phases 5-6) ---------------------------------------------

# `run --rm` rather than `up`: the job exits when it is done, and this way its
# exit code reaches Make instead of being swallowed by the container runtime.
SPARK_RUN = docker compose --profile spark run --rm --build spark

.PHONY: silver
silver: ## Reconcile Bronze into current-state Silver tables
	$(SPARK_RUN) python scripts/silver.py reconcile

.PHONY: silver-status
silver-status: ## What the next Silver run would do, and why
	$(SPARK_RUN) python scripts/silver.py --log-level WARNING status

.PHONY: silver-full
silver-full: ## Rebuild every Silver table from the snapshot export
	$(SPARK_RUN) python scripts/silver.py reconcile --full

.PHONY: silver-plan
silver-plan: ## Same, but count everything and write nothing
	$(SPARK_RUN) python scripts/silver.py reconcile --dry-run

.PHONY: silver-all
silver-all: ## Reconcile, and copy the append-only event tables through as well
	$(SPARK_RUN) python scripts/silver.py reconcile --include-events

.PHONY: silver-ls
silver-ls: ## What is in the Silver layer
	$(PYTHON) scripts/silver.py --log-level WARNING ls

.PHONY: silver-show
silver-show: ## Read rows back (make silver-show TABLE=campaigns [AS_OF=<snapshot_id>])
	$(SPARK_RUN) python scripts/silver.py show $(or $(TABLE),campaigns) \
	  $(if $(AS_OF),--as-of $(AS_OF),)

.PHONY: silver-history
silver-history: ## Every version of a table and the Bronze run behind it
	$(SPARK_RUN) python scripts/silver.py history $(or $(TABLE),campaigns)

.PHONY: silver-drop-legacy
silver-drop-legacy: ## Show phase 5's flat Parquet, which Iceberg superseded
	$(PYTHON) scripts/silver.py --log-level WARNING drop-legacy

.PHONY: silver-drop-legacy-commit
silver-drop-legacy-commit: ## Actually delete it
	$(PYTHON) scripts/silver.py drop-legacy --commit

.PHONY: silver-compact
silver-compact: ## Show which small Bronze CDC objects would be merged
	$(PYTHON) scripts/silver.py --log-level WARNING compact

.PHONY: silver-compact-commit
silver-compact-commit: ## Actually merge them, deleting the inputs afterwards
	$(PYTHON) scripts/silver.py compact --commit

.PHONY: spark-test
spark-test: ## Run the Spark tests inside the Spark container (needs Java)
	# no:cacheprovider because the project is mounted read-only, and pytest
	# otherwise warns once per run about not being able to write .pytest_cache.
	$(SPARK_RUN) python -m pytest -q -m spark -p no:cacheprovider \
		tests/test_spark.py tests/test_merge.py

.PHONY: spark-shell
spark-shell: ## Open a shell in the Spark container
	$(SPARK_RUN) bash

.PHONY: test
test: ## Run the test suite (PostgreSQL integration tests included when a database is up)
	$(PYTHON) -m pytest -q

.PHONY: test-unit
test-unit: ## Run only the tests that need no database
	$(PYTHON) -m pytest -q -m "not postgres"

.PHONY: lint
lint: ## Lint and format-check the Python code
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

.PHONY: seed-sql
seed-sql: ## Regenerate postgres/seed.sql from the reference YAML
	$(PYTHON) scripts/render_seed_sql.py

.PHONY: psql
psql: ## Open a psql shell against the running database
	docker compose exec -it postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

.PHONY: queries
queries: ## Run the example analytical queries against the loaded data
	docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) \
		-f /dev/stdin < scripts/example_queries.sql
