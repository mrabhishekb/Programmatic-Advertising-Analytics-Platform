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
	# --profile traffic so the optional traffic container is torn down too,
	# rather than left running against a database that has gone away.
	docker compose --profile traffic down

.PHONY: reset
reset: ## Destroy the database volume and start again from a clean schema
	docker compose --profile traffic down -v
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
bronze-sink: ## Land the CDC change stream in Bronze as date-partitioned Parquet
	$(PYTHON) scripts/bronze.py sink

.PHONY: bronze-sink-forever
bronze-sink-forever: ## Same, but keep running instead of stopping when caught up
	$(PYTHON) scripts/bronze.py sink --idle-timeout 0

.PHONY: bronze-ls
bronze-ls: ## What is in the Bronze bucket, by table and day
	$(PYTHON) scripts/bronze.py --log-level WARNING ls

.PHONY: bronze-peek
bronze-peek: ## Read change events back out of the newest Bronze file
	$(PYTHON) scripts/bronze.py --log-level WARNING peek --payload

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
