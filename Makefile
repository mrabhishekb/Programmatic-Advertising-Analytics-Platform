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

POSTGRES_USER ?= adtech
POSTGRES_DB   ?= adtech
POSTGRES_PORT ?= 5432

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
down: ## Stop PostgreSQL, keeping the data volume
	docker compose down

.PHONY: reset
reset: ## Destroy the database volume and start again from a clean schema
	docker compose down -v
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
