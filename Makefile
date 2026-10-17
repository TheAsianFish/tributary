.PHONY: check fmt lint typecheck test test-integration test-warehouse up up-lakehouse up-airflow down logs demo-data sample-stats dbt eval-guard

check: ## The quality gate (CI runs exactly this)
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy
	uv run pytest

fmt:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff check .

typecheck:
	uv run mypy

test: ## Unit tests (offline)
	uv run pytest

up: ## Kafka, PostgreSQL, MinIO and the stream job, without the sample replay
	docker compose up -d --build --wait postgres kafka minio lake-init stream

up-lakehouse: up ## ... plus Trino
	docker compose --profile lakehouse up -d --wait trino

up-airflow: up ## ... plus the Airflow scheduler, DAG processor and UI (localhost:8080)
	docker compose --profile airflow up -d --build --wait

down: ## Stop everything and delete the volumes (topic, tables, lake, checkpoints)
	docker compose --profile lakehouse --profile airflow down -v

logs:
	docker compose logs -f stream producer

test-integration: ## Needs `make up` first
	uv run pytest tests/integration -m integration
	@# A crash that Compose restarted would pass the tests from the checkpoint; refuse that.
	@restarts=$$(docker inspect -f '{{.RestartCount}}' $$(docker compose ps -q stream)); \
	test "$$restarts" = 0 || { echo "stream job restarted $$restarts time(s)"; exit 1; }

dbt: ## Build and test the warehouse (seeds, staging, marts, quality tests)
	uv run dbt build --project-dir warehouse --profiles-dir warehouse

test-warehouse: ## dbt and DAG tests against PostgreSQL alone (TRIBUTARY_PG_*)
	uv run pytest tests/warehouse -m warehouse

demo-data: ## Re-capture the committed sample from the live stream (~45 min)
	uv run tributary sample capture --count 50000

sample-stats:
	uv run tributary sample stats

PROVIDER ?= fake

eval-guard: ## Guard suite: every unsafe statement blocked, no warehouse gold query blocked (offline)
	uv run python -m eval.guard_eval
