.PHONY: check fmt lint typecheck test test-integration up down logs demo-data sample-stats

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

up: ## Kafka, PostgreSQL and the stream job, without the sample replay
	docker compose up -d --build --wait postgres kafka stream

down: ## Stop everything and delete the volumes (topic, table, checkpoints)
	docker compose down -v

logs:
	docker compose logs -f stream producer

test-integration: ## Needs `make up` first
	uv run pytest tests/integration -m integration

demo-data: ## Re-capture the committed sample from the live stream (~45 min)
	uv run tributary sample capture --count 50000

sample-stats:
	uv run tributary sample stats
