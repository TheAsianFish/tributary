.PHONY: check fmt lint typecheck test test-integration up down logs demo-data sample-stats image

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

test-integration: ## Needs `docker compose up -d --wait` first
	uv run pytest tests/integration -m integration
