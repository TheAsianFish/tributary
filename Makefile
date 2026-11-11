.PHONY: check fmt lint typecheck test test-integration test-warehouse up up-lakehouse up-airflow up-api down logs demo-data sample-stats dbt eval-guard eval-guard-db eval-warehouse record serve bird eval eval-link results bench bench-scale bench-query bench-results

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

up-api: ## ... plus the API on localhost:8000 (needs TRIBUTARY_API_KEYS)
	@test -n "$$TRIBUTARY_API_KEYS" || { echo "set TRIBUTARY_API_KEYS first"; exit 1; }
	$(MAKE) up-airflow
	docker compose --profile api up -d --build --wait api

down: ## Stop everything and delete the volumes (topic, tables, lake, checkpoints)
	docker compose --profile lakehouse --profile airflow --profile api --profile ollama down -v

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

eval-guard-db: ## The unsafe suite with the guard off: what the agent's role alone refuses (disposable DB!)
	uv run python -m eval.guard_db

eval-warehouse: ## The 40 warehouse questions (PROVIDER=fake replays recordings); needs PostgreSQL
	uv run python -m eval.warehouse_eval --provider $(PROVIDER)

record: ## Re-record the 40 questions' model calls from a live provider (PROVIDER=anthropic|ollama)
	@test "$(PROVIDER)" != fake || { echo "make record PROVIDER=anthropic (or ollama)"; exit 1; }
	uv run python -m eval.warehouse_eval --provider $(PROVIDER) --record

# BIRD Mini-Dev (M4). `make bird` needs PostgreSQL (`docker compose up -d postgres`);
# `make eval` with PROVIDER=fake replays recorded model calls and database executions and
# needs neither a model nor a database (CI runs it).
VARIANTS ?=
TRACKS ?= official,corrected
SUBSET ?= dev50

bird: ## Download BIRD Mini-Dev, load it into PostgreSQL, verify every gold query, index values (~3 min)
	uv run python -m eval.bird.load
	uv run python -m eval.bird.values

eval: ## BIRD execution accuracy (PROVIDER=fake replays; ollama|anthropic run live, RECORD=1 stores)
	uv run python -m eval.bird.run --provider $(PROVIDER) $(if $(VARIANTS),--variants $(VARIANTS)) \
		--tracks $(TRACKS) --subset $(SUBSET) $(if $(MODEL),--model $(MODEL)) $(if $(RECORD),--record) $(if $(RESUME),--resume)
	uv run python -m eval.bird.report $(if $(filter fake,$(PROVIDER)),--check)

eval-link: ## Schema-linking recall and precision on BIRD, full vs bm25 (offline)
	uv run python -m eval.bird.link_eval
	uv run python -m eval.bird.link_eval --track corrected

results: ## Re-render RESULTS.md's BIRD tables from eval/results/
	uv run python -m eval.bird.report

serve: ## The API on localhost:8000 (needs TRIBUTARY_API_KEYS)
	uv run tributary serve

# Ingest benchmarks (M5). Both drive the Compose stack from the host, kill and recreate
# its containers and write bench/results/; run them on a stack you can throw away
# (`make down` afterwards). A rerun skips every unit whose result file exists.
BENCH_REPS ?= 3

bench: ## Exactly-once under SIGKILL, latency, trigger/backpressure tuning, freshness (~1 h)
	$(MAKE) up-lakehouse up-airflow
	uv run python -m bench.ingest --reps $(BENCH_REPS)
	uv run python -m bench.report

bench-scale: ## Sustained events/s, p95 and the bottleneck at 1, 3 and 6 partitions (~2 h)
	$(MAKE) up
	uv run python -m bench.scale --reps $(BENCH_REPS)
	uv run python -m bench.report

# Query engines (M6): PostgreSQL vs Trino over PostgreSQL vs Trino over Iceberg. Generates
# bench.events_<N> in both stores, recreates PostgreSQL, MinIO and Trino under
# bench/compose.query.yaml's memory limits, stops the stream job, and restarts the engines
# for the cold runs (dropping the host page cache through `sudo -n` where allowed).
SCALE ?= 1m,20m
RUNS ?= 10

bench-query: ## Latency per query, engine and scale (warm and cold), pruning by layout (~3 h)
	$(MAKE) up-lakehouse
	docker compose build stream
	uv run python -m bench.query --scales $(SCALE) --runs $(RUNS)
	uv run python -m bench.report

bench-results: ## Re-render RESULTS.md's ingest and query tables from bench/results/
	uv run python -m bench.report
