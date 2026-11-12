# Contributing

This file is the contributor guide and the first thing to read before working
on the repository. PLAN.md is the source of truth for scope and milestones;
RESULTS.md records every measured number with the command that produced it.

## Setup

Requirements: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker with
Compose v2, Java 17 (only for running the Spark job or its tests locally).

```bash
uv sync --all-extras          # .venv with the dev group and the spark, warehouse, lakehouse, airflow extras
cp .env.example .env          # optional; the defaults match compose.yaml
make up-lakehouse             # PostgreSQL, Kafka, MinIO, the Spark stream job, Trino
uv run tributary produce --limit 5000
make test-integration
```

`docker compose up -d` additionally starts the producer, which replays the
whole sample once. `make down` deletes the volumes (topic, tables, lake
files and checkpoints together, so they never disagree). The warehouse can
be worked on without the streaming stack: any PostgreSQL 17 plus
`uv run tributary sample load` (the sample straight into `raw.recentchange`)
is enough for `make dbt` and `make test-warehouse`. BIRD needs PostgreSQL
alone too: `docker compose up -d postgres && make bird`.

## Commands

| Command | What it does |
|---|---|
| `make check` | ruff format check, ruff lint, mypy strict, pytest (offline). The quality gate; CI runs exactly this. |
| `make test` | Unit tests only (fast, offline, deterministic; the Spark tests run in local mode and need Java 17). |
| `make test-integration` | Integration tests against the Compose services: 5,000 events into PostgreSQL and Iceberg, pruning. Run `make up-lakehouse` first. |
| `make up` / `make down` | Start Kafka, PostgreSQL, MinIO and the stream job (no replay) / stop everything and delete the volumes. |
| `make up-lakehouse` | `make up` plus Trino (profile `lakehouse`, about 2 GB). |
| `make up-airflow` | `make up` plus the Airflow scheduler, DAG processor and UI on http://localhost:8080 (profile `airflow`). |
| `make dbt` | `dbt build` on the warehouse: seeds, staging, marts and every test. Needs `raw.recentchange` populated. |
| `make test-warehouse` | dbt quality-test fixtures, `dbt build` on the sample and the warehouse DAGs under `dag.test()`, against PostgreSQL alone. |
| `make logs` | Follow the stream job and producer logs. |
| `make demo-data` | Regenerate the committed sample from the live stream (maintainers only; keeps factual fields, drops comment text; about 45 minutes). |
| `make sample-stats` | Counts, duplicates, emit delay and lateness of the committed sample. |
| `make eval-guard` | The guard suite (offline, in CI): every statement in `eval/guard/unsafe.yaml` blocked, none of the 40 gold queries blocked. |
| `make eval-guard-db` | The same suite with the guard off, as the agent's role: what PostgreSQL alone refuses. Disposable databases only. |
| `make eval-guard-diff` | The guard's differential oracle: every one-argument function in `pg_proc` crossed with the spelling templates in `eval/guard/templates.yaml` (and the 40 gold queries rewritten), every admitted statement resolved by PostgreSQL as a rolled-back temporary view and its calls read from `pg_rewrite.ev_action`; then each historical fix switched off in turn. Never executes a generated statement. Needs PostgreSQL with the marts built; about 25 minutes; the `Guard differential` workflow runs it by hand. After a PostgreSQL upgrade, `uv run python -m eval.guard.differential functions` regenerates the guard's list of callable names (`make test-warehouse` fails until it is). |
| `make eval-warehouse` | The 40 warehouse questions end to end; `PROVIDER=fake` (default) replays the recordings, `ollama`/`anthropic` call a model. Needs PostgreSQL. |
| `make record PROVIDER=...` | Re-run the 40 questions on a live model and store every call in `TRIBUTARY_RECORDINGS_PATH`. |
| `make bird` | Download BIRD Mini-Dev (cached, sha256-pinned), load the eleven databases into PostgreSQL (database `bird`, one schema each, read-only role `tributary_bird`), write the committed catalogs and verify every gold query, transpiling the corrected track. Needs PostgreSQL; about 2 minutes after the 800 MB download. |
| `make eval` | BIRD execution accuracy on the `dev50` subset, variants `full` and `bm25-k4`, official and corrected tracks. `PROVIDER=fake` (default) replays recorded model calls and database executions with no model and no database, checks every verdict against the recorded run and checks RESULTS.md's tables (CI runs it). `PROVIDER=ollama`/`anthropic` with `MODEL=...` runs live after `make bird`; `RECORD=1` stores the calls (`RESUME=1` continues an interrupted recording from the calls already stored). `VARIANTS`, `TRACKS`, `SUBSET=all` select the run. |
| `make eval-link` | Schema-linking table recall and precision on the 500 BIRD questions for every strategy (offline, in CI). |
| `make results` | Re-render RESULTS.md's BIRD tables from `eval/results/`. |
| `make bench` | Brings up the `lakehouse` and `airflow` profiles. It then tunes the trigger interval and `maxOffsetsPerTrigger`, produces 50,000 keyed events per run with the stream job and then the broker killed (SIGKILL) at three points, checks zero loss and zero duplicates in PostgreSQL and Iceberg, and reads freshness from `ops.quality_runs`. Writes `bench/results/m5-bench/`. About an hour. It recreates containers, so use a stack you can throw away. |
| `make bench-scale` | Sustained events/s and p95 at 1, 3 and 6 partitions, the per-stage ceilings (broker, Spark `noop`, Spark + Python, the PostgreSQL sink alone), the bottleneck, and the Iceberg sink's cost at P = 3. Writes `bench/results/m5-scale/`. About 2 hours. It truncates `raw.recentchange` before each run, so use a disposable stack. A rerun skips finished units. |
| `make bench-query` | Generates the sample scaled to `SCALE` rows (default `1m,20m`) in PostgreSQL and Iceberg (`tributary bench load`), then times ten queries on PostgreSQL, Trino over PostgreSQL and Trino over Iceberg (warm and cold), the three Iceberg layouts on the window queries, and one federated query. Writes `bench/results/m6-query/`. About 2 h for both scales. It recreates PostgreSQL, MinIO and Trino under `bench/compose.query.yaml`'s memory limits, stops the stream job and restarts the engines (dropping the host page cache through `sudo -n` where allowed), so use a disposable stack. A rerun skips finished units. |
| `make bench-results` | Re-render RESULTS.md's ingest tables from `bench/results/` (a unit test fails if they are stale). |
| `make serve` / `make up-api` | The API on localhost:8000 from the host / in Compose (profile `api`); both need `TRIBUTARY_API_KEYS`. |
| `make dist` | Build the sdist and wheel into `dist/`, `twine check --strict` them, and install the wheel (with `api`) in a clean venv outside the checkout and run it. The release workflow runs exactly this. |
| `make test-live` | The Anthropic provider against the real API, one call (skipped without `ANTHROPIC_API_KEY`; under $0.01 on Haiku). The `Live` workflow runs it by hand. |

On a database created before M3, run `uv run tributary warehouse
grant-agent` once to create the agent's role. Releases follow RELEASE.md:
a `v*` tag publishes `tributary-platform` to PyPI and the images to GHCR;
both workflows run by hand on a branch as dry runs.

## Conventions

- **Typed.** `mypy --strict` passes on `tributary/` and `eval/`. No `Any`
  without a comment saying why.
- **Formatted and linted** with ruff (line length 100). Imports sorted by
  ruff.
- **Tests** live in `tests/unit`, `tests/warehouse` (PostgreSQL only),
  `tests/integration` (the Compose stack), `tests/live`. Unit tests never
  touch the network or Docker; the Anthropic client is replaced
  by the `fake` provider, which replays recordings from
  `tests/recordings/` (for BIRD, database executions are recorded too, in
  `tests/recordings/bird-executions.jsonl.gz`, with value types kept). A recording is real model output (Claude, or a local
  model through Ollama) captured once with `make record`; never hand-write
  one. A changed prompt, schema or message misses its recording, so
  re-record after changing any of them.
- **Config via environment** (`tributary/config.py`, pydantic-settings).
  `.env.example` lists every variable with a comment. No secrets in the repo.
- **Logging** through structlog, JSON in containers, key-value in a terminal.
  Every request carries a `request_id`; every model call logs model, tokens,
  cost and latency.
- **SQL dialect** is PostgreSQL everywhere. sqlglot transpiles only in the
  corrected-label evaluation track.
- **Numbers.** A figure appears in README or RESULTS.md only if `make eval`
  or `make bench` produced it in this repository, with the raw output under
  `eval/results/` or `bench/results/` and the cost noted. Mocks and
  recordings are for tests, never for a reported number.
- **Model calls.** Default model `claude-opus-5-5`; evaluations use the
  cheapest model that answers the question and say which. Structured outputs
  for anything parsed by code. Prompt caching on the schema prefix.
- **Commits.** One logical step per commit, imperative subject under 72
  characters, body only when the why is not obvious. Every commit passes
  `make check`. Branches are `dev/<milestone>`; one PR per milestone.
- **Docs.** README is for users; `docs/` holds architecture notes and
  screenshots; PLAN.md is updated when scope changes, and a milestone
  checkbox is ticked only when its definition of done is met.

## Quality gates

A PR merges when:

1. `make check` is green locally and in CI.
2. New behaviour has tests that fail on the old code.
3. Any new reported number has its command, raw output and cost in
   RESULTS.md.
4. No secrets, no recorded API keys, no generated data over 10 MB.
5. The milestone's definition of done in PLAN.md is met, or the PR says
   exactly what is left.
