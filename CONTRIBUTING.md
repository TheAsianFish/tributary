# Contributing

This file is the contributor guide and the first thing to read before working
on the repository. PLAN.md is the source of truth for scope and milestones;
RESULTS.md records every measured number with the command that produced it.

## Setup

Requirements: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker with
Compose v2, Java 17 (only for running the Spark job or its tests locally).

```bash
uv sync --all-extras          # creates .venv with dev, spark and eval extras
cp .env.example .env          # fill in ANTHROPIC_API_KEY only if you have one
docker compose up -d          # PostgreSQL + Kafka + MinIO + producer + Spark job
uv run tributary ask "Which wiki had the most edits in the last hour?"
```

Profiles: `docker compose --profile lakehouse up` adds Trino over the Iceberg
and PostgreSQL catalogs; `--profile airflow` adds the scheduler and UI;
`--profile bird` loads the benchmark databases (downloads about 300 MB on
first use, cached under `~/.cache/tributary`).

## Commands

| Command | What it does |
|---|---|
| `make check` | ruff format check, ruff lint, mypy strict, pytest (offline). The quality gate; CI runs exactly this. |
| `make test` | Unit tests only (fast, offline, deterministic). |
| `make test-integration` | Integration tests against Compose services (Kafka, Spark, Postgres). Needs Docker. |
| `make test-live` | Tests that call the real Anthropic API. Skipped without `ANTHROPIC_API_KEY`. |
| `make bird` | Download and load BIRD Mini-Dev into PostgreSQL. |
| `make eval` | Execution-accuracy benchmark. `VARIANT=`, `MODEL=`, `TRACK=official|corrected`, `LIMIT=` select the run. Writes `eval/results/` and updates RESULTS.md tables. |
| `make eval-link` | Schema-linking recall and precision (offline). |
| `make eval-guard` | Guard suite: block rate and false-block rate (offline). |
| `make bench` | Ingest fault-injection and latency benchmark against Compose. |
| `make bench-scale` | Sustained throughput and p95 at 1, 3 and 6 partitions, with per-stage ceilings. |
| `make bench-query` | Postgres vs Trino-over-Iceberg query latency and partition pruning. `SCALE=` sets the row count. |
| `make dbt` | `dbt build` on the warehouse project. |
| `make demo-data` | Regenerate the committed sample from the live stream (maintainers only; keeps factual fields, drops comment text). |

## Conventions

- **Typed.** `mypy --strict` passes on `tributary/` and `eval/`. No `Any`
  without a comment saying why.
- **Formatted and linted** with ruff (line length 100). Imports sorted by
  ruff.
- **Tests** live in `tests/unit`, `tests/integration`, `tests/live`. Unit
  tests never touch the network or Docker; the Anthropic client is replaced
  by the `fake` provider, which replays recordings from
  `tests/recordings/`. A recording is real API output captured once with
  `make record`; never hand-write one.
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
