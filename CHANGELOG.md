# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). Every number below is measured in
[RESULTS.md](RESULTS.md), with the command that produced it.

## [Unreleased]

## [0.1.0] - 2026-11-11

The first release: the pipeline, the warehouse and lake, the guarded agent, its
benchmark harness and the ingest and query benchmarks. Published to PyPI as
`tributary-platform` (the name `tributary` was taken); the import name and the
command stay `tributary`.

### Added

- **Ingest (M1).** A producer that replays the committed 50,000-event sample of
  Wikipedia's `recentchange` stream, or reads the live stream with `--live`,
  into Kafka 4.2 (KRaft) with the idempotent producer. A Spark Structured
  Streaming job parses, watermarks (10 minutes, chosen from the sample's
  measured lateness) and deduplicates the events, then writes them to
  PostgreSQL by `COPY` and `INSERT ... ON CONFLICT DO NOTHING`, so a replayed
  micro-batch changes nothing.
- **Warehouse, lake and orchestration (M2).** A dbt project (staging, five
  marts, seeds, freshness and hourly volume-anomaly tests) with every check's
  result in `ops.quality_runs`. A second streaming query writes an Iceberg
  table on MinIO, partitioned by hour, which Trino queries. Airflow 3 runs
  `warehouse_refresh` when new rows land and `lake_maintenance` every hour
  (compaction, snapshot expiry, a reconciliation of every closed hour).
- **The agent and the API (M3).** `tributary ask`, `tributary sql` and a
  FastAPI service (`/query`, `/sql`, `/catalog`, `/freshness`). A planner turns
  the question into SQL with a model (Claude through the Anthropic SDK with
  structured outputs and prompt caching, or a local model through Ollama),
  repairs it from the guard's or PostgreSQL's error, and answers with the rows,
  the SQL and each table's freshness and quality status. A sqlglot AST guard
  and a read-only database role with a timeout and a cost ceiling stand
  between the model and the warehouse. The guard suite, `make eval-guard`,
  checks both.
- **BIRD Mini-Dev in PostgreSQL (M4).** `make bird` loads the eleven
  databases and verifies every gold query. The corrected-label track
  transpiles and verifies the SQLite gold. `make eval` grades execution
  accuracy on both tracks from recorded model calls and database executions,
  so CI replays it with no model and no database. `make eval-link` measures
  schema linking with the `full` and `bm25` linkers.
- **Ingest benchmarks (M5).** `make bench` kills the stream job (aimed between
  a sink's write and Spark's commit) and the broker, then checks zero loss and
  zero duplicates in both stores. `make bench-scale` measures sustained
  throughput at 1, 3 and 6 partitions and names the bottleneck.
- **Query engines (M6).** `tributary bench load` and `make bench-query` time
  ten queries on PostgreSQL, Trino over PostgreSQL and Trino over Iceberg at
  1M and 20M rows, warm and cold. They also compare three Iceberg layouts on
  pruning, and run one federated query.
- **Agent quality (M7).** Value hints from a value index, worked examples from
  the marts' dbt docs or BIRD's held-out questions, self-consistency voting by
  result hash, the M7 ablation with McNemar tests and a hand-checked failure
  taxonomy.
- **Release (M8).** The `tributary-platform` sdist and wheel, `make dist`
  (twine check plus a clean-venv install of the wheel), a tag-guarded PyPI
  workflow by trusted publishing, multi-architecture GHCR images for the API
  and the stream job, RELEASE.md and this changelog.

### Security

- The API limits each key's requests (a token bucket per key, 429 with
  `Retry-After`), caps request bodies, sets `nosniff`, `no-store` and a
  deny-all content security policy, serves no interactive docs by default, and
  exposes authenticated Prometheus metrics at `/metrics`.
- The guard blocks a function called through column notation on a FROM
  item (`p.pg_terminate_backend` over an `unnest` of the role's own pids
  killed its other sessions from a read-only transaction). It also blocks
  `IS DOCUMENT` and `XMLTABLE` reaching libxml2 and `OPERATOR()` into any
  schema, and refuses statements with more than 64 brackets before sqlglot's
  quadratic subscript parse. The suite has 119 statements, all blocked.
- Every base image in the Dockerfiles and `compose.yaml` is pinned by digest.
  Every image built here runs as a non-root user, and the API container runs
  with a read-only root filesystem, no capabilities and `no-new-privileges`.

[Unreleased]: https://github.com/TheAsianFish/tributary/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/TheAsianFish/tributary/releases/tag/v0.1.0
