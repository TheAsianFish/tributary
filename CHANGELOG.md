# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). Every number below is measured in
[RESULTS.md](RESULTS.md), with the command that produced it.

## [Unreleased]

### Added

- **A differential oracle for the guard (M9).** `make eval-guard-diff` crosses
  every one-argument function in `pg_proc` with committed spelling templates
  and the 40 gold queries, and asks PostgreSQL, without running anything, which
  functions each admitted statement resolves to (a rolled-back temporary view
  and `pg_rewrite.ev_action`), judged against the guard's own policy. A
  dispatch-only workflow runs it.
- **A result byte cap.** `TRIBUTARY_AGENT_MAX_RESULT_BYTES` (8 MiB): PostgreSQL
  measures the values it is about to send and fails the query past the cap,
  reported as the repairable error `too_large`, before the client holds a
  byte of it.
- **A held-out BIRD sample (M10).** `holdout50`, 50 official questions
  disjoint from `dev50` and drawn by the same stratified hash rule, recorded
  on `qwen2.5-coder:7b` for `full`, `full+a` and `full+v+f+a`; `make eval`
  replays both subsets. `uv run python -m eval.bird.paired A B` compares any
  two variants question by question (exact McNemar), on each subset and
  pooled. Under a rule written down before the run, `full+v+f+a` stays the
  live default.
- **The `stream_loss` quality check (M11).** Each micro-batch's rows dropped by
  the watermark and records rejected by the parser (`malformed`, `future`,
  and canaries) are written to `ops.stream_progress` for both queries. The
  check sums the last 24 h and fails on any dropped row, or on more than 0.1%
  of the input rejected. It runs in the `raw_recentchange` DAG and as
  `tributary warehouse check-stream`, and an answer's provenance names it.
- `make bench-skew` (loss against one future-stamped event's skew, Spark local
  mode, with the prediction from the sample written first) and
  `make bench-poison` (the same on the Compose stack, both sinks).
- **`tributary stream reconcile` (M12).** A Spark batch read of the topic up to
  the offsets the warehouse query has committed, through the job's own
  parser: every record classed as landed, rejected or missing, the missing
  ones written back with the sink's idempotent write, and the pass recorded
  in `ops.reconcile_runs` and `ops.reconcile_missing`. `--start uncovered`
  reads from the first batch with drops no pass has covered; `--dry-run`
  only reports; `--every N` repeats. The `raw_recentchange` DAG runs a pass
  when drops are uncovered, so the Airflow image now carries a Java 21
  runtime, PySpark and the Kafka connector.
- `make bench-late` (the sample replayed twice, then 50,000 events with 100
  late and two rejected, with and without passes every 30 s).

### Changed

- **The future-skew bound is half the watermark delay (300 s), not a day.**
  `TRIBUTARY_MAX_FUTURE_SKEW_S` overrides it; the job warns at start when it
  is not below the delay. An event accepted S seconds ahead moves the
  watermark to `clock + S − delay`. With the one-day bound, one event 23 h
  ahead cost 39,000 of the sample's 50,000 events in local mode.
- `ops.stream_progress.max_event_ts` may be NULL (a batch the progress report
  recorded alone), and the table has the loss counters. `ops.sql` migrates an
  existing table.
- The fault runs' precondition refuses a stack holding events more than the
  skew bound ahead of the clock (it was 60 s).
- **`stream_loss` fails only on loss a reconciliation has not ruled out.** A
  dropped row in a batch some pass has read is a copy of a landed event or
  was written back, so the warehouse check now fails on dropped rows no pass
  has covered and on events a pass found missing that are still absent. With
  no pass on record it is the old check. The lake's check is unchanged.
- `ops.stream_progress` records each batch's Kafka start and end offsets and
  the streaming query's id. `tributary stream` is now a command group;
  `tributary stream` alone still runs the job.

### Fixed

- **A redelivered or replayed event read as a lost one.** Spark counts a late
  copy as dropped before the dedup can know its first copy landed, so a second
  replay of the sample turned `stream_loss` red for a day (35,554 "dropped",
  0 lost). A reconciliation pass now settles it.

- **The guard admitted calls written as a column of a relation.** PostgreSQL
  reads `w.f` as `f(w)` when `f` is not a column; over a mart, a subquery, a
  CTE or `VALUES`, 17 functions outside the policy were reachable this way
  (`pg_typeof`, `pg_column_size`, `quote_literal`, `record_send` and others,
  none volatile). The guard now knows each relation's columns and refuses a
  qualified name that is not a column but is a function PostgreSQL could
  call. A source whose columns it cannot name (a `LATERAL` subquery, a
  parenthesised join's alias, a qualifier no scope binds) fails closed, and a
  CTE is bound by the alias it is referenced under. Twelve statements were
  added to the unsafe suite.
- **A `char(n)` value's padding escaped the byte cap.** Its cast to `text`
  drops the trailing spaces the server still sends; it is now measured as it
  is.
- `test_marts_from_information_schema` skips with the reason when the marts
  are not built.
- **The integration tests replayed events hours into the future.** They placed
  their events just after the newest landed row; on a stack holding
  wall-clock events that was up to 2.8 h ahead of the clock, and the next
  benchmark run lost every event (the M5 incident). They now time events
  behind the clock.

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
