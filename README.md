# Tributary

Streaming data platform with an evaluated analytics agent.

Tributary ingests an event stream through Kafka into a PostgreSQL warehouse
with Spark Structured Streaming, keeps an Apache Iceberg copy of the raw
events on object storage for Trino, models the warehouse with dbt under
Airflow with data-quality checks, serves it through FastAPI, and answers
questions about it in plain English with a text-to-SQL agent that is graded
on BIRD Mini-Dev (execution accuracy, PostgreSQL) and guarded against unsafe
queries. The demo data is the live Wikipedia edit stream; one
`docker compose up` brings the pipeline up with a committed sample.

**Status: ingest, warehouse, lake and orchestration work (milestone 2 of
8).** Kafka feeds one Spark application with two streaming queries: one into
the PostgreSQL warehouse, modelled with dbt, and one into an Iceberg table on
MinIO, queried with Trino. Airflow rebuilds the marts when new rows land,
records every quality check in `ops.quality_runs`, publishes the catalog the
agent will read, and keeps the lake compacted and reconciled with the
warehouse hour by hour. The agent, the API and the benchmarks are planned in
[PLAN.md](PLAN.md); measured numbers live in [RESULTS.md](RESULTS.md).

## Why

Teams that collect event data want a pipeline they can trust (no lost or
duplicated events) and a way to ask questions of the result. The pipeline
half has to be correct under retries, restarts and late data before any
question about it is worth answering, so that is where this starts.

## Quickstart

Requirements: Docker with Compose v2. For the host commands: Python 3.12 and
[uv](https://docs.astral.sh/uv/); Java 17 only to run Spark on the host.

```bash
docker compose up -d --build        # Kafka, PostgreSQL, MinIO, the stream job, a one-shot sample replay
docker compose logs -f producer stream
docker compose exec postgres psql -U tributary -c \
  "SELECT wiki, count(*) FROM raw.recentchange GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
```

The producer replays the 50,000-event sample at 500 events/s into both
sinks. Add the query engine and the orchestrator with profiles:

```bash
docker compose --profile lakehouse up -d    # Trino on localhost:8081
docker compose exec trino trino --execute \
  "SELECT date_trunc('hour', event_ts) AS hour, count(*) FROM iceberg.raw.recentchange GROUP BY 1"
docker compose --profile airflow up -d      # Airflow UI on http://localhost:8080
```

With the `airflow` profile up, `raw_recentchange` notices new rows within five
minutes and triggers `warehouse_refresh` (dbt build, tests, quality results,
catalog); `lake_maintenance` runs hourly.

![Airflow: warehouse_refresh scheduled on the raw.recentchange asset, its last run green in 36 s, and the asset events it emitted for the five marts and the catalog](docs/airflow-warehouse-refresh.png)

The same steps run by hand from the host:

```bash
uv sync --all-extras
uv run tributary warehouse refresh                 # dbt seed+run, dbt test -> ops.quality_runs, publish the catalog
uv run tributary lake reconcile                    # closed hours, warehouse vs lake -> ops.quality_runs
uv run tributary lake pruning 2026-10-16T23        # does a one-hour filter read only that hour's files?
uv run tributary lake maintain                     # compact closed hours, expire snapshots, remove orphans
```

From the host, for the ingest side:

```bash
uv sync --all-extras
uv run tributary produce --limit 1000 --rate 0     # replay part of the sample, unthrottled
uv run tributary produce --live                    # the real Wikimedia stream (Ctrl-C to stop)
uv run tributary sample stats                      # what the sample looks like, incl. lateness
uv run tributary stream --available-now            # run the Spark job on the host until caught up
```

Demo: *screenshot placeholder.* To capture one, run the commands above and
record `docker compose logs -f stream` next to the `psql` query (for example
with `asciinema rec` and `agg`), saved as `docs/demo.gif`.

![dbt lineage: raw.recentchange and the namespaces seed feed stg_recentchange, which feeds fct_edits; fct_edits feeds dim_page, agg_edits_hourly, agg_editor_activity and, with stg_recentchange, dim_wiki; the event_types seed backs a relationships test](docs/dbt-docs-lineage.png)

## Architecture

```mermaid
flowchart LR
    W[Wikimedia EventStreams<br/>recentchange SSE] -. --live .-> P
    S[(Committed sample<br/>50,000 events, gzip JSONL)] -- replay --> P
    P[Producer<br/>trim to factual fields<br/>idempotent, key = event UUID] --> K[(Kafka 4.2 KRaft<br/>wiki.recentchange, 3 partitions)]
    subgraph Spark[Spark application, one checkpoint per query]
        WQ[warehouse query<br/>parse, watermark, dedup<br/>foreachBatch COPY + ON CONFLICT DO NOTHING]
        LQ[lake query<br/>parse, watermark, dedup<br/>Iceberg streaming sink]
    end
    K --> WQ
    K --> LQ
    WQ --> RAW[(PostgreSQL 17<br/>raw.recentchange<br/>PK event_id)]
    LQ --> ICE[(Iceberg v2 on MinIO<br/>lake.raw.recentchange<br/>partitioned by hours of event_ts)]
    JC[(JDBC catalog<br/>schema iceberg_catalog)] -.- ICE
    RAW --> DBT[dbt<br/>staging, marts, seeds<br/>freshness + volume tests]
    DBT --> MARTS[(marts.*)]
    DBT --> CAT[catalog/current<br/>manifest, catalog, value index]
    ICE --> TR[Trino 484<br/>iceberg + postgresql catalogs]
    RAW -.-> TR
    subgraph Airflow[Airflow 3.3]
        RW[raw_recentchange<br/>every 5 min: new rows?] -->|asset event| WR[warehouse_refresh]
        LM[lake_maintenance<br/>hourly]
    end
    WR -->|runs| DBT
    WR --> QR[(ops.quality_runs)]
    LM -->|optimize, expire, orphans| TR
    LM -->|reconcile closed hours| QR
```

**The hard part: every event exactly once, under retries, restarts and late
data.** No single layer can promise that on its own, so each one covers a
different failure:

| Failure | What handles it |
|---|---|
| Producer retries after a lost ack | Kafka's idempotent producer (`enable.idempotence`, `acks=all`): the broker drops the retried copy. |
| The same event sent twice (re-run, live-stream reconnect) | `dropDuplicatesWithinWatermark` on the event UUID, keyed in Spark's state store. A copy arriving after its key expired is normally behind the watermark and dropped as late; if the watermark jumped by more than the delay in one batch (after an outage) it passes as a new key, and then the primary key below stops it (both cases pinned by a test). |
| A micro-batch replayed after a crash between the sink's write and Spark's commit | The sink: `COPY` into a temp table, then `INSERT ... ON CONFLICT (event_id) DO NOTHING`. Writing the same batch twice is a no-op. Spark's state cannot help here: the replay re-runs the batch from the previous state. |
| Offsets lost or truncated in Kafka | `failOnDataLoss=true`: the job stops instead of silently skipping. |
| Events that arrive late | The watermark delay (10 min) is chosen from the measured lateness of the real stream: p99 8 s, max 143 s over 50,000 events, 0 that a 10-minute delay would drop. Rows dropped by the watermark are logged per batch. |
| A poison message (bad JSON, bad UUID, a timestamp past year 9999, a NUL byte in a text field) | Counted as malformed and filtered before the sink, so one bad record cannot fail every replay of its batch. |

Kafka transactions cannot reach PostgreSQL, so end-to-end exactly-once here
means at-least-once delivery into an idempotent sink. The fault-injection
benchmark that kills the job and the broker mid-stream is milestone 5.

**The lake copy has no primary key, so it is checked instead.** The Iceberg
query runs the same parser and dedup with its own checkpoint, and Iceberg's
streaming sink records the query id and batch id in each snapshot and skips
a batch it has already committed, so a replayed micro-batch does not append
twice. What it cannot absorb is a duplicate that got past the dedup state.
The `lake_maintenance` DAG therefore compares every *closed* hour (one the
watermark has passed, so neither sink will receive another row for it):
distinct ids in PostgreSQL against rows and distinct ids in Iceberg, one
`ops.quality_runs` row per hour. An hour is closed on the newest event of the
warehouse's *fully committed* batches (the sink records each in
`ops.stream_progress`), because a batch commits partition by partition and the
table's own maximum can run ahead of rows still being written. The two
queries trigger at different rates, so an event later than the watermark
delay could be kept by one and dropped by the other; the hour's details say
which kind of mismatch it was. A lake that has stopped would close no hours
and pass silently, so a separate `lake_lag` check fails when the lake's
newest event trails the warehouse's by more than 15 minutes. On the sample
the closed hour matched (19,271 = 19,271, 0 duplicates); with one row copied
into the lake a second time, the check failed with `lake_duplicates = 1`.

**Partitioning is measured, not assumed.** `tributary lake pruning HOUR` runs
`EXPLAIN ANALYZE` on a one-hour filter and compares what the scan read with
the table's `$files` metadata: on the sample, the 23:00 hour is 2 files and
19,271 rows of the table's 4 files and 50,000 rows, and the scan read 2
splits and 19,271 rows. The integration test asserts the same on every run.

One consequence of event-time processing to know before replaying data by
hand: the watermark only moves forward. Once the job has seen events from
time *T*, anything older than *T* − 10 min is dropped as late, so replaying an
old file into a stack that has already ingested newer events loses it. Start
from `make down` (which deletes the checkpoint) to replay from scratch.

### Design decisions

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| Event time | MediaWiki `timestamp` (when the change happened) | `meta.dt` (when EventGate emitted it) | Questions are about when edits happened. `meta.dt` trails the change (p50 1.5 s, max 144 s in the sample); it is kept as `emitted_at`. |
| What enters Kafka | An allowlist of factual fields | The full event | Editor comments are free text under CC BY-SA; nothing downstream needs them, and an allowlist cannot leak a new field by accident. |
| Sink write | `COPY` into a temp table, then `INSERT ... ON CONFLICT DO NOTHING`, one connection per Spark partition | Spark JDBC writer | The JDBC writer cannot express `ON CONFLICT`, so a replayed batch would fail or duplicate. Per-partition connections give P parallel writers, which the scaling benchmark needs. |
| Dedup state store | RocksDB with changelog checkpointing | The default heap store | State spans twice the delay of event time (27,952 keys once the sample replay has settled, 30,827 at peak); RocksDB keeps it off the JVM heap and checkpoints only each batch's changes. The provider cannot change without a new checkpoint, so it is chosen up front. |
| Stream image | The official `apache/spark:4.1.3-python3` with the Kafka connector jars baked in (sha256-pinned) and a Python 3.12 venv | `--packages` at start-up | Nothing is downloaded when the job starts, so a restart works offline and the jars cannot change underneath it. |
| One image for producer and job | Shared | Separate slim producer image | One build in CI. The API milestone adds a slim app image. |
| Writing the lake | A second streaming query with Iceberg's native sink, own checkpoint | Both sinks in one `foreachBatch` | Iceberg's sink skips a batch it already committed, so replays stay idempotent without a `MERGE` on `event_id`, and one sink's failure does not block the other's commit. Kafka is read twice; M5 measures what that costs. |
| Iceberg catalog | JDBC catalog in the existing PostgreSQL (schema `iceberg_catalog`) | REST catalog (Polaris, Lakekeeper), Hive Metastore | Spark and Trino both speak it and it adds no service: a commit is a row update under PostgreSQL's transactions. A REST catalog is the production direction (credential vending) and is on the roadmap. |
| Lake trigger | 1 minute, 200,000 offsets per trigger | The warehouse's 5 s | Every trigger commits a snapshot and at least one file per hour it touches; a slower trigger means fewer, larger files. The cap is separate because the warehouse's 20,000 per trigger would limit a one-minute trigger to 333 events/s. |
| Snapshot retention | 1 day (Trino's floor lowered to match) | Trino's default 7 days | A data batch commits twice (data, then an empty no-data batch that evicts dedup state), so a week of minute triggers is ~20,000 snapshots in every rewritten `metadata.json`. The sink finds its last committed batch by walking snapshot ancestry, so expiry is skipped while the newest streaming snapshot is older than half the retention: an outage longer than a day cannot expire the sink's last commit and duplicate the batch replayed on restart. |
| MinIO image | Built from the last release tag, in a multi-stage Dockerfile | `minio/minio` from Docker Hub | The upstream repository was archived and the image is no longer published. The store is reached only through S3 settings, so another S3-compatible store is a config change. |
| `fct_edits` loading | Incremental on arrival time (`ingested_at`) with a 60-minute lookback, `merge` on `event_id` | Incremental on event time; full rebuild | A late event has an old `event_ts` but a fresh `ingested_at`, so filtering on event time would miss it forever. The lookback covers a sink transaction that stamped its rows earlier but committed later. Rows already loaded are filtered out before the `merge` (which would otherwise rewrite every matched row: 25,901 updates per run on the sample, 0 now); `merge` stays as the safety net. |
| Data-quality tests | dbt generic tests plus two custom ones: arrival freshness, and an hourly volume z-score that counts empty hours as zero | Great Expectations, Soda, dbt source freshness | A few SQL statements, versioned with the models; results go to `ops.quality_runs` so answers can carry them. The volume test skips with too little history or zero variance rather than firing on noise; a fixture project proves each test passes and fails where it should. |
| Triggering the warehouse | A 5-minute DAG that emits the `raw.recentchange` asset only when rows arrived (high-water mark kept in the asset event's `extra`); `warehouse_refresh` is scheduled on the asset | A cron schedule; the stream job calling Airflow's API | No rebuild when nothing changed, no coupling of the stream job to the orchestrator, and the trigger's state lives with the asset it describes. When nothing arrived, the same task runs the freshness tests alone, so a stalled sink turns the run red instead of leaving the last `pass` standing. |
| Airflow image | `python:3.12-slim` plus this repository's `uv.lock` | `apache/airflow` with pip installs on top | The scheduler runs exactly the dbt, Trino client and Airflow versions the tests ran, with no constraint-file conflicts. |

## Results

From [RESULTS.md](RESULTS.md), where each number has its command and raw
output. No model calls yet, so $0 spent.

| Measurement | Result |
|---|---|
| Sample lateness (50,000 live events, stream order) | 54% out of order; p99 8 s, max 143 s; 0 dropped at a 10-min watermark |
| `docker compose up` replay | 50,000 produced, 50,000 rows, 0 dropped, 0 malformed; last row 113 s after `up` |
| End-to-end test | 5,000 events land once; re-sending all of them changes no row and does not stall or restart the job |
| Both sinks on the full sample | 50,000 / 50,000 distinct ids in PostgreSQL (last row 112 s after the producer started) and in Iceberg (133 s, one-minute trigger) |
| One-hour filter on the lake (sample) | Scan read 2 of 4 files and 19,271 of 50,000 rows, exactly the hour's `$files` |
| Per-hour reconciliation | Closed hour 19,271 = 19,271 ids with equal fingerprints, 0 duplicates; an injected duplicate fails it, and so does an exchange of ids that keeps every count equal |
| `dbt build` on the sample | PASS=57 (2 seeds, 5 marts, 1 staging view, 49 tests) |

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). `make check` is the gate (ruff,
mypy strict, offline unit tests including Spark in local mode);
`make test-warehouse` runs dbt and the warehouse DAG against a bare
PostgreSQL; `make test-integration` runs against Compose.

## Roadmap

M3 the guarded agent and API; M4 BIRD harness; M5 fault-injection and
scaling benchmarks (with the lake sink's cost measured); M6 Postgres vs Trino
over Iceberg; M7 agent quality; M8 release. Beyond that: an Iceberg REST
catalog in place of the JDBC catalog, and a Trino dialect for the guard.
Details in PLAN.md.

## Licence

MIT. The sample is derived from Wikimedia's public recentchange stream
(factual fields only, no comment text). Benchmark data (BIRD Mini-Dev,
CC BY-SA 4.0) is downloaded on demand and never committed.
