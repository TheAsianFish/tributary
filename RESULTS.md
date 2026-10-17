# Results

Every number here was produced by a command in this repository, on the
machine noted, with its raw output committed. Nothing is estimated or taken
from a mock. Model spend so far: **$0** (no model calls exist yet).

Machine for all runs below: 4 vCPU, 15 GB RAM, Linux 6.17, Docker 28.0.4,
Compose v2.38.2; Compose memory limits as in `compose.yaml` (Kafka 1 GB,
PostgreSQL 1 GB, stream job 3 GB).

## The sample stream (M1)

`make sample-stats` on `data/sample/recentchange.jsonl.gz` (50,000 consecutive
live events, 2026-10-16 23:45 to 2026-10-17 00:21 UTC). Raw output:
[`bench/results/m1-sample-stats.txt`](bench/results/m1-sample-stats.txt).

| | |
|---|---|
| Events / distinct ids | 50,000 / 50,000 (0 duplicates, despite three server-side disconnects resumed with `Last-Event-ID`) |
| Event-time span | 36 min 18 s (about 23 events/s) |
| Types | edit 24,188 · categorize 21,614 · log 2,485 · new 1,713 |
| Wikis | 160 (commonswiki 15,747, wikidatawiki 9,832, enwiki 8,610) |
| Bot share | 39.5% |
| Emit delay (`meta.dt` − `timestamp`) | p50 1.5 s, p99 9.4 s, max 144 s |
| Events behind the running max of event time | 27,030 (54.1%) |
| Lateness (running max − event time) | p50 1 s, p90 2 s, p99 8 s, p99.9 36 s, max 143 s |
| Dropped by a per-event watermark of 1 / 2 / 5 / 10 / 30 min | 17 / 1 / 0 / 0 / 0 |

**What it means.** More than half the events arrive out of event-time order,
but only by seconds; the worst straggler is 143 s late, and its lateness
matches the worst emit delay (144 s), so disorder comes from events emitted
late rather than from transport. The stream job's default watermark delay of
10 minutes would have dropped none of these events even with a watermark
advanced per event (Spark advances it once per micro-batch, which is more
lenient), a margin of about 4× over the worst case seen. The price is
state: see the next table.

## Ingest functional run (M1)

`docker compose down -v && docker compose up -d --wait`, then poll
`SELECT count(*) FROM raw.recentchange` until it reaches 50,000. The producer
replays the whole sample at its default 500 events/s. Two runs; this is a
check that the spine works, not a throughput benchmark (that is `make bench`,
M5). Raw progress and sink lines of the first run:
[`bench/results/m1-compose-replay.jsonl`](bench/results/m1-compose-replay.jsonl);
the count poll of the second run, which is where the time to the last row is
read from: [`bench/results/m1-compose-replay-poll.txt`](bench/results/m1-compose-replay-poll.txt).

| | |
|---|---|
| Produced / delivered / failed | 50,000 / 50,000 / 0 in 100.0 s (500.0 events/s) |
| Rows in `raw.recentchange` / distinct ids | 50,000 / 50,000 |
| Time from `up` to the last row | 113 s (first run); between 110 s and 115 s (second run, 5 s poll) |
| Micro-batches with data | 20 (5 s trigger) |
| Rows dropped by the watermark / malformed / canary | 0 / 0 / 0 |
| Rows skipped by `ON CONFLICT` | 0 (nothing reached the sink twice) |
| Dedup state after the last batch with data / after the closing empty batch | 29,245 / 27,952 keys (peak 30,827) |

The state size follows from how `dropDuplicatesWithinWatermark` expires keys:
a key lives until the watermark (newest event time − delay) passes its event
time + delay, so state spans about twice the delay of event time. At 23
events/s and a 10-minute delay that is about 27,600 keys. Eviction runs once
per micro-batch, so the count overshoots by up to a batch's worth of keys
while data flows (27,952 to 30,827 over the last ten batches) and settles at
27,952 once the final batch has evicted. The span is in event time, so
replaying faster should not change it; that has not been measured.

## Verified behaviour (M1)

These are properties checked by tests rather than measured quantities.

| Property | Where | Result |
|---|---|---|
| 5,000 events (fresh ids per run) through Kafka and Spark land once; after re-sending all 5,000, every row still carries the Kafka offset of its first copy, the job keeps running and Compose has not restarted it | `make up && make test-integration` (`tests/integration/test_pipeline.py`) | pass: 12.7 s on a fresh stack, 18.4 s on a stack already holding the full sample (whose newer events would put unshifted replays behind the watermark); 0 restarts of the job in both |
| A replayed or partly committed batch, and duplicates inside one batch, land once | `tests/integration/test_sink_postgres.py` against PostgreSQL 17 | pass |
| Spark 4.1.3 `dropDuplicatesWithinWatermark`: late rows are filtered with the *previous* batch's watermark but state is evicted with the *current* one (asserted on `numRowsRemoved` and `numRowsTotal` per batch), so a row up to one batch behind still gets through; a duplicate arriving after its key expired is dropped as late if it is behind the watermark, and passes as a new key if it is not (plain `dropDuplicates` would still drop it) | `tests/unit/test_stream_spark.py::test_deduplicate_semantics_across_micro_batches` | pass |
| Deleting the last commit-log entry makes Spark replay the same batch id with the same rows | same test | pass: only the sink's primary key keeps the replay out |
| Spark parser and the Python parser produce identical rows | `test_parse_events_matches_the_python_parser` | pass |

## Warehouse and lake (M2)

### Both sinks on the full sample

`docker compose --profile lakehouse down -v`, then `docker compose
--profile lakehouse up -d --wait postgres kafka minio lake-init stream trino`
(21 s until every service was healthy, images already built), then `docker compose up -d producer`
(the whole sample at 500 events/s) while polling both stores every ~7 s:
`count(*)` on `raw.recentchange` through `psql` and on
`iceberg.raw.recentchange` through Trino, then `count(DISTINCT event_id)` on both. Raw poll:
[`bench/results/m2-compose-replay-poll.txt`](bench/results/m2-compose-replay-poll.txt).

| | Warehouse (PostgreSQL) | Lake (Iceberg on MinIO, via Trino) |
|---|---|---|
| Rows after the replay / distinct ids | 50,000 / 50,000 | 50,000 / 50,000 |
| Seconds after the producer started until the last row | 112 | 133 |
| Trigger | 5 s | 1 min |

The lake arrives in steps (3,818 at 28 s, 32,451 at 71 s, 50,000 at 133 s)
because it commits once a minute: fewer, larger files at the cost of up to a
minute of extra latency. Its per-trigger offset cap is separate from the
warehouse's (200,000 vs 20,000); with the warehouse's cap a one-minute
trigger could never exceed 333 events/s and would fall behind the 500
events/s replay, which is what the first run of this check showed.

### Partition pruning, compaction and reconciliation on the sample

Same stack, right after the replay. Commands and full output (including the
`EXPLAIN ANALYZE` plan):
[`bench/results/m2-lake-sample.txt`](bench/results/m2-lake-sample.txt).
`tributary lake pruning HOUR` runs `EXPLAIN ANALYZE SELECT count(DISTINCT
event_id) ... WHERE event_ts >= HOUR AND event_ts < HOUR + 1h` and compares
the table scan's `Input: N rows ... Splits: K` with the table's own `$files`
metadata for that `hours(event_ts)` partition.

| Hour (UTC) | Files in the partition (`$files`) | Rows in the partition | Splits read by the scan | Rows read by the scan | Table total |
|---|---|---|---|---|---|
| 2026-10-16 23:00 | 2 | 19,271 | 2 | 19,271 | 4 files, 50,000 rows |
| 2026-10-17 00:00 | 2 | 30,729 | 2 | 30,729 | 4 files, 50,000 rows |
| 2026-10-16 23:00 after `tributary lake maintain` | 1 | 19,271 | 1 | 19,271 | 3 files, 50,000 rows |

Files of this size are one split each, so a scan that reads exactly the
partition's splits and rows has opened only that hour's files. `tributary
lake maintain` compacted the one closed hour (2 files rewritten into 1); the
00:00 hour is still inside the watermark (newest event 00:21:44 minus 10
minutes) and was left alone.

`tributary lake reconcile` then compared the closed hour: 19,271 distinct ids
in PostgreSQL, 19,271 rows and 19,271 distinct ids in Iceberg, the same id
fingerprint on both sides (-221,607,977,733: the sum of a 32-bit MD5 prefix
over the hour's distinct ids), lake lag 0 s; both checks `pass` in
`ops.quality_runs`. Two injected faults, through Trino, each failed the hour,
exited 1 and recorded `fail` (same file):

| Fault in the closed hour of the lake | Rows / distinct (warehouse 19,271) | Fingerprints equal | Result |
|---|---|---|---|
| One row copied a second time | 19,272 / 19,271 (`lake_duplicates = 1`) | yes | fail |
| Then: a copy of that row under a fresh uuid added, every row with the original id deleted (an exchange) | 19,271 / 19,271 | no (-219,942,845,227) | fail |

The exchange keeps every count equal, so a count-only comparison (the check
before the review fixes) passes it; only the fingerprint catches it.

### dbt and the warehouse DAGs

The steps of the `Warehouse` workflow, run locally against a fresh
`postgres:17` container: `uv run tributary sample load` (50,000 rows straight
into `raw.recentchange`), `make dbt`, `make test-warehouse`. Raw output:
[`bench/results/m2-warehouse-ci.txt`](bench/results/m2-warehouse-ci.txt).

| | Result |
|---|---|
| `make dbt` on the sample | PASS=57 WARN=0 ERROR=0: 2 seeds, 1 staging view, 1 incremental and 4 table marts, 49 data tests; dbt reports 5.11 s, the command 10.0 s |
| `fct_edits` rows updated by an incremental run (`pg_stat_user_tables.n_tup_upd`) | 0 with the current model; 25,901 with the model before the review fixes, whose `merge` rewrote every row the 60-minute lookback re-read (here the whole sample, loaded at once) |
| Mart sizes | `fct_edits` 25,901 · `dim_page` 20,677 · `agg_editor_activity` 2,304 · `agg_edits_hourly` 242 · `dim_wiki` 160 |
| Custom tests against fixtures | volume: flat pass, spike fail, outage (zero-row hour) fail, short history pass (fails with `min_history=2`), constant history pass, one event dated in a future hour pass (failed before the closed hour was capped at the current one); freshness: fresh pass, stale fail, stale within a wider limit pass, empty fail |
| `dag.test()` timings | `airflow db migrate` (SQLite) 3.0 s · `raw_recentchange` with new rows 7.1 s, with none 5.8 s (freshness tests pass, then skipped, no asset event), with none and a 0-minute freshness limit 6.2 s (failed, 2 `fail` rows recorded) · `warehouse_refresh` 30.1 s |
| `fct_edits` after a first build on an empty table | loads every edit once data arrives (`test_fct_edits_loads_after_a_first_build_on_an_empty_table`, first 500 events of the sample) |
| `warehouse_refresh` results | 49 rows in `ops.quality_runs` for the run, all `pass`; 8 rows in `ops.refresh_runs` (2 seeds, the staging view, 5 marts); catalog published with `manifest.json`, `catalog.json`, `values.json` (asserted by the test; row counts in the raw file) |

### The `airflow` profile on the Compose stack

On the stack of the previous sections (lake still holding the injected
exchange), `docker compose --profile airflow up -d --build --wait ...` (26 s,
fresh Airflow metadata database) and then nothing by hand for about 17
minutes. Raw output:
[`bench/results/m2-airflow-compose.txt`](bench/results/m2-airflow-compose.txt);
screenshot (an earlier run): [`docs/airflow-warehouse-refresh.png`](docs/airflow-warehouse-refresh.png).

| DAG run | Outcome |
|---|---|
| `raw_recentchange` at 16:55 | success: rows had arrived since the last event, asset event emitted with the new high-water mark |
| `warehouse_refresh`, triggered by that event | success in 38 s; 49 `pass` rows in `ops.quality_runs`; catalog version swapped in under `/data/catalog/current`; no user-name or title column in `values.json` |
| `raw_recentchange` at 17:00 | nothing new: the two freshness tests ran (2 `pass` rows recorded under the run's id), then the task skipped and no asset event was emitted |
| `lake_maintenance` for 16:07 (the latest interval, run on start) | `optimize_closed_hours`, `expire_snapshots`, `remove_orphan_files` success; `reconcile_closed_hours` **failed** after its retry on the injected exchange (`ids_match` false), so the run is red |

The first attempt at this run found a packaging bug, recorded at the end of
the same file: the Airflow image reused uv's cached wheel of the package from
its first build, so after the review fixes the scheduler ran stale code
(`raw_recentchange` failed to import, and the old count-only reconciliation
passed the exchange). The Dockerfile now reinstalls the package on every
build.

## Verified behaviour (M2)

| Property | Where | Result |
|---|---|---|
| 5,000 events (fresh ids, re-timed over three hours) reach both stores; PostgreSQL and Trino over Iceberg count the same 5,000 distinct ids, the lake has no duplicate rows, and the ids are the ones produced | `make up-lakehouse && make test-integration` (`tests/integration/test_lakehouse.py`) | pass |
| Once a replay's batches have committed, `ops.stream_progress` (the newest event of each fully written warehouse batch, which the reconciliation closes hours on) equals the replay's newest event | same file | pass |
| `EXPLAIN ANALYZE` of a one-hour filter reads exactly the hour's files (splits = `$files` count) and rows (= `sum(record_count)`), fewer than the table's | same file | pass |
| `lake_maintenance` under `dag.test()`: all four tasks green, at least one closed hour reconciled, every reconciliation row `pass` | `uv run pytest tests/warehouse -m lakehouse` after the integration tests | pass: 7.4 s; three closed hours checked (721, 1,800 and 1,800 ids, equal counts and fingerprints in both stores), the open hours left out (how many hours close depends on the wall-clock minute the replay starts at) |
| The whole integration run on a fresh stack | `bench/results/m2-integration.txt` | 7 passed in 87.1 s, stream job not restarted (`make test-integration` fails otherwise) |
| One failing check, of any kind, turns `warehouse_refresh` red only after every result is recorded; the catalog is then not republished | `tests/unit/test_refresh.py` | pass |
| A catalog reader never sees a half-written version | `tests/unit/test_catalog.py` (versioned directory, atomic symlink swap, last 3 kept) | pass |

## To measure

| What | Command | Milestone | Cost |
|---|---|---|---|
| Zero loss and zero duplicates with the job and the broker killed at three points; p50/p95 end-to-end latency; sustained throughput | `make bench` | M5 | $0 |
| Throughput and p95 at 1, 3 and 6 partitions with per-stage ceilings | `make bench-scale` | M5 | $0 |
| Postgres vs Trino over Iceberg latency, and pruning at 1M and 20M rows (partitioned vs unpartitioned vs uncompacted) | `make bench-query` | M6 | $0 |
| What the second Kafka read and the Iceberg sink cost in throughput, at P = 3 | `make bench-scale` | M5 | $0 |
| Execution accuracy on BIRD Mini-Dev (both label tracks) | `make eval` | M4, M7 | ~$3 Haiku grid, ~$7 Opus subset |
