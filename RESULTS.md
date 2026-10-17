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
(34 s until every service was healthy), then `docker compose up -d producer`
(the whole sample at 500 events/s) while polling both stores every ~7 s:
`count(*)` on `raw.recentchange` through `psql` and on
`iceberg.raw.recentchange` through Trino. Raw poll:
[`bench/results/m2-compose-replay-poll.txt`](bench/results/m2-compose-replay-poll.txt).

| | Warehouse (PostgreSQL) | Lake (Iceberg on MinIO, via Trino) |
|---|---|---|
| Rows after the replay / distinct ids | 50,000 / 50,000 | 50,000 / 50,000 |
| Seconds after the producer started until the last row | 109 | 145 |
| Trigger | 5 s | 1 min |

The lake arrives in steps (10,015 at 28 s, 40,015 at 88 s, 50,000 at 145 s)
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
in PostgreSQL, 19,271 rows and 19,271 distinct ids in Iceberg, lake lag 0 s;
both checks `pass` in `ops.quality_runs`. To check that the reconciliation can
fail, one row of the closed hour was copied into the lake a second time
through Trino (`INSERT ... SELECT ... LIMIT 1`): the next run failed the hour
with `lake_duplicates = 1`, exited 1 and recorded `fail` (same file).

### dbt and the warehouse DAGs

The steps of the `Warehouse` workflow, run locally against a fresh
`postgres:17` container: `uv run tributary sample load` (50,000 rows straight
into `raw.recentchange`), `make dbt`, `make test-warehouse`. Raw output:
[`bench/results/m2-warehouse-ci.txt`](bench/results/m2-warehouse-ci.txt).

| | Result |
|---|---|
| `make dbt` on the sample | PASS=57 WARN=0 ERROR=0: 2 seeds, 1 staging view, 1 incremental and 4 table marts, 49 data tests; dbt reports 2.33 s, the command 6.6 s |
| Mart sizes | `fct_edits` 25,901 · `dim_page` 20,677 · `agg_editor_activity` 2,304 · `agg_edits_hourly` 242 · `dim_wiki` 160 |
| Custom tests against fixtures | volume: flat pass, spike fail, outage (zero-row hour) fail, short history pass (fails with `min_history=2`), constant history pass; freshness: fresh pass, stale fail, stale within a wider limit pass, empty fail |
| `dag.test()` timings | `airflow db migrate` (SQLite) 3.0 s · `raw_recentchange` with new rows 6.9 s, with none 0.1 s (skipped, no asset event) · `warehouse_refresh` 26.6 s |
| `fct_edits` after a first build on an empty table | loads every edit once data arrives (246 of 246 for the first 500 events; 0 before the fix) |
| `warehouse_refresh` results | 49 rows in `ops.quality_runs` for the run, all `pass`; 8 rows in `ops.refresh_runs` (2 seeds, the staging view, 5 marts); catalog published with `manifest.json`, `catalog.json`, `values.json` |

### The `airflow` profile on the Compose stack

On the stack of the previous sections, `docker compose --profile airflow up
-d --build --wait ...` (44 s) and then nothing by hand. Raw output:
[`bench/results/m2-airflow-compose.txt`](bench/results/m2-airflow-compose.txt);
screenshot: [`docs/airflow-warehouse-refresh.png`](docs/airflow-warehouse-refresh.png).

| DAG run | Outcome |
|---|---|
| `raw_recentchange` at 15:40 | success: rows had arrived since the last event, asset event emitted with the new high-water mark |
| `warehouse_refresh`, triggered by that event | success in 36 s; 49 `pass` rows in `ops.quality_runs`; asset events for the 5 marts and the catalog; catalog version swapped in under `/data/catalog/current` |
| `lake_maintenance` for 15:07 (the latest interval, run on start) | `optimize_closed_hours`, `expire_snapshots`, `remove_orphan_files` success; `reconcile_closed_hours` **failed** after its retry, because the stack still held the duplicate injected into the lake above: the check did its job under the real scheduler, and the run is red |

## Verified behaviour (M2)

| Property | Where | Result |
|---|---|---|
| 5,000 events (fresh ids, re-timed over three hours) reach both stores; PostgreSQL and Trino over Iceberg count the same 5,000 distinct ids, the lake has no duplicate rows, and the ids are the ones produced | `make up-lakehouse && make test-integration` (`tests/integration/test_lakehouse.py`) | pass |
| Once a replay's batches have committed, `ops.stream_progress` (the newest event of each fully written warehouse batch, which the reconciliation closes hours on) equals the replay's newest event | same file | pass |
| `EXPLAIN ANALYZE` of a one-hour filter reads exactly the hour's files (splits = `$files` count) and rows (= `sum(record_count)`), fewer than the table's | same file | pass |
| `lake_maintenance` under `dag.test()`: all four tasks green, every reconciliation row `pass` | `uv run pytest tests/warehouse -m lakehouse` after the integration tests | pass: 7.0 s; two closed hours checked (1,461 and 1,800 ids, equal in both stores), the open hours left out (how many hours close depends on the wall-clock minute the replay starts at) |
| The whole integration run on a fresh stack | `bench/results/m2-integration.txt` | 7 passed in 32.9 s, stream job not restarted |
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
