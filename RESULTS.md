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

## To measure

| What | Command | Milestone | Cost |
|---|---|---|---|
| Zero loss and zero duplicates with the job and the broker killed at three points; p50/p95 end-to-end latency; sustained throughput | `make bench` | M5 | $0 |
| Throughput and p95 at 1, 3 and 6 partitions with per-stage ceilings | `make bench-scale` | M5 | $0 |
| Postgres vs Trino over Iceberg latency and pruning | `make bench-query` | M6 | $0 |
| Execution accuracy on BIRD Mini-Dev (both label tracks) | `make eval` | M4, M7 | ~$3 Haiku grid, ~$7 Opus subset |
