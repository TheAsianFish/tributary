# Results

Every number here was produced by a command in this repository, on the
machine noted, with its raw output committed. Nothing is estimated or taken
from a mock. Model spend so far: **$0**. Every model call so far ran on a
local open model through Ollama (no Anthropic key was available); the Claude
runs are listed under [To measure](#to-measure) with their commands.

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

## The agent on its own warehouse (M3)

### The guard suite

`make eval-guard` (offline, also in CI): 112 unsafe statements in
[`eval/guard/unsafe.yaml`](eval/guard/unsafe.yaml) and, as the benign set, the
40 gold queries of the warehouse question set, through the guard with the
catalog's allowlist (the five marts). Raw verdicts:
[`eval/results/guard.json`](eval/results/guard.json).

| | |
|---|---|
| Unsafe statements blocked | **112 / 112 (100%)**: dangerous functions 19, session 12, encoding tricks 11, outside the allowlist 11, DML 8, file access 8, system catalog 8, DDL 7, procedural 7, comment tricks 5, stacked 4, locking 4, privilege 4, exfiltration 4 |
| Gold queries blocked (false blocks) | **0 / 40** |
| Queries the 7B model wrote (46, repairs included) blocked | 0 / 46 |

The last row is from the recorded run below: every draft the model produced
for the 40 questions was a read of the marts, and the guard let each one
through (counted by re-checking `tests/recordings/warehouse.jsonl`).

### What the database layer alone stops

`make eval-guard-db`: the same 112 statements sent straight to
`PostgresExecutor` as the agent's role, **with the guard switched off**, on a
plain `postgres:17` set up like CI (`tributary sample load`, `make dbt`,
`tributary warehouse grant-agent`), statement timeout 1 s. Raw:
[`eval/results/guard-db.json`](eval/results/guard-db.json).

| Outcome | Statements |
|---|---:|
| Refused by PostgreSQL: syntax, one-statement prepared protocol, unknown objects, a backend that terminated itself | 53 |
| Refused: permission (raw, staging, temp tables, superuser-only functions, other roles) | 29 |
| Cancelled by the statement timeout | 7 |
| Refused: read-only transaction | 1 |
| **Ran** | **22** |

The 22 that ran are the guard's alone to stop: catalog reads
(`information_schema.columns`, `pg_roles`, `pg_user`, `pg_stat_activity`, a
`UNION` appending one, and `pg_stat_get_activity` reached through field
notation), `ops.quality_runs` (the role may read `ops` for provenance),
`version()`, `inet_server_addr()`, `current_user` and its keyword forms
`user`, `current_role` and `system_user`, `txid_current()`,
`pg_advisory_lock`, `pg_notify` (rolled back, so never delivered),
`pg_cancel_backend` (on the suite's made-up pid it returned false),
`set_config` (which can lift the statement timeout inside the query), a
three-part name for a mart (`tributary.marts.fct_edits`, which PostgreSQL
accepts for the current database), a top-level `VALUES`, and both `xml`
casts. None of them writes, but two are worth knowing:
`set_config('statement_timeout', '0', true)` makes the timeout only as
strong as the guard's ban on it, and PostgreSQL lets a role signal its own
sessions: `SELECT pg_terminate_backend(pid) FROM pg_stat_activity` (counted
under "refused" above, because the first backend it killed was its own)
would end every concurrent agent query. The guard blocks all of these.

**Field notation.** The seven statements added by the review are the
ones a reader of the guard's source would not think to try: PostgreSQL
reads `(x).f` as the call `f(x)`, so `SELECT (10).pg_sleep` sleeps,
`((NULL::int).pg_stat_get_activity).*` lists every backend, and
`(pid).pg_terminate_backend` over that list kills the agent's other
sessions, none of which contains a function call as far as sqlglot is
concerned (they parse as a plain dotted expression). The first version of
the guard let all three through, and the role stops none of them (sleeping,
reading `pg_stat_activity` and signalling one's own sessions need no
privilege). The guard now refuses every dotted expression that is not a
column, and denies the SQL-standard keyword functions (`user` is
`current_user`) that parse as columns. A name with four parts
(`tributary.marts.fct_edits.wiki`) is refused by the guard too, rather than
left for the database to reject.

Two executor defects came out of this run and were fixed before the numbers
above: a parameterless query went over the simple protocol, so a stacked
`SELECT 1; COMMIT; ...` ran every statement (the `EXPLAIN` pass included;
queries are now prepared statements, which hold exactly one), and a query
that terminated its own backend crashed the executor's rollback instead of
returning an error.

### The 40 warehouse questions

`make record PROVIDER=ollama` with `TRIBUTARY_OLLAMA_MODEL=qwen2.5-coder:7b`
(Ollama 0.40.6 on the CPU of the machine above, temperature 0, seed 0,
16k context): every question goes through the planner, the guard and the
agent's role, and its rows are compared with the gold query's as a multiset
of tuples with numerics rounded to six decimals (execution accuracy as BIRD
defines it, but stricter: BIRD compares sets, so duplicate rows would not
count there). Every model call was recorded
(`tests/recordings/warehouse.jsonl`); `make eval-warehouse` replays them.
Raw: [`eval/results/warehouse-ollama-qwen2.5-coder-7b.json`](eval/results/warehouse-ollama-qwen2.5-coder-7b.json)
and, for the same run on the 14B model (recordings in
`tests/recordings/warehouse-qwen2.5-coder-14b.jsonl`),
[`eval/results/warehouse-ollama-qwen2.5-coder-14b.json`](eval/results/warehouse-ollama-qwen2.5-coder-14b.json).
Cost $0 (local models).

| Model | Correct | Simple | Moderate | Challenging | Model calls | Repaired into a correct answer | Tokens in / out | Latency per question, p50 / p95 (nearest rank, n = 40) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| qwen2.5-coder:7b (Ollama, CPU) | **25 / 40** | 9 / 12 | 13 / 20 | 3 / 8 | 46 | 1 | 122,191 / 6,987 | 37.9 s / 112.8 s |
| qwen2.5-coder:14b (Ollama, CPU) | **31 / 40** | 9 / 12 | 18 / 20 | 4 / 8 | 42 | 1 | 110,576 / 6,764 | 80.8 s / 203.6 s |

With the 7B model every question produced a query that ran (status
`answered` 40 / 40); the 15 misses are wrong answers, not refusals or blocks. Six drafts failed in
PostgreSQL (five unknown columns, one ambiguous column) and were repaired
from the error message; one of those repairs ended correct. The 15 misses,
read one by one against the gold (all 15 are model errors; no gold query
needed changing):

| Error | Count | Example |
|---|---:|---|
| Wrong measure: a byte sum (`length_new - length_old`, `byte_delta`) or `pages_edited` where the question counts edits or wikis | 5 | "edits by Japanese wikis per family" answered in bytes |
| Wrong table or grain: hourly aggregate for a ten-minute window, per-editor flag for per-edit bot counts, hour for "minute" | 5 | `sum(edits) FROM agg_editor_activity WHERE is_bot` counts all edits of anyone who ever made a bot edit (10,168, gold 10,099) |
| Missing filter | 2 | Wikidata's hour without `wiki = 'wikidatawiki'` |
| Fan-out: `sum(dim_wiki.edits)` after joining `fct_edits` | 2 | 22,422,333 Wikipedia "edits" |
| Wrong interpretation of a definition | 1 | "wikis with no edits" as "no edit that changed bytes" |

**What model size fixes, and what it does not.** The 14B model fixes 9 of
the 7B model's 15 misses (three of the five wrong-measure errors, both
fan-outs, both missing filters, one grain error, the misread definition)
and adds three of its own (`wikis-with-edits` counted wikis with any event,
`wikidata-bot-vs-human` counted editors instead of edits,
`wikipedia-article-creations` counted every article instead of the ones
created), at about twice the latency. Six questions defeat both. Four of
them are the same error: answering from a pre-aggregated mart whose grain
does not fit the question (`agg_edits_hourly` for a ten-minute window and
for "the busiest minute", `agg_editor_activity` for a per-edit bot count
and for one hour's distinct editors). The 14B model even declared "the
busiest minute" unanswerable because "the tables do not have a
minute-level granularity", although `fct_edits.event_ts` is documented at
one-second resolution. The other two need a two-level aggregation (each
family's top wiki; a count of editors grouped by name), which both models
flattened. Choosing the table whose grain fits is a schema-linking
problem, not a size problem, and it is what M4's linker and M7's few-shot
examples target. All nine 14B misses were read against the gold as well;
none needed a change.

The replay, which is what the tests run, reproduces the recorded run exactly:
`make eval-warehouse` gives the same 40 verdicts and the same SQL in 2.1 s,
against about 31 minutes live for the 7B model (the sum of the per-question
latencies). The 14B run took 68 minutes, and
`TRIBUTARY_RECORDINGS_PATH=tests/recordings/warehouse-qwen2.5-coder-14b.jsonl
make eval-warehouse` replays it to the same 40 verdicts too. A latency is one
question end to end, repairs included, not one model call. The difficulty
labels in the question file are the author's judgement, not a rubric: both
models miss a "simple" question and answer a "challenging" one.

## Verified behaviour (M3)

| Property | Where | Result |
|---|---|---|
| Every gold query passes the guard, reads exactly the tables it lists, and runs as the agent's role | `tests/warehouse/test_agent_pg.py` | pass |
| The fake provider replays every recorded call (46 for 7B, 42 for 14B) and each question's status, correctness, attempts and SQL equal the recorded run, per recordings file | same file | pass |
| As the agent's role: `raw` and `staging` refused (permission), temp tables refused (read-only), `pg_sleep` cancelled, a fact-table cross join refused by the cost ceiling before running, an unqualified name resolves to nothing, rows capped with `truncated` set | same file | pass |
| A stacked `SELECT 1; COMMIT; DELETE ...` never runs past parsing; outside the executor, a `COMMIT` in a stacked string still leaves the next statement read-only (the role's default) | same file | pass |
| `/sql`, `/query`, `/freshness` on the real database: guarded and qualified SQL, provenance from `ops`, request id in header and body | same file | pass |
| API keys (401 without or with a wrong key, no start without one), request ids in every log line of a request, status mapping (422 blocked or refused, 502 provider failure, 503 no catalog) | `tests/unit/test_api.py` | pass |
| The Anthropic request: cached system prefix, structured output schema and effort in `output_config`, `fallbacks: "default"` with its beta header, no sampling or thinking parameters; refusal, truncation and error mapping; cost priced at the model that served | `tests/unit/test_providers_anthropic.py` (the SDK over a mocked transport) | pass |
| A recording misses (never replays a stale answer) when the system prompt, a message or the schema changes | `tests/unit/test_providers_fake.py` | pass |

## To measure

| What | Command | Milestone | Cost |
|---|---|---|---|
| Zero loss and zero duplicates with the job and the broker killed at three points; p50/p95 end-to-end latency; sustained throughput | `make bench` | M5 | $0 |
| Throughput and p95 at 1, 3 and 6 partitions with per-stage ceilings | `make bench-scale` | M5 | $0 |
| Postgres vs Trino over Iceberg latency, and pruning at 1M and 20M rows (partitioned vs unpartitioned vs uncompacted) | `make bench-query` | M6 | $0 |
| What the second Kafka read and the Iceberg sink cost in throughput, at P = 3 | `make bench-scale` | M5 | $0 |
| The 40 warehouse questions on Claude, recorded so the fake provider replays them: the M3 definition of done | `ANTHROPIC_API_KEY=... TRIBUTARY_RECORDINGS_PATH=tests/recordings/warehouse.jsonl make record PROVIDER=anthropic` (default model `claude-opus-5-5`) | M3 | est. under $1 on Opus 5.5 (about 46 calls with a cached ~3k-token prefix), a few cents on `TRIBUTARY_MODEL=claude-haiku-5-5` |
| Execution accuracy on BIRD Mini-Dev (both label tracks) | `make eval` | M4, M7 | ~$3 Haiku grid, ~$7 Opus subset |
