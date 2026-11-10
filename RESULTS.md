# Results

Every number here was produced by a command in this repository, on the
machine noted, with its raw output committed. Nothing is taken from a mock,
and the only estimates are the costs of Claude runs not yet made, labelled
as such and priced from token counts this repository measured. Model spend so far: **$0**. Every model call so far ran on a
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

## BIRD Mini-Dev (M4)

Machine for this section: 4 vCPU (AMD EPYC 9V74), 15 GB RAM, Linux 7.0,
Docker 29.4.2, Compose 5.1.3; PostgreSQL 17 from `compose.yaml` (1 GB
limit), Ollama 0.40.6 in Compose (8 GB limit), CPU only.

### The benchmark in PostgreSQL

`make bird` on a fresh `postgres:17` (the Compose service): downloads
`minidev.zip` (800 MB, sha256-pinned), restores BIRD's PostgreSQL dump into
the database `bird` with one schema per BIRD database, grants the read-only
role `tributary_bird`, writes the catalogs the agent sees
([`eval/bird/data/catalogs.json`](eval/bird/data/catalogs.json)), runs every
official gold query and transpiles and verifies the corrected track. Raw:
[`eval/results/bird-gold.json`](eval/results/bird-gold.json).

<!-- bird:gold -->
| | |
|---|---|
| Tables loaded | 75 in 11 schemas (source: dump) |
| Official gold queries that execute | **500 / 500** (2 return no rows) |
| Corrected gold transpiled and verified on both engines | **449 / 498**; dropped: fails on PostgreSQL 34, different rows from SQLite 15 |
| Verified corrected questions worded exactly as in the official release | 333 / 449 |
| ... whose corrected gold returns different rows from the official gold | **66 / 333 (19.8%)** |
| Reworded questions whose gold rows differ (partly a different question) | 51 / 116 |
<!-- /bird:gold -->

**One label in five changes the answer.** The corrections keep BIRD dev's
original wording, which Mini-Dev changed for some questions, so a
different gold on a reworded question may just be a different question.
Counting only the 333 verified questions worded identically in both
releases (question and evidence), 66 corrected gold queries (19.8%) return
different rows from the official gold: graded against the corrections,
BIRD's own answer key scores 267 / 333 (80.2%) on those questions. The two
tracks are therefore not two measurements of the same thing, and the gap
between a model's scores on them is the part of its error that is the
benchmark's. (Over all 449 the count is 117, but 51 of those are among the
116 reworded questions, where the comparison says less.)

**Verification is what makes the corrected track usable.** sqlglot's
SQLite-to-PostgreSQL output alone runs and matches SQLite for 418 of the
498 corrected queries (65 fail on PostgreSQL, 15 return other rows). Five
rewrites of SQLite semantics, each a rule rather than a per-query patch,
bring that to 449: plain identifiers lowercased as the PostgreSQL copy of
BIRD lowercased them (reserved words such as the `order` table kept
quoted), `REAL` as `double precision` (8 bytes in SQLite), `LIKE` as
`ILIKE` (SQLite ignores ASCII case), a number compared with a text column
turned into a string (SQLite's type affinity), and `SUM(a = b)` cast to
integer. The 49 still dropped are
counted by reason above: arithmetic on text columns, `julianday`, `LIKE` on
a date, SQLite's lax `GROUP BY`, and 15 that run on both engines but return
different rows. Two of the mismatches are the database, not the query:

- **The dump's timestamps are 8 hours behind the SQLite files.** Every one
  of the 40,325 `codebase_community.users.creationdate` values (and the
  database's ten other `timestamptz` columns) reads exactly 8 hours earlier
  in BIRD's PostgreSQL dump than in its SQLite file, as if the dump had been
  taken in a UTC−8 session. PostgreSQL's gold queries are consistent with
  their own data, but an answer near a date boundary differs per engine
  (question 532: user `Niroshan` joined on 2012-01-01 in SQLite and on
  2011-12-31 in PostgreSQL). Two *official* PostgreSQL gold queries return
  no rows at all on BIRD's own dump, because they match an exact timestamp
  copied from the SQLite data (563: `T2.CreationDate = '2014-04-23
  20:29:39.0'`; 565 likewise): any answer to those two questions is graded
  against an empty result. Three more official golds (532, 533, 683) return
  different values than they do on the SQLite data, so 5 of the 31
  `codebase_community` questions have a gold answer moved by the dump.
- **Floating-point sums depend on the plan.** Several tables store 4-byte
  `real` columns; a parallel aggregate adds partial sums in the order its
  workers finish, so `SUM(consumption)` changed in the sixth digit between
  runs and one question flipped between verified and mismatched. Gold
  verification runs with serial plans (`max_parallel_workers_per_gather =
  0`), and three consecutive `make bird` runs now write byte-identical
  files; SQLite results are cached per database file and query, so the one
  corrected query that needs minutes on SQLite (a join on unindexed columns)
  is paid for once.

**The SQLite fallback.** `make bird` with `--source sqlite` (database
`bird_sqlite`, converted from the eleven SQLite files in 49 s, 3,898,114
rows): all 500 official gold queries execute, 494 return the same rows as
on the dump, and 451 corrected queries verify (before serial plans). The
six that differ are one `real` sum (1482) and the five shifted-timestamp
questions above, where the SQLite copy holds the original times: the
fallback is a working substitute, and on those five it is the more faithful
one.

**The scorer, checked against BIRD's.** BIRD's `calculate_ex` is
`set(predicted) == set(gold)` on the values the driver returned, so a
`numeric` average (`Decimal('61.5714285714285714')`) is never equal to a
`float8` gold (`61.57142857142857`): the same number in another type is a
wrong answer. The harness grades with exactly that rule (recorded
executions keep every value's type so a replay grades the same values), and
compiles the official function from the pinned upstream file
(`bird-bench/mini_dev@1c49a37`, sha256-checked) to grade every pair a
second time. On the 449 corrected-vs-official gold pairs, which are
near-misses by construction, the two agree on **449 / 449**; one verdict
(question 1039) is decided by the value type alone, and a type-blind
comparison would call it equal. The live run below reports the same check
on its own pairs.

### Schema linking without a model

`make eval-link` (offline, in CI): for each of the 500 official questions,
the gold tables are parsed from the gold SQL with sqlglot and compared with
the tables a strategy links from the question and its evidence. Raw:
[`eval/results/bird-link.json`](eval/results/bird-link.json) (and
[`bird-link-corrected.json`](eval/results/bird-link-corrected.json) for the
corrected track, within 0.01 of these).

<!-- bird:link -->
| Strategy | k | Recall | All gold tables found | Precision | Tables shown |
|---|---:|---:|---:|---:|---:|
| `full` | all | 1.000 | 100.0% (500/500) | 0.340 | 7.26 |
| `bm25` | 1 | 0.518 | 16.0% (80/500) | 0.942 | 1.00 |
| `bm25` | 2 | 0.768 | 52.8% (264/500) | 0.742 | 2.00 |
| `bm25` | 3 | 0.870 | 71.6% (358/500) | 0.575 | 3.00 |
| `bm25` | 4 | 0.927 | 82.0% (410/500) | 0.491 | 3.84 |
| `bm25` | 5 | 0.957 | 89.4% (447/500) | 0.437 | 4.60 |
| `bm25` | 6 | 0.981 | 95.4% (477/500) | 0.402 | 5.30 |
| `core` | | not implemented yet | | | |
<!-- /bird:link -->

A gold query reads 2.06 tables on average, and a database has between 3
and 13 (7.26 shown on average with `full`). Lexical retrieval finds every
gold table for 82% of questions at k = 4 while showing about half the
tables; the other 18% (90 questions) are questions the planner can no
longer answer correctly whatever it writes, so bm25 trades a hard ceiling
on accuracy for a smaller prompt.

**Why bm25 misses: hub tables lose to the tables that point at them.** Of
the 98 gold tables missed at k = 4, 59 are named in the question itself:
`formula_1.races` alone is missed 27 times, `financial.account` 17,
`superhero.superhero` 15. The word that names a hub table also appears in
the descriptions of the tables that reference it ("race" occurs in 8 of
`formula_1`'s 13 table documents), so it barely discriminates, and any
other shared word decides the ranking. In "Please give the name of the race
held on the circuits in Germany" the only term `races` matches is "race"
(score 1.04), while `qualifying` collects 1.09 from the single word
"evidence": the harness appends the question's evidence after a label
"Evidence:", and BIRD's column descriptions are full of "commonsense
evidence:" notes. Removing that one label word lifts the share of questions
with every gold table linked at k = 4 from 82.0% to 83.4% (measured with
the word added to the stopwords); the baseline is left as it is, since
the recorded bm25 run used it, and the word is one more thing `core` must
not be fooled by. Only
9 misses are bridge tables the question never names; the other 30 are
named neither directly nor by a join. This is the evidence the `core`
strategy is specified against (a table whose name the question uses is
kept, values pick their table, join paths add bridges): its contract tests
are in `tests/unit/test_linker_core.py`.

### Execution accuracy on dev50

`docker compose --profile ollama up -d ollama && make bird && make eval
PROVIDER=ollama MODEL=qwen2.5-coder:7b RECORD=1` (with `RESUME=1` for the
second half; see below): 50 questions of the official release, a fixed
sample stratified as the release is (15 simple, 25 moderate, 10
challenging; `eval/bird/data.py`), each answered through the product's own
planner, guard and executor (role `tributary_bird`, 30 s timeout) and
graded by BIRD's rule on both label tracks. 45 of the 50 are in the
verified corrected track; 13 of those are worded differently there and
were answered again in that wording. Ollama 0.40.6 on the CPU above,
temperature 0, seed 0, 16k context. Raw:
[`eval/results/bird-dev50-ollama-qwen2.5-coder-7b.json`](eval/results/bird-dev50-ollama-qwen2.5-coder-7b.json);
every model call is in `tests/recordings/bird-qwen2.5-coder-7b.jsonl` and
every query the run sent to PostgreSQL in
`tests/recordings/bird-executions.jsonl.gz`. Cost $0 (local model).

<!-- bird:ex -->
| Model | Variant | Track | EX | 95% interval | Simple | Moderate | Challenging | Model calls | Tokens in / out | Model time per question, p50 / p95 |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| qwen2.5-coder:7b (ollama) | `full-r0` | official | **21/50** (42.0%) | 29.4%–55.8% | 8/15 | 11/25 | 2/10 | 50 | 104,241 / 8,402 | 37.6 s / 112.7 s |
| qwen2.5-coder:7b (ollama) | `full-r0` | corrected | **16/45** (35.6%) | 23.2%–50.2% | 7/13 | 8/23 | 1/9 | 13 | 35,813 / 2,773 | 33.8 s / 106.0 s |
| qwen2.5-coder:7b (ollama) | `full-r2` | official | **21/50** (42.0%) | 29.4%–55.8% | 8/15 | 11/25 | 2/10 | 62 | 131,273 / 10,687 | 42.5 s / 225.1 s |
| qwen2.5-coder:7b (ollama) | `full-r2` | corrected | **18/45** (40.0%) | 27.0%–54.5% | 7/13 | 10/23 | 1/9 | 21 | 62,073 / 4,522 | 34.6 s / 190.5 s |
| qwen2.5-coder:7b (ollama) | `bm25-k4-r0` | official | **21/50** (42.0%) | 29.4%–55.8% | 9/15 | 9/25 | 3/10 | 50 | 84,189 / 8,664 | 47.2 s / 158.1 s |
| qwen2.5-coder:7b (ollama) | `bm25-k4-r0` | corrected | **16/45** (35.6%) | 23.2%–50.2% | 8/13 | 6/23 | 2/9 | 13 | 32,842 / 2,805 | 43.2 s / 74.2 s |
| qwen2.5-coder:7b (ollama) | `bm25-k4-r2` | official | **21/50** (42.0%) | 29.4%–55.8% | 9/15 | 9/25 | 3/10 | 62 | 111,740 / 11,161 | 51.2 s / 191.9 s |
| qwen2.5-coder:7b (ollama) | `bm25-k4-r2` | corrected | **17/45** (37.8%) | 25.1%–52.4% | 8/13 | 7/23 | 2/9 | 16 | 41,644 / 3,575 | 43.5 s / 158.2 s |
<!-- /bird:ex -->

The `-r0` rows are the same run read at its first attempt (the first model
call is the same request with or without repairs), so they cost nothing
extra. "Model calls" counts the calls a variant would make on its own: on
the corrected track only the 13 re-worded questions need new calls, and on
databases with at most four tables bm25 shows exactly what `full` shows, so
those requests were sent once and counted for both variants.

**What the numbers say, and what they cannot.**

- **42% (21 / 50), with a 95% interval of 29% to 56%.** Fifty questions
  bound EX to about ±13 points; the subset is for regression testing and
  for comparing variants on the same questions, not for a leaderboard
  figure. One of the 21 (question 563) is right only by accident: its
  official gold returns no rows on BIRD's dump (the timestamp shift above),
  and so does the model's query, so EX without it is 20 / 50.
- **bm25 neither helped nor hurt, on different questions.** Both variants
  score 21 / 50 on the official track; 18 questions are right in both, 3
  only with `full` and 3 only with `bm25-k4` (a 3-to-3 split, which says
  nothing either way at this size). bm25 left a gold table out of the
  prompt for 9 questions: the model refused 3 of those as unanswerable,
  answered 2 wrongly and still answered 4 correctly through another join
  path (a gold table missing from the prompt is not always fatal). It cut
  input tokens by 15% (111,740 against 131,273 with repairs) but raised the
  median time per question from 42 s to 51 s, because every question now has
  its own system prompt and Ollama's prefix cache no longer carries over
  between questions on one database, the same effect prompt caching would
  have on the Claude path.
- **The dominant failure is a refusal, not a wrong query.** With every
  table in front of it, the model set `answerable` to false on 11 of the 50
  questions (22%), each time on a question the schema can answer: it says
  that drivers' birth dates are missing (`formula_1.drivers.dob` is in the
  prompt), that "Branch location 1" is undefined (`financial` encodes
  branches as district ids), that the `colour` table has no 'No Colour'
  entry. Of the 50: 21 answered correctly, 13 answered wrongly, 11 refused,
  5 failed all three attempts. The planner's rule "if these tables cannot
  answer the question, set answerable to false" is there for the warehouse,
  where unanswerable questions are real; on a 7B model it is an exit taken
  far too often, and it is the first thing M7 measures.
- **Repairs fix syntax, not meaning.** 7 official questions needed more
  than one attempt (a PostgreSQL error fed back); none of them ended
  correct. On the corrected track, two re-worded questions were repaired
  into correct answers (`full`), one with `bm25-k4`. Most wrong answers
  run fine and return the wrong rows, which no error message can repair.
- **The corrected track grades the same answers the same way.** On the 32
  questions worded identically in both tracks, not one verdict changes
  between the official and the corrected gold: only one of those 32 labels
  differs (question 1265), and the model's query failed on it in both. The
  tracks' different totals come from the 13 re-worded questions, answered
  once in each wording.
- **The scorer.** BIRD's own `calculate_ex` agreed with the harness on all
  134 (prediction, gold) pairs where the predicted query ran; the type-blind
  count equals EX on every row of the table (no verdict in this run turned
  on a value type), and the stricter multiset count is 17 / 50 for `full` on
  the official track: BIRD's set rule accepts 4 answers that repeat the
  gold's rows (question 850 returns 76 rows for a 3-row gold).

**Recording, and how it was checked.** The run was recorded in two
sittings: the first was stopped after 52 model calls to fix two harness
defects a review found (each variant re-ran the gold query and re-sent an
identical request, while a recording keeps one outcome per request, so a
replay could have graded a different reply than the live run did), and
`--resume` then served those 52 from the recordings and made the other 83
calls live (69 min). Every reported verdict is therefore graded from the
recorded reply and from this run's own query executions, and the offline
replay (`make eval`, which CI runs) reproduces all 190 verdicts of the
report in 0.4 s with no model and no database.

### Cost

<!-- bird:ledger -->
| Run | Variant | Model calls | Tokens in / out | Cost | Same tokens on `claude-haiku-5-5` / `claude-opus-5-5` |
|---|---|---:|---:|---:|---:|
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `full-r0` | 63 | 140,054 / 11,175 | $0.00 | $0.02 / $0.78 |
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `full-r2` | 83 | 193,346 / 15,209 | $0.00 | $0.03 / $1.08 |
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `bm25-k4-r0` | 63 | 117,031 / 11,469 | $0.00 | $0.02 / $0.70 |
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `bm25-k4-r2` | 78 | 153,384 / 14,736 | $0.00 | $0.02 / $0.91 |

Estimated cost of the live runs: the token counts `bird-dev50-ollama-qwen2.5-coder-7b.json` measured for its two variants with repairs (346,730 in / 29,945 out, both tracks), scaled by question count, at list price with no cache hits (an upper bound on input: the planner caches its schema prefix), Claude's own tokenizer and thinking tokens not modelled:

| What | Command | `claude-haiku-5-5` | `claude-opus-5-5` |
|---|---|---:|---:|
| dev50, both variants, both tracks | `make eval PROVIDER=anthropic MODEL=<model> RECORD=1` | $0.05 | $1.99 |
| all 500, both variants, both tracks | `make eval PROVIDER=anthropic MODEL=<model> SUBSET=all RECORD=1` | $0.50 | $19.86 |
<!-- /bird:ledger -->

## Ingest under faults and at scale (M5)

These numbers come from `make bench` and `make bench-scale`. They make no model calls and cost $0. Raw output for every
run (one JSON file per unit of work, with the progress lines, samples and
settings it was judged on) is under
[`bench/results/m5-bench/`](bench/results/m5-bench/) and
[`bench/results/m5-scale/`](bench/results/m5-scale/). The tables between the
`bench:` markers are rendered from those files by `make bench-results`, and a
unit test fails if they drift apart.

Machine: the same class as the rest of this file (4 vCPU, 15 GB). Every service shares the four cores; there are no CPU limits, so CPU taken by the load generator, the broker or PostgreSQL is CPU the stream job does not get:

<!-- bench:environment -->
4 vCPU (AMD EPYC 9V74 80-Core Processor), 15.6 GiB RAM, Linux 7.0.0-1019-azure, Docker 29.4.2, Compose 5.1.3. Memory limits: kafka 1 GiB, minio 0.5 GiB, postgres 1 GiB, stream 3 GiB. CPU limits: none (every container may use all cores).
<!-- /bench:environment -->

### Throughput at 1, 3 and 6 partitions

`make bench-scale` (1 h 50 min). For each partition count P the topic has P partitions, Spark runs `local[P]` with `spark.sql.shuffle.partitions = P`, and the sink opens P connections. Every run starts from an empty checkpoint and an empty `raw.recentchange`, with the lake off except in the Iceberg row. Settings: 5 s trigger, `maxOffsetsPerTrigger` 200,000 (cap / trigger is 40,000 events/s, above every rate reached, so the cap never binds). These are the M4 trigger and a raised cap; the tuning below changed the shipped defaults afterwards. At these loads batches run back to back, so the trigger hardly matters here.

**How each number is measured.**
- **Sustained** is the highest step of an open-loop ladder the job kept up with. Steps are 90 s and start at 0.75 times the full drain rate, rising 15% until a step fails or falling 15% until one passes. A step passes when the queue each micro-batch finds (the broker's latest offsets minus the batch's start offsets) grows by at most 5% of the offered rate per second over the step's last 60 s (`bench/rules.py`). The resolution is therefore 15%, and the range across the three repetitions shows it.
- **p95** is end-to-end latency (`ingested_at` minus the producer's Kafka timestamp, one host clock) over a separate 3-minute run at 80% of that repetition's sustained rate, after a 30 s warm-up.
- **The stage ceilings** are drain rates of one 1.2M-event backlog, measured from an empty checkpoint each time: Spark with the `noop` sink (Kafka read, parse and dedup only), Spark with the `python` sink (rows also handed to the Python workers, then dropped), and the full job.
- **The PostgreSQL sink alone** replays the full job's batch shapes from P processes through the same `write_rows`, with no Spark.
- **The broker** is measured with Kafka's own `kafka-consumer-perf-test` and `kafka-producer-perf-test` (sample events, `acks=all`, idempotent, lz4), run inside its container.

<!-- bench:scale -->
| P | Sustained events/s, median (range) | p95 at 80% of it, median | Full drain | Spark `noop` | Spark + Python | PostgreSQL sink alone | Broker (consume / produce) | Bottleneck |
|---|---|---|---|---|---|---|---|---|
| 1 | 18,524 (18,524–21,303) | 17.18 s | 24,699 | 44,998 | 34,734 | 88,027 | 821,918 / 212,465 | spark (JVM) |
| 3 | 29,765 (25,882–29,765) | 14.12 s | 34,510 | 73,926 | 48,414 | 91,387 | 329,308 / 265,604 | spark (JVM), CPU-bound (host 3.5 of 4 cores busy) |
| 3 + Iceberg | 25,882 (22,506–25,882) | 42.07 s | – | – | – | – | – | see below |
| 6 | 28,753 (21,742–28,753) | 14.89 s | 33,337 | 77,796 | 44,600 | 115,868 | 950,872 / 204,255 | spark (JVM), CPU-bound (host 3.7 of 4 cores busy) |
<!-- /bench:scale -->

The bottleneck is named by the rule fixed in `bench/rules.py`. Within one Spark task the stages run one after another on the same rows, so their per-event times add up:
- JVM time is 1 / `noop`;
- the trip to Python is 1 / `python` minus 1 / `noop`;
- the database's time is 1 / its replay rate.

The biggest share is the bottleneck. The broker runs in parallel in its own process, so it would be named only if its own ceiling were below the pipeline's, and it is 10 to 30 times above it. The table below is the evidence for each attribution. Its second column checks the model: the stage times, added up, predict the full drain within 1–9% at every P.


<!-- bench:evidence -->
| P | Per-event time: JVM / transfer / database (µs) | Predicted vs measured drain | Database share of the sink's time | CPU at the top sustained step: stream / PostgreSQL / whole host (cores of nproc) | PostgreSQL backends, busy share and top wait |
|---|---|---|---|---|---|
| 1 | 22.2 / 6.6 / 11.4 | 24,906 vs 24,699 | 29% | 1.17 / 0.23 / 2.16 | 13% busy; CPU:active 53% |
| 3 | 13.5 / 7.1 / 10.9 | 31,648 vs 34,510 | 29% | 2.15 / 0.42 / 3.53 | 16% busy; CPU:active 43% |
| 3 + Iceberg | – | – | – | 2.24 / 0.31 / 3.48 | 9% busy; CPU:active 47% |
| 6 | 12.8 / 9.6 / 8.6 | 32,204 vs 33,337 | 32% | 2.34 / 0.41 / 3.72 | 16% busy; CPU:active 33% |
<!-- /bench:evidence -->

**What it means.**

*The bottleneck.* The limit is the Spark side of the job: reading Kafka, parsing the JSON, and keeping the dedup state in RocksDB. At every P it is the biggest share of per-event time, and it beats the trip to the Python worker and the PostgreSQL write. It is not the broker, which reads 330,000–950,000 events/s. It is not the database either:
- PostgreSQL alone takes the same batches at 88,000–116,000 rows/s.
- During the sustained steps its backends were busy 13–16% of the time, and when busy they were mostly running on CPU, not waiting on locks or I/O.

*From P = 1 to P = 3.* At P = 1, one task does everything in sequence. The host is half idle (2.2 of 4 cores busy), and the job sustains 18,500–21,300 events/s.

Three partitions give three tasks:
- The JVM's per-event time falls from 22 to 13.5 µs.
- The sustained rate rises to 25,900–29,800.
- The host reaches 3.5 of 4 cores. That is the stream job 2.2, the broker 0.4, PostgreSQL 0.4 and the load generator about 0.35, measured from its own `getrusage`.

*From P = 3 to P = 6.* There are no more cores to use:
- The `noop` ceiling barely moves (73,900 to 77,800).
- The Python transfer gets slower, as six Python workers compete for four cores.
- The sustained rate does not improve (21,700–28,800 events/s).

So on this machine class the scaling curve stops at three partitions because the box is CPU-bound in Spark, as PLAN.md's risk table anticipated. More partitions would help only with more cores.

*Latency at 80% load.* p95 is 14–17 s. Near saturation each batch carries 70,000–110,000 events and runs longer than the trigger, so an event waits for the batch in progress and then for its own. Latency at ordinary rates is in the fault and tuning tables below: p95 under 3 s at 500 events/s.


### What the Iceberg sink costs at P = 3

<!-- bench:iceberg -->
| P = 3 | Sustained events/s, median | p95 at 80% | Stream job CPU at the top step | Lake batch time at the top step, max |
|---|---|---|---|---|
| warehouse only | 29,765 (25,882–29,765) | 14.12 s | 2.15 | – |
| warehouse + Iceberg | 25,882 (22,506–25,882) | 42.07 s | 2.24 | 32.18 s |
<!-- /bench:iceberg -->

Same ladder at P = 3, with the lake query running beside the warehouse query: a one-minute trigger, and a cap of 5,000,000 so the cap does not limit it.

The second sink costs one step of the ladder: the median sustained rate falls from 29,800 to 25,900 events/s, about 13%. It also roughly triples p95 at 80% load, from 14 s to 42 s, for three reasons:
- the lake reads Kafka a second time;
- it parses and deduplicates everything again;
- it writes Parquet to MinIO in batches of up to 32 s.

Those batches compete for the same cores and stretch the warehouse batches that overlap them. The lake kept up: every lake batch finished well inside its one-minute trigger. The second Kafka read is the price PLAN.md accepted for keeping the two sinks' failures independent, and this is its measured size on 4 vCPU.


### Exactly-once with the job and the broker killed

`make bench`, fault phase (30 min). Each run sends 50,000 events with fresh ids at 500 events/s (100 s) into the platform's own topic, with the stream job in its default configuration: both queries, the tuned defaults below, and Airflow and Trino running. Each kill is SIGKILL, followed by 10 s down and a restart on the same volumes.

- Scenario `stream` kills the job three times, when 20%, 50% and 80% of the events have been sent.
- Scenario `broker` kills Kafka at the same points.
- Scenario `none` is the control.

Afterwards every acknowledged id is fetched back by its run's id range from `raw.recentchange` and, through Trino, from the Iceberg table, and compared with the ids produced. Iceberg's `$snapshots` table is also checked for any streaming batch id committed twice.

<!-- bench:faults -->
| Scenario | Run | Kills (window hit) | Acked | PostgreSQL missing / dup | Iceberg missing / dup | Iceberg batches committed twice | Rows the key absorbed | Latency p50 / p95 / max, whole run | p50 / p95 after the first 30 s |
|---|---|---|---|---|---|---|---|---|---|
| none | 1 | – | 50,000 | 0 / 0 | 0 / 0 | 0 | 0 | 2.00 s / 11.20 s / 15.84 s | 1.72 s / 3.06 s |
| none | 2 | – | 50,000 | 0 / 0 | 0 / 0 | 0 | 0 | 1.66 s / 2.62 s / 3.55 s | 1.64 s / 2.60 s |
| none | 3 | – | 50,000 | 0 / 0 | 0 / 0 | 0 | 0 | 1.64 s / 2.63 s / 3.62 s | 1.68 s / 2.73 s |
| stream | 1 | 3 (2 of 2 checked) | 50,000 | 0 / 0 | 0 / 0 | 0 | 3,103 | 27.55 s / 60.73 s / 64.72 s | 31.52 s / 61.44 s |
| stream | 2 | 3 (2 of 2 checked) | 50,000 | 0 / 0 | 0 / 0 | 0 | 996 | 22.11 s / 49.85 s / 53.40 s | 25.61 s / 50.60 s |
| stream | 3 | 3 (2 of 2 checked) | 50,000 | 0 / 0 | 0 / 0 | 0 | 997 | 24.80 s / 53.97 s / 58.75 s | 27.81 s / 54.88 s |
| broker | 1 | 3 | 50,000 | 0 / 0 | 0 / 0 | 0 | 0 | 22.13 s / 55.37 s / 60.44 s | 24.17 s / 47.13 s |
| broker | 2 | 3 | 50,000 | 0 / 0 | 0 / 0 | 0 | 0 | 25.89 s / 62.30 s / 67.40 s | 27.78 s / 54.16 s |
| broker | 3 | 3 | 50,000 | 0 / 0 | 0 / 0 | 0 | 0 | 12.69 s / 28.99 s / 31.76 s | 14.46 s / 29.16 s |
<!-- /bench:faults -->

A kill between two micro-batches proves nothing: the restarted job finds every batch committed and replays none. Each stream kill is therefore aimed at the window where a replay happens: the batch's offsets are logged, its commit is not. Each kill has its own target:

- **`warehouse-written`** fires when the sink's row for batch N appears in `ops.stream_progress`. By then all of the batch's rows are committed in PostgreSQL, and Spark has not yet written its commit.
- **`lake-committed`** fires when the Iceberg table's metadata pointer moves in the JDBC catalog. The snapshot is committed, Spark's commit is not.
- **`warehouse`** fires anywhere inside a warehouse batch.

Both detections poll PostgreSQL every 2 ms. The kill writes `1` to the container's `cgroup.kill` through an already-open `sudo tee`, which kills every process in the container at once. A first attempt with `docker kill` arrived after Spark had committed every time, because the CLI and the daemon take tens of milliseconds; the bench falls back to `docker kill` only where sudo is unavailable, and records the method used. The checkpoint, read from the stopped container's volume, shows each kill landed where it was aimed: the batch is logged in `offsets/` and missing from `commits/`, so the restart ran it again.

<!-- bench:kills -->
| Run | Aimed at | Method | Batch | Checkpoint after the kill: offsets / commits | Window hit | Iceberg snapshots carrying that batch id |
|---|---|---|---|---|---|---|
| 1 | warehouse-written | cgroup.kill | 132 | 132 / 131 | yes | – |
| 1 | lake-committed | cgroup.kill | 18 | 18 / 17 | yes | 1 |
| 1 | warehouse | cgroup.kill | 136 | – | not checked | – |
| 2 | warehouse-written | cgroup.kill | 201 | 201 / 200 | yes | – |
| 2 | lake-committed | cgroup.kill | 22 | 22 / 21 | yes | 1 |
| 2 | warehouse | cgroup.kill | 205 | – | not checked | – |
| 3 | warehouse-written | cgroup.kill | 289 | 289 / 288 | yes | – |
| 3 | lake-committed | cgroup.kill | 29 | 29 / 28 | yes | 1 |
| 3 | warehouse | cgroup.kill | 292 | – | not checked | – |
<!-- /bench:kills -->

**What it means.** 18 kills across 9 runs (9 of the job, 9 of the broker), and 450,000 events landed exactly once in both stores.

*The `warehouse-written` kills.* Each one replayed a batch whose rows were already in PostgreSQL. The primary key absorbed every replayed row: the "Rows the key absorbed" column counts 996–3,103 of them per run, from the sink's own `duplicates_skipped`, and no row of the run was duplicated.

*The `lake-committed` kills.* Each one replayed a lake batch whose snapshot was already committed. Afterwards exactly one snapshot carries that batch id, so Iceberg's streaming sink recognised the batch and skipped the second append. This is the guarantee the lake relies on instead of a primary key, and it held all three times.

*The broker kills.* None of them restarted the stream job: Spark's Kafka consumer waited for the broker to return. The idempotent producer buffered through each outage, at the cost of sending for 115–121 s instead of 100 s. Nothing was lost, so a broker that dies with its data still in the page cache comes back with every acknowledged write.

*Latency during faults.* The p50 of 13–28 s and maximum of 32–67 s during fault runs are the outages. Each restart of the job takes 8–10 s to come back healthy (the broker 16–25 s), then catches up. In the control runs p95 is 2.6–3.1 s once the job is warm. The control's first run starts seconds after the job was recreated, which is the 11 s p95 over its whole run; the right-hand column leaves out the first 30 s.

*What was not tested.* A broker that loses its page cache (a machine crash rather than a process kill) would lose writes that `acks=all` with one replica has not flushed. With one broker that is a known limit, not something this bench can show.


### Trigger interval and backpressure

`make bench`, tuning phase. These runs used the M4 defaults (5 s trigger, `maxOffsetsPerTrigger` 20,000) except for the setting under test.

- **Trigger interval:** the stream job in its default configuration (both queries) on a fresh topic and checkpoint, at 500 and 5,000 events/s for 90 s each; latency after a 20 s warm-up.
- **Cap:** a 300,000-event backlog drained from an empty checkpoint with the lake off, at the 5 s trigger.

<!-- bench:trigger -->
| Rate (events/s) | Trigger | p50 | p95 | p99 | Batch time p50 | Stream job CPU (cores) |
|---|---|---|---|---|---|---|
| 500 | 1 second | 1.34 s | 2.90 s | 3.51 s | 854 ms | 1.34 |
| 500 | 2 seconds | 1.75 s | 2.95 s | 3.75 s | 743 ms | 0.86 |
| 500 | 5 seconds | 3.33 s | 5.55 s | 5.95 s | 890 ms | 0.53 |
| 5,000 | 1 second | 1.64 s | 5.37 s | 6.44 s | 948 ms | 1.78 |
| 5,000 | 2 seconds | 2.31 s | 9.37 s | 11.05 s | 1,362 ms | 1.30 |
| 5,000 | 5 seconds | 19.00 s | 25.76 s | 27.83 s | 1,479 ms | 0.83 |
<!-- /bench:trigger -->

<!-- bench:cap -->
| `maxOffsetsPerTrigger` | Ceiling at a 5 s trigger | Batches | Longest batch | 300,000-event backlog cleared in | Effective rate | Stream job peak memory |
|---|---|---|---|---|---|---|
| 20,000 | 4,000 events/s | 16 | 11.22 s | 80.1 s | 3,745 events/s | 1.30 GiB |
| 100,000 | 20,000 events/s | 4 | 13.86 s | 24.7 s | 12,146 events/s | 1.33 GiB |
| 250,000 | 50,000 events/s | 2 | 26.41 s | 30.9 s | 9,709 events/s | 1.39 GiB |
<!-- /bench:cap -->

**What it means, and what changed.**

*The old cap was a speed limit.* With a processing-time trigger, a batch that finishes early waits for the next trigger, so the job can never ingest more than cap / trigger: 20,000 per 5 s is 4,000 events/s. At 5,000 events/s the job fell further behind for the whole run, p50 19 s and rising. A 300,000-event backlog took 80 s to clear at 3,745 events/s, against the 35,000 the job can drain.

*The cap.* 100,000 cleared the backlog fastest, in 24.7 s. 250,000 was slower: two large batches meant less overlap, and one of them ran 26 s. Peak memory barely moved (1.30 to 1.39 GiB), so memory does not decide it on this sample.

*The trigger.*
- At 500 events/s a 2 s trigger has the same p95 as 1 s (2.95 against 2.90 s) for 0.86 cores instead of 1.34. A warehouse micro-batch takes 0.75–0.9 s even with a few hundred rows in it (the lake query runs beside it), so at low rates a shorter trigger buys CPU use, not latency.
- At 5,000 events/s 1 s still wins (p95 5.4 s against 9.4 s), but 2 s keeps more CPU for Airflow and Trino on a 4-core machine.

*The new defaults.* A 2 s trigger and a cap of 100,000. The ceiling is now 50,000 events/s, above everything the job sustains, and a catch-up batch is bounded at about 14 s. The lake's per-trigger cap went from 200,000 to 2,000,000: at one trigger a minute, 200,000 was a ceiling of 3,333 events/s, and 2,000,000 is 33,000, above the warehouse's sustained rate. The fault runs above ran on the new defaults.


### Freshness during the fault runs

Read from `ops.quality_runs` for the fault phase (30 min). Since M5 each freshness check records the lag it measured: `now() − max(ingested_at)` of the relation, in `details.lag_seconds`. dbt's freshness test alone returns rows only when it fails.

<!-- bench:freshness -->
| | |
|---|---|
| `warehouse_refresh` runs with every check passing | 4 of 4 |
| Freshness-only polls (no new rows) | 0 |
| Freshness lag of `marts.fct_edits` at check time, p50 / p95 / max | 380.2 / 441.3 / 449.3 s (4 checks) |
| Freshness lag of `raw.recentchange` at check time, p50 / p95 / max | 29.8 / 61.0 / 65.1 s (4 checks) |
| Time between rebuilds of `marts.fct_edits`, p50 / p95 / max | 413 / 420 / 421 s (4 rebuilds) |
| Check rows recorded | 196 (pass 196) |
<!-- /bench:freshness -->

Every `warehouse_refresh` during the fault runs passed every check: 4 of 4 runs, 196 check rows. Those were 18 kills of the job and the broker, with the lake reconciliation and the volume test included in the checks.

*Mart freshness is the refresh, not the stream.* The landing table was 30–65 s behind its newest arrival when checked, which is the stream plus the fault outages. `marts.fct_edits` was 6–7.5 minutes behind (p50 380 s, max 449 s). On this loaded 4-vCPU machine one refresh takes about 7 minutes, and a new one starts as soon as the five-minute poll finds rows, so the marts trail the stream by about one refresh.

Freshness of what the agent reads is therefore bounded by dbt's run time, not by Kafka or Spark. That is the number to look at before tightening the 60-minute freshness threshold. Faster refreshes would need incremental models for the other marts, or less contention on the box.


## To measure

| What | Command | Milestone | Cost |
|---|---|---|---|
| Postgres vs Trino over Iceberg latency, and pruning at 1M and 20M rows (partitioned vs unpartitioned vs uncompacted) | `make bench-query` | M6 | $0 |
| The 40 warehouse questions on Claude, recorded so the fake provider replays them (M3 is done on the `qwen2.5-coder:7b` recordings; this adds the Claude column) | `ANTHROPIC_API_KEY=... TRIBUTARY_RECORDINGS_PATH=tests/recordings/warehouse.jsonl make record PROVIDER=anthropic` (default model `claude-opus-5-5`) | M3 | est. under $1 on Opus 5.5 (about 46 calls with a cached ~3k-token prefix), a few cents on `TRIBUTARY_MODEL=claude-haiku-5-5` |
| BIRD dev50 on Claude Haiku 5.5, both variants and tracks, recorded so CI replays it | `docker compose up -d postgres && make bird && ANTHROPIC_API_KEY=... make eval PROVIDER=anthropic MODEL=claude-haiku-5-5 RECORD=1` | M4 | see [Cost](#cost): estimated from this run's measured tokens |
| BIRD, all 500 questions on Claude Haiku 5.5 | `... make eval PROVIDER=anthropic MODEL=claude-haiku-5-5 SUBSET=all RECORD=1` | M7 | see [Cost](#cost) |
| BIRD dev50 on Claude Opus 5.5 | `... make eval PROVIDER=anthropic MODEL=claude-opus-5-5 RECORD=1` | M7 | see [Cost](#cost) |
| BIRD, all 500 questions on the local model (about 13 h of CPU at this run's pace) | `make bird && make eval PROVIDER=ollama MODEL=qwen2.5-coder:7b SUBSET=all RECORD=1` | M7 | $0 |
