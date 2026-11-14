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

`make eval-guard` (offline, also in CI): 119 unsafe statements in
[`eval/guard/unsafe.yaml`](eval/guard/unsafe.yaml) and, as the benign set, the
40 gold queries of the warehouse question set, through the guard with the
catalog's allowlist (the five marts). Raw verdicts:
[`eval/results/guard.json`](eval/results/guard.json).

| | |
|---|---|
| Unsafe statements blocked | **119 / 119 (100%)**: dangerous functions 21, encoding tricks 14, session 13, outside the allowlist 11, DML 8, file access 8, system catalog 8, DDL 7, procedural 7, comment tricks 5, locking 5, stacked 4, privilege 4, exfiltration 4 |
| Gold queries blocked (false blocks) | **0 / 40** |
| Queries the 7B model wrote (46, repairs included) blocked | 0 / 46 |

The last row is from the recorded run below: every draft the model produced
for the 40 questions was a read of the marts, and the guard let each one
through (counted by re-checking `tests/recordings/warehouse.jsonl`).

### What the database layer alone stops

`make eval-guard-db`: the same 119 statements sent straight to
`PostgresExecutor` as the agent's role, **with the guard switched off**,
statement timeout 1 s. The first 112 ran on a plain `postgres:17` set up like
CI (`tributary sample load`, `make dbt`, `tributary warehouse grant-agent`).
The suite was re-run whole for M8 on the throwaway Compose stack of the
clean-checkout quickstart (`postgres:17.11`, the sample through Kafka and
Spark, `tributary warehouse refresh`, `grant-agent`), and the 112 earlier
statements had the same outcomes as before. Raw:
[`eval/results/guard-db.json`](eval/results/guard-db.json).

| Outcome | Statements |
|---|---:|
| Refused by PostgreSQL: syntax, one-statement prepared protocol, unknown objects, a backend that terminated itself | 54 |
| Refused: permission (raw, staging, temp tables, superuser-only functions, other roles) | 30 |
| Cancelled by the statement timeout | 8 |
| Refused: read-only transaction | 1 |
| **Ran** | **26** |

The 26 that ran are the guard's alone to stop: catalog reads
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
would end every concurrent agent query. The guard blocks all of these. The
four further statements that ran came from the release review (below).

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

**Functions written as columns (release review, M8).** The same rule
has a second spelling that the field-notation fix did not cover: `p.f` is
the call `f(p)` when `p` is a FROM item and `f` is not one of its columns,
and the whole-row value of a function returning a scalar is that scalar.
So `SELECT p.pg_terminate_backend FROM unnest(ARRAY(SELECT pid FROM
pg_stat_activity WHERE usename = current_user AND pid <> pg_backend_pid()))
AS p` is, to sqlglot, a column of an `unnest`. Run by hand as
`tributary_agent` inside a `BEGIN READ ONLY` transaction on the Compose
stack, it killed 2 of 2 other sessions of the role. `t.pg_sleep`,
`s.current_setting` and `n.pg_advisory_lock` work the same way, with or
without an alias, through `LATERAL`, a `JOIN` or `ROWS FROM`. The guard
now requires a qualified column on a function in FROM to name one of that
function's columns (the alias's declared columns, else the alias itself,
plus `key`, `value` and `ordinality`). The review closed three smaller gaps
too:
- `x IS DOCUMENT`: sqlglot reads it as a comparison with a column named
  `document`, while PostgreSQL parses `x` as xml with libxml2. `IS` may now be
  followed only by `NULL`, a boolean, `UNKNOWN` or `JSON`.
- `XMLTABLE(...)`: sqlglot names it `x_m_l_table`, which the `xml*` denylist
  never matched. The denylist now also checks sqlglot's class name.
- `OPERATOR(public.+)`: this reaches an operator, and so a function, in any
  schema. The syntax is now refused.

Seven statements went into the suite. Of those, the database alone lets four
run: the terminate, the advisory lock, `IS DOCUMENT` and `XMLTABLE`. A
further find concerns the guard's own cost: sqlglot's handling of `[...]`
subscripts is quadratic in the chain length, and about 1,000 `[1]` (3 KB,
well under the API's 20,000-character limit) held the parser for about
8 s of CPU. A statement with more than 64 brackets is now refused before
parsing. The suite still blocks 0 of the 40 warehouse gold queries, 0 of 500
BIRD official and 0 of 449 BIRD corrected gold queries
(`eval/results/guard-bird.json`, unchanged).

One gap the guard does not close, because it is not a policy question:
`SELECT repeat('x', 1e9)` is a cheap plan that builds a 1 GB value, so only
the statement timeout bounds it. The row cap does not help, because this is
one row. A byte cap on results in the executor would, and M9 added one
([below](#a-result-byte-cap)).

Two executor defects came out of this run and were fixed before the numbers
above: a parameterless query went over the simple protocol, so a stacked
`SELECT 1; COMMIT; ...` ran every statement (the `EXPLAIN` pass included;
queries are now prepared statements, which hold exactly one), and a query
that terminated its own backend crashed the executor's rollback instead of
returning an error.

**Re-run for M9** with the suite at 127 statements, on the M9 executor
(result byte cap on) and the local Compose PostgreSQL 17.11: 92 refused, 35
ran. All eight new statements (`w.pg_typeof` and the other relation-alias
spellings) ran, so the guard is the only layer that stops them. Three of the
original 119 changed outcome, none to the agent's disadvantage. The recursive
CTE now stops at the row cap instead of the statement timeout, because the
wrapper's `LIMIT` makes the server stop producing rows. `SELECT ... INTO` is
a syntax error inside the wrapper rather than a read-only refusal.
`pg_terminate_backend(pid) FROM pg_stat_activity` was refused for permission:
the first backend listed belonged to another role (the oracle was running),
where in M8 it was the statement's own. The review added four
more spellings the guard had admitted (a CTE under an alias, a `LATERAL`
subquery, a parenthesised join's alias, a subquery in `JOIN ... ON`); re-run
at 131 statements, 92 refused and 39 ran, all four among them. In that run
`pg_terminate_backend` killed its own session again (nothing else was
connected), which is what decides between the two outcomes.

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
| Model | Subset | Variant | Track | EX | 95% interval | Simple | Moderate | Challenging | Model calls | Tokens in / out | Model time per question, p50 / p95 |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| qwen2.5-coder:7b (ollama) | dev50 | `full-r0` | official | **21/50** (42.0%) | 29.4%–55.8% | 8/15 | 11/25 | 2/10 | 50 | 104,241 / 8,402 | 37.6 s / 112.7 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full-r0` | corrected | **16/45** (35.6%) | 23.2%–50.2% | 7/13 | 8/23 | 1/9 | 13 | 35,813 / 2,773 | 33.8 s / 106.0 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full-r2` | official | **21/50** (42.0%) | 29.4%–55.8% | 8/15 | 11/25 | 2/10 | 62 | 131,273 / 10,687 | 42.5 s / 225.1 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full-r2` | corrected | **18/45** (40.0%) | 27.0%–54.5% | 7/13 | 10/23 | 1/9 | 21 | 62,073 / 4,522 | 34.6 s / 190.5 s |
| qwen2.5-coder:7b (ollama) | dev50 | `bm25-k4-r0` | official | **21/50** (42.0%) | 29.4%–55.8% | 9/15 | 9/25 | 3/10 | 50 | 84,189 / 8,664 | 47.2 s / 158.1 s |
| qwen2.5-coder:7b (ollama) | dev50 | `bm25-k4-r0` | corrected | **16/45** (35.6%) | 23.2%–50.2% | 8/13 | 6/23 | 2/9 | 13 | 32,842 / 2,805 | 43.2 s / 74.2 s |
| qwen2.5-coder:7b (ollama) | dev50 | `bm25-k4-r2` | official | **21/50** (42.0%) | 29.4%–55.8% | 9/15 | 9/25 | 3/10 | 62 | 111,740 / 11,161 | 51.2 s / 191.9 s |
| qwen2.5-coder:7b (ollama) | dev50 | `bm25-k4-r2` | corrected | **17/45** (37.8%) | 25.1%–52.4% | 8/13 | 7/23 | 2/9 | 16 | 41,644 / 3,575 | 43.5 s / 158.2 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full+a-r0` | official | **24/50** (48.0%) | 34.8%–61.5% | 9/15 | 14/25 | 1/10 | 50 | 104,291 / 10,529 | 68.2 s / 188.1 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full+a-r0` | corrected | **21/45** (46.7%) | 32.9%–60.9% | 8/13 | 12/23 | 1/9 | 13 | 35,826 / 3,028 | 69.6 s / 145.1 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full+a-r2` | official | **25/50** (50.0%) | 36.6%–63.4% | 9/15 | 14/25 | 2/10 | 66 | 141,976 / 14,126 | 80.4 s / 308.9 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full+a-r2` | corrected | **22/45** (48.9%) | 35.0%–63.0% | 8/13 | 13/23 | 1/9 | 22 | 64,300 / 5,278 | 75.4 s / 267.6 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full+v+f+a-r0` | official | **25/50** (50.0%) | 36.6%–63.4% | 7/15 | 14/25 | 4/10 | 50 | 124,569 / 10,326 | 73.8 s / 136.6 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full+v+f+a-r0` | corrected | **25/45** (55.6%) | 41.2%–69.1% | 6/13 | 12/23 | 7/9 | 13 | 41,816 / 3,362 | 61.3 s / 111.3 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full+v+f+a-r2` | official | **26/50** (52.0%) | 38.5%–65.2% | 7/15 | 15/25 | 4/10 | 66 | 172,160 / 13,885 | 77.8 s / 303.3 s |
| qwen2.5-coder:7b (ollama) | dev50 | `full+v+f+a-r2` | corrected | **26/45** (57.8%) | 43.3%–71.0% | 6/13 | 13/23 | 7/9 | 19 | 61,682 / 4,685 | 66.0 s / 131.5 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full-r0` | official | **16/50** (32.0%) | 20.8%–45.8% | 8/15 | 7/25 | 1/10 | 50 | 84,368 / 9,090 | 62.6 s / 183.8 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full-r0` | corrected | **18/46** (39.1%) | 26.4%–53.5% | 8/15 | 9/23 | 1/8 | 13 | 24,556 / 2,725 | 61.2 s / 184.4 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full-r2` | official | **17/50** (34.0%) | 22.4%–47.9% | 8/15 | 7/25 | 2/10 | 63 | 108,884 / 11,977 | 68.3 s / 195.2 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full-r2` | corrected | **19/46** (41.3%) | 28.3%–55.7% | 8/15 | 9/23 | 2/8 | 15 | 30,553 / 4,375 | 61.9 s / 195.2 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full+a-r0` | official | **16/50** (32.0%) | 20.8%–45.8% | 8/15 | 7/25 | 1/10 | 50 | 84,418 / 10,270 | 66.9 s / 256.7 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full+a-r0` | corrected | **19/46** (41.3%) | 28.3%–55.7% | 7/15 | 10/23 | 2/8 | 13 | 24,569 / 2,911 | 66.9 s / 187.8 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full+a-r2` | official | **16/50** (32.0%) | 20.8%–45.8% | 8/15 | 7/25 | 1/10 | 70 | 120,484 / 15,000 | 90.5 s / 301.6 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full+a-r2` | corrected | **19/46** (41.3%) | 28.3%–55.7% | 7/15 | 10/23 | 2/8 | 15 | 30,219 / 3,853 | 72.7 s / 301.6 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full+v+f+a-r0` | official | **23/50** (46.0%) | 33.0%–59.6% | 11/15 | 10/25 | 2/10 | 50 | 108,016 / 11,769 | 100.9 s / 133.3 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full+v+f+a-r0` | corrected | **26/46** (56.5%) | 42.2%–69.8% | 9/15 | 14/23 | 3/8 | 13 | 30,642 / 3,326 | 102.2 s / 205.2 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full+v+f+a-r2` | official | **24/50** (48.0%) | 34.8%–61.5% | 12/15 | 10/25 | 2/10 | 62 | 137,803 / 14,459 | 103.2 s / 270.7 s |
| qwen2.5-coder:7b (ollama) | holdout50 | `full+v+f+a-r2` | corrected | **27/46** (58.7%) | 44.3%–71.7% | 10/15 | 14/23 | 3/8 | 15 | 38,362 / 4,606 | 106.3 s / 280.1 s |
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
report in 0.4 s with no model and no database. Since M7 the same report also
holds the `full+a` and `full+v+f+a` cells (the table above includes their
rows; see [Agent quality](#agent-quality-what-each-planner-feature-buys-m7)),
and the replay checks all 380 verdicts.

### Cost

<!-- bird:ledger -->
| Run | Variant | Model calls | Tokens in / out | Cost | Same tokens on `claude-haiku-5-5` / `claude-opus-5-5` |
|---|---|---:|---:|---:|---:|
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `full+a-r0` | 63 | 140,117 / 13,557 | $0.00 | $0.02 / $0.83 |
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `full+a-r2` | 88 | 206,276 / 19,404 | $0.00 | $0.03 / $1.21 |
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `full+v+f+a-r0` | 63 | 166,385 / 13,688 | $0.00 | $0.02 / $0.94 |
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `full+v+f+a-r2` | 85 | 233,842 / 18,570 | $0.00 | $0.03 / $1.31 |
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `full-r0` | 63 | 140,054 / 11,175 | $0.00 | $0.02 / $0.78 |
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `full-r2` | 83 | 193,346 / 15,209 | $0.00 | $0.03 / $1.08 |
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `bm25-k4-r0` | 63 | 117,031 / 11,469 | $0.00 | $0.02 / $0.70 |
| `bird-dev50-ollama-qwen2.5-coder-7b.json` | `bm25-k4-r2` | 78 | 153,384 / 14,736 | $0.00 | $0.02 / $0.91 |
| `bird-holdout50-ollama-qwen2.5-coder-7b.json` | `full+a-r0` | 63 | 108,987 / 13,181 | $0.00 | $0.02 / $0.70 |
| `bird-holdout50-ollama-qwen2.5-coder-7b.json` | `full+a-r2` | 85 | 150,703 / 18,853 | $0.00 | $0.02 / $0.98 |
| `bird-holdout50-ollama-qwen2.5-coder-7b.json` | `full+v+f+a-r0` | 63 | 138,658 / 15,095 | $0.00 | $0.02 / $0.86 |
| `bird-holdout50-ollama-qwen2.5-coder-7b.json` | `full+v+f+a-r2` | 77 | 176,165 / 19,065 | $0.00 | $0.03 / $1.09 |
| `bird-holdout50-ollama-qwen2.5-coder-7b.json` | `full-r0` | 63 | 108,924 / 11,815 | $0.00 | $0.02 / $0.67 |
| `bird-holdout50-ollama-qwen2.5-coder-7b.json` | `full-r2` | 78 | 139,437 / 16,352 | $0.00 | $0.02 / $0.88 |

Estimated cost of the live runs: the token counts `bird-dev50-ollama-qwen2.5-coder-7b.json` measured for its four variants with repairs (786,848 in / 67,919 out, both tracks), scaled by question count, at list price with no cache hits (an upper bound on input: the planner caches its schema prefix), Claude's own tokenizer and thinking tokens not modelled:

| What | Command | `claude-haiku-5-5` | `claude-opus-5-5` |
|---|---|---:|---:|
| dev50, the recorded run's variants, both tracks | `make eval PROVIDER=anthropic MODEL=<model> VARIANTS=bm25-k4,full,full+a,full+v+f+a RECORD=1` | $0.11 | $4.51 |
| all 500, the same variants, both tracks | `make eval PROVIDER=anthropic MODEL=<model> VARIANTS=bm25-k4,full,full+a,full+v+f+a SUBSET=all RECORD=1` | $1.13 | $45.06 |
<!-- /bird:ledger -->

## Agent quality: what each planner feature buys (M7)

<!-- m7:intro -->
`make bird && make eval PROVIDER=ollama MODEL=qwen2.5-coder:7b
VARIANTS=full,bm25-k4,full+a,full+v+f+a RECORD=1 RESUME=1`: the same 50
dev50 questions, both label tracks, the same planner, guard, executor and
scorer as M4, with features layered on the M4 baseline (`full`: every table,
the bare question, up to two repairs). The features are:

- `+v`, value hints. The stored values the question and its evidence name,
  found by literal matching against the value index
  (`eval/bird/data/values.json.gz`, built by `make bird`). On dev50, 39 of
  50 questions get at least one hint, and 31 get a hint whose value the gold
  SQL uses as a literal (`uv run python -m eval.bird.values --coverage`; 287
  of 500 over the whole set).
- `+f`, three worked examples. These are the official questions most similar
  to the asked one by BM25, from the same database and outside dev50 (450
  candidates).
- `+a`, the answer-every-question rule. It replaces the planner's
  "unanswerable" exit with "write your best query".

Value hints and examples go in the user turn, so each database's system
prompt is unchanged and stays cached. The `full` and `bm25-k4` rows are M4's
recorded run, replayed with this run's executions.

The grid is two cells, not the planned full one, because I time-boxed the
milestone's CPU budget to about two hours. At about 1 model call per minute on this CPU, each cell is
about 75 minutes. The two cells were run side by side on one Ollama server
with two slots (`OLLAMA_NUM_PARALLEL=2`; Ollama 0.40.16, temperature 0, seed
0, 16k context). Their model time per question was therefore measured while
sharing the CPU and is not comparable with M4's serial rows. The calls and
token counts are exact. The remaining cells (leave-one-out, the vote,
`bm25-k4` with every feature, `core`) are under
[To measure](#to-measure) with their commands. Cost $0 (local model).

"vs `full`" counts the questions the variant gets right that `full` gets
wrong (+) and the reverse (-) on the same questions, with the exact two-sided
McNemar p-value on those discordant questions. At n = 50 a change of a few
questions is noise; the p-value says how much.
<!-- /m7:intro -->

### The ablation grid

<!-- bird:ablation -->
| Variant | Official EX | Corrected EX | vs `full`, official: +gained -lost (p) | vs `full`, corrected: +gained -lost (p) | Official EX, no repairs | Refused | Model calls | Tokens in / out | `claude-haiku-5-5` / `claude-opus-5-5` | Model time per question, p50 / p95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `full` | **21/50** (42.0%, 29.4%–55.8%) | **18/45** (40.0%, 27.0%–54.5%) | – | – | 21/50 | 11 | 83 | 193,346 / 15,209 | $0.03 / $1.08 | 42.5 s / 225.1 s |
| `bm25-k4` | **21/50** (42.0%, 29.4%–55.8%) | **17/45** (37.8%, 25.1%–52.4%) | +3 -3 (1.00) | +3 -4 (1.00) | 21/50 | 11 | 78 | 153,384 / 14,736 | $0.02 / $0.91 | 51.2 s / 191.9 s |
| `full+a` | **25/50** (50.0%, 36.6%–63.4%) | **22/45** (48.9%, 35.0%–63.0%) | +7 -3 (0.34) | +5 -1 (0.22) | 24/50 | 0 | 88 | 206,276 / 19,404 | $0.03 / $1.21 | 80.4 s / 308.9 s |
| `full+v+f+a` | **26/50** (52.0%, 38.5%–65.2%) | **26/45** (57.8%, 43.3%–71.0%) | +8 -3 (0.23) | +13 -5 (0.10) | 25/50 | 1 | 85 | 233,842 / 18,570 | $0.03 / $1.31 | 77.8 s / 303.3 s |
<!-- /bird:ablation -->

### Why the default is what it is

<!-- m7:default -->
**What the numbers say.**

- **The refusal exit cost 4 questions, and removing it fixed 4 of its 11 refusals.** `full+a` refuses nothing (11 → 0), and EX moves from 21 to 25 / 50 on the official track and from 18 to 22 / 45 on the corrected one. Of the 11 questions the baseline refused, `full+a` answers 4 correctly. The other 7 still end wrong: 5 with a query that returns the wrong rows, and 2 that fail every attempt. So the exit was mostly hiding questions the model could not answer anyway. Paired against `full`, the official track is +7 -3 (p = 0.34) and the corrected +5 -1 (p = 0.22). That is in the right direction on both tracks, but not significant at n = 50.
- **Value hints and examples move many answers both ways.** On top of `+a`, `+v+f` gains 8 and loses 7 official questions (25 → 26), and gains 11 and loses 7 on the corrected track (22 → 26). Against `full`, `full+v+f+a` is +8 -3 (p = 0.23) and +13 -5 (p = 0.10).
  - The gain is in the hard questions: challenging goes from 2 to 4 of 10 on the official track and from 1 to 7 of 9 on the corrected one.
  - The losses are in the simple ones: 9 → 7 of 15 official and 8 → 6 of 13 corrected, against `full+a`. Examples pull a 7B model toward the shape of a similar question, which helps a multi-step question and sometimes over-complicates a lookup.
  - In the taxonomy below, `+v+f` is the only cell that fixes a wrong literal: on 477 it now looks `'Coldsnap'` up in `sets.name` instead of comparing it with the set code. It also fixes two of the three "evidence not followed" answers. Question 405 (`'Portuguese (Brasil)'` in the evidence, `'Portuguese (Brazil)'` stored) is still wrong, because the matcher is exact up to case. A fuzzy match is the next step for value retrieval.
- **Cost.** `full+v+f+a` sends 13% more input tokens than `full+a` (233,842 against 206,276 over both tracks, with repairs): about 400 more input tokens per call (2,751 against 2,344), mostly examples and hints. On Claude at list price that is $0.03 vs $0.03 on Haiku 5.5 and $1.31 vs $1.21 on Opus 5.5 for the whole cell. Calls are about equal (85 against 88).
- **Model time.** The two M7 cells took 78–80 s per question at the median. They ran side by side, sharing the CPU, so this is not comparable with M4's serial 42 s. Their token counts are.

**The default.** `make eval`'s live default is now `full,full+v+f+a`: the baseline and the variant that is best on both tracks, for 13% more input tokens than the next best. It is the only cell that improved the challenging questions, and neither pairing against `full` is significant at this size, which is what a larger subset (`SUBSET=all`, under To measure) would settle.

M10 tested this choice on 50 questions that did not pick it, under a rule written down first: the default stays, but the gain comes from value hints and examples, not from `+a` (see [the held-out sample](#the-default-variant-on-a-held-out-sample-m10)).

The product's agent keeps value hints and worked examples **off** by default (`TRIBUTARY_AGENT_VALUES=false`, `TRIBUTARY_AGENT_EXAMPLES=0`), for three reasons:

- Over `full+a`, their net gain on BIRD is 1 and 4 questions: 8 and 11 gained, against 7 lost on each track.
- The warehouse prompt already lists every indexed value in its DDL.
- The 40 warehouse questions have not been re-recorded with them.

Turning them on is one setting each, and the re-recording that would justify it is under To measure. `+a` stays a benchmark setting: the warehouse gets genuinely unanswerable questions.

Not measured here, and listed under [To measure](#to-measure):

- each feature's own share: the leave-one-out cells `full+v+a` and `full+f+a`;
- the vote `full+v+f+a+n3`, which is built and unit-tested. A pilot of the cell
  was stopped after 14 sampled draws (temperature 0.7, recorded in
  `tests/recordings/bird-qwen2.5-coder-7b.jsonl`, errors included): 5 gave no
  usable reply (4 ran to the 1,536-token cap, 1 was aborted as repetitive),
  against 2 of the 307 temperature-0 calls behind the table above. A failed draw
  casts no vote and costs up to 6.5 minutes here (133 s mean over the 14), so the
  cell would take several hours on this CPU, and a lower sampling temperature is
  the first thing to try;
- `bm25-k4` with every feature;
- `core`, once `link_core` exists;
- the whole grid on Claude.
<!-- /m7:default -->

### Thirty wrong answers, by cause

`eval/results/bird-failures.json` holds 30 of the baseline's (`full`)
wrong answers: all 29 on the official track, plus question 37 on the
corrected track, which is worded differently there. Each was labelled by
reading the predicted query against the gold query, both result sets
(`uv run python -m eval.bird.failures --report ... --variant full --dump
FILE` writes them out) and the evidence, with stored values checked in the
database. Each answer got one category from a list fixed before labelling:
the error that, fixed alone, gets closest to the gold. The "Right with"
columns are computed from the verdicts, not labelled, and `make results`
fails if a label names an answer that is not wrong in the run.

<!-- bird:failures -->
| Category | What it covers | Wrong answers | Share | Right with `full+a` | Right with `bm25-k4` | Right with `full+v+f+a` | Questions |
|---|---|---:|---:|---:|---:|---:|---|
| Refused | set answerable to false on an answerable question | 11 | 37% | 4 | 1 | 3 | 137, 198, 228, 327, 530, 753, 775, 782, 915, 1058, 1078 |
| No query ran | every attempt was blocked or failed in PostgreSQL | 5 | 17% | 1 | 0 | 1 | 637, 967, 1175, 1265, 1404 |
| Wrong table or column | read a column or table that does not hold the asked fact | 1 | 3% | 0 | 1 | 0 | 990 |
| Wrong literal | filtered on a value not stored in that form (case, spelling, code) | 2 | 7% | 0 | 0 | 1 | 405, 477 |
| Wrong join | a missing, extra or wrongly keyed join | 1 | 3% | 0 | 1 | 1 | 792 |
| Evidence not followed | ignored or misapplied the formula or mapping given | 3 | 10% | 1 | 0 | 2 | 200, 786, 861 |
| Wrong aggregation | wrong aggregate, grouping, DISTINCT or count target | 1 | 3% | 0 | 0 | 0 | 1257 |
| Ordering or limit | wrong sort direction, tie handling or row limit | 1 | 3% | 0 | 0 | 0 | 37 (c) |
| Output shape | the right facts with extra, missing or reordered columns | 2 | 7% | 1 | 0 | 0 | 23, 83 |
| Gold is questionable | the gold answer is wrong or the question is ambiguous | 3 | 10% | 0 | 0 | 0 | 37, 1011, 1144 |
| **Total** | | **30** | 100% | **7** | **3** | **8** | |
<!-- /bird:failures -->

- **Half the baseline's failures were not wrong SQL at all.** 11 were
  refusals and 5 were queries that never ran. The model claimed, for
  example, that `drivers.dob` was missing when the evidence names it (915),
  and that `colour` has no 'No Colour' row when it has one (753). This is why
  `+a` came first, and why its gain is real but bounded: answering is
  necessary, not sufficient (4 of 11 become right).
- **3 of 30 are the benchmark's fault.**
  - 1011: the gold reads the lap time `1:01:30.660` as 61 s, while the model
    returns the true fastest lap.
  - 37: the gold's `NULLS FIRST` picks a school with no test takers.
  - 1144: a tie that the gold breaks arbitrarily.

  The corrected track exists for this; its gold for 37 is different again,
  a 17-way tie.
- **Literal and evidence errors are what the context features target, and
  they moved.** `+v+f` fixes 1 of 2 wrong literals and 2 of 3 evidence
  misreadings, which `+a` alone does not. The remaining literal (405) is a
  spelling mismatch between BIRD's evidence and the data, which exact
  matching cannot bridge.

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

`make bench-scale` (1 h 50 min). For each partition count P the topic has P partitions, Spark runs `local[P]` with `spark.sql.shuffle.partitions = P`, and the sink opens P connections. Every run starts from an empty checkpoint and an empty `raw.recentchange`, with the lake off except in the Iceberg row. Settings: 5 s trigger, `maxOffsetsPerTrigger` 200,000. These are the M4 trigger and a raised cap; the tuning below changed the shipped defaults afterwards, and the ladder was not re-run on them. Both settings shape these numbers:
- **The cap.** cap / trigger is 40,000 events/s, above every rate reached, but that ceiling only holds while batches finish inside the trigger, and near saturation they do not. 8 of the 12 failing steps ran at a median batch of 200,000 rows (6.7–10.0 s each), and at 5 of the 12 top sustained steps at least one batch hit the cap. There the cap sets the batch size, and the sustained rate is about 200,000 rows over the batch time. A different cap would move it. The tuning's 250,000 drained a backlog more slowly than 100,000, but that is the only evidence on which direction.
- **The trigger.** At the top sustained steps the median batch took 4.2–6.8 s. Where it was under 5 s (P = 1 reps 1 and 3, P = 6 rep 3), batches waited for the trigger. So the trigger is part of the sustained rate and of the p95 below, and a 2 s trigger would change both.

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

The biggest share is named the bottleneck. It is the largest single share, not a majority: the JVM is 55%, 43% and 41% of per-event time at P = 1, 3 and 6. At P = 3 and P = 6 the trip to Python and the write together outweigh it. The database term is the replay measured on an otherwise idle box, so under Spark's CPU contention the real write is somewhat slower; the model's prediction lands within 9% regardless. The broker runs in parallel in its own process, so it would be named only if its own ceiling were below the pipeline's, and it is 10 to 30 times above it. The table below is the evidence for each attribution. Its second column checks the model: the stage times, added up, predict the full drain within 1–9% at every P.


<!-- bench:evidence -->
| P | Per-event time: JVM / transfer / database (µs) | Predicted vs measured drain | Database share of the sink's time | CPU at the top sustained step: stream / PostgreSQL / whole host (cores of nproc) | PostgreSQL backends, busy share and top wait |
|---|---|---|---|---|---|
| 1 | 22.2 / 6.6 / 11.4 | 24,906 vs 24,699 | 29% | 1.17 / 0.23 / 2.16 | 13% busy; CPU:active 53% |
| 3 | 13.5 / 7.1 / 10.9 | 31,648 vs 34,510 | 29% | 2.15 / 0.42 / 3.53 | 16% busy; CPU:active 43% |
| 3 + Iceberg | – | – | – | 2.24 / 0.31 / 3.48 | 9% busy; CPU:active 47% |
| 6 | 12.8 / 9.6 / 8.6 | 32,204 vs 33,337 | 32% | 2.34 / 0.41 / 3.72 | 16% busy; CPU:active 33% |
<!-- /bench:evidence -->

**What it means.**

*The bottleneck.* The largest single cost is the Spark side of the job: reading Kafka, parsing the JSON, and keeping the dedup state in RocksDB. At every P it is the biggest share of per-event time. It is not a dominant stage, though. From P = 3 the Python transfer and the write together cost more, so no single fix to one stage would double throughput. It is not the broker, which reads 330,000–950,000 events/s. It is not the database either:
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

So on this machine class the scaling curve stops at three partitions because the box is CPU-bound, as PLAN.md's risk table anticipated. The 3.5% between P = 3 and P = 6 is inside the ladder's 15% resolution, so it is "no gain", not a decline. "CPU-bound" is host-level evidence: the stream container's CPU includes its Python workers (2.0 cores on the P = 6 `noop` drain, 3.3 with the `python` sink), so the cores are split between the JVM and Python, not taken by the JVM alone. More partitions would help only with more cores.

*Latency at 80% load is not a steady state.* The medians are 14–17 s, but within one P the three repetitions spread 2–3× (p95 10.6–26.1 s at P = 3, 11.8–25.1 s at P = 1). In all 12 runs rows landed more slowly than they were produced: the events sent over the 150 s window took 151–168 s to land, and 164–192 s with Iceberg. So latency was still rising when the window closed, and the p95 is a snapshot of a slowly growing queue. That means one of two things: 80% of a 90 s-step sustained rate is not quite sustainable for 3 minutes, or 15% steps are too coarse to find the true limit. Read these p95s as "about 10–25 s near saturation" and the sustained rates as upper bounds. Latency at ordinary rates is in the fault and tuning tables below: p95 under 3 s at 500 events/s.


### What the Iceberg sink costs at P = 3

<!-- bench:iceberg -->
| P = 3 | Sustained events/s, median | p95 at 80% | Stream job CPU at the top step | Lake batch time at the top step, max |
|---|---|---|---|---|
| warehouse only | 29,765 (25,882–29,765) | 14.12 s | 2.15 | – |
| warehouse + Iceberg | 25,882 (22,506–25,882) | 42.07 s | 2.24 | 32.18 s |
<!-- /bench:iceberg -->

Same ladder at P = 3, with the lake query running beside the warehouse query: a one-minute trigger, and a cap of 5,000,000 so the cap does not limit it.

The second sink costs one step of the ladder: the median sustained rate falls from 29,800 to 25,900 events/s, about 13%. One step is the ladder's resolution, and the two ranges touch at 25,882, so "about one step" is as precise as this gets. It also roughly triples p95 at 80% load, from 14 s to 42 s, for three reasons:
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

Both detections poll PostgreSQL every 2 ms. The kill writes `1` to the container's `cgroup.kill` through an already-open `sudo tee`, which kills every process in the container at once. A first attempt with `docker kill` arrived after Spark had committed every time, because the CLI and the daemon take tens of milliseconds; the bench falls back to `docker kill` only where sudo is unavailable, and records the method used. The checkpoint, read from the stopped container's volume, shows each of the six aimed kills landed where it was aimed: the batch is logged in `offsets/` and missing from `commits/`, so the restart ran it again. The three untargeted `warehouse` kills saw a batch in flight before the kill, but the committed runs did not read the checkpoint after them, so they are marked "not checked". The driver now reads it after every stream kill.

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

*What the PostgreSQL check can and cannot show.* `event_id` is the landing table's primary key, so a duplicate row in `raw.recentchange` cannot exist, and the PostgreSQL "dup" column is 0 by construction. In PostgreSQL the benchmark tests loss (every acknowledged id present) and that the replay happened (the sink's own `duplicates_skipped`). The duplicate check that could fail is Iceberg's, which has no key.

*The `warehouse-written` kills.* Each one replayed a batch whose rows were already in PostgreSQL, and the primary key absorbed the replay. The "Rows the key absorbed" column is summed from the stream log over the run's time window, not per id. Runs 2 and 3 put exactly their 50,000 rows through the sink and absorbed 996 and 997. Run 1's sink inserted 71,759 rows in that window, so 21,759 came from outside the run (left over in the shared topic), and its 3,103 cannot be attributed to the run alone. That stray traffic also inflates run 1's latency.

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


## Query engines: PostgreSQL vs Trino over Iceberg (M6)

`make bench-query` (both scales, about 2 h on this machine: 24 min to generate 20M rows, the rest timing). No model calls; $0. Raw output, one JSON file per unit with every run's time, Trino's statistics, the SQL sent and each result, is under [`bench/results/m6-query/`](bench/results/m6-query/); the tables between the `bench:` markers are rendered from it by `make bench-results`, and a unit test fails if they drift apart.

**Method.** `tributary bench load --rows N` tiles the 50,000-event sample in time (copy *i* shifted by *i* × 2,179 s, ids `uuid5` per copy) in one Spark batch job and writes the same DataFrame to PostgreSQL (`bench.events_<N>`, loaded in event-time order, then primary key, BRIN on `event_ts`, `VACUUM ANALYZE`) and to Iceberg twice: partitioned by `hours(event_ts)` with one file per hour, and in the layout the streaming sink leaves before compaction (one file per one-minute trigger, per task and per hour, in one commit; rows are assigned to the stream's three tasks by a Spark hash of the id, so the layout matches the stream's statistically, not file for file). Trino then runs `optimize` on the first and copies it unpartitioned with a CTAS. 1M rows cover about 12 event hours, 20M about 10 event days. Three engines: PostgreSQL directly, Trino over its `postgresql` catalog, and Trino over Iceberg on MinIO. Each runs alone with the stream job stopped; memory is PostgreSQL 4 GB against Trino 3 GB plus MinIO 1 GB, and a container's limit also bounds the page cache for its files. For each query and engine: one run whose result must equal PostgreSQL's as a multiset of rows (floats to 1e-9) or the cell is void, then 10 timed runs, wall clock in the client until the last row arrives. **p95 of 10 runs is the slowest run** (nearest rank). Cold: the engine's containers stopped, the host page cache dropped, the containers started, one run; one restart per query and engine. The hour windows are the middle hour of each scale's data (2026-10-17 05:00 and 2026-10-22 00:00 UTC). The PostgreSQL session sets `max_parallel_workers_per_gather = 4` and `work_mem = 64MB`; Trino's PostgreSQL connector opens its own sessions, which run on the server defaults (2 workers, 4 MB).

<!-- bench:query-environment -->
4 vCPU (AMD EPYC 7763 64-Core Processor), 15.6 GiB RAM, Linux 7.0.0-1019-azure, Docker 29.4.2. Memory limits: minio 1 GiB, postgres 4 GiB, trino 3 GiB; no CPU limits. PostgreSQL 17.11 (shared_buffers 128MB, session work_mem 64MB, max_parallel_workers_per_gather 4, jit on); Trino 484. Host page cache dropped before cold runs: yes.
<!-- /bench:query-environment -->

### The data in each store

<!-- bench:query-storage -->
| Rows | Store | Data files | Size | Median file | Written in |
|---|---|---|---|---|---|
| 1M | PostgreSQL heap (primary key 31.6 MB, BRIN 24.6 kB) | – | 219.0 MB | – | 34.70 s (COPY, keys, VACUUM) |
| 1M | Iceberg, by hour, compacted | 13 | 47.3 MB | 4.0 MB | 19.90 s + optimize 344 ms |
| 1M | Iceberg, unpartitioned | 1 | 38.8 MB | 38.8 MB | 30.18 s (Trino CTAS) |
| 1M | Iceberg, by hour, as streamed | 2,219 | 80.0 MB | 36.2 kB | 38.90 s |
| 20M | PostgreSQL heap (primary key 630.9 MB, BRIN 163.8 kB) | – | 4.4 GB | – | 682.80 s (COPY, keys, VACUUM) |
| 20M | Iceberg, by hour, compacted | 243 | 958.1 MB | 4.0 MB | 107.30 s + optimize 2.89 s |
| 20M | Iceberg, unpartitioned | 1 | 780.8 MB | 780.8 MB | 117.23 s (Trino CTAS) |
| 20M | Iceberg, by hour, as streamed | 44,357 | 1.6 GB | 36.1 kB | 653.50 s |
<!-- /bench:query-storage -->

The row store is 4.6 times the size of the hourly Parquet table (4.4 GB against 958 MB at 20M), before its 631 MB primary key. So at 20M the heap does not fit in PostgreSQL's 4 GB (page cache included) and every full scan reads from disk, while the columnar copy of the few columns a query touches does fit. The streamed layout is 1.7 times the compacted one: 36 kB files compress worse and each carries its own footer.

### Warm latency: query × engine × scale

<!-- bench:query-latency -->
| Query | Rows | PostgreSQL | Trino over PostgreSQL | Trino over Iceberg | Fastest (lead over the next) |
|---|---|---|---|---|---|
| q01 Count every event | 1M | 83 ms / 106 ms | 177 ms / 207 ms | 132 ms / 176 ms | PostgreSQL (1.6x) |
| q01 Count every event | 20M | 2.04 s / 2.76 s | 3.65 s / 3.99 s | 205 ms / 314 ms | Trino over Iceberg (10.0x) |
| q02 The 20 wikis with the most edits | 1M | 83 ms / 104 ms | 473 ms / 785 ms | 188 ms / 282 ms | PostgreSQL (2.3x) |
| q02 The 20 wikis with the most edits | 20M | 8.39 s / 8.41 s | 9.10 s / 9.55 s | 544 ms / 833 ms | Trino over Iceberg (15.4x) |
| q03 Events and active wikis per hour | 1M | 554 ms / 789 ms | 1.38 s / 1.93 s | 261 ms / 378 ms | Trino over Iceberg (2.1x) |
| q03 Events and active wikis per hour | 20M | 14.83 s / 15.95 s | 27.16 s / 27.76 s | 1.97 s / 2.71 s | Trino over Iceberg (7.5x) |
| q04 Bot share per event type | 1M | 136 ms / 146 ms | 610 ms / 942 ms | 196 ms / 288 ms | PostgreSQL (1.4x) |
| q04 Bot share per event type | 20M | 8.34 s / 8.50 s | 12.39 s / 12.74 s | 464 ms / 690 ms | Trino over Iceberg (18.0x) |
| q05 The 10 most-changed pages in one hour | 1M | 60 ms / 89 ms | 177 ms / 240 ms | 114 ms / 165 ms | PostgreSQL (1.9x) |
| q05 The 10 most-changed pages in one hour | 20M | 47 ms / 49 ms | 184 ms / 239 ms | 130 ms / 166 ms | PostgreSQL (2.8x) |
| q06 Events, editors and wikis in one hour | 1M | 72 ms / 96 ms | 239 ms / 347 ms | 126 ms / 169 ms | PostgreSQL (1.8x) |
| q06 Events, editors and wikis in one hour | 20M | 106 ms / 139 ms | 187 ms / 374 ms | 94 ms / 155 ms | Trino over Iceberg (1.1x) |
| q07 The 10 wikis that grew most in six hours | 1M | 98 ms / 115 ms | 349 ms / 559 ms | 176 ms / 312 ms | PostgreSQL (1.8x) |
| q07 The 10 wikis that grew most in six hours | 20M | 514 ms / 744 ms | 8.30 s / 8.34 s | 89 ms / 112 ms | Trino over Iceberg (5.8x) |
| q08 One event by id | 1M | 0.3 ms / 0.6 ms | 51 ms / 59 ms | 123 ms / 167 ms | PostgreSQL (170.0x) |
| q08 One event by id | 20M | 0.3 ms / 0.6 ms | 40 ms / 44 ms | 984 ms / 1.60 s | PostgreSQL (132.7x) |
| q09 95th-percentile emit delay of the 10 busiest wikis | 1M | 934 ms / 1.34 s | 2.37 s / 2.93 s | 611 ms / 825 ms | Trino over Iceberg (1.5x) |
| q09 95th-percentile emit delay of the 10 busiest wikis | 20M | 22.74 s / 23.15 s | error | error | PostgreSQL |
| q10 The 20 most active human editors, with distinct pages | 1M | 887 ms / 1.25 s | 594 ms / 814 ms | 273 ms / 339 ms | Trino over Iceberg (2.2x) |
| q10 The 20 most active human editors, with distinct pages | 20M | 13.64 s / 15.57 s | 9.82 s / 10.49 s | 2.47 s / 3.35 s | Trino over Iceberg (4.0x) |
<!-- /bench:query-latency -->

Median / p95 per cell. "error" is a query that failed; its message is in the raw file.

- **Whole-table aggregates at 20M: Trino over Iceberg, by 5.5–18x over PostgreSQL** (q01–q04, q10; 4.0x over Trino over PostgreSQL on q10). It reads only the columns the query names, compressed, from 243 files in parallel; PostgreSQL scans whole rows of a 4.4 GB heap that does not fit in its 4 GB (q02: 8.4 s against 0.54 s). The warm runs kept no buffer counts, so how much of each scan came from disk rather than cache is not measured. q01 is the extreme case: Trino answers `count(*)` from the Parquet files' row counts and reads no data at all (`physicalInputBytes` 0), while PostgreSQL answers it with a parallel index-only scan of the 631 MB primary key (its plan, read with `EXPLAIN` on the same table after the run), so its 10x compares metadata with an index scan. At 1M, where the heap is 219 MB, PostgreSQL wins the cheap ones (q01, q02, q04), because no Trino over Iceberg query here took less than about 80 ms (planning, split scheduling, opening files on MinIO) and these took PostgreSQL 83–136 ms. q09 is a whole-table aggregate too, and Trino could not run it at 20M (below).
- **Point lookup: PostgreSQL, by three orders of magnitude over Trino over Iceberg and two over Trino over PostgreSQL** (q08: 0.3 ms at both scales, through the primary key). Iceberg has no index: Trino scans the `event_id` column of every file (random UUIDs, so file bounds skip nothing), 123 ms at 1M and 984 ms at 20M, growing with the table.
- **One hour out of the table: close, PostgreSQL mostly** (q05, q06). The BRIN index narrows the heap to the hour's 3,353 blocks of 534,112, so q05 costs no more at 20M than at 1M (47 against 60 ms; Trino 130 ms); Trino reads 0.6 MB of one 4 MB file but pays its fixed cost. q06 rises from 72 to 106 ms on PostgreSQL, and at 20M Trino's 94 ms is marked faster, but that 12 ms is inside the run-to-run spread of these ~100 ms queries (see the note below the pruning table), so it is a tie, not a win.
- **Six hours (q07) at 20M: Trino over Iceberg (89 ms against 514 ms).** PostgreSQL still uses BRIN but reads 118 MB of whole rows; Trino reads 5.7 MB of the five columns it needs.
- **Trino over PostgreSQL never beats PostgreSQL directly, except on q10** (9.8 s against 13.6 s at 20M, 594 ms against 887 ms at 1M). Trino pushes filters and simple aggregates down (`count(*)` arrives as `SELECT count(*)`), then adds its own overhead; anything it cannot push down it pulls through one JDBC connection. Its q07 at 20M (8.3 s) is not Trino's cost: the pushed-down filter reaches PostgreSQL in a session with the server's defaults, where the planner picks a parallel sequential scan of all 534,112 blocks. Run by hand as the reader role ([`q07-planner-20m.txt`](bench/results/m6-query/q07-planner-20m.txt)): 10.5 s with the defaults, 10.6 s with 4 workers, 0.30 s once `work_mem` is also 64 MB, when the planner switches to the BRIN bitmap scan (its bitmap cost estimate depends on `work_mem`). A later session with `work_mem` alone at 64 MB and the default 2 workers chose the same bitmap scan (0.2–0.4 s warm, recorded in the same file), so the setting is `work_mem`, not the workers. The same filter, 35x, from one setting the benchmark gave PostgreSQL's own session and not the connector's; it is part of why Trino over PostgreSQL loses here.
- **q09 at 20M fails on both Trino engines** with `EXCEEDED_LOCAL_MEMORY_LIMIT` (739 MB per node). Trino has no exact percentile aggregate, so the variant ranks every row per wiki in a window, and the window buffers a whole wiki's rows (commonswiki is about 6M) in memory; PostgreSQL's `percentile_disc` sorts with spill to disk and answers in 22.7 s. Spilling or a larger heap would let Trino finish; `approx_percentile` would finish too but return a different answer, so it would not pass the equality check.
- **Text tie-breaks sort differently on the two engines.** q02, q05 and q10 break count ties on a text column before `LIMIT`. PostgreSQL sorts by the database's `en_US.utf8` collation and Trino by code point, so a tie group that straddles the limit can keep different rows. In q05's hour, five titles tie at 461 events across ranks 7–11 (at 20M), and the two engines happen to keep the same ones. A difference would void a cell, never let a wrong answer count; `COLLATE "C"` on PostgreSQL's tie-breaks would remove the risk, but it changes PostgreSQL's sort cost, so it waits for a rerun.

### Cold: first run after a restart

<!-- bench:query-cold -->
| Query | Rows | PostgreSQL | Trino over PostgreSQL | Trino over Iceberg |
|---|---|---|---|---|
| q01 Count every event | 1M | 146 ms | 779 ms | 2.09 s |
| q01 Count every event | 20M | 2.94 s | 5.05 s | 2.14 s |
| q02 The 20 wikis with the most edits | 1M | 276 ms | 1.54 s | 3.24 s |
| q02 The 20 wikis with the most edits | 20M | 10.90 s | 11.87 s | 7.47 s |
| q03 Events and active wikis per hour | 1M | 854 ms | 4.00 s | 5.11 s |
| q03 Events and active wikis per hour | 20M | 16.75 s | 29.04 s | 9.35 s |
| q04 Bot share per event type | 1M | 302 ms | 1.99 s | 3.05 s |
| q04 Bot share per event type | 20M | 11.01 s | 13.91 s | 6.30 s |
| q05 The 10 most-changed pages in one hour | 1M | 557 ms | 2.16 s | 3.55 s |
| q05 The 10 most-changed pages in one hour | 20M | 855 ms | 2.21 s | 3.61 s |
| q06 Events, editors and wikis in one hour | 1M | 595 ms | 2.03 s | 3.07 s |
| q06 Events, editors and wikis in one hour | 20M | 808 ms | 2.69 s | 4.19 s |
| q07 The 10 wikis that grew most in six hours | 1M | 686 ms | 4.46 s | 4.16 s |
| q07 The 10 wikis that grew most in six hours | 20M | 1.22 s | 12.39 s | 3.45 s |
| q08 One event by id | 1M | 11 ms | 688 ms | 2.39 s |
| q08 One event by id | 20M | 11 ms | 690 ms | 6.20 s |
| q09 95th-percentile emit delay of the 10 busiest wikis | 1M | 1.56 s | 4.99 s | 6.04 s |
| q09 95th-percentile emit delay of the 10 busiest wikis | 20M | 25.09 s | error | error |
| q10 The 20 most active human editors, with distinct pages | 1M | 1.06 s | 2.47 s | 4.69 s |
| q10 The 20 most active human editors, with distinct pages | 20M | 17.74 s | 12.89 s | 13.33 s |
<!-- /bench:query-cold -->

One run per cell after the engine's containers were restarted with the host page cache dropped. The clock starts once Trino reports an active node, so the JVM's start (the restart itself, up to 22 s, is recorded as `restart_seconds`) is not in these times. Cold Trino over Iceberg pays 2.0–5.4 s over its warm median at 1M and 1.9–10.9 s at 20M (q10: 13.3 s against 2.5 s warm). Trino over PostgreSQL, restarted the same way, answers its count and its lookup at 1M in 0.69–0.78 s, so most of that cost is the Iceberg path: metadata, manifests and Parquet footers fetched cold from MinIO, and code the fresh JVM has not compiled yet (the Iceberg catalog's JDBC database is PostgreSQL, which a Trino-over-Iceberg cold run does not restart). A cold PostgreSQL pays its reads from disk. Cold, PostgreSQL wins every query at 1M; at 20M Trino over Iceberg still wins the whole-table aggregates q01–q04 (q03: 9.4 s against 16.8 s), because reading the heap from disk costs more than the cold Iceberg path. These are single runs; cold q10 at 20M is Trino over PostgreSQL's by 0.4 s (12.9 against 13.3 s).

### Pruning: partitioned vs unpartitioned vs uncompacted

<!-- bench:query-pruning -->
| Query | Rows | Layout | Data files in the table | Files the window's bounds match | Splits read | Rows read | Bytes read | Median / p95 |
|---|---|---|---|---|---|---|---|---|
| q05 The 10 most-changed pages in one hour | 1M | Iceberg, by hour, compacted | 13 | 1 | 1 | 83,149 | 607.6 kB | 77 ms / 86 ms |
| q05 The 10 most-changed pages in one hour | 1M | Iceberg, unpartitioned | 1 | 1 | 1 | 1,000,000 | 3.9 MB | 153 ms / 212 ms |
| q05 The 10 most-changed pages in one hour | 1M | Iceberg, by hour, as streamed | 2,219 | 183 | 183 | 83,149 | 6.6 MB | 264 ms / 354 ms |
| q05 The 10 most-changed pages in one hour | 1M | PostgreSQL heap, BRIN on `event_ts` | – | – | 3,074 of 26,720 blocks | – | 25.2 MB touched | 60 ms / 89 ms |
| q06 Events, editors and wikis in one hour | 1M | Iceberg, by hour, compacted | 13 | 1 | 1 | 83,149 | 709.8 kB | 77 ms / 106 ms |
| q06 Events, editors and wikis in one hour | 1M | Iceberg, unpartitioned | 1 | 1 | 1 | 1,000,000 | 2.2 MB | 180 ms / 233 ms |
| q06 Events, editors and wikis in one hour | 1M | Iceberg, by hour, as streamed | 2,219 | 183 | 183 | 83,149 | 6.6 MB | 253 ms / 386 ms |
| q06 Events, editors and wikis in one hour | 1M | PostgreSQL heap, BRIN on `event_ts` | – | – | 3,074 of 26,720 blocks | – | 25.2 MB touched | 72 ms / 96 ms |
| q07 The 10 wikis that grew most in six hours | 1M | Iceberg, by hour, compacted | 13 | 6 | 6 | 495,094 | 5.7 MB | 88 ms / 117 ms |
| q07 The 10 wikis that grew most in six hours | 1M | Iceberg, unpartitioned | 1 | 1 | 1 | 1,000,000 | 3.7 MB | 145 ms / 182 ms |
| q07 The 10 wikis that grew most in six hours | 1M | Iceberg, by hour, as streamed | 2,219 | 1,099 | 1,099 | 495,094 | 39.6 MB | 921 ms / 1.29 s |
| q07 The 10 wikis that grew most in six hours | 1M | PostgreSQL heap, BRIN on `event_ts` | – | – | 14,106 of 26,720 blocks | – | 115.6 MB touched | 98 ms / 115 ms |
| q05 The 10 most-changed pages in one hour | 20M | Iceberg, by hour, compacted | 243 | 1 | 1 | 83,345 | 608.3 kB | 82 ms / 194 ms |
| q05 The 10 most-changed pages in one hour | 20M | Iceberg, unpartitioned | 1 | 1 | 6 | 20,000,000 | 32.0 MB | 348 ms / 699 ms |
| q05 The 10 most-changed pages in one hour | 20M | Iceberg, by hour, as streamed | 44,357 | 183 | 183 | 83,345 | 6.6 MB | 341 ms / 437 ms |
| q05 The 10 most-changed pages in one hour | 20M | PostgreSQL heap, BRIN on `event_ts` | – | – | 3,353 of 534,112 blocks | – | 27.5 MB touched | 47 ms / 49 ms |
| q06 Events, editors and wikis in one hour | 20M | Iceberg, by hour, compacted | 243 | 1 | 1 | 83,345 | 710.7 kB | 80 ms / 141 ms |
| q06 Events, editors and wikis in one hour | 20M | Iceberg, unpartitioned | 1 | 1 | 6 | 20,000,000 | 25.4 MB | 284 ms / 436 ms |
| q06 Events, editors and wikis in one hour | 20M | Iceberg, by hour, as streamed | 44,357 | 183 | 183 | 83,345 | 6.6 MB | 633 ms / 1.04 s |
| q06 Events, editors and wikis in one hour | 20M | PostgreSQL heap, BRIN on `event_ts` | – | – | 3,353 of 534,112 blocks | – | 27.5 MB touched | 106 ms / 139 ms |
| q07 The 10 wikis that grew most in six hours | 20M | Iceberg, by hour, compacted | 243 | 6 | 6 | 495,300 | 5.7 MB | 85 ms / 177 ms |
| q07 The 10 wikis that grew most in six hours | 20M | Iceberg, unpartitioned | 1 | 1 | 6 | 20,000,000 | 51.4 MB | 410 ms / 698 ms |
| q07 The 10 wikis that grew most in six hours | 20M | Iceberg, by hour, as streamed | 44,357 | 1,099 | 1,098 | 495,295 | 39.6 MB | 732 ms / 1.06 s |
| q07 The 10 wikis that grew most in six hours | 20M | PostgreSQL heap, BRIN on `event_ts` | – | – | 14,393 of 534,112 blocks | – | 117.9 MB touched | 514 ms / 744 ms |
<!-- /bench:query-pruning -->

The three window queries on each Iceberg layout through Trino, with PostgreSQL's BRIN plan beside them. Files the window's bounds match come from the table's `$files` metadata (`event_ts` lower and upper bounds); splits and rows read from `EXPLAIN ANALYZE` of the same query; bytes read are Trino's physical input, median over the 10 runs. For PostgreSQL the blocks are those `EXPLAIN (ANALYZE, BUFFERS)` touched, 8 kB each.

- **Partitioning: one hour reads one file**, 0.6 MB and the hour's rows (83,149 at 1M, 83,345 at 20M), at both scales; the 20M table is 20 times larger and the hour query costs the same 80 ms.
- **Unpartitioned: nothing can be skipped.** The CTAS writes one file (39 MB at 1M, 781 MB at 20M) and does not sort it, so neither file bounds nor its row groups' statistics exclude anything and the scan reads all 20,000,000 rows to keep 83,345. It stays at 0.3–0.4 s because Parquet still lets it read only the needed columns (25–51 MB of 781 MB), but it is 3.5–4.8 times the partitioned table at 20M and grows with the table, where the partitioned one does not. A copy sorted by `event_ts` could skip row groups; that was not measured.
- **Uncompacted: the right files, too many of them.** Pruning works (183 files for the hour, 1,099 for six hours) and the rows read are the same as compacted (at 20M one small file of the 1,099 is also skipped on `event_type` statistics), but each 36 kB file is an object request and a footer: 7–11 times the bytes (6.6 MB instead of 0.6 MB for the hour), and q07 is 8.6 times slower at 20M (732 ms against 85 ms) and 10.5 times at 1M. This is what `lake_maintenance`'s `optimize` exists to remove. The layout was written in one commit, so these numbers leave out the cost of the thousands of snapshots a real stream would also have left.
- **PostgreSQL's BRIN index is its partitioning.** It reads 3,353 blocks of 534,112 for the hour (27.5 MB, whole rows) and wins q05, but q06 at 20M is a tie and the six-hour query reads 118 MB of rows where Trino reads 5.7 MB of columns.
- **Unpartitioned against uncompacted.** The streamed layout is slower than the unpartitioned file in five of the six cells (1.4–6.4x) and ties it on q05 at 20M (341 against 348 ms, with 6.6 MB read against 32 MB). The unpartitioned table's cost grows with the table (1.6–2.3x the partitioned one at 1M, 3.5–4.8x at 20M) while the streamed layout's does not, so at some larger scale partitioning would matter more than compaction; at 1M and 20M compaction matters more.

**Run-to-run spread.** The three window queries on the partitioned table ran twice with identical SQL: in the warm unit (between the other engines' runs) and in this pruning unit (ten in a row). The medians differ more than the engines do: q05 114 against 77 ms at 1M and 130 against 82 ms at 20M, q06 126 against 77 ms, q07 176 against 88 ms at 1M. The cause was not established. So below about 200 ms, a difference under 2x between engines is not resolved by these runs: q06 at both scales and q07 at 1M (PostgreSQL 98 ms against Trino's 88–176 ms) are ties, whatever the latency table's last column says.

### A federated query: lake events joined with a warehouse mart

<!-- bench:query-federated -->
| Rows | Trino: Iceberg ⋈ PostgreSQL | The same join inside PostgreSQL | Result |
|---|---|---|---|
| 1M | 166 ms / 246 ms | 59 ms / 77 ms | matches, 15 rows |
| 20M | 155 ms / 203 ms | 93 ms / 114 ms | matches, 15 rows |
<!-- /bench:query-federated -->

<!-- bench:query-federated-result -->
Result at 20M rows, hour starting 2026-10-22T00:00:00+00:00:

| project_family | events | wikis |
|---|---|---|
| wikipedia | 31,344 | 105 |
| commons | 26,323 | 1 |
| wikidata | 16,439 | 2 |
| wiktionary | 7,954 | 20 |
| wikisource | 465 | 10 |
| wikiquote | 433 | 2 |
| wikimedia | 195 | 7 |
| mediawiki | 53 | 1 |
| wikibooks | 36 | 1 |
| wikivoyage | 32 | 4 |
| wikifunctions | 31 | 1 |
| wikiversity | 18 | 3 |
| wikispecies | 16 | 1 |
| abstract_wikipedia | 4 | 1 |
| (none) | 2 | 1 |
<!-- /bench:query-federated-result -->

The Trino query joins the Iceberg events with `marts.dim_wiki`, which dbt builds in PostgreSQL, in one statement across two catalogs; its result equals the same join run inside PostgreSQL on `bench.events_<N>`. Trino reads one hour's file from the lake and the 160-row dimension through the connector, and the join costs it 155 ms at 20M.

## Release (M8)

Run on 2026-11-11 on a 4 vCPU, 15 GB machine (Linux 7.0, Docker 29.4.2,
Compose 5.1.3). The Docker build cache was warm from an earlier build of the
same Dockerfiles, so these times do not include a cold MinIO compile (about
4 min). $0: the one live model call was a local model.

### The README quickstart from a clean checkout

A fresh `git clone` of the release branch into an empty directory, with no
virtualenv and nothing else from the working copy, ran the
[quickstart](README.md#quickstart) command by command:

| Step | Result |
|---|---|
| `docker compose up -d --build` | Containers up in 45 s; the producer reported `produce_done` (50,000 sent, 50,000 delivered, 0 failed), and `raw.recentchange` held 50,000 rows 2 min 51 s after the command started |
| `pip install '.[warehouse]'` into a new `python3.12 -m venv` | 78 s; `tributary version` printed `0.1.0.dev0` (this run was before the version commit) |
| `tributary warehouse refresh` | 90 s: 8 relations built, 49 checks passed, catalog published under `./catalog` |
| `tributary warehouse grant-agent` | `role tributary_agent: SELECT on marts and ops, read-only sessions` |
| `tributary sql` top 5 wikis | wikidatawiki 9,745, enwiki 4,564, commonswiki 4,047, kuwiktionary 722, zhwiki 636: the same rows as the README |
| `tributary sql "DELETE FROM marts.dim_wiki"` | `blocked by the safety policy: write_or_ddl: Delete statement`, exit 1 |
| `tributary ask --provider ollama "Which 5 wikis had the most edits (including page creations)? ..."` (Ollama 0.40.6 in Compose, `qwen2.5-coder:7b`, CPU, first call of a cold model) | The same five rows, from `SUM(edits)` over `marts.agg_editor_activity`; 2,618 input and 131 output tokens, 309.6 s of model time, 5 min 10 s wall |
| `tributary ask --provider fake` with the same question | The recorded answer of the same model, with the same SQL and rows and the same 2,618 tokens, in under 50 ms of planner time. The recording was made against a database loaded straight from the sample, and it replays against the one Kafka and Spark filled, so the two catalogs match |

The live answer and the replayed one wrote the same SQL. That is one data
point, not evidence of determinism: the model ran at its default
temperature.

### Packaging and the release workflows

| Check | Result |
|---|---|
| `make dist` (version 0.1.0) | sdist 160,787 B, wheel 169,964 B; `twine check --strict` PASSED for both; the wheel with the `api` extra installs into a fresh venv outside the checkout, and `tributary version` prints `0.1.0` there, and the app imports from `site-packages` |
| `release.yml`, run by hand (dry run) under `act` 0.2.97 | build job green (version check skipped off a tag, `make dist`, artifact uploaded); the PyPI and GitHub-release jobs skipped |
| `release.yml` on a simulated `v0.1.0` tag push, build job | green, with the tag-matches-version check passing |
| The same with a `v0.1.1` tag | failed at the check: `tag v0.1.1, package 0.1.0` |
| `images.yml`, run by hand (dry run) | both images built for linux/amd64 and linux/arm64 (QEMU), in 4 min 21 s (api) and 4 min 34 s (stream); not pushed, and the registry login was skipped |

### The hardened API container and the CI jobs

`TRIBUTARY_API_KEYS=... docker compose --profile api up -d --build --wait api`
on the same stack (no catalog published, so data routes answer 503):

| Check | Result |
|---|---|
| `docker inspect` | `ReadonlyRootfs=true`, `CapDrop=[ALL]`, `no-new-privileges:true`, user `tributary`; health check `healthy` |
| `GET /health` | 200 with `nosniff`, `X-Frame-Options: DENY`, `default-src 'none'; frame-ancestors 'none'`, `no-referrer`, `no-store` |
| No key; `/docs` | 401; 404 (docs off by default) |
| 12 authenticated requests at once (`/freshness`, burst 10) | 10 answered (503, no catalog), then 429 with `Retry-After: 1` |
| A 70,000-byte body to `/sql` | 413 before parsing |
| `/metrics` with the key | Every request above counted by route template and status, plus `tributary_rate_limited_total 3`. The 413 is counted under `unmatched`, because the body cap answers before routing. |

The three CI workflows ran their steps locally on the release branch. For
`CI`, that was `make check` (1312 passed, 7 xfailed, on Java 17),
`make eval-guard`, `make eval`, `make eval-link`, and
`git diff --exit-code eval/results RESULTS.md` (clean). For `Warehouse`, on a
fresh `postgres:17.11`: `tributary sample load`, `make dbt` (PASS=57) and
`make test-warehouse` (32 passed). For `Integration`, on a fresh
`--profile lakehouse` stack: `make test-integration` (7 passed, stream job
not restarted) and `pytest tests/warehouse -m lakehouse` (1 passed).

`act` reproduces the runner, not GitHub. The trusted-publishing exchange with
PyPI, the `pypi` environment's approval and the push to GHCR cannot run
without the repository's credentials. RELEASE.md lists them as the
maintainer's steps.

## A differential oracle for the guard (M9)

Run on 2026-11-12 on the machine above (4 vCPU, 15 GB, PostgreSQL 17.11 from
`compose.yaml`, the sample loaded and `make dbt` built). $0: no model is
involved. Raw output: [`eval/results/guard-diff/`](eval/results/guard-diff/),
one JSON and one text summary per run, each stamped with its commit.

**Method.** `make eval-guard-diff` (`eval/guard/differential.py`). The
generator takes every function in `pg_proc` callable with one argument
(1,312: one declared argument, or the rest defaulted) and substitutes each
into the ten spelling templates of
[`eval/guard/templates.yaml`](eval/guard/templates.yaml): field notation
`(x).f`, `alias.f` over a mart and over its bare name, a derived table, a
CTE, `VALUES`, a set-returning and a scalar function in FROM (the scalar one
chosen to return the function's own argument type), and an alias rebound in
a nested scope, over a function and over a mart. Each goes in four positions
(select list, `WHERE`, `ORDER BY`, `FILTER`). The 40 warehouse gold queries
are added unchanged, with `alias.f` in the outer `WHERE` for every outer
alias, and wrapped as a derived table and as a CTE with `g.f` selected:
220,456 statements in all. The templates were committed (`a78c6dd`) before
the guard was changed. The crossing by argument type is narrower than it
sounds: the generator has a value for 510 of the 1,312 functions' first
argument types, and passes the integer `1` to the other 802, so a function
taking `bytea`, `inet`, `uuid`, `tsvector` or `xml` is never fed a value of
its own type in the `field` and `scalar_from_function` templates (an integer
does reach `oid` and the `reg*` types by implicit cast).

The oracle never executes a generated statement. Each one the guard admits
is wrapped as `SELECT count(*) FROM (<the guard's regenerated SQL>) AS q`,
created as a temporary view by the owner role with the agent's empty
`search_path`, in a transaction that is then rolled back. Creating a view
runs PostgreSQL's parse analysis and stores the resolved tree in
`pg_rewrite.ev_action`. The oracle reads every function, aggregate, window
function and operator id out of that tree, plus sort operators and relations,
and judges each against the guard's own policy (`function_refusal`: the
denylist for every function however it is reached, and the allowlist for a
call written by name). A statement is *flagged* if it resolves to something
the guard would have refused had it seen it. The parser and the judge are
unit-tested offline against `ev_action` text recorded from this server
(`tests/fixtures/guard_oracle/`).

**Before the fix** (`before`, commit `a78c6dd`, 459 s):

| Generated | Admitted by the guard | Refused by PostgreSQL at parse analysis | Resolved | Flagged |
|---:|---:|---:|---:|---:|
| 220,456 | 199,464 | 194,584 | 4,880 | **2,768** |

Almost everything is admitted because, to sqlglot, `w.f` is a column. Most
of it then fails parse analysis: 192,736 with "column does not exist"
(42703), because no function `f` accepts the row. The 4,880 that resolve
reach 17 functions outside the policy, all through a relation, derived
table, CTE or `VALUES` alias (72 flagged per template, 760 to 816 per gold
rewrite):

| Volatility | Functions reachable outside the policy |
|---|---|
| immutable (6) | `any_out`, `anycompatible_out`, `anycompatiblenonarray_out`, `anyelement_out`, `anynonarray_out`, `hash_record` |
| stable (11) | `pg_typeof`, `pg_column_size`, `pg_column_compression`, `pg_column_toast_chunk_id`, `pg_collation_for`, `quote_literal`, `quote_nullable`, `record_out`, `record_send`, `json_agg_strict`, `jsonb_agg_strict` |
| volatile | none |

The hypothesis was 10 to 25 functions, none volatile; 17, none volatile.
What it did not predict: field notation reaches aggregates, so
`w.jsonb_agg_strict` is an aggregate call over the whole table. The
function-in-FROM templates, field notation and the rebound function alias
were admitted 0 times, because the three earlier fixes hold. A further 1,368
statements were refused as 42809 and 480 as 42803, the grouping error.

**Each historical fix switched off in turn** (a test-only switch,
`fixes_disabled`, at the same commit, templates only, without the gold
rewrites):

| Fix switched off | Where the oracle flags it | Flagged there | Reachable outside the policy, volatile among them |
|---|---|---:|---|
| Field notation `(x).f` (M3) | `field` | 1,874 | 432 functions, 97 volatile: `pg_sleep`, `pg_terminate_backend`, `pg_read_file`, `lo_import`, `pg_advisory_lock`, `setseed`, ... |
| A qualified column on a function in FROM (M8) | `from_function` 1,384, `scalar_from_function` 1,858, `reused_alias` 1,384 | 4,626 | 428, 96 volatile |
| Intersecting an alias bound twice (M8 review) | `reused_alias` | 1,384 | 314, 74 volatile |

The oracle flags every one of the three. Switching off the M8 check reopens
its refinement too, because the rebound alias is a case of it. Each run also
still flags the 432 relation-alias statements the guard had not yet closed.

**After the fix** (`after`, commit `673503e`, 129 s): the guard binds each
qualifier as PostgreSQL scopes it (sqlglot's scope analysis) and refuses a
name that is not a column of the bound source but is one of pg_catalog's
one-argument functions.

| Generated | Admitted | Refused by PostgreSQL | Resolved | Flagged |
|---:|---:|---:|---:|---:|
| 220,456 | 192 | 152 | 40 | **0** |

The 40 that resolve are the unchanged gold queries. The 152 are
`w._pg_expandarray` in every template and position, a function in
`information_schema` that the agent's empty `search_path` cannot reach.
PostgreSQL refused each one, which is the reason the guard's list holds only
pg_catalog's names (the count is one function across 24 template, 40 + 40
gold-wrap and 48 outer-alias slots; the JSON keeps the totals, not the
statements). The fix also refuses 2,072 statements that resolved, before it,
to functions the policy allows (4,880 resolved, less 2,768 flagged and the
40 gold): `w.to_json`, `w.row_to_json` and the like. That is the same line
the field-notation check has drawn since M3. A whole-row call is refused
however harmless, because the guard cannot tell it from a column without
the catalog. Switching the new check off at the same commit
(`after-without-relation_alias`) gives back the same 432 flagged template
statements and the same 17 functions. `make eval-guard` after the fix:

| | |
|---|---|
| The 119 statements of the suite before M9 | 119 / 119 blocked |
| With the eight added for M9 (one per spelling the oracle found) | 127 / 127 blocked |
| With four more the review found the guard admitting (see below) | 131 / 131 blocked |
| Gold queries blocked | 0 / 40 warehouse, 0 / 500 BIRD official, 0 / 449 BIRD corrected |
| Recorded model SQL whose verdict changes (381 statements in `tests/recordings/`) | 0; `make eval` replays verdict for verdict and `make eval-warehouse` gives 25 / 40 |

**What the review found after these runs.** The check skipped a qualifier
it could not bind, on the reasoning that PostgreSQL would refuse it too. It
does not: a CTE referenced under an alias (`FROM c AS m`, bound by the CTE's
own name instead of `m`), a `LATERAL` subquery, a parenthesised join's alias
and a subquery inside `JOIN ... ON` (whose columns no sqlglot scope lists)
all reached `pg_typeof(<row>)`, confirmed by the same temporary-view
resolution. The generator had no template for any of them, so the oracle's 0
was a 0 over the spellings it was given. The check now binds a CTE by its
reference and fails closed on a callable name whose source it cannot name.
Re-run on the fixed guard (`after`, rerun into a scratch directory, not
committed): still 0 flagged; `make eval-guard` 131 / 131 with 0 / 40, 0 /
500 and 0 / 449 false blocks; `make eval` verdict for verdict and `make
eval-warehouse` 25 / 40.

**Why the check stops at callable names.** A first version refused every
qualified name that was not a column. That changed 51 of the 381 recorded
model statements. All 51 were model typos, such as `T1.link_to_event` on the
wrong table or `fe.edits` on `fct_edits`, which PostgreSQL had already
refused with "column does not exist". That message, with its hint, is what
the model repaired from. A name that is neither a column nor a callable
function cannot hide a call, so it is left to the database. The list
(`tributary/agent/pg_functions.py`, 1,074 names) is generated from the
server, and `make test-warehouse` fails if the server it runs against has a
name the list lacks.

**What the oracle cannot see, counted.** A cast of a literal is folded by
the parser into a constant, so the type's input function has already run
and leaves no id. An I/O conversion cast (`COERCEVIAIO`) calls an output and
an input function that the tree names by type, not by id. Both are counted
per run, not excluded. `before` and `after` have 0 of each: no admitted
template or gold query casts a literal, and none resolved through an I/O
conversion. With a fix switched off, 652 admitted statements carry a literal
cast in the field-notation run and again in the function-alias run (the
`bigint`, `smallint`, `real` and `double precision` arguments, written
`CAST(1 AS bigint)`), and 36, 132 and 48 resolved statements in the three
runs contain an I/O conversion. The input and output functions those run
are not in the reachable lists above.

**Reproducing these files.** `before` and the three `without-*` runs are
`python -m eval.guard.differential run --label <label> [--disable <fix>
--no-gold]` at `a78c6dd`, before the relation-alias check existed; `after`
and `after-without-relation_alias` are the same at `673503e`. `make
eval-guard-diff` runs the current tree (label `current`) and every fix
switched off in turn, the relation-alias check included; at a commit after
the fix the `without-*` runs no longer carry the 432 relation-alias
statements, since that check stays on in them.

**Known blind spots after the review.** The oracle's tree reader treated
every `[` as the start of a constant's bytes; a column alias spelled `"["`
hid every call after it. No committed template or gold query has one, so
the runs above are unaffected; it now opens a byte array only after
`:constvalue <length>`. It also reads a `TABLESAMPLE` method's handler now.
It still cannot see the comparison function behind `GREATEST`/`LEAST`, a
type's subscripting handler, a domain's `CHECK` or a SQL/JSON expression's
coercions, which the tree names by type or which are chosen when the plan
is made; all are pg_catalog's own and pure today.

### A result byte cap

`TRIBUTARY_AGENT_MAX_RESULT_BYTES` (default 8 MiB). The executor wraps each
query so that PostgreSQL adds up the text size of the values it is about to
send, row by row with a running total. Past the cap it fails the statement
with a cast error carrying the size, reported as the repairable error
`too_large`. Columns are renamed by position rather than measured as a
whole-row composite. On a 20 MB row the composite took the server from
72 MB to 229 MB; positional renaming leaves it at 73 MB. On the first
attempt the composite twice got the 1 GiB container's backend killed with
the 200 MB row. A `char(n)` value is measured as it is, not
through `::text`, whose cast strips the padding the server still sends
(`''::char(10485760)` is 0 bytes as text and 10 MiB on the wire). An inner `LIMIT row_cap + 1` stops the planner evaluating the
query's expressions once per mention. Raw:
[`eval/results/result-cap/`](eval/results/result-cap/).

| `/usr/bin/time -v uv run tributary sql "..."` | Peak RSS before (`4ae818f`) | After | Exit |
|---|---:|---:|---|
| `SELECT repeat('x', 200000000)` | 1,639,668 kB | **77,904 kB** | 0 (200 MB printed) → 1, "the result reached 200,000,000 bytes, over the cap of 8,388,608" |
| `SELECT 1` | 77,528 kB | 77,808 kB | 0 → 0 |

Recorded results over the cap (`uv run python -m eval.result_bytes`):
**0 of 275**. That is 0 of 94 BIRD gold results (largest 49,955 bytes), 0 of
141 BIRD results of the model's SQL (largest 580,926 bytes, 42,823 rows) and
0 of 40 warehouse gold results (largest 177 bytes). The warehouse sizes come
from PostgreSQL itself, as the agent's role; the BIRD ones from the recorded
values, with the size function matching PostgreSQL's `::text` on 23 types.
The BIRD harness runs without the cap, as it runs without the cost ceiling,
because it scores the model's SQL and replays recorded bare executions. 52
queries (the 40 gold plus edge cases: `ORDER BY`, duplicate names, `WITH`,
set operations, `VALUES`, arrays, bytea, NaN) return identical columns, row
order, values and types with and without the cap.

## The default variant on a held-out sample (M10)

M7 chose `full+v+f+a` as `make eval`'s live default on the same 50 questions
it is reported on (+8 −3, p = 0.23 and +13 −5, p = 0.10 against `full`).
This section asks whether that gain survives on 50 questions that did not
pick it.

### Fixed before any model call

<!-- m10:prereg -->
**The sample.** `holdout50` is drawn by dev50's rule from what dev50 left:
per difficulty, the official questions Arcwise-Plat-SQL also covers, minus
dev50's 50, sorted by `sha256("tributary:holdout50:<id>")`, the smallest 15
simple, 25 moderate and 10 challenging (`eval/bird/data.py`,
`subset_ids`). Its ids are committed in `eval/bird/data/subsets.json`, and
a unit test fails if the rule ever draws anything else. 46 of the 50 are in
the verified corrected track. Each run's worked examples (`+f`) come from
the questions outside its own subset, so `holdout50`'s pool holds dev50's
questions and the committed dev50 recordings replay unchanged.

**The run.** `make eval PROVIDER=ollama MODEL=qwen2.5-coder:7b
SUBSET=holdout50 VARIANTS=full+a,full+v+f+a,full RECORD=1` (resumable with
`RESUME=1`), both tracks, temperature 0, seed 0, 16k context, one request
at a time; `full+a` and `full+v+f+a` first, since they decide the default.

**The decision rule.** `full+v+f+a` stays `make eval`'s live default only
if, on `holdout50` alone, it is at least level with `full+a` on both
tracks (correct answers with repairs, official and corrected). Otherwise
the default becomes `full+a`, and this section says the M7 gain did not
replicate. dev50 does not vote: it is the sample that picked the variant.

**The hypotheses (PLAN.md, M10).**

1. `+a` keeps a net gain over `full` on `holdout50` (gained > lost on
   both tracks): refusals are a mechanism, not a fluke.
2. `+v+f` over `+a` is within ±2 questions on the official track.
3. Pooled to n = 100 (dev50 + `holdout50`), at most one of the two
   pairings against `full` (`full+a`, `full+v+f+a`) reaches p < 0.05 on
   the official track.
<!-- /m10:prereg -->

### holdout50: each variant on the held-out questions

<!-- bird:holdout -->
| Variant | Official EX (95% interval) | Corrected EX (95% interval) | Official EX, no repairs | Refused | Model calls | Tokens in / out | Model time per question, p50 / p95 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `full` | **17/50** (34.0%, 22.4%–47.9%) | **19/46** (41.3%, 28.3%–55.7%) | 16/50 | 6 | 78 | 139,437 / 16,352 | 68.3 s / 195.2 s |
| `full+a` | **16/50** (32.0%, 20.8%–45.8%) | **19/46** (41.3%, 28.3%–55.7%) | 16/50 | 0 | 85 | 150,703 / 18,853 | 90.5 s / 301.6 s |
| `full+v+f+a` | **24/50** (48.0%, 34.8%–61.5%) | **27/46** (58.7%, 44.3%–71.7%) | 23/50 | 0 | 77 | 176,165 / 19,065 | 103.2 s / 270.7 s |
<!-- /bird:holdout -->

### The pairings, on each subset and pooled

"+gained -lost" counts the questions the second variant gets right and the
first gets wrong, and the reverse, on the same questions; p is the exact
two-sided McNemar test on those discordant questions. The pooled columns add
the two subsets' counts (n = 100 on the official track, 45 + 46 = 91 on the
corrected one). `uv run python -m eval.bird.paired A B` prints the same
comparison for any two recorded variants.

<!-- bird:pairs -->
| Pairing | Track | dev50: n | dev50: correct | dev50: +gained -lost | dev50: p | holdout50: n | holdout50: correct | holdout50: +gained -lost | holdout50: p | dev50 + holdout50: n | dev50 + holdout50: correct | dev50 + holdout50: +gained -lost | dev50 + holdout50: p |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `full+a` vs `full` | official | 50 | 21 → 25 | +7 -3 | 0.344 | 50 | 17 → 16 | +3 -4 | 1.000 | 100 | 38 → 41 | +10 -7 | 0.629 |
| `full+a` vs `full` | corrected | 45 | 18 → 22 | +5 -1 | 0.219 | 46 | 19 → 19 | +4 -4 | 1.000 | 91 | 37 → 41 | +9 -5 | 0.424 |
| `full+v+f+a` vs `full` | official | 50 | 21 → 26 | +8 -3 | 0.227 | 50 | 17 → 24 | +10 -3 | 0.092 | 100 | 38 → 50 | +18 -6 | 0.023 |
| `full+v+f+a` vs `full` | corrected | 45 | 18 → 26 | +13 -5 | 0.096 | 46 | 19 → 27 | +11 -3 | 0.057 | 91 | 37 → 53 | +24 -8 | 0.007 |
| `full+v+f+a` vs `full+a` | official | 50 | 25 → 26 | +8 -7 | 1.000 | 50 | 16 → 24 | +12 -4 | 0.077 | 100 | 41 → 50 | +20 -11 | 0.150 |
| `full+v+f+a` vs `full+a` | corrected | 45 | 22 → 26 | +11 -7 | 0.481 | 46 | 19 → 27 | +11 -3 | 0.057 | 91 | 41 → 53 | +22 -10 | 0.050 |
<!-- /bird:pairs -->

Pooled execution accuracy:

<!-- bird:pooled -->
| Variant | Official EX, dev50 + holdout50 | Corrected EX, dev50 + holdout50 |
|---|---:|---:|
| `full` | **38/100** (38.0%, 29.1%–47.8%) | **37/91** (40.7%, 31.1%–50.9%) |
| `full+a` | **41/100** (41.0%, 31.9%–50.8%) | **41/91** (45.1%, 35.2%–55.3%) |
| `full+v+f+a` | **50/100** (50.0%, 40.4%–59.6%) | **53/91** (58.2%, 48.0%–67.8%) |
<!-- /bird:pooled -->

### The default, under the rule

<!-- bird:decision -->
**The rule, applied.** On holdout50 alone, `full+v+f+a` answers official 24 against 16 for `full+a`; corrected 27 against 19 for `full+a`. `full+v+f+a` is at least level with `full+a` on both tracks, so it stays `make eval`'s live default (`full,full+v+f+a`).
<!-- /bird:decision -->

`DEFAULT_LIVE_VARIANTS` in `eval/bird/run.py` is unchanged, and a unit test
(`test_the_live_default_is_what_the_rule_picks_on_the_committed_run`) fails
if it ever disagrees with the rule applied to the committed holdout50 run.

### The hypotheses, answered

1. **`+a` keeps a net gain over `full`: no.** On holdout50 `full+a` against
   `full` is +3 −4 official (p = 1.000) and +4 −4 corrected (p = 1.000).
   The mechanism did work: `full` refused 6 official questions and `full+a`
   answered all of them, 2 correctly (dev50: 4 of 11). What cancelled it is
   the rest of the prompt change. All 4 official questions `full+a` lost
   (707, 717, 733, 1239) were ones `full` had answered correctly, not
   refusals: adding the rule's sentence changed the SQL the model wrote on
   questions the rule was never meant to touch (733 became a statement the
   guard blocked). So M7's +7 −3 on dev50 was a real mechanism plus
   perturbation that happened to fall its way. On fresh questions the
   perturbation was as large as the mechanism's gain.
2. **`+v+f` over `+a` within ±2 on the official track: no, it is larger.**
   `full+v+f+a` against `full+a` is +12 −4 official (16 → 24, p = 0.077)
   and +11 −3 corrected (19 → 27, p = 0.057). On dev50 the same pairing was
   +8 −7. The gain M7 attributed mostly to `+a` belongs, on this sample, to
   value hints and worked examples. M7 saw the opposite, which is what
   noise on 50 questions looks like when three variants are compared.
3. **Pooled to n = 100, at most one of the two pairings against `full`
   reaches p < 0.05 on the official track: yes, exactly one.**
   `full+v+f+a` against `full` is +18 −6 (38 → 50 of 100, p = 0.023);
   `full+a` against `full` is +10 −7 (p = 0.629). On the corrected track
   the pooled `full+v+f+a` pairing is +24 −8 (37 → 53 of 91, p = 0.007).
   Read the pooled p with care: dev50 is the sample that chose the variant,
   so it is biased toward it. holdout50 alone does not reach 0.05 (p =
   0.092 official, 0.057 corrected), but it points the same way with a
   larger net (+7 and +8) than dev50 had (+5 and +8).

**What this changes.** The default stays, on fresh evidence rather than on
the sample that picked it. The explanation of *why* it wins changes: the
answer-every-question rule alone is a wash, and value hints with worked
examples carry the gain. The M7 ablation cannot separate `+v` from `+f`
(the leave-one-out cells are still under [To measure](#to-measure)).

The product's agent still keeps value hints and worked examples off. The
first of M7's three reasons (a net gain of 1 and 4 over `full+a`) is
weaker now: on holdout50 it is 8 and 8. The other two stand, and the
warehouse re-recording under To measure is what would decide it, not
BIRD.

### The run

`make eval PROVIDER=ollama MODEL=qwen2.5-coder:7b SUBSET=holdout50
VARIANTS=full+a,full+v+f+a,full RECORD=1 RESUME=1`, `qwen2.5-coder:7b`
(Ollama digest `dae161e27b0e`, 4.7 GB) on Ollama 0.40.6 in Compose (8 GB
limit), CPU only, 4 vCPU, temperature 0, seed 0, one request at a time.
240 model calls, 6.14 h of model time in total (the sum of each recorded
call's latency). The run took two sessions: the first was interrupted
after 144 calls (all of `full+a` and the start of `full+v+f+a`; 3.65 h of
model time); `RESUME=1` answered those from the recordings and made the
remaining 96 calls live, in 149 min 23 s of wall time (`wall_seconds`
8960.8 in the report). The corrected track needs few calls of its own,
because most of its questions are worded as in the official track and
reuse the same request. BIRD's `calculate_ex` agrees with the harness on
245 / 245 pairs. Cost: $0. The replay (`make eval`, what CI runs) checks
the run verdict for verdict in 9 s, beside dev50's.

## One future-stamped event against the watermark (M11)

The question: how far ahead of the clock can one event be before real events
are dropped, and what bound on that skew costs nothing? The stream job's
watermark is the newest event time it has seen minus 10 minutes, and it never
moves back. Until this milestone the parser accepted an event time up to a day
ahead of its Kafka record time. So one event stamped hours ahead could put the
watermark hours past the clock, and every real event after it was then "late".

Everything below ran in Spark 4.1.3 local mode (`local[2]`, RocksDB state, Java
17) on the committed sample, through the stream job's own `parse_events` and
`deduplicate`. The only exception is the Compose run at the end. Raw output is
in `bench/results/m11-skew/`. Cost: $0, no model involved.

### The M5 incident, reproduced and explained

In M5 a fault run on a stack that had just run `make test-integration` lost
all 50,000 events. The sample's own timestamps are a month in the past, so how
a replay put the watermark ahead of the clock was not understood.
`python -m bench.skew incident` replays the sequence as one stream. First comes
a fault run (bench events stamped at the clock). Then the two integration
tests, in the order pytest runs them, are sent the way they sent them, at full
speed, so the Kafka record time is the send time. Last comes the next fault
run. It runs once with the old one-day bound and once with the new 300 s
bound. The wall clock is simulated; only the gaps between phases matter.

<!-- bench:incident -->
| Phase | Sent at | Events | Event times | Ahead of record time (max) | Landed, 1-day bound | Watermark after | Landed, 300 s bound |
|:--|:-:|---:|:-:|---:|---:|:-:|---:|
| fault run 1 | 16:00:00 | 50,000 | 15:58:20 to 16:01:39 | 0 (0.00 h) | 50,000 | 15:51:39 | 50,000 |
| test_lakehouse | 16:03:40 | 5,000 | 16:01:40 to 18:48:18 | 4,939 (2.74 h) | 5,000 | 18:38:18 | 211 |
| test_pipeline | 16:04:40 | 5,001 | 18:48:19 to 18:52:16 | 5,001 (2.79 h) | 5,001 | 18:42:16 | 0 |
| fault run 2 | 16:07:40 | 50,000 | 16:06:00 to 16:09:19 | 0 (0.00 h) | 0 | - | 50,000 |
<!-- /bench:incident -->

The records that ran ahead of the wall clock were the integration tests' own.
To stay clear of the watermark, both tests placed their events *just after the
newest row already landed*. They did not anchor to the clock. `test_lakehouse`
re-timed its 5,000 events 2 s apart from that row, and `test_pipeline` shifted
its 5,000 past the lakehouse ones. On a stack whose newest row came from a
fault run (stamped at the clock), that means 2.8 hours of event time starting
now. The producer sends all of it in a second, so 4,939 of the lakehouse events
and all 5,001 of the pipeline's were ahead of their record time, by up to
2.79 h. The one-day bound let them through. The watermark went to 18:42, 2 h 35
min past the clock that sent the next fault run, and that run landed 0 of
50,000. This matches the M5 note ("the watermark 2 h ahead"). With the 300 s
bound the same replays are rejected (4,789 and 5,001 records counted as
`future`), the watermark stays behind the clock, and the next fault run lands
all 50,000. The old tests would themselves have failed under the bound, so
both were changed to time their events behind the clock (`test_pipeline` at
the clock minus each event's sample lateness; `test_lakehouse` with its 2.8 h
ending a minute before now, skipped on a stack with no room for it).

### Loss against skew, predicted from the sample before the run

`make bench-skew`: the 50,000 sample events in stream order, 100 micro-batches
of 500 (one JSON file per trigger). Each record's Kafka time is its
`meta.dt`, so the bound sees what it would see live. One poison is inserted at
stream position 10,250 (the middle of batch 20). It is a copy of the event
there with its own id, stamped X s past that event's `meta.dt`.

`python -m bench.skew predict` wrote `prediction.json` from the sample alone. It
was committed before any Spark run, in its own commit ("Add make bench-skew and its prediction from the sample"). It replays Spark's rule: batch
k drops an event when its time is **at or below** the newest accepted event
time through batch **k − 2**, minus the delay. Late rows are filtered with the
previous batch's watermark (pinned by
`test_deduplicate_semantics_across_micro_batches` since M1), so a poison's
effect starts two batches later. The PLAN estimate is the first version of the
rule, "every event after the poison's batch stamped below
t_poison + X − 600". Each sweep (8 skews) took 4 to 5 minutes of wall time on 4
vCPU.

<!-- bench:skew -->
| Poison skew X | PLAN estimate | Before: predicted lost | Before: measured lost (dropped by watermark) | Same ids | After: poison | After: lost |
|---:|---:|---:|---:|:-:|:--|---:|
| 0 s | 0 | 0 | 0 (0) | yes | accepted | 0 |
| 60 s | 0 | 0 | 0 (0) | yes | accepted | 0 |
| 300 s | 0 | 0 | 0 (0) | yes | accepted | 0 |
| 450 s | 0 | 0 | 0 (0) | yes | rejected (future) | 0 |
| 600 s | 4 | 2 | 2 (2) | yes | rejected (future) | 0 |
| 900 s | 6,510 | 6,010 | 6,010 (6,010) | yes | rejected (future) | 0 |
| 3,600 s | 39,500 | 39,000 | 39,000 (39,000) | yes | rejected (future) | 0 |
| 82,800 s | 39,500 | 39,000 | 39,000 (39,000) | yes | rejected (future) | 0 |
<!-- /bench:skew -->

"Same ids": the set of real events missing from the sink is exactly the
predicted set, at every skew and in both arms. Every run read all 50,001
records in 100 data batches. The poison's batch delivered all 500 of its real
events in every run.

### The hypotheses, answered

1. **The loss is predictable to the row from the sample alone: yes**, once
   the rule accounts for the two-batch lag. The measured loss equals the
   prediction at all eight skews, the same ids each time. The first estimate
   was off for one reason. The batch right after the poison's (batch 21, 500
   events) is filtered against the watermark from *before* the poison, so it
   always survives. That is the whole gap at 3,600 s (39,500 against 39,000),
   and most of it at 900 s (6,510 against 6,010). Ties go to Spark's `<=`:
   at 900 s, 15 events sit exactly on the threshold, and the measured 6,010
   counts them (with `<` it would be 5,995).
2. **Nothing is lost up to about 457 s (600 − 143): yes, and further.**
   Measured: 0 at 450 s, 2 at 600 s. Predicted to the second, the first lossy
   skew is **575 s**, not 457. The 143 s maximum lateness is measured against
   the running maximum of event time, which can be any earlier event. The
   threshold here is set by the poison alone and first applies two batches
   later, from 36 s of stream after the poison. The oldest event from that
   point on trails the poison's emit time by only 25.5 s, so the poison has to
   be 600 − 25 s ahead before anything is dropped.
3. **The loss is a window that widens with X, not a step: yes.** 2 events at
   600 s, 6,010 at 900 s, then the whole rest of the stream at 3,600 s, where
   it saturates (the sample spans 36 minutes).
4. **At 3,600 s and 82,800 s every event in every later batch is lost: all
   but one batch.** 39,000 lost is every real event in batches 22 to 99.
   Batch 21 survives, for the reason in 1.
5. **The poison's own batch survives: yes**, 500 of 500 in every run, the
   poison included when accepted. The watermark moves between batches.
6. **A bound well under the delay rejects 0 of the 50,000 sample events:
   yes.** With the bound at 300 s the only rejection in any run is the poison
   (`future_rows` = 1 at skews of 450 s and up), and 0 real events are lost.
   No sample event is ahead of its emit time at all: the largest
   `timestamp − meta.dt` is −0.007 s.

### The bound, and why 300 s

An event accepted S seconds ahead of its record time moves the watermark to
`clock + S − delay`. After that, every real event more than `delay − S` behind
the clock is dropped. So the bound spends part of the lateness budget, and it
must stay below the delay. The default is half the delay
(`TRIBUTARY_MAX_FUTURE_SKEW_S` unset, so 300 s for the 10-minute delay;
`Settings.future_skew_s`). That is about 300 s more than any sample event
needs (none is ahead of its emit time). It leaves 300 s for real lateness,
twice the sample's largest emit delay (144 s) and lateness (143 s). The sweep's
"after" arm shows the result: a 300 s poison is accepted and costs nothing.
The cost is that an event with a clock really more than 300 s fast is rejected
and counted instead of landed. The job logs `stream.skew_bound_unsafe` at start
when the bound is set at or above the delay.

### What the job now records, and the check that reads it

Each batch's progress report writes its loss counters to
`ops.stream_progress`, for both queries: rows dropped by the watermark, and
records rejected as `malformed` or `future` (plus canaries, which are not
errors). The `stream_loss` check (`tributary warehouse check-stream`, and the
`check_stream` task of the `raw_recentchange` DAG every five minutes) sums the
last 24 h per query. It fails when any row was dropped by the watermark, or
when more than 0.1% of the input was rejected. The warehouse check targets
`raw.recentchange`, so an answer from any mart built on it carries
"failing checks: stream_loss_warehouse" in its provenance.

### The poisoned run on the Compose stack

`make bench-poison ARM=before|after`: a fresh stack (`make down`, then
`make up-lakehouse`, both sinks), 50,000 bench events at 200 events/s, with
event 5,000 stamped 82,800 s (23 h) past its send time. `before` runs the job
with `TRIBUTARY_MAX_FUTURE_SKEW_S=86400`, the old bound under the same
predicate. `after` runs it with the default.

<!-- bench:poison -->
| Arm | Bound | Sink | Landed of 50,000 | Real events lost | Poison landed | Dropped by watermark | Rejected as future | `stream_loss` check |
|:--|---:|:--|---:|---:|:-:|---:|---:|:--|
| before | 86400 | PostgreSQL | 5,650 | 44,350 | yes | 44,350 | 0 | fail |
| before | 86400 | Iceberg | 18,449 | 31,551 | yes | 31,551 | 0 | fail |
| after | 300 (default) | PostgreSQL | 49,999 | 0 | no | 0 | 1 | pass |
| after | 300 (default) | Iceberg | 49,999 | 0 | no | 0 | 1 | pass |
<!-- /bench:poison -->

Before the fix, the counters the job now writes equal the loss exactly:
44,350 rows dropped by the watermark is every real event missing from
PostgreSQL, and 31,551 every one missing from Iceberg. Both `stream_loss`
checks failed ("44350 rows dropped by the watermark (allowed 0)"), and
`ops.latest_quality` holds both as `fail`. The job logged
`stream.skew_bound_unsafe` at start. What survived after the poison fits
the two-batch lag (the run does not record per-batch ids, so this is the
mechanism's reading, not a count by batch). PostgreSQL kept the 5,000 events
before the poison, the poison and 649 after it, about what the poison's own
2 s batch and the next one hold at 200 events/s. The lake triggers once a
minute, so the same two batches held 13,448 real events after the poison. The watermark ended at 03:24 the next day, 23 h less 10 min past the
clock, so this stack is now unusable for any wall-clock run (the
precondition below refuses it).

After the fix the poison is the only record not landed, in both stores. It
is counted as `future` (1 of 50,000 input rows, under the 0.1% threshold),
nothing is dropped, and both checks pass, with the rejected record in their
details. The watermark stayed 10 minutes behind the newest real event.
Each run took 250 s of producing plus about 2.5 min for the counts to
settle, on 4 vCPU with Trino running. Cost: $0.

### The M5 watermark precondition

Kept, with a tolerance tied to the bound. A fault run refuses a stack whose
landing table holds an event more than the skew bound ahead of the clock
(before: 60 s). The job can no longer get there on its own. The check stays
because a stack is state that outlives the code: a volume from before this
milestone, or a job run with a looser bound (`make bench-poison ARM=before`
leaves exactly that), would lose a whole run the same way. Refusing costs one
query, while recording that run would cost an hour.

## A dropped row is a loss only if the event is missing (M12)

The question: can the pipeline tell a late copy of an event that already
landed from an event that is really lost, and bring the lost one back? The
watermark counter M11 turned into the `stream_loss` check cannot. Spark counts
a row as dropped before the dedup can know it is a copy, so a redelivery
reads as loss, and a truly late event is never written anywhere.

`tributary stream reconcile` reads the topic itself, as a Spark batch through
the stream job's own parser. It stops at the end offsets of the warehouse
query's last recorded batch: a batch's progress report is posted after Spark
writes its commit, which is after the sink wrote every partition, so every
record below those offsets has either landed or been dropped for good. It
classes each record as landed, rejected (`malformed`, `future`) or missing
(valid, and its id not in `raw.recentchange`), writes the missing ones back
with the sink's own `COPY` + `ON CONFLICT` write, and records the pass and
every missing id in `ops.reconcile_runs` and `ops.reconcile_missing`.
`stream_loss` now fails on dropped rows in batches no pass has read, or on an
event a pass found missing that is still absent. With no pass on record it is
exactly the old check. The `raw_recentchange` DAG runs a pass from the first
uncovered batch when there is one.

`make bench-late ARM=before|after` ran each arm on a fresh Compose stack
(`make down`, then `make up-lakehouse`: both sinks and Trino; 4 vCPU, 15 GB):

1. The committed sample through the platform's producer at 500 events/s,
   waited on until the warehouse query had committed every offset in the
   topic and held it for 20 s; then `tributary warehouse refresh`, so
   `marts.fct_edits` exists and every later build of it is incremental.
2. The sample again, the same way. The prediction for this replay was
   committed before any run (`bench/results/m12-late/prediction.json`,
   commit `e984a59`): once the first replay has settled, the watermark is the
   newest event time minus 10 minutes, and a copy at or below it is dropped
   as late. That is 35,554 of the 50,000; the dedup state catches the other
   14,446.
3. 50,000 bench events at 200 events/s, timed by the wall clock. 100 of them
   (every 400th from event 10,000) are stamped 1,500 s behind their send time,
   15 minutes behind the watermark. Event 20,200 is a record cut off mid-JSON,
   and event 30,200 is stamped an hour ahead. In `after`,
   `tributary stream reconcile --every 30` ran in its own container
   (`docker compose run`, the stream image, `local[3]`) from the start of the
   load to its end, and one more pass ran once the warehouse had committed
   everything.
4. The read-out: the batches' counters in `ops.stream_progress`, every
   expected id looked up in both stores, every pass with its missing ids,
   the `stream_loss` checks, `tributary warehouse refresh` again, the late
   ids in `marts.fct_edits`, `tributary sql "SELECT count(*) AS edits FROM
   marts.fct_edits"` with its provenance, and the per-hour PostgreSQL against
   Iceberg comparison (`tributary.lake.reconcile.compare_hours`) over every
   hour the run wrote.

Raw output is in `bench/results/m12-late/` (`run-before.json`,
`run-after.json`, the reconciler's log). Each arm took about 15 minutes.
Cost: $0, no model involved.

<!-- bench:late -->
| | Predicted | before | after |
|:--|:--|:--|:--|
| Second replay: rows dropped by the watermark (warehouse) | 35,554 | 35,554 | 35,554 |
| Second replay: rows dropped by the watermark (lake) | - | 33,224 | 33,635 |
| Second replay: sample events missing from PostgreSQL | 0 | 0 | 0 |
| `stream_loss_warehouse` after the second replay, before any pass | fail | fail | fail |
| Load: rows dropped by the watermark (warehouse) | 100 | 100 | 100 |
| Load: records rejected | 1 malformed, 1 future | 1 malformed, 1 future | 1 malformed, 1 future |
| Rejected records landed (both stores) | 0 | 0 | 0 |
| Reconciliation passes | - | none | 9 during, 1 after |
| In-flight events reported missing, over every pass | 0 | - | 0 |
| Late events found missing, over every pass | 100 | - | 100 |
| Late events missing from PostgreSQL at the end | 100 before, 0 after | 100 of 100 | 0 of 100 |
| Any other event missing from PostgreSQL | 0 | 0 | 0 |
| `stream_loss_warehouse` at the end | fail before, pass after | fail | pass |
| `stream_loss_lake` at the end | - | fail | fail |
| Late edits in `marts.fct_edits` after the next refresh | all, after | 0 of 0 | 51 of 51 |
| Provenance of `SELECT count(*) FROM marts.fct_edits` | - | `marts.fct_edits: rebuilt 2026-11-13T18:08:51.938850+00:00, quality fail (37 checks)` / `marts.fct_edits: failing checks: stream_loss_warehouse` | `marts.fct_edits: rebuilt 2026-11-13T17:55:54.045462+00:00, quality pass (37 checks)` |
| The final pass, whole topic | under 60 s | - | 9.9 s over 150,000 records (16.1 s with the container and JVM start) |
| Late events in Iceberg (never repaired) | - | 20 of 100 | 26 of 100 |
| Per-hour PostgreSQL against Iceberg | - | 2026-11-13T17: 0 missing, 20 extra | 2026-11-13T17: 74 missing, 0 extra |
<!-- /bench:late -->

Every pass of the `after` arm. Each read the whole topic up to the committed
offsets, so a pass during the load read the two replays as well.

<!-- bench:late-passes -->
| Pass | When | Records read | Landed | Rejected | Missing (late / other) | Written back | Left missing | Seconds |
|--:|:--|--:|--:|--:|:--|--:|--:|--:|
| 1 | during the load | 100,241 | 100,241 | 0 | 0 / 0 | 0 | 0 | 10.0 |
| 2 | during the load | 106,244 | 106,244 | 0 | 0 / 0 | 0 | 0 | 3.2 |
| 3 | during the load | 112,241 | 112,235 | 0 | 6 / 0 | 6 | 0 | 3.2 |
| 4 | during the load | 118,240 | 118,225 | 0 | 15 / 0 | 15 | 0 | 2.3 |
| 5 | during the load | 124,242 | 124,226 | 1 | 15 / 0 | 15 | 0 | 3.4 |
| 6 | during the load | 130,241 | 130,224 | 2 | 15 / 0 | 15 | 0 | 3.2 |
| 7 | during the load | 136,244 | 136,227 | 2 | 15 / 0 | 15 | 0 | 2.7 |
| 8 | during the load | 142,240 | 142,223 | 2 | 15 / 0 | 15 | 0 | 3.3 |
| 9 | during the load | 148,241 | 148,224 | 2 | 15 / 0 | 15 | 0 | 2.4 |
| 10 | after the load | 150,000 | 149,994 | 2 | 4 / 0 | 4 | 0 | 9.9 |
<!-- /bench:late-passes -->

### The hypotheses, answered

| Hypothesis (PLAN.md, Evolution round 2) | Result |
|---|---|
| A second replay of the settled sample drops exactly 35,554 rows and loses none | **Yes**, in both arms: 35,554 rows dropped by the warehouse query's watermark, as predicted from the sample, and all 50,000 sample events still in PostgreSQL. |
| That replay turns today's check red, and a reconciliation makes the same run green | **Yes.** After the replay `stream_loss_warehouse` failed in both arms ("35554 rows dropped by the watermark"). At the end it is still red in `before` (35,654 uncovered) and green in `after`: every one of the 35,654 dropped rows is in a batch a pass read, and nothing a pass found is still missing. |
| No pass reports an in-flight event as missing (0 false positives over every pass) | **Yes**: 0 over 10 passes, 9 of them while events were arriving at 200/s. Every id any pass reported missing is one of the 100 late events, and each was reported by exactly one pass (the next one found it landed). |
| The two rejected records are counted as rejected and not brought back | **Yes**: every pass that read them counted 1 `malformed` and 1 `future`, the batch counters agree, and neither is in PostgreSQL or Iceberg in either arm. |
| The 100 late events: missing before, in PostgreSQL after (a demonstration: an anti-join finds what it is built to find) | 100 of 100 missing at the end of `before`. In `after`, 0 missing: 96 were written back by the passes during the load, the last 4 by the pass after it. |
| A recovered event reaches the mart | **Yes**: 51 of the 100 late events are edits (`edit` or `new`, what `fct_edits` keeps), and all 51 are in `marts.fct_edits` after the next refresh, which built it incrementally on arrival time. In `before`, 0. |
| One pass over the 50,000-event topic takes under 60 s | **Yes, with room**: the final pass read the whole topic, 150,000 records (both replays and the load), in 9.9 s from planning to its record, 16.1 s including the container and the JVM. The passes during the load took 2.3 to 10.0 s over 100,000 to 148,000 records; the first pass in a JVM is the slow one. |

What the provenance says. In `before`, the answer to `SELECT count(*) AS
edits FROM marts.fct_edits` carries "quality fail" and the warning
"failing checks: stream_loss_warehouse", although no sample event was
lost; 100 bench events were. In `after`, the same answer is "quality pass".

### What is left: the lake

A pass repairs PostgreSQL only. Writing back into Iceberg would be a batch
append beside the streaming lake query, a second writer on the table that
needs its own fault runs (PLAN.md rejected it for this milestone). So the
lake keeps its losses, and two things report them:

- `stream_loss_lake` stays a counter: a pass judges PostgreSQL, so it covers
  nothing for the lake, and the check failed at the end of both arms. The
  lake's own second-replay drops differ from the warehouse's (33,224 and
  33,635 against 35,554 in both): it triggers once a minute, so its
  watermark at any offset is not the warehouse's.
- The per-hour reconciliation names the gap. The lake's slower trigger also
  let some late events through (its watermark lagged further): 20 of the 100
  in `before`, 26 in `after`. So in `before` the hour holding them shows 20
  ids in the lake that PostgreSQL lacks, and in `after`, once PostgreSQL has
  all 100, it shows the 74 the lake lacks. The hourly DAG compares closed
  hours only, and the run ended before the watermark closed that hour; the
  comparison here is the same function over every hour the run wrote.

### What the reconciliation cannot do

- **Kafka's retention bounds it.** A pass can only read what the broker
  still holds (here the topic's default, 7 days). Once retention has deleted
  the offsets a batch with drops read, no pass can cover that batch, and the
  check stays red until the batch leaves the 24 h window. A pass started
  from `uncovered` leaves such batches out, and the DAG then starts no
  Spark for them. A check window longer than retention would be red for
  good, which is the honest answer: the drops can no longer be judged.
- **It judges what the warehouse committed, under the current query.** The
  bound is the current streaming query's offsets (a reset checkpoint is a
  new query id), and a benchmark's discarding sink records no offsets.
- **It recovers into one store.** See the lake above.

## An answer that says when it is probably wrong (M13)

The planner's own `confidence` field says "high" on 526 of 527 answerable
BIRD drafts, right or wrong, so it tells a reader nothing. M10 found that
one added sentence changes answers it was never about. This section asks
whether that instability is a signal: when two prompt variants at
temperature 0 return the same rows, is the answer more often right? The
idea came from looking at the BIRD runs, so they cannot test it. The 40
warehouse questions did not suggest it, and they test it here.

### Fixed before any model call

<!-- m13:prereg -->
**The graded answer.** The recorded plain pass on the 40 warehouse
questions: `tests/recordings/warehouse.jsonl`, `qwen2.5-coder:7b`
(digest `dae161e27b0e`), 25 / 40 right under the warehouse rule (rows as
a multiset, numbers to six decimals; [The 40 warehouse
questions](#the-40-warehouse-questions)). It is not re-run and its
verdicts do not change.

**The second pass.** The same model, value hints on and three worked
examples from the dbt docs (`TRIBUTARY_AGENT_VALUES=true
TRIBUTARY_AGENT_EXAMPLES=3`), temperature 0, seed 0, up to two repairs, as
the plain pass:
`TRIBUTARY_AGENT_VALUES=true TRIBUTARY_AGENT_EXAMPLES=3
TRIBUTARY_RECORDINGS_PATH=tests/recordings/warehouse-vf.jsonl make record
PROVIDER=ollama`. The executions of both passes and of the gold queries
are recorded with their value types
(`tests/recordings/warehouse-executions.jsonl.gz`), so `make eval-agree`
re-runs both passes offline. The plain pass was recorded on Ollama 0.40.6;
this machine has 0.40.16 with the same model digest. The second pass's own
score is reported. The product's defaults stay as they are, whatever it
is: M10's rule is what would change them, and 40 questions are too few for
it.

**Agreement.** A question is *agreed* when both passes answered (status
`answered`) and their rows are equal under BIRD's set rule (the same set
of rows, values compared with their database types,
`eval/bird/score.py::execution_match`). Anything else is *differed*: other
rows, an error, a block or a refusal in either pass.

**The leak check.** A worked example that repeats a gold query would make
the second pass agree mostly when the plain pass is right, and favour the
hypothesis. A unit test fails if any example in the dbt YAML asks one of
the 40 questions (also as a near paraphrase) or carries one of the 40 gold
queries (compared as parsed SQL with output aliases removed).

**The hypothesis.** The two passes agree on 14 to 24 of the 40 questions,
and the plain pass is right more often when they agree than when they
differ: one-sided Fisher exact test on the 2 × 2 table of agreement against
correctness, p < 0.05. With 25 right in 40, p < 0.05 needs at least this
many right among the agreed answers:

| Agreed | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Right among them, at least | 12 | 13 | 13 | 14 | 15 | 15 | 16 | 17 | 17 | 18 | 18 |

**The rule.** p < 0.05: the signal *transfers*. Otherwise, a gap under 15
points between the accuracy of agreed and of differed answers (or no
agreed or no differed answer) is reported as *not seen at n = 40*, not as
evidence that there is no signal; a gap of 15 points or more is
*inconclusive*, with the counts that would have been needed. Nothing else
counts as a result.

**The odds before the run.** BIRD's split, grading the plain pass and
rescaled to 25 right in 40 (78% right when agreed, 52% when not, 41%
agreed), is P(agreed | right) = 0.51 and P(agreed | wrong) = 0.24. Holding
the 25 / 15 split fixed and drawing agreement per question with those
rates, 100,000 runs (seed 13, `eval/agree_stats.py::simulate`) give
*transfers* 41.6%, *inconclusive* 35.6% and *not seen* 22.8%. With BIRD's
own unscaled rates (0.84 and 0.15) it would be 99.8% / 0.2% / 0.0%; with
agreement independent of correctness (0.41 for both), 2.8% / 13.2% / 84.0%.
n = 40 is small, and these odds say so before the result does.

**Beside it, not tests.** Two baselines, each with its own table, Fisher
p and coverage (the share of questions it marks as probably right):
answered without a repair, and the questions labelled `simple`. Agreement
beats a baseline only if its agreed answers are more accurate at a
coverage at least as large. The share of answers whose stated
`confidence` is "high" (expected at least 95%, as on BIRD). The same
table on per-difficulty rows, and on BIRD dev50 and holdout50 graded on
each pass (`full` 32 / 41 agreed against 6 / 59 differed; `full+v+f+a`
32 / 41 against 18 / 59), marked exploratory: those runs suggested the
idea. The model time the second pass adds per question.
<!-- /m13:prereg -->

### The second pass

`TRIBUTARY_OLLAMA_MODEL=qwen2.5-coder:7b TRIBUTARY_AGENT_VALUES=true
TRIBUTARY_AGENT_EXAMPLES=3
TRIBUTARY_RECORDINGS_PATH=tests/recordings/warehouse-vf.jsonl make record
PROVIDER=ollama`, on 2026-11-14: `qwen2.5-coder:7b` (digest
`dae161e27b0e`) on Ollama 0.40.16, run natively (not in Compose), whose
server used 2 threads on the 4 vCPU (AMD EPYC 7763, 15 GB), temperature 0,
seed 0. Wall time **56 min 36 s** for the 40 questions; 46 model calls,
135,167 input and 8,080 output tokens; **$0**. Raw:
[`eval/results/warehouse-ollama-qwen2.5-coder-7b-vf.json`](eval/results/warehouse-ollama-qwen2.5-coder-7b-vf.json),
recordings in `tests/recordings/warehouse-vf.jsonl`, the executions of
both passes and the gold queries in
`tests/recordings/warehouse-executions.jsonl.gz`.

Its own score, with the product's defaults left alone: **28 / 40**
(simple 11 / 12, moderate 14 / 20, challenging 3 / 8), against the plain
pass's 25 / 40: +6 −3 question by question, exact McNemar p = 0.51. It
answered 34, declared 4 unanswerable and failed 2 after its repairs; the
plain pass answered all 40. Turning value hints and examples on in the
product would take M10's kind of rule on more questions than these 40, so
the defaults stay off (the plan said so before the run).

**Added model time.** The second pass waited on the model for a median of
60.4 s per question (mean 84.9 s, 56.6 min in all), on top of the first
pass. The plain pass's recorded median, 37.9 s, was measured on Ollama
0.40.6 in Compose, so the two medians are not a like-for-like comparison of
the prompts; the second pass's own time is what `--second-pass` adds to each answer.

### The 40 warehouse questions: agreement against correctness

`make eval-agree` (offline: no model, no database; in CI) re-runs both
passes through the agent from their recordings and the recorded
executions, checks every replayed verdict against its recorded run
(status, correctness, attempts, SQL), and builds the tables below. Raw:
[`eval/results/agree.json`](eval/results/agree.json). Graded answer: the
plain pass.

<!-- agree:warehouse -->
| Questions | Agreed: plain pass right | Differed: plain pass right | Gap (points) | One-sided Fisher p | Under the rule |
|---|---:|---:|---:|---:|---|
| **All 40** | **22 / 25** | **3 / 15** | +68.0 | **2.7e-05** | **transfers** |
| simple | 9 / 10 | 0 / 2 | +90.0 | 0.045 | (not tested) |
| moderate | 11 / 13 | 2 / 7 | +56.0 | 0.022 | (not tested) |
| challenging | 2 / 2 | 1 / 6 | +83.3 | 0.107 | (not tested) |

| Signal marking an answer as probably right | Coverage | Marked: right | Not marked: right | Gap (points) | One-sided Fisher p |
|---|---:|---:|---:|---:|---:|
| The two passes agree | 62.5% | 22 / 25 | 3 / 15 | +68.0 | 2.7e-05 |
| Answered without a repair | 87.5% | 24 / 35 | 1 / 5 | +48.6 | 0.056 |
| Labelled `simple` | 30.0% | 9 / 12 | 16 / 28 | +17.9 | 0.241 |
<!-- /agree:warehouse -->

The per-difficulty rows and the baselines are context, not tests: only the
40-question row was registered.

**Under the rule: the signal transfers.** When the two passes return the
same rows, the plain pass is right 22 times in 25 (88%); when they do not,
3 times in 15 (20%). One-sided Fisher p = 2.7 × 10⁻⁵, far under 0.05; at
25 agreed it needed 19 right. The 15 differed questions split two ways: in
9, the second pass answered with other rows, and the plain pass was wrong
on all 9; in 6, the second pass refused or failed, and the plain pass was
right on 3 of those. Graded on the second pass instead, the split is
22 / 25 against 6 / 15 (p = 0.002).

**The hypotheses, answered.**

| Hypothesis (fixed before the run) | Result |
|---|---|
| The passes agree on 14 to 24 of the 40 | **No, by one:** 25 agreed |
| The plain pass is right more often when they agree, one-sided Fisher p < 0.05 | **Yes:** 22 / 25 against 3 / 15, p = 2.7 × 10⁻⁵ (*transfers*; the simulation gave this outcome 41.6% before the run) |
| The stated `confidence` is "high" on at least 95% of answers | **Yes, on all of them:** 40 / 40 in the plain pass, 34 / 34 answered in the second |

**The simulated odds, re-run.** The pre-registered block's odds (41.6%
*transfers*, 35.6% *inconclusive*, 22.8% *not seen*) do not come back
exactly from the call it names: `simulate([True] * 25 + [False] * 15,
0.51, 0.24)` (100,000 runs, seed 13) gives 42.0%, 35.6% and 22.5%, and the
unrounded rates (0.512, 0.241) give 42.2%, 35.4% and 22.3%. The block
stays as committed; the difference is under half a point and changes no
reading of it. A unit test now pins the re-run figures, so the stated
odds and the code cannot drift apart again.

**Against the baselines.** Answered without a repair marks more questions
(88% against 62.5%) and is right on fewer of them (69% against 88%), so by
the stated rule agreement does not beat it: agreement is not more accurate
at a coverage at least as large; it is a different trade-off. It leaves
out 10 more questions, of which the plain pass got only 2 right. It does
beat the `simple` label, which marks 30% of the questions at 75%.

**What agreement cannot see.** The three agreed answers that were wrong are
the same query in both passes, up to aliases, and the same errors the M3
failure analysis found: `bot-edits-total` sums a per-editor table where
the question counts edits (`agg_editor_activity WHERE is_bot`, 10,168
against 10,099), `edits-by-project-family` sums `dim_wiki.edits` after
joining `fct_edits` (a fan-out), and `english-language-edits-by-family`
sums bytes where it should count edits. Value hints and examples did not
change how the model reads those tables, so both prompts made the same
mistake and agreed on it. Agreement flags an answer that one prompt change
moves; it cannot flag an error that every prompt shares.

### BIRD, graded on each pass (exploratory)

The same split on the recorded BIRD runs (official track, `full` against
`full+v+f+a`, both at temperature 0), from the committed reports and
executions. These runs suggested the idea, so they test nothing; they show
the warehouse result is not one dataset's accident.

<!-- agree:bird -->
| Subset (official track) | Graded on | Agreed: right | Differed: right | Gap (points) | One-sided Fisher p | Agreed but wrong |
|---|---|---:|---:|---:|---:|---|
| dev50 | `full` | 18 / 21 | 3 / 29 | +75.4 | 7.4e-08 | 37, 200, 405 |
| dev50 | `full+v+f+a` | 18 / 21 | 8 / 29 | +58.1 | 5.0e-05 | (same questions) |
| holdout50 | `full` | 14 / 20 | 3 / 30 | +60.0 | 1.7e-05 | 231, 469, 963, 1145, 1241, 1281 |
| holdout50 | `full+v+f+a` | 14 / 20 | 10 / 30 | +36.7 | 0.012 | (same questions) |
| both, n = 100 | `full` | 32 / 41 | 6 / 59 | +67.9 | 2.9e-12 | 37, 200, 405, 231, 469, 963, 1145, 1241, 1281 |
| both, n = 100 | `full+v+f+a` | 32 / 41 | 18 / 59 | +47.5 | 2.5e-06 | (same questions) |
<!-- /agree:bird -->

The nine agreed answers that `full` got wrong are listed by question id.
Graded on `full+v+f+a` the split is weaker (18 / 59 right when differed)
because that variant is the stronger of the two and is often right where
`full` failed, which is the same pattern as the warehouse second pass.


## To measure

| What | Command | Milestone | Cost |
|---|---|---|---|
| The 40 warehouse questions on Claude, recorded so the fake provider replays them (M3 is done on the `qwen2.5-coder:7b` recordings; this adds the Claude column) | `ANTHROPIC_API_KEY=... TRIBUTARY_RECORDINGS_PATH=tests/recordings/warehouse.jsonl make record PROVIDER=anthropic` (default model `claude-opus-5-5`) | M3 | est. under $1 on Opus 5.5 (about 46 calls with a cached ~3k-token prefix), a few cents on `TRIBUTARY_MODEL=claude-haiku-5-5` |
| BIRD dev50 on Claude Haiku 5.5, both variants and tracks, recorded so CI replays it | `docker compose up -d postgres && make bird && ANTHROPIC_API_KEY=... make eval PROVIDER=anthropic MODEL=claude-haiku-5-5 RECORD=1` | M4 | see [Cost](#cost): estimated from this run's measured tokens |
| BIRD, all 500 questions on Claude Haiku 5.5 | `... make eval PROVIDER=anthropic MODEL=claude-haiku-5-5 SUBSET=all RECORD=1` | M7 | see [Cost](#cost) |
| BIRD dev50 on Claude Opus 5.5 | `... make eval PROVIDER=anthropic MODEL=claude-opus-5-5 RECORD=1` | M7 | see [Cost](#cost) |
| BIRD, all 500 questions on the local model (about 13 h of CPU at this run's pace) | `make bird && make eval PROVIDER=ollama MODEL=qwen2.5-coder:7b SUBSET=all RECORD=1` | M7 | $0 |
| The rest of the M7 grid on the local model: the leave-one-out cells `full+v+a`, `full+f+a`, `full+v+f`, the vote `full+v+f+a+n3`, and `bm25-k4+v+f+a`, so each feature's share of the gain is measured rather than inferred | `make eval PROVIDER=ollama MODEL=qwen2.5-coder:7b VARIANTS=full,bm25-k4,full+a,full+v+f+a,full+v+a,full+f+a,full+v+f,full+v+f+a+n3,bm25-k4+v+f+a RECORD=1 RESUME=1` (the recorded cells replay; about 75 min of CPU per new cell here; the vote is several hours, see [the pilot](#why-the-default-is-what-it-is)) | M7 | $0 |
| The `core` linker in the grid, once `link_core` is written | `make eval PROVIDER=ollama MODEL=qwen2.5-coder:7b VARIANTS=full,bm25-k4,full+a,full+v+f+a,core-k4+v+f+a RECORD=1 RESUME=1`, then `make eval-link` | M7 | $0 |
| The M7 grid on Claude Haiku 5.5 | `ANTHROPIC_API_KEY=... make eval PROVIDER=anthropic MODEL=claude-haiku-5-5 VARIANTS=<as above> RECORD=1` | M7 | see [Cost](#cost): the four recorded cells, estimated from their measured tokens |
| The provider against the real API: one structured-output call with usage and cost | `ANTHROPIC_API_KEY=... make test-live` (or the `Live` workflow, run by hand) | M8 | under $0.01 on `claude-haiku-5-5` (one call, about 100 input and 50 output tokens) |
| The `Guard differential` workflow on GitHub (M9 ran it locally only) | `gh workflow run guard-diff.yml --ref dev/m9`, then compare the artifact with `eval/results/guard-diff/after.json` | M9 | $0 (the repository's Actions minutes: about 25 min) |
| CI on GitHub for this release: `CI`, `Integration` and `Warehouse` green on the pull request, and both release workflows' dry runs green on the branch | push the branch, open the pull request, then `gh workflow run release.yml --ref dev/m8` and `gh workflow run images.yml --ref dev/m8` | M8 | $0 (the repository's Actions minutes: a few minutes for the gate, up to 35 for Integration, about 10 for the images) |
