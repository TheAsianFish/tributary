"""``make bench-query``: ten queries on PostgreSQL, Trino over PostgreSQL, Trino over Iceberg.

For each scale (``--scales 1m,20m``):

1. **Load.** ``tributary bench load --rows N`` in the stream image writes the
   scaled sample to ``bench.events_<N>`` (PostgreSQL) and to the Iceberg tables
   ``events_<N>`` (one file per hour) and ``events_<N>_stream`` (the streaming
   sink's uncompacted layout) from one DataFrame. Trino then runs ``optimize``
   on the hourly table, as the ``lake_maintenance`` DAG would, and copies it
   into ``events_<N>_flat`` with no partitioning (a CTAS).
2. **Warm.** Per engine and query: one run whose result is compared with
   PostgreSQL's (a mismatch or error voids the cell), then ``--runs`` timed runs.
   Time is wall clock in this process from sending the statement to holding
   the last row; Trino's own statistics (splits, physical input bytes, CPU)
   are kept for every run.
3. **Pruning.** The window queries on the three Iceberg layouts, timed the
   same way, plus one ``EXPLAIN ANALYZE`` each for the splits and rows the
   scan read, and the table's ``$files`` metadata for how many files the
   window's bounds can match. PostgreSQL's ``EXPLAIN (ANALYZE, BUFFERS)``
   for the same queries gives its blocks read beside them.
4. **Federated.** Lake events joined with ``marts.dim_wiki`` through Trino,
   checked against the same join inside PostgreSQL.
5. **Cold.** Per engine and query: stop the engine's containers, drop the
   host's page cache (``/proc/sys/vm/drop_caches`` through ``sudo -n``, when
   allowed; recorded either way), start them, run the query once.

Each unit writes one JSON file under ``bench/results/m6-query/``; a rerun
skips units whose file exists. PostgreSQL sessions use two settings an
analyst would change first (``max_parallel_workers_per_gather = 4`` so it can
use the four cores Trino uses, ``work_mem = 64MB``); everything else is the
image's default, and both are recorded.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import structlog
import trino.dbapi
import trino.exceptions

from bench import queries as Q  # noqa: N812
from bench.stack import resources
from tributary.config import Settings, get_settings
from tributary.logging import configure_logging
from tributary.querybench.generate import FLAT_SUFFIX, STREAM_SUFFIX, read_base, scaled_id

log = structlog.get_logger("bench.query")

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "bench/results/m6-query"
COMPOSE = ("docker", "compose", "-f", "compose.yaml", "-f", "bench/compose.query.yaml")
PROFILE = ("--profile", "lakehouse")
ENGINE_SERVICES = {
    "postgres": ("postgres",),
    "trino-postgres": ("postgres", "trino"),
    "trino-iceberg": ("minio", "trino"),
}
"""Containers each engine reads through, in start order (a cold run restarts them)."""
LAYOUTS = {"partitioned": "", "unpartitioned": FLAT_SUFFIX, "uncompacted": STREAM_SUFFIX}
PG_SESSION = {
    "TimeZone": "UTC",
    "max_parallel_workers_per_gather": "4",
    "work_mem": "64MB",
    "statement_timeout": "15min",
}
TRINO_SESSION = {"query_max_run_time": "15m"}
TRINO_STATS = (
    "physicalInputBytes",
    "processedRows",
    "processedBytes",
    "totalSplits",
    "cpuTimeMillis",
    "wallTimeMillis",
    "elapsedTimeMillis",
    "queuedTimeMillis",
    "peakMemoryBytes",
)
_SCAN_STATS = re.compile(
    r"Input: (?P<rows>\d+) rows \((?P<input>[^)]+)\).*?"
    r"Physical input: (?P<physical>[\d.]+[kMGTP]?B).*?Splits: (?P<splits>\d+)"
)
_UNITS = {"B": 1, "kB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "PB": 1e15}


def parse_scale(text: str) -> int:
    """``1m`` → 1,000,000; ``20m`` → 20,000,000; ``250k`` → 250,000; digits as they are."""
    text = text.strip().lower().replace("_", "")
    for suffix, factor in (("m", 1_000_000), ("k", 1_000)):
        if text.endswith(suffix):
            return int(float(text[:-1]) * factor)
    return int(text)


def nearest_rank(values: Sequence[float], q: float) -> float:
    """The nearest-rank quantile: with 10 runs, p95 is the slowest run."""
    ordered = sorted(values)
    rank = max(1, -(-round(q * 1000) * len(ordered) // 1000))  # ceil(q * n), exact
    return ordered[rank - 1]


def summarise(seconds: Sequence[float]) -> dict[str, float]:
    return {
        "median": statistics.median(seconds),
        "p95": nearest_rank(seconds, 0.95),
        "min": min(seconds),
        "max": max(seconds),
    }


def parse_size(text: str) -> float:
    match = re.fullmatch(r"([\d.]+)([kMGTP]?B)", text.strip())
    if match is None:
        raise ValueError(f"not a Trino data size: {text!r}")
    return float(match[1]) * _UNITS[match[2]]


def scan_stats(plan: str, table: str) -> dict[str, float]:
    """Rows, input size, physical input and splits of the scan of ``table`` in an EXPLAIN ANALYZE.

    ``table`` is ``schema.name``; Trino prints it as ``iceberg:schema.name$data@<snapshot>``.
    """
    marker = re.search(rf"table = \w+:{re.escape(table)}(\$data)?[@\s,\]]", plan)
    if marker is None:
        raise ValueError(f"no scan of {table} in the plan")
    stats = _SCAN_STATS.search(plan, marker.end())
    if stats is None:
        raise ValueError(f"no Input/Physical input/Splits after the scan of {table}")
    return {
        "rows": int(stats["rows"]),
        "input_bytes": parse_size(stats["input"]),
        "physical_input_bytes": parse_size(stats["physical"]),
        "splits": int(stats["splits"]),
    }


def pg_buffers(plan: Mapping[str, Any]) -> dict[str, Any]:
    """Blocks the top node of an ``EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`` touched.

    A node's buffer counts include its children's, so the top node is the
    whole query. Also the scan node types, to show whether the BRIN index ran.
    """
    top = plan["Plan"]
    scans: list[str] = []

    def walk(node: Mapping[str, Any]) -> None:
        kind = str(node.get("Node Type", ""))
        if "Scan" in kind:
            index = node.get("Index Name")
            scans.append(f"{kind} using {index}" if index else kind)
        for child in node.get("Plans", []):
            walk(child)

    walk(top)
    hit = int(top.get("Shared Hit Blocks", 0))
    read = int(top.get("Shared Read Blocks", 0))
    return {
        "shared_hit_blocks": hit,
        "shared_read_blocks": read,
        "blocks": hit + read,
        "scans": scans,
    }


# ------------------------------------------------------------------ the stack


def compose(*args: str, timeout: float = 900, check: bool = True) -> str:
    result = subprocess.run(
        [*COMPOSE, *PROFILE, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} failed:\n{result.stderr[-4000:]}")
    return result.stdout


def prepare_stack() -> None:
    """The engines under the benchmark's memory limits; the stream job and producer stopped."""
    compose("up", "-d", "--wait", "postgres", "minio", "lake-init", "trino")
    compose("stop", "stream", "producer", check=False)


def can_drop_caches() -> bool:
    return subprocess.run(["sudo", "-n", "true"], capture_output=True, check=False).returncode == 0


def drop_caches() -> None:
    subprocess.run(["sync"], check=True)
    subprocess.run(
        ["sudo", "-n", "tee", "/proc/sys/vm/drop_caches"],
        input=b"3\n",
        stdout=subprocess.DEVNULL,
        check=True,
    )


def restart_cold(services: Sequence[str], *, drop: bool) -> float:
    """Stop ``services``, drop the page cache if allowed, start them; returns seconds taken."""
    start = time.monotonic()
    compose("stop", *reversed(services))
    if drop:
        drop_caches()
    compose("up", "-d", "--wait", "--no-recreate", *services)
    return time.monotonic() - start


# ------------------------------------------------------------------ engines


class Engines:
    """One PostgreSQL session and one Trino connection; ``run`` times a statement."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.pg = psycopg.connect(settings.pg_conninfo, autocommit=True)
        for name, value in PG_SESSION.items():
            self.pg.execute(f"SET {name} = '{value}'")
        self.trino = trino.dbapi.Connection(
            host=settings.trino_host,
            port=settings.trino_port,
            user="bench",
            http_scheme="http",
            source="tributary-bench",
            timezone="UTC",
            session_properties=dict(TRINO_SESSION),
        )

    def close(self) -> None:
        self.pg.close()
        self.trino.close()  # type: ignore[no-untyped-call]  # the trino client is untyped

    def run(self, engine: str, sql: str) -> tuple[list[list[Q.Value]], float, dict[str, Any]]:
        """(normalised rows, wall seconds, engine statistics)."""
        if engine == "postgres":
            with self.pg.cursor() as cur:
                start = time.perf_counter()
                cur.execute(sql.encode())  # the bench's own SQL text, not a literal
                rows = cur.fetchall()
                elapsed = time.perf_counter() - start
            return Q.normalise_rows(rows), elapsed, {}
        cur = self.trino.cursor()
        try:
            start = time.perf_counter()
            cur.execute(sql)
            rows = cur.fetchall()
            elapsed = time.perf_counter() - start
            stats = {k: cur.stats.get(k) for k in TRINO_STATS}
            stats["queryId"] = cur.query_id
        finally:
            cur.close()
        return Q.normalise_rows(rows), elapsed, stats

    def fetch(self, engine: str, sql: str) -> list[list[Q.Value]]:
        return self.run(engine, sql)[0]

    def pg_explain(self, sql: str) -> dict[str, Any]:
        with self.pg.cursor() as cur:
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}".encode())
            row = cur.fetchone()
        assert row is not None
        plan: dict[str, Any] = row[0][0]
        return plan


def wait_for_trino(engines: Engines, timeout: float = 180) -> None:
    """Block until Trino accepts queries (it reports healthy before its catalogs load)."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            engines.fetch("trino-iceberg", "SELECT count(*) FROM system.metadata.catalogs")
            return
        except (
            trino.exceptions.TrinoQueryError,
            trino.exceptions.HttpError,
            trino.exceptions.TrinoConnectionError,
            OSError,
        ):
            if time.monotonic() > deadline:
                raise
            time.sleep(2)


def measure(
    engines: Engines,
    engine: str,
    sql: str,
    runs: int,
    reference: Sequence[Sequence[Q.Value]] | None,
) -> dict[str, Any]:
    """One discarded run checked against ``reference``, then ``runs`` timed runs.

    PostgreSQL is the reference, so it alone runs with ``reference=None``; any
    other engine without one (PostgreSQL failed) is void, never timed unchecked.
    Every timed run's rows must equal the first run's.
    """
    cell: dict[str, Any] = {"engine": engine, "sql": sql}
    if reference is None and engine != "postgres":
        cell["mismatch"] = "no PostgreSQL result to check against"
        return cell
    try:
        rows, first, _ = engines.run(engine, sql)
    except Exception as exc:  # any engine failure voids the cell, with its message
        cell["error"] = f"{type(exc).__name__}: {exc}"[:2000]
        log.warning("bench.query.error", engine=engine, error=cell["error"][:300])
        return cell
    cell["warmup_seconds"] = first
    cell["result_rows"] = len(rows)
    cell["result"] = rows if len(rows) <= 300 else rows[:300]
    mismatch = Q.compare(reference, rows) if reference is not None else None
    if mismatch is not None:
        cell["mismatch"] = mismatch
        log.warning("bench.query.mismatch", engine=engine, mismatch=mismatch[:300])
        return cell
    seconds: list[float] = []
    stats: list[dict[str, Any]] = []
    for _ in range(runs):
        got, elapsed, run_stats = engines.run(engine, sql)
        drift = Q.compare(rows, got)
        if drift is not None:
            cell["mismatch"] = f"a timed run differs from the first: {drift}"
            return cell
        seconds.append(elapsed)
        stats.append(run_stats)
    cell["seconds"] = seconds
    cell["summary"] = summarise(seconds)
    if any(stats):
        cell["stats"] = stats
    return cell


# ------------------------------------------------------------------ units of work


def _write(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=1, sort_keys=True, default=str) + "\n")
    tmp.replace(path)
    log.info("bench.query.wrote", path=str(path))


def _read(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text())
    return data


def table_files(engines: Engines, table: str) -> dict[str, Any]:
    t = table.split(".")
    files = f'iceberg.{t[1]}."{t[2]}$files"'
    snapshots = f'iceberg.{t[1]}."{t[2]}$snapshots"'
    row = engines.fetch(
        "trino-iceberg",
        f"SELECT count(*), sum(file_size_in_bytes), sum(record_count), min(file_size_in_bytes),"
        f" approx_percentile(file_size_in_bytes, 0.5), max(file_size_in_bytes) FROM {files}",
    )[0]
    snaps = engines.fetch("trino-iceberg", f"SELECT count(*) FROM {snapshots}")[0][0]
    keys = ("files", "bytes", "rows", "min_file_bytes", "median_file_bytes", "max_file_bytes")
    return {**dict(zip(keys, row, strict=True)), "snapshots": snaps}


def pg_sizes(engines: Engines, name: str) -> dict[str, Any]:
    row = engines.fetch(
        "postgres",
        f"SELECT pg_relation_size('bench.{name}'), pg_table_size('bench.{name}'),"
        f" pg_relation_size('bench.{name}_pkey'), pg_relation_size('bench.{name}_event_ts_brin'),"
        f" (SELECT reltuples::bigint FROM pg_class WHERE oid = 'bench.{name}'::regclass)",
    )[0]
    keys = ("heap_bytes", "table_bytes", "pkey_bytes", "brin_bytes", "reltuples")
    return dict(zip(keys, row, strict=True))


CTAS_SESSION = {
    "task_max_writer_count": "1",
    "task_scale_writers_enabled": "false",
    "scale_writers": "false",
}
"""One Parquet writer for the unpartitioned copy: at 20M rows Trino's default four
writers buffer more than its 739 MB per-node query memory and the CTAS fails."""


def ctas_writer(settings: Settings, sql: str) -> None:
    conn = trino.dbapi.Connection(
        host=settings.trino_host,
        port=settings.trino_port,
        user="bench",
        timezone="UTC",
        session_properties=dict(CTAS_SESSION),
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchall()
    finally:
        conn.close()  # type: ignore[no-untyped-call]  # the trino client is untyped


def load_unit(settings: Settings, rows: int, name: str, path: Path) -> dict[str, Any]:
    """Generate the data in the stream image, then optimize and copy it flat through Trino.

    The generator's report is saved on its own (``generate-<label>.json``) as
    soon as it returns, so a failure in the Trino steps does not cost the load.
    """
    if path.exists():
        return _read(path)
    generated = path.with_name(path.name.replace("load-", "generate-", 1))
    if generated.exists():
        saved = _read(generated)
        report, generate_seconds = saved["report"], saved["seconds"]
    else:
        start = time.monotonic()
        out = compose(
            "run",
            "--rm",
            "--no-deps",
            "-e",
            "TRIBUTARY_SPARK_MASTER=local[4]",
            "stream",
            "bench",
            "load",
            "--rows",
            str(rows),
            timeout=4 * 3600,
        )
        generate_seconds = time.monotonic() - start
        report = json.loads(out.strip().splitlines()[-1])
        _write(generated, {"report": report, "seconds": generate_seconds})
    engines = Engines(settings)
    try:
        table = f"iceberg.bench.{name}"
        t0 = time.monotonic()
        engines.fetch("trino-iceberg", f"ALTER TABLE {table} EXECUTE optimize")
        optimize_seconds = time.monotonic() - t0
        engines.fetch("trino-iceberg", f"DROP TABLE IF EXISTS {table}{FLAT_SUFFIX}")
        t0 = time.monotonic()
        # A fresh JVM for the copy: after optimize at 20M rows the same CTAS filled
        # Trino's heap (GC thrash, then exit) although it fits on a fresh one.
        compose("restart", "trino")
        compose("up", "-d", "--wait", "--no-recreate", "trino")
        wait_for_trino_cold(settings)
        ctas_writer(settings, f"CREATE TABLE {table}{FLAT_SUFFIX} AS SELECT * FROM {table}")
        ctas_seconds = time.monotonic() - t0
        layouts = {
            layout: table_files(engines, f"{table}{suffix}") for layout, suffix in LAYOUTS.items()
        }
        sizes = pg_sizes(engines, name)
    finally:
        engines.close()
    data = {
        "rows": rows,
        "name": name,
        "generator": report,
        "generate_seconds": generate_seconds,
        "optimize_seconds": optimize_seconds,
        "flat_ctas_seconds": ctas_seconds,
        "iceberg": layouts,
        "postgres": sizes,
    }
    _write(path, data)
    return data


def window_for(settings: Settings, load: Mapping[str, Any]) -> Q.Window:
    """The middle hour of the data, and the event at the middle offset as the lookup."""
    gen = load["generator"]
    first = datetime.fromisoformat(gen["first_event_ts"])
    last = datetime.fromisoformat(gen["last_event_ts"])
    base = read_base(settings.sample_path)
    offset = int(load["rows"]) // 2
    copy, seq = divmod(offset, len(base.rows))
    return Q.Window(Q.middle_hour(first, last), scaled_id(copy, str(base.rows[seq][1])))


def warm_unit(
    engines: Engines, name: str, window: Q.Window, runs: int, path: Path
) -> dict[str, Any]:
    if path.exists():
        return _read(path)
    cells: dict[str, dict[str, Any]] = {}
    for query in Q.load_queries():
        reference: list[list[Q.Value]] | None = None
        for engine in Q.ENGINES:
            sql = Q.render(query, engine, Q.table_ref(engine, name), window)
            cell = measure(engines, engine, sql, runs, reference)
            if engine == "postgres":
                reference = cell.get("result") if "error" not in cell else None
                if reference is not None and cell.get("result_rows", 0) > 300:
                    raise RuntimeError(f"{query.name}: result too large to keep as the reference")
            cells[f"{query.name}/{engine}"] = cell
            log.info(
                "bench.query.cell",
                query=query.name,
                engine=engine,
                median=cell.get("summary", {}).get("median"),
            )
    data = {"name": name, "runs": runs, "window": _window_json(window), "cells": cells}
    _write(path, data)
    return data


def _window_json(window: Q.Window) -> dict[str, str]:
    return {
        "hour_start": window.hour_start.isoformat(),
        "hour_end": window.hour_end.isoformat(),
        "six_hours_end": window.six_hours_end.isoformat(),
        "event_id": window.event_id,
    }


def files_in_window(engines: Engines, table: str, start: datetime, end: datetime) -> dict[str, Any]:
    """Data files whose ``event_ts`` bounds overlap [start, end): what the metadata cannot skip."""
    t = table.split(".")
    files = f'iceberg.{t[1]}."{t[2]}$files"'
    bound = "from_iso8601_timestamp(json_extract_scalar(readable_metrics, '$.event_ts.{}'))"
    row = engines.fetch(
        "trino-iceberg",
        f"SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM {files}"
        f" WHERE {bound.format('upper_bound')} >= from_iso8601_timestamp('{start.isoformat()}')"
        f" AND {bound.format('lower_bound')} < from_iso8601_timestamp('{end.isoformat()}')",
    )[0]
    return {"files": row[0], "bytes": row[1]}


def pruning_unit(
    engines: Engines, name: str, window: Q.Window, runs: int, path: Path
) -> dict[str, Any]:
    if path.exists():
        return _read(path)
    cells: dict[str, dict[str, Any]] = {}
    for query in [q for q in Q.load_queries() if q.kind == "window"]:
        end = window.six_hours_end if "six_hours_end" in query.postgres else window.hour_end
        pg_sql = Q.render(query, "postgres", Q.table_ref("postgres", name), window)
        reference = engines.fetch("postgres", pg_sql)
        explain = engines.pg_explain(pg_sql)
        cells[f"{query.name}/postgres"] = {
            "sql": pg_sql,
            "buffers": pg_buffers(explain),
            "execution_ms": explain.get("Execution Time"),
            "plan": explain,
        }
        for layout, suffix in LAYOUTS.items():
            table = Q.table_ref("trino-iceberg", name, layout=suffix)
            sql = Q.render(query, "trino-iceberg", table, window)
            cell = measure(engines, "trino-iceberg", sql, runs, reference)
            plan = str(engines.fetch("trino-iceberg", f"EXPLAIN ANALYZE {sql}")[0][0])
            cell["scan"] = scan_stats(plan, table.split(".", 1)[1])
            cell["plan"] = plan
            cell["window_files"] = files_in_window(engines, table, window.hour_start, end)
            cell["layout"] = layout
            cells[f"{query.name}/{layout}"] = cell
            log.info(
                "bench.query.pruning",
                query=query.name,
                layout=layout,
                median=cell.get("summary", {}).get("median"),
                splits=cell["scan"]["splits"],
            )
    data = {"name": name, "runs": runs, "window": _window_json(window), "cells": cells}
    _write(path, data)
    return data


def federated_unit(
    engines: Engines, name: str, window: Q.Window, runs: int, path: Path
) -> dict[str, Any]:
    if path.exists():
        return _read(path)
    query = Q.load_federated()
    pg_sql = Q.render(query, "postgres", Q.table_ref("postgres", name), window)
    reference = measure(engines, "postgres", pg_sql, runs, None)
    sql = Q.render(query, "trino-iceberg", Q.table_ref("trino-iceberg", name), window)
    cell = measure(engines, "trino-iceberg", sql, runs, reference.get("result"))
    plan = str(engines.fetch("trino-iceberg", f"EXPLAIN {sql}")[0][0])
    data = {
        "name": name,
        "runs": runs,
        "window": _window_json(window),
        "postgres": reference,
        "trino": cell,
        "plan": plan,
    }
    _write(path, data)
    return data


def cold_unit(
    settings: Settings, name: str, window: Q.Window, warm: Mapping[str, Any], path: Path
) -> dict[str, Any]:
    """Each query once per engine after restarting the engine's containers."""
    if path.exists():
        return _read(path)
    drop = can_drop_caches()
    cells: dict[str, dict[str, Any]] = {}
    for engine in Q.ENGINES:
        for query in Q.load_queries():
            reference = warm["cells"][f"{query.name}/postgres"].get("result")
            restart = restart_cold(ENGINE_SERVICES[engine], drop=drop)
            engines = Engines(settings)
            try:
                if engine != "postgres":
                    wait_for_trino_cold(settings)
                sql = Q.render(query, engine, Q.table_ref(engine, name), window)
                cell = measure_once(engines, engine, sql, reference)
            finally:
                engines.close()
            cell["restart_seconds"] = restart
            cells[f"{query.name}/{engine}"] = cell
            log.info("bench.query.cold", query=query.name, engine=engine, s=cell.get("seconds"))
    data = {
        "name": name,
        "window": _window_json(window),
        "page_cache_dropped": drop,
        "method": (
            "containers stopped, host page cache dropped, containers started"
            if drop
            else "containers stopped and started; the host page cache was not dropped"
        ),
        "cells": cells,
    }
    _write(path, data)
    return data


def wait_for_trino_cold(settings: Settings, timeout: float = 180) -> None:
    """Wait until Trino has a worker, without touching the catalogs under test."""
    deadline = time.monotonic() + timeout
    while True:
        conn = trino.dbapi.Connection(
            host=settings.trino_host, port=settings.trino_port, user="bench", timezone="UTC"
        )
        try:
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'")
            if cur.fetchall()[0][0] >= 1:
                return
        except (
            trino.exceptions.TrinoQueryError,
            trino.exceptions.HttpError,
            trino.exceptions.TrinoConnectionError,
            OSError,
        ):
            pass
        finally:
            conn.close()  # type: ignore[no-untyped-call]  # the trino client is untyped
        if time.monotonic() > deadline:
            raise TimeoutError("Trino has no active node")
        time.sleep(1)


def measure_once(
    engines: Engines, engine: str, sql: str, reference: Sequence[Sequence[Q.Value]] | None
) -> dict[str, Any]:
    cell: dict[str, Any] = {"engine": engine, "sql": sql}
    try:
        rows, seconds, stats = engines.run(engine, sql)
    except Exception as exc:  # recorded; the cell is void
        cell["error"] = f"{type(exc).__name__}: {exc}"[:2000]
        return cell
    cell["seconds"] = seconds
    if stats:
        cell["stats"] = stats
    mismatch = Q.compare(reference, rows) if reference is not None else None
    if mismatch is not None:
        cell["mismatch"] = mismatch
    return cell


def ensure_marts(settings: Settings) -> None:
    """``marts.dim_wiki`` for the federated query: load the sample and build dbt if missing."""
    with psycopg.connect(settings.pg_conninfo) as conn:
        exists = conn.execute("SELECT to_regclass('marts.dim_wiki') IS NOT NULL").fetchone()
        populated = False
        if exists and exists[0]:
            row = conn.execute("SELECT count(*) FROM marts.dim_wiki").fetchone()
            populated = bool(row and row[0])
    if populated:
        return
    log.info("bench.query.building_marts")
    subprocess.run(["uv", "run", "tributary", "sample", "load"], cwd=ROOT, check=True)
    subprocess.run(
        ["uv", "run", "dbt", "build", "--project-dir", "warehouse", "--profiles-dir", "warehouse"],
        cwd=ROOT,
        check=True,
    )


def environment(settings: Settings, path: Path) -> None:
    engines = Engines(settings)
    try:
        pg = {
            name: engines.fetch("postgres", f"SHOW {name}")[0][0]
            for name in (
                "server_version",
                "shared_buffers",
                "work_mem",
                "max_parallel_workers_per_gather",
                "max_parallel_workers",
                "jit",
                "effective_cache_size",
            )
        }
        trino_version = engines.fetch(
            "trino-iceberg", "SELECT node_version FROM system.runtime.nodes"
        )
    finally:
        engines.close()
    _write(
        path,
        {
            "resources": resources(),
            "postgres_settings": pg,
            "trino_version": trino_version[0][0],
            "page_cache_droppable": can_drop_caches(),
            "recorded_at": datetime.now(UTC).isoformat(),
        },
    )


def run_scale(settings: Settings, rows: int, runs: int, out: Path, *, cold: bool) -> None:
    from tributary.querybench.generate import table_name

    name = table_name(rows)
    label = name.removeprefix("events_")
    load = load_unit(settings, rows, name, out / f"load-{label}.json")
    window = window_for(settings, load)
    engines = Engines(settings)
    try:
        wait_for_trino(engines)
        warm = warm_unit(engines, name, window, runs, out / f"warm-{label}.json")
        pruning_unit(engines, name, window, runs, out / f"pruning-{label}.json")
        federated_unit(engines, name, window, runs, out / f"federated-{label}.json")
    finally:
        engines.close()
    if cold:
        cold_unit(settings, name, window, warm, out / f"cold-{label}.json")


def main(argv: Sequence[str] | None = None, *, run: Callable[..., None] = run_scale) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--scales", default="1m,20m", help="comma-separated row counts")
    parser.add_argument("--runs", type=int, default=10, help="timed runs per cell")
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--no-cold", action="store_true", help="skip the cold runs")
    parser.add_argument("--no-stack", action="store_true", help="use the stack as it is")
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_format, settings.log_level)
    if not args.no_stack:
        prepare_stack()
    ensure_marts(settings)
    out: Path = args.out
    if not (out / "environment.json").exists():
        environment(settings, out / "environment.json")
    for scale in args.scales.split(","):
        run(settings, parse_scale(scale), args.runs, out, cold=not args.no_cold)
    return 0


if __name__ == "__main__":
    sys.exit(main())
