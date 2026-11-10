"""The PostgreSQL sink on its own: the stream job's writes replayed without Spark.

``P`` processes, one connection each (the stream job opens one per partition),
each write the same number of partitions of the same size as the stream job's
batches did, through the same function (:func:`tributary.stream.sink.write_rows`:
``COPY`` into a temporary table, then ``INSERT ... ON CONFLICT DO NOTHING``).
The rows are built before the clock starts, so the rate is the database's and
the driver's ``COPY`` loop's, nothing upstream of it.
"""

from __future__ import annotations

import multiprocessing as mp
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from bench.load import bench_id, load_templates
from tributary.events import to_row
from tributary.stream.sink import write_rows


@dataclass(frozen=True)
class ReplayResult:
    rows: int
    seconds: float
    per_process_seconds: tuple[float, ...]

    @property
    def rows_per_s(self) -> float:
        return self.rows / self.seconds if self.seconds > 0 else 0.0


def build_rows(
    sample_path: Path, prefix: str, first_seq: int, count: int, partition: int
) -> list[tuple[object, ...]]:
    """``count`` landing-table rows (``COLUMNS`` order) with fresh ids, as the job would write."""
    import json

    templates = load_templates(sample_path)
    now = time.time()
    kafka_ts = datetime.fromtimestamp(now, UTC)
    rows = []
    for i in range(count):
        seq = first_seq + i
        event = json.loads(templates[seq % len(templates)].render(bench_id(prefix, seq), now))
        row = to_row(event, partition=partition, offset=seq, kafka_ts=kafka_ts)
        rows.append(row.as_tuple())
    return rows


def _replay(
    conninfo: str,
    build: tuple[Path, str, int, int, int],
    sizes: Sequence[int],
    start: Any,
    ready: Any,
    out: Any,
    index: int,
) -> None:
    rows = build_rows(*build)
    with psycopg.connect(conninfo) as conn:
        conn.execute("SELECT 1")
        conn.commit()
        with ready.get_lock():
            ready.value += 1
        while start.value == 0.0:
            time.sleep(0.005)
        time.sleep(max(0.0, start.value - time.monotonic()))
        t = time.perf_counter()
        cursor = 0
        for size in sizes:
            write_rows(conn, rows[cursor : cursor + size])
            cursor += size
        out[index] = time.perf_counter() - t


def replay(
    conninfo: str, sample_path: Path, prefix: str, partitions: int, sizes: Sequence[int]
) -> ReplayResult:
    """Write ``sizes`` (rows per partition per batch, in order) from ``partitions`` processes.

    Every process writes every batch's share, as the job's tasks do; each
    builds its rows and connects before the common start, and the elapsed
    time is the slowest process's.
    """
    per_process = sum(sizes)
    ctx = mp.get_context("spawn")
    start = ctx.Value("d", 0.0)
    ready = ctx.Value("i", 0)
    out = ctx.Array("d", partitions)
    procs = [
        ctx.Process(
            target=_replay,
            args=(
                conninfo,
                (sample_path, prefix, p * per_process, per_process, p),
                list(sizes),
                start,
                ready,
                out,
                p,
            ),
        )
        for p in range(partitions)
    ]
    for proc in procs:
        proc.start()
    while ready.value < partitions:
        if any(proc.exitcode not in (None, 0) for proc in procs):
            raise RuntimeError("a sink replay process failed before the start")
        time.sleep(0.1)
    with start.get_lock():
        start.value = time.monotonic() + 0.5
    for proc in procs:
        proc.join()
        if proc.exitcode != 0:
            raise RuntimeError(f"sink replay process exited {proc.exitcode}")
    elapsed = tuple(float(x) for x in out[:])
    return ReplayResult(
        rows=per_process * partitions, seconds=max(elapsed), per_process_seconds=elapsed
    )
