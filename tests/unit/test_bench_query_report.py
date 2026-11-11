"""The query-engine tables in RESULTS.md, rendered from synthetic raw files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from bench import query_report as R  # noqa: N812

QUERIES = ("q01_count", "q05_hour_pages")


def _cell(median: float, p95: float, **extra: Any) -> dict[str, Any]:
    return {"summary": {"median": median, "p95": p95, "min": median, "max": p95}, **extra}


def _write(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data))


@pytest.fixture
def results(tmp_path: Path) -> Path:
    layouts = {
        "partitioned": {"files": 13, "bytes": 46e6, "median_file_bytes": 3.6e6},
        "unpartitioned": {"files": 2, "bytes": 45e6, "median_file_bytes": 22e6},
        "uncompacted": {"files": 2250, "bytes": 80e6, "median_file_bytes": 36e3},
    }
    _write(
        tmp_path / "load-1m.json",
        {
            "rows": 1_000_000,
            "name": "events_1m",
            "generator": {"seconds": {"iceberg": 20.0, "stream": 60.0, "postgres": 40.0}},
            "optimize_seconds": 3.0,
            "flat_ctas_seconds": 5.0,
            "iceberg": layouts,
            "postgres": {
                "heap_bytes": 8192 * 27_200,
                "table_bytes": 223e6,
                "pkey_bytes": 31e6,
                "brin_bytes": 24_576,
            },
        },
    )
    _write(
        tmp_path / "warm-1m.json",
        {
            "name": "events_1m",
            "cells": {
                "q01_count/postgres": _cell(0.040, 0.050),
                "q01_count/trino-postgres": _cell(0.200, 0.220),
                "q01_count/trino-iceberg": _cell(0.150, 0.160),
                "q05_hour_pages/postgres": _cell(0.0021, 0.003),
                "q05_hour_pages/trino-postgres": {"mismatch": "row 0 differs"},
                "q05_hour_pages/trino-iceberg": {"error": "TrinoQueryError: boom"},
            },
        },
    )
    _write(
        tmp_path / "cold-1m.json",
        {
            "name": "events_1m",
            "cells": {
                "q01_count/postgres": {"seconds": 0.5},
                "q01_count/trino-postgres": {"seconds": 1.5},
                "q01_count/trino-iceberg": {"seconds": 2.5, "mismatch": "x"},
            },
        },
    )
    _write(
        tmp_path / "pruning-1m.json",
        {
            "name": "events_1m",
            "window": {"hour_start": "2026-10-17T05:00:00+00:00"},
            "cells": {
                "q05_hour_pages/postgres": {"buffers": {"blocks": 2_000}},
                "q05_hour_pages/partitioned": _cell(
                    0.1,
                    0.2,
                    scan={"splits": 1, "rows": 82_000},
                    window_files={"files": 1},
                    stats=[{"physicalInputBytes": 5e5}, {"physicalInputBytes": 7e5}],
                ),
                "q05_hour_pages/uncompacted": _cell(
                    0.5,
                    0.9,
                    scan={"splits": 180, "rows": 82_000},
                    window_files={"files": 180},
                    stats=[{"physicalInputBytes": 6e6}],
                ),
            },
        },
    )
    _write(
        tmp_path / "federated-1m.json",
        {
            "name": "events_1m",
            "window": {"hour_start": "2026-10-17T05:00:00+00:00"},
            "trino": _cell(
                0.3, 0.4, result=[["wikipedia", 31972, 105], [None, 2, 1]], result_rows=2
            ),
            "postgres": _cell(0.05, 0.06),
        },
    )
    return tmp_path


def test_times_read_in_the_unit_that_fits() -> None:
    assert R.fmt_seconds(0.0006) == "0.6 ms"
    assert R.fmt_seconds(0.0123) == "12 ms"
    assert R.fmt_seconds(1.234) == "1.23 s"
    assert R.fmt_seconds(None) == R.DASH
    assert R.fmt_bytes(6.13e6) == "6.1 MB"
    assert R.fmt_bytes(512) == "512 B"
    assert R.scale_label(20_000_000) == "20M"
    assert R.scale_label(250_000) == "250k"


def test_the_latency_table_marks_void_cells_and_names_the_fastest(results: Path) -> None:
    table = R.latency_table(results)
    count = next(line for line in table.splitlines() if line.startswith("| q01"))
    assert "| 1M | 40 ms / 50 ms | 200 ms / 220 ms | 150 ms / 160 ms | PostgreSQL (3.8x) |" in count
    hour = next(line for line in table.splitlines() if line.startswith("| q05"))
    assert "| 2.1 ms / 3.0 ms | result differs | error | PostgreSQL |" in hour


def test_a_cold_cell_that_differs_shows_no_time(results: Path) -> None:
    row = next(line for line in R.cold_table(results).splitlines() if line.startswith("| q01"))
    assert row.endswith("| 500 ms | 1.50 s | result differs |")


def test_the_pruning_table_puts_postgres_blocks_beside_the_layouts(results: Path) -> None:
    lines = R.pruning_table(results).splitlines()
    partitioned = next(line for line in lines if "by hour, compacted" in line)
    assert "| 13 | 1 | 1 | 82,000 | 600.0 kB | 100 ms / 200 ms |" in partitioned
    streamed = next(line for line in lines if "as streamed" in line)
    assert "| 2,250 | 180 | 180 |" in streamed
    pg = next(line for line in lines if "PostgreSQL heap" in line)
    assert "2,000 of 27,200 blocks" in pg
    assert "16.4 MB" in pg  # 2,000 blocks of 8 kB
    assert pg.endswith("| 2.1 ms / 3.0 ms |")
    assert not any("unpartitioned" in line for line in lines)  # no cell, no row


def test_storage_and_federated_tables(results: Path) -> None:
    storage = R.storage_table(results)
    assert "| 1M | Iceberg, by hour, as streamed | 2,250 | 80.0 MB | 36.0 kB | 60.00 s |" in storage
    assert "PostgreSQL heap (primary key 31.0 MB, BRIN 24.6 kB)" in storage
    fed = R.federated_table(results)
    assert "| 1M | 300 ms / 400 ms | 50 ms / 60 ms | matches, 2 rows |" in fed
    result = R.federated_result(results)
    assert "| wikipedia | 31,972 | 105 |" in result
    assert "| (none) | 2 | 1 |" in result


def test_nothing_recorded_renders_a_placeholder(tmp_path: Path) -> None:
    assert R.latency_table(tmp_path) == "No run recorded yet."
    assert R.render(tmp_path / "missing") == {}
