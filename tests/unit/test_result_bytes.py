"""Sizing recorded results as the executor's byte cap counts them (eval/result_bytes.py)."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from eval.bird.recorded import encode
from eval.result_bytes import Sized, bird_sizes, pg_text, result_bytes, summarise, value_bytes


# Each expected string is what PostgreSQL 17 printed for ``(<expr>)::text`` with
# TimeZone UTC, the value being what psycopg decoded from ``<expr>``.
@pytest.mark.parametrize(
    ("value", "text"),
    [
        (True, "true"),  # the cast spells it out; the output function would send "t"
        (42, "42"),
        ("héllo", "héllo"),
        (1.5, "1.5"),
        (100.0, "100"),
        (1e20, "1e+20"),
        (float("nan"), "NaN"),
        (Decimal("1E-7"), "0.0000001"),
        (Decimal("1.50"), "1.50"),
        (date(2012, 1, 1), "2012-01-01"),
        (datetime(2012, 1, 1, 10), "2012-01-01 10:00:00"),
        (datetime(2012, 1, 1, 10, 0, 0, 500000, tzinfo=UTC), "2012-01-01 10:00:00.5+00"),
        (time(9, 30), "09:30:00"),
        (timedelta(days=1, seconds=7384), "1 day 02:03:04"),
        (timedelta(seconds=90), "00:01:30"),
        (timedelta(days=-2), "-2 days"),
        (b"\x00\xff", "\\x00ff"),
        ([1, 2], "{1,2}"),
        (["a b", None, "c"], '{"a b",NULL,c}'),
        ({"a": [1, 2]}, '{"a": [1, 2]}'),
        (UUID(int=1), "00000000-0000-0000-0000-000000000001"),
    ],
)
def test_pg_text(value: object, text: str) -> None:
    assert pg_text(value) == text


def test_value_bytes_counts_utf8_and_null_as_zero() -> None:
    assert value_bytes(None) == 0
    assert value_bytes("é") == 2
    assert result_bytes([(1, "ab", None), (True, Decimal("0.5"))]) == 1 + 2 + 0 + 4 + 3


def test_bird_sizes_reads_gold_and_predicted_results(tmp_path: Path) -> None:
    records = [
        {"key": "a" * 64, "database": "gold/db", "result": {"rows": [[1, "xy"]]}},
        {"key": "b" * 64, "database": "db", "result": {"rows": [[encode(Decimal("2.5"))]]}},
        {"key": "c" * 64, "database": "db", "error": {"kind": "database"}},
    ]
    path = tmp_path / "executions.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.writelines(json.dumps(r) + "\n" for r in records)
    assert bird_sizes(path) == [
        Sized("bird-gold", "gold/db:aaaaaaaaaaaa", 1, 3),
        Sized("bird-predicted", "db:bbbbbbbbbbbb", 1, 3),
    ]


def test_summarise_counts_results_over_the_cap() -> None:
    sized = [Sized("s", "small", 1, 10), Sized("s", "big", 2, 11), Sized("t", "x", 0, 0)]
    summary = summarise(sized, cap=10)
    assert summary["s"]["results"] == 2
    assert summary["s"]["over_cap"] == 1
    assert summary["s"]["largest"] == {"source": "s", "id": "big", "rows": 2, "bytes": 11}
    assert summary["t"]["over_cap"] == 0
