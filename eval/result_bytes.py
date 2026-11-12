"""How many recorded gold results the agent's result byte cap would refuse.

    uv run python -m eval.result_bytes            (warehouse part needs PostgreSQL)
    uv run python -m eval.result_bytes --no-db    (BIRD recordings only, offline)

The executor refuses a result whose values add up to more than
``TRIBUTARY_AGENT_MAX_RESULT_BYTES`` as text (see
:func:`tributary.agent.execute.row_bytes`). This measures that size for

- every BIRD execution recorded in ``tests/recordings/bird-executions.jsonl.gz``
  (gold and predicted queries, separately), from the recorded values: no BIRD
  database is needed, so the size is :func:`value_bytes`'s rendering of each
  value the way PostgreSQL prints it, close but not byte-exact (a ``real``
  column decoded to a Python float prints with a double's digits, an interval
  approximately);
- the 40 warehouse gold queries, measured by PostgreSQL itself with the
  executor's own size expression, as the agent's role; the same rows are also
  sized by :func:`value_bytes`, and the largest disagreement is reported as a
  check of that approximation.

Writes ``eval/results/result-cap/result-bytes.json``.
"""

from __future__ import annotations

import argparse
import contextlib
import gzip
import json
import math
import sys
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql

from eval.bird.recorded import decode
from eval.warehouse_eval import QUESTIONS, load_questions
from tributary.agent.execute import capped_columns, row_bytes
from tributary.config import REPO_ROOT, Settings, get_settings

EXECUTIONS = REPO_ROOT / "tests" / "recordings" / "bird-executions.jsonl.gz"
RESULT_PATH = REPO_ROOT / "eval" / "results" / "result-cap" / "result-bytes.json"


def _float_text(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    text = repr(value)  # shortest round-trip digits, as PostgreSQL 12+ prints a double
    return text[:-2] if text.endswith(".0") else text


def _interval_text(value: timedelta) -> str:
    days, seconds = value.days, value.seconds
    clock = f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"
    if value.microseconds:
        clock += f".{value.microseconds:06d}".rstrip("0")
    if not days:
        return clock
    unit = "day" if abs(days) == 1 else "days"
    return f"{days} {unit}" if not seconds and not value.microseconds else f"{days} {unit} {clock}"


def _array_element(value: object) -> str:
    if value is None:
        return "NULL"
    text = pg_text(value)
    if isinstance(value, list | tuple):
        return text
    if text == "" or any(c in text for c in '{}," \\') or text.upper() == "NULL":
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def pg_text(value: object) -> str:
    """A non-NULL value as ``value::text`` prints it in PostgreSQL (DateStyle ISO).

    That is the type's text output, except for a boolean, whose cast to text
    spells ``true``/``false`` where the output function sends ``t``/``f``.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | str):
        return str(value)
    if isinstance(value, float):
        return _float_text(value)
    if isinstance(value, Decimal):
        if value.is_nan():
            return "NaN"
        if value.is_infinite():
            return "Infinity" if value > 0 else "-Infinity"
        return format(value, "f")
    if isinstance(value, datetime | time):
        text = value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
        if value.microsecond:  # PostgreSQL prints .5, not .500000
            fraction = f".{value.microsecond:06d}"
            text = text.replace(fraction, fraction.rstrip("0"), 1)
        # and drops a zero minutes part of the offset: +02, not +02:00
        whole_hours = value.tzinfo is not None and text[-6] in "+-" and text.endswith(":00")
        return text[:-3] if whole_hours else text
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, timedelta):
        return _interval_text(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes | bytearray | memoryview):
        return "\\x" + bytes(value).hex()
    if isinstance(value, list | tuple):
        return "{" + ",".join(_array_element(v) for v in value) + "}"
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)  # jsonb's own ", " and ": "
    return str(value)


def value_bytes(value: object) -> int:
    """Bytes of one value as the cap counts it: its text output in UTF-8, NULL as 0."""
    return 0 if value is None else len(pg_text(value).encode("utf-8"))


def result_bytes(rows: Iterable[Sequence[object]]) -> int:
    return sum(value_bytes(v) for row in rows for v in row)


@dataclass(frozen=True, slots=True)
class Sized:
    source: str
    id: str
    rows: int
    bytes: int


def bird_sizes(path: Path = EXECUTIONS) -> list[Sized]:
    """Every recorded BIRD result (errors skipped), sized from its decoded values."""
    sized = []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            record = json.loads(line)
            if "result" not in record:
                continue
            database = str(record["database"])
            source = "bird-gold" if database.startswith("gold/") else "bird-predicted"
            rows = [[decode(v) for v in row] for row in record["result"]["rows"]]
            ident = f"{database}:{record['key'][:12]}"
            sized.append(Sized(source, ident, len(rows), result_bytes(rows)))
    return sized


def _measure(cur: psycopg.Cursor[Any], query: str, limit: int) -> tuple[int, int, int]:
    """Rows, the server's byte count and :func:`result_bytes` of the same rows.

    Any: rows are tuples of whatever types the columns decode to.
    """
    bare = sql.SQL("SELECT * FROM (\n{}\n) AS q LIMIT {}").format(
        sql.SQL(query), sql.Literal(limit)
    )
    cur.execute(bare, prepare=True)
    width = len(cur.description or ())
    python = result_bytes(cur.fetchall())
    if not width:
        return cur.rowcount, 0, 0
    cur.execute(
        sql.SQL("SELECT count(*), coalesce(sum({}), 0) FROM ({}) AS r({})").format(
            row_bytes(width), bare, sql.SQL(", ").join(capped_columns(width))
        ),
        prepare=True,
    )
    count, server = cur.fetchone() or (0, 0)
    return int(count), int(server), python


def warehouse_sizes(settings: Settings, questions: Path = QUESTIONS) -> tuple[list[Sized], int]:
    """The warehouse gold results sized by PostgreSQL, and the approximation's worst miss."""
    sized, worst = [], 0
    with psycopg.connect(settings.agent_conninfo) as conn, conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SET LOCAL search_path = ''")
        for question in load_questions(questions):
            rows, server, python = _measure(cur, question.sql, settings.agent_row_cap + 1)
            sized.append(Sized("warehouse-gold", question.id, rows, server))
            worst = max(worst, abs(server - python))
        with contextlib.suppress(psycopg.Error):
            conn.rollback()
    return sized, worst


def summarise(sized: Sequence[Sized], cap: int) -> dict[str, Any]:
    """Per source: results, how many are over ``cap``, the largest (Any: a JSON document)."""
    sources: dict[str, Any] = {}
    for source in sorted({s.source for s in sized}):
        group = [s for s in sized if s.source == source]
        largest = max(group, key=lambda s: s.bytes)
        sources[source] = {
            "results": len(group),
            "over_cap": sum(s.bytes > cap for s in group),
            "largest": asdict(largest),
            "total_bytes": sum(s.bytes for s in group),
        }
    return sources


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--executions", type=Path, default=EXECUTIONS)
    parser.add_argument("--questions", type=Path, default=QUESTIONS)
    parser.add_argument("--no-db", action="store_true", help="skip the warehouse queries")
    parser.add_argument("--output", type=Path, default=RESULT_PATH)
    args = parser.parse_args(argv)

    settings = get_settings()
    cap = settings.agent_max_result_bytes
    sized = bird_sizes(args.executions)
    document: dict[str, Any] = {"cap_bytes": cap, "measured": {}}
    document["measured"]["bird-gold"] = document["measured"]["bird-predicted"] = (
        "value_bytes() over the recorded values"
    )
    if not args.no_db:
        warehouse, worst = warehouse_sizes(settings, args.questions)
        sized += warehouse
        document["measured"]["warehouse-gold"] = (
            "PostgreSQL, the executor's row_bytes() as the agent's role, "
            f"at most {settings.agent_row_cap + 1} rows"
        )
        document["approximation_max_abs_error_bytes"] = worst
    document["sources"] = summarise(sized, cap)
    print(f"cap: {cap:,} bytes")
    for source, s in document["sources"].items():
        big = s["largest"]
        print(
            f"{source}: {s['results']} results, {s['over_cap']} over the cap, "
            f"largest {big['bytes']:,} bytes ({big['id']}, {big['rows']} rows)"
        )
    if "approximation_max_abs_error_bytes" in document:
        worst = document["approximation_max_abs_error_bytes"]
        print(f"value_bytes() vs PostgreSQL on the warehouse gold results: max error {worst} bytes")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
