"""The query benchmark's SQL: load the templates, render them per engine, compare results.

Each query in ``bench/queries/`` is written once, in SQL that PostgreSQL and
Trino both accept. A ``<name>.trino.sql`` file replaces it on the two Trino
engines where a function differs (only the exact percentile). The header
comments give a title and a kind: ``scan`` (reads the whole table),
``window`` (an event-time range: the queries the pruning comparison runs),
``lookup`` (one row by primary key).

Placeholders are rendered per engine: ``{events}`` the table, ``{hour_start}``,
``{hour_end}`` and ``{six_hours_end}`` timestamp literals, ``{event_id}`` an id
literal (a ``uuid`` column in PostgreSQL, which Trino's PostgreSQL connector
exposes as ``UUID``; a string in Iceberg).

A result counts only if it equals PostgreSQL's as a multiset of rows, values
normalised (decimals as floats, timestamps as UTC instants, UUIDs as text)
and floats compared with a relative tolerance of 1e-9.
"""

from __future__ import annotations

import math
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

QUERIES_DIR = Path(__file__).resolve().parent / "queries"
FLOAT_TOLERANCE = 1e-9

ENGINES = ("postgres", "trino-postgres", "trino-iceberg")
"""PostgreSQL directly; Trino over its ``postgresql`` catalog; Trino over Iceberg."""

_HEADER = re.compile(r"^--\s*(\w+):\s*(.+)$")
_PLACEHOLDER = re.compile(r"\{(\w+)\}")


@dataclass(frozen=True, slots=True)
class Query:
    name: str
    title: str
    kind: str
    postgres: str
    trino: str

    @property
    def has_variant(self) -> bool:
        return self.postgres != self.trino

    def sql(self, engine: str) -> str:
        return self.postgres if engine == "postgres" else self.trino


def _header(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in text.splitlines():
        match = _HEADER.match(line.strip())
        if match:
            fields[match[1]] = match[2].strip()
    return fields


def load_queries(directory: Path = QUERIES_DIR) -> list[Query]:
    """The ten timed queries (``qNN_*.sql``), in name order."""
    queries = []
    for path in sorted(directory.glob("q[0-9][0-9]_*.sql")):
        if path.name.endswith(".trino.sql"):
            continue
        text = path.read_text(encoding="utf-8")
        fields = _header(text)
        variant = path.with_name(path.stem + ".trino.sql")
        trino = variant.read_text(encoding="utf-8") if variant.exists() else text
        kind = fields.get("kind", "")
        if kind not in {"scan", "window", "lookup"}:
            raise ValueError(f"{path.name}: kind must be scan, window or lookup, not {kind!r}")
        queries.append(
            Query(path.stem, fields.get("title", path.stem), kind, postgres=text, trino=trino)
        )
    return queries


def load_federated(directory: Path = QUERIES_DIR) -> Query:
    """The federated query (Trino) and its PostgreSQL twin, the reference result."""
    trino = (directory / "federated.trino.sql").read_text(encoding="utf-8")
    postgres = (directory / "federated.sql").read_text(encoding="utf-8")
    return Query("federated", _header(trino).get("title", "federated"), "window", postgres, trino)


@dataclass(frozen=True, slots=True)
class Window:
    """The event-time range the window queries filter on, and the looked-up id."""

    hour_start: datetime
    event_id: str

    @property
    def hour_end(self) -> datetime:
        return self.hour_start + timedelta(hours=1)

    @property
    def six_hours_end(self) -> datetime:
        return self.hour_start + timedelta(hours=6)


def middle_hour(first: datetime, last: datetime) -> datetime:
    """The hour containing the midpoint of the data's event time (UTC)."""
    mid = first.astimezone(UTC) + (last - first) / 2
    return mid.replace(minute=0, second=0, microsecond=0)


def _ts_literal(engine: str, value: datetime) -> str:
    text = value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    if engine == "postgres":
        return f"TIMESTAMPTZ '{text}+00'"
    return f"TIMESTAMP '{text} UTC'"


def _id_literal(engine: str, value: str) -> str:
    uuid.UUID(value)  # refuse anything that is not a UUID before quoting it
    return f"UUID '{value}'" if engine == "trino-postgres" else f"'{value}'"


def table_ref(engine: str, name: str, *, layout: str = "") -> str:
    """The table on ``engine``; ``layout`` (``_flat``, ``_stream``) only exists in Iceberg."""
    if layout and engine != "trino-iceberg":
        raise ValueError("only the Iceberg engine has layouts")
    return {
        "postgres": f"bench.{name}",
        "trino-postgres": f"postgresql.bench.{name}",
        "trino-iceberg": f"iceberg.bench.{name}{layout}",
    }[engine]


def render(query: Query, engine: str, table: str, window: Window) -> str:
    values = {
        "events": table,
        "hour_start": _ts_literal(engine, window.hour_start),
        "hour_end": _ts_literal(engine, window.hour_end),
        "six_hours_end": _ts_literal(engine, window.six_hours_end),
        "event_id": _id_literal(engine, window.event_id),
    }

    def substitute(match: re.Match[str]) -> str:
        key = match[1]
        if key not in values:
            raise KeyError(f"{query.name}: unknown placeholder {{{key}}}")
        return values[key]

    body = "\n".join(
        line for line in query.sql(engine).splitlines() if not line.lstrip().startswith("--")
    )
    return _PLACEHOLDER.sub(substitute, body).strip()


# ------------------------------------------------------------------ result equality

Value = None | bool | int | float | str


def normalise(value: object) -> Value:
    """One value as a JSON-safe scalar comparable across drivers."""
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(f"unexpected result value {value!r} ({type(value).__name__})")


def normalise_rows(rows: Sequence[Sequence[object]]) -> list[list[Value]]:
    return [[normalise(v) for v in row] for row in rows]


def _sort_key(row: Sequence[Value]) -> tuple[tuple[int, str], ...]:
    def key(v: Value) -> tuple[int, str]:
        if v is None:
            return (0, "")
        if isinstance(v, bool):
            return (1, str(v))
        if isinstance(v, int | float):
            return (2, f"{float(v):.6e}")  # coarse, so near-equal floats sort together
        return (3, v)

    return tuple(key(v) for v in row)


def _equal(a: Value, b: Value) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b  # True is not the integer 1 here
    if isinstance(a, int | float) and isinstance(b, int | float):
        return math.isclose(a, b, rel_tol=FLOAT_TOLERANCE, abs_tol=FLOAT_TOLERANCE)
    return a == b


def compare(expected: Sequence[Sequence[Value]], got: Sequence[Sequence[Value]]) -> str | None:
    """None if ``got`` equals ``expected`` as a multiset of rows; else what differs."""
    if len(expected) != len(got):
        return f"{len(got)} rows, expected {len(expected)}"
    for i, (a, b) in enumerate(
        zip(sorted(expected, key=_sort_key), sorted(got, key=_sort_key), strict=True)
    ):
        if len(a) != len(b) or not all(_equal(x, y) for x, y in zip(a, b, strict=True)):
            return f"row {i} differs: expected {list(a)}, got {list(b)}"
    return None


def describe(queries: Sequence[Query]) -> list[Mapping[str, object]]:
    """The query set as recorded in the raw results (name, title, kind, both SQL texts)."""
    return [
        {
            "name": q.name,
            "title": q.title,
            "kind": q.kind,
            "postgres": q.postgres,
            "trino": q.trino if q.has_variant else None,
        }
        for q in queries
    ]
