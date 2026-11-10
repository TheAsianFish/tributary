"""Recorded database executions, so a BIRD run replays in CI without the databases.

Model calls are already recorded by request fingerprint
(:mod:`tributary.agent.providers.fake`). A BIRD run also needs the database:
the gold query's rows, the predicted query's rows, and, for a repair, the
exact error text the model was shown. :class:`RecordingExecutor` wraps a live
executor and stores each outcome under ``sha256(database, sql)``;
:class:`ReplayExecutor` returns it, and raises on a query it has never seen
rather than guess.

Values keep their type: BIRD's scorer compares rows with Python's ``==``, so
``Decimal('0.5')`` and ``0.5`` are different answers to it, and a replay
must hand the scorer the same types the live run had. JSON's own types carry
``None``, booleans, integers, strings and finite floats; anything else is a
tagged object (``{"$decimal": "61.57"}``, ``{"$date": "2012-01-01"}``, ...)
decoded back into the original type. The file is JSON Lines,
gzip-compressed and sorted by key, so a re-record gives a stable file.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import tempfile
import threading
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from tributary.agent.execute import ExecutionError, Executor, QueryResult


class ExecutionMissing(Exception):  # noqa: N818 (mirrors RecordingMissing)
    """The replay has no recorded execution for this query."""


def execution_key(database: str, query: str) -> str:
    return hashlib.sha256(f"{database}\x00{query}".encode()).hexdigest()


def load_executions(path: Path) -> dict[str, dict[str, Any]]:
    """Recorded executions by key (Any: one decoded JSON record each)."""
    if not path.exists():
        return {}
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return {r["key"]: r for r in map(json.loads, fh) if r}


def write_executions(path: Path, records: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    os.close(fd)
    try:
        # No name and mtime=0: the gzip header would otherwise hold the temporary file's name
        # and the write time, and a re-record of the same executions would differ in bytes.
        with (
            open(tmp, "wb") as raw,
            gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as gz,
        ):
            for key in sorted(records):
                line = json.dumps(records[key], sort_keys=True, ensure_ascii=False) + "\n"
                gz.write(line.encode("utf-8"))
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def encode(value: object) -> object:
    """A database value as JSON that :func:`decode` turns back into the same type."""
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {"$float": repr(value)}
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, time):
        return {"$time": value.isoformat()}
    if isinstance(value, timedelta):
        return {"$interval": [value.days, value.seconds, value.microseconds]}
    if isinstance(value, UUID):
        return {"$uuid": str(value)}
    if isinstance(value, bytes | bytearray | memoryview):
        return {"$bytes": bytes(value).hex()}
    if isinstance(value, list | tuple):
        return {"$array": [encode(v) for v in value]}
    if isinstance(value, dict):
        return {"$json": json.dumps(value, sort_keys=True)}
    raise TypeError(f"cannot record a {type(value).__name__} value")


_DECODERS: dict[str, Any] = {
    "$float": float,
    "$decimal": Decimal,
    "$datetime": datetime.fromisoformat,
    "$date": date.fromisoformat,
    "$time": time.fromisoformat,
    "$interval": lambda parts: timedelta(days=parts[0], seconds=parts[1], microseconds=parts[2]),
    "$uuid": UUID,
    "$bytes": bytes.fromhex,
    "$array": lambda items: [decode(v) for v in items],
    "$json": json.loads,
}


def decode(value: object) -> object:
    if isinstance(value, dict) and len(value) == 1:
        ((tag, body),) = value.items()
        if tag in _DECODERS:
            return _DECODERS[tag](body)
    if isinstance(value, dict | list):
        raise ValueError(f"not a recorded value: {value!r}")
    return value


def to_record(database: str, query: str, outcome: QueryResult | ExecutionError) -> dict[str, Any]:
    record: dict[str, Any] = {
        "key": execution_key(database, query),
        "database": database,
        "sql": query,
    }
    if isinstance(outcome, ExecutionError):
        record["error"] = {
            "kind": outcome.kind,
            "message": outcome.message,
            "sqlstate": outcome.sqlstate,
        }
        return record
    record["result"] = {
        "columns": list(outcome.columns),
        "rows": [[encode(v) for v in row] for row in outcome.rows],
        "truncated": outcome.truncated,
        "elapsed_ms": round(outcome.elapsed_ms, 3),
        "cost": outcome.cost,
    }
    return record


def from_record(record: dict[str, Any]) -> QueryResult:
    """The recorded outcome: its result, or the recorded error raised again."""
    if "error" in record:
        e = record["error"]
        raise ExecutionError(e["kind"], e["message"], e.get("sqlstate"))
    r = record["result"]
    return QueryResult(
        columns=tuple(r["columns"]),
        rows=tuple(tuple(decode(v) for v in row) for row in r["rows"]),
        truncated=bool(r["truncated"]),
        elapsed_ms=float(r["elapsed_ms"]),
        cost=r.get("cost"),
    )


class Recorder:
    """The shared store a run's recording executors write into; saved once at the end."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.records = load_executions(path)
        self._lock = threading.Lock()

    def add(self, record: dict[str, Any]) -> None:
        with self._lock:
            self.records[record["key"]] = record

    def save(self) -> None:
        with self._lock:
            write_executions(self.path, self.records)


class RecordingExecutor:
    """A live executor whose every outcome is also stored in a :class:`Recorder`."""

    def __init__(self, inner: Executor, database: str, recorder: Recorder) -> None:
        self.inner = inner
        self.database = database
        self.recorder = recorder

    def run(self, query: str) -> QueryResult:
        try:
            result = self.inner.run(query)
        except ExecutionError as exc:
            if exc.repairable:  # an outage is not a property of the query; never record it
                self.recorder.add(to_record(self.database, query, exc))
            raise
        self.recorder.add(to_record(self.database, query, result))
        return result


class ReplayExecutor:
    """Returns recorded outcomes; an unrecorded query is an error, never a guess."""

    def __init__(self, records: dict[str, dict[str, Any]], database: str) -> None:
        self.records = records
        self.database = database

    def run(self, query: str) -> QueryResult:
        record = self.records.get(execution_key(self.database, query))
        if record is None:
            raise ExecutionMissing(
                f"no recorded execution on {self.database} for {query[:80]!r}; "
                "re-record with `make eval PROVIDER=ollama RECORD=1` against `make bird`"
            )
        return from_record(record)
