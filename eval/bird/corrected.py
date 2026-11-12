"""The corrected-label track, transpiled to PostgreSQL and verified by execution.

Arcwise-Plat-SQL corrects BIRD Mini-Dev's gold SQL, but in SQLite. Each
corrected query is transpiled with sqlglot and then *verified*: it runs on
SQLite (the file it was written for) and, transpiled, on PostgreSQL, and is
kept only if both return the same set of rows. A query that does not
transpile, fails on either engine, or returns different rows is dropped from
the track and counted by reason, so the track's size is a measured number,
not an assumption.

sqlglot already turns ``IIF`` into ``CASE``, guards division with
``NULLIF`` (SQLite returns NULL on a zero divisor, PostgreSQL raises) and
pins ``NULLS LAST``. The transpiler adds five rewrites for SQLite semantics
the corrected SQL relies on and sqlglot does not carry over:

- identifiers: SQLite matches names ignoring case, and the PostgreSQL copy
  of BIRD lowercased every plain name, so a plain identifier is written in
  lowercase and unquoted (``T2.`CDSCode``` becomes ``t2.cdscode``); names
  that need quoting (``"Free Meal Count (K-12)"``) keep their spelling, and a
  reserved word stays quoted (the ``financial.order`` table);
- ``REAL`` and ``FLOAT`` are 8-byte in SQLite and 4-byte in PostgreSQL, so a
  ``CAST(x AS REAL)`` ratio would lose digits; they become ``DOUBLE PRECISION``;
- SQLite's ``LIKE`` ignores ASCII case; PostgreSQL's does not, so ``LIKE``
  becomes ``ILIKE``;
- type affinity: SQLite compares a text column with a number as text
  (``district.a2 = 1``); PostgreSQL refuses ``text = integer``, so the number
  becomes a string literal when the database's catalog types the column as
  text;
- ``SUM(gender = 'M')`` adds SQLite's 0/1 truth values; PostgreSQL has no
  ``sum(boolean)``, so a comparison inside ``SUM`` or ``AVG`` is cast to
  ``integer``.

Verification compares rows with numbers at six significant digits (SQLite
divides in floating point where PostgreSQL may return ``numeric``), and it
also runs every *official* gold query, so the report says how many official
golds execute and, for each verified corrected question, whether its result
differs from the official gold's. Only questions worded identically in both
releases measure label disagreement: the corrections keep BIRD dev's original
wording, which Mini-Dev changed for some questions.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

import sqlglot
import structlog
from sqlglot import exp
from sqlglot.errors import SqlglotError

from eval.bird import data
from eval.bird.load import pg_name
from eval.bird.recorded import decode, encode
from eval.bird.score import (
    ScorerUnavailable,
    cross_check,
    execution_match,
    lenient_match,
    normal_rows,
    official_calculate_ex,
    rows_digest,
)
from tributary.agent.catalog import Catalog, quote_ident
from tributary.agent.execute import ExecutionError, PostgresExecutor, QueryResult
from tributary.config import Settings

log = structlog.get_logger(__name__)

TIMEOUT_S = 30  # BIRD's evaluation_ex.py default meta_time_out
# SQLite runs only to verify a transpilation. A few corrected queries join unindexed
# columns and take minutes there; each result is cached by (database file, query), so the
# limit is paid once and a rerun cannot flip a verdict by finishing just in or out of time.
SQLITE_TIMEOUT_S = 900
ROW_CAP = 100_000


class TranspileError(Exception):
    pass


_COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)
_PREDICATES = (*_COMPARISONS, exp.Like, exp.ILike, exp.In, exp.Is, exp.And, exp.Or, exp.Not)
_TEXT_TYPES = ("text", "character varying", "character", "varchar")


def column_types(catalog: Catalog) -> dict[str, str]:
    """Lowercase column name → type, for names with one type across the database."""
    seen: dict[str, set[str]] = {}
    for table in catalog.tables:
        for column in table.columns:
            seen.setdefault(column.name.lower(), set()).add(column.data_type.lower())
    return {name: next(iter(types)) for name, types in seen.items() if len(types) == 1}


def _is_number(node: exp.Expr) -> bool:
    if isinstance(node, exp.Neg):
        node = node.this
    return isinstance(node, exp.Literal) and not node.is_string


def _as_text(node: exp.Expr) -> exp.Expr:
    """A number as SQLite renders it when it compares it with text: ``05`` → ``'5'``,
    ``1e3`` → ``'1000.0'`` (integers in decimal, reals with a fractional part)."""
    sign = ""
    if isinstance(node, exp.Neg):
        sign, node = "-", node.this
    text = str(node.this)
    try:
        rendered = str(int(text)) if text.isdigit() else repr(float(text))
    except ValueError:
        rendered = text
    return exp.Literal.string(sign + rendered)


def _is_text_column(node: exp.Expr, types: Mapping[str, str]) -> bool:
    return isinstance(node, exp.Column) and types.get(node.name.lower(), "") in _TEXT_TYPES


def _identifier(name: str) -> exp.Identifier:
    """``name`` as the PostgreSQL copy of BIRD spells it, quoted only where it must be."""
    spelled = pg_name(name)
    return exp.Identifier(this=spelled, quoted=quote_ident(spelled) != spelled)


def _ilike(tree: exp.Expr) -> exp.Expr:
    """Every ``LIKE`` as ``ILIKE``, nested ones included (replaced in place, children kept)."""
    for like in list(tree.find_all(exp.Like)):
        # Every argument carries over: NOT LIKE is a Like with negate=True in sqlglot.
        like.replace(exp.ILike(**like.args))
    return tree


def _sqlite_semantics(types: Mapping[str, str]) -> Callable[[exp.Expr], exp.Expr]:
    def rewrite(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Identifier):
            return _identifier(str(node.this)) if node.this else node
        if isinstance(node, exp.DataType) and node.this in (
            exp.DataType.Type.FLOAT,
            exp.DataType.Type.DOUBLE,
        ):
            return exp.DataType.build("double precision")
        if isinstance(node, _COMPARISONS):
            left, right = node.this, node.expression
            if _is_text_column(left, types) and _is_number(right):
                node.set("expression", _as_text(right))
            elif _is_text_column(right, types) and _is_number(left):
                node.set("this", _as_text(left))
        if isinstance(node, exp.In) and _is_text_column(node.this, types):
            node.set(
                "expressions",
                [_as_text(e) if _is_number(e) else e for e in node.expressions],
            )
        if isinstance(node, exp.Sum | exp.Avg) and isinstance(node.this, _PREDICATES):
            node.set("this", exp.cast(node.this, exp.DataType.build("int")))
        return node

    return rewrite


def transpile(sqlite_sql: str, types: Mapping[str, str] | None = None) -> str:
    """One SQLite query as PostgreSQL, keeping the SQLite semantics listed above.

    ``types`` (from :func:`column_types`) enables the affinity rewrite; without
    it a text column compared with a number is left for PostgreSQL to refuse.
    """
    try:
        statements = sqlglot.parse(sqlite_sql, read="sqlite")
    except SqlglotError as exc:
        raise TranspileError(str(exc).splitlines()[0]) from None
    trees = [s for s in statements if s is not None]
    if len(trees) != 1:
        raise TranspileError(f"expected one statement, got {len(trees)}")
    try:
        # The LIKE swap runs after the other rewrites: a node returned by transform is not
        # visited again, so swapping first would leave the identifiers under it untouched.
        tree = _ilike(trees[0].transform(_sqlite_semantics(types or {})))
        return tree.sql(dialect="postgres")
    except SqlglotError as exc:
        raise TranspileError(str(exc).splitlines()[0]) from None


def _approx(value: object) -> object:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float | Decimal):
        number = float(value)
        if number != number:  # NaN
            return "NaN"
        return float(f"{number:.6g}")
    return value


def same_rows_approx(left: QueryResult | list[tuple[object, ...]], right: list[Any]) -> bool:
    lrows = left.rows if isinstance(left, QueryResult) else left
    a = {tuple(_approx(v) for v in r) for r in normal_rows(lrows)}
    b = {tuple(_approx(v) for v in r) for r in normal_rows(right)}
    return a == b


def run_sqlite(
    path: Path, query: str, timeout_s: float = SQLITE_TIMEOUT_S
) -> list[tuple[object, ...]]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    deadline = time.monotonic() + timeout_s
    conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 10_000)
    try:
        return [tuple(r) for r in conn.execute(query).fetchmany(ROW_CAP + 1)]
    finally:
        conn.close()


def cached_sqlite(path: Path, query: str, cache: Path) -> list[tuple[object, ...]]:
    """:func:`run_sqlite`, remembered under ``cache`` by the file's hash and the query.

    A failure (including the timeout) is remembered too and raised again as
    :class:`sqlite3.Error`, so every run sees the same outcome.
    """
    key = hashlib.sha256(f"{_file_digest(path)}\x00{query}".encode()).hexdigest()
    entry = cache / f"{key}.json"
    if entry.exists():
        stored = json.loads(entry.read_text(encoding="utf-8"))
        if "error" in stored:
            raise sqlite3.OperationalError(stored["error"])
        return [tuple(decode(v) for v in row) for row in stored["rows"]]
    cache.mkdir(parents=True, exist_ok=True)
    try:
        rows = run_sqlite(path, query)
    except sqlite3.Error as exc:
        _write_atomic(entry, {"error": f"{exc} (limit {SQLITE_TIMEOUT_S} s)"})
        raise sqlite3.OperationalError(f"{exc} (limit {SQLITE_TIMEOUT_S} s)") from None
    _write_atomic(entry, {"rows": [[encode(v) for v in row] for row in rows]})
    return rows


def _write_atomic(path: Path, document: object) -> None:
    """A killed run leaves the old entry or none, never half a JSON file."""
    partial = path.with_name(f".{path.name}.{threading.get_ident()}.part")
    partial.write_text(json.dumps(document))
    os.replace(partial, path)


_DIGESTS: dict[Path, str] = {}


def _file_digest(path: Path) -> str:
    if path not in _DIGESTS:
        stat = path.stat()
        # Size and mtime stand in for the content: the files come from one pinned zip.
        _DIGESTS[path] = f"{path.name}:{stat.st_size}:{int(stat.st_mtime)}"
    return _DIGESTS[path]


def gold_executors(settings: Settings) -> dict[str, PostgresExecutor]:
    """One executor per BIRD database, as the read-only role, unqualified names resolved.

    Serial plans only: several gold queries sum 4-byte ``real`` columns, and a
    parallel aggregate's result then depends on the order its workers finish.
    """
    from eval.bird.load import bird_conninfo

    conninfo = bird_conninfo(settings, as_reader=True)
    return {
        db: PostgresExecutor(
            conninfo,
            statement_timeout_ms=TIMEOUT_S * 1000,
            row_cap=ROW_CAP,
            cost_ceiling=None,
            max_result_bytes=None,
            search_path=db,
            parallel=False,
        )
        for db in data.DATABASES
    }


def _run_pg(executor: PostgresExecutor, query: str) -> QueryResult | ExecutionError:
    try:
        return executor.run(query)
    except ExecutionError as exc:
        return exc


def _parallel(
    items: list[Any], work: Callable[[Any], dict[str, Any]], workers: int = 4
) -> list[dict[str, Any]]:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(work, items))


def verify_official(
    executors: Mapping[str, PostgresExecutor],
) -> tuple[dict[str, Any], dict[int, QueryResult]]:
    """Run every official gold query; returns the report and the results by id."""
    results: dict[int, QueryResult] = {}

    def work(q: data.BirdQuestion) -> dict[str, Any]:
        outcome = _run_pg(executors[q.db_id], q.sql)
        if isinstance(outcome, ExecutionError):
            return {"id": q.id, "status": "error", "error": outcome.feedback()}
        results[q.id] = outcome
        return {
            "id": q.id,
            "status": "ok",
            "rows": len(outcome.rows),
            "truncated": outcome.truncated,
            "ms": round(outcome.elapsed_ms),
            "digest": rows_digest(outcome.rows),
        }

    started = time.perf_counter()
    items = _parallel(data.official_questions(), work)
    failed = [i for i in items if i["status"] != "ok"]
    report = {
        "total": len(items),
        "executed": len(items) - len(failed),
        "empty": sum(1 for i in items if i.get("rows") == 0),
        "truncated": sum(1 for i in items if i.get("truncated")),
        "failed": failed,
        "seconds": round(time.perf_counter() - started, 1),
        "slowest_ms": max((i.get("ms", 0) for i in items), default=0),
        # Row-set fingerprints (numbers by value), to compare two loads of the databases.
        "digests": {str(i["id"]): i["digest"] for i in items if "digest" in i},
    }
    return report, results


def verify_corrected(
    executors: Mapping[str, PostgresExecutor],
    minidev: Path,
    official: Mapping[int, QueryResult],
    catalogs: Mapping[str, Catalog],
) -> dict[str, Any]:
    types = {db: column_types(c) for db, c in catalogs.items()}
    # (label, corrected rows, official rows) as fetched, for the scorer cross-check.
    pairs: list[tuple[str, Any, Any]] = []
    rows = json.loads((data.DATA_DIR / data.CORRECTED.name).read_text(encoding="utf-8"))
    wording = {q.id: (q.question, q.evidence) for q in data.official_questions()}

    def work(r: dict[str, Any]) -> dict[str, Any]:
        qid, db_id, source = int(r["question_id"]), str(r["db_id"]), str(r["SQL"])
        item: dict[str, Any] = {
            "id": qid,
            "db_id": db_id,
            "sqlite": source,
            # Arcwise keeps BIRD dev's original wording; Mini-Dev reworded some questions,
            # and a reworded question's gold may differ because the question does.
            "same_wording": wording.get(qid) == (str(r["question"]), str(r.get("evidence") or "")),
        }
        try:
            item["postgres"] = transpile(source, types.get(db_id))
        except TranspileError as exc:
            return item | {"status": "transpile_error", "error": str(exc)}
        sqlite_path = minidev / "MINIDEV" / "dev_databases" / db_id / f"{db_id}.sqlite"
        try:
            expected = cached_sqlite(sqlite_path, source, minidev.parent / "sqlite-results")
        except sqlite3.Error as exc:
            return item | {"status": "sqlite_error", "error": str(exc)}
        outcome = _run_pg(executors[db_id], item["postgres"])
        if isinstance(outcome, ExecutionError):
            return item | {"status": "postgres_error", "error": outcome.feedback()}
        item["rows"] = len(outcome.rows)
        if not same_rows_approx(outcome, expected):
            return item | {
                "status": "mismatch",
                "sqlite_rows": len(expected),
                "sqlite_sample": [repr(x) for x in expected[:3]],
                "postgres_sample": [repr(x) for x in outcome.rows[:3]],
            }
        item["status"] = "verified"
        gold = official.get(qid)
        item["differs_from_official"] = (
            None if gold is None else not execution_match(outcome.rows, gold.rows)
        )
        if gold is not None:
            pairs.append((f"corrected/{qid}", outcome.rows, gold.rows))
        return item

    started = time.perf_counter()
    items = _parallel(rows, work)
    by_status: dict[str, int] = {}
    for i in items:
        by_status[i["status"]] = by_status.get(i["status"], 0) + 1
    verified = [i for i in items if i["status"] == "verified"]
    return {
        "total": len(items),
        "verified": len(verified),
        "by_status": dict(sorted(by_status.items())),
        "differs_from_official": sum(1 for i in verified if i["differs_from_official"]),
        "same_wording": sum(1 for i in verified if i["same_wording"]),
        "differs_with_same_wording": sum(
            1 for i in verified if i["same_wording"] and i["differs_from_official"]
        ),
        "same_as_official": sum(1 for i in verified if i["differs_from_official"] is False),
        "official_gold_missing": sum(1 for i in verified if i["differs_from_official"] is None),
        "seconds": round(time.perf_counter() - started, 1),
        "items": sorted(items, key=lambda i: i["id"]),
        "pairs": pairs,
    }


def verify_all(
    settings: Settings, minidev: Path, *, load_summary: Mapping[str, Any]
) -> dict[str, Any]:
    """The report ``make bird`` writes to ``eval/results/bird-gold.json`` (Any: JSON)."""
    executors = gold_executors(settings)
    official_report, official_results = verify_official(executors)
    log.info("bird.official_verified", executed=official_report["executed"])
    from eval.bird.load import load_catalogs

    corrected = verify_corrected(executors, minidev, official_results, load_catalogs())
    log.info("bird.corrected_verified", verified=corrected["verified"])
    pairs = sorted(corrected.pop("pairs"), key=lambda p: p[0])
    try:
        official = official_calculate_ex(settings.bird_cache_dir)
    except ScorerUnavailable as exc:
        log.warning("bird.cross_check_skipped", error=str(exc))
        check = None
    else:
        check = asdict(cross_check(pairs, official))
        # Pairs whose verdict depends only on value types (numeric vs float8 and the like).
        check["type_decided"] = [
            label for label, a, b in pairs if execution_match(a, b) != lenient_match(a, b)
        ]
    return {
        "load": dict(load_summary),
        "official": official_report,
        "corrected": corrected,
        # Each verified corrected gold graded against its official gold by BIRD's own
        # calculate_ex (rows as fetched) and by this harness's scorer (rows normalised).
        "cross_check": check,
    }


def write_transpiled(report: Mapping[str, Any], path: Path = data.TRANSPILED) -> None:
    """The corrected track's PostgreSQL gold, committed so the track runs without SQLite."""
    # Status and SQL only: an error's text can name whichever bad value a parallel scan
    # met first, and this file must be byte-stable (the run report keeps the errors).
    questions = {
        str(i["id"]): {"status": i["status"], "postgres": i.get("postgres")}
        for i in report["corrected"]["items"]
    }
    document = {
        "transpiled_from": data.CORRECTED.name,
        "verified": report["corrected"]["verified"],
        "questions": questions,
    }
    path.write_text(json.dumps(document, indent=1, sort_keys=True, ensure_ascii=False) + "\n")
