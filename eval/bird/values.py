"""The BIRD value index: the distinct text values of the eleven databases, for value retrieval.

    uv run python -m eval.bird.values              # build eval/bird/data/values.json.gz
    uv run python -m eval.bird.values --coverage   # how often questions match (offline)

BIRD's catalogs come from ``information_schema`` and carry no values, so this
module reads them from the loaded ``bird`` database (``make bird``): for every
text column, the distinct values whose trimmed length is 3 to 100 characters
and that contain a letter. A column with more than ``MAX_DISTINCT`` such values
is skipped whole and listed in the document's ``skipped``. The committed file
is deterministic (sorted keys and values, gzip mtime 0), so a rebuild of the
same databases yields the same bytes, and the replayed evaluation reads it with
no database (:func:`load_value_indexes`).
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import psycopg
import structlog
from psycopg import sql

from eval.bird import data
from tributary.agent.catalog import Catalog
from tributary.agent.values import ValueIndex, indexable
from tributary.config import get_settings
from tributary.logging import bind_run_id, configure_logging

log = structlog.get_logger(__name__)

VALUES = data.DATA_DIR / "values.json.gz"
# Above this a column is skipped. 50,000 keeps the name columns questions cite most
# (users.displayname, posts.title, cards.name, player.player_name) under the size limit.
MAX_DISTINCT = 50_000
MAX_BYTES = 5_000_000  # the committed file's hard limit
TEXT_TYPES = frozenset({"text", "character varying", "character", "varchar", "char", "bpchar"})

_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def skip_reason(values: Sequence[str], fetched: int, max_distinct: int) -> str | None:
    """Why a column is not indexed, or None: too many values, identifiers, or URLs.

    UUIDs and URLs are half the bytes of BIRD's text values and no question names
    one by anything but copying it, which a model does without help.
    """
    if fetched > max_distinct:
        return f"more than {max_distinct} distinct values"
    if not values:
        return None
    if sum(bool(_UUID.match(v.strip())) for v in values) * 2 > len(values):
        return "identifiers (mostly UUIDs)"
    if sum("://" in v for v in values) * 2 > len(values):
        return "URLs"
    return None


_DISTINCT_SQL = (
    "SELECT DISTINCT {col} FROM {schema}.{table}"
    " WHERE {col} IS NOT NULL AND length(btrim({col})) BETWEEN 3 AND 100"
    " AND {col} ~ '[[:alpha:]]' LIMIT %s"
)


def build_value_indexes(
    conn: psycopg.Connection[Any],
    catalogs: Mapping[str, Catalog],
    *,
    max_distinct: int = MAX_DISTINCT,
    skipped: list[dict[str, Any]] | None = None,
) -> dict[str, ValueIndex]:
    """One index per database; columns :func:`skip_reason` rejects go to ``skipped``."""
    # Any: the connection's row type, and JSON entries in skipped.
    indexes: dict[str, ValueIndex] = {}
    for db_id in sorted(catalogs):
        columns: dict[tuple[str, str], tuple[str, ...]] = {}
        for table in catalogs[db_id].tables:
            for column in table.columns:
                if column.data_type not in TEXT_TYPES:
                    continue
                query = sql.SQL(_DISTINCT_SQL).format(
                    col=sql.Identifier(column.name),
                    schema=sql.Identifier(table.schema),
                    table=sql.Identifier(table.name),
                )
                rows = conn.execute(query, (max_distinct + 1,)).fetchall()
                found = sorted({str(r[0]) for r in rows if indexable(str(r[0]))})
                reason = skip_reason(found, len(rows), max_distinct)
                if reason is not None:
                    if skipped is not None:
                        skipped.append(
                            {
                                "database": db_id,
                                "relation": table.relation,
                                "column": column.name,
                                "reason": reason,
                            }
                        )
                    log.info(
                        "bird.values_skipped",
                        relation=table.relation,
                        column=column.name,
                        reason=reason,
                    )
                    continue
                if found:
                    columns[(table.relation, column.name)] = tuple(found)
        indexes[db_id] = ValueIndex(columns)
        log.info(
            "bird.values_indexed",
            database=db_id,
            columns=len(columns),
            values=sum(len(v) for v in columns.values()),
        )
    return indexes


def encode(
    indexes: Mapping[str, ValueIndex], skipped: Sequence[Mapping[str, Any]], max_distinct: int
) -> bytes:
    """The committed file's bytes: sorted JSON, gzip with mtime 0 (reproducible)."""
    # Any: JSON entries.
    document = {
        "databases": {db: indexes[db].to_dict() for db in sorted(indexes)},
        "max_distinct": max_distinct,
        "skipped": sorted((dict(s) for s in skipped), key=lambda s: (s["relation"], s["column"])),
    }
    text = json.dumps(document, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, mtime=0, compresslevel=9) as gz:
        gz.write(text.encode("utf-8"))
    return buffer.getvalue()


def load_document(path: Path = VALUES) -> dict[str, Any]:
    # Any: the JSON document encode writes.
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        document: dict[str, Any] = json.load(fh)
    return document


def load_value_indexes(path: Path = VALUES) -> dict[str, ValueIndex]:
    """The committed value indexes, one per BIRD database (offline)."""
    document = load_document(path)
    return {db: ValueIndex.from_dict(entry) for db, entry in document["databases"].items()}


# --- coverage -----------------------------------------------------------------------------------

_LITERAL = re.compile(r"'((?:[^']|'')*)'")


def gold_literals(gold: str) -> set[str]:
    """The string literals of a gold query, unescaped."""
    return {m.group(1).replace("''", "'") for m in _LITERAL.finditer(gold)}


def coverage(
    indexes: Mapping[str, ValueIndex], subset: str | None = "dev50", *, limit: int = 8
) -> dict[str, Any]:
    """How many questions match a value, and how many matches the gold query uses.

    ``useful``: at least one matched value is a string literal of the gold SQL
    (crude: a gold ``LIKE '%x%'`` or a numeric literal never counts).
    ``gold_literals_found``: of the gold's string literals that are indexable,
    the share some match of the question names (a recall).
    """
    # Any: a JSON-ready summary.
    questions = data.select(data.questions("official"), subset)
    matched = useful = wanted = found = 0
    with_literal = 0
    for q in questions:
        index = indexes.get(q.db_id)
        matches = index.match(q.prompt, limit=limit) if index is not None else ()
        values = {m.value for m in matches}
        literals = gold_literals(q.sql)
        matched += bool(matches)
        useful += bool(values & literals)
        candidates = {lit for lit in literals if indexable(lit)}
        with_literal += bool(candidates)
        wanted += len(candidates)
        found += len(candidates & values)
    return {
        "subset": subset or "all",
        "questions": len(questions),
        "with_match": matched,
        "useful": useful,
        "with_indexable_gold_literal": with_literal,
        "gold_literals": wanted,
        "gold_literals_found": found,
    }


def _print_coverage(summary: Mapping[str, Any]) -> None:
    n = summary["questions"]
    print(
        f"{summary['subset']}: {summary['with_match']}/{n} questions match a value; "
        f"{summary['useful']}/{n} have a matched value as a gold string literal; "
        f"{summary['gold_literals_found']}/{summary['gold_literals']} indexable gold literals "
        f"named by a match ({summary['with_indexable_gold_literal']} questions have one)"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--coverage", action="store_true", help="report match coverage only")
    parser.add_argument("--max-distinct", type=int, default=MAX_DISTINCT)
    parser.add_argument("--output", type=Path, default=VALUES)
    args = parser.parse_args(argv)
    if args.coverage:
        indexes = load_value_indexes(args.output)
        for subset in ("dev50", None):
            _print_coverage(coverage(indexes, subset))
        return 0
    settings = get_settings()
    configure_logging(settings.log_format, settings.log_level)
    bind_run_id("bird-values")
    from eval.bird.load import bird_conninfo, load_catalogs

    started = time.monotonic()
    skipped: list[dict[str, Any]] = []
    with psycopg.connect(bird_conninfo(settings, as_reader=False)) as conn:
        conn.read_only = True
        indexes = build_value_indexes(
            conn, load_catalogs(), max_distinct=args.max_distinct, skipped=skipped
        )
    payload = encode(indexes, skipped, args.max_distinct)
    if len(payload) > MAX_BYTES:
        print(f"values: {len(payload)} bytes is over the {MAX_BYTES}-byte limit", file=sys.stderr)
        return 1
    args.output.write_bytes(payload)
    kept = sum(len(i.columns) for i in indexes.values())
    print(
        f"values: {sum(len(i) for i in indexes.values())} values in {kept} columns, "
        f"{len(skipped)} columns skipped (see the file's skipped); "
        f"{len(payload)} bytes in {args.output} ({time.monotonic() - started:.1f} s)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
