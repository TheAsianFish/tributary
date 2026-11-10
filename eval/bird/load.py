"""``make bird``: the eleven BIRD Mini-Dev databases in PostgreSQL, ready for the agent.

    uv run python -m eval.bird.load [--source dump|sqlite] [--fresh]

Steps, each idempotent:

1. Download ``minidev.zip`` into the cache (pinned sha256) and unpack the
   databases and the dump.
2. Create the database ``bird`` (``TRIBUTARY_BIRD_DATABASE``) and fill one
   schema per BIRD database (``california_schools.frpm``, ...). The official
   PostgreSQL dump puts all 75 tables in ``public``; they are restored there
   and moved into their database's schema, so an agent asked about one
   database sees only its tables, and an unqualified gold query resolves
   through ``search_path`` exactly as it does in the official setup.
   ``--source sqlite`` is the fallback for when the dump is unavailable: each
   SQLite file is converted (names lowercased where the dump lowercases them,
   types mapped), and the gold verification below says what that costs.
3. Create the read-only role ``tributary_bird`` (``SELECT`` on the eleven
   schemas, read-only sessions, no ``TEMPORARY``): every predicted and gold
   query runs as it.
4. Write the catalog the agent sees (``eval/bird/data/catalogs.json``): each
   database's tables, types and keys from ``information_schema``, with
   BIRD's column descriptions from its CSV files.
5. Run every official gold query and transpile and verify the corrected
   track (:mod:`eval.bird.corrected`); the report goes to
   ``eval/results/bird-gold.json``.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
import structlog
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from eval.bird import data
from tributary.agent.catalog import Catalog, from_information_schema
from tributary.agent.roles import agent_role_statements
from tributary.config import REPO_ROOT, Settings, get_settings
from tributary.logging import bind_run_id, configure_logging

log = structlog.get_logger(__name__)

CATALOGS = data.DATA_DIR / "catalogs.json"
GOLD_REPORT = REPO_ROOT / "eval" / "results" / "bird-gold.json"
MARKER_TABLE = "bird_load"


def bird_conninfo(settings: Settings, *, as_reader: bool) -> str:
    """The ``bird`` database as the owner (loading) or as the read-only role (queries)."""
    if as_reader:
        user, password = settings.bird_user, settings.bird_password.get_secret_value()
    else:
        user, password = settings.pg_user, settings.pg_password.get_secret_value()
    return make_conninfo(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.bird_database,
        user=user,
        password=password,
    )


def ensure_database(settings: Settings, *, fresh: bool) -> None:
    with psycopg.connect(settings.pg_conninfo, autocommit=True) as conn:
        name = settings.bird_database
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
        if exists and fresh:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
            exists = None
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            log.info("bird.database_created", database=name)


# --- the official dump --------------------------------------------------------------------------

_SKIP = re.compile(r"\bOWNER TO\b|^(GRANT|REVOKE)\b", re.IGNORECASE)
_COPY = re.compile(r"^COPY\s+.+\s+FROM\s+stdin;\s*$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class DumpStatement:
    sql: str
    copy_rows: tuple[str, ...] | None = None


def dump_statements(lines: Iterable[str]) -> Iterator[DumpStatement]:
    """Split a plain ``pg_dump`` file into statements and ``COPY ... FROM stdin`` blocks.

    Statements end with ``;`` at the end of a line (pg_dump writes them so; the
    BIRD dump defines no functions, so there are no dollar-quoted bodies to
    track). A ``COPY`` block's data lines run up to ``\\.`` and are yielded
    with it. Ownership and privilege statements are dropped: they name the
    dump author's role, which does not exist here.
    """
    buffer: list[str] = []
    iterator = iter(lines)
    for line in iterator:
        if not buffer and (not line.strip() or line.startswith("--")):
            continue
        buffer.append(line)
        if not line.rstrip().endswith(";"):
            continue
        statement = "".join(buffer).strip()
        buffer = []
        if _COPY.match(statement):
            rows = []
            for row in iterator:
                if row.rstrip("\n") == "\\.":
                    break
                rows.append(row)
            else:
                raise ValueError(f"unterminated COPY block: {statement[:80]}")
            yield DumpStatement(statement, tuple(rows))
        elif not _SKIP.search(statement):
            yield DumpStatement(statement)
    if "".join(buffer).strip():
        raise ValueError(f"dump ends inside a statement: {''.join(buffer)[:80]}")


def restore_dump(conn: psycopg.Connection[Any], dump: Path) -> int:
    """Restore the dump into ``public``; returns the number of rows copied."""
    copied = 0
    with dump.open(encoding="utf-8") as fh, conn.cursor() as cur:
        for statement in dump_statements(fh):
            if statement.copy_rows is None:
                cur.execute(statement.sql.encode())
                continue
            with cur.copy(statement.sql.encode()) as copy:
                for start in range(0, len(statement.copy_rows), 5000):
                    copy.write("".join(statement.copy_rows[start : start + 5000]))
            copied += len(statement.copy_rows)
    return copied


def table_databases(minidev: Path) -> dict[str, str]:
    """Table name (lowercase) → BIRD database, from ``dev_tables.json``."""
    with (minidev / "MINIDEV" / "dev_tables.json").open(encoding="utf-8") as fh:
        tables = json.load(fh)
    owner: dict[str, str] = {}
    for entry in tables:
        for name in entry["table_names_original"]:
            key = str(name).lower()
            if key in owner and owner[key] != entry["db_id"]:
                raise ValueError(f"table {key} is in both {owner[key]} and {entry['db_id']}")
            owner[key] = str(entry["db_id"])
    return owner


def move_to_schemas(conn: psycopg.Connection[Any], owner: Mapping[str, str]) -> None:
    rows = conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
    ).fetchall()
    unknown = [r[0] for r in rows if r[0] not in owner and r[0] != MARKER_TABLE]
    if unknown:
        raise ValueError(f"tables in the dump with no BIRD database: {unknown}")
    for db_id in sorted(set(owner.values())):
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(db_id)))
    for (table,) in rows:
        if table == MARKER_TABLE:
            continue
        conn.execute(
            sql.SQL("ALTER TABLE public.{} SET SCHEMA {}").format(
                sql.Identifier(table), sql.Identifier(owner[table])
            )
        )


# --- the SQLite fallback ------------------------------------------------------------------------

_PLAIN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def pg_name(name: str) -> str:
    """The dump's naming rule: plain identifiers lowercased, quoted ones kept as written."""
    return name.lower() if _PLAIN.match(name) else name


def pg_type(declared: str) -> str:
    """PostgreSQL type for a SQLite declared type, as the dump maps them."""
    t = declared.strip().upper()
    if "INT" in t:
        return "bigint"
    if any(k in t for k in ("REAL", "FLOA", "DOUB")):
        return "real"
    if any(k in t for k in ("NUMERIC", "DECIMAL")):
        return "numeric"
    if t == "DATE":
        return "date"
    if t in ("DATETIME", "TIMESTAMP"):
        return "timestamp"
    return "text"


def convert_sqlite(conn: psycopg.Connection[Any], sqlite_path: Path, schema: str) -> int:
    """Copy one SQLite database into ``schema``; returns rows copied.

    A column whose values do not parse as its mapped type (SQLite stores
    whatever it is given) is created as ``text`` instead, and the table is
    copied again.
    """
    source = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    copied = 0
    try:
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        tables = [
            r[0]
            for r in source.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
                " AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        for table in tables:
            info = source.execute(f'PRAGMA table_info("{table}")').fetchall()
            columns = [(str(c[1]), pg_type(str(c[2] or ""))) for c in info]
            pk = [str(c[1]) for c in sorted(info, key=lambda c: c[5]) if c[5]]
            for attempt in range(2):
                types = columns if attempt == 0 else [(n, "text") for n, _ in columns]
                try:
                    with conn.transaction():
                        copied += _copy_table(conn, source, schema, table, types, pk)
                    break
                except psycopg.errors.DataError as exc:
                    if attempt:
                        raise
                    log.warning("bird.sqlite_retry_as_text", table=table, error=str(exc)[:120])
    finally:
        source.close()
    return copied


def _copy_table(
    conn: psycopg.Connection[Any],
    source: sqlite3.Connection,
    schema: str,
    table: str,
    columns: Sequence[tuple[str, str]],
    pk: Sequence[str],
) -> int:
    target = sql.Identifier(schema, pg_name(table))
    definitions = [
        sql.SQL("{} {}").format(sql.Identifier(pg_name(n)), sql.SQL(t)) for n, t in columns
    ]
    conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target))
    conn.execute(sql.SQL("CREATE TABLE {} ({})").format(target, sql.SQL(", ").join(definitions)))
    names = sql.SQL(", ").join(sql.Identifier(pg_name(n)) for n, _ in columns)
    selected = ", ".join('"' + n.replace('"', '""') + '"' for n, _ in columns)
    count = 0
    with conn.cursor().copy(sql.SQL("COPY {} ({}) FROM STDIN").format(target, names)) as copy:
        for row in source.execute(f'SELECT {selected} FROM "{table}"'):
            copy.write_row(tuple(_text(v) for v in row))
            count += 1
    if pk:
        # BIRD's SQLite keys are not always unique; a key that does not hold is left out.
        try:
            with conn.transaction():
                conn.execute(
                    sql.SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(
                        target, sql.SQL(", ").join(sql.Identifier(pg_name(c)) for c in pk)
                    )
                )
        except psycopg.Error as exc:
            log.warning("bird.sqlite_key_dropped", table=table, error=str(exc)[:120])
    return count


def _text(value: object) -> object:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value


# --- role, descriptions, catalogs ---------------------------------------------------------------


def grant_reader(conn: psycopg.Connection[Any], settings: Settings) -> None:
    user = settings.bird_user
    if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,)).fetchone():
        conn.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(user)))
    for statement in agent_role_statements(
        user,
        settings.bird_password.get_secret_value(),
        settings.bird_database,
        data.DATABASES,
    ):
        conn.execute(statement)
    # The role may read public's marker table no more than anything else in public.
    conn.execute(sql.SQL("REVOKE ALL ON SCHEMA public FROM PUBLIC"))


def read_descriptions(minidev: Path, db_id: str) -> dict[str, dict[str, str]]:
    """BIRD's description CSVs as ``{relation: {"": table text, column: text}}``.

    A column's text is its description, then its value description (BIRD often
    writes ``commonsense evidence:`` notes there); either is left out when it
    only repeats the column's name.
    The CSVs come in assorted encodings; undecodable bytes are replaced.
    """
    folder = minidev / "MINIDEV" / "dev_databases" / db_id / "database_description"
    out: dict[str, dict[str, str]] = {}
    for path in sorted(folder.glob("*.csv")):
        table = path.stem.lower()
        raw = path.read_bytes()
        for encoding in ("utf-8-sig", "cp1252"):
            try:
                text = raw.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            text = raw.decode("utf-8", errors="replace")
        columns: dict[str, str] = {}
        for row in csv.DictReader(text.splitlines()):
            name = (row.get("original_column_name") or "").strip()
            if not name:
                continue
            parts = [
                " ".join((row.get("column_description") or "").split()),
                " ".join((row.get("value_description") or "").split()),
            ]
            # A description that only repeats the column's name tells the model nothing.
            echoes = {_bare(name), _bare(row.get("column_name") or "")}
            parts = [p for p in parts if p and _bare(p) not in echoes]
            columns[pg_name(name)] = ". ".join(p.rstrip(".") for p in parts) + (
                "." if parts else ""
            )
        out[f"{db_id}.{table}"] = columns
    return out


def _bare(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.lower())


def build_catalogs(conn: psycopg.Connection[Any], minidev: Path) -> dict[str, Catalog]:
    def query(statement: sql.Composed, params: Sequence[object]) -> list[tuple[Any, ...]]:
        return conn.execute(statement, params).fetchall()

    return {
        db_id: from_information_schema(
            query, [db_id], descriptions=read_descriptions(minidev, db_id)
        )
        for db_id in data.DATABASES
    }


def write_catalogs(catalogs: Mapping[str, Catalog], path: Path = CATALOGS) -> None:
    document = {db: catalogs[db].to_dict() for db in sorted(catalogs)}
    path.write_text(json.dumps(document, indent=1, sort_keys=True, ensure_ascii=False) + "\n")


def load_catalogs(path: Path = CATALOGS) -> dict[str, Catalog]:
    """The committed catalogs, one per BIRD database (what ``make bird`` wrote)."""
    with path.open(encoding="utf-8") as fh:
        document = json.load(fh)
    return {db: Catalog.from_dict(entry) for db, entry in document.items()}


# --- the whole load -----------------------------------------------------------------------------


def loaded_source(conn: psycopg.Connection[Any]) -> str | None:
    exists = conn.execute(
        "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = %s", (MARKER_TABLE,)
    ).fetchone()
    if not exists:
        return None
    row = conn.execute(sql.SQL("SELECT source FROM public.{}").format(sql.Identifier(MARKER_TABLE)))
    found = row.fetchone()
    return None if found is None else str(found[0])


def load(settings: Settings, *, source: str, fresh: bool, cache: Path) -> dict[str, Any]:
    """Download, restore or convert, grant, describe; returns a summary (Any: JSON)."""
    zip_path = data.fetch(data.MINIDEV_ZIP, cache)
    minidev = data.unpack(zip_path, cache)
    ensure_database(settings, fresh=fresh)
    started = time.perf_counter()
    rows = 0
    with psycopg.connect(bird_conninfo(settings, as_reader=False), autocommit=True) as conn:
        found = loaded_source(conn)
        if found is not None and found != source:
            raise SystemExit(
                f"the {settings.bird_database} database holds the {found} load; "
                f"rerun with --fresh to replace it with --source {source}"
            )
        if found is None:
            if source == "dump":
                with conn.transaction():
                    rows = restore_dump(conn, minidev / "MINIDEV_postgresql" / "BIRD_dev.sql")
                    move_to_schemas(conn, table_databases(minidev))
            else:
                for db_id in data.DATABASES:
                    path = minidev / "MINIDEV" / "dev_databases" / db_id / f"{db_id}.sqlite"
                    rows += convert_sqlite(conn, path, db_id)
            conn.execute(
                sql.SQL("CREATE TABLE public.{} (source text NOT NULL, rows bigint)").format(
                    sql.Identifier(MARKER_TABLE)
                )
            )
            conn.execute(
                sql.SQL("INSERT INTO public.{} VALUES (%s, %s)").format(
                    sql.Identifier(MARKER_TABLE)
                ),
                (source, rows),
            )
            conn.execute("ANALYZE")
            log.info("bird.loaded", source=source, rows=rows, seconds=time.perf_counter() - started)
        grant_reader(conn, settings)
        catalogs = build_catalogs(conn, minidev)
    return {
        "source": source,
        "rows_copied": rows,
        "seconds": round(time.perf_counter() - started, 1),
        "tables": {db: len(c.tables) for db, c in catalogs.items()},
        "catalogs": catalogs,
        "minidev": minidev,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--source", choices=["dump", "sqlite"], default="dump")
    parser.add_argument("--fresh", action="store_true", help="drop and reload the database")
    parser.add_argument("--cache", type=Path, default=None)
    parser.add_argument(
        "--no-write", action="store_true", help="do not rewrite the committed catalogs/report"
    )
    parser.add_argument("--report", type=Path, default=None, help="gold report path")
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_format, settings.log_level)
    bind_run_id("bird-load")
    cache = args.cache or settings.bird_cache_dir
    summary = load(settings, source=args.source, fresh=args.fresh, cache=cache)
    catalogs: dict[str, Catalog] = summary.pop("catalogs")
    minidev: Path = summary.pop("minidev")
    if not args.no_write:
        write_catalogs(catalogs)
    from eval.bird.corrected import verify_all

    report = verify_all(settings, minidev, load_summary=summary)
    if args.report is not None:
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if not args.no_write:
        GOLD_REPORT.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        from eval.bird.corrected import write_transpiled

        write_transpiled(report)
    official = report["official"]
    corrected = report["corrected"]
    print(
        f"bird: {sum(summary['tables'].values())} tables in {len(summary['tables'])} schemas "
        f"({args.source}, {summary['seconds']} s); official gold: {official['executed']}/"
        f"{official['total']} execute; corrected: {corrected['verified']}/{corrected['total']} "
        "transpiled and verified"
    )
    host = conninfo_to_dict(bird_conninfo(settings, as_reader=True)).get("host")
    print(f"queries run as {settings.bird_user} on {host}/{settings.bird_database}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
