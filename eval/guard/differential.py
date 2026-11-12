"""A differential oracle for the guard: PostgreSQL's own name resolution as the judge.

    uv run python -m eval.guard.differential run --label after   (PostgreSQL, owner role)
    make eval-guard-diff

Every guard bypass so far was one class: sqlglot and PostgreSQL read the same
text differently, so the guard approved a call it never saw as a call
(``(x).f``, ``p.f`` over a function in FROM, an alias rebound in a nested
scope). This finds that class mechanically instead of by reading code.

**Generator.** Every function in ``pg_proc`` callable with one argument is
crossed with the spelling templates in ``eval/guard/templates.yaml`` (committed
before the guard change this was built to test) in four positions, and with
the same rewrites applied to the 40 warehouse gold queries.

**Oracle.** Each statement the guard admits is never run. Its regenerated SQL
(what the agent would execute) is wrapped as ``SELECT count(*) FROM (...) AS
q`` (so any output columns make a valid view), created as a temporary view by
the owner role inside a transaction that is rolled back, with the agent's
empty ``search_path``. Creating a view runs PostgreSQL's parse analysis and
stores the resolved tree in ``pg_rewrite.ev_action``; the function,
aggregate, window function and operator ids in it are what the statement
would call. Each is judged against the guard's own policy
(:func:`tributary.agent.guard.function_refusal`), and every relation against
the allowlist. A statement with a call the policy refuses is *flagged*: the
guard admitted something it would have refused had it seen it.

**What it cannot see**, counted rather than left out: a cast of a literal is
folded by the parser into a constant (the type's input function already ran,
and no id is left), and an I/O conversion cast (``COERCEVIAIO``) calls an
output and an input function that the tree names by type, not by id. Both
are pure type I/O for the types the guard admits, but they are counted per
run so a reader knows how much of the admitted set they touch.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from sqlglot import exp
from sqlglot import parse_one as sqlglot_parse_one

from tributary.agent.guard import (
    DIALECT,
    HISTORICAL_FIXES,
    Guard,
    fixes_disabled,
    function_refusal,
)

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES_PATH = ROOT / "eval" / "guard" / "templates.yaml"
GOLD_PATH = ROOT / "eval" / "warehouse_questions.yaml"
RESULTS_DIR = ROOT / "eval" / "results" / "guard-diff"
VIEW = "tributary_guard_oracle"

# -- the catalog's view of a function ---------------------------------------------------


@dataclass(frozen=True, slots=True)
class Proc:
    """One row of ``pg_proc``, as the oracle needs it."""

    oid: int
    schema: str
    name: str
    kind: str  # f function, a aggregate, w window, p procedure
    volatility: str  # i immutable, s stable, v volatile
    arg_type: str  # the first argument's type, as regtype prints it


@dataclass(frozen=True, slots=True)
class Operator:
    oid: int
    schema: str
    name: str
    function: int  # oprcode


@dataclass(slots=True)
class Catalog:
    """Name lookups for the ids found in ``ev_action``; filled lazily from the database."""

    procs: dict[int, Proc] = field(default_factory=dict)
    operators: dict[int, Operator] = field(default_factory=dict)
    relations: dict[int, str] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        """Any: a JSON document of three id-keyed tables."""
        return {
            "procs": {str(k): list(_proc_row(v)) for k, v in sorted(self.procs.items())},
            "operators": {
                str(k): [v.schema, v.name, v.function] for k, v in sorted(self.operators.items())
            },
            "relations": {str(k): v for k, v in sorted(self.relations.items())},
        }

    @classmethod
    def from_json(cls, document: dict[str, Any]) -> Catalog:
        """Any: the document :meth:`to_json` wrote."""
        procs = {
            int(k): Proc(int(k), str(v[0]), str(v[1]), str(v[2]), str(v[3]), str(v[4]))
            for k, v in document.get("procs", {}).items()
        }
        operators = {
            int(k): Operator(int(k), str(v[0]), str(v[1]), int(v[2]))
            for k, v in document.get("operators", {}).items()
        }
        relations = {int(k): str(v) for k, v in document.get("relations", {}).items()}
        return cls(procs, operators, relations)


def _proc_row(proc: Proc) -> tuple[str, str, str, str, str]:
    return (proc.schema, proc.name, proc.kind, proc.volatility, proc.arg_type)


# -- ev_action: what PostgreSQL resolved -------------------------------------------------

# Node-tree text: "{NAME :field value ...}", lists in "(...)", byte arrays in "[...]".
# outToken escapes special characters inside names with a backslash.
_ESCAPED = re.compile(r"\\.")
_TOKEN = re.compile(r"[{}()\[\]]|[^\s{}()\[\]]+")
_LIST_TAGS = frozenset({"o", "i", "b", "x"})


def _nodes(text: str) -> Iterator[tuple[str, dict[str, int | tuple[int, ...]]]]:
    """Each node in a node-tree text with its own scalar and id-list fields, innermost first."""
    tokens = _TOKEN.findall(_ESCAPED.sub("_", text))
    stack: list[tuple[str, dict[str, int | tuple[int, ...]]]] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok == "{" and i + 1 < len(tokens):
            stack.append((tokens[i + 1], {}))
            i += 2
            continue
        if tok == "}":
            if stack:
                yield stack.pop()
            i += 1
            continue
        if tok == "[":
            while i < len(tokens) and tokens[i] != "]":
                i += 1
            i += 1
            continue
        if tok.startswith(":") and stack and i + 1 < len(tokens):
            key, value = tok[1:], tokens[i + 1]
            if value == "(" and i + 2 < len(tokens) and tokens[i + 2] in _LIST_TAGS:
                j = i + 3
                items: list[int] = []
                while j < len(tokens) and tokens[j] != ")":
                    if tokens[j].lstrip("-").isdigit():
                        items.append(int(tokens[j]))
                    j += 1
                stack[-1][1][key] = tuple(items)
                i = j + 1
                continue
            if value.lstrip("-").isdigit():
                stack[-1][1][key] = int(value)
                i += 2
                continue
        i += 1


# FuncExpr.funcformat (CoercionForm): 0 an explicit call, 1 an explicit cast, 2 an implicit
# cast, 3 SQL syntax (EXTRACT, SUBSTRING ... FROM, AT TIME ZONE).
_CALL, _EXPLICIT_CAST, _IMPLICIT_CAST, _SQL_SYNTAX = 0, 1, 2, 3

# SQLValueFunction.op values that name the session's identity or schema (PostgreSQL 17's
# SVFOP_CURRENT_ROLE .. SVFOP_CURRENT_SCHEMA): the guard denies them by name.
_IDENTITY_SVF = {9: "current_role", 10: "current_user", 11: "user", 12: "session_user"}
_IDENTITY_SVF |= {13: "current_catalog", 14: "current_schema"}

_OPERATOR_NODES = frozenset({"OPEXPR", "DISTINCTEXPR", "NULLIFEXPR", "SCALARARRAYOPEXPR"})


@dataclass(frozen=True, slots=True)
class Reference:
    """One thing a resolved statement calls or reads."""

    kind: str  # call, cast, syntax, aggregate, window, operator, relation, sql_function, xml
    oid: int
    how: str = ""  # the node it came from


@dataclass(frozen=True, slots=True)
class Resolution:
    """What one ``ev_action`` holds: references, and the node types it contains."""

    references: tuple[Reference, ...]
    nodes: dict[str, int]

    @property
    def io_casts(self) -> int:
        return self.nodes.get("COERCEVIAIO", 0)


def parse_ev_action(text: str) -> Resolution:
    """Every function, aggregate, window function, operator and relation in a rule's tree.

    ``text`` is ``pg_rewrite.ev_action`` (PostgreSQL's node-tree text); each
    node's fields are read at its own level, not its children's.
    """
    references: list[Reference] = []
    nodes: Counter[str] = Counter()
    for name, raw in _nodes(text):
        nodes[name] += 1
        fields = {k: v for k, v in raw.items() if isinstance(v, int)}
        if name == "FUNCEXPR":
            form = fields.get("funcformat", _CALL)
            kind = {
                _CALL: "call",
                _EXPLICIT_CAST: "cast",
                _IMPLICIT_CAST: "cast",
                _SQL_SYNTAX: "syntax",
            }.get(form, "call")
            references.append(Reference(kind, fields["funcid"], name))
        elif name == "AGGREF":
            references.append(Reference("aggregate", fields["aggfnoid"], name))
        elif name == "WINDOWFUNC":
            references.append(Reference("window", fields["winfnoid"], name))
        elif name in _OPERATOR_NODES:
            references.append(Reference("operator", fields["opno"], name))
            for key in ("hashfuncid", "negfuncid"):
                if fields.get(key):
                    references.append(Reference("syntax", fields[key], name))
        elif name == "SORTGROUPCLAUSE":
            for key in ("eqop", "sortop"):
                if fields.get(key):
                    references.append(Reference("operator", fields[key], name))
        elif name == "WINDOWCLAUSE":
            for key in ("startInRangeFunc", "endInRangeFunc"):
                if fields.get(key):
                    references.append(Reference("syntax", fields[key], name))
        elif name == "ROWCOMPAREEXPR":
            opnos = raw.get("opnos", ())
            for opno in opnos if isinstance(opnos, tuple) else ():
                references.append(Reference("operator", opno, name))
        elif name == "SQLVALUEFUNCTION" and fields.get("op") in _IDENTITY_SVF:
            references.append(Reference("sql_function", fields["op"], name))
        elif name in {"XMLEXPR", "XMLTABLE"} or (
            name == "TABLEFUNC" and not fields.get("functype")
        ):
            references.append(Reference("xml", 0, name))
        elif name == "RANGETBLENTRY" and fields.get("relid"):
            references.append(Reference("relation", fields["relid"], name))
    return Resolution(tuple(references), dict(sorted(nodes.items())))


@dataclass(frozen=True, slots=True)
class Finding:
    """A reference the guard's policy refuses: the guard admitted something it bans."""

    rule: str  # denied, not_allowed, schema, relation, identity, xml
    name: str  # schema.name of the function, operator or relation
    kind: str
    volatility: str = ""


def judge(
    resolution: Resolution, catalog: Catalog, allowlist: frozenset[str]
) -> tuple[Finding, ...]:
    """The references in ``resolution`` that the guard's policy would have refused."""
    findings: dict[tuple[str, str, str], Finding] = {}

    def add(finding: Finding) -> None:
        findings.setdefault((finding.rule, finding.name, finding.kind), finding)

    def function(oid: int, kind: str, *, explicit: bool) -> None:
        proc = catalog.procs.get(oid)
        if proc is None:
            add(Finding("unknown", f"oid {oid}", kind))
            return
        shown = f"{proc.schema}.{proc.name}"
        if proc.schema != "pg_catalog":
            add(Finding("schema", shown, kind, proc.volatility))
        refusal = function_refusal(proc.name, explicit_call=explicit)
        if refusal:
            add(Finding(refusal, shown, kind, proc.volatility))

    for ref in resolution.references:
        if ref.kind in {"call", "aggregate", "window"}:
            function(ref.oid, ref.kind, explicit=True)
        elif ref.kind in {"cast", "syntax"}:
            function(ref.oid, ref.kind, explicit=False)
        elif ref.kind == "operator":
            op = catalog.operators.get(ref.oid)
            if op is None:
                add(Finding("unknown", f"operator {ref.oid}", ref.kind))
                continue
            if op.schema != "pg_catalog":
                add(Finding("schema", f"{op.schema}.{op.name}", ref.kind))
            if op.function:
                function(op.function, "operator", explicit=False)
        elif ref.kind == "relation":
            relation = catalog.relations.get(ref.oid, f"oid {ref.oid}")
            if relation not in allowlist:
                add(Finding("relation", relation, ref.kind))
        elif ref.kind == "sql_function":
            add(Finding("identity", _IDENTITY_SVF[ref.oid], ref.kind))
        elif ref.kind == "xml":
            add(Finding("xml", ref.how.lower(), ref.kind))
    return tuple(findings[key] for key in sorted(findings))


# -- the generator ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Case:
    """One generated statement and where it came from."""

    group: str  # template id, or gold:<rewrite>
    position: str
    function: str  # the substituted function's name ("" for an unchanged gold query)
    source: str  # the gold query's id, or ""
    sql: str


@dataclass(frozen=True, slots=True)
class Templates:
    positions: dict[str, str]
    templates: tuple[dict[str, str], ...]
    arguments: dict[str, str]
    gold_rewrites: tuple[str, ...]


def load_templates(path: Path = TEMPLATES_PATH) -> Templates:
    data = yaml.safe_load(path.read_text())
    return Templates(
        positions={str(k): str(v) for k, v in data["positions"].items()},
        templates=tuple({str(k): str(v) for k, v in t.items()} for t in data["templates"]),
        arguments={str(k): str(v) for k, v in data["arguments"].items()},
        gold_rewrites=tuple(str(r) for r in data["gold_rewrites"]),
    )


_PLAIN_NAME = re.compile(r"^[a-z_][a-z0-9_$]*$")


def quote(name: str, keywords: frozenset[str]) -> str:
    """``name`` as an identifier PostgreSQL reads back as the same name."""
    if _PLAIN_NAME.match(name) and name not in keywords:
        return name
    return '"' + name.replace('"', '""') + '"'


def generate(
    procs: Sequence[Proc], templates: Templates, keywords: frozenset[str]
) -> Iterator[Case]:
    """Every template in every position for every function: the fixed generated set."""
    for proc in procs:
        name = quote(proc.name, keywords)
        arg = templates.arguments.get(proc.arg_type, templates.arguments["default"])
        for template in templates.templates:
            expr = template["expr"].format(f=name, arg=arg)
            from_ = template.get("from")
            from_sql = f" FROM {from_.format(f=name, arg=arg)}" if from_ else ""
            with_ = template.get("with", "")
            for position, shape in templates.positions.items():
                sql = shape.format(**{"with": with_, "expr": expr, "from": from_sql})
                yield Case(template["id"], position, proc.name, "", sql)


def gold_rewrites(
    gold: Sequence[tuple[str, str]],
    procs: Sequence[Proc],
    rewrites: Sequence[str],
    keywords: frozenset[str],
) -> Iterator[Case]:
    """The 40 gold queries unchanged and with ``alias.f`` spliced in, for every function."""
    for qid, sql in gold:
        if "identity" in rewrites:
            yield Case("gold:identity", "query", "", qid, sql)
        aliases = _outer_aliases(sql)
        body = sql.strip().rstrip(";")
        for proc in procs:
            name = quote(proc.name, keywords)
            if "outer_alias" in rewrites:
                for alias in aliases:
                    rewritten = _add_where(sql, f"({alias}.{name}) IS NOT NULL")
                    if rewritten is not None:
                        yield Case("gold:outer_alias", "where", proc.name, qid, rewritten)
            if "as_subquery" in rewrites:
                yield Case(
                    "gold:as_subquery",
                    "select",
                    proc.name,
                    qid,
                    f"SELECT g.{name} AS x FROM ({body}) AS g",
                )
            if "as_cte" in rewrites:
                yield Case(
                    "gold:as_cte",
                    "select",
                    proc.name,
                    qid,
                    f"WITH g AS ({body}) SELECT g.{name} AS x FROM g",
                )


def _outer_aliases(sql: str) -> tuple[str, ...]:
    """The names the outer query's FROM items are referenced by."""
    try:
        tree = sqlglot_parse_one(sql, read=DIALECT)
    except Exception:  # an unparseable gold query has no aliases to rewrite
        return ()
    if not isinstance(tree, exp.Select):
        return ()
    names: list[str] = []
    sources = [tree.args.get("from_"), *(tree.args.get("joins") or [])]
    for source in sources:
        if source is None:
            continue
        item = source.this
        if isinstance(item, exp.Table | exp.Subquery) and item.alias_or_name:
            names.append(item.alias_or_name)
    return tuple(dict.fromkeys(names))


def _add_where(sql: str, condition: str) -> str | None:
    try:
        tree = sqlglot_parse_one(sql, read=DIALECT)
        assert isinstance(tree, exp.Select)
        return tree.where(condition, append=True, dialect=DIALECT).sql(dialect=DIALECT)
    except Exception:  # a rewrite sqlglot cannot make is not generated
        return None


def literal_casts(sql: str) -> int:
    """Casts of a literal in ``sql``: the parser folds each into a constant the oracle can't see."""
    try:
        tree = sqlglot_parse_one(sql, read=DIALECT)
    except Exception:
        return 0
    count = 0
    for node in tree.walk():
        if isinstance(node, exp.Cast):
            inner = node.this
            while isinstance(inner, exp.Neg | exp.Paren):
                inner = inner.this
            if isinstance(inner, exp.Literal | exp.Null):
                count += 1
    return count


# -- the oracle against PostgreSQL ------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Outcome:
    """The oracle's verdict on one admitted statement."""

    resolved: bool
    sqlstate: str = ""
    error: str = ""
    findings: tuple[Finding, ...] = ()
    io_casts: int = 0
    nodes: dict[str, int] = field(default_factory=dict)
    ev_action: str = ""


class Oracle:
    """Resolves admitted statements in PostgreSQL without running them.

    One connection as the owner role with the agent's empty ``search_path``;
    each statement gets its own transaction, rolled back once its rule has
    been read, so nothing the oracle creates survives it.
    """

    def __init__(self, conninfo: str, allowlist: Iterable[str], *, timeout_ms: int = 5000) -> None:
        import psycopg

        self._conn = psycopg.connect(conninfo, autocommit=True)
        self._conn.execute("SET search_path = ''")
        self._conn.execute(f"SET statement_timeout = {int(timeout_ms)}")
        row = self._conn.execute("SHOW server_version").fetchone()
        self.server_version = str(row[0]) if row else ""
        self._conn.autocommit = False
        self.catalog = Catalog()
        self.allowlist = frozenset(allowlist)

    def close(self) -> None:
        self._conn.rollback()
        self._conn.close()

    def procs(self) -> list[Proc]:
        """Every function callable with one argument (one declared, or the rest defaulted)."""
        rows = self._conn.execute(
            """
            SELECT p.oid, n.nspname, p.proname, p.prokind, p.provolatile,
                   format_type(p.proargtypes[0], NULL)
            FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE p.pronargs = 1 OR (p.pronargs > 1 AND p.pronargs - p.pronargdefaults <= 1)
            ORDER BY n.nspname, p.proname, p.oid
            """
        ).fetchall()
        procs = [Proc(int(r[0]), r[1], r[2], r[3], r[4], r[5]) for r in rows]
        for proc in procs:
            self.catalog.procs[proc.oid] = proc
        return procs

    def keywords(self) -> frozenset[str]:
        """Keywords that cannot be a bare column name (all but the unreserved)."""
        rows = self._conn.execute(
            "SELECT word FROM pg_catalog.pg_get_keywords() WHERE catcode <> 'U'"
        ).fetchall()
        return frozenset(str(r[0]) for r in rows)

    def resolve(self, sql: str) -> Outcome:
        import psycopg

        wrapped = f"CREATE TEMP VIEW {VIEW} AS SELECT count(*) FROM ({sql}) AS q"
        try:
            self._conn.execute(wrapped)
            row = self._conn.execute(
                "SELECT ev_action::text FROM pg_catalog.pg_rewrite"
                f" WHERE ev_class = 'pg_temp.{VIEW}'::regclass AND rulename = '_RETURN'"
            ).fetchone()
        except psycopg.Error as exc:
            diag = getattr(exc, "diag", None)
            message = getattr(diag, "message_primary", None) or str(exc)
            return Outcome(False, exc.sqlstate or "", message.splitlines()[0])
        finally:
            self._conn.rollback()
        text = str(row[0]) if row else ""
        resolution = parse_ev_action(text)
        self._fill(resolution)
        self._conn.rollback()
        findings = judge(resolution, self.catalog, self.allowlist)
        return Outcome(True, "", "", findings, resolution.io_casts, resolution.nodes, text)

    def _fill(self, resolution: Resolution) -> None:
        """Look up every id the catalog does not know yet."""
        funcs = {
            r.oid
            for r in resolution.references
            if r.kind in {"call", "cast", "syntax", "aggregate", "window"}
        }
        ops = {r.oid for r in resolution.references if r.kind == "operator"}
        rels = {r.oid for r in resolution.references if r.kind == "relation"}
        missing_ops = sorted(ops - self.catalog.operators.keys())
        if missing_ops:
            for oid, schema, name, code in self._conn.execute(
                "SELECT o.oid, n.nspname, o.oprname, o.oprcode::oid FROM pg_catalog.pg_operator o"
                " JOIN pg_catalog.pg_namespace n ON n.oid = o.oprnamespace"
                " WHERE o.oid = ANY(%s)",
                (missing_ops,),
            ).fetchall():
                self.catalog.operators[int(oid)] = Operator(int(oid), schema, name, int(code))
                funcs.add(int(code))
        missing_funcs = sorted(f for f in funcs - self.catalog.procs.keys() if f)
        if missing_funcs:
            for row in self._conn.execute(
                "SELECT p.oid, n.nspname, p.proname, p.prokind, p.provolatile,"
                " coalesce(format_type(p.proargtypes[0], NULL), '')"
                " FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n"
                " ON n.oid = p.pronamespace WHERE p.oid = ANY(%s)",
                (missing_funcs,),
            ).fetchall():
                self.catalog.procs[int(row[0])] = Proc(
                    int(row[0]), row[1], row[2], row[3], row[4], row[5]
                )
        missing_rels = sorted(rels - self.catalog.relations.keys())
        if missing_rels:
            for oid, name in self._conn.execute(
                "SELECT c.oid, n.nspname || '.' || c.relname FROM pg_catalog.pg_class c"
                " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
                " WHERE c.oid = ANY(%s)",
                (missing_rels,),
            ).fetchall():
                self.catalog.relations[int(oid)] = str(name)


# -- a run ------------------------------------------------------------------------------


@dataclass(slots=True)
class Tally:
    generated: int = 0
    admitted: int = 0
    refused_by_postgres: int = 0
    resolved: int = 0
    flagged: int = 0
    with_io_cast: int = 0
    with_literal_cast: int = 0

    def to_json(self) -> dict[str, int]:
        return {
            "admitted": self.admitted,
            "flagged": self.flagged,
            "generated": self.generated,
            "refused_by_postgres": self.refused_by_postgres,
            "resolved": self.resolved,
            "with_io_cast": self.with_io_cast,
            "with_literal_cast": self.with_literal_cast,
        }


@dataclass(slots=True)
class Report:
    """Everything one run found; :meth:`to_json` is the committed raw output."""

    label: str
    disabled: tuple[str, ...]
    server_version: str = ""
    commit: str = ""
    functions: int = 0
    total: Tally = field(default_factory=Tally)
    by_group: dict[str, Tally] = field(default_factory=lambda: defaultdict(Tally))
    by_position: dict[str, Tally] = field(default_factory=lambda: defaultdict(Tally))
    reachable: dict[str, dict[str, Any]] = field(default_factory=dict)
    sqlstates: Counter[str] = field(default_factory=Counter)
    nodes: Counter[str] = field(default_factory=Counter)
    seconds: float = 0.0

    def add(self, case: Case, admitted: bool, guard_sql: str, outcome: Outcome | None) -> None:
        tallies = (self.total, self.by_group[case.group], self.by_position[case.position])
        for tally in tallies:
            tally.generated += 1
        if not admitted or outcome is None:
            return
        literal = literal_casts(guard_sql) > 0
        for tally in tallies:
            tally.admitted += 1
            tally.with_literal_cast += literal
            if not outcome.resolved:
                tally.refused_by_postgres += 1
                continue
            tally.resolved += 1
            tally.with_io_cast += outcome.io_casts > 0
            tally.flagged += bool(outcome.findings)
        if not outcome.resolved:
            self.sqlstates[outcome.sqlstate] += 1
            return
        self.nodes.update(outcome.nodes)
        for finding in outcome.findings:
            entry = self.reachable.setdefault(
                finding.name,
                {
                    "rule": finding.rule,
                    "kind": finding.kind,
                    "volatility": finding.volatility,
                    "statements": 0,
                    "groups": Counter(),
                    "example": guard_sql,
                },
            )
            entry["statements"] += 1
            entry["groups"][case.group] += 1

    def to_json(self) -> dict[str, Any]:
        """Any: the run as a JSON document (sorted, no timestamps beyond the duration)."""
        by_volatility: dict[str, list[str]] = defaultdict(list)
        for name, entry in sorted(self.reachable.items()):
            by_volatility[_VOLATILITY.get(entry["volatility"], "n/a")].append(name)
        return {
            "label": self.label,
            "disabled_fixes": list(self.disabled),
            "server_version": self.server_version,
            "commit": self.commit,
            "functions": self.functions,
            "seconds": round(self.seconds, 1),
            "total": self.total.to_json(),
            "by_group": {k: v.to_json() for k, v in sorted(self.by_group.items())},
            "by_position": {k: v.to_json() for k, v in sorted(self.by_position.items())},
            "refused_by_postgres_sqlstates": dict(sorted(self.sqlstates.items())),
            "resolved_node_types": dict(sorted(self.nodes.items())),
            "reachable": {
                name: {**entry, "groups": dict(sorted(entry["groups"].items()))}
                for name, entry in sorted(self.reachable.items())
            },
            "reachable_by_volatility": dict(sorted(by_volatility.items())),
        }


_VOLATILITY = {"i": "immutable", "s": "stable", "v": "volatile"}


def run(
    cases: Iterable[Case],
    guard: Guard,
    resolve: Callable[[str], Outcome],
    report: Report,
    *,
    progress: Callable[[int], None] | None = None,
) -> Report:
    """Judge every case with the guard, and every admitted one with the oracle."""
    started = time.perf_counter()
    with fixes_disabled(*report.disabled):
        for index, case in enumerate(cases, 1):
            verdict = guard.check(case.sql)
            outcome = resolve(verdict.sql) if verdict.allowed else None
            report.add(case, verdict.allowed, verdict.sql, outcome)
            if progress is not None and index % 5000 == 0:
                progress(index)
    report.seconds += time.perf_counter() - started
    return report


def load_gold(path: Path = GOLD_PATH) -> list[tuple[str, str]]:
    data = yaml.safe_load(path.read_text())
    return [(str(q["id"]), str(q["sql"])) for q in data["questions"]]


def _commit() -> str:
    """The checkout's commit, marked dirty when tracked files differ from it."""
    git = shutil.which("git")
    if git is None:
        return ""
    out = subprocess.run(  # noqa: S603 - a fixed argv
        [git, "rev-parse", "--short", "HEAD"], capture_output=True, text=True, cwd=ROOT
    )
    dirty = subprocess.run(  # noqa: S603 - a fixed argv
        [git, "status", "--porcelain", "--untracked-files=no"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    return out.stdout.strip() + ("+dirty" if dirty.stdout.strip() else "")


def render(document: dict[str, Any]) -> str:
    """A run as text for the terminal."""
    total = document["total"]
    lines = [
        f"{document['label']} (fixes disabled: {', '.join(document['disabled_fixes']) or 'none'})",
        f"  generated {total['generated']}, admitted {total['admitted']}, refused by PostgreSQL"
        f" {total['refused_by_postgres']}, resolved {total['resolved']},"
        f" flagged {total['flagged']}",
        f"  invisible to the oracle: {total['with_literal_cast']} admitted with a literal cast,"
        f" {total['with_io_cast']} resolved with an I/O conversion cast",
    ]
    for group, tally in document["by_group"].items():
        lines.append(
            f"  {group:<22} gen {tally['generated']:>6}  adm {tally['admitted']:>6}"
            f"  pg-refused {tally['refused_by_postgres']:>6}  flagged {tally['flagged']:>5}"
        )
    for volatility, names in document["reachable_by_volatility"].items():
        lines.append(f"  reachable outside the policy, {volatility}: {', '.join(names)}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m eval.guard.differential")
    sub = parser.add_subparsers(dest="command", required=True)
    go = sub.add_parser("run", help="generate, check with the guard, resolve with PostgreSQL")
    go.add_argument("--label", required=True, help="names the output file")
    go.add_argument("--disable", action="append", default=[], choices=HISTORICAL_FIXES)
    go.add_argument("--no-gold", action="store_true", help="skip the gold-query rewrites")
    go.add_argument("--limit", type=int, default=0, help="only the first N functions (smoke)")
    go.add_argument("--output-dir", type=Path, default=RESULTS_DIR)
    record = sub.add_parser("record", help="record ev_action fixtures for the unit tests")
    record.add_argument("output", type=Path)
    record.add_argument("sql", nargs="+")
    args = parser.parse_args(argv)

    import logging

    from eval.guard_eval import catalog_relations
    from tributary.config import get_settings

    logging.getLogger("sqlglot").setLevel(logging.ERROR)
    relations = catalog_relations()
    guard = _guard(relations)
    oracle = Oracle(get_settings().pg_conninfo, relations)
    try:
        if args.command == "record":
            return _record(oracle, args.output, args.sql)
        procs = oracle.procs()
        if args.limit:
            procs = procs[: args.limit]
        templates = load_templates()
        keywords = oracle.keywords()
        cases: Iterable[Case] = generate(procs, templates, keywords)
        if not args.no_gold:
            gold = gold_rewrites(load_gold(), procs, templates.gold_rewrites, keywords)
            cases = _chain(cases, gold)
        report = Report(
            args.label, tuple(args.disable), oracle.server_version, _commit(), len(procs)
        )
        run(
            cases,
            guard,
            oracle.resolve,
            report,
            progress=lambda n: print(f"  {n} ...", file=sys.stderr),
        )
    finally:
        oracle.close()
    document = report.to_json()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / f"{args.label}.json"
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    print(render(document))
    print(f"wrote {path.relative_to(ROOT) if path.is_relative_to(ROOT) else path}")
    return 0


def _guard(relations: Sequence[str]) -> Guard:
    """The guard as the agent builds it."""
    return Guard(relations)


def _chain(*parts: Iterable[Case]) -> Iterator[Case]:
    for part in parts:
        yield from part


def _record(oracle: Oracle, output: Path, statements: Sequence[str]) -> int:
    """Store each statement's ev_action and the catalog rows it names, for offline tests."""
    cases = []
    for sql in statements:
        outcome = oracle.resolve(sql)
        cases.append({"sql": sql, "ev_action": outcome.ev_action, "error": outcome.error})
    document = {
        "server_version": oracle.server_version,
        "cases": cases,
        "catalog": oracle.catalog.to_json(),
    }
    output.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n")
    print(f"recorded {len(cases)} statements to {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
