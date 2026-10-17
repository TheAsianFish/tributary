"""The SQL guard: what model-written SQL may run against the warehouse, and as what.

The agent's executor runs as a read-only role with a statement timeout and a
cost ceiling; the guard is the layer in front of it that does not depend on
any of that being configured right. It parses the statement with sqlglot
(PostgreSQL dialect), walks the whole tree and decides, failing closed: a
statement it does not understand is blocked, never passed through.

What is allowed is one read-only query: a SELECT, or a set operation of
SELECTs, optionally with a non-recursive WITH, reading only the relations
on the allowlist, calling only functions known to be read-only, with no
placeholders. The SQL that runs is not the SQL that came in: it is
regenerated from the tree, so comments never reach the database, every
unqualified relation is schema-qualified (search_path cannot redirect it)
and the root query carries a LIMIT no larger than ``max_rows``. The
regenerated SQL is parsed and checked again and must come out identical, so
what runs is exactly what was checked.

Functions are policed in two ways, because sqlglot maps the functions it
knows to typed expression classes and everything else to ``exp.Anonymous``:

- a denylist of the known-dangerous, matched by name however the call
  parsed (lowercased, so ``"PG_SLEEP"(1)`` and ``Pg_Sleep(1)`` are caught):
  every ``pg_*`` and ``lo_*`` function (server files, sleeps, signals,
  advisory locks, configuration reloads, large objects), ``dblink*``,
  settings (``set_config``, ``current_setting``), functions that run a query
  given as text (``query_to_xml`` and its siblings, ``ts_stat``,
  ``ts_rewrite``), sequences (``nextval`` and friends write), ``txid_*``,
  ``inet_*``, privilege probes, ``version`` and the session-identity
  functions (reconnaissance with no analytic use). The XML functions
  (``xml*``, ``xpath*``) are denied too: they are pure, but they hand
  untrusted text to libxml2, whose entity handling has leaked server files
  before, and no warehouse question needs them;
- an allowlist for the unknown: an ``exp.Anonymous`` call runs only if its
  name is one of the read-only PostgreSQL built-ins sqlglot does not type
  (``ANONYMOUS_ALLOWED``). A typed function is allowed unless denylisted;
  sqlglot only types functions it models, and none of those has side
  effects. ``random()`` is allowed: nondeterministic but harmless.
  ``generate_series`` is allowed: the executor's cost ceiling and timeout
  bound its size. A schema-qualified call (``pg_catalog.pg_sleep``,
  ``public.f``) is always blocked, whatever the function.

A cast to a type sqlglot does not know runs that type's input function, so
it is blocked as a function; casts to the ``reg*`` object-identifier types
look names up in the system catalog and are blocked as catalog access.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

DIALECT = "postgres"

_DENIED_FUNCTION = re.compile(
    r"^(?:"
    r"pg_\w*|lo_\w*|dblink\w*"
    r"|set_config|current_setting"
    r"|(?:query|cursor|table|schema|database)_to_xml\w*|ts_stat|ts_rewrite"
    r"|nextval|setval|currval|lastval|setseed"
    r"|txid_\w*|inet_\w*|has_\w*_privilege"
    r"|(?:current_)?version"
    r"|current_user|current_role|session_user|current_schemas?|current_database"
    r"|current_catalog|current_query"
    r"|xml\w*|xpath\w*"
    r")$"
)

ANONYMOUS_ALLOWED: frozenset[str] = frozenset(
    {
        # dates and times
        "age",
        "clock_timestamp",
        "isfinite",
        "make_date",
        "make_timestamptz",
        "statement_timestamp",
        "timezone",
        "transaction_timestamp",
        # aggregates
        "every",
        "jsonb_agg",
        # numbers
        "gcd",
        "lcm",
        "scale",
        "trim_scale",
        # strings and regular expressions
        "octet_length",
        "regexp_match",
        "regexp_matches",
        "regexp_split_to_array",
        "regexp_split_to_table",
        "string_to_table",
        # arrays
        "array_dims",
        "array_lower",
        "array_ndims",
        "array_positions",
        "array_replace",
        "array_upper",
        "cardinality",
        # null counting
        "num_nonnulls",
        "num_nulls",
        # JSON
        "array_to_json",
        "json_array_length",
        "json_build_array",
        "json_build_object",
        "json_typeof",
        "jsonb_array_elements",
        "jsonb_array_elements_text",
        "jsonb_array_length",
        "jsonb_build_array",
        "jsonb_build_object",
        "jsonb_each",
        "jsonb_each_text",
        "jsonb_extract_path_text",
        "jsonb_object_keys",
        "jsonb_pretty",
        "jsonb_strip_nulls",
        "jsonb_typeof",
        "row_to_json",
        "to_json",
        "to_jsonb",
    }
)

_SYSTEM_SCHEMAS = frozenset({"pg_catalog", "information_schema"})

# Statements that change data or schema, wherever they sit in the tree.
_WRITE_OR_DDL: tuple[type[exp.Expr], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.AlterRename,
    exp.RenameColumn,
    exp.TruncateTable,
    exp.Grant,
    exp.Revoke,
    exp.Copy,
    exp.Comment,
    exp.LoadData,
    exp.Refresh,
    exp.Cache,
    exp.Uncache,
    exp.Analyze,
    exp.Attach,
    exp.Detach,
    exp.Install,
    exp.Export,
)

# Statements that are not queries: session, transaction and utility commands, and
# exp.Command, sqlglot's opaque fallback for statements it does not model.
_NOT_QUERY: tuple[type[exp.Expr], ...] = (
    exp.Command,
    exp.Set,
    exp.Transaction,
    exp.Commit,
    exp.Rollback,
    exp.Use,
    exp.Pragma,
    exp.Describe,
    exp.Kill,
    exp.Show,
    exp.Declare,
    exp.Execute,
    exp.Summarize,
)


@dataclass(frozen=True, slots=True)
class GuardResult:
    """The verdict on one statement, and the SQL to execute when it is allowed."""

    allowed: bool
    sql: str
    reasons: tuple[str, ...]
    tables: tuple[str, ...]
    limit: int | None

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(reason.split(":", 1)[0] for reason in self.reasons)


class GuardBlocked(Exception):  # noqa: N818 - the name is the agent-wide interface
    """The guard refused a statement; ``result`` says why."""

    def __init__(self, result: GuardResult) -> None:
        super().__init__("; ".join(result.reasons) or "blocked")
        self.result = result


def _ident(identifier: exp.Expr | None) -> str | None:
    """An identifier as PostgreSQL resolves it: unquoted folds to lowercase."""
    if identifier is None:
        return None
    if isinstance(identifier, exp.Identifier):
        text = str(identifier.this)
        return text if identifier.quoted else text.lower()
    return identifier.name.lower()


def _cte_name(cte: exp.Expr) -> str | None:
    alias = cte.args.get("alias")
    return _ident(alias.this) if isinstance(alias, exp.TableAlias) else None


def _cte_in_scope(table: exp.Table, name: str) -> bool:
    """Whether an unqualified ``name`` at ``table`` refers to a CTE visible there.

    Scoping follows PostgreSQL: a WITH is visible in its query's body and, for a
    non-recursive WITH, in the CTEs defined after it. A CTE defined inside a
    subquery does not shadow anything outside that subquery.
    """
    child: exp.Expr = table
    node = table.parent
    while node is not None:
        if isinstance(node, exp.With):
            ctes = list(node.expressions)
            index = next((i for i, cte in enumerate(ctes) if cte is child), len(ctes))
            if any(_cte_name(cte) == name for cte in ctes[:index]):
                return True
            # The query owning this WITH is done: its body's scope is not ours.
            owner = node.parent
            if owner is None:
                return False
            child, node = owner, owner.parent
            continue
        with_ = node.args.get("with_")
        if (
            isinstance(with_, exp.With)
            and with_ is not child
            and any(_cte_name(cte) == name for cte in with_.expressions)
        ):
            return True
        child, node = node, node.parent
    return False


def _function_names(func: exp.Func) -> tuple[tuple[str, ...], bool]:
    """A call's lowercased name(s) for the denylist, and whether sqlglot left it untyped."""
    if isinstance(func, exp.Anonymous | exp.AnonymousAggFunc):
        this = func.this
        name = this.this if isinstance(this, exp.Identifier) else str(this)
        return (name.lower(),), True
    return tuple(name.lower() for name in func.sql_names()), False


def _unknown_function_name(func: exp.Func) -> str | None:
    """The name PostgreSQL would resolve for an untyped call, or None if quoted oddly."""
    this = func.this
    if isinstance(this, exp.Identifier):
        # A quoted name is case-sensitive: "Age" is not age.
        text = str(this.this)
        return text if this.quoted else text.lower()
    return str(this).lower()


class Guard:
    """The policy for one allowlist of relations; :meth:`check` is pure."""

    def __init__(self, relations: Iterable[str], *, max_rows: int = 1000) -> None:
        if max_rows < 1:
            raise ValueError("max_rows must be positive")
        allowed: set[str] = set()
        by_table: dict[str, list[str]] = {}
        for relation in relations:
            parts = relation.strip().lower().split(".")
            if len(parts) != 2 or not all(parts):
                raise ValueError(f"relation must be schema.table: {relation!r}")
            allowed.add(".".join(parts))
            by_table.setdefault(parts[1], []).append(parts[0])
        self.relations = frozenset(allowed)
        self._by_table = {table: sorted(set(schemas)) for table, schemas in by_table.items()}
        self.max_rows = max_rows

    def enforce(self, sql: str) -> GuardResult:
        """Check ``sql`` and raise :class:`GuardBlocked` unless it is allowed."""
        result = self.check(sql)
        if not result.allowed:
            raise GuardBlocked(result)
        return result

    def check(self, sql: str) -> GuardResult:
        first = self._analyse(sql)
        if not first.allowed:
            return first
        # What runs must be exactly what was checked: the regenerated SQL is checked
        # again and has to be allowed and unchanged (a fixed point of the rewrite).
        second = self._analyse(first.sql)
        if not second.allowed or second.sql != first.sql:
            reasons = (*second.reasons, "parse_error: the regenerated SQL does not round-trip")
            return GuardResult(False, "", _dedupe(reasons), first.tables, None)
        return first

    def _analyse(self, sql: str) -> GuardResult:
        reasons: list[str] = []
        tables: set[str] = set()
        try:
            parsed = sqlglot.parse(sql, dialect=DIALECT)
        except Exception as error:  # any parser failure means "not understood"
            message = str(error).splitlines()[0] if str(error) else type(error).__name__
            return GuardResult(False, "", (f"parse_error: {message}",), (), None)

        statements = [s for s in parsed if s is not None and not isinstance(s, exp.Semicolon)]
        if not statements:
            return GuardResult(False, "", ("empty: no SQL statement",), (), None)
        if len(statements) > 1:
            reasons.append(
                f"multiple_statements: {len(statements)} statements; only one query may run"
            )

        for statement in statements:
            self._check_root(statement, reasons)
            self._check_tree(statement, reasons, tables)

        limit: int | None = None
        out = ""
        if not reasons:
            root = statements[0]
            if not isinstance(root, exp.Query):  # _check_root guarantees it; belt and braces
                reasons.append("not_select: the statement is not a query")
            else:
                try:
                    root, limit = self._apply_limit(root, reasons)
                    out = root.sql(dialect=DIALECT, comments=False)
                except Exception as error:  # unrenderable means not understood
                    reasons.append(f"parse_error: cannot regenerate the SQL ({error})")

        if reasons:
            return GuardResult(False, "", _dedupe(reasons), tuple(sorted(tables)), None)
        return GuardResult(True, out, (), tuple(sorted(tables)), limit)

    # -- statement shape -------------------------------------------------------------------

    def _check_root(self, statement: exp.Expr, reasons: list[str]) -> None:
        node = statement
        while isinstance(node, exp.Subquery):
            node = node.this
        if isinstance(node, exp.Select | exp.SetOperation):
            return
        if isinstance(node, _WRITE_OR_DDL):
            return  # reported as write_or_ddl by the tree walk
        kind = type(node).__name__
        if isinstance(node, exp.Command):
            kind = str(node.this).upper()
        reasons.append(f"not_select: {kind} is not a SELECT query")

    def _check_tree(self, statement: exp.Expr, reasons: list[str], tables: set[str]) -> None:
        for node in statement.walk():
            if isinstance(node, _WRITE_OR_DDL):
                reasons.append(f"write_or_ddl: {type(node).__name__} statement")
            elif isinstance(node, _NOT_QUERY) and node is not statement:
                reasons.append(f"not_select: embedded {type(node).__name__} statement")
            elif isinstance(node, exp.Into):
                reasons.append("select_into: SELECT ... INTO creates a table")
            elif isinstance(node, exp.Lock):
                reasons.append("locking_clause: FOR UPDATE/SHARE takes row locks")
            elif isinstance(node, exp.With) and node.args.get("recursive"):
                reasons.append("recursive_cte: WITH RECURSIVE is not allowed")
            elif isinstance(node, exp.Placeholder | exp.Parameter):
                reasons.append(f"parameter: placeholder {node.sql(dialect=DIALECT)} has no value")
            elif isinstance(node, exp.Table):
                self._check_table(node, reasons, tables)
            elif isinstance(node, exp.Column):
                self._check_column(node, reasons)
            elif isinstance(node, exp.Dot) and isinstance(node.expression, exp.Func):
                reasons.append(
                    f"qualified_function: {node.sql(dialect=DIALECT)} is schema-qualified"
                )
            elif isinstance(node, exp.ObjectIdentifier):
                reasons.append(
                    f"system_catalog: a cast to {node.name.lower()}, an object-identifier type,"
                    " resolves names in the system catalog"
                )
            elif isinstance(node, exp.DataType) and node.this == exp.DataType.Type.USERDEFINED:
                reasons.append(
                    f"function_not_allowed: a cast to the unknown type "
                    f"{node.sql(dialect=DIALECT)} runs its input function"
                )
            if isinstance(node, exp.Func):
                self._check_function(node, reasons)

    # -- relations -----------------------------------------------------------------------

    def _check_table(self, table: exp.Table, reasons: list[str], tables: set[str]) -> None:
        if not isinstance(table.this, exp.Identifier):
            return  # a table function or ROWS FROM: its calls are checked as functions
        name = _ident(table.this)
        schema = _ident(table.args.get("db"))
        catalog = _ident(table.args.get("catalog"))
        assert name is not None
        shown = ".".join(part for part in (catalog, schema, name) if part)

        if schema is None and catalog is None and _cte_in_scope(table, name):
            return
        if (
            (schema is not None and (schema in _SYSTEM_SCHEMAS or schema.startswith("pg_")))
            or (catalog is not None and (catalog in _SYSTEM_SCHEMAS or catalog.startswith("pg_")))
            or name.startswith("pg_")
        ):
            tables.add(shown)
            reasons.append(f"system_catalog: {shown} is a system catalog relation")
            return
        if catalog is not None:
            tables.add(shown)
            reasons.append(f"relation_not_allowed: {shown} is catalog-qualified")
            return
        if schema is not None:
            tables.add(shown)
            if shown not in self.relations:
                reasons.append(f"relation_not_allowed: {shown} is not on the allowlist")
            return

        schemas = self._by_table.get(name, [])
        if not schemas:
            tables.add(name)
            reasons.append(f"relation_not_allowed: {name} is not on the allowlist")
        elif len(schemas) > 1:
            tables.update(f"{s}.{name}" for s in schemas)
            reasons.append(
                f"ambiguous_relation: {name} could be any of "
                + ", ".join(f"{s}.{name}" for s in schemas)
            )
        else:
            # Pin the schema so search_path cannot redirect the reference.
            table.set("db", exp.to_identifier(schemas[0]))
            tables.add(f"{schemas[0]}.{name}")

    def _check_column(self, column: exp.Column, reasons: list[str]) -> None:
        for key in ("db", "catalog"):
            part = _ident(column.args.get(key))
            if part is not None and (part in _SYSTEM_SCHEMAS or part.startswith("pg_")):
                reasons.append(
                    f"system_catalog: {column.sql(dialect=DIALECT)} names a system schema"
                )

    # -- functions -----------------------------------------------------------------------

    def _check_function(self, func: exp.Func, reasons: list[str]) -> None:
        names, unknown = _function_names(func)
        for name in names:
            if _DENIED_FUNCTION.match(name):
                reasons.append(f"function_not_allowed: {name}() is denied")
                return
        if unknown:
            resolved = _unknown_function_name(func)
            if resolved not in ANONYMOUS_ALLOWED:
                reasons.append(f"function_not_allowed: {resolved}() is not on the allowlist")

    # -- the row limit -------------------------------------------------------------------

    def _apply_limit(self, root: exp.Query, reasons: list[str]) -> tuple[exp.Query, int]:
        cap = self.max_rows
        current = root.args.get("limit")
        if current is None:
            root.set("limit", exp.Limit(expression=exp.Literal.number(cap)))
            return root, cap
        if isinstance(current, exp.Fetch):
            options = current.args.get("limit_options")
            if isinstance(options, exp.LimitOptions) and (
                options.args.get("with_ties") or options.args.get("percent")
            ):
                return self._wrap(root), cap
            count = current.args.get("count")
            requested = 1 if count is None else _literal_int(count)
            effective = cap if requested is None else min(requested, cap)
            root.set("limit", exp.Limit(expression=exp.Literal.number(effective)))
            return root, effective
        if isinstance(current, exp.Limit):
            if current.args.get("expressions") or current.args.get("limit_options"):
                return self._wrap(root), cap
            requested = _literal_int(current.expression)
            effective = cap if requested is None else min(requested, cap)
            current.set("expression", exp.Literal.number(effective))
            return root, effective
        reasons.append(f"parse_error: unrecognised limit clause {type(current).__name__}")
        return root, cap

    def _wrap(self, root: exp.Query) -> exp.Query:
        """Bound a query whose own limit cannot be lowered in place (WITH TIES, PERCENT)."""
        return (
            exp.select("*")
            .from_(exp.Subquery(this=root, alias=exp.TableAlias(this=exp.to_identifier("q"))))
            .limit(self.max_rows)
        )


def _literal_int(node: exp.Expr | None) -> int | None:
    """A non-negative integer literal's value, or None for anything else (ALL, NULL, 2+3)."""
    if isinstance(node, exp.Literal) and not node.is_string and node.is_int:
        value = int(node.this)
        return value if value >= 0 else None
    return None


def _dedupe(reasons: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(reasons))
