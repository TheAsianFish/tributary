"""The agent's view of the schema: tables, columns, keys, lineage and known values.

The planner never reads dbt artifacts or system catalogs itself; it reads a
:class:`Catalog`, built from one of two sources:

- ``dbt``: a published catalog (:mod:`tributary.warehouse.catalog`). The
  descriptions are the YAML docs, the types are what the database reports
  (``catalog.json``), and keys come from the tests, because dbt creates no
  constraints: a column with both ``unique`` and ``not_null`` is a primary
  key, so is the column list of a ``unique_combination`` test, and every
  ``relationships`` test is a foreign key. The value index supplies the
  literals a question names ("German Wikipedia" -> ``dewiki``).
- ``information_schema``: any PostgreSQL database without dbt (the BIRD
  databases). Only the tables and columns the role may see, declared primary
  and foreign keys, comments as descriptions, overridable by descriptions
  supplied from elsewhere. No value index.

:meth:`Catalog.render` is the text the model sees. It is deterministic (sorted
relations, database column order, no timestamps or versions), so the same
catalog always yields the same prompt prefix: prompt caching hits, and replay
fingerprints stay stable across publishes of an unchanged schema.
"""

from __future__ import annotations

import re
import textwrap
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from psycopg import sql

from tributary.warehouse.catalog import PublishedCatalog, QueryFn, load_current

Source = Literal["dbt", "information_schema"]
_LINEAGE_TYPES = ("model", "seed", "snapshot")
_REF_RE = re.compile(r"""ref\(\s*['"]([^'"]+)['"]\s*\)""")
_PLAIN_IDENT = re.compile(r"^[a-z_][a-z0-9_$]*$")
# PostgreSQL's reserved key words (appendix C): as column names they need quoting.
_RESERVED_WORDS = """
    all analyse analyze and any array as asc asymmetric authorization binary both case cast
    check collate collation column concurrently constraint create cross current_catalog
    current_date current_role current_schema current_time current_timestamp current_user
    default deferrable desc distinct do else end except false fetch for foreign freeze from
    full grant group having ilike in initially inner intersect into is isnull join lateral
    leading left like limit localtime localtimestamp natural not notnull null offset on only
    or order outer overlaps placing primary references returning right select session_user
    similar some symmetric system_user table tablesample then to trailing true union unique
    user using variadic verbose when where window with
"""
_RESERVED = frozenset(_RESERVED_WORDS.split())
_WIDTH = 100


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    data_type: str
    description: str
    distinct: int | None = None
    values: tuple[str, ...] | None = None  # most frequent first; None when not indexed


@dataclass(frozen=True, slots=True)
class ForeignKey:
    column: str
    ref_relation: str
    ref_column: str


@dataclass(frozen=True, slots=True)
class Table:
    schema: str
    name: str
    description: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKey, ...]
    upstream: tuple[str, ...]

    @property
    def relation(self) -> str:
        return f"{self.schema}.{self.name}"

    def column(self, name: str) -> Column:
        for column in self.columns:
            if column.name == name:
                return column
        for column in self.columns:
            if column.name.lower() == name.lower():
                return column
        known = ", ".join(c.name for c in self.columns)
        raise KeyError(f"{self.relation} has no column {name!r}; columns: {known}")


@dataclass(frozen=True, slots=True)
class Catalog:
    tables: tuple[Table, ...]
    source: Source
    version: str | None = None
    generated_at: str | None = None

    def relations(self) -> tuple[str, ...]:
        return tuple(t.relation for t in self.tables)

    def table(self, relation: str) -> Table:
        for table in self.tables:
            if table.relation == relation:
                return table
        hint = [t.relation for t in self.tables if t.name == relation.rsplit(".", 1)[-1]]
        known = ", ".join(self.relations()) or "none"
        suggestion = f"; did you mean {' or '.join(hint)}?" if hint else ""
        raise KeyError(f"unknown relation {relation!r}{suggestion} (known: {known})")

    def to_dict(self) -> dict[str, Any]:
        # Any: a JSON document.
        return {
            "source": self.source,
            "version": self.version,
            "generated_at": self.generated_at,
            "tables": [
                {
                    "relation": t.relation,
                    "schema": t.schema,
                    "name": t.name,
                    "description": t.description,
                    "primary_key": list(t.primary_key),
                    "foreign_keys": [
                        {
                            "column": f.column,
                            "ref_relation": f.ref_relation,
                            "ref_column": f.ref_column,
                        }
                        for f in t.foreign_keys
                    ],
                    "upstream": list(t.upstream),
                    "columns": [
                        {
                            "name": c.name,
                            "type": c.data_type,
                            "description": c.description,
                            "distinct": c.distinct,
                            "values": None if c.values is None else list(c.values),
                        }
                        for c in t.columns
                    ],
                }
                for t in self.tables
            ],
        }

    def render(
        self,
        relations: Sequence[str] | None = None,
        *,
        max_values: int = 25,
        sample_values: int = 10,
    ) -> str:
        """PostgreSQL DDL with descriptions and known values as comments.

        An indexed column with at most ``max_values`` distinct values lists them
        all; one with more lists the ``sample_values`` most frequent. Values are
        SQL string literals, ready to paste into a ``WHERE`` clause.
        """
        tables = (
            self.tables
            if relations is None
            else tuple(self.table(r) for r in sorted(set(relations)))
        )
        return "\n\n".join(_render_table(t, max_values, sample_values) for t in tables) + "\n"


# --- rendering ----------------------------------------------------------------------------


def quote_ident(name: str) -> str:
    """An identifier as PostgreSQL needs it written (quoted only when it must be)."""
    if _PLAIN_IDENT.match(name) and name not in _RESERVED:
        return name
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """A string as a PostgreSQL literal, on one line (control characters as ``E''`` escapes)."""
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        escaped = "".join(
            f"\\x{ord(ch):02x}" if ord(ch) < 32 or ord(ch) == 127 else ch
            for ch in value.replace("\\", "\\\\").replace("'", "''")
        )
        return f"E'{escaped}'"
    return "'" + value.replace("'", "''") + "'"


def _one_line(text: str) -> str:
    return " ".join(text.split())


def _values_note(column: Column, max_values: int, sample_values: int) -> str:
    if column.values is None or not column.values:
        return ""
    distinct = column.distinct if column.distinct is not None else len(column.values)
    if distinct <= max_values:
        listed = column.values
        return "Values: " + ", ".join(quote_literal(v) for v in listed) + "."
    listed = column.values[:sample_values]
    return (
        f"Values ({distinct} distinct, most frequent first), e.g. "
        + ", ".join(quote_literal(v) for v in listed)
        + "."
    )


def _render_table(table: Table, max_values: int, sample_values: int) -> str:
    lines = []
    if table.description:
        lines += textwrap.wrap(
            _one_line(table.description),
            width=_WIDTH,
            initial_indent="-- ",
            subsequent_indent="-- ",
            break_long_words=False,
            break_on_hyphens=False,
        )
    if table.upstream:
        lines.append("-- Built from: " + ", ".join(table.upstream) + ".")
    lines.append(f"CREATE TABLE {quote_ident(table.schema)}.{quote_ident(table.name)} (")
    single_pk = table.primary_key[0] if len(table.primary_key) == 1 else None
    references = {f.column: f for f in table.foreign_keys}
    body: list[tuple[str, str]] = []
    for column in table.columns:
        definition = f"  {quote_ident(column.name)} {column.data_type}"
        if column.name == single_pk:
            definition += " PRIMARY KEY"
        fk = references.get(column.name)
        if fk is not None:
            ref_schema, _, ref_name = fk.ref_relation.partition(".")
            definition += (
                f" REFERENCES {quote_ident(ref_schema)}.{quote_ident(ref_name)}"
                f"({quote_ident(fk.ref_column)})"
            )
        note = " ".join(
            part
            for part in (
                _one_line(column.description),
                _values_note(column, max_values, sample_values),
            )
            if part
        )
        body.append((definition, note))
    if len(table.primary_key) > 1:
        body.append(
            ("  PRIMARY KEY (" + ", ".join(quote_ident(c) for c in table.primary_key) + ")", "")
        )
    for i, (definition, note) in enumerate(body):
        comma = "," if i < len(body) - 1 else ""
        lines.append(f"{definition}{comma}  -- {note}" if note else f"{definition}{comma}")
    lines.append(");")
    return "\n".join(lines)


# --- from a published dbt catalog -----------------------------------------------------------


def _manifest_relation(node: Mapping[str, Any]) -> str:
    if node.get("resource_type") == "source":
        return f"{node['schema']}.{node.get('identifier') or node['name']}"
    return f"{node['schema']}.{node.get('alias') or node['name']}"


def _lookup_column(columns: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    entry = columns.get(name)
    if entry is None:
        # Manifest column keys keep the case they were declared with.
        entry = next((v for k, v in columns.items() if k.lower() == name.lower()), None)
    return entry or {}


def _upstream(unique_id: str, nodes: Mapping[str, Any], sources: Mapping[str, Any]) -> set[str]:
    seen: set[str] = set()
    found: set[str] = set()
    stack = list(nodes.get(unique_id, {}).get("depends_on", {}).get("nodes", []))
    while stack:
        parent = stack.pop()
        if parent in seen:
            continue
        seen.add(parent)
        if parent in sources:
            found.add(_manifest_relation(sources[parent]))
        elif parent in nodes and nodes[parent].get("resource_type") in _LINEAGE_TYPES:
            found.add(_manifest_relation(nodes[parent]))
            stack.extend(nodes[parent].get("depends_on", {}).get("nodes", []))
    return found


def _ref_target(test: Mapping[str, Any], nodes: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """The node a ``relationships`` test points at: its other dependency, else its ``to``."""
    attached = test.get("attached_node")
    others = [n for n in test.get("depends_on", {}).get("nodes", []) if n != attached]
    if len(others) == 1 and others[0] in nodes:
        target: Mapping[str, Any] = nodes[others[0]]
        return target
    # Last resort: a self-referencing test, or a manifest without depends_on.
    match = _REF_RE.search(str(test.get("test_metadata", {}).get("kwargs", {}).get("to", "")))
    if match:
        candidates: list[Mapping[str, Any]] = [
            n
            for n in nodes.values()
            if n.get("resource_type") in _LINEAGE_TYPES and n.get("name") == match.group(1)
        ]
        if len(candidates) == 1:
            return candidates[0]
    return None


@dataclass(slots=True)
class _Tests:
    unique: set[str]
    not_null: set[str]
    combinations: list[tuple[str, ...]]
    foreign_keys: list[ForeignKey]


def _tests_by_node(nodes: Mapping[str, Any]) -> dict[str, _Tests]:
    by_node: dict[str, _Tests] = {}
    for unique_id in sorted(nodes):
        test = nodes[unique_id]
        metadata = test.get("test_metadata") or {}
        attached = test.get("attached_node")
        if test.get("resource_type") != "test" or not attached or not metadata:
            continue
        found = by_node.setdefault(attached, _Tests(set(), set(), [], []))
        kwargs = metadata.get("kwargs") or {}
        column = test.get("column_name") or kwargs.get("column_name")
        name = metadata.get("name")
        if name == "unique" and column:
            found.unique.add(column)
        elif name == "not_null" and column:
            found.not_null.add(column)
        elif name == "unique_combination" and kwargs.get("combination_of_columns"):
            found.combinations.append(tuple(kwargs["combination_of_columns"]))
        elif name == "relationships" and column and kwargs.get("field"):
            target = _ref_target(test, nodes)
            if target is not None:
                found.foreign_keys.append(
                    ForeignKey(column, _manifest_relation(target), str(kwargs["field"]))
                )
    return by_node


def _primary_key(tests: _Tests | None, column_order: Sequence[str]) -> tuple[str, ...]:
    if tests is None:
        return ()
    for name in column_order:  # the first such column, if a table has several
        if name in tests.unique and name in tests.not_null:
            return (name,)
    if tests.combinations:
        return min(tests.combinations, key=lambda c: (len(c), c))
    return ()


def from_published(published: PublishedCatalog, schemas: Sequence[str] = ("marts",)) -> Catalog:
    """The relations of ``schemas`` in a published catalog (pure)."""
    wanted = set(schemas)
    nodes: Mapping[str, Any] = published.manifest.get("nodes", {})
    sources: Mapping[str, Any] = published.manifest.get("sources", {})
    indexed: Mapping[str, Any] = published.values.get("columns", {})
    tests = _tests_by_node(nodes)
    tables = []
    for unique_id, entry in published.catalog.get("nodes", {}).items():
        metadata = entry.get("metadata", {})
        schema, name = metadata.get("schema"), metadata.get("name")
        if schema not in wanted or not name:
            continue
        node = nodes.get(unique_id, {})
        manifest_columns = node.get("columns") or {}
        columns = []
        for col in sorted(entry.get("columns", {}).values(), key=lambda c: c.get("index", 0)):
            doc = _lookup_column(manifest_columns, col["name"])
            index = indexed.get(f"{schema}.{name}.{col['name']}") or {}
            values = (
                tuple(str(v["value"]) for v in index.get("values", []))
                if index.get("indexed")
                else None
            )
            columns.append(
                Column(
                    name=col["name"],
                    data_type=str(col.get("type", "")),
                    description=str(doc.get("description") or col.get("comment") or "").strip(),
                    distinct=index.get("distinct"),
                    values=values,
                )
            )
        found = tests.get(unique_id)
        relation = f"{schema}.{name}"
        tables.append(
            Table(
                schema=schema,
                name=name,
                description=str(node.get("description") or metadata.get("comment") or "").strip(),
                columns=tuple(columns),
                primary_key=_primary_key(found, [c.name for c in columns]),
                foreign_keys=tuple(
                    sorted(found.foreign_keys, key=lambda f: (f.column, f.ref_relation))
                    if found
                    else ()
                ),
                upstream=tuple(sorted(_upstream(unique_id, nodes, sources) - {relation})),
            )
        )
    tables.sort(key=lambda t: t.relation)
    generated_at = published.export.get("generated_at")
    return Catalog(
        tables=tuple(tables),
        source="dbt",
        version=published.version,
        generated_at=None if generated_at is None else str(generated_at),
    )


def load_dbt(catalog_dir: Path, schemas: Sequence[str] = ("marts",)) -> Catalog:
    """The current published catalog under ``catalog_dir``."""
    return from_published(load_current(catalog_dir), schemas)


# --- from the database's own catalog -------------------------------------------------------

# information_schema.columns lists only what the role may use, which is what the agent may
# query. ordinal_position is pg_attribute.attnum, so col_description can take it.
COLUMNS_SQL = sql.SQL(
    "SELECT c.table_schema, c.table_name, c.column_name,"
    " CASE WHEN c.data_type IN ('ARRAY', 'USER-DEFINED') THEN c.udt_name ELSE c.data_type END,"
    " obj_description(cls.oid, 'pg_class'),"
    " col_description(cls.oid, c.ordinal_position::int)"
    " FROM information_schema.columns c"
    " JOIN information_schema.tables t"
    " ON t.table_schema = c.table_schema AND t.table_name = c.table_name"
    " JOIN pg_catalog.pg_namespace ns ON ns.nspname = c.table_schema"
    " JOIN pg_catalog.pg_class cls ON cls.relnamespace = ns.oid AND cls.relname = c.table_name"
    " WHERE c.table_schema = ANY(%s) AND t.table_type IN ('BASE TABLE', 'VIEW')"
    " ORDER BY c.table_schema, c.table_name, c.ordinal_position"
)
# One row per key column, in key order; a composite foreign key yields one row per column.
KEYS_SQL = sql.SQL(
    "SELECT ns.nspname, cls.relname, con.contype, a.attname, rns.nspname, rcls.relname, ra.attname"
    " FROM pg_catalog.pg_constraint con"
    " JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid"
    " JOIN pg_catalog.pg_namespace ns ON ns.oid = cls.relnamespace"
    " CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)"
    " JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum"
    " LEFT JOIN pg_catalog.pg_class rcls ON rcls.oid = con.confrelid"
    " LEFT JOIN pg_catalog.pg_namespace rns ON rns.oid = rcls.relnamespace"
    " LEFT JOIN pg_catalog.pg_attribute ra"
    " ON ra.attrelid = con.confrelid AND ra.attnum = con.confkey[k.ord]"
    " WHERE con.contype IN ('p', 'f') AND ns.nspname = ANY(%s)"
    " ORDER BY ns.nspname, cls.relname, con.conname, k.ord"
)


def _override(
    descriptions: Mapping[str, Mapping[str, str]] | None, relation: str, key: str
) -> str | None:
    if not descriptions or relation not in descriptions:
        return None
    given = descriptions[relation]
    if key in given:
        return given[key]
    return next((v for k, v in given.items() if k and k.lower() == key.lower()), None)


def from_information_schema(
    query: QueryFn,
    schemas: Sequence[str],
    *,
    descriptions: Mapping[str, Mapping[str, str]] | None = None,
) -> Catalog:
    """The tables and views of ``schemas`` as PostgreSQL describes them.

    ``descriptions`` maps a relation to ``{"": table description, column: description}``
    and wins over the database comments (BIRD ships its descriptions as CSV files).
    """
    params = (list(schemas),)
    columns: dict[str, list[Column]] = {}
    table_comments: dict[str, str] = {}
    for schema, table, column, data_type, table_comment, column_comment in query(
        sql.Composed([COLUMNS_SQL]), params
    ):
        relation = f"{schema}.{table}"
        table_comments[relation] = table_comment or ""
        description = _override(descriptions, relation, column)
        columns.setdefault(relation, []).append(
            Column(
                column,
                str(data_type),
                ((column_comment or "") if description is None else description).strip(),
            )
        )
    primary: dict[str, list[str]] = {}
    foreign: dict[str, list[ForeignKey]] = {}
    for schema, table, kind, column, ref_schema, ref_table, ref_column in query(
        sql.Composed([KEYS_SQL]), params
    ):
        relation = f"{schema}.{table}"
        if kind == "p":
            primary.setdefault(relation, []).append(column)
        elif ref_table is not None and ref_column is not None:
            foreign.setdefault(relation, []).append(
                ForeignKey(column, f"{ref_schema}.{ref_table}", ref_column)
            )
    tables = []
    for relation in sorted(columns):
        schema, _, name = relation.partition(".")
        description = _override(descriptions, relation, "")
        tables.append(
            Table(
                schema=schema,
                name=name,
                description=(
                    table_comments[relation] if description is None else description
                ).strip(),
                columns=tuple(columns[relation]),
                primary_key=tuple(primary.get(relation, ())),
                foreign_keys=tuple(
                    sorted(foreign.get(relation, ()), key=lambda f: (f.column, f.ref_relation))
                ),
                upstream=(),
            )
        )
    return Catalog(tables=tuple(tables), source="information_schema")
