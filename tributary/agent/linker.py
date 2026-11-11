"""Schema linking: which tables of the catalog the planner should see for a question.

The planner's prompt holds the schema it may use. Sending every table is the
safest choice on a small database and the costliest on a large one: tokens
grow with the schema, and so does the chance that the model picks a
plausible-looking table with the wrong grain. A linker narrows the schema to
the tables a question is about.

:func:`link_schema` is the one entry point; strategies are selected by name:

- ``full``: every table, in catalog order. The baseline every other strategy
  must beat or explain.
- ``bm25``: lexical retrieval. Each table is a document made of its name,
  its description, its columns' names and descriptions and its indexed
  values; the question (with any evidence appended) is the query; the ``k``
  best-scoring tables are returned. No model, no embeddings, deterministic.
- ``core``: the strategy the agent is meant to use (value matching and
  join-path expansion, so the subset is connected and names the tables a
  literal in the question lives in). Selecting it before it exists raises
  :class:`LinkerUnavailable` rather than quietly falling back.

A :class:`Link` lists relations best first. Scores are what the strategy
ranked by (``full`` gives every table 1.0); they are for inspection and
evaluation (``make eval-link``), not a probability.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from tributary.agent.catalog import Catalog, Table

if TYPE_CHECKING:  # values imports this module's STOPWORDS
    from tributary.agent.values import ValueIndex

Strategy = Literal["full", "bm25", "core"]
STRATEGIES: tuple[Strategy, ...] = ("full", "bm25", "core")
DEFAULT_K = 4


class LinkerUnavailable(Exception):  # noqa: N818 (public API names)
    """The selected strategy is not implemented in this build."""


@dataclass(frozen=True, slots=True)
class Link:
    strategy: str
    tables: tuple[str, ...]
    scores: Mapping[str, float] = field(default_factory=dict)

    def __contains__(self, relation: object) -> bool:
        return relation in self.tables


# --- text ---------------------------------------------------------------------------------------

_WORD = re.compile(r"[A-Za-z][a-z]+|[A-Z]+(?![a-z])|[a-z]+|\d+")
_STOPWORD_TEXT = """
    a about above after all also an and any are as at be been before being below between both
    but by can could did do does doing down during each few for from further had has have
    having he her here hers how i if in into is it its itself just me more most my no nor not
    now of off on once only or other our out over own per same she should so some such than
    that the their them then there these they this those through to too under until up very
    was we were what when where which while who whom why will with would you your
    list show give find name names number numbers many much please among
"""
STOPWORDS = frozenset(_STOPWORD_TEXT.split())


def _stem(token: str) -> str:
    """A light plural strip, enough to match ``schools`` with ``school``."""
    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 3 and token.endswith("s") and not token.endswith(("ss", "us", "is")):
        return token[:-1]
    return token


def tokens(text: str) -> list[str]:
    """Lowercase word tokens: camelCase and snake_case split, stopwords dropped, stemmed."""
    out = []
    for match in _WORD.finditer(text):
        word = match.group(0).lower()
        if word not in STOPWORDS:
            out.append(_stem(word))
    return out


def table_document(table: Table) -> list[str]:
    """The words a table is retrieved by. Names count twice: they are what questions echo."""
    parts = [table.name, table.name, table.description]
    for column in table.columns:
        parts += [column.name, column.name, column.description]
        if column.values:
            parts += list(column.values)
    return tokens(" ".join(parts))


# --- strategies ---------------------------------------------------------------------------------


def link_full(
    question: str, catalog: Catalog, k: int | None = None, values: ValueIndex | None = None
) -> Link:
    relations = catalog.relations()
    return Link("full", relations, {r: 1.0 for r in relations})


@dataclass(frozen=True, slots=True)
class BM25:
    """Okapi BM25 over a catalog's tables (``k1`` and ``b`` at their usual values)."""

    k1: float = 1.5
    b: float = 0.75

    def scores(self, question: str, catalog: Catalog) -> dict[str, float]:
        documents = {t.relation: Counter(table_document(t)) for t in catalog.tables}
        if not documents:
            return {}
        lengths = {r: sum(d.values()) for r, d in documents.items()}
        average = sum(lengths.values()) / len(lengths) or 1.0
        n = len(documents)
        # Sorted so the float sum, and so any tie broken by name, is the same in every
        # process: set order follows the per-process string hash seed.
        query = sorted(set(tokens(question)))
        frequency = {term: sum(1 for d in documents.values() if term in d) for term in query}
        scores: dict[str, float] = {}
        for relation, document in documents.items():
            score = 0.0
            for term in query:
                tf = document.get(term, 0)
                if not tf:
                    continue
                df = frequency[term]
                idf = math.log(1 + (n - df + 0.5) / (df + 0.5))
                norm = self.k1 * (1 - self.b + self.b * lengths[relation] / average)
                score += idf * tf * (self.k1 + 1) / (tf + norm)
            scores[relation] = score
        return scores


def link_bm25(
    question: str, catalog: Catalog, k: int | None = None, values: ValueIndex | None = None
) -> Link:
    scores = BM25().scores(question, catalog)
    ranked = sorted(scores, key=lambda r: (-scores[r], r))
    chosen = tuple(ranked[: DEFAULT_K if k is None else k])
    return Link("bm25", chosen, {r: round(scores[r], 6) for r in ranked})


def link_core(
    question: str, catalog: Catalog, k: int | None = None, values: ValueIndex | None = None
) -> Link:
    """Value matching plus join-path expansion over the foreign-key graph.

    ``values`` is the value index to match the question against
    (:meth:`~tributary.agent.values.ValueIndex.match` gives each matched
    value's relation); ``None`` means the catalog's own indexed values
    (``ValueIndex.from_catalog(catalog)``), which is what the warehouse has.
    BIRD's catalogs carry no values, so the BIRD harness passes its index.

    Contract (``tests/unit/test_linker_core.py``):

    - every relation returned is in the catalog, best first, no duplicates;
    - ``k`` bounds the tables chosen on lexical evidence; two kinds of table
      are added beyond it: a table holding an indexed value the question
      quotes, and the tables on a shortest foreign-key path joining the
      chosen ones, so the subset is connected whenever the catalog is;
    - with no lexical or value evidence at all it returns the ``bm25`` result;
    - deterministic: the same question and catalog give the same link.
    """
    raise LinkerUnavailable("the core schema linker is not implemented yet; choose full or bm25")


LINKERS: Mapping[str, Callable[[str, Catalog, int | None, ValueIndex | None], Link]] = {
    "full": link_full,
    "bm25": link_bm25,
    "core": link_core,
}


def link_schema(
    question: str,
    catalog: Catalog,
    *,
    strategy: str = "full",
    k: int | None = None,
    values: ValueIndex | None = None,
) -> Link:
    """The tables to show the planner for ``question``, by ``strategy``.

    ``values`` is a value index for strategies that match literals (``core``);
    the others ignore it.
    """
    try:
        linker = LINKERS[strategy]
    except KeyError:
        known = ", ".join(LINKERS)
        raise ValueError(f"unknown linking strategy {strategy!r} (known: {known})") from None
    if k is not None and k < 1:
        raise ValueError(f"k must be at least 1, got {k}")
    return linker(question, catalog, k, values)


# --- graph helpers a strategy can use -------------------------------------------------------------


def foreign_key_graph(catalog: Catalog) -> dict[str, set[str]]:
    """Undirected adjacency between relations joined by a declared foreign key."""
    graph: dict[str, set[str]] = {t.relation: set() for t in catalog.tables}
    for table in catalog.tables:
        for fk in table.foreign_keys:
            if fk.ref_relation in graph and fk.ref_relation != table.relation:
                graph[table.relation].add(fk.ref_relation)
                graph[fk.ref_relation].add(table.relation)
    return graph


def is_connected(relations: Sequence[str], graph: Mapping[str, set[str]]) -> bool:
    """Whether ``relations`` form one connected piece of ``graph`` using only themselves."""
    wanted = set(relations)
    if len(wanted) <= 1:
        return True
    start = next(iter(sorted(wanted)))
    seen = {start}
    stack = [start]
    while stack:
        for neighbour in graph.get(stack.pop(), ()):
            if neighbour in wanted and neighbour not in seen:
                seen.add(neighbour)
                stack.append(neighbour)
    return seen == wanted


__all__ = [
    "BM25",
    "DEFAULT_K",
    "STRATEGIES",
    "Link",
    "LinkerUnavailable",
    "foreign_key_graph",
    "is_connected",
    "link_schema",
    "tokens",
]
