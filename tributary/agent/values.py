"""Value retrieval: which stored column values a question names.

A question names literals ("schools in Alameda County", "the artist 'Kev Walker'"),
and a model left alone writes them with the wrong case or against the wrong
column. A :class:`ValueIndex` holds the distinct values of the indexed columns
and finds, deterministically and without a model, the ones a question mentions,
so the planner can say ``"alameda" matches california_schools.schools."County" =
'Alameda'``.

Matching is on whole words: text and values are split into alphanumeric tokens,
each normalised (Unicode NFKC, casefolded), and a value matches a contiguous run
of the question's tokens that equals its token sequence. A quoted string in the
question ('...' or "...") is also compared whole, punctuation included. Values
that would match noise are not indexed: shorter than three characters, made of
digits only (models copy numbers fine), made only of stopwords, or longer than
100 characters. When matched spans overlap, the longest wins.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from tributary.agent.catalog import Catalog, quote_ident, quote_literal
from tributary.agent.linker import STOPWORDS

MIN_LENGTH = 3
MAX_LENGTH = 100
MAX_TOKENS = 12  # longer values are matched only through a quoted string
PER_SPAN = 3  # columns reported for one matched span
DEFAULT_LIMIT = 8

_TOKEN = re.compile(r"[^\W_]+")
# A quote opens after a non-word character and closes before one, so the apostrophe in
# "driver's" opens nothing; inside single quotes an apostrophe before a letter ('O'Brien')
# does not close the string.
_QUOTED = re.compile(
    r"""(?<!\w)(?:'((?:[^'\n]|'(?=\w)){1,100}?)'(?!\w)|"([^"\n]{1,100}?)"(?!\w))"""
)


@dataclass(frozen=True, slots=True)
class ValueMatch:
    relation: str  # e.g. "california_schools.schools"
    column: str  # exactly as in the catalog
    value: str  # exactly as stored
    span: str  # the question text that matched, as written


def _norm(text: str) -> str:
    return unicodedata.normalize("NFKC", text).casefold()


def _tokens(text: str) -> list[tuple[str, int, int]]:
    """(token as written, start, end) for every alphanumeric run."""
    return [(m.group(), m.start(), m.end()) for m in _TOKEN.finditer(text)]


def _token_key(text: str) -> str:
    return " ".join(_norm(t) for t, _, _ in _tokens(text))


def _whole_key(text: str) -> str:
    return " ".join(_norm(text).split())


def indexable(value: str) -> bool:
    """Whether ``value`` is worth indexing (not noise a question would match by accident)."""
    stripped = value.strip()
    if not MIN_LENGTH <= len(stripped) <= MAX_LENGTH:
        return False
    words = [_norm(t) for t, _, _ in _tokens(stripped)]
    if not words:
        return False
    if all(w.isdigit() for w in words):
        return False
    return not all(w in STOPWORDS for w in words)


def _wordlike(value: str, key: str) -> bool:
    """Whether a value may match by its words; if not, only a quoted string matches it.

    "C++" is the token "c" and "<goal />" the token "goal": matched by their words they
    would hit every "C" and every "goal", so a value whose words are under three
    characters, or which is more punctuation than letters, needs quoting.
    """
    letters = len(key.replace(" ", ""))
    punctuation = sum(not ch.isalnum() and not ch.isspace() for ch in value)
    return letters >= MIN_LENGTH and punctuation * 2 <= letters


@dataclass(frozen=True, slots=True)
class _Entry:
    relation: str
    column: str
    value: str
    exact: tuple[str, ...]  # the value's tokens, NFKC but not casefolded


@dataclass(frozen=True, slots=True)
class _Candidate:
    start: int
    end: int
    entries: tuple[_Entry, ...]
    exact: tuple[str, ...]  # the span's tokens as written (NFKC)


@dataclass(frozen=True, slots=True)
class ValueIndex:
    columns: Mapping[tuple[str, str], tuple[str, ...]]  # (relation, column) -> distinct values
    _by_tokens: dict[str, list[_Entry]] = field(
        init=False, repr=False, compare=False, default_factory=dict
    )
    _by_whole: dict[str, list[_Entry]] = field(
        init=False, repr=False, compare=False, default_factory=dict
    )
    _max_tokens: int = field(init=False, repr=False, compare=False, default=0)

    def __post_init__(self) -> None:
        longest = 0
        for relation, column in sorted(self.columns):
            for value in self.columns[(relation, column)]:
                if not indexable(value):
                    continue
                words = _tokens(value)
                entry = _Entry(
                    relation,
                    column,
                    value,
                    tuple(unicodedata.normalize("NFKC", t) for t, _, _ in words),
                )
                self._by_whole.setdefault(_whole_key(value), []).append(entry)
                key = _token_key(value)
                if len(words) <= MAX_TOKENS and _wordlike(value, key):
                    self._by_tokens.setdefault(key, []).append(entry)
                    longest = max(longest, len(words))
        object.__setattr__(self, "_max_tokens", longest)

    def __len__(self) -> int:
        return sum(len(v) for v in self.columns.values())

    def match(self, text: str, *, limit: int = DEFAULT_LIMIT) -> tuple[ValueMatch, ...]:
        """The indexed values ``text`` names, in text order (see the module docstring)."""
        if limit <= 0:
            return ()
        # Candidates for one span (a quoted string and its words) pool their entries.
        pooled: dict[tuple[int, int], _Candidate] = {}
        for cand in self._candidates(text):
            seen = pooled.get((cand.start, cand.end))
            pooled[(cand.start, cand.end)] = (
                cand
                if seen is None
                else _Candidate(cand.start, cand.end, seen.entries + cand.entries, seen.exact)
            )
        # Longest span first (ties: the earlier one); a span overlapping a kept one is dropped.
        chosen: list[_Candidate] = []
        for cand in sorted(pooled.values(), key=lambda c: (-(c.end - c.start), c.start)):
            if all(cand.end <= k.start or cand.start >= k.end for k in chosen):
                chosen.append(cand)
        # (rank within its span, inexact, -span length, start, match)
        ranked: list[tuple[int, bool, int, int, ValueMatch]] = []
        for cand in chosen:
            span = text[cand.start : cand.end]
            entries = sorted(
                set(cand.entries),
                key=lambda e: (e.exact != cand.exact, e.relation, e.column, e.value),
            )
            for rank, entry in enumerate(entries[:PER_SPAN]):
                found = ValueMatch(entry.relation, entry.column, entry.value, span)
                inexact = entry.exact != cand.exact
                ranked.append((rank, inexact, cand.start - cand.end, cand.start, found))
        # Under the limit: every span's best column before any span's second, exact case
        # first, then longer spans; a value named twice (question and evidence) counts once.
        kept: list[tuple[int, bool, int, int, ValueMatch]] = []
        named: set[tuple[str, str, str]] = set()
        for item in sorted(ranked, key=lambda r: r[:4]):
            key = (item[4].relation, item[4].column, item[4].value)
            if key not in named and len(kept) < limit:
                named.add(key)
                kept.append(item)
        kept.sort(key=lambda r: (r[3], r[1], r[4].relation, r[4].column, r[4].value))
        return tuple(r[4] for r in kept)

    def _candidates(self, text: str) -> list[_Candidate]:
        words = _tokens(text)
        normed = [_norm(t) for t, _, _ in words]
        written = [unicodedata.normalize("NFKC", t) for t, _, _ in words]
        out: list[_Candidate] = []
        for i in range(len(words)):
            for n in range(1, min(self._max_tokens, len(words) - i) + 1):
                entries = self._by_tokens.get(" ".join(normed[i : i + n]))
                if entries:
                    out.append(
                        _Candidate(
                            words[i][1],
                            words[i + n - 1][2],
                            tuple(entries),
                            tuple(written[i : i + n]),
                        )
                    )
        for m in _QUOTED.finditer(text):
            group = 1 if m.group(1) is not None else 2
            inner = m.group(group)
            entries = self._by_whole.get(_whole_key(inner))
            if not entries:
                continue
            lead = len(inner) - len(inner.lstrip())
            start = m.start(group) + lead
            end = start + len(inner.strip())
            exact = tuple(unicodedata.normalize("NFKC", t) for t, _, _ in _tokens(inner))
            out.append(_Candidate(start, end, tuple(entries), exact))
        return out

    @classmethod
    def from_catalog(cls, catalog: Catalog) -> ValueIndex:
        """Every column of ``catalog`` with a value index (the warehouse's dbt value index)."""
        columns = {
            (table.relation, column.name): column.values
            for table in catalog.tables
            for column in table.columns
            if column.values
        }
        return cls(columns)

    def to_dict(self) -> dict[str, Any]:
        # Any: a JSON document.
        return {
            "columns": [
                {"relation": relation, "column": column, "values": list(values)}
                for (relation, column), values in sorted(self.columns.items())
            ]
        }

    @classmethod
    def from_dict(cls, document: Mapping[str, Any]) -> ValueIndex:
        """The inverse of :meth:`to_dict`."""
        # Any: a JSON document shaped as to_dict writes it.
        return cls(
            {
                (str(c["relation"]), str(c["column"])): tuple(map(str, c["values"]))
                for c in document["columns"]
            }
        )


def hint_lines(matches: Sequence[ValueMatch]) -> str:
    """The hint block for the model: one line per match, ready-to-paste SQL."""
    lines = []
    for m in matches:
        schema, _, name = m.relation.partition(".")
        target = f"{quote_ident(schema)}.{quote_ident(name)}" if name else quote_ident(schema)
        lines.append(
            f'- "{m.span}" matches {target}.{quote_ident(m.column)} = {quote_literal(m.value)}'
        )
    return "\n".join(lines)
