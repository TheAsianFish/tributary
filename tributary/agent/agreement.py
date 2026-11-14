"""Whether two passes on one question returned the same rows, by BIRD's set rule.

Two results agree when they hold the same *set* of rows: row order and
duplicate rows do not count, column order does (rows are tuples), and values
compare with Python's ``==`` on what the driver returned, so a ``numeric``
``Decimal('61.5714285714285714')`` differs from a ``float8``
``61.57142857142857`` (they are equal only when the float is exactly the
decimal's value). Arrays become tuples and
JSON objects sorted ``(key, repr(value))`` pairs so that rows stay hashable.
This is the semantics of ``eval.bird.score.execution_match``, kept here so the
product does not import the evaluation package (a unit test checks the two
agree).
"""

from __future__ import annotations

from collections.abc import Sequence

Rows = Sequence[Sequence[object]]


def _hashable(value: object) -> object:
    if isinstance(value, list | tuple):
        return tuple(_hashable(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((str(k), repr(v)) for k, v in value.items()))
    return value


def _row_set(rows: Rows) -> set[tuple[object, ...]]:
    return {tuple(_hashable(v) for v in row) for row in rows}


def same_rows(a: Rows, b: Rows) -> bool:
    """The same set of rows, values compared as the driver returned them."""
    return _row_set(a) == _row_set(b)
