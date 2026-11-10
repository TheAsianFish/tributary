"""Execution accuracy as BIRD defines it, and the check against BIRD's own scorer.

BIRD's ``evaluation_ex.py`` fetches every row of the predicted and the gold
query and calls ``calculate_ex``: correct when ``set(predicted) ==
set(gold)``. Row order and duplicate rows do not count; column order does
(rows are tuples); values compare with Python's ``==`` on whatever the
driver returned. That last clause has teeth: a ``numeric`` average arrives
as ``Decimal('61.5714285714285714')`` and never equals a ``float8`` gold of
``61.57142857142857``, so the same number in a different type is wrong.

- :func:`execution_match` is that rule exactly, on the values the database
  returned (arrays as tuples so rows stay hashable). It is the headline EX.
  Recorded executions keep each value's type (:mod:`eval.bird.recorded`), so
  a replay grades exactly what the live run graded.
- :func:`lenient_match` compares numbers as floats (after
  :func:`~tributary.agent.execute.jsonable`): the type no longer matters,
  only the value. Reported beside EX to show how many verdicts the type rule
  decides.
- :func:`strict_match` is the stricter rule the warehouse questions use
  (duplicates count, floats to six decimals).

:func:`official_calculate_ex` loads the official function from the pinned
upstream file, and every live run and ``make bird`` grade their pairs with
both it and :func:`execution_match` and report any disagreement.
"""

from __future__ import annotations

import ast
import hashlib
import math
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from tributary.agent.execute import jsonable

OFFICIAL_SCORER_URL = (
    "https://raw.githubusercontent.com/bird-bench/mini_dev/"
    "1c49a375e76c471e218564da3f4617513d33e371/evaluation/evaluation_ex.py"
)
OFFICIAL_SCORER_SHA256 = "da1bbcd4530be83692d7c650c814ea9704bb710d0c953eb75d02ccb38233cf89"

Rows = Sequence[Sequence[object]]


def _hashable(value: object) -> object:
    if isinstance(value, list | tuple):
        return tuple(_hashable(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((str(k), repr(v)) for k, v in value.items()))
    return value


def raw_rows(rows: Rows) -> list[tuple[object, ...]]:
    """Rows as hashable tuples of the values as fetched (arrays and JSON made hashable)."""
    return [tuple(_hashable(v) for v in row) for row in rows]


def execution_match(predicted: Rows, gold: Rows) -> bool:
    """BIRD's EX: the same set of rows, values compared as the driver returned them."""
    return set(raw_rows(predicted)) == set(raw_rows(gold))


def _plain(value: object) -> object:
    plain = jsonable(value)
    if isinstance(plain, list):
        return tuple(_plain(v) for v in plain)
    if isinstance(plain, dict):
        return tuple(sorted((str(k), repr(v)) for k, v in plain.items()))
    return plain


def normal_rows(rows: Rows) -> list[tuple[object, ...]]:
    """Rows as hashable tuples of JSON-plain values (numbers lose their database type)."""
    return [tuple(_plain(v) for v in row) for row in rows]


def lenient_match(predicted: Rows, gold: Rows) -> bool:
    """EX with numbers compared by value whatever their type (``numeric`` vs ``float8``)."""
    return set(normal_rows(predicted)) == set(normal_rows(gold))


def _strict_value(value: object) -> object:
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, float):
        return ("nan",) if math.isnan(value) else round(value, 6)
    return value


def strict_match(predicted: Rows, gold: Rows) -> bool:
    """The same multiset of rows, floats to six decimals, booleans distinct from integers."""

    def key(rows: Rows) -> Counter[tuple[object, ...]]:
        return Counter(tuple(_strict_value(v) for v in row) for row in normal_rows(rows))

    return key(predicted) == key(gold)


def rows_digest(rows: Rows) -> str:
    """Order-independent fingerprint of a result's row set (for reports, not grading)."""
    lines = sorted(repr(r) for r in set(normal_rows(rows)))
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16]


# --- the official scorer ------------------------------------------------------------------------


class ScorerUnavailable(Exception):  # noqa: N818 (reads as a state, like LinkerUnavailable)
    """The official script is not cached and could not be downloaded."""


def official_calculate_ex(cache: Path, *, download: bool = True) -> Callable[[Any, Any], int]:
    """``calculate_ex`` from BIRD's ``evaluation_ex.py`` at a pinned commit.

    Only that one function is compiled, from the parsed file: the module's
    top level imports ``func_timeout`` and a database driver, neither of
    which the comparison needs. The file is checked against its sha256
    before anything in it is compiled. Any: the function takes and returns
    what the official harness passes (lists of row tuples, an int).
    """
    path = cache / "evaluation_ex.py"
    if not path.exists():
        if not download:
            raise ScorerUnavailable(f"{path} is not cached")
        try:
            reply = httpx.get(OFFICIAL_SCORER_URL, timeout=30, follow_redirects=True)
        except httpx.HTTPError as exc:
            raise ScorerUnavailable(f"cannot download the official scorer: {exc}") from None
        if reply.status_code != 200:
            raise ScorerUnavailable(f"GET {OFFICIAL_SCORER_URL}: HTTP {reply.status_code}")
        cache.mkdir(parents=True, exist_ok=True)
        path.write_bytes(reply.content)
    source = path.read_bytes()
    digest = hashlib.sha256(source).hexdigest()
    if digest != OFFICIAL_SCORER_SHA256:
        raise ScorerUnavailable(
            f"{path} has sha256 {digest}, expected {OFFICIAL_SCORER_SHA256}; delete it"
        )
    tree = ast.parse(source)
    found = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "calculate_ex"]
    if len(found) != 1:
        raise ScorerUnavailable("evaluation_ex.py has no single calculate_ex function")
    module = ast.Module(body=list[ast.stmt](found), type_ignores=[])
    namespace: dict[str, Any] = {"__builtins__": {"set": set}}
    exec(compile(module, str(path), "exec"), namespace)  # noqa: S102 (pinned by sha256 above)
    function: Callable[[Any, Any], int] = namespace["calculate_ex"]
    return function


@dataclass(frozen=True, slots=True)
class CrossCheck:
    pairs: int
    agree: int
    disagreements: tuple[dict[str, object], ...]


def cross_check(
    pairs: Sequence[tuple[str, Rows, Rows]], official: Callable[[Any, Any], int]
) -> CrossCheck:
    """Grade each (label, predicted, gold) with both scorers on the rows as fetched."""
    disagreements = []
    for label, predicted, gold in pairs:
        theirs = bool(official(raw_rows(predicted), raw_rows(gold)))
        ours = execution_match(predicted, gold)
        if theirs != ours:
            disagreements.append(
                {
                    "item": label,
                    "official": theirs,
                    "ours": ours,
                    "predicted": [repr(r) for r in list(predicted)[:3]],
                    "gold": [repr(r) for r in list(gold)[:3]],
                }
            )
    return CrossCheck(len(pairs), len(pairs) - len(disagreements), tuple(disagreements))
