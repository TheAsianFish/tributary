"""Per-question context for the planner's user turn: worked examples and value hints.

Each function here returns a callable the :class:`~tributary.agent.planner.Planner`
calls with the question; it returns a text block, or ``""`` when it has nothing
to add (no example scored above zero, no value matched), in which case the
user turn is exactly what it would be without the feature. Both blocks live
in the user turn rather than the system prompt because they change with every
question, and the system prompt is the prefix the providers cache.
"""

from __future__ import annotations

from collections.abc import Callable

from tributary.agent.examples import ExamplePool, render_examples
from tributary.agent.values import DEFAULT_LIMIT, ValueIndex, hint_lines

VALUES_HEADER = (
    "Values stored in the database that the question mentions "
    "(compare with them exactly as written):"
)


def worked_examples(pool: ExamplePool, k: int = 3) -> Callable[[str], str]:
    """The ``k`` examples most similar to the question, rendered."""

    def block(question: str) -> str:
        return render_examples(pool.select(question, k))

    return block


def value_hints(index: ValueIndex, limit: int = DEFAULT_LIMIT) -> Callable[[str], str]:
    """The indexed values the question names, with the column each one is in."""

    def block(question: str) -> str:
        lines = hint_lines(index.match(question, limit=limit))
        return f"{VALUES_HEADER}\n{lines}" if lines else ""

    return block


__all__ = ["VALUES_HEADER", "value_hints", "worked_examples"]
