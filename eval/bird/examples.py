"""Few-shot pools for BIRD: the official questions held out of the evaluation subset.

BIRD Mini-Dev ships no training split here. The 450 official questions not in
the graded subset (``dev50``) play the part of a team's documented queries:
each database's pool holds only that database's questions, with BIRD's
evidence line kept in the question text (it is part of what the agent is
asked) and the PostgreSQL gold as the SQL. The caller passes the graded ids,
so no graded question can be shown to the model as an example of itself.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence

from eval.bird.data import BirdQuestion, official_questions
from tributary.agent.examples import Example, ExamplePool


def bird_example_pools(
    exclude_ids: Collection[int],
    questions: Sequence[BirdQuestion] | None = None,
) -> dict[str, ExamplePool]:
    """One pool per database, from the official questions whose id is not excluded.

    Pools are keyed by ``db_id`` in sorted order; examples keep question id order.
    ``questions`` defaults to the 500 official questions (tests pass their own).
    """
    excluded = set(exclude_ids)
    grouped: dict[str, list[Example]] = {}
    pool = official_questions() if questions is None else questions
    for q in sorted(pool, key=lambda q: q.id):
        if q.id in excluded:
            continue
        grouped.setdefault(q.db_id, []).append(Example(q.prompt, q.sql, f"bird:{q.id}"))
    return {db: ExamplePool(tuple(grouped[db])) for db in sorted(grouped)}
