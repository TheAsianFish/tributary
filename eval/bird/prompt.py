"""The planner's rules for a BIRD database (the warehouse has its own, in the planner).

Same structured output, same guard and the same repair loop as the product;
only the rules differ, because BIRD's databases are not the warehouse: their
column names often need quoting, and each question may come with *evidence*
(BIRD's "external knowledge"), which defines the terms the question uses.
Kept short and generic on purpose: M7 measures what few-shot examples and
value hints add on top of this baseline.

:data:`BIRD_RULES_ANSWER` differs in one rule. The product keeps an exit for
questions its tables cannot answer, because users ask those; every BIRD
question is answerable, and M4 found a 7B model taking the exit on 11 of 50
of them. The variant swaps the exit for "always write your best query", so
the ablation measures what the exit costs on a benchmark that never needs it.
"""

from __future__ import annotations

BIRD_RULES = """\
You answer questions about one PostgreSQL database by writing one SQL query. Rules:
- Write a single SELECT statement (WITH clauses are fine). Never write to the \
database, change settings or call system functions; such queries are rejected.
- Read only the tables listed below and qualify each with its schema, as listed.
- Write column names exactly as listed; a name with capitals, spaces or symbols \
must be double-quoted.
- When the question is followed by evidence, the evidence defines the terms the \
question uses: follow it.
- Return exactly the columns the question asks for, in the order it asks for them, \
and nothing else.
- Cast to numeric or double precision before dividing when a ratio or a \
percentage is asked for.
- Add ORDER BY and LIMIT only when the question asks for a ranking or a number of \
rows.
- If these tables cannot answer the question, set answerable to false, leave sql \
empty and say why in assumptions.

Tables:

{schema}
"""

_EXIT = """\
- If these tables cannot answer the question, set answerable to false, leave sql \
empty and say why in assumptions.
"""
_ALWAYS = """\
- Every question can be answered from these tables: set answerable to true and \
write your best query, even when unsure.
"""
if _EXIT not in BIRD_RULES:  # pragma: no cover (an edit to the rules above broke the swap)
    raise RuntimeError("BIRD_RULES no longer contains the unanswerable exit")
BIRD_RULES_ANSWER = BIRD_RULES.replace(_EXIT, _ALWAYS)
