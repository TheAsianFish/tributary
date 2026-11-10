"""The planner's rules for a BIRD database (the warehouse has its own, in the planner).

Same structured output, same guard and the same repair loop as the product;
only the rules differ, because BIRD's databases are not the warehouse: their
column names often need quoting, and each question may come with *evidence*
(BIRD's "external knowledge"), which defines the terms the question uses.
Kept short and generic on purpose: M7 measures what few-shot examples and
value hints add on top of this baseline.
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
