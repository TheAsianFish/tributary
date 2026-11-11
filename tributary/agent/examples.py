"""Few-shot examples: worked questions with the SQL that answers them, chosen per question.

The planner shows the model up to ``k`` examples before the question. They
come from where a team already documents its data:

- the warehouse: each mart's ``meta.examples`` in the dbt YAML, carried by
  the :class:`~tributary.agent.catalog.Catalog` built from the published
  manifest (so the people who write the model docs also write the examples);
- the BIRD benchmark: the official questions held out of the evaluation
  subset, same database only (``eval/bird/examples.py``).

Selection is lexical: Okapi BM25 over the examples' question text, with the
tokeniser schema linking uses (:func:`tributary.agent.linker.tokens`). No
model and no embeddings, so the choice is deterministic and the prompt (and
therefore every recorded model call) is reproducible. An example whose
question is the asked question is never chosen, so a pool that happens to
contain the question cannot hand the model its answer.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tributary.agent.catalog import Catalog

DEFAULT_K = 3
_K1, _B = 1.5, 0.75  # Okapi BM25 at its usual values, as in the linker
HEADER = "Examples of questions on this database and the SQL that answers them:"


@dataclass(frozen=True, slots=True)
class Example:
    question: str
    sql: str
    source: str = ""  # where it is documented, e.g. "marts.fct_edits" or "bird:1234"


def _normalise(text: str) -> str:
    return " ".join(text.split()).casefold()


def _tokens(text: str) -> list[str]:
    # Imported here: linker imports catalog, which imports this module for Example.
    from tributary.agent.linker import tokens

    return tokens(text)


@dataclass(frozen=True, slots=True)
class ExamplePool:
    examples: tuple[Example, ...]

    @classmethod
    def from_catalog(cls, catalog: Catalog) -> ExamplePool:
        return cls(catalog.examples)

    def __len__(self) -> int:
        return len(self.examples)

    def scores(self, question: str) -> list[float]:
        """BM25 of each example's question against ``question``, in pool order."""
        documents = [Counter(_tokens(e.question)) for e in self.examples]
        if not documents:
            return []
        lengths = [sum(d.values()) for d in documents]
        average = sum(lengths) / len(lengths) or 1.0
        n = len(documents)
        # Sorted, not a set: the float sum must not depend on the process's hash seed.
        query = sorted(set(_tokens(question)))
        frequency = {term: sum(1 for d in documents if term in d) for term in query}
        out = []
        for document, length in zip(documents, lengths, strict=True):
            score = 0.0
            norm = _K1 * (1 - _B + _B * length / average)
            for term in query:
                tf = document.get(term, 0)
                if not tf:
                    continue
                df = frequency[term]
                idf = math.log(1 + (n - df + 0.5) / (df + 0.5))
                score += idf * tf * (_K1 + 1) / (tf + norm)
            out.append(score)
        return out

    def select(self, question: str, k: int = DEFAULT_K) -> tuple[Example, ...]:
        """The ``k`` examples most similar to ``question``, best first.

        Ties keep pool order. Examples with no word in common (score 0) and
        any example asking ``question`` itself are left out, so fewer than
        ``k`` (or none) may come back.
        """
        if k <= 0:
            return ()
        asked = _normalise(question)
        ranked = sorted(
            (
                (-score, i)
                for i, score in enumerate(self.scores(question))
                if score > 0 and _normalise(self.examples[i].question) != asked
            ),
        )
        return tuple(self.examples[i] for _, i in ranked[:k])


def render_examples(examples: Sequence[Example]) -> str:
    """The block the planner puts before the question ("" when there are none)."""
    if not examples:
        return ""
    blocks = [f"Question: {e.question.strip()}\nSQL: {e.sql.strip()}" for e in examples]
    return HEADER + "\n\n" + "\n\n".join(blocks) + "\n"
