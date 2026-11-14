"""M13's pre-registered block in RESULTS.md must stay as it was committed before the run.

The block between the ``m13:prereg`` markers was written, and its sha256 put
here, in the commit that came before any model call of the second pass. Any
later edit to the block (a threshold moved, a hypothesis reworded after the
result was known) fails this test.
"""

from __future__ import annotations

import hashlib

from tributary.config import REPO_ROOT

RESULTS = REPO_ROOT / "RESULTS.md"
START, END = "<!-- m13:prereg -->\n", "<!-- /m13:prereg -->"
PREREG_SHA256 = "09b90e44df37ce0049c2bf9f72b80475ad43504d04b8d0146c9de3f8430f1db4"


def prereg_block(text: str) -> str:
    start = text.index(START) + len(START)
    return text[start : text.index(END, start)]


def test_the_m13_prereg_block_is_unchanged() -> None:
    block = prereg_block(RESULTS.read_text(encoding="utf-8"))
    assert hashlib.sha256(block.encode("utf-8")).hexdigest() == PREREG_SHA256
