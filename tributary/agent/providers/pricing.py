"""What a Claude call cost, from the token counts the API reports.

Prices are USD per million tokens, Anthropic first-party, copied on
2026-10-06 and checked again on 2026-11-09. Cache writes use the 5-minute
TTL, billed at 1.25x input; cache reads are listed per model. A model
missing from the table has no price: :func:`cost` returns ``None`` and the
caller records it as unpriced rather than guessing, so a reported cost is
never invented.
"""

from __future__ import annotations

from dataclasses import dataclass

CACHE_WRITE_MULTIPLIER = 1.25


@dataclass(frozen=True, slots=True)
class Price:
    input: float
    output: float
    cache_read: float

    @property
    def cache_write(self) -> float:
        return self.input * CACHE_WRITE_MULTIPLIER


# Source: Anthropic pricing page, 2026-10-06; re-checked 2026-11-09.
PRICES: dict[str, Price] = {
    "claude-opus-5-5": Price(input=4.00, output=20.00, cache_read=0.20),
    "claude-sonnet-5-5": Price(input=2.00, output=10.00, cache_read=0.10),
    "claude-haiku-5-5": Price(input=0.10, output=0.50, cache_read=0.01),
    "claude-fable-5-2": Price(input=8.00, output=40.00, cache_read=0.20),
    "claude-fable-5-1": Price(input=10.00, output=50.00, cache_read=0.25),
}


def price(model: str) -> Price | None:
    return PRICES.get(model)


def cost(
    model: str,
    *,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int = 0,
    cache_write_tokens: int = 0,
) -> float | None:
    """USD for one call, or ``None`` when the model has no known price.

    ``input_tokens`` is the uncached input only, as the API reports it; cached
    reads and writes are counted separately and priced at their own rates.
    """
    p = PRICES.get(model)
    if p is None:
        return None
    return (
        input_tokens * p.input
        + output_tokens * p.output
        + cache_read_tokens * p.cache_read
        + cache_write_tokens * p.cache_write
    ) / 1_000_000
