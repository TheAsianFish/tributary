from collections.abc import Iterator

import pytest
import structlog
from structlog.contextvars import clear_contextvars


@pytest.fixture(autouse=True)
def _reset_logging() -> Iterator[None]:
    """Undo ``configure_logging`` from CLI tests, whose stderr is closed afterwards."""
    yield
    structlog.reset_defaults()
    clear_contextvars()
