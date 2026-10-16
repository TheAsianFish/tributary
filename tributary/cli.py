"""The ``tributary`` command line."""

from __future__ import annotations

import typer

from tributary import __version__
from tributary.config import get_settings
from tributary.logging import bind_run_id, configure_logging

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)


@app.callback()
def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_format, settings.log_level)


@app.command()
def version() -> None:
    """Print the installed version."""
    bind_run_id("version")
    typer.echo(__version__)
