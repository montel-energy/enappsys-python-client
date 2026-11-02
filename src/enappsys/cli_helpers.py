"""Shared helpers for the EnAppSys CLI."""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import typer

from enappsys.client import EnAppSys
from enappsys.exceptions import (
    APIError,
    ClientError,
    EnAppSysException,
    ValidationError,
)


EXIT_UPSTREAM = 1
EXIT_USAGE = 2

TableFlag = Annotated[
    bool,
    typer.Option(
        "--table",
        help=(
            "Open an interactive Textual table view instead of printing CSV to stdout "
            "(first 10,000 rows). Requires: pip install enappsys[tui]. "
            "Can be combined with -o to also save."
        ),
    ),
]


@dataclass
class CliState:
    user: str | None = None
    secret: str | None = None
    credentials_file: str | None = None
    verbose: bool = False


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def make_client(state: CliState) -> EnAppSys:
    return EnAppSys(
        user=state.user,
        secret=state.secret,
        credentials_file=state.credentials_file,
    )


def write_response(response: Any, output: Path | None) -> None:
    """Write a service ``.response`` body to stdout or a file."""
    if isinstance(response, (dict, list)):
        text = json.dumps(response, indent=2, ensure_ascii=False)
        if not text.endswith("\n"):
            text += "\n"
    else:
        text = response if isinstance(response, str) else str(response)
        if text and not text.endswith("\n"):
            text += "\n"

    if output is None:
        sys.stdout.write(text)
        return

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text, encoding="utf-8")


def deliver_result(
    result: Any,
    output: Path | None,
    table: bool,
    title: str = "EnAppSys",
) -> None:
    """Write raw response and/or open the optional Textual table viewer."""
    if output is not None:
        write_response(result.response, output)
    elif not table:
        write_response(result.response, None)
    if table:
        from enappsys.cli_table import show_table

        show_table(result, title=title)


def parse_chunk_rows(value: str | None) -> bool | int | None:
    """Parse ``--chunk-rows``: omitted, true/false, or an integer budget."""
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered in {"true", "yes", "1", "on"}:
        return True
    if lowered in {"false", "no", "0", "off"}:
        return False
    try:
        return int(value)
    except ValueError as e:
        raise typer.BadParameter(
            "Expected true, false, or an integer row budget.",
            param_hint="--chunk-rows",
        ) from e


def parse_time_display(value: str | None) -> dict | None:
    if value is None:
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as e:
        raise typer.BadParameter(
            f"Invalid JSON for --time-display: {e}",
            param_hint="--time-display",
        ) from e
    if not isinstance(parsed, dict):
        raise typer.BadParameter(
            "Expected a JSON object for --time-display.",
            param_hint="--time-display",
        )
    return parsed


def handle_cli_error(exc: BaseException, verbose: bool) -> None:
    if isinstance(exc, (ClientError, ValidationError)):
        typer.echo(str(exc), err=True)
        if verbose:
            typer.echo(repr(exc), err=True)
        raise typer.Exit(EXIT_USAGE) from exc
    if isinstance(exc, (APIError, EnAppSysException)):
        typer.echo(str(exc), err=True)
        if verbose:
            typer.echo(repr(exc), err=True)
        raise typer.Exit(EXIT_UPSTREAM) from exc
    raise exc
