"""``enappsys epex`` download commands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer

from enappsys.cli_helpers import (
    CliState,
    TableFlag,
    deliver_result,
    handle_cli_error,
    make_client,
)

app = typer.Typer(help="Download EPEX contract or settlement evolution data.")


def _parse_max_points(value: str) -> int | str:
    lowered = value.strip().lower()
    if lowered == "all":
        return "all"
    try:
        return int(value)
    except ValueError as e:
        raise typer.BadParameter(
            "Expected an integer or 'all'.",
            param_hint="--max-points",
        ) from e


@app.command("contract")
def contract(
    ctx: typer.Context,
    code: Annotated[
        str,
        typer.Option("--code", help="EPEX market code."),
    ],
    contract: Annotated[
        str,
        typer.Option("--contract", help="Contract identifier."),
    ],
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Response format: csv, json, or xml."),
    ] = "csv",
    output: Annotated[
        Optional[Path],
        typer.Option("--output", "-o", help="Write to this file instead of stdout."),
    ] = None,
    table: TableFlag = False,
    max_points: Annotated[
        str,
        typer.Option("--max-points", help="Maximum points to return, or 'all'."),
    ] = "all",
    tz: Annotated[
        str,
        typer.Option("--tz", help="Time zone for the request, e.g. CET."),
    ] = "CET",
) -> None:
    """Fetch EPEX contract evolution data."""
    state: CliState = ctx.obj["state"]
    client = None
    try:
        client = make_client(state)
        result = client.epex.contract.get(
            format,
            code=code,
            contract=contract,
            max_points=_parse_max_points(max_points),
            time_zone=tz,
        )
        deliver_result(result, output, table, title=f"epex contract:{code}")
    except Exception as exc:
        handle_cli_error(exc, state.verbose)
    finally:
        if client is not None:
            client.close()


@app.command("settlement")
def settlement(
    ctx: typer.Context,
    code: Annotated[
        str,
        typer.Option("--code", help="EPEX market code."),
    ],
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Response format: csv, json, or xml."),
    ] = "csv",
    output: Annotated[
        Optional[Path],
        typer.Option("--output", "-o", help="Write to this file instead of stdout."),
    ] = None,
    table: TableFlag = False,
    settlement_date: Annotated[
        Optional[str],
        typer.Option(
            "--settlement-date",
            help="Settlement date (use with --settlement-period).",
        ),
    ] = None,
    settlement_period: Annotated[
        Optional[int],
        typer.Option(
            "--settlement-period",
            help="Settlement period number (use with --settlement-date).",
        ),
    ] = None,
    settlement_datetime: Annotated[
        Optional[str],
        typer.Option(
            "--settlement-datetime",
            help="Settlement datetime instead of date + period.",
        ),
    ] = None,
    max_points: Annotated[
        str,
        typer.Option("--max-points", help="Maximum points to return, or 'all'."),
    ] = "all",
    tz: Annotated[
        str,
        typer.Option("--tz", help="Time zone for the request, e.g. CET."),
    ] = "CET",
) -> None:
    """Fetch EPEX settlement trade evolution data."""
    state: CliState = ctx.obj["state"]
    client = None
    try:
        client = make_client(state)
        result = client.epex.settlement.get(
            format,
            code=code,
            settlement_date=settlement_date,
            settlement_period=settlement_period,
            settlement_datetime=settlement_datetime,
            max_points=_parse_max_points(max_points),
            time_zone=tz,
        )
        deliver_result(result, output, table, title=f"epex settlement:{code}")
    except Exception as exc:
        handle_cli_error(exc, state.verbose)
    finally:
        if client is not None:
            client.close()
