"""``enappsys pvc`` download command."""

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


def register(app: typer.Typer) -> None:
    @app.command("pvc")
    def pvc(
        ctx: typer.Context,
        code: Annotated[
            str,
            typer.Option(
                "--code",
                help="Price-volume curve code, e.g. hu/elec/ancillary/capacity/afrr/daily/up.",
            ),
        ],
        dt: Annotated[
            str,
            typer.Option("--dt", help="Datetime for the curve, e.g. 2025-01-01T00:00."),
        ],
        tz: Annotated[
            str,
            typer.Option("--tz", help="Time zone for the request, e.g. CET or UTC."),
        ],
        currency: Annotated[
            str,
            typer.Option("--currency", help="Currency code, e.g. EUR."),
        ],
        format: Annotated[
            str,
            typer.Option("--format", "-f", help="Response format: csv or xml."),
        ] = "csv",
        output: Annotated[
            Optional[Path],
            typer.Option("--output", "-o", help="Write to this file instead of stdout."),
        ] = None,
        table: TableFlag = False,
        delimiter: Annotated[
            str,
            typer.Option(
                "--delimiter",
                help="CSV delimiter name: comma, semicolon, tab.",
            ),
        ] = "comma",
    ) -> None:
        """Fetch a price-volume curve and write the raw response."""
        state: CliState = ctx.obj["state"]
        client = None
        try:
            client = make_client(state)
            result = client.price_volume_curve.get(
                format,
                code=code,
                dt=dt,
                time_zone=tz,
                currency=currency,
                delimiter=delimiter,
            )
            deliver_result(result, output, table, title=f"pvc:{code}")
        except Exception as exc:
            handle_cli_error(exc, state.verbose)
        finally:
            if client is not None:
                client.close()
