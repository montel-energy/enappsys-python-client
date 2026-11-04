"""``enappsys chart`` download command."""

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
    parse_time_display,
)


def register(app: typer.Typer) -> None:
    @app.command("chart")
    def chart(
        ctx: typer.Context,
        code: Annotated[
            str,
            typer.Option("--code", help="Chart code, e.g. de/elec/pricing/daprices."),
        ],
        resolution: Annotated[
            str,
            typer.Option("--resolution", help="Series resolution, e.g. qh, hr, daily."),
        ],
        start: Annotated[
            Optional[str],
            typer.Option(
                "--start",
                help="Start datetime. Required unless --time-display is set.",
            ),
        ] = None,
        end: Annotated[
            Optional[str],
            typer.Option(
                "--end",
                help="End datetime. Required unless --time-display is set.",
            ),
        ] = None,
        tz: Annotated[
            str,
            typer.Option("--tz", help="Time zone for the request, e.g. CET or UTC."),
        ] = "UTC",
        currency: Annotated[
            str,
            typer.Option("--currency", help="Currency code, e.g. EUR."),
        ] = "EUR",
        format: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Response format: csv, json, json_map, xml.",
            ),
        ] = "csv",
        output: Annotated[
            Optional[Path],
            typer.Option("--output", "-o", help="Write to this file instead of stdout."),
        ] = None,
        table: TableFlag = False,
        units: Annotated[
            Optional[str],
            typer.Option(
                "--units",
                help="Request a unit conversion (CSV only), e.g. EUR/MWh.",
            ),
        ] = None,
        min_avg_max: Annotated[
            bool,
            typer.Option(
                "--min-avg-max",
                help="Request min/avg/max columns when supported.",
            ),
        ] = False,
        delimiter: Annotated[
            str,
            typer.Option(
                "--delimiter",
                help="CSV delimiter name: comma, semicolon, tab.",
            ),
        ] = "comma",
        enable_settlement_period: Annotated[
            bool,
            typer.Option(
                "--enable-settlement-period",
                help="Include settlement period column (CSV only).",
            ),
        ] = False,
        time_display: Annotated[
            Optional[str],
            typer.Option(
                "--time-display",
                help='Rolling window as JSON, e.g. \'{"mode":"rolling","periodback":"daily",...}\'.',
            ),
        ] = None,
    ) -> None:
        """Fetch Chart API data and write the raw response."""
        state: CliState = ctx.obj["state"]
        client = None
        try:
            client = make_client(state)
            result = client.chart.get(
                format,
                code=code,
                resolution=resolution,
                start_dt=start,
                end_dt=end,
                time_zone=tz,
                currency=currency,
                min_avg_max=min_avg_max,
                delimiter=delimiter,
                enable_settlement_period=enable_settlement_period,
                time_display=parse_time_display(time_display),
                units=units,
            )
            deliver_result(result, output, table, title=f"chart:{code}")
        except Exception as exc:
            handle_cli_error(exc, state.verbose)
        finally:
            if client is not None:
                client.close()
