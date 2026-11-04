"""``enappsys bulk`` download command."""

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
    parse_chunk_rows,
)


def register(app: typer.Typer) -> None:
    @app.command("bulk")
    def bulk(
        ctx: typer.Context,
        data_type: Annotated[
            str,
            typer.Option(
                "--data-type",
                help="Bulk data type, e.g. EPEX_HR_AUCTION_RESULTS_DE.",
            ),
        ],
        start: Annotated[
            str,
            typer.Option(
                "--start",
                help="Start datetime (inclusive), e.g. 2025-01-01T00:00.",
            ),
        ],
        end: Annotated[
            str,
            typer.Option(
                "--end",
                help="End datetime (exclusive), e.g. 2025-01-02T00:00.",
            ),
        ],
        resolution: Annotated[
            str,
            typer.Option("--resolution", help="Series resolution, e.g. qh, hr, daily."),
        ],
        tz: Annotated[
            str,
            typer.Option("--tz", help="Time zone for the request, e.g. CET or UTC."),
        ],
        entity: Annotated[
            Optional[list[str]],
            typer.Option(
                "--entity",
                help="Entity within the data type. Repeat for multiple. Omit for all.",
            ),
        ] = None,
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
            typer.Option("--units", help="Request a unit conversion, e.g. EUR/MWh."),
        ] = None,
        chunk_rows: Annotated[
            Optional[str],
            typer.Option(
                "--chunk-rows",
                help="Split wide requests: true, false, or an integer row budget.",
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
    ) -> None:
        """Fetch Bulk API data and write the raw response."""
        state: CliState = ctx.obj["state"]
        client = None
        try:
            client = make_client(state)
            result = client.bulk.get(
                format,
                data_type=data_type,
                entities=entity,
                start_dt=start,
                end_dt=end,
                resolution=resolution,
                time_zone=tz,
                min_avg_max=min_avg_max,
                delimiter=delimiter,
                chunk_rows=parse_chunk_rows(chunk_rows),
                units=units,
            )
            deliver_result(result, output, table, title=f"bulk:{data_type}")
        except Exception as exc:
            handle_cli_error(exc, state.verbose)
        finally:
            if client is not None:
                client.close()
