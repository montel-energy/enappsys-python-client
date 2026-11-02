"""EnAppSys command-line interface."""

from __future__ import annotations

from typing import Annotated, Optional

import typer

from enappsys import __version__
from enappsys.cli_commands import bulk, chart, epex, price_volume_curve
from enappsys.cli_helpers import CliState, configure_logging, handle_cli_error
from enappsys.credentials import Credentials
from enappsys.exceptions import ClientError

app = typer.Typer(
    name="enappsys",
    help="Download EnAppSys data from the terminal.",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)
auth_app = typer.Typer(help="Credential helpers.")
app.add_typer(auth_app, name="auth")
app.add_typer(epex.app, name="epex")

bulk.register(app)
chart.register(app)
price_volume_curve.register(app)


@app.callback()
def main(
    ctx: typer.Context,
    user: Annotated[
        Optional[str],
        typer.Option("--user", help="EnAppSys username (overrides env / credentials file)."),
    ] = None,
    secret: Annotated[
        Optional[str],
        typer.Option("--secret", help="EnAppSys secret (overrides env / credentials file)."),
    ] = None,
    credentials_file: Annotated[
        Optional[str],
        typer.Option(
            "--credentials-file",
            help="Path to credentials JSON (default: ~/.credentials/enappsys.json).",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Show debug logging and fuller errors."),
    ] = False,
) -> None:
    """EnAppSys CLI. Credentials resolve like the Python client."""
    configure_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["state"] = CliState(
        user=user,
        secret=secret,
        credentials_file=credentials_file,
        verbose=verbose,
    )


@app.command("version")
def version() -> None:
    """Print the installed package version."""
    typer.echo(__version__)


@auth_app.command("check")
def auth_check(ctx: typer.Context) -> None:
    """Verify that credentials can be loaded (does not call the API)."""
    state: CliState = ctx.obj["state"]
    try:
        credentials = Credentials(
            user=state.user,
            secret=state.secret,
            credentials_file=state.credentials_file,
        )
        typer.echo(
            f"Credentials OK for user={credentials.user} (via {credentials.method})"
        )
    except ClientError as exc:
        handle_cli_error(exc, state.verbose)
    except Exception as exc:
        handle_cli_error(exc, state.verbose)
