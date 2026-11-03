"""Allow ``python -m enappsys`` to run the CLI."""

import sys


def main() -> None:
    """Run the CLI, or say how to install it when the cli extra is missing."""
    try:
        from enappsys.cli import app
    except ModuleNotFoundError as exc:
        if exc.name != "typer":
            raise
        sys.exit("The enappsys command requires the cli extra: pip install enappsys[cli]")
    app()


if __name__ == "__main__":
    main()
