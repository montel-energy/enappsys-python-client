"""Optional Textual table viewer for CLI download results."""

from __future__ import annotations

from typing import Any

import typer

from enappsys.cli_helpers import EXIT_USAGE
from enappsys.utils import require_pandas

#: Max rows fed into the Textual DataTable. Larger results are truncated.
TABLE_ROW_LIMIT = 10_000


def require_textual():
    try:
        import textual
    except ModuleNotFoundError as e:
        raise ImportError(
            "Interactive table view requires the tui extra. "
            "Install with: pip install enappsys[tui]"
        ) from e
    return textual


def _dataframe_from_result(result: Any):
    if not hasattr(result, "to_df"):
        raise ValueError(
            "This response format cannot be shown as a table. "
            "Use -f csv or -f json (XML has no table view)."
        )
    pd = require_pandas()
    df = result.to_df()
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Could not convert the response to a table.")
    if df.index.name is not None or not isinstance(df.index, pd.RangeIndex):
        df = df.reset_index()
    return df


def _limit_dataframe(df, limit: int = TABLE_ROW_LIMIT):
    """Return ``(view_df, total_rows, truncated)`` capped at ``limit`` rows."""
    total = len(df)
    if total <= limit:
        return df, total, False
    return df.iloc[:limit].copy(), total, True


def _cells_from_dataframe(df) -> tuple[list[str], list[tuple[str, ...]]]:
    pd = require_pandas()
    columns = [str(c) for c in df.columns]

    def cell(value: Any) -> str:
        if value is None or (isinstance(value, float) and value != value):
            return ""
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass
        return str(value)

    rows = [
        tuple(cell(value) for value in row)
        for row in df.itertuples(index=False, name=None)
    ]
    return columns, rows


def _subtitle(shown: int, total: int, truncated: bool) -> str:
    if truncated:
        return f"showing {shown} of {total} rows (limit {TABLE_ROW_LIMIT})"
    return f"{total} rows"


def show_table(result: Any, title: str = "EnAppSys") -> None:
    """Open a Textual DataTable for ``result`` (needs ``enappsys[tui]``).

    At most :data:`TABLE_ROW_LIMIT` rows are shown; larger frames are truncated.
    """
    try:
        require_textual()
        df = _dataframe_from_result(result)
        view_df, total_rows, truncated = _limit_dataframe(df)
        columns, rows = _cells_from_dataframe(view_df)
        subtitle = _subtitle(len(rows), total_rows, truncated)
    except ImportError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(EXIT_USAGE) from e
    except ValueError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(EXIT_USAGE) from e

    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.widgets import DataTable, Footer, Header

    class DatasetTableApp(App[None]):
        CSS = """
        DataTable {
            height: 1fr;
        }
        """
        BINDINGS = [
            Binding("q", "quit", "Quit"),
        ]

        def __init__(self) -> None:
            super().__init__()
            self._table_title = title
            self._subtitle = subtitle
            self._columns = columns
            self._rows = rows

        def compose(self) -> ComposeResult:
            yield Header()
            yield DataTable(zebra_stripes=True)
            yield Footer()

        def on_mount(self) -> None:
            self.title = self._table_title
            self.sub_title = self._subtitle
            table = self.query_one(DataTable)
            table.cursor_type = "row"
            table.add_columns(*self._columns)
            if self._rows:
                table.add_rows(self._rows)

    DatasetTableApp().run()
