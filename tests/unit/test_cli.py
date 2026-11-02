"""Unit tests for the EnAppSys CLI."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import typer
from typer.testing import CliRunner

from enappsys import __version__
from enappsys.cli import app
from enappsys.cli_helpers import EXIT_USAGE, parse_chunk_rows, write_response
from enappsys.exceptions import NoCredentialsError, ValidationError

runner = CliRunner()


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_bulk_missing_required_flags():
    result = runner.invoke(app, ["bulk"])
    assert result.exit_code != 0


def test_bulk_writes_response(monkeypatch, tmp_path):
    captured = {}

    class FakeBulk:
        def get(self, response_format, **kwargs):
            captured["format"] = response_format
            captured["kwargs"] = kwargs
            return SimpleNamespace(response="col1,col2\n1,2\n")

    class FakeClient:
        def __init__(self, state):
            captured["state"] = state
            self.bulk = FakeBulk()

        def close(self):
            captured["closed"] = True

    monkeypatch.setattr("enappsys.cli_commands.bulk.make_client", FakeClient)

    out = tmp_path / "out.csv"
    result = runner.invoke(
        app,
        [
            "--user",
            "u",
            "--secret",
            "s",
            "bulk",
            "--data-type",
            "EPEX_HR_AUCTION_RESULTS_DE",
            "--entity",
            "DA_PRICE",
            "--entity",
            "DA_VOLUME",
            "--start",
            "2025-01-01T00:00",
            "--end",
            "2025-01-02T00:00",
            "--resolution",
            "qh",
            "--tz",
            "CET",
            "--chunk-rows",
            "true",
            "-o",
            str(out),
        ],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    assert out.read_text(encoding="utf-8") == "col1,col2\n1,2\n"
    assert captured["format"] == "csv"
    assert captured["kwargs"]["data_type"] == "EPEX_HR_AUCTION_RESULTS_DE"
    assert captured["kwargs"]["entities"] == ["DA_PRICE", "DA_VOLUME"]
    assert captured["kwargs"]["start_dt"] == "2025-01-01T00:00"
    assert captured["kwargs"]["end_dt"] == "2025-01-02T00:00"
    assert captured["kwargs"]["resolution"] == "qh"
    assert captured["kwargs"]["time_zone"] == "CET"
    assert captured["kwargs"]["chunk_rows"] is True
    assert captured["closed"] is True


def test_bulk_stdout_json(monkeypatch):
    class FakeBulk:
        def get(self, response_format, **kwargs):
            return SimpleNamespace(response={"data": [1]})

    class FakeClient:
        def __init__(self, state):
            self.bulk = FakeBulk()

        def close(self):
            pass

    monkeypatch.setattr("enappsys.cli_commands.bulk.make_client", FakeClient)

    result = runner.invoke(
        app,
        [
            "--user",
            "u",
            "--secret",
            "s",
            "bulk",
            "--data-type",
            "X",
            "--start",
            "2025-01-01T00:00",
            "--end",
            "2025-01-02T00:00",
            "--resolution",
            "qh",
            "--tz",
            "UTC",
            "-f",
            "json",
        ],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    assert '"data"' in result.stdout
    assert "1" in result.stdout


def test_bulk_validation_error_exit_code(monkeypatch):
    class FakeBulk:
        def get(self, *args, **kwargs):
            raise ValidationError(reason="bad", parameter="data_type")

    class FakeClient:
        def __init__(self, state):
            self.bulk = FakeBulk()

        def close(self):
            pass

    monkeypatch.setattr("enappsys.cli_commands.bulk.make_client", FakeClient)

    result = runner.invoke(
        app,
        [
            "--user",
            "u",
            "--secret",
            "s",
            "bulk",
            "--data-type",
            "X",
            "--start",
            "2025-01-01T00:00",
            "--end",
            "2025-01-02T00:00",
            "--resolution",
            "qh",
            "--tz",
            "UTC",
        ],
    )

    assert result.exit_code == EXIT_USAGE
    assert "data_type" in result.stderr or "data_type" in result.stdout


def test_auth_check_success(monkeypatch):
    class FakeCredentials:
        def __init__(self, **kwargs):
            self.user = "example_user"
            self.method = "explicit arguments"

    monkeypatch.setattr("enappsys.cli.Credentials", FakeCredentials)

    result = runner.invoke(app, ["--user", "u", "--secret", "s", "auth", "check"])
    assert result.exit_code == 0
    assert "example_user" in result.stdout
    assert "explicit arguments" in result.stdout


def test_auth_check_failure(monkeypatch):
    def boom(**kwargs):
        raise NoCredentialsError()

    monkeypatch.setattr("enappsys.cli.Credentials", boom)

    result = runner.invoke(app, ["auth", "check"])
    assert result.exit_code == EXIT_USAGE
    assert "credentials" in (result.stderr + result.stdout).lower()


def test_chart_passes_time_display(monkeypatch):
    captured = {}

    class FakeChart:
        def get(self, response_format, **kwargs):
            captured["kwargs"] = kwargs
            return SimpleNamespace(response="a,b\n")

    class FakeClient:
        def __init__(self, state):
            self.chart = FakeChart()

        def close(self):
            pass

    monkeypatch.setattr("enappsys.cli_commands.chart.make_client", FakeClient)

    result = runner.invoke(
        app,
        [
            "--user",
            "u",
            "--secret",
            "s",
            "chart",
            "--code",
            "de/elec/pricing/daprices",
            "--resolution",
            "qh",
            "--time-display",
            '{"mode":"rolling_period","periodfor":"yearly","amountfor":4}',
        ],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    assert captured["kwargs"]["time_display"] == {
        "mode": "rolling_period",
        "periodfor": "yearly",
        "amountfor": 4,
    }


def test_pvc_and_epex_commands(monkeypatch):
    calls = []

    class FakePvc:
        def get(self, *args, **kwargs):
            calls.append(("pvc", args, kwargs))
            return SimpleNamespace(response="pvc\n")

    class FakeContract:
        def get(self, *args, **kwargs):
            calls.append(("contract", args, kwargs))
            return SimpleNamespace(response="contract\n")

    class FakeSettlement:
        def get(self, *args, **kwargs):
            calls.append(("settlement", args, kwargs))
            return SimpleNamespace(response="settlement\n")

    class FakeClient:
        def __init__(self, state):
            self.price_volume_curve = FakePvc()
            self.epex = SimpleNamespace(
                contract=FakeContract(),
                settlement=FakeSettlement(),
            )

        def close(self):
            pass

    monkeypatch.setattr(
        "enappsys.cli_commands.price_volume_curve.make_client", FakeClient
    )
    monkeypatch.setattr("enappsys.cli_commands.epex.make_client", FakeClient)

    pvc = runner.invoke(
        app,
        [
            "--user",
            "u",
            "--secret",
            "s",
            "pvc",
            "--code",
            "hu/elec/x",
            "--dt",
            "2025-01-01T00:00",
            "--tz",
            "CET",
            "--currency",
            "EUR",
        ],
    )
    assert pvc.exit_code == 0, pvc.stdout + pvc.stderr
    assert pvc.stdout == "pvc\n"

    contract = runner.invoke(
        app,
        [
            "--user",
            "u",
            "--secret",
            "s",
            "epex",
            "contract",
            "--code",
            "DE",
            "--contract",
            "ABC",
        ],
    )
    assert contract.exit_code == 0, contract.stdout + contract.stderr

    settlement = runner.invoke(
        app,
        [
            "--user",
            "u",
            "--secret",
            "s",
            "epex",
            "settlement",
            "--code",
            "DE",
            "--settlement-datetime",
            "2025-01-01T12:00",
        ],
    )
    assert settlement.exit_code == 0, settlement.stdout + settlement.stderr
    assert len(calls) == 3


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("true", True),
        ("false", False),
        ("10000", 10000),
    ],
)
def test_parse_chunk_rows(value, expected):
    assert parse_chunk_rows(value) == expected


def test_write_response_json_file(tmp_path):
    path = tmp_path / "nested" / "out.json"
    write_response({"a": 1}, path)
    assert path.read_text(encoding="utf-8") == '{\n  "a": 1\n}\n'


def test_make_client_kwargs_from_global_options(monkeypatch):
    seen = {}

    class FakeCredentials:
        def __init__(self, **kwargs):
            seen.update(kwargs)
            self.user = kwargs.get("user") or "u"
            self.method = "explicit arguments"

    monkeypatch.setattr("enappsys.cli.Credentials", FakeCredentials)

    result = runner.invoke(
        app,
        [
            "--user",
            "alice",
            "--secret",
            "secret",
            "--credentials-file",
            "/tmp/creds.json",
            "auth",
            "check",
        ],
    )
    assert result.exit_code == 0
    assert seen["user"] == "alice"
    assert seen["secret"] == "secret"
    assert seen["credentials_file"] == "/tmp/creds.json"


def test_bulk_table_opens_viewer_without_stdout(monkeypatch):
    shown = {}

    class FakeBulk:
        def get(self, *args, **kwargs):
            return SimpleNamespace(response="a,b\n1,2\n", to_df=lambda: None)

    class FakeClient:
        def __init__(self, state):
            self.bulk = FakeBulk()

        def close(self):
            pass

    def fake_show_table(result, title="EnAppSys"):
        shown["title"] = title
        shown["result"] = result

    monkeypatch.setattr("enappsys.cli_commands.bulk.make_client", FakeClient)
    monkeypatch.setattr("enappsys.cli_table.show_table", fake_show_table)

    result = runner.invoke(
        app,
        [
            "--user",
            "u",
            "--secret",
            "s",
            "bulk",
            "--data-type",
            "X",
            "--start",
            "2025-01-01T00:00",
            "--end",
            "2025-01-02T00:00",
            "--resolution",
            "qh",
            "--tz",
            "UTC",
            "--table",
        ],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    assert result.stdout == ""
    assert shown["title"] == "bulk:X"


def test_bulk_table_with_output_still_writes_file(monkeypatch, tmp_path):
    shown = {}

    class FakeBulk:
        def get(self, *args, **kwargs):
            return SimpleNamespace(response="a,b\n1,2\n")

    class FakeClient:
        def __init__(self, state):
            self.bulk = FakeBulk()

        def close(self):
            pass

    def fake_show_table(result, title="EnAppSys"):
        shown["called"] = True

    monkeypatch.setattr("enappsys.cli_commands.bulk.make_client", FakeClient)
    monkeypatch.setattr("enappsys.cli_table.show_table", fake_show_table)

    out = tmp_path / "out.csv"
    result = runner.invoke(
        app,
        [
            "--user",
            "u",
            "--secret",
            "s",
            "bulk",
            "--data-type",
            "X",
            "--start",
            "2025-01-01T00:00",
            "--end",
            "2025-01-02T00:00",
            "--resolution",
            "qh",
            "--tz",
            "UTC",
            "--table",
            "-o",
            str(out),
        ],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    assert out.read_text(encoding="utf-8") == "a,b\n1,2\n"
    assert shown["called"] is True


def test_show_table_requires_tui_extra(monkeypatch):
    from enappsys import cli_table

    def boom():
        raise ImportError("Interactive table view requires the tui extra.")

    monkeypatch.setattr(cli_table, "require_textual", boom)

    with pytest.raises(typer.Exit) as exc_info:
        cli_table.show_table(SimpleNamespace())
    assert exc_info.value.exit_code == EXIT_USAGE


def test_dataframe_from_result_rejects_xml_like():
    from enappsys.cli_table import _dataframe_from_result

    with pytest.raises(ValueError, match="table"):
        _dataframe_from_result(SimpleNamespace(response="<xml/>"))


def test_limit_dataframe_caps_at_10k():
    import pandas as pd

    from enappsys.cli_table import TABLE_ROW_LIMIT, _limit_dataframe, _subtitle

    assert TABLE_ROW_LIMIT == 10_000
    df = pd.DataFrame({"a": range(TABLE_ROW_LIMIT + 50)})
    view, total, truncated = _limit_dataframe(df)
    assert truncated is True
    assert total == TABLE_ROW_LIMIT + 50
    assert len(view) == TABLE_ROW_LIMIT
    assert _subtitle(len(view), total, truncated) == (
        f"showing {TABLE_ROW_LIMIT} of {total} rows (limit {TABLE_ROW_LIMIT})"
    )

    small = pd.DataFrame({"a": [1, 2, 3]})
    view, total, truncated = _limit_dataframe(small)
    assert truncated is False
    assert total == 3
    assert len(view) == 3
    assert _subtitle(3, 3, False) == "3 rows"
