from __future__ import annotations

from typing import TYPE_CHECKING

import logging

from enappsys.services.bulk import BulkCSV, BulkJSON, BulkJSONMap
from enappsys.services.chart import ChartCSV, ChartJSON, ChartJSONMap

if TYPE_CHECKING:
    from enappsys import EnAppSys


def test_csv_to_list(client: "EnAppSys", api_params, expected_data):
    out = client.bulk.get("csv", **api_params)
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    header = ["datetime"] + list(df.columns)
    rows = [[idx, *values] for idx, *values in df.itertuples(index=True, name=None)]
    actual = [header, *rows]
    assert actual == expected_data


def test_csv_to_df(client: "EnAppSys", api_params, expected_df):
    out = client.bulk.get("csv", **api_params)
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    assert df.equals(expected_df)


def test_json_to_df(client: "EnAppSys", api_params, expected_df):
    out = client.bulk.get("json", **api_params)
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    assert df.equals(expected_df)


def test_json_map_to_df(client: "EnAppSys", api_params, expected_df):
    out = client.bulk.get("json_map", **api_params)
    df = out.to_df(
        data_type_in_columns=True,
        rename_columns=["be_dap", "nl_dap"],
    )
    assert df.equals(expected_df)


def test_empty_bulk_csv_to_df_returns_empty_dataframe(caplog):
    params = {
        "type": "ENTSOE_DAY_AHEAD_PRICES",
        "entities": ["BE.BELGIUM"],
        "start": "202301010000",
        "end": "202301010300",
        "user": "test-user",
        "pass": "test-secret",
    }
    out = BulkCSV(
        "Datetime,Units,BE.BELGIUM\n",
        "bulk",
        params,
        "csv",
        "ENTSOE_DAY_AHEAD_PRICES",
        ["BE.BELGIUM"],
        None,
        None,
        "hourly",
        "UTC",
        False,
    )

    with caplog.at_level(logging.WARNING):
        df = out.to_df()

    assert df.empty
    assert list(df.columns) == ["BE.BELGIUM"]
    assert "returning empty DataFrame" in caplog.text
    assert "url=bulk" in caplog.text
    assert "ENTSOE_DAY_AHEAD_PRICES" in caplog.text
    assert "BE.BELGIUM" in caplog.text
    assert "202301010000" in caplog.text
    assert "test-user" not in caplog.text
    assert "test-secret" not in caplog.text


def test_bulk_empty_warning_uses_full_url(client: "EnAppSys", monkeypatch, caplog):
    def fake_get(url, params):
        assert url == "csvapi"
        return "Datetime,Units,BE.BELGIUM\n"

    monkeypatch.setattr(client._session, "get", fake_get)

    out = client.bulk.get(
        "csv",
        data_type="ENTSOE_DAY_AHEAD_PRICES",
        entities=["BE.BELGIUM"],
        start_dt="2023-01-01T00:00",
        end_dt="2023-01-01T03:00",
        resolution="hourly",
        time_zone="UTC",
    )

    with caplog.at_level(logging.WARNING):
        df = out.to_df()

    assert df.empty
    assert "url=https://app.enappsys.com/csvapi" in caplog.text


def test_empty_chart_csv_to_df_returns_empty_dataframe(caplog):
    params = {
        "code": "day-ahead-prices",
        "start": "202301010000",
        "end": "202301010300",
    }
    out = ChartCSV(
        "Datetime,BE.BELGIUM\n,EUR/MWh\n",
        "datadownload",
        params,
        "csv",
        "day-ahead-prices",
        None,
        None,
        "hourly",
        "UTC",
        "EUR",
        False,
    )

    with caplog.at_level(logging.WARNING):
        df = out.to_df()

    assert df.empty
    assert list(df.columns) == ["BE.BELGIUM"]
    assert "returning empty DataFrame" in caplog.text
    assert "url=datadownload" in caplog.text
    assert "day-ahead-prices" in caplog.text
    assert "202301010300" in caplog.text


def test_chart_empty_warning_uses_full_url(client: "EnAppSys", monkeypatch, caplog):
    def fake_get(url, params):
        assert url == "datadownload"
        return "Datetime,BE.BELGIUM\n,EUR/MWh\n"

    monkeypatch.setattr(client._session, "get", fake_get)

    out = client.chart.get(
        "csv",
        code="day-ahead-prices",
        start_dt="2023-01-01T00:00",
        end_dt="2023-01-01T03:00",
        resolution="hourly",
        time_zone="UTC",
    )

    with caplog.at_level(logging.WARNING):
        df = out.to_df()

    assert df.empty
    assert "url=https://app.enappsys.com/datadownload" in caplog.text


def test_empty_json_to_df_returns_empty_dataframe(caplog):
    response = {"data": [], "metadata": {"dataTypes": {}}}
    params = {
        "type": "ENTSOE_DAY_AHEAD_PRICES",
        "entities": ["BE.BELGIUM"],
        "start": "202301010000",
        "end": "202301010300",
    }
    out = BulkJSON(
        response,
        "bulk",
        params,
        "json",
        "ENTSOE_DAY_AHEAD_PRICES",
        ["BE.BELGIUM"],
        None,
        None,
        "hourly",
        "UTC",
        False,
    )

    with caplog.at_level(logging.WARNING):
        df = out.to_df()

    assert df.empty
    assert "returning empty DataFrame" in caplog.text
    assert "ENTSOE_DAY_AHEAD_PRICES" in caplog.text
    assert "BE.BELGIUM" in caplog.text


def test_empty_json_map_to_df_returns_empty_dataframe(caplog):
    response = {"data": {}, "metadata": {"dataTypes": {}}}
    params = {
        "type": "ENTSOE_DAY_AHEAD_PRICES",
        "entities": ["BE.BELGIUM"],
        "start": "202301010000",
        "end": "202301010300",
    }
    out = BulkJSONMap(
        response,
        "bulk",
        params,
        "json_map",
        "ENTSOE_DAY_AHEAD_PRICES",
        ["BE.BELGIUM"],
        None,
        None,
        "hourly",
        "UTC",
        False,
    )

    with caplog.at_level(logging.WARNING):
        df = out.to_df()

    assert df.empty
    assert df.index.name == "dateTime"
    assert "returning empty DataFrame" in caplog.text
    assert "ENTSOE_DAY_AHEAD_PRICES" in caplog.text
    assert "BE.BELGIUM" in caplog.text


def test_empty_chart_json_to_df_returns_empty_dataframe(caplog):
    response = {"data": [], "metadata": {"dataTypes": {}}}
    params = {
        "code": "day-ahead-prices",
        "start": "202301010000",
        "end": "202301010300",
    }
    out = ChartJSON(
        response,
        "datadownload",
        params,
        "json",
        "day-ahead-prices",
        None,
        None,
        "hourly",
        "UTC",
        "EUR",
        False,
    )

    with caplog.at_level(logging.WARNING):
        df = out.to_df()

    assert df.empty
    assert "returning empty DataFrame" in caplog.text
    assert "day-ahead-prices" in caplog.text
    assert "202301010000" in caplog.text


def test_empty_chart_json_map_to_df_returns_empty_dataframe(caplog):
    response = {"data": {}, "metadata": {"dataTypes": {}}}
    params = {
        "code": "day-ahead-prices",
        "start": "202301010000",
        "end": "202301010300",
    }
    out = ChartJSONMap(
        response,
        "datadownload",
        params,
        "json_map",
        "day-ahead-prices",
        None,
        None,
        "hourly",
        "UTC",
        "EUR",
        False,
    )

    with caplog.at_level(logging.WARNING):
        df = out.to_df()

    assert df.empty
    assert df.index.name == "dateTime"
    assert "returning empty DataFrame" in caplog.text
    assert "day-ahead-prices" in caplog.text
    assert "202301010300" in caplog.text
