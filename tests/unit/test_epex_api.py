from __future__ import annotations

from datetime import datetime


CSV_RESPONSE = """\
,QH Price,QH Volume
,EUR/MWh,MW
[01/01/2023 00:00:00],10.0,100.0
[01/01/2023 00:15:00],11.0,110.0
"""

JSON_RESPONSE = {
    "data": [
        {"dateTime": "2023-01-01T00:00:00", "Buy": 10.0, "Sell": 12.0},
        {"dateTime": "2023-01-01T00:15:00", "Buy": 11.0, "Sell": 13.0},
    ],
    "metadata": {
        "items": {
            "Sell": {"unit": "EUR/MWh"},
            "Buy": {"unit": "EUR/MWh"},
        }
    },
}


def test_epex_contract_csv_request_and_to_df(client, monkeypatch):
    captured = {}

    def fake_get(url, params):
        captured["url"] = url
        captured["params"] = dict(params)
        return CSV_RESPONSE

    monkeypatch.setattr(client._session, "get", fake_get)

    out = client.epex.contract.get(
        "csv",
        code="nl/elec/spectron/power/baseload/evolution/download",
        contract="DEC-23",
        max_points=10,
        time_zone="UTC",
    )
    df = out.to_df(
        rename_columns=["qh_price", "qh_volume"],
        unit_in_columns=True,
    )

    assert captured["url"] == "apxdownload"
    assert captured["params"] == {
        "code": "nl/elec/spectron/power/baseload/evolution/download",
        "contract": "DEC-23",
        "max_points": 10,
        "timezone": "UTC",
        "res": "qh",
        "tag": "CSV",
    }
    assert list(df.columns) == ["qh_price (EUR/MWh)", "qh_volume (MW)"]
    assert df.iloc[0].to_list() == [10.0, 100.0]


def test_epex_settlement_json_request_normalization_and_to_df(client, monkeypatch):
    captured = {}

    def fake_get(url, params):
        captured["url"] = url
        captured["params"] = dict(params)
        return JSON_RESPONSE

    monkeypatch.setattr(client._session, "get", fake_get)

    out = client.epex.settlement.get(
        "json",
        code="nl/elec/epex/tradeevo/download",
        settlement_datetime="2023-01-01T00:07",
        max_points="all",
        time_zone="UTC",
    )
    df = out.to_df(
        rename_columns={"Sell": "sell", "Buy": "buy"},
        unit_in_columns=True,
    )

    assert captured["url"] == "apxdownload"
    assert captured["params"] == {
        "code": "nl/elec/epex/tradeevo/download",
        "start": "202301010000",
        "end": "202301010000",
        "max_points": "all",
        "timezone": "UTC",
        "res": "qh",
        "tag": "JSON",
    }
    assert out.settlement_period == 1
    assert out.settlement_datetime == datetime(2023, 1, 1, 0, 0)
    assert list(df.columns) == ["sell (EUR/MWh)", "buy (EUR/MWh)"]
    assert df.iloc[1].to_list() == [13.0, 11.0]
