from __future__ import annotations

from datetime import datetime

import pytest

from enappsys.services_async.epex import AsyncEpexAPI


JSON_RESPONSE = {
    "data": [
        {"dateTime": "2023-01-01T00:15:00", "Buy": 11.0, "Sell": 13.0},
    ],
    "metadata": {
        "items": {
            "Sell": {"unit": "EUR/MWh"},
            "Buy": {"unit": "EUR/MWh"},
        }
    },
}


@pytest.mark.asyncio
async def test_async_epex_settlement_request_and_to_df():
    captured = {}

    class StubSession:
        async def get(self, url, params):
            captured["url"] = url
            captured["params"] = dict(params)
            return JSON_RESPONSE

    class StubClient:
        def __init__(self):
            self._session = StubSession()

    client_async = StubClient()
    service = AsyncEpexAPI(client_async)

    out = await service.settlement.get(
        "json",
        code="nl/elec/epex/tradeevo/download",
        settlement_date="2023-01-01",
        settlement_period=2,
        max_points=25,
        time_zone="UTC",
    )
    df = out.to_df(unit_in_columns=True)

    assert captured["url"] == "apxdownload"
    assert captured["params"] == {
        "code": "nl/elec/epex/tradeevo/download",
        "start": "202301010015",
        "end": "202301010015",
        "max_points": 25,
        "timezone": "UTC",
        "res": "qh",
        "tag": "JSON",
    }
    assert out.settlement_period == 2
    assert out.settlement_datetime == datetime(2023, 1, 1, 0, 15)
    assert list(df.columns) == ["Sell (EUR/MWh)", "Buy (EUR/MWh)"]
