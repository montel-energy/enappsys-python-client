"""
Pytest fixtures for EnAppSys testing, using yield for setup and teardown logic.
"""

import asyncio
import pandas as pd
import pytest_asyncio
import pytest

from datetime import datetime
from zoneinfo import ZoneInfo

from enappsys import EnAppSys, EnAppSysAsync


@pytest.fixture(scope="module")
def client():
    c = EnAppSys()
    yield c
    c._session.session.close()


@pytest_asyncio.fixture(scope="function")
async def client_async():
    c = EnAppSysAsync()
    yield c
    await c._session.close()


@pytest.fixture(scope="module")
def api_params():
    return {
        "data_type": "ENTSOE_DAY_AHEAD_PRICES",
        "entities": ["BE.BELGIUM", "NL.NETHERLANDS"],
        "start_dt": datetime(year=2023, month=1, day=1),
        "end_dt": datetime(year=2023, month=1, day=1, hour=3),
        "resolution": "hourly",
        "time_zone": "UTC",
    }


@pytest.fixture(scope="module")
def expected_data(api_params):
    zone_info = ZoneInfo(api_params["time_zone"])
    return [
        [
            "datetime",
            "ENTSOE_DAY_AHEAD_PRICES.be_dap",
            "ENTSOE_DAY_AHEAD_PRICES.nl_dap",
        ],
        [datetime(2023, 1, 1, 0, tzinfo=zone_info), -1.75, -1.46],
        [datetime(2023, 1, 1, 1, tzinfo=zone_info), -1.46, -1.52],
        [datetime(2023, 1, 1, 2, tzinfo=zone_info), -5.27, -5.0],
    ]


@pytest.fixture(scope="module")
def expected_df(expected_data):
    df = pd.DataFrame(data=expected_data[1:], columns=expected_data[0]).set_index(
        "datetime"
    )
    return df
