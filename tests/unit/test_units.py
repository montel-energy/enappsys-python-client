"""The ``units`` argument asks the platform to convert a series, on bulk and chart requests.

Without it the platform returns the native unit, e.g. USD/t for API2 coal,
which is easy to mistake for a converted EUR/MWh value downstream.

These tests use a fake session rather than the live API, so they assert on the
query parameters sent rather than on the conversion itself.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from enappsys.config import CHUNK_ROWS
from enappsys.exceptions import ValidationError
from enappsys.services.bulk import BulkAPI
from enappsys.services.chart import ChartAPI
from enappsys.services_async.bulk import AsyncBulkAPI
from enappsys.services_async.chart import AsyncChartAPI


class FakeSession:
    """Records every request and returns a small CSV labelled with the requested unit."""

    def __init__(self):
        self.requests = []

    def get(self, url, params):
        self.requests.append(dict(params))
        return self._csv(params)

    @staticmethod
    def _csv(params):
        unit = params.get("units", "USD/teCOAL")
        return f"Datetime,Units,ALL\n01/01/2024 00:00,{unit},1.0\n01/01/2024 01:00,{unit},2.0\n"

    def build_url(self, url):
        return f"https://example.invalid/{url}"


class FakeAsyncSession(FakeSession):
    async def get(self, url, params):
        self.requests.append(dict(params))
        return self._csv(params)


class FakeClient:
    def __init__(self, session):
        self._session = session


START = datetime(2024, 1, 1)


def kwargs(*, days=1, **extra):
    return dict(
        data_type="spectron_closing_prices_coal_api2_cif_ara_mid",
        entities=["ALL"],
        start_dt=START,
        end_dt=START + timedelta(days=days),
        resolution="hourly",
        time_zone="CET",
        **extra,
    )


@pytest.fixture
def api():
    return BulkAPI(FakeClient(FakeSession()))


@pytest.fixture
def api_async():
    return AsyncBulkAPI(FakeClient(FakeAsyncSession()))


class TestUnits:
    def test_omitted_by_default(self, api):
        api.get("csv", **kwargs())
        assert "units" not in api._session.requests[0]

    @pytest.mark.parametrize("response_format", ["csv", "json", "json_map", "xml"])
    def test_sent_for_every_format(self, api, response_format):
        api.get(response_format, **kwargs(units="EUR/MWh"))
        assert api._session.requests[0]["units"] == "EUR/MWh"

    def test_returned_unit_is_readable_from_the_frame(self, api):
        df = api.get("csv", **kwargs(units="EUR/MWh")).to_df(unit_in_columns=True)
        assert list(df.columns) == ["ALL (EUR/MWh)"]

    def test_carried_to_every_chunk(self, api):
        """A split request must convert every chunk, not only the first."""
        api.get("csv", **kwargs(days=400, units="EUR/MWh", chunk_rows=CHUNK_ROWS))
        requests = api._session.requests
        assert len(requests) > 1
        assert all(r["units"] == "EUR/MWh" for r in requests)

    @pytest.mark.parametrize("units", ["", 12])
    def test_invalid_units_rejected(self, api, units):
        with pytest.raises(ValidationError):
            api.get("csv", **kwargs(units=units))
        assert api._session.requests == []


class TestUnitsAsync:
    @pytest.mark.asyncio
    async def test_omitted_by_default(self, api_async):
        await api_async.get("csv", **kwargs())
        assert "units" not in api_async._session.requests[0]

    @pytest.mark.asyncio
    async def test_carried_to_every_chunk(self, api_async):
        await api_async.get("csv", **kwargs(days=400, units="EUR/MWh", chunk_rows=5000))
        requests = api_async._session.requests
        assert len(requests) > 1
        assert all(r["units"] == "EUR/MWh" for r in requests)


class FakeChartSession(FakeSession):
    """Answers like the Chart API: applies only units it knows, else returns the base unit."""

    KNOWN = {"EUR-MWh": "EUR/MWh", "EUR-MWh_55_Eff": "EUR/MWh 55% Eff"}

    @staticmethod
    def _csv(params):
        unit = FakeChartSession.KNOWN.get(params.get("units"), "USD/teCOAL")
        return f"Date (CET),BID,MID\n,{unit},{unit}\n[01/01/2024 00:00],1.0,2.0\n"


class FakeAsyncChartSession(FakeChartSession):
    async def get(self, url, params):
        self.requests.append(dict(params))
        return self._csv(params)


def chart_kwargs(**extra):
    return dict(
        code="de/elec/spectron/coal",
        start_dt=START,
        end_dt=START + timedelta(days=1),
        resolution="daily",
        time_zone="CET",
        **extra,
    )


@pytest.fixture
def chart_api():
    return ChartAPI(FakeClient(FakeChartSession()))


@pytest.fixture
def chart_api_async():
    return AsyncChartAPI(FakeClient(FakeAsyncChartSession()))


class TestChartUnits:
    def test_omitted_by_default(self, chart_api):
        chart_api.get("csv", **chart_kwargs())
        assert "units" not in chart_api._session.requests[0]

    def test_spelled_the_way_the_chart_api_expects(self, chart_api):
        """Same spelling as the Bulk API; "/" becomes "-", spaces "_", "%" is dropped."""
        df = chart_api.get("csv", **chart_kwargs(units="EUR/MWh 55% Eff")).to_df(unit_in_columns=True)
        assert chart_api._session.requests[0]["units"] == "EUR-MWh_55_Eff"
        assert list(df.columns) == ["BID (EUR/MWh 55% Eff)", "MID (EUR/MWh 55% Eff)"]

    def test_ignored_unit_raises(self, chart_api):
        """The Chart API returns the base unit instead of rejecting the request."""
        with pytest.raises(ValidationError, match="USD/teCOAL"):
            chart_api.get("csv", **chart_kwargs(units="EUR/te"))

    @pytest.mark.parametrize("response_format", ["json", "json_map", "xml"])
    def test_only_csv_supported(self, chart_api, response_format):
        """JSON responses are relabelled with the requested unit but not converted."""
        with pytest.raises(ValidationError):
            chart_api.get(response_format, **chart_kwargs(units="EUR/MWh"))
        assert chart_api._session.requests == []

    @pytest.mark.asyncio
    async def test_async(self, chart_api_async):
        await chart_api_async.get("csv", **chart_kwargs(units="EUR/MWh"))
        assert chart_api_async._session.requests[0]["units"] == "EUR-MWh"
        with pytest.raises(ValidationError):
            await chart_api_async.get("csv", **chart_kwargs(units="EUR/te"))
