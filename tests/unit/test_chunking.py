"""Proactive chunking: splitting wide requests before sending them.

The platform answers some series far more slowly than others, and for those the
cost grows faster than linearly with the rows requested -- a one-year quarter-
hourly request measured 105s as a single call and 7.7s split into 24. Chunking
already existed here, but only as a fallback after an HTTP 413, which those
requests never trigger because they succeed, slowly.

These tests use a fake session rather than the live API, so they assert on how
requests are split rather than on how long they take.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from enappsys.config import CHUNK_ROWS
from enappsys.services.bulk import BulkAPI


class FakeSession:
    """Records every request and returns a CSV chunk covering its range."""

    def __init__(self):
        self.requests = []

    def get(self, url, params):
        self.requests.append(dict(params))
        start, end = params["start"], params["end"]
        return f"Datetime,Units,X\n{start},MW,1.0\n{end},MW,2.0\n"

    def build_url(self, url):
        return f"https://example.invalid/{url}"


class FakeClient:
    def __init__(self):
        self._session = FakeSession()


@pytest.fixture
def api():
    return BulkAPI(FakeClient())


START = datetime(2024, 1, 1)


def call(api, *, days, entities=("A",), response_format="csv", **kwargs):
    return api.get(
        response_format,
        data_type="SOME_TYPE",
        entities=list(entities),
        start_dt=START,
        end_dt=START + timedelta(days=days),
        resolution="qh",
        time_zone="UTC",
        **kwargs,
    )


class TestRowEstimate:
    @pytest.mark.parametrize(
        "resolution,expected",
        [("qh", 96), ("hh", 48), ("hourly", 24), ("min", 1440)],
    )
    def test_one_day_at_each_resolution(self, api, resolution, expected):
        rows = api._estimated_rows(
            datetime(2024, 1, 1), datetime(2024, 1, 2), resolution
        )
        assert rows == expected

    def test_resolution_not_span_drives_the_estimate(self, api):
        """The platform stores each resolution separately rather than
        aggregating, so the same span is a different number of rows."""
        span = (datetime(2024, 1, 1), datetime(2025, 1, 1))
        assert api._estimated_rows(*span, "qh") == 35136
        assert api._estimated_rows(*span, "daily") == 366


class TestShouldChunk:
    def test_below_the_budget_is_left_alone(self, api):
        assert not api._should_chunk(
            datetime(2024, 1, 1), datetime(2024, 1, 10), "qh"
        )

    def test_above_the_budget_is_split(self, api):
        assert api._should_chunk(
            datetime(2024, 1, 1), datetime(2024, 12, 31), "qh"
        )

    def test_entities_count_against_the_budget(self, api):
        """Two entities cost the same as two single-entity requests, so the
        budget is cells rather than rows."""
        span = (datetime(2024, 1, 1), datetime(2024, 2, 1))  # 2976 rows, under 5000
        assert not api._should_chunk(*span, "qh", series=1)
        assert api._should_chunk(*span, "qh", series=2)

    def test_a_missing_bound_means_a_rolling_window(self, api):
        """Chart-style rolling windows have no range to walk."""
        assert not api._should_chunk(None, datetime(2024, 12, 31), "qh")
        assert not api._should_chunk(datetime(2024, 1, 1), None, "qh")

    def test_zero_budget_disables_it(self, api):
        assert not api._should_chunk(
            datetime(2020, 1, 1), datetime(2024, 12, 31), "qh", chunk_rows=0
        )


class TestChunkSize:
    def test_defaults_to_the_configured_budget(self, api):
        assert api._chunk_size() == CHUNK_ROWS

    def test_divides_by_the_number_of_series(self, api):
        assert api._chunk_size(1000, series=4) == 250

    def test_never_returns_zero(self, api):
        assert api._chunk_size(3, series=10) == 1


class TestRequestSplitting:
    def test_a_short_range_stays_one_request(self, api):
        call(api, days=5)
        assert len(api._session.requests) == 1

    def test_a_wide_range_is_split(self, api):
        call(api, days=360)
        # 360 days of qh is 34,560 rows against a 5,000 budget.
        assert len(api._session.requests) == 7

    def test_chunks_tile_the_range_without_gaps(self, api):
        call(api, days=360)
        requests = api._session.requests
        for earlier, later in zip(requests, requests[1:]):
            assert earlier["end"] == later["start"]
        assert requests[0]["start"] == START.strftime("%Y%m%d%H%M")
        assert requests[-1]["end"] == (START + timedelta(days=360)).strftime("%Y%m%d%H%M")

    def test_more_entities_means_more_chunks(self, api):
        """The budget is cells, so four entities halve the rows per chunk
        against two."""
        call(api, days=360, entities=("A", "B"))
        two = len(api._session.requests)
        api._session.requests.clear()
        call(api, days=360, entities=("A", "B", "C", "D"))
        assert len(api._session.requests) == two * 2

    def test_an_explicit_budget_overrides_the_default(self, api):
        call(api, days=360, chunk_rows=50_000)
        assert len(api._session.requests) == 1

    def test_xml_is_never_split(self, api):
        """XML is passed through unprocessed and _assemble_chunks cannot
        stitch it, so splitting it would return a list of fragments."""
        call(api, days=360, response_format="xml")
        assert len(api._session.requests) == 1

    def test_assembled_csv_keeps_a_single_header(self, api):
        result = call(api, days=360)
        assert result.response.count("Datetime,Units,X") == 1


class TestBoundaryAlignment:
    """Chunk boundaries must land on the resolution grid.

    The platform rounds a request's bounds outward to the nearest grid point.
    An interior boundary sitting between two points is therefore rounded
    outward by both neighbouring chunks, and the row at that point is returned
    twice. Callers passing `datetime.now()` hit this on every boundary.
    """

    def test_interior_boundaries_land_on_the_grid(self, api):
        unaligned = datetime(2024, 1, 1, 8, 41, 35, 699892)
        api.get(
            "csv",
            data_type="SOME_TYPE",
            entities=["A"],
            start_dt=unaligned,
            end_dt=unaligned + timedelta(days=365),
            resolution="qh",
            time_zone="UTC",
        )
        requests = api._session.requests
        assert len(requests) > 1, "expected this window to be split"

        # Every boundary except the caller's own start and end.
        for request in requests[1:]:
            minute = int(request["start"][-2:])
            assert minute % 15 == 0, f"boundary off-grid: {request['start']}"

    def test_the_callers_own_bounds_are_left_alone(self, api):
        """Snapping the outer bounds would change the window requested, so the
        split result would stop matching an unsplit one."""
        unaligned = datetime(2024, 1, 1, 8, 41, 35, 699892)
        end = unaligned + timedelta(days=365)
        api.get(
            "csv",
            data_type="SOME_TYPE",
            entities=["A"],
            start_dt=unaligned,
            end_dt=end,
            resolution="qh",
            time_zone="UTC",
        )
        requests = api._session.requests
        assert requests[0]["start"] == unaligned.strftime("%Y%m%d%H%M")
        assert requests[-1]["end"] == end.strftime("%Y%m%d%H%M")

    def test_boundaries_never_go_backwards(self, api):
        """Snapping must not produce a chunk that ends before it starts."""
        unaligned = datetime(2024, 1, 1, 8, 41, 35, 699892)
        api.get(
            "csv",
            data_type="SOME_TYPE",
            entities=["A"],
            start_dt=unaligned,
            end_dt=unaligned + timedelta(days=120),
            resolution="qh",
            time_zone="UTC",
            chunk_rows=97,
        )
        for request in api._session.requests:
            assert request["start"] < request["end"]

    @pytest.mark.parametrize("resolution", ["1s", "min", "qh", "hh", "hourly"])
    def test_every_sub_daily_resolution_snaps(self, api, resolution):
        from enappsys.enum import ResolutionEnum

        delta = ResolutionEnum._from_value(resolution).delta
        unaligned = datetime(2024, 1, 1, 8, 41, 35, 699892)
        snapped = api._floor_to_resolution(unaligned, delta)
        assert snapped <= unaligned
        midnight = unaligned.replace(hour=0, minute=0, second=0, microsecond=0)
        assert (snapped - midnight).total_seconds() % delta.total_seconds() == 0
