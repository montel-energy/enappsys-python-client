"""Proactive chunking: splitting wide requests before sending them.

Chunking already existed here, but only as a fallback after an HTTP 413.
Requests wide enough to be worth splitting do not necessarily trigger that, so
it now also happens up front, above a row budget.

These tests use a fake session rather than the live API, so they assert on how
requests are split, not on what comes back.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from enappsys.config import CHUNK_ROWS
from enappsys.exceptions import ContentTooLarge
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
    """Opts in to splitting unless a test says otherwise; it is off by default."""
    kwargs.setdefault("chunk_rows", CHUNK_ROWS)
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
            datetime(2024, 1, 1), datetime(2024, 1, 10), "qh", chunk_rows=CHUNK_ROWS
        )

    def test_above_the_budget_is_split(self, api):
        assert api._should_chunk(
            datetime(2024, 1, 1), datetime(2024, 12, 31), "qh", chunk_rows=CHUNK_ROWS
        )

    def test_entities_count_against_the_budget(self, api):
        """Two entities cost the same as two single-entity requests, so the
        budget is cells rather than rows."""
        span = (datetime(2024, 1, 1), datetime(2024, 2, 1))  # 2976 rows, under 5000
        assert not api._should_chunk(*span, "qh", CHUNK_ROWS, series=1)
        assert api._should_chunk(*span, "qh", CHUNK_ROWS, series=2)

    def test_a_missing_bound_means_a_rolling_window(self, api):
        """Chart-style rolling windows have no range to walk."""
        assert not api._should_chunk(
            None, datetime(2024, 12, 31), "qh", chunk_rows=CHUNK_ROWS
        )
        assert not api._should_chunk(
            datetime(2024, 1, 1), None, "qh", chunk_rows=CHUNK_ROWS
        )

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

    def test_a_wider_budget_means_fewer_requests(self, api):
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
            chunk_rows=CHUNK_ROWS,
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
            chunk_rows=CHUNK_ROWS,
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


    def test_boundaries_clear_the_hour_for_coarser_series(self, api):
        """A series can be published coarser than the resolution requested.

        ENTSOE generation for the Nordic zones is hourly and is routinely
        fetched at quarter-hourly. A boundary on the requested grid but inside
        an hour is rounded outward by both neighbouring chunks on the series'
        own grid, and the row at that point comes back twice. The real case:
        28 days over seven entities, starting at 12:45.
        """
        start = datetime(2024, 1, 1, 12, 45)
        api.get(
            "csv",
            data_type="SOME_TYPE",
            entities=["A", "B", "C", "D", "E", "F", "G"],
            start_dt=start,
            end_dt=start + timedelta(days=28),
            resolution="qh",
            time_zone="UTC",
            chunk_rows=CHUNK_ROWS,
        )
        requests = api._session.requests
        assert len(requests) > 1, "expected this window to be split"
        for request in requests[1:]:
            assert request["start"].endswith("00"), (
                f"boundary sits inside an hour: {request['start']}"
            )


AWARE = datetime(2024, 1, 1, tzinfo=timezone.utc)
NAIVE = datetime(2024, 1, 1)


class TestMixedAwarenessBounds:
    """One aware and one naive bound, which callers produce by accident.

    `pendulum` returns a *naive* DateTime from `astimezone(...) + timedelta`,
    so deriving `end_dt` that way from an aware `start_dt` yields one of each.
    Before chunking the client only ever formatted each bound, which does not
    care; the first arithmetic between them turned that into a TypeError at the
    top of every request, whatever its size, and even when nothing would split.
    """

    @pytest.mark.parametrize(
        "start,end",
        [
            (AWARE, NAIVE + timedelta(days=1)),
            (NAIVE, AWARE + timedelta(days=1)),
            (AWARE, AWARE + timedelta(days=1)),
            (NAIVE, NAIVE + timedelta(days=1)),
        ],
        ids=["aware-naive", "naive-aware", "both-aware", "both-naive"],
    )
    def test_the_chunk_decision_never_raises(self, api, start, end):
        assert api._should_chunk(start, end, "qh", CHUNK_ROWS, series=1) is False

    def test_the_estimate_ignores_the_offset(self, api):
        """Bounds are sent as wall clock, so the span is the wall-clock one."""
        assert api._estimated_rows(AWARE, NAIVE + timedelta(days=1), "qh") == 96

    def test_a_split_request_survives_mixed_bounds(self, api):
        api.get(
            "csv",
            data_type="SOME_TYPE",
            entities=["A"],
            start_dt=AWARE,
            end_dt=NAIVE + timedelta(days=150),
            resolution="qh",
            time_zone="UTC",
            chunk_rows=CHUNK_ROWS,
        )
        assert len(api._session.requests) > 1


class TestSplittingIsOptIn:
    """Several requests are not one request.

    If the range is updated while the chunks are in flight, the later ones
    carry the new values and the earlier ones do not. Nothing here can detect
    that, let alone repair it, so a caller chooses splitting rather than
    getting it by default.
    """

    def _fetch(self, api, days=1095, **kwargs):
        api.get(
            "csv",
            data_type="SOME_TYPE",
            entities=["A"],
            start_dt=START,
            end_dt=START + timedelta(days=days),
            resolution="qh",
            time_zone="UTC",
            **kwargs,
        )
        return api._session.requests

    def test_a_wide_range_is_one_request_by_default(self, api):
        assert len(self._fetch(api)) == 1

    def test_asking_for_it_splits(self, api):
        assert len(self._fetch(api, chunk_rows=CHUNK_ROWS)) > 1

    def test_the_413_fallback_still_splits(self, api):
        """The reactive path is untouched: it is what keeps an oversized
        request working at all, and it is not a choice the caller makes."""
        original = api._session.get
        calls = {"n": 0}

        def raise_once(url, params):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ContentTooLarge("payload too large")
            return original(url, params)

        api._session.get = raise_once
        # The reactive path splits at API_MAX_ROWS, so the range has to exceed it.
        assert len(self._fetch(api, days=1825)) > 1


class TestBoundaryGrids:
    def test_grids_come_from_the_platform_resolutions(self, api):
        """Every resolution up to a day divides a day evenly, which is what
        makes flooring from midnight well defined. Weekly and longer need an
        epoch to count from, so they are left out."""
        grids = api._boundary_grids(timedelta(minutes=15))
        assert grids == sorted(grids, reverse=True)
        assert grids[0] == timedelta(days=1)
        assert all(timedelta(minutes=15) < g <= timedelta(days=1) for g in grids)
        assert all(86400 % g.total_seconds() == 0 for g in grids)

    def test_nothing_coarser_than_the_resolution_asked_for(self, api):
        assert api._boundary_grids(timedelta(days=1)) == []


class TestOptInForms:
    """What `chunk_rows` accepts, and what each form means."""

    WIDE = timedelta(days=1095)   # 105,120 qh rows, under API_MAX_ROWS

    def _requests(self, api, **kwargs):
        api.get(
            "csv",
            data_type="SOME_TYPE",
            entities=["A"],
            start_dt=START,
            end_dt=START + self.WIDE,
            resolution="qh",
            time_zone="UTC",
            **kwargs,
        )
        return api._session.requests

    @pytest.mark.parametrize("value", [None, False, 0])
    def test_these_all_mean_one_request(self, api, value):
        assert api._resolve_chunk_rows(value) is None
        assert len(self._requests(api, chunk_rows=value)) == 1

    def test_true_means_the_configured_budget(self, api):
        """Opting in without naming a number picks CHUNK_ROWS, so `True` and
        passing that constant are the same request pattern."""
        assert api._resolve_chunk_rows(True) == CHUNK_ROWS

    def test_true_and_the_constant_split_identically(self, api):
        by_flag = [dict(r) for r in self._requests(api, chunk_rows=True)]
        api._session.requests.clear()
        by_number = [dict(r) for r in self._requests(api, chunk_rows=CHUNK_ROWS)]
        assert len(by_flag) > 1
        assert by_flag == by_number

    def test_an_int_names_the_budget(self, api):
        assert api._resolve_chunk_rows(1234) == 1234

    def test_true_is_not_read_as_the_number_one(self, api):
        """`isinstance(True, int)` is True, so the order of the checks matters:
        read as 1, every request would split into single rows."""
        assert api._resolve_chunk_rows(True) != 1
        assert api._resolve_chunk_rows(1) == 1
