import logging

import pandas as pd
import pytest

from enappsys.services.bulk import BulkCSV
from enappsys.timezones import localize_index


def naive(*times):
    return pd.DatetimeIndex(list(times))


def offsets(index):
    return [t.strftime("%H:%M%z") for t in index]


def test_a_regular_series_is_inferred_without_a_warning(caplog):
    index = naive("2026-10-25 01:00", "2026-10-25 02:00", "2026-10-25 02:00", "2026-10-25 03:00")

    with caplog.at_level(logging.WARNING):
        localized = localize_index(index, "CET")

    assert offsets(localized) == ["01:00+0200", "02:00+0200", "02:00+0100", "03:00+0100"]
    assert caplog.text == ""


def test_an_autumn_hour_given_once_is_taken_as_summer_time_and_warned(caplog):
    """The platform returns some series on a grid that skips the repeated hour."""
    index = naive("2026-10-25 01:00", "2026-10-25 02:00", "2026-10-25 03:00")

    with caplog.at_level(logging.WARNING):
        localized = localize_index(index, "CET", context="params={'type': 'PRICES_FOR_NL_CCGT_SS'}")

    assert offsets(localized) == ["01:00+0200", "02:00+0200", "03:00+0100"]
    assert localized.is_monotonic_increasing and localized.is_unique
    assert "Could not localize timestamps to CET" in caplog.text
    assert "PRICES_FOR_NL_CCGT_SS" in caplog.text


def test_a_spring_hour_that_does_not_exist_moves_forward_and_is_warned(caplog):
    index = naive("2026-03-29 01:00", "2026-03-29 02:00", "2026-03-29 04:00")

    with caplog.at_level(logging.WARNING):
        localized = localize_index(index, "CET")

    assert offsets(localized) == ["01:00+0100", "03:00+0200", "04:00+0200"]
    assert "Could not localize timestamps to CET" in caplog.text


def test_other_errors_still_raise(caplog):
    with caplog.at_level(logging.WARNING), pytest.raises(Exception):
        localize_index(naive("2026-10-25 01:00"), "Not/AZone")

    assert "Could not localize" not in caplog.text


def test_bulk_csv_keeps_working_across_a_clock_change_with_a_missing_hour(caplog):
    params = {"type": "PRICES_FOR_NL_CCGT_SS", "entities": ["UP"], "user": "u", "pass": "secret"}
    response = (
        "Datetime,Units,UP\n"
        "25/10/2026 01:00,EUR/MWh,1145.81\n"
        "25/10/2026 02:00,EUR/MWh,1145.81\n"
        "25/10/2026 03:00,EUR/MWh,1145.81\n"
    )
    out = BulkCSV(response, "bulk", params, "csv", "PRICES_FOR_NL_CCGT_SS", ["UP"], None, None, "hourly", "CET", False)

    with caplog.at_level(logging.WARNING):
        df = out.to_df()

    assert offsets(df.index) == ["01:00+0200", "02:00+0200", "03:00+0100"]
    assert "PRICES_FOR_NL_CCGT_SS" in caplog.text
    assert "secret" not in caplog.text
