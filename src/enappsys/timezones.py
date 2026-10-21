"""Localizing the naive timestamps the platform returns in a requested time zone."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from enappsys.utils import require_pandas

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

def resolve_ambiguous_fold(index: pd.DatetimeIndex, tz: str) -> tuple:
    """Resolve which DST occurrence each ambiguous (autumn clock-change) timestamp in
    ``index`` belongs to, using row order instead of sort order.

    Trade-level data (e.g. settlement downloads) is a blotter of individual trade
    timestamps in row order, not a sorted/regular time series, so pandas'
    ``ambiguous="infer"`` (which assumes a single monotonic run) mis-detects many
    spurious DST switches and raises.

    Instead:
      1. Localize twice, forcing ``ambiguous=True`` and ``ambiguous=False``. Positions
         where the two results disagree are the genuinely ambiguous ones - this is
         vectorized and needs no sorting.
      2. Within each ambiguous window (grouped by local date, in case the data spans
         more than one autumn clock change), find the large backward jump in *row
         order* that marks the moment the clock actually went back: rows before it are
         the first occurrence (DST), rows at/after it are the second occurrence
         (standard time).
      3. If no such jump is found (e.g. the first occurrence has no rows at all, so
         every ambiguous row already belongs to the second occurrence with nothing to
         jump from), the two occurrences can't be told apart from the data alone. We
         assume the first (DST) occurrence for those rows and report that via the
         returned ``all_resolved`` flag so the caller can warn.

    Returns a ``(fold, all_resolved)`` tuple: ``fold`` is a bool array suitable for
    ``ambiguous=`` (True selects the first/DST occurrence; values at non-ambiguous
    positions are ignored by ``tz_localize``), and ``all_resolved`` is False if any
    window had to fall back to the step 3 assumption.
    """
    import numpy as np

    pd = require_pandas()

    loc_dst = index.tz_localize(tz, ambiguous=True, nonexistent="shift_forward")
    loc_std = index.tz_localize(tz, ambiguous=False, nonexistent="shift_forward")
    is_ambiguous = np.asarray(loc_dst != loc_std)

    fold = np.ones(len(index), dtype=bool)
    if not is_ambiguous.any():
        return fold, True

    all_resolved = True
    dates = index.normalize()
    for day in pd.unique(dates[is_ambiguous]):
        positions = np.flatnonzero(is_ambiguous & np.asarray(dates == day))
        deltas = np.diff(index[positions].values) / np.timedelta64(1, "s")
        expected_jump = abs((loc_std[positions[0]] - loc_dst[positions[0]]).total_seconds())
        jump = np.flatnonzero(deltas <= -expected_jump / 2)
        if jump.size:
            fold[positions[jump[0] + 1 :]] = False
        else:
            all_resolved = False

    return fold, all_resolved


def localize_index(index: pd.DatetimeIndex, time_zone: str, *, context: str = "") -> pd.DatetimeIndex:
    """Localize a naive time series index returned in ``time_zone``.

    A regular series repeats the autumn clock-change hour, which pandas resolves
    by inference. When the data makes that impossible, because the repeated hour
    appears only once or a spring hour that does not exist is present, the
    request still succeeds: ambiguous times are placed by row order and
    otherwise taken as the first (summer-time) occurrence, non-existent times
    move forward to the next valid time, and a warning names the request.
    """
    try:
        return index.tz_localize(time_zone, ambiguous="infer")
    except Exception as exc:  # noqa: BLE001 - pytz's or pandas' own class, by version
        failure = exc

    # Raises again for anything that is not a clock-change problem, such as an
    # unknown time zone.
    fold, _ = resolve_ambiguous_fold(index, time_zone)
    logger.warning(
        "Could not localize timestamps to %s: %s. The data does not follow the "
        "clock change, so ambiguous times were taken as the first (summer-time) "
        "occurrence and non-existent times moved forward. Check the series for a "
        "missing or extra hour around the change.%s",
        time_zone,
        failure,
        f" ({context})" if context else "",
    )
    return index.tz_localize(time_zone, ambiguous=fold, nonexistent="shift_forward")
