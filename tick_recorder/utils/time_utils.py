"""Time utilities — UTC helpers used across the codebase."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# Cache ZoneInfo instances -- constructing them repeatedly is slow.
_TZ_CACHE: dict[str, ZoneInfo] = {}

_NS_PER_DAY = 86_400 * 1_000_000_000  # ns in 24h; UTC days are always 86400s


def _get_tz(name: str) -> ZoneInfo:
    if name not in _TZ_CACHE:
        _TZ_CACHE[name] = ZoneInfo(name)
    return _TZ_CACHE[name]


def utc_now_iso() -> str:
    """Current UTC time as ISO 8601 string, e.g. 2026-10-29T14:30:00.123456Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# def utc_day(ns: int) -> str:
#     """Nanosecond epoch timestamp -> UTC YYYY-MM-DD string. -> Used for raw frames (always UTC)."""
#     return datetime.fromtimestamp(ns // 1e9, timezone.utc).strftime("%Y-%m-%d")


# def trading_day(ns: int, firm_timezone: str) -> str:
#     """Firm-local YYYY-MM-DD string. Used for spots/events file rollover.

#     The day boundary is midnight in firm_timezone. DST is handled automatically by zoneinfo.

#     Example: ns corresponds to 2026-07-15 21:30 UTC.
#     - firm_timezone= 'Europe/Prague' (CEST, UTC+2 in summer)
#     - Firm-local time: 2026-07-15 23:30 (still July 15 in Prague)
#     - Returns: '2026-07-15'

#     Same ns, but 22:30 UTC:
#     - Firm-local time: 2026-07-16 00:30 (now July 16 in Prague)
#     - Returns: '2026-07-16'
#     """

#     tz = _get_tz(firm_timezone)
#     dt = datetime.fromtimestamp(ns / 1e9, tz)
#     return dt.strftime("%Y-%m-%d")


# --------------------------------------------------------------------------
# Day-window variants: return the ns bounds of the day along with its string.
# These exist so hot paths (per-tick writers) can cache the window and only
# recompute on rollover instead of doing datetime work on every tick.
# --------------------------------------------------------------------------

def utc_day_window(ns: int) -> tuple[int, int, str]:
    """(start_ns, end_ns, 'YYYY-MM-DD') of the UTC day containing ns.

    start_ns <= ns < end_ns, end_ns - start_ns == 24h exactly (UTC days have
    no DST). Pure integer math except the one-time string format.

    NOTE: start/end are exact integers (pure integer math), so
    the rollover check `_win_start <= ns < _win_end` in BufferedDailyFile is
    an exact integer comparison — float precision never participates in day
    classification. Floating point appears only in the day-string format
    below, off the classification path entirely.
    """
    start = ns - (ns % _NS_PER_DAY)
    day = datetime.fromtimestamp(start / 1e9, timezone.utc).strftime("%Y-%m-%d")
    return start, start + _NS_PER_DAY, day


def trading_day_window(ns: int, firm_timezone: str) -> tuple[int, int, str]:
    """(start_ns, end_ns, firm-local 'YYYY-MM-DD') of the firm-local day containing ns.

    start_ns <= ns < end_ns, where both bounds are midnights in firm_timezone.
    Window length is 23/24/25h across DST transitions. Bounds are computed via
    int(seconds) * 1e9 so they are exact integers (no float-ns rounding).

    NOTE: both bounds are exact integers — midnights computed as
    int(seconds) * 1_000_000_000, never float ns — so the rollover check
    `_win_start <= ns < _win_end` in BufferedDailyFile is exact integer
    comparison of raw int ns against integer midnights. A tick can never be
    misclassified across a boundary by rounding. The day STRING is derived
    via float (datetime.fromtimestamp(ns / 1e9, tz)) and is therefore only
    ~microsecond-exact, which is immaterial: the string names the file, the
    bounds decide the day. Time-source jitter (e.g. NTP, time.time_ns()
    error) affects how well ns reflects reality but cannot change which
    window the recorded ns falls in.
    """
    tz = _get_tz(firm_timezone)
    dt = datetime.fromtimestamp(ns / 1e9, tz)
    day = dt.strftime("%Y-%m-%d")
    start = int(dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()) * 1_000_000_000
    nxt = (dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = int(nxt.timestamp()) * 1_000_000_000
    return start, end, day