"""Time utilities — UTC helpers used across the codebase."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo 

# Cache ZoneInfo instances -- constructing them repeatedly is slow. 
_TZ_CACHE: dict[str, ZoneInfo] = {}  

def _get_tz(name: str) -> ZoneInfo:
    if name not in _TZ_CACHE:
        _TZ_CACHE[name] = ZoneInfo(name)
    return _TZ_CACHE[name]


def utc_now_iso() -> str:
    """Current UTC time as ISO 8601 string, e.g. 2026-10-29T14:30:00.123456Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def utc_day(ns: int) -> str:
    """Nanosecond epoch timestamp -> UTC YYYY-MM-DD string. -> Used for raw frames (always UTC)."""
    return datetime.fromtimestamp(ns / 1e9, timezone.utc).strftime("%Y-%m-%d")

def trading_day(ns: int, firm_timezone: str) -> str: 
    """Firm-local YYYY-MM-DD string. Used for spots/events file rollover. 

    The day boundary is midnight in firm_timezone. DST is handled automatically by zoneinfo. 

    Example: ns corresponds to 2026-07-15 21:30 UTC. 
    - firm_timezone= 'Europe/Prague' (CEST, UTC+2 in summer)
    - Firm-local time: 2026-07-15 23:30 (still July 15 in Prague)
    - Returns: '2026-07-15'

    Same ns, but 22:30 UTC:
    - Firm-local time: 2026-07-16 00:30 (now July 16 in Prague)
    - Returns: '2026-07-16' 
    """

    tz = _get_tz(firm_timezone)
    dt = datetime.fromtimestamp(ns / 1e9, tz)
    return dt.strftime("%Y-%m-%d")

    
