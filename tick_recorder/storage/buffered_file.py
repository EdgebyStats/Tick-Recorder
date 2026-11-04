"""BufferedDailyFile — append-only, daily-rolling, RAM-buffered file writer."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from tick_recorder.storage.constants import BUFFER_MAX_BYTES
from tick_recorder.utils.time_utils import utc_day, trading_day


class BufferedDailyFile:
    """Append-only file, rolls over daily, writes are buffered in RAM.

    Lifecycle:
        write(data, ns)   # appends to in-memory buffer (cheap, non-blocking)
        maybe_flush()     # called by the 1 Hz flusher — writes buffer to disk
        close()           # final flush + fsync + close

    Day-rollover triggers an immediate flush (yesterday's bytes don't belong
    in today's file).

    Thread safety: must only be called from the reactor thread. The owning
    Storage class enforces this via assertion.

    Day rollover can use either UTC (default) or a firm timezone. 
    - Raw frames: always UTC (firm-agnostic -- bytes are bytes)
    - Spots/events: firm timezone (so files match firm trading days)
    """

    def __init__(self, folder: str, suffix: str, binary: bool,
                 header: str | bytes | None = None,
                 firm_timezone: str | None = None):
        self.folder = folder
        self.suffix = suffix
        self.binary = binary
        self.header = header
        self.firm_timezone = firm_timezone  # None = UTC (raw frames)
        self.day: str | None = None
        self.fh = None
        self._buffer = bytearray()
        self._buffer_day: str | None = None

    def _compute_day(self, ns: int) -> str:
        """Return the day string for this timestamp, in UTC or firm time. """
        if self.firm_timezone is None:
            return utc_day(ns)
        return trading_day(ns, self.firm_timezone)

    # ---- write (cheap, never blocks on disk) -------------------------------
    def write(self, data: bytes, ns: int) -> None:
        day = self._compute_day(ns)
        # Day changed: flush yesterday's buffer before opening new file.
        if self._buffer_day is not None and day != self._buffer_day:
            self._flush_to_disk()
        if day != self.day:
            self._open_for_day(day)
        self._buffer.extend(data)
        # Safety valve: if buffer is huge, flush now.
        if len(self._buffer) >= BUFFER_MAX_BYTES:
            self._flush_to_disk()
        self._buffer_day = day

    def _open_for_day(self, day: str) -> None:
        # Close any existing handle first.
        if self.fh is not None:
            self._flush_to_disk()
            self.fh.close()
            self.fh = None
        os.makedirs(self.folder, exist_ok=True)
        path = os.path.join(self.folder, day + self.suffix)
        is_new = not os.path.exists(path) or os.path.getsize(path) == 0
        try:
            # ALWAYS open in binary mode -- buffer is bytearray (bytes)
            # Callers are responsible for encoding strings to bytes before write()
            self.fh = open(path, "ab")
            if is_new and self.header:
                # Header may be str (CSV) or bytes (binary) -- handle both
                self.fh.write(self.header.encode("utf-8"))
            # BUG FIX: only update self.day after successful open.
            self.day = day
        except OSError as e:
            # Leave self.day at old value, self.fh as None.
            # Next write will retry the open.
            import logging
            logging.getLogger("tick.storage").error(
                "failed to open %s: %s", path, e
            )

    # ---- flush (called by the 1 Hz LoopingCall) ----------------------------
    def maybe_flush(self) -> None:
        """Cheap to call frequently — does nothing if buffer is empty."""
        if self._buffer:
            self._flush_to_disk()

    def _flush_to_disk(self) -> None:
        if not self._buffer or self.fh is None:
            return
        self.fh.write(bytes(self._buffer))
        self.fh.flush()
        self._buffer.clear()

    # ---- close --------------------------------------------------------------
    def close(self) -> None:
        self._flush_to_disk()
        if self.fh is not None:
            self.fh.flush()
            os.fsync(self.fh.fileno())
            self.fh.close()
            self.fh = None
