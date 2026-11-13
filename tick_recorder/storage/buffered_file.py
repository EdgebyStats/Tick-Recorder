"""BufferedDailyFile — append-only, daily-rolling, RAM-buffered file writer."""

from __future__ import annotations

import os
import time
import logging

from tick_recorder.storage.constants import BUFFER_MAX_BYTES
from tick_recorder.utils.time_utils import utc_day_window, trading_day_window


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

    Per-tick cost: the day string used to be recomputed on every write()
    (datetime + strftime per tick, ~6.5M times/day). Now the day *window*
    (start/end ns + day string) is cached and write() only does two integer
    compares; the datetime work happens on rollover ticks only (~once/day,
    plus clock jumps). See _roll().

    NOTE: day classification is exact integer arithmetic. The
    cached window bounds are exact integer midnights (see time_utils
    *_day_window), and the hot path compares the raw int ns against them
    with `self._win_start <= ns < self._win_end`. Floating point / datetime
    math never runs on the per-tick path and never decides which file a
    tick lands in — only which filename string the day gets.
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
        self._flush_max: float = 0.0
        self.fh = None
        self._buffer = bytearray()
        self._buffer_day: str | None = None
        # Cached day window: _win_start <= ns < _win_end belong to self.day.
        # None = not yet computed (first write, or previous rollover failed).
        self._win_start: int | None = None      # Daily window start
        self._win_end: int | None = None        # Daily window end

    def _compute_window(self, ns: int) -> tuple[int, int, str]:
        """(start_ns, end_ns, day string) for this timestamp, UTC or firm time."""
        if self.firm_timezone is None:
            return utc_day_window(ns)
        return trading_day_window(ns, self.firm_timezone)

    def _compute_day(self, ns: int) -> str:
        """Return the day string for this timestamp (kept for compatibility)."""
        return self._compute_window(ns)[2]

    def _roll(self, ns: int) -> None:
        """Rollover path: recompute the day window for ns.

        Called only when ns falls outside the cached window (rollover, first
        write, or a clock jump) — never on the hot path. If the day changed,
        open its file; on open failure, leave the window uncached so the next
        write retries (same retry semantics as before).
        """
        start, end, day = self._compute_window(ns)
        if day != self.day:
            self._open_for_day(day)
            if self.day != day:
                return  # open failed; window stays uncached -> retry next write
        self._win_start, self._win_end = start, end

    # ---- write (cheap, never blocks on disk) -------------------------------
    def write(self, data: bytes, ns: int) -> None:
        # Hot path: two int compares. Full day recompute only on rollover.
        if self._win_start is None or not (self._win_start <= ns < self._win_end):
            self._roll(ns)
        day = self.day
        # Day changed: flush yesterday's buffer before opening new file.
        if self._buffer_day is not None and day != self._buffer_day:
            self._flush_to_page_cache()
        self._buffer.extend(data)
        # Safety valve: if buffer is huge, flush now.
        if len(self._buffer) >= BUFFER_MAX_BYTES:
            self._flush_to_page_cache()
        self._buffer_day = day

    def _open_for_day(self, day: str) -> None:
        # Close any existing handle first.
        if self.fh is not None:
            self._flush_to_page_cache()
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
            logging.getLogger("tick.storage").error(
                "failed to open %s: %s", path, e
            )

    # ---- flush (called by the 1 Hz LoopingCall) ----------------------------
    def maybe_flush(self) -> None:
        """Cheap to call frequently — does nothing if buffer is empty."""
        if self._buffer:
            self._flush_to_page_cache()

    def _flush_to_page_cache(self) -> None:
        """Hand buffer to the kernel page cache.
        Not durable: data sits as dirty pages until kernel writeback (30s approx)
        or close()'s fsync. Crash before that loses this buffer
        """
        if not self._buffer or self.fh is None:
            return
        n = len(self._buffer)
        t0w = time.perf_counter()
        t0c = time.process_time()
        self.fh.write(bytes(self._buffer))
        self.fh.flush()
        wall_ms = (time.perf_counter() - t0w) * 1e3
        cpu_ms = (time.process_time() - t0c) * 1e3
        self._buffer.clear()
        if wall_ms > 2.0:
            kind = "CPU/GC" if cpu_ms > wall_ms * 0.5 else "I/O wait"
            logging.getLogger("tick.storage").warning(
                "slow flush: %.2f ms wall (%.2f ms cpu) for %d bytes [%s]", wall_ms, cpu_ms, n, kind
            )

    # ---- close --------------------------------------------------------------
    def close(self) -> None:
        self._flush_to_page_cache()
        if self.fh is not None:
            self.fh.flush()
            os.fsync(self.fh.fileno())
            self.fh.close()
            self.fh = None