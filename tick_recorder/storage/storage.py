"""Storage — single I/O gateway. No other class touches kernel page cache directly."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from tick_recorder.storage.buffered_file import BufferedDailyFile
from tick_recorder.storage.constants import CSV_HEADER, RAW_HEADER
from tick_recorder.utils.time_utils import utc_now_iso

logger = logging.getLogger("tick.storage")
logger.setLevel(logging.INFO)

# ============================================================================
# Critical events — written synchronously so they survive hard crashes.
# Add to this set as you build the trading layer.
# ============================================================================
CRITICAL_EVENTS: frozenset = frozenset({
    "server_error",
    "account_setup_failed",
    "app_auth_failed",
    "token_invalidated",
    "no_spots",
    "disconnected",
    "heartbeat_start_failed",
    "heartbeat_failed",
    # Trading layer (add when built):
    # "order_rejected",
    # "position_closed_unexpectedly",
    # "daily_dd_warning",
    # "daily_dd_breached",
})


class Storage:
    """Single I/O gateway. No other class touches kernel page cache directly."""

    def __init__(self, data_dir: str, base_path: str, accounts: list):
        self.data_dir = data_dir
        self.full_dir_path = os.path.join(base_path, data_dir)
        os.makedirs(self.full_dir_path, exist_ok=True)

        self.raw: dict[str, BufferedDailyFile] = {}
        self.spots: dict[str, BufferedDailyFile] = {}
        self.closed = False

        # Critical events — synchronous write, immediately durable.
        # Separate file so we can scan them quickly without grepping all events.
        critical_path = os.path.join(self.full_dir_path, "critical_events.jsonl")
        self.critical_fh = open(critical_path, "a", buffering=1)  # line-buffered

        # Capture reactor thread ID for safety assertions.
        self._reactor_thread = threading.get_ident()

        self.accounts_by_label = {a.label: a for a in accounts}

        # Events file uses firm timezone of the FIRST account (since events
        # span all accounts). If accounts have mized timezones, you'd need 
        # per-account event files -- but in practice all your firms use CE(S)T.
        #primary_tz = accounts[0].firm_timezone if accounts else None 
        # Routine events — batched in RAM, flushed once per second.
        self.events = BufferedDailyFile(
            folder=os.path.join(self.data_dir, "events"),
            suffix=".jsonl",
            binary=False,
            header=None,
        )

        # critical_events.jsonl is a single file (not daily-routed currently)
        # But we could apply firm_timezone here too if need to make it daily
        


    # ------------------------------------------------------------------
    # Thread safety
    # ------------------------------------------------------------------
    def _assert_reactor_thread(self) -> None:
        """Catch bugs where Storage is called from a worker thread."""
        if threading.get_ident() != self._reactor_thread:
            raise RuntimeError(
                f"Storage accessed from non-reactor thread "
                f"(reactor={self._reactor_thread}, caller={threading.get_ident()}). "
                f"Use deferToThread carefully — Storage is not thread-safe."
            )

    # ------------------------------------------------------------------
    # Raw frames (byte-for-byte server frames)
    # ------------------------------------------------------------------
    def raw_frame(self, host: str, data: bytes, ns: int) -> None:
        self._assert_reactor_thread()
        if self.closed:
            return
        f = self.raw.get(host)
        if f is None:
            f = self.raw[host] = BufferedDailyFile(
                os.path.join(self.data_dir, "raw", host), ".bin", True
            )
        f.write(RAW_HEADER.pack(ns, len(data)) + data, ns)

    # ------------------------------------------------------------------
    # Spot rows (one per ProtoOASpotEvent)
    # ------------------------------------------------------------------
    def spot_row(self, label: str, row: str, ns: int) -> None:
        self._assert_reactor_thread()
        if self.closed:
            return
        f = self.spots.get(label)
        if f is None:
            # Use the firm timezone from this account's config 
            account = self.accounts_by_label.get(label)
            tz = account.firm_timezone if account else None 
            f = self.spots[label] = BufferedDailyFile(
                os.path.join(self.data_dir, "spots", label), ".csv", False, CSV_HEADER,
                firm_timezone = tz,
            )
        f.write(row.encode("utf-8"), ns)
    # raw_frame stays UTC (bytes are bytes -- no timezone semantics)

    # ------------------------------------------------------------------
    # Meta snapshots (one JSON file per (re)connect per account)
    # ------------------------------------------------------------------
    def meta(self, label: str, payload: dict) -> None:
        self._assert_reactor_thread()
        if self.closed:
            return
        folder = os.path.join(self.data_dir, "meta", label)
        os.makedirs(folder, exist_ok=True)
        name = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ") + "_session.json"
        with open(os.path.join(folder, name), "w") as fh:
            json.dump(payload, fh, indent=2)

    # ------------------------------------------------------------------
    # Events — split into routine (batched) and critical (sync)
    # ------------------------------------------------------------------
    def event(self, kind: str, **fields) -> None:
        """Log a lifecycle event.

        Routine events are batched in RAM and flushed once per second.
        Critical events (errors, disconnects) are written synchronously to
        a separate file so they survive even a hard crash.

        Always also routes to the logger for console/journald visibility.
        """
        self._assert_reactor_thread()

        rec = {"utc": utc_now_iso(), "event": kind, **fields}
        row = (json.dumps(rec, default=str) + "\n").encode("utf-8")

        if kind in CRITICAL_EVENTS:
            # Synchronous write — these are rare and MUST persist immediately.
            if not self.closed:
                self.critical_fh.write(row.decode("utf-8"))
                self.critical_fh.flush()
                # Also write to batched events file for unified chronological view.
                self.events.write(row, time.time_ns())
        else:
            # Routine event — batch it.
            if not self.closed:
                self.events.write(row, time.time_ns())

        # Always log to logger.
        kv = " ".join(f"{k}={v}" for k, v in fields.items())
        logger.info("%s %s", kind, kv)

    # ------------------------------------------------------------------
    # Flush + close
    # ------------------------------------------------------------------
    def flush(self) -> None:
        """Called by Recorder's 1 Hz LoopingCall — drains all buffers to kernel page cache."""
        self._assert_reactor_thread()
        if self.closed:
            return
        for f in list(self.raw.values()) + list(self.spots.values()) + [self.events]:
            f.maybe_flush()
        # critical_fh is line-buffered — no explicit flush needed.

    def close(self) -> None:
        self._assert_reactor_thread()
        if self.closed:
            return
        for f in list(self.raw.values()) + list(self.spots.values()):
            f.close()
        self.events.close()
        self.critical_fh.close()
        self.closed = True
