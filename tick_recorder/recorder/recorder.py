"""Recorder — top-level orchestrator for the tick recorder."""

from __future__ import annotations
import os

from twisted.internet import defer, reactor, task

from tick_recorder.core.account import Account
from tick_recorder.core.session_deps import SessionDeps
from tick_recorder.network.host_session import HostSession
from tick_recorder.storage.constants import BUFFER_FLUSH_INTERVAL_S
from tick_recorder.storage.storage import Storage

from tick_recorder.config.schema import RecorderConfig 
from tick_recorder.config.loader import load_config, save_config, ConfigError


class Recorder:
    """Top-level orchestrator.

    Owns:
    - Storage (single I/O gateway)
    - One HostSession per host (demo / live)
    - Two LoopingCalls: 1Hz flusher + 60s silence watchdog
    - Shutdown hook for clean teardown
    """

    def __init__(self, config_path: str, base_path: str):
        self.config_path = os.path.join(base_path, config_path)
        self.base_path = base_path
        
        # Load + Validate config via Pydantic 
        try: 
            self.config: RecorderConfig = load_config(self.config_path)
        except ConfigError as e:
            raise SystemExit(str(e))

        self.client_id = self.config.client_id
        self.client_secret = self.config.client_secret

        self.all_accounts = [Account.from_config(c) for c in self.config.accounts]
        # (Label uniqueness is already enforce by Pydantic, but keep the check)
        # as defense-in-depth in case someone bypasses the schema.)
        labels = [a.label for a in self.all_accounts]
        if len(labels) != len(set(labels)):
            raise SystemExit("Each account needs a unique label in the config.")

        self.storage = Storage(self.config.data_dir, base_path, self.all_accounts)

        deps = SessionDeps(
            storage=self.storage,
            client_id=self.client_id,
            client_secret=self.client_secret,
            all_accounts=self.all_accounts,
            save_tokens=self.save_tokens,
            base_path=self.base_path,
        )

        self.sessions: list[HostSession] = []
        for host_key in ("demo", "live"):
            accs = [a for a in self.all_accounts if a.host == host_key]
            if accs:
                self.sessions.append(HostSession(deps, host_key, accs))

        # Populated by start().
        self.flusher: task.LoopingCall | None = None
        self.watchdog: task.LoopingCall | None = None

    def save_tokens(self):
        """Write refreshed tokens back into the config file (atomic, with .bak).

        Syncs runtime Account state back into the Pydantic config model,
        then persists via save_config().
        """
        by_label = {a.label: a for a in self.all_accounts}
        for acc_cfg in self.config.accounts:
            a = by_label[acc_cfg.label]
            acc_cfg.access_token = a.access_token
            acc_cfg.refresh_token = a.refresh_token
        save_config(self.config_path, self.config)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self):
        """Orchestrator only. Real work is in the helpers below."""
        self._start_flusher()
        self._start_watchdog()
        self._register_shutdown_hook()
        for s in self.sessions:
            s.start()

    def _start_flusher(self):
        """1 Hz loop that drains all DailyFile buffers to disk."""
        self.flusher = task.LoopingCall(self.storage.flush)
        self.flusher.start(BUFFER_FLUSH_INTERVAL_S, now=False)

    def _start_watchdog(self):
        """60s loop that flags accounts which have gone silent."""
        self.watchdog = task.LoopingCall(
            lambda: [s.check_silence() for s in self.sessions]
        )
        self.watchdog.start(60.0, now=False)

    def _register_shutdown_hook(self):
        """Make sure buffers are flushed and sockets closed on exit.

        Uses defer.ensureDeferred so the async shutdown runs cleanly inside
        the reactor's 'before shutdown' trigger.
        """
        reactor.addSystemEventTrigger(
            "before", "shutdown",
            lambda: defer.ensureDeferred(self.shutdown())
        )

    @defer.inlineCallbacks
    def shutdown(self):
        """Async shutdown — fixes the original race condition.

        Original code called s.stop() without yielding, then storage.close()
        ran while sockets were still tearing down. Now we:
        1. Stop each session and yield on its Deferred.
        2. Stop the LoopingCalls.
        3. Close storage.
        4. Log completion.
        """
        self.storage.event("stopping")
        # 1. Stop sessions — yield on each Deferred.
        for s in self.sessions:
            try:
                yield s.stop()
            except Exception as e:
                self.storage.event("session_stop_failed", error=str(e))
        # 2. Stop LoopingCalls.
        if self.flusher and self.flusher.running:
            self.flusher.stop()
        if self.watchdog and self.watchdog.running:
            self.watchdog.stop()
        # 3. Close storage — buffers flush + files close.
        self.storage.close()
        self.storage.event("shutdown_complete")

    # NOTE: inspect_raw is intentionally NOT on this class — it lives in
    # tick_recorder.recorder.inspect as a standalone function. Call via CLI:
    #   python -m tick_recorder.main inspect <file> --limit 100
