"""HostSession — main class + lifecycle + request/response."""

from __future__ import annotations

import time
import logging 

from twisted.internet import defer, task, reactor

from ctrader_open_api import Client, EndPoints, Protobuf
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import (
    ProtoErrorRes,
    ProtoHeartbeatEvent,
)

from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAErrorRes,
)

from tick_recorder.core.retry_policy import RetryPolicy, ApiError
from tick_recorder.storage.constants import SILENCE_ALERT_MIN
from tick_recorder.network.protocol import RecordingProtocol
from .handlers import MessageHandlerMixin
from .setup import AccountSetupMixin
from .auth import AuthMixin

logger = logging.getLogger("tick.network")


class HostSession(MessageHandlerMixin, AccountSetupMixin, AuthMixin):
    """One Open API connection (demo or live host) serving one or more accounts.

    Functionality is split across mixins for readability:
    - MessageHandlerMixin: incoming message dispatch + 5 handlers
    - AccountSetupMixin: account authorization + symbol subscription
    - AuthMixin: token refresh

    Lifecycle:
        start() -> on_connected -> setup_all -> setup_account (per account)
        stop()  -> returns Deferred from stopService() for clean shutdown
    """

    def __init__(self, deps, host_key: str, accounts: list):
        self.deps = deps
        self.storage = deps.storage
        self.host_key = host_key
        self.accounts = accounts
        self.generation = 0

        host = (EndPoints.PROTOBUF_LIVE_HOST if host_key == "live"
                else EndPoints.PROTOBUF_DEMO_HOST)
        self.client = Client(host, EndPoints.PROTOBUF_PORT, RecordingProtocol)
        self.client.storage = self.storage
        self.client.host_key = host_key
        self.client.last_recv_ns = time.time_ns()
        self.client.setConnectedCallback(self.on_connected)
        self.client.setDisconnectedCallback(self.on_disconnected)
        self.client.setMessageReceivedCallback(self.on_message)
        self.app_authed = False
        self.pending: set = set()
        self.retry_policy = RetryPolicy()

        # Build dispatch table (defined in MessageHandlerMixin).
        self._dispatch = self._build_dispatch()

        self.heartbeat_loop: task.LoopingCall | None = None 

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self):
        self.storage.event("starting", host=self.host_key,
                           accounts=[a.label for a in self.accounts])
        self.client.startService()

    def stop(self):
        """Stop the session cleanly. Returns a Deferred for caller to yield on. 

        Order: 
        1. Stop heartbeat loop (no more periodic sends)
        2. Stop the TCP client service (drains sockets)
        """
        # Stop heartbeat first -- no more periodic sends during teardown 
        if self.heartbeat_loop and self.heartbeat_loop.running:
            try:
                self.heartbeat_loop.stop()
            except Exception:
                pass # Already stopped -- not a problem 
        # Then stop the client service (returns Deferred)
        return self.client.stopService()

    def on_connected(self, _client):
        self.generation += 1
        self.app_authed = False
        self.storage.event("connected", host=self.host_key, generation=self.generation)

        # Start the 10-second heartbeat loop 
        try:
            self.heartbeat_loop = task.LoopingCall(self.send_heartbeat)
            self.heartbeat_loop.start(10.0, now = False)
        except Exception as e:
            self.storage.event("heartbeat_start_failed", error=str(e))

        self.setup_all(self.generation)  # from AccountSetupMixin

    def on_disconnected(self, _client, reason):
        self.generation += 1
        self.app_authed = False

        # Stop heartbeat loop safely 
        if self.heartbeat_loop and self.heartbeat_loop.running:
            try: 
                self.heartbeat_loop.stop()
            except Exception:
                pass # Loop already stopped -- not a problem

        for a in self.accounts:
            a.subscribed = False
        self.storage.event("disconnected", host=self.host_key,
                           reason=getattr(reason, "value", reason))
        for d in list(self.pending):
            if not d.called:
                d.errback(ConnectionError("connection lost"))
        self.pending.clear()

    def send_heartbeat(self):
        """Send keep-alive heartbeat to prevent connection timeout."""
        if not self.client.isConnected:
            return 
        try:
            d = self.client.send(ProtoHeartbeatEvent())
            d.addErrback(self._heartbeat_failed)
        except Exception as e:
            logger.error("[Heartbeat] sync failure: %s",e)

    def _heartbeat_failed(self, failure):
        """Errback for heartbeat sends.
        
        Heartbeats are events — server doesn't respond per heartbeat. The SDK's
        send() Deferred times out after 5s waiting for a response. This is EXPECTED
        behavior, not a real failure. The heartbeat was sent successfully.
        
        Only log genuine errors (connection issues, etc.), not timeouts.
        """
        from twisted.internet.defer import CancelledError, TimeoutError as DeferredTimeoutError
        
        # Silence expected timeouts (heartbeats are fire-and-forget events)
        if failure.check(CancelledError, DeferredTimeoutError):
            return
        
        # Real error — log it
        self.storage.event("heartbeat_failed", error=failure.getErrorMessage())
        logger.error("[Heartbeat] async failure: %s", failure.getErrorMessage())

    # ------------------------------------------------------------------
    # Request/response with retry
    # ------------------------------------------------------------------
    @defer.inlineCallbacks
    def call(self, request, retries: int | None = None):
        """Send a request, await its response, raise ApiError on server errors."""
        policy = self.retry_policy
        max_retries = policy.max_retries if retries is None else retries

        for attempt in range(max_retries + 1):
            response = yield self._send_one(request, policy.timeout_s)
            if not isinstance(response, (ProtoOAErrorRes, ProtoErrorRes)):
                return response

            retry_after = getattr(response, "retryAfter", 0)
            if response.errorCode in policy.retryable_codes and attempt < max_retries:
                delay = max(retry_after, policy.min_retry_delay_s)
                yield task.deferLater(reactor, delay, lambda: None)
                continue
            raise ApiError(response.errorCode,
                           getattr(response, "description", ""),
                           retry_after)

    @defer.inlineCallbacks
    def _send_one(self, request, timeout_s: float):
        """Send a single request, track its deferred, return Protobuf response."""
        d = self.client.send(request, responseTimeoutInSeconds=timeout_s)
        self.pending.add(d)
        d.addBoth(self._forget, d)
        message = yield d
        return Protobuf.extract(message)

    def _forget(self, result, d):
        self.pending.discard(d)
        return result

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    def account_by_ctid(self, ctid):
        return next((a for a in self.accounts if a.ctid == ctid), None)

    def check_silence(self):
        """Flag accounts that have gone silent — called by 60s watchdog."""
        now = time.time_ns()
        for a in self.accounts:
            if (a.subscribed and not a.silent_logged and a.last_spot_ns
                    and now - a.last_spot_ns > SILENCE_ALERT_MIN * 60e9):
                a.silent_logged = True
                self.storage.event("no_spots", label=a.label,
                                   minutes=SILENCE_ALERT_MIN)
