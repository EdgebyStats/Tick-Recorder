"""Incoming message handlers — MessageHandlerMixin."""

from __future__ import annotations

from ctrader_open_api import Protobuf
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOASpotEvent,
    ProtoOAAccountDisconnectEvent,
    ProtoOAAccountsTokenInvalidatedEvent,
    ProtoOAClientDisconnectEvent,
    ProtoOAErrorRes,
)
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoHeartbeatEvent
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import (
    ProtoErrorRes,
)


class MessageHandlerMixin:
    """Handles incoming Open API messages.

    Mixed into HostSession. Provides:
    - _build_dispatch(): payloadType -> handler mapping
    - on_message(): reactor callback, routes messages to handlers
    - _on_spot, _on_account_disconnect, etc.: per-event-type handlers
    """

    def _build_dispatch(self) -> dict:
        """payloadType -> handler(message, ns)."""
        return {
            ProtoOASpotEvent().payloadType:                     self._on_spot,
            ProtoOAAccountDisconnectEvent().payloadType:        self._on_account_disconnect,
            ProtoOAAccountsTokenInvalidatedEvent().payloadType: self._on_token_invalidated,
            ProtoOAClientDisconnectEvent().payloadType:         self._on_client_disconnect,
            ProtoOAErrorRes().payloadType:                      self._on_server_error,
            ProtoErrorRes().payloadType:                        self._on_server_error,
        }

    def on_message(self, _client, message):
        """Route message to its handler."""
        # Filter out incoming server heartbeats (PayloadType 51)
        if message.payloadType == ProtoHeartbeatEvent().payloadType:
            return 
            
        ns = self.client.last_recv_ns
        handler = self._dispatch.get(message.payloadType)
        if handler is None:
            return  # unknown payload type — silent skip
        # Unsolicited server errors get logged; response-to-our-request errors
        # are handled by the request's own deferred.
        if message.payloadType in (ProtoOAErrorRes().payloadType,
                                   ProtoErrorRes().payloadType) and message.clientMsgId:
            return
        handler(message, ns)

    # ---- per-event-type handlers ---------------------------------------
    def _on_spot(self, message, ns):
        ev = Protobuf.extract(message)
        a = self.account_by_ctid(ev.ctidTraderAccountId)
        if a is None:
            return
        if a.silent_logged:
            self.storage.event("spots_resumed", label=a.label,
                               silent_s=round((ns - a.last_spot_ns) / 1e9, 1))
            a.silent_logged = False
        a.last_spot_ns = ns
        self.storage.spot_row(a.label, self._format_spot_row(ev, a, ns), ns)

    @staticmethod
    def _format_spot_row(ev, a, ns) -> str:
        """Pure function — easy to unit test without a live connection."""
        return "{},{},{},{},{},{},{}\n".format(
            ns,
            ev.timestamp if ev.HasField("timestamp") else "",
            ev.symbolId,
            a.symbols.get(ev.symbolId, ""),
            ev.bid if ev.HasField("bid") else "",
            ev.ask if ev.HasField("ask") else "",
            ev.sessionClose if ev.HasField("sessionClose") else "",
        )

    def _on_account_disconnect(self, message, ns):
        ev = Protobuf.extract(message)
        a = self.account_by_ctid(ev.ctidTraderAccountId)
        if not a:
            return
        a.subscribed = False
        self.storage.event("account_disconnected_by_server", label=a.label)
        self.schedule_setup(a, 5)  # from AccountSetupMixin

    def _on_token_invalidated(self, message, ns):
        ev = Protobuf.extract(message)
        self.storage.event("token_invalidated", host=self.host_key,
                           reason=ev.reason, ctids=list(ev.ctidTraderAccountIds))
        for ctid in ev.ctidTraderAccountIds:
            a = self.account_by_ctid(ctid)
            if a:
                a.subscribed = False
                self.schedule_setup(a, 1)  # from AccountSetupMixin

    def _on_client_disconnect(self, message, ns):
        ev = Protobuf.extract(message)
        self.storage.event("client_disconnected_by_server",
                           host=self.host_key, reason=ev.reason)

    def _on_server_error(self, message, ns):
        ev = Protobuf.extract(message)
        self.storage.event("server_error", host=self.host_key,
                           code=ev.errorCode,
                           description=getattr(ev, "description", ""))
