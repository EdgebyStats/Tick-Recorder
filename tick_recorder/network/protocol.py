"""RecordingProtocol — TcpProtocol that records every frame before parsing."""

from __future__ import annotations

import time
from collections import deque

from ctrader_open_api import TcpProtocol


class RecordingProtocol(TcpProtocol):
    """TcpProtocol that saves every frame byte-for-byte before parsing it.

    Each connection gets its own send queue — the SDK's TcpProtocol keeps the
    queue on the class, so it would be shared by every connection.
    """

    def connectionMade(self):
        self._send_queue = deque()
        self._send_task = None
        super().connectionMade()

    def stringReceived(self, data):
        # CRITICAL HOT PATH — keep this method tiny.
        ns = time.time_ns()
        client = self.factory.client
        client.last_recv_ns = ns
        client.storage.raw_frame(client.host_key, data, ns)
        return super().stringReceived(data)
