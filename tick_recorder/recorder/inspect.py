"""Inspect utility — decode and print raw .bin frames for debugging."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from ctrader_open_api import Protobuf
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoMessage
from google.protobuf.json_format import MessageToDict

from tick_recorder.storage.constants import RAW_HEADER


def inspect_raw(path: str, limit: int | None):
    """Print the frames in a raw .bin file, decoded, to check what was received.

    Args:
        path: Path to the raw .bin file.
        limit: Max frames to print, or None for all.
    """
    shown = 0
    with open(path, "rb") as fh:
        while limit is None or shown < limit:
            header = fh.read(RAW_HEADER.size)
            if len(header) < RAW_HEADER.size:
                break
            ns, length = RAW_HEADER.unpack(header)
            data = fh.read(length)
            if len(data) < length:
                print("(truncated final record)")
                break
            msg = ProtoMessage()
            msg.ParseFromString(data)
            try:
                body = MessageToDict(Protobuf.extract(msg))
            except Exception:
                body = {"payloadType": msg.payloadType, "undecoded_bytes": len(msg.payload)}
            ts = datetime.fromtimestamp(ns / 1e9, timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
            print(ts, json.dumps(body))
            shown += 1
