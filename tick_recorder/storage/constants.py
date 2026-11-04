"""Constants used across storage and network layers."""

from __future__ import annotations

import struct

# ============================================================================
# Timeouts and retry delays
# ============================================================================
REQUEST_TIMEOUT = 30       # Seconds to wait for an API response.
SETUP_RETRY_DELAY = 30     # Seconds before retrying a failed account setup.
SILENCE_ALERT_MIN = 5      # Log when an account sends no spots for this long.

# ============================================================================
# BufferedDailyFile — flush behavior
# ============================================================================
BUFFER_FLUSH_INTERVAL_S = 1.0              # 1 Hz flusher
BUFFER_MAX_BYTES = 1 << 20                  # 1 MiB per file before forced flush

# ============================================================================
# On-disk format constants
# ============================================================================
RAW_HEADER = struct.Struct("<qI")          # int64 recv_ns, uint32 len
CSV_HEADER = "recv_ns, server_ts_ms, symbol_id, symbol, bid, ask, session_close\n"
