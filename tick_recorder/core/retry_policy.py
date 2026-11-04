"""Retry policy and API error class."""

from __future__ import annotations

from dataclasses import dataclass

from tick_recorder.storage.constants import REQUEST_TIMEOUT


@dataclass
class RetryPolicy:
    """Retry configuration for cTrader API requests."""

    max_retries: int = 5
    timeout_s: float = REQUEST_TIMEOUT
    retryable_codes: frozenset = frozenset({"BLOCKED_PAYLOAD_TYPE"})
    min_retry_delay_s: float = 1.0


class ApiError(Exception):
    """Raised when the cTrader server returns an error response."""

    def __init__(self, code: str, description: str = "", retry_after: float = 0):
        super().__init__(f"{code}: {description}")
        self.code = code
        self.retry_after = retry_after
        self.description = description
