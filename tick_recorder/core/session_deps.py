"""SessionDeps — the narrow contract HostSession receives (loose coupling)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass
class SessionDeps:
    """Dependencies injected into HostSession.

    Was originally the whole Recorder; this dataclass decouples HostSession
    from Recorder so HostSession can be tested/instantiated in isolation.
    """

    storage: object              # Storage instance (avoid circular import)
    client_id: str
    client_secret: str
    all_accounts: list           # list[Account]
    save_tokens: Callable[[], None]
    base_path: str
