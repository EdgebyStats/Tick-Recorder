"""Account dataclass — one cTrader account to be recorded/traded."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Account:
    """One cTrader account.

    Splits into two functional groups:
    - Config (set at construction): connection identity
    - Runtime state (mutated by HostSession during connect/subscribe)
    """

    # --- Config (set once at construction) ---
    label: str
    login: int
    access_token: str
    symbol_names: list
    host: str = "demo"
    refresh_token: str = ""
    firm_timezone: str = "Europe/Prague"

    # --- Runtime state (mutated by HostSession during connect/subscribe) ---
    ctid: int | None = None
    symbols: dict = field(default_factory=dict)  # symbolId -> name
    subscribed: bool = False
    last_spot_ns: int | None = None
    silent_logged: bool = False
    setup_running: int | bool = False

    @classmethod
    def from_config(cls, cfg) -> "Account":
        """Build an Account from a config dict OR Pydantic model.

        Accepts:
        - plain dict (legacy)
        - AccountConfig pydantic model (we'll add in Step 10)
        """
        if hasattr(cfg, "model_dump"):
            cfg = cfg.model_dump()
        return cls(
            label=cfg["label"],
            login=int(cfg["login"]),
            host=cfg.get("host", "demo"),
            access_token=cfg["access_token"],
            refresh_token=cfg.get("refresh_token", ""),
            symbol_names=cfg["symbols"],
            firm_timezone=cfg.get("firm_timezone", "Europe/Prague"),
        )

    def __repr__(self) -> str:
        return (f"Account(label={self.label!r}, login={self.login}, "
                f"host={self.host!r}, subscribed={self.subscribed})")
