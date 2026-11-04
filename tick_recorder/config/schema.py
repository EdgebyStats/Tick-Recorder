"""Pydantic schemas for the recorder/bot configuration."""

from __future__ import annotations

from typing import Literal
from zoneinfo import ZoneInfo 

from pydantic import BaseModel, Field, field_validator, model_validator


class AccountConfig(BaseModel):
    """One cTrader account to be recorded/traded."""

    label: str = Field(..., description="Unique human-readable identifier")
    login: int = Field(..., description="cTrader login (traderLogin)")
    access_token: str = Field(..., min_length=10)
    refresh_token: str = Field(default="")
    host: Literal["demo", "live"] = Field(default="demo")
    symbols: list[str] = Field(..., min_length=1)

    @field_validator("label")
    @classmethod
    def label_must_be_simple(cls, v: str) -> str:
        """Labels are used in file paths — no slashes or spaces."""
        if "/" in v or "\\" in v or " " in v:
            raise ValueError("label must not contain slashes or spaces")
        return v

    @field_validator("host", mode="before")
    @classmethod
    def normalize_host(cls, v):
        """Accept 'Demo', 'DEMO', 'live', 'LIVE' — normalize to lowercase."""
        if isinstance(v, str):
            v_lower = v.lower().strip()
            if v_lower in ("demo", "live"):
                return v_lower
        return v

    @model_validator(mode="before")
    @classmethod
    def strip_underscored_fields(cls, data):
        """Allow _comment and similar documentation fields by stripping them.

        This lets users add inline documentation like:
            "_comment": "host must be exactly 'demo' or 'live'"
        without breaking extra='forbid' validation.
        """
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if not k.startswith("_")}
        return data

    model_config = {
        "extra": "forbid",  # Reject unknown fields — catches typos
        "str_strip_whitespace": True,
    }

    firm_timezone: str = Field(
        default = "Europe/Prague",
        description="IANA timezone name for firm's trading day boundary."
                    "Used for file rollover. Example: 'Europe/Prage' for FTMO, "
                    "'Asia/Jerusalem' for the 5%ers"
    )

    @field_validator("firm_timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """verify it's a valid IANA timezone."""
        try:
            ZoneInfo(v)
        except Exception:
            raise ValueError(f"Invalid IANA timezone: {v!r}"
                             f"(use 'Europe/Prague', 'Asia/Jerusalem', etc.)")
        return v


class RecorderConfig(BaseModel):
    """Top-level configuration for the recorder."""

    client_id: str = Field(..., min_length=10)
    client_secret: str = Field(..., min_length=10)
    accounts: list[AccountConfig] = Field(..., min_length=1)
    data_dir: str = Field(default="ticks")

    @field_validator("accounts")
    @classmethod
    def labels_must_be_unique(cls, accounts: list[AccountConfig]) -> list[AccountConfig]:
        labels = [a.label for a in accounts]
        if len(labels) != len(set(labels)):
            duplicates = sorted({l for l in labels if labels.count(l) > 1})
            raise ValueError(f"duplicate account labels: {duplicates}")
        return accounts

    @model_validator(mode="before")
    @classmethod
    def strip_underscored_fields(cls, data):
        """Allow top-level _comment fields too."""
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if not k.startswith("_")}
        return data

    model_config = {
        "extra": "forbid",
        "str_strip_whitespace": True,
    }
