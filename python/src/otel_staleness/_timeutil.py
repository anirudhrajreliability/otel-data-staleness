"""Shared time helpers (single source of truth, previously duplicated per probe)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def to_epoch(value) -> Optional[float]:
    """Normalize None / int / float / datetime to Unix seconds.

    Naive datetimes are treated as UTC. Raises TypeError for other types so a
    misconfigured source fails loudly rather than fabricating a value.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    raise TypeError(f"cannot convert {type(value)!r} to epoch seconds")


def parse_iso_epoch(value: Optional[str]) -> Optional[float]:
    """Parse an ISO-8601 timestamp string to Unix seconds; None if unparseable."""
    if not value:
        return None
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()
