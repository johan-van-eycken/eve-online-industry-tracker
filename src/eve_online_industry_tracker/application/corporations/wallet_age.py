"""How long ago the corp wallet was last read successfully."""
from __future__ import annotations

from datetime import datetime, timezone


def wallet_age_hours(refreshed_at: datetime | str | None, now: datetime) -> float | None:
    """Hours between refreshed_at and now; None when unknown or unparseable. Naive times are UTC."""
    if refreshed_at is None:
        return None
    try:
        when = datetime.fromisoformat(refreshed_at) if isinstance(refreshed_at, str) else refreshed_at
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return (now - when).total_seconds() / 3600
    except (ValueError, TypeError):
        return None


def wallet_stale_reason(refreshed_at: datetime | str | None, now: datetime, max_age_hours: float) -> str | None:
    """None when the wallet read is fresh enough, else why it counts as unknown."""
    if refreshed_at is None:
        return "corp wallet never read successfully"
    age = wallet_age_hours(refreshed_at, now)
    if age is None:
        return "corp wallet never read successfully"
    if age > max_age_hours:
        return f"corp wallet last read {age:.1f} h ago (limit {max_age_hours:g} h)"
    return None
