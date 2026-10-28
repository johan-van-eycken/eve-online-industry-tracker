"""Small value helpers shared across the daily planner."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    """Now in UTC, naive: the planner's DateTime columns store naive UTC."""
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


def attr(obj: Any, name: str) -> Any:
    """`name` of a dict row or an ORM/namespace object, None when absent."""
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)
