from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def _int(name: str, default: int) -> int:
    raw = _env(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def public_structures_cache_ttl_seconds() -> int:
    """How long cached public structures in db_app are considered fresh.

    Keeps using the existing env var name for backwards compatibility.
    """

    return _int("FLASK_PUBLIC_STRUCTURES_TTL", default=24 * 3600)
