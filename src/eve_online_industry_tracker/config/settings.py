from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def _bool(name: str, default: bool) -> bool:
    raw = _env(name, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _int(name: str, default: int) -> int:
    raw = _env(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def public_structures_cache_ttl_seconds() -> int:
    return _int("FLASK_PUBLIC_STRUCTURES_TTL", default=3600)


def public_structures_startup_scan_enabled() -> bool:
    return _bool("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN", default=True)


def public_structures_startup_scan_max_workers() -> int:
    return _int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_WORKERS", default=10)


def public_structures_startup_scan_scan_cap() -> int:
    return _int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_CAP", default=5000)


def public_structures_startup_scan_time_budget_seconds() -> int:
    return _int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_TIME_BUDGET", default=60)


def public_structures_startup_scan_batch_size() -> int:
    return _int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_BATCH_SIZE", default=100)


def public_structures_startup_scan_pause_seconds() -> int:
    return _int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_PAUSE", default=5)
