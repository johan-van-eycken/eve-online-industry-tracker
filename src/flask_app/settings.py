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


def flask_host() -> str:
    return os.getenv("FLASK_HOST", "localhost")


def flask_port() -> int:
    return int(os.getenv("FLASK_PORT", "5000"))


def flask_debug() -> bool:
    return _bool("FLASK_DEBUG", default=False)


def refresh_metadata_on_startup() -> bool:
    # Keeping existing behavior (True) unless explicitly disabled.
    return _bool("FLASK_REFRESH_METADATA", default=True)


def health_poll_timeout_seconds() -> int:
    return _int("FLASK_HEALTH_POLL_TIMEOUT", default=120)


def health_request_timeout_seconds() -> int:
    return _int("FLASK_HEALTH_REQUEST_TIMEOUT", default=2)


def api_request_timeout_seconds() -> int:
    return _int("FLASK_API_REQUEST_TIMEOUT", default=10)


def public_structures_cache_ttl_seconds() -> int:
    # How long cached public structures in db_app are considered fresh.
    return _int("FLASK_PUBLIC_STRUCTURES_TTL", default=3600)


def public_structures_startup_scan_enabled() -> bool:
    # Run a background global scan of /universe/structures on startup.
    # This can be heavy; defaults to enabled but bounded by conservative limits.
    return _bool("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN", default=True)


def public_structures_startup_scan_max_workers() -> int:
    return _int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_WORKERS", default=10)


def public_structures_startup_scan_scan_cap() -> int:
    # Max structure IDs to attempt per "slice".
    return _int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_CAP", default=5000)


def public_structures_startup_scan_time_budget_seconds() -> int:
    # Max seconds per slice; the scanner yields between slices.
    return _int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_TIME_BUDGET", default=60)


def public_structures_startup_scan_batch_size() -> int:
    return _int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_BATCH_SIZE", default=100)


def public_structures_startup_scan_pause_seconds() -> int:
    # Sleep between slices to be kind to ESI.
    return _int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_PAUSE", default=5)


def api_base() -> str:
    return f"http://{flask_host()}:{flask_port()}"
