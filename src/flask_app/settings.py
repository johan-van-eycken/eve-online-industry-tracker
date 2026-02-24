from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
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


@dataclass(frozen=True)
class FlaskSettings:
    flask_host: str
    flask_port: int
    flask_debug: bool
    refresh_metadata_on_startup: bool
    health_poll_timeout_seconds: int
    health_request_timeout_seconds: int
    api_request_timeout_seconds: int
    public_structures_cache_ttl_seconds: int
    public_structures_startup_scan_enabled: bool
    public_structures_startup_scan_max_workers: int
    public_structures_startup_scan_scan_cap: int
    public_structures_startup_scan_time_budget_seconds: int
    public_structures_startup_scan_batch_size: int
    public_structures_startup_scan_pause_seconds: int
    public_structures_esi_request_timeout_seconds: int


@lru_cache(maxsize=1)
def get_settings() -> FlaskSettings:
    return FlaskSettings(
        flask_host=os.getenv("FLASK_HOST", "localhost"),
        flask_port=int(os.getenv("FLASK_PORT", "5000")),
        flask_debug=_bool("FLASK_DEBUG", default=False),
        refresh_metadata_on_startup=_bool("FLASK_REFRESH_METADATA", default=True),
        health_poll_timeout_seconds=_int("FLASK_HEALTH_POLL_TIMEOUT", default=120),
        health_request_timeout_seconds=_int("FLASK_HEALTH_REQUEST_TIMEOUT", default=2),
        api_request_timeout_seconds=_int("FLASK_API_REQUEST_TIMEOUT", default=10),
        public_structures_cache_ttl_seconds=_int("FLASK_PUBLIC_STRUCTURES_TTL", default=24 * 3600),
        public_structures_startup_scan_enabled=_bool("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN", default=True),
        public_structures_startup_scan_max_workers=_int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_WORKERS", default=5),
        public_structures_startup_scan_scan_cap=_int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_CAP", default=5000),
        public_structures_startup_scan_time_budget_seconds=_int(
            "FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_TIME_BUDGET", default=60
        ),
        public_structures_startup_scan_batch_size=_int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_BATCH_SIZE", default=100),
        public_structures_startup_scan_pause_seconds=_int("FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_PAUSE", default=5),
        public_structures_esi_request_timeout_seconds=_int("FLASK_PUBLIC_STRUCTURES_ESI_TIMEOUT", default=10),
    )


def flask_host() -> str:
    return get_settings().flask_host


def flask_port() -> int:
    return get_settings().flask_port


def flask_debug() -> bool:
    return get_settings().flask_debug


def refresh_metadata_on_startup() -> bool:
    # Keeping existing behavior (True) unless explicitly disabled.
    return get_settings().refresh_metadata_on_startup


def health_poll_timeout_seconds() -> int:
    return get_settings().health_poll_timeout_seconds


def health_request_timeout_seconds() -> int:
    return get_settings().health_request_timeout_seconds


def api_request_timeout_seconds() -> int:
    return get_settings().api_request_timeout_seconds


def public_structures_cache_ttl_seconds() -> int:
    # How long cached public structures in db_app are considered fresh.
    return get_settings().public_structures_cache_ttl_seconds


def public_structures_startup_scan_enabled() -> bool:
    # Run a background global scan of /universe/structures on startup.
    # This can be heavy; defaults to enabled but bounded by conservative limits.
    return get_settings().public_structures_startup_scan_enabled


def public_structures_startup_scan_max_workers() -> int:
    return get_settings().public_structures_startup_scan_max_workers


def public_structures_startup_scan_scan_cap() -> int:
    # Max structure IDs to attempt per "slice".
    return get_settings().public_structures_startup_scan_scan_cap


def public_structures_startup_scan_time_budget_seconds() -> int:
    # Max seconds per slice; the scanner yields between slices.
    return get_settings().public_structures_startup_scan_time_budget_seconds


def public_structures_startup_scan_batch_size() -> int:
    return get_settings().public_structures_startup_scan_batch_size


def public_structures_startup_scan_pause_seconds() -> int:
    # Sleep between slices to be kind to ESI.
    return get_settings().public_structures_startup_scan_pause_seconds


def public_structures_esi_request_timeout_seconds() -> int:
    # Upper-bound (seconds) for individual ESI requests during public structures scans.
    return get_settings().public_structures_esi_request_timeout_seconds


def api_base() -> str:
    return f"http://{flask_host()}:{flask_port()}"
