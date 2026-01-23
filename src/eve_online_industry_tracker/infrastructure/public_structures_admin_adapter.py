from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.infrastructure.public_structures_cache_service import (
    refresh_public_structures_for_system as _refresh_public_structures_for_system,
    stop_global_public_structures_scan as _stop_global_public_structures_scan,
    trigger_global_public_structures_scan as _trigger_global_public_structures_scan,
    trigger_refresh_public_structures_for_system as _trigger_refresh_public_structures_for_system,
)


def trigger_refresh_public_structures_for_system(
    state: Any,
    system_id: int,
    *,
    scan_cap: int,
    max_workers: int,
    time_budget_seconds: float,
    max_results: int,
) -> bool:
    return _trigger_refresh_public_structures_for_system(
        state=state,
        system_id=system_id,
        scan_cap=scan_cap,
        max_workers=max_workers,
        time_budget_seconds=time_budget_seconds,
        max_results=max_results,
    )


def refresh_public_structures_for_system(
    state: Any,
    system_id: int,
    *,
    scan_cap: int,
    max_workers: int,
    time_budget_seconds: float,
    max_results: int,
) -> int:
    return _refresh_public_structures_for_system(
        state=state,
        system_id=system_id,
        scan_cap=scan_cap,
        max_workers=max_workers,
        time_budget_seconds=time_budget_seconds,
        max_results=max_results,
    )


def trigger_global_public_structures_scan(
    state: Any,
    *,
    scan_cap: int,
    max_workers: int,
    time_budget_seconds: float,
    batch_size: int,
    pause_seconds: float,
    request_timeout_seconds: float = 5.0,
) -> bool:
    return _trigger_global_public_structures_scan(
        state=state,
        scan_cap=scan_cap,
        max_workers=max_workers,
        time_budget_seconds=time_budget_seconds,
        batch_size=batch_size,
        pause_seconds=pause_seconds,
        request_timeout_seconds=float(request_timeout_seconds),
    )


def stop_global_public_structures_scan(state: Any) -> bool:
    return _stop_global_public_structures_scan(state=state)
