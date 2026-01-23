from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import func, select

from eve_online_industry_tracker.db_models import PublicStructuresModel

from eve_online_industry_tracker.infrastructure.public_structures_admin_adapter import (
    refresh_public_structures_for_system,
    stop_global_public_structures_scan,
    trigger_global_public_structures_scan,
    trigger_refresh_public_structures_for_system,
)


class AdminService:
    def __init__(self, *, state: Any):
        self._state = state

    def health_payload(self) -> tuple[dict, int]:
        if self._state.init_state != "Ready":
            payload: dict[str, Any] = {"status": "not_ready", "init_state": self._state.init_state}
            if self._state.init_error:
                payload["error"] = self._state.init_error
            return payload, 503
        payload: dict[str, Any] = {"status": "OK"}
        warnings = getattr(self._state, "init_warnings", None)
        if warnings:
            payload["warnings"] = list(warnings)
        return payload, 200

    def public_structures_status(self, *, system_id: int | None) -> dict:
        if self._state.db_app is None:
            raise RuntimeError("App DB not initialized")

        session = self._state.db_app.Session()
        try:
            count_stmt = select(func.count()).select_from(PublicStructuresModel)
            max_stmt = select(func.max(PublicStructuresModel.updated_at)).select_from(PublicStructuresModel)
            if system_id is not None:
                count_stmt = count_stmt.where(PublicStructuresModel.system_id == system_id)
                max_stmt = max_stmt.where(PublicStructuresModel.system_id == system_id)

            count = session.execute(count_stmt).scalar_one()
            max_updated_at = session.execute(max_stmt).scalar_one()

            return {
                "rows": int(count or 0),
                "max_updated_at": (max_updated_at.isoformat() if max_updated_at else None),
                "last_refresh": {
                    "system_id": self._state.public_structures_last_refresh_system_id,
                    "started_at": (
                        self._state.public_structures_last_refresh_started_at.isoformat()
                        if self._state.public_structures_last_refresh_started_at
                        else None
                    ),
                    "finished_at": (
                        self._state.public_structures_last_refresh_finished_at.isoformat()
                        if self._state.public_structures_last_refresh_finished_at
                        else None
                    ),
                    "error": self._state.public_structures_last_refresh_error,
                    "facilities_count": self._state.public_structures_last_refresh_facilities_count,
                    "structure_facilities_count": self._state.public_structures_last_refresh_structure_facilities_count,
                    "rows_written": self._state.public_structures_last_refresh_rows_written,
                },
                "global_scan": {
                    "running": self._state.public_structures_global_scan_running,
                    "started_at": (
                        self._state.public_structures_global_scan_started_at.isoformat()
                        if self._state.public_structures_global_scan_started_at
                        else None
                    ),
                    "heartbeat_at": (
                        self._state.public_structures_global_scan_last_heartbeat_at.isoformat()
                        if self._state.public_structures_global_scan_last_heartbeat_at
                        else None
                    ),
                    "finished_at": (
                        self._state.public_structures_global_scan_finished_at.isoformat()
                        if self._state.public_structures_global_scan_finished_at
                        else None
                    ),
                    "error": self._state.public_structures_global_scan_error,
                    "total_ids": self._state.public_structures_global_scan_total_ids,
                    "cursor": self._state.public_structures_global_scan_cursor,
                    "attempted": self._state.public_structures_global_scan_attempted,
                    "rows_written": self._state.public_structures_global_scan_rows_written,
                },
            }
        finally:
            session.close()

    def refresh_public_structures(self, *, system_id: int, scan: int, workers: int, max_results: int, time_budget: float, async_flag: bool) -> dict:
        if async_flag:
            started = trigger_refresh_public_structures_for_system(
                self._state,
                system_id,
                scan_cap=scan,
                max_workers=workers,
                time_budget_seconds=time_budget,
                max_results=max_results,
            )
            return {
                "message": "Public structures refresh triggered",
                "meta": {"started": started, "system_id": system_id, "scan": scan, "workers": workers, "time_budget": time_budget},
            }

        rows_written = refresh_public_structures_for_system(
            self._state,
            system_id,
            scan_cap=scan,
            max_workers=workers,
            time_budget_seconds=time_budget,
            max_results=max_results,
        )
        return {
            "message": "Public structures refresh completed",
            "meta": {"rows_written": rows_written, "system_id": system_id, "scan": scan, "workers": workers, "time_budget": time_budget},
        }

    def start_public_structures_scan(
        self,
        *,
        scan: int,
        workers: int,
        time_budget: float,
        batch_size: int,
        pause: float,
        request_timeout_seconds: float,
    ) -> dict:
        # If a previous run was stopped, allow a new run.
        try:
            ev = getattr(self._state, "public_structures_global_scan_stop_event", None)
            if ev is not None:
                ev.clear()
        except Exception:
            pass

        started = trigger_global_public_structures_scan(
            self._state,
            scan_cap=scan,
            max_workers=workers,
            time_budget_seconds=time_budget,
            batch_size=batch_size,
            pause_seconds=pause,
            request_timeout_seconds=float(request_timeout_seconds),
        )
        return {
            "message": ("Global public structures scan started" if started else "Global public structures scan already running"),
            "meta": {
                "started": started,
                "scan": scan,
                "workers": workers,
                "time_budget": time_budget,
                "batch_size": batch_size,
                "pause": pause,
                "request_timeout_seconds": float(request_timeout_seconds),
            },
        }

    def stop_public_structures_scan(self) -> dict:
        signaled = stop_global_public_structures_scan(self._state)
        return {
            "message": ("Stop signal sent" if signaled else "No scan stop event available"),
            "meta": {"signaled": bool(signaled), "running": bool(getattr(self._state, "public_structures_global_scan_running", False))},
        }
