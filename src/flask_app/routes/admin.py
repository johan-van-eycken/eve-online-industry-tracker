from __future__ import annotations

import logging
import os
import signal

from flask import Blueprint, jsonify, request

from sqlalchemy import func, select

from flask_app.state import state
from flask_app.http import ok, error

from classes.database_models import PublicStructuresModel
from flask_app.bootstrap import require_ready
from flask_app.settings import (
    public_structures_startup_scan_batch_size,
    public_structures_startup_scan_max_workers,
    public_structures_startup_scan_pause_seconds,
    public_structures_startup_scan_scan_cap,
    public_structures_startup_scan_time_budget_seconds,
)
from flask_app.services.public_structures_cache_service import (
    refresh_public_structures_for_system,
    trigger_global_public_structures_scan,
    trigger_refresh_public_structures_for_system,
)


admin_bp = Blueprint("admin", __name__)


@admin_bp.get("/health")
def health_check():
    if state.init_state != "Ready":
        payload = {"status": "not_ready", "init_state": state.init_state}
        if state.init_error:
            payload["error"] = state.init_error
        return jsonify(payload), 503
    return jsonify({"status": "OK"}), 200


@admin_bp.route("/shutdown", methods=["GET", "POST"])
def shutdown():
    """Shutdown the Flask server."""
    try:
        logging.info("Shutdown request received")

        # For Windows
        if os.name == "nt":
            os.kill(os.getpid(), signal.SIGTERM)
        else:
            # For Unix-like systems
            func = request.environ.get("werkzeug.server.shutdown")
            if func is None:
                os.kill(os.getpid(), signal.SIGTERM)
            else:
                func()

        return ok(message="Server shutting down...")
    except Exception as e:
        logging.error("Error during shutdown: %s", e)
        return error(message=str(e))


@admin_bp.get("/public_structures_status")
def public_structures_status():
    """Return cached public structures status + last refresh diagnostics."""
    try:
        if state.db_app is None:
            return error(message="App DB not initialized", status_code=503)

        system_id_param = request.args.get("system_id")
        system_id = int(system_id_param) if system_id_param not in (None, "") else None

        session = state.db_app.Session()
        count_stmt = select(func.count()).select_from(PublicStructuresModel)
        max_stmt = select(func.max(PublicStructuresModel.updated_at)).select_from(PublicStructuresModel)
        if system_id is not None:
            count_stmt = count_stmt.where(PublicStructuresModel.system_id == system_id)
            max_stmt = max_stmt.where(PublicStructuresModel.system_id == system_id)

        count = session.execute(count_stmt).scalar_one()
        max_updated_at = session.execute(max_stmt).scalar_one()
        session.close()

        return ok(
            data={
                "rows": int(count or 0),
                "max_updated_at": (max_updated_at.isoformat() if max_updated_at else None),
                "last_refresh": {
                    "system_id": state.public_structures_last_refresh_system_id,
                    "started_at": (
                        state.public_structures_last_refresh_started_at.isoformat()
                        if state.public_structures_last_refresh_started_at
                        else None
                    ),
                    "finished_at": (
                        state.public_structures_last_refresh_finished_at.isoformat()
                        if state.public_structures_last_refresh_finished_at
                        else None
                    ),
                    "error": state.public_structures_last_refresh_error,
                    "facilities_count": state.public_structures_last_refresh_facilities_count,
                    "structure_facilities_count": state.public_structures_last_refresh_structure_facilities_count,
                    "rows_written": state.public_structures_last_refresh_rows_written,
                },
                "global_scan": {
                    "running": state.public_structures_global_scan_running,
                    "started_at": (
                        state.public_structures_global_scan_started_at.isoformat()
                        if state.public_structures_global_scan_started_at
                        else None
                    ),
                    "heartbeat_at": (
                        state.public_structures_global_scan_last_heartbeat_at.isoformat()
                        if state.public_structures_global_scan_last_heartbeat_at
                        else None
                    ),
                    "finished_at": (
                        state.public_structures_global_scan_finished_at.isoformat()
                        if state.public_structures_global_scan_finished_at
                        else None
                    ),
                    "error": state.public_structures_global_scan_error,
                    "total_ids": state.public_structures_global_scan_total_ids,
                    "cursor": state.public_structures_global_scan_cursor,
                    "attempted": state.public_structures_global_scan_attempted,
                    "rows_written": state.public_structures_global_scan_rows_written,
                },
            }
        )
    except Exception as e:
        return error(message=f"Failed to compute public structures status: {e}")


@admin_bp.post("/refresh_public_structures")
def refresh_public_structures():
    """Manually trigger a refresh of the public_structures cache for a system."""
    try:
        require_ready_flag = request.args.get("require_ready", "1")
        if require_ready_flag not in ("0", "1"):
            return error(message="require_ready must be 0 or 1", status_code=400)

        if require_ready_flag == "1":
            require_ready()

        system_id_param = request.args.get("system_id")
        if system_id_param in (None, ""):
            return error(message="system_id query parameter is required", status_code=400)
        system_id = int(system_id_param)

        def _int_arg(name: str, default: int) -> int:
            raw = request.args.get(name)
            if raw in (None, ""):
                return default
            return int(raw)

        def _float_arg(name: str, default: float) -> float:
            raw = request.args.get(name)
            if raw in (None, ""):
                return default
            return float(raw)

        scan = _int_arg("scan", 250)
        workers = _int_arg("workers", 5)
        max_results = _int_arg("max_results", 50)
        time_budget = _float_arg("time_budget", 8.0)

        async_flag = request.args.get("async", "0")
        if async_flag not in ("0", "1"):
            return error(message="async must be 0 or 1", status_code=400)

        if async_flag == "1":
            started = trigger_refresh_public_structures_for_system(
                system_id,
                scan_cap=scan,
                max_workers=workers,
                time_budget_seconds=time_budget,
                max_results=max_results,
            )
            return ok(
                message="Public structures refresh triggered",
                meta={"started": started, "system_id": system_id, "scan": scan, "workers": workers, "time_budget": time_budget},
            )

        rows_written = refresh_public_structures_for_system(
            system_id,
            scan_cap=scan,
            max_workers=workers,
            time_budget_seconds=time_budget,
            max_results=max_results,
        )
        return ok(
            message="Public structures refresh completed",
            meta={
                "rows_written": rows_written,
                "system_id": system_id,
                "scan": scan,
                "workers": workers,
                "time_budget": time_budget,
            },
        )
    except Exception as e:
        return error(message=f"Failed to refresh public structures: {e}")


@admin_bp.post("/public_structures_scan/start")
def start_public_structures_scan():
    """Start (or no-op if already running) the global public_structures scan."""
    try:
        require_ready()

        def _int_arg(name: str, default: int) -> int:
            raw = request.args.get(name)
            if raw in (None, ""):
                return default
            return int(raw)

        def _float_arg(name: str, default: float) -> float:
            raw = request.args.get(name)
            if raw in (None, ""):
                return default
            return float(raw)

        scan = _int_arg("scan", public_structures_startup_scan_scan_cap())
        workers = _int_arg("workers", public_structures_startup_scan_max_workers())
        time_budget = _float_arg("time_budget", float(public_structures_startup_scan_time_budget_seconds()))
        batch_size = _int_arg("batch_size", public_structures_startup_scan_batch_size())
        pause = _float_arg("pause", float(public_structures_startup_scan_pause_seconds()))

        started = trigger_global_public_structures_scan(
            scan_cap=scan,
            max_workers=workers,
            time_budget_seconds=time_budget,
            batch_size=batch_size,
            pause_seconds=pause,
        )

        return ok(
            message=("Global public structures scan started" if started else "Global public structures scan already running"),
            meta={
                "started": started,
                "scan": scan,
                "workers": workers,
                "time_budget": time_budget,
                "batch_size": batch_size,
                "pause": pause,
            },
        )
    except Exception as e:
        return error(message=f"Failed to start global public structures scan: {e}")
