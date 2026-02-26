from __future__ import annotations

import logging
import os
import signal
import threading

from flask import Blueprint, jsonify, request

from flask_app.deps import get_state
from flask_app.http import ok, error

from eve_online_industry_tracker.application.admin.service import AdminService
from flask_app.bootstrap import require_ready
from flask_app.background_jobs import stop_background_jobs
from flask_app.settings import (
    public_structures_esi_request_timeout_seconds,
    public_structures_startup_scan_batch_size,
    public_structures_startup_scan_max_workers,
    public_structures_startup_scan_pause_seconds,
    public_structures_startup_scan_scan_cap,
    public_structures_startup_scan_time_budget_seconds,
)

from utils.esi_monitor import get_esi_monitor


admin_bp = Blueprint("admin", __name__)


@admin_bp.get("/health")
def health_check():
    svc = AdminService(state=get_state())
    payload, status = svc.health_payload()
    return jsonify(payload), status


@admin_bp.route("/shutdown", methods=["GET", "POST"])
def shutdown():
    """Shutdown the Flask server."""
    try:
        logging.info("Shutdown request received")

        # Best-effort: ask background threads to stop before process termination.
        stop_background_jobs(get_state())

        # For Windows, respond first then terminate.
        if os.name == "nt":
            def _kill_soon() -> None:
                try:
                    import time

                    time.sleep(0.2)
                finally:
                    os.kill(os.getpid(), signal.SIGTERM)

            threading.Thread(target=_kill_soon, daemon=True, name="shutdown-kill").start()
            return ok(message="Server shutting down...")

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
    system_id_param = request.args.get("system_id")
    system_id = int(system_id_param) if system_id_param not in (None, "") else None
    svc = AdminService(state=get_state())
    data = svc.public_structures_status(system_id=system_id)
    return ok(data=data)


@admin_bp.post("/refresh_public_structures")
def refresh_public_structures():
    """Manually trigger a refresh of the public_structures cache for a system."""
    require_ready_flag = request.args.get("require_ready", "1")
    if require_ready_flag not in ("0", "1"):
        return error(message="require_ready must be 0 or 1", status_code=400)

    if require_ready_flag == "1":
        require_ready(get_state())

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

    svc = AdminService(state=get_state())
    out = svc.refresh_public_structures(
        system_id=system_id,
        scan=scan,
        workers=workers,
        max_results=max_results,
        time_budget=time_budget,
        async_flag=(async_flag == "1"),
    )
    return ok(message=out["message"], meta=out["meta"])


@admin_bp.post("/public_structures_scan/start")
def start_public_structures_scan():
    """Start (or no-op if already running) the global public_structures scan."""
    require_ready(get_state())

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
    request_timeout_seconds = _float_arg("request_timeout_seconds", float(public_structures_esi_request_timeout_seconds()))

    svc = AdminService(state=get_state())
    out = svc.start_public_structures_scan(
        scan=scan,
        workers=workers,
        time_budget=time_budget,
        batch_size=batch_size,
        pause=pause,
        request_timeout_seconds=request_timeout_seconds,
    )

    return ok(message=out["message"], meta=out["meta"])


@admin_bp.post("/public_structures_scan/stop")
def stop_public_structures_scan():
    """Signal the global public_structures scan to stop (does not shutdown the app)."""
    svc = AdminService(state=get_state())
    out = svc.stop_public_structures_scan()
    return ok(message=out["message"], meta=out["meta"])


@admin_bp.get("/esi_metrics")
def esi_metrics():
    """Return in-process ESI call metrics.

    This endpoint is intentionally available even while the app is initializing,
    so developers can observe startup ESI traffic.
    """

    def _int_arg(name: str, default: int) -> int:
        raw = request.args.get(name)
        if raw in (None, ""):
            return int(default)
        return int(raw)

    window = _int_arg("window", 900)
    bucket = _int_arg("bucket", 5)
    top = _int_arg("top", 20)

    snap = get_esi_monitor().snapshot(window_seconds=window, bucket_seconds=bucket, top_n=top)
    return ok(data=snap)
