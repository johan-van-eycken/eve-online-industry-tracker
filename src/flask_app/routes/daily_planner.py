from __future__ import annotations

from flask import Blueprint, jsonify, request

from flask_app.bootstrap import require_ready
from flask_app.deps import get_state
from flask_app.http import error, ok


daily_planner_bp = Blueprint("daily_planner", __name__)


@daily_planner_bp.post("/planner/compute")
def compute():
    require_ready(get_state())
    get_state().daily_planner_service.compute_plan_async()
    return ok(data={"status": "started"})


@daily_planner_bp.get("/planner/status")
def status():
    require_ready(get_state())
    return ok(data=get_state().daily_planner_service.get_compute_status())


@daily_planner_bp.get("/planner/plan")
def plan():
    # Side effect: recomputes freshness_score on each call (intentional per spec)
    require_ready(get_state())
    return ok(data=get_state().daily_planner_service.get_active_plan())


@daily_planner_bp.post("/planner/action/done")
def mark_done():
    require_ready(get_state())
    payload = request.get_json(silent=True) or {}
    action_id = payload.get("action_id")
    if not action_id:
        return error(message="action_id required", status_code=400)
    get_state().daily_planner_service.mark_action_done(int(action_id))
    return ok(data={"status": "done"})


@daily_planner_bp.patch("/planner/action/<int:action_id>")
def set_action_status(action_id: int):
    # Body: {"status": "pending" | "skipped"}
    # Allowed transitions: done→pending, pending→skipped, skipped→pending.
    # Returns 409 if feedback already processed (repo raises ValueError).
    require_ready(get_state())
    payload = request.get_json(silent=True) or {}
    new_status = payload.get("status")
    if new_status not in ("pending", "skipped"):
        return error(message="status must be 'pending' or 'skipped'", status_code=400)

    # Validate the state-machine transition before writing
    current = get_state().daily_planner_service.get_action(action_id)
    if current is None:
        return error(message="Action not found", status_code=404)
    current_status = str(current.get("status") or "")
    allowed = {
        "pending": {"skipped"},
        "skipped": {"pending"},
        "done": {"pending"},
    }
    if new_status not in allowed.get(current_status, set()):
        return error(
            message=f"Transition {current_status!r} → {new_status!r} is not allowed",
            status_code=422,
        )

    try:
        get_state().daily_planner_service.set_action_status(action_id, new_status)
    except ValueError as e:
        return error(message=str(e), status_code=409)
    return ok(data={"status": new_status})


@daily_planner_bp.get("/planner/analytics")
def analytics():
    require_ready(get_state())
    return ok(data=get_state().daily_planner_service.get_analytics())


@daily_planner_bp.route("/planner/market_intel/status", methods=["GET"])
def market_intel_status():
    state = get_state()
    mij = getattr(state, "_market_intelligence_job", None)
    if mij is None:
        return jsonify({"status": "not_started", "last_completed_at": None, "last_error": None})
    return jsonify(mij.get_status())
