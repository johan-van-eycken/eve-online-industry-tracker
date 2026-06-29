from __future__ import annotations

from flask import Blueprint, request

from flask_app.bootstrap import require_ready
from flask_app.deps import get_state
from flask_app.http import ok

from eve_online_industry_tracker.application.corporations.service import CorporationsService


corporations_bp = Blueprint("corporations", __name__)


@corporations_bp.get("/corporations")
def corporations():
    require_ready(get_state())
    svc = CorporationsService(state=get_state())
    return ok(data=svc.list_corporations())


@corporations_bp.get("/corporations/assets")
def corporations_assets():
    require_ready(get_state())
    svc = CorporationsService(state=get_state())
    corporation_id_raw = (request.args.get("corporation_id") or "").strip()
    corporation_id = int(corporation_id_raw) if corporation_id_raw.isdigit() else None
    return ok(data=svc.list_assets(corporation_id=corporation_id))


@corporations_bp.get("/corporations/market_orders")
def corporations_get_market_orders():
    require_ready(get_state())
    svc = CorporationsService(state=get_state())
    refresh_raw = (request.args.get("refresh") or "0").strip().lower()
    refresh = refresh_raw in {"1", "true", "yes", "y", "on"}

    corporation_id_raw = (request.args.get("corporation_id") or "").strip()
    corporation_id = int(corporation_id_raw) if corporation_id_raw.isdigit() else None

    return ok(data=svc.get_market_orders(refresh=refresh, corporation_id=corporation_id))


@corporations_bp.get("/corporations/realized_profit")
def corporations_realized_profit():
    require_ready(get_state())
    svc = CorporationsService(state=get_state())
    refresh_raw = (request.args.get("refresh") or "0").strip().lower()
    refresh = refresh_raw in {"1", "true", "yes", "y", "on"}

    corporation_id_raw = (request.args.get("corporation_id") or "").strip()
    corporation_id = int(corporation_id_raw) if corporation_id_raw.isdigit() else None

    return ok(data=svc.get_realized_profit_ledger(refresh=refresh, corporation_id=corporation_id))
