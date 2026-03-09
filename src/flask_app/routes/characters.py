from __future__ import annotations

from flask import Blueprint, request

from flask_app.deps import get_state
from flask_app.bootstrap import require_ready
from flask_app.http import ok

from eve_online_industry_tracker.application.characters.service import CharactersService


characters_bp = Blueprint("characters", __name__)


@characters_bp.get("/characters")
def characters():
    require_ready(get_state())
    svc = CharactersService(state=get_state())
    return ok(data=svc.list_characters())


@characters_bp.get("/characters/oauth")
def characters_oauth():
    """Return non-secret OAuth metadata for characters.

    Exposes scopes and token expiry so the UI can show auth status.
    Does NOT expose refresh/access tokens.
    """
    require_ready(get_state())
    svc = CharactersService(state=get_state())
    return ok(data=svc.list_oauth_metadata())


@characters_bp.get("/characters/wallet_balances")
def characters_get_wallet_balances():
    require_ready(get_state())
    svc = CharactersService(state=get_state())
    return ok(data=svc.get_wallet_balances())


@characters_bp.get("/characters/assets")
def characters_get_assets():
    require_ready(get_state())
    svc = CharactersService(state=get_state())
    return ok(data=svc.get_assets())


@characters_bp.get("/characters/market_orders")
def characters_get_market_orders():
    require_ready(get_state())
    svc = CharactersService(state=get_state())
    refresh_raw = (request.args.get("refresh") or "0").strip().lower()
    compare_raw = (request.args.get("compare") or "1").strip().lower()
    refresh = refresh_raw in {"1", "true", "yes", "y", "on"}
    compare = compare_raw in {"1", "true", "yes", "y", "on"}

    refreshed_orders = svc.get_market_orders_enriched(
        refresh=refresh,
        include_orderbook_comparison=compare,
    )
    return ok(data=refreshed_orders)
