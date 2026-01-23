from __future__ import annotations

from flask import Blueprint

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
    refreshed_orders = svc.get_market_orders_enriched()
    return ok(data=refreshed_orders)
