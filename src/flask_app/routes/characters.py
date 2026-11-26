from __future__ import annotations

from flask import Blueprint, request

from flask_app.deps import get_state
from flask_app.bootstrap import require_ready
from flask_app.http import error, ok, ok_batch, optional_id_param
from flask_app.restart import request_restart

from eve_online_industry_tracker.application.characters import login
from eve_online_industry_tracker.application.characters.service import CharactersService
from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.infrastructure.oauth_character_repository import OAuthCharacterRepository


characters_bp = Blueprint("characters", __name__)

_HISTORY_FIELDS = ("wallet_journal", "wallet_transactions", "market_orders", "assets")


@characters_bp.get("/characters")
def characters():
    require_ready(get_state())
    svc = CharactersService(state=get_state())
    characters = svc.list_characters()
    if request.args.get("summary") == "1":
        # Selectors need ids and names, not each character's full history.
        characters = [{k: v for k, v in c.items() if k not in _HISTORY_FIELDS} for c in characters]
    return ok(data=characters)


@characters_bp.get("/characters/oauth")
def characters_oauth():
    """Return non-secret OAuth metadata for characters.

    Exposes scopes and token expiry so the UI can show auth status.
    Does NOT expose refresh/access tokens.
    """
    require_ready(get_state())
    svc = CharactersService(state=get_state())
    return ok(data=svc.list_oauth_metadata())


def _roster_state():
    """The state, for the roster routes: these work before Ready, not before the OAuth DB is open."""
    state = get_state()
    if state.db_oauth is None:
        raise ServiceError("The OAuth database is not open yet.", status_code=503)
    return state


def _flags(body: dict) -> dict:
    flags = {k: body[k] for k in ("is_main", "is_corp_director", "industry_enabled") if k in body}
    if not all(isinstance(v, bool) for v in flags.values()):
        raise ServiceError("Character flags must be true or false.", status_code=400)
    return flags


@characters_bp.get("/characters/roster")
def characters_roster():
    return ok(data=CharactersService(state=_roster_state()).roster())


@characters_bp.post("/characters/login")
def characters_login():
    state = _roster_state()
    body = request.get_json(silent=True) or {}
    name = str(body.get("character_name") or "").strip()
    if not name:
        return error(message="character_name is required", status_code=400)
    flags = _flags(body)
    repo = OAuthCharacterRepository(state.db_oauth.session)
    row = next((r for r in repo.list_metadata() if (r.character_name or "").casefold() == name.casefold()), None)
    if row is None:
        repo.add(name, **flags)
        is_corp_director = flags.get("is_corp_director", False)
    else:   # an existing row logs in under its stored name and flags
        name, is_corp_director = row.character_name, row.is_corp_director
    url = login.start(state.cfg_manager, state.db_oauth, name, is_corp_director)
    if url is None:
        return error(message="Another character login is in progress.", status_code=409)
    return ok(data={"login_url": url})


@characters_bp.get("/characters/login/status")
def characters_login_status():
    return ok(data=login.status())


@characters_bp.patch("/characters/<name>")
def characters_set_flags(name: str):
    state = _roster_state()
    flags = _flags(request.get_json(silent=True) or {})
    if not flags:
        return error(message="No character flags given.", status_code=400)
    try:
        OAuthCharacterRepository(state.db_oauth.session).set_flags(name, **flags)
    except KeyError:
        return error(message=f"No character {name!r}.", status_code=404)
    return ok(data={"restarting": request_restart()})


@characters_bp.delete("/characters/<name>")
def characters_delete(name: str):
    """Remove the roster row and its login; the app data of the character stays."""
    state = _roster_state()
    try:
        OAuthCharacterRepository(state.db_oauth.session).delete(name)
    except KeyError:
        return error(message=f"No character {name!r}.", status_code=404)
    return ok(data={"restarting": request_restart()})


@characters_bp.get("/characters/wallet_balances")
def characters_get_wallet_balances():
    require_ready(get_state())
    svc = CharactersService(state=get_state())
    return ok_batch(data=svc.get_wallet_balances())


@characters_bp.get("/characters/assets")
def characters_get_assets():
    require_ready(get_state())
    svc = CharactersService(state=get_state())
    character_id = optional_id_param("character_id")
    return ok_batch(data=svc.get_assets(character_id=character_id))


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
    return ok_batch(data=refreshed_orders)


@characters_bp.get("/characters/realized_profit")
def characters_get_realized_profit():
    require_ready(get_state())
    svc = CharactersService(state=get_state())
    refresh_raw = (request.args.get("refresh") or "0").strip().lower()
    refresh = refresh_raw in {"1", "true", "yes", "y", "on"}

    character_id = optional_id_param("character_id")

    return ok(data=svc.get_realized_profit_ledger(refresh=refresh, character_id=character_id))
