from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from flask import Blueprint

from flask_app.state import state
from flask_app.bootstrap import require_ready
from flask_app.http import ok, error

from classes.database_models import OAuthCharacter


characters_bp = Blueprint("characters", __name__)


@characters_bp.get("/characters")
def characters():
    try:
        require_ready()
        characters_data = state.char_manager.get_characters()
        return ok(data=characters_data)
    except Exception as e:
        logging.error("Error fetching characters: %s", e)
        return error(message="Error in GET Method `/characters`: " + str(e))


@characters_bp.get("/characters/oauth")
def characters_oauth():
    """Return non-secret OAuth metadata for characters.

    Exposes scopes and token expiry so the UI can show auth status.
    Does NOT expose refresh/access tokens.
    """
    try:
        require_ready()
        session = state.db_oauth.session
        rows = session.query(OAuthCharacter).all()

        out = []
        now = datetime.now(timezone.utc).timestamp()
        for r in rows:
            token_expiry = getattr(r, "token_expiry", None)
            expires_in = None
            if token_expiry is not None:
                try:
                    expires_in = int(token_expiry) - int(now)
                except Exception:
                    expires_in = None

            out.append(
                {
                    "character_name": getattr(r, "character_name", None),
                    "character_id": getattr(r, "character_id", None),
                    "scopes": getattr(r, "scopes", "") or "",
                    "token_expiry": token_expiry,
                    "expires_in_seconds": expires_in,
                    "has_refresh_token": bool(getattr(r, "refresh_token", None)),
                    "has_access_token": bool(getattr(r, "access_token", None)),
                }
            )

        out.sort(key=lambda x: (str(x.get("character_name") or "").lower()))
        return ok(data=out)
    except Exception as e:
        logging.error("Error fetching character OAuth metadata: %s", e)
        return error(message="Error in GET Method `/characters/oauth`: " + str(e))


@characters_bp.get("/characters/wallet_balances")
def characters_get_wallet_balances():
    try:
        require_ready()
        refreshed_data = state.char_manager.get_wallet_balances()
        return ok(data=refreshed_data)
    except Exception as e:
        logging.error("Error refreshing wallet balances: %s", e)
        return error(message="Error in GET Method `/characters/wallet_balances`: " + str(e))


@characters_bp.get("/characters/assets")
def characters_get_assets():
    try:
        require_ready()
        refreshed_data = state.char_manager.get_assets()
        return ok(data=refreshed_data)
    except Exception as e:
        logging.error("Error refreshing assets: %s", e)
        return error(message="Error in GET Method `/characters/assets`: " + str(e))


@characters_bp.get("/characters/market_orders")
def characters_get_market_orders():
    try:
        require_ready()
        refreshed_data = state.char_manager.get_market_orders()
    except Exception as e:
        error_message = f"Failed to get market orders: {str(e)}"
        logging.error(error_message)
        return error(message=error_message)

    try:
        now = datetime.now(timezone.utc)
        refreshed_orders = []

        for character in refreshed_data:
            if isinstance(character, str):
                character = json.loads(character)

            for order in character.get("market_orders", []):
                issued_dt = datetime.fromisoformat(order["issued"].replace("Z", "+00:00"))
                expires_dt = issued_dt + timedelta(days=order["duration"])
                expires_in_td = expires_dt - now
                if expires_in_td.total_seconds() > 0:
                    days = expires_in_td.days
                    hours = expires_in_td.seconds // 3600
                    mins = expires_in_td.seconds % 3600 // 60
                    expires_in = f"{days}d {hours}h {mins}m"
                else:
                    expires_in = "Expired"

                price_difference = 0
                price_status = "⚪N/A"

                if order["is_buy_order"]:
                    order_book = state.esi_service.get_type_buyprices([order["type_id"]])
                    prices_list = []
                    if isinstance(order_book, dict):
                        prices_list = order_book.get(order["type_id"], [])
                    if prices_list and all(isinstance(o, dict) for o in prices_list):
                        highest_price = max(o["price"] for o in prices_list if "price" in o)
                        price_difference = order["price"] - highest_price
                        price_status = "🟢Best price" if price_difference > 0 else "🔴Undercut"
                else:
                    order_book = state.esi_service.get_type_sellprices([order["type_id"]])
                    prices_list = []
                    if isinstance(order_book, dict):
                        prices_list = order_book.get(order["type_id"], [])
                    if prices_list and all(isinstance(o, dict) for o in prices_list):
                        lowest_price = min(o["price"] for o in prices_list if "price" in o)
                        price_difference = order["price"] - lowest_price
                        price_status = "🔴Undercut" if price_difference > 0 else "🟢Best price"

                refreshed_orders.append(
                    {
                        "owner": order["owner"],
                        "type_id": order["type_id"],
                        "type_name": order["type_name"],
                        "price": order["price"],
                        "price_status": price_status,
                        "price_difference": price_difference,
                        "volume": str(order["volume_remain"]) + "/" + str(order["volume_total"]),
                        "total_price": order["price"] * order["volume_remain"],
                        "range": order["range"],
                        "min_volume": order["min_volume"],
                        "expires_in": expires_in,
                        "escrow_remaining": order.get("escrow", 0),
                        "station": order.get("station_name")
                        or order.get("location_name")
                        or f"Location {order.get('location_id', 'Unknown')}",
                        "region": order.get("region_name")
                        or (f"Region {order.get('region_id')}" if order.get("region_id") else "Unknown"),
                        "is_buy_order": order["is_buy_order"],
                        "type_group_id": order.get("type_group_id", -1),
                        "type_group_name": order.get("type_group_name", "Unknown"),
                        "type_category_id": order.get("type_category_id", -1),
                        "type_category_name": order.get("type_category_name", "Unknown"),
                        "is_blueprint_copy": False,
                    }
                )

        return ok(data=refreshed_orders)
    except Exception as e:
        return error(message="Error in GET Method `/characters/market_orders`: " + str(e))
