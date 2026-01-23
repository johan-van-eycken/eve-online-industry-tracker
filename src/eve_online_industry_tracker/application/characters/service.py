from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from eve_online_industry_tracker.infrastructure.oauth_character_repository import (
    OAuthCharacterRepository,
)


class CharactersService:
    def __init__(self, *, state: Any):
        self._state = state

    def list_characters(self) -> Any:
        return self._state.char_manager.get_characters()

    def list_oauth_metadata(self) -> list[dict[str, Any]]:
        repo = OAuthCharacterRepository(self._state.db_oauth.session)
        return [asdict(x) for x in repo.list_metadata()]

    def get_wallet_balances(self) -> Any:
        return self._state.char_manager.get_wallet_balances()

    def get_assets(self) -> Any:
        return self._state.char_manager.get_assets()

    def get_market_orders_enriched(self) -> list[dict[str, Any]]:
        refreshed_data = self._state.char_manager.get_market_orders()

        now = datetime.now(timezone.utc)
        enriched_orders: list[dict[str, Any]] = []

        buy_type_ids: set[int] = set()
        sell_type_ids: set[int] = set()
        for character in refreshed_data:
            for order in (character or {}).get("market_orders", []):
                type_id = order.get("type_id")
                if not isinstance(type_id, int):
                    continue
                if order.get("is_buy_order"):
                    buy_type_ids.add(type_id)
                else:
                    sell_type_ids.add(type_id)

        buy_order_book: dict[int, list[dict[str, Any]]] = {}
        sell_order_book: dict[int, list[dict[str, Any]]] = {}

        try:
            if buy_type_ids:
                buy_order_book = self._state.esi_service.get_type_buyprices(sorted(buy_type_ids))
        except Exception:
            buy_order_book = {}

        try:
            if sell_type_ids:
                sell_order_book = self._state.esi_service.get_type_sellprices(sorted(sell_type_ids))
        except Exception:
            sell_order_book = {}

        for character in refreshed_data:
            for order in (character or {}).get("market_orders", []):
                issued_raw = order.get("issued")
                try:
                    issued_dt = datetime.fromisoformat(str(issued_raw).replace("Z", "+00:00"))
                except Exception:
                    issued_dt = now

                duration_days = order.get("duration")
                try:
                    duration_days_int = int(duration_days)
                except Exception:
                    duration_days_int = 0

                expires_dt = issued_dt + timedelta(days=duration_days_int)
                expires_in_td = expires_dt - now
                if expires_in_td.total_seconds() > 0:
                    days = expires_in_td.days
                    hours = expires_in_td.seconds // 3600
                    mins = (expires_in_td.seconds % 3600) // 60
                    expires_in = f"{days}d {hours}h {mins}m"
                else:
                    expires_in = "Expired"

                price_difference: float = 0
                price_status = "⚪N/A"

                type_id = order.get("type_id")
                try:
                    order_price = float(order.get("price"))
                except Exception:
                    order_price = 0.0

                if isinstance(type_id, int):
                    if order.get("is_buy_order"):
                        prices_list = buy_order_book.get(type_id, []) if isinstance(buy_order_book, dict) else []
                        if prices_list and all(isinstance(o, dict) for o in prices_list):
                            try:
                                highest_price = max(o.get("price", 0) for o in prices_list)
                                price_difference = order_price - float(highest_price)
                                price_status = "🟢Best price" if price_difference > 0 else "🔴Undercut"
                            except Exception:
                                pass
                    else:
                        prices_list = sell_order_book.get(type_id, []) if isinstance(sell_order_book, dict) else []
                        if prices_list and all(isinstance(o, dict) for o in prices_list):
                            try:
                                lowest_price = min(o.get("price", 0) for o in prices_list)
                                price_difference = order_price - float(lowest_price)
                                price_status = "🔴Undercut" if price_difference > 0 else "🟢Best price"
                            except Exception:
                                pass

                enriched_orders.append(
                    {
                        "owner": order.get("owner"),
                        "type_id": type_id,
                        "type_name": order.get("type_name"),
                        "price": order.get("price"),
                        "price_status": price_status,
                        "price_difference": price_difference,
                        "volume": str(order.get("volume_remain")) + "/" + str(order.get("volume_total")),
                        "total_price": order_price * float(order.get("volume_remain") or 0),
                        "range": order.get("range"),
                        "min_volume": order.get("min_volume"),
                        "expires_in": expires_in,
                        "escrow_remaining": order.get("escrow", 0),
                        "station": order.get("station_name")
                        or order.get("location_name")
                        or f"Location {order.get('location_id', 'Unknown')}",
                        "region": order.get("region_name")
                        or (f"Region {order.get('region_id')}" if order.get("region_id") else "Unknown"),
                        "is_buy_order": order.get("is_buy_order"),
                        "type_group_id": order.get("type_group_id", -1),
                        "type_group_name": order.get("type_group_name", "Unknown"),
                        "type_category_id": order.get("type_category_id", -1),
                        "type_category_name": order.get("type_category_name", "Unknown"),
                        "is_blueprint_copy": False,
                    }
                )

        return enriched_orders
