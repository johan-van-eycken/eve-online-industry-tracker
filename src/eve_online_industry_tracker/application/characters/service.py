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

        # Pre-resolve missing region names (best-effort).
        region_ids: set[int] = set()
        for character in refreshed_data:
            for order in (character or {}).get("market_orders", []):
                if not isinstance(order, dict):
                    continue
                if order.get("region_name"):
                    continue
                region_id = order.get("region_id")
                if isinstance(region_id, int) and region_id > 0:
                    region_ids.add(region_id)

        region_name_by_id: dict[int, str] = {}
        try:
            for rid in sorted(region_ids):
                try:
                    name = None
                    try:
                        name = self._state.esi_service.get_region_name(int(rid))
                    except Exception:
                        name = None

                    if not name:
                        info = self._state.esi_service.get_location_info(int(rid))
                        if isinstance(info, dict) and info.get("name"):
                            name = str(info.get("name"))

                    if name:
                        region_name_by_id[int(rid)] = str(name)
                except Exception:
                    continue
        except Exception:
            region_name_by_id = {}

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

                price_difference: float = 0.0

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
                                # Positive means we're best (we beat the current highest buy).
                                price_difference = order_price - float(highest_price)
                            except Exception:
                                pass
                    else:
                        prices_list = sell_order_book.get(type_id, []) if isinstance(sell_order_book, dict) else []
                        if prices_list and all(isinstance(o, dict) for o in prices_list):
                            try:
                                lowest_price = min(o.get("price", 0) for o in prices_list)
                                # Positive means we're best (we're cheaper than the current lowest sell).
                                price_difference = float(lowest_price) - order_price
                            except Exception:
                                pass

                # Volume formatting: <remain>/<total>
                remain_raw = order.get("volume_remain")
                total_raw = order.get("volume_total")
                try:
                    remain_num = float(remain_raw) if remain_raw is not None else 0.0
                except Exception:
                    remain_num = 0.0
                try:
                    total_num = float(total_raw) if total_raw is not None else 0.0
                except Exception:
                    total_num = 0.0

                def _fmt_intish(v: float) -> str:
                    try:
                        if float(v).is_integer():
                            return str(int(v))
                    except Exception:
                        pass
                    return str(v)

                volume_display = ""
                if total_raw is not None or remain_raw is not None:
                    volume_display = f"{_fmt_intish(remain_num)}/{_fmt_intish(total_num)}"

                # Fallback: some callers may already provide a preformatted volume string.
                if not volume_display:
                    vol_raw = order.get("volume")
                    if isinstance(vol_raw, str) and "/" in vol_raw:
                        volume_display = vol_raw
                        if remain_raw is None:
                            try:
                                remain_num = float(vol_raw.split("/", 1)[0])
                            except Exception:
                                pass

                region_id = order.get("region_id")
                region_display = order.get("region_name")
                if not region_display and isinstance(region_id, int) and region_id > 0:
                    region_display = region_name_by_id.get(int(region_id))
                if not region_display:
                    region_display = (f"Region {region_id}" if region_id else "Unknown")

                enriched_orders.append(
                    {
                        "owner": order.get("owner"),
                        "type_id": type_id,
                        "type_name": order.get("type_name"),
                        "price": order.get("price"),
                        "price_difference": price_difference,
                        "volume": volume_display,
                        "total_price": order_price * float(remain_num or 0),
                        "range": order.get("range"),
                        "min_volume": order.get("min_volume"),
                        "expires_in": expires_in,
                        "escrow_remaining": order.get("escrow", 0),
                        "station": order.get("station_name")
                        or order.get("location_name")
                        or f"Location {order.get('location_id', 'Unknown')}",
                        "region": region_display,
                        "is_buy_order": order.get("is_buy_order"),
                        "type_group_id": order.get("type_group_id", -1),
                        "type_group_name": order.get("type_group_name", "Unknown"),
                        "type_category_id": order.get("type_category_id", -1),
                        "type_category_name": order.get("type_category_name", "Unknown"),
                        "is_blueprint_copy": False,
                    }
                )

        return enriched_orders
