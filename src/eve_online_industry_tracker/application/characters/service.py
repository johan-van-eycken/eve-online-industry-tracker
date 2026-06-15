from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from eve_online_industry_tracker.application.characters.realized_profit import (
    CharacterRealizedProfitLedgerService,
    summarize_realized_profit_rows,
)
from eve_online_industry_tracker.application.market_analysis.pricing_suggestion_service import (
    PricingSuggestionService,
)
from eve_online_industry_tracker.application.market_analysis.market_history_service import (
    MarketHistoryService,
)
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

    def get_assets(
        self,
        *,
        character_id: int | None = None,
    ) -> Any:
        return self._state.char_manager.get_assets(character_id=character_id)

    def get_realized_profit_ledger(
        self,
        *,
        refresh: bool = False,
        character_id: int | None = None,
    ) -> dict[str, Any]:
        if refresh:
            self._state.char_manager.refresh_realized_profit_inputs(character_id=character_id)

        market_prices = self._state.esi_service.get_market_prices()
        ledger_service = CharacterRealizedProfitLedgerService(
            app_session=self._state.db_app.session,
            sde_session=self._state.db_sde.session,
            market_prices=market_prices if isinstance(market_prices, list) else [],
        )

        rows = ledger_service.list_rows(character_id=character_id)
        if refresh or not rows:
            rows = ledger_service.rebuild(character_id=character_id)

        return {
            "rows": rows,
            "summary": summarize_realized_profit_rows(rows),
        }

    def get_market_orders_enriched(
        self,
        *,
        refresh: bool = False,
        include_orderbook_comparison: bool = True,
    ) -> list[dict[str, Any]]:
        refreshed_data = self._state.char_manager.get_market_orders(refresh=bool(refresh))

        now = datetime.now(timezone.utc)
        enriched_orders: list[dict[str, Any]] = []

        buy_order_book: dict[int, list[dict[str, Any]]] = {}
        sell_order_book: dict[int, list[dict[str, Any]]] = {}

        # Initialize pricing suggestion service for sell price analysis
        pricing_svc = PricingSuggestionService(state=self._state)
        market_history_svc = MarketHistoryService(state=self._state)

        if include_orderbook_comparison:
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

            # Fetch and cache market history for sell orders
            try:
                for type_id in sell_type_ids:
                    market_history_svc.fetch_and_store_history(type_id=type_id, region_id=10000002)
            except Exception:
                pass

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

                if include_orderbook_comparison and isinstance(type_id, int):
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

                # Best-effort: older cached orders may have region_id but no region_name
                # due to ID classification/enrichment issues at the time they were stored.
                region_name = order.get("region_name")
                if (not region_name) and isinstance(order.get("region_id"), int):
                    try:
                        region_info = self._state.esi_service.get_location_info(int(order.get("region_id")))
                        if isinstance(region_info, dict):
                            region_name = region_info.get("name")
                    except Exception:
                        region_name = None

                # Get pricing suggestion for sell orders
                advised_price_data = None
                character_id = order.get("character_id") or (character or {}).get("character_id")
                if not order.get("is_buy_order") and isinstance(type_id, int) and isinstance(character_id, int):
                    try:
                        advised_price_data = pricing_svc.suggest_price(
                            character_id=character_id,
                            type_id=type_id,
                            current_price=order_price,
                            quantity=int(order.get("volume_remain") or 0),
                            order_duration_days=duration_days_int or 90,
                        )
                    except Exception:
                        advised_price_data = None

                enriched_order = {
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
                    "region": region_name
                    or (f"Region {order.get('region_id')}" if order.get("region_id") else "Unknown"),
                    "is_buy_order": order.get("is_buy_order"),
                    "type_group_id": order.get("type_group_id", -1),
                    "type_group_name": order.get("type_group_name", "Unknown"),
                    "type_category_id": order.get("type_category_id", -1),
                    "type_category_name": order.get("type_category_name", "Unknown"),
                    "is_blueprint_copy": False,
                }

                # Add pricing suggestion if available
                if advised_price_data:
                    enriched_order["advised_price"] = advised_price_data.get("advised_price")
                    enriched_order["advised_price_confidence"] = advised_price_data.get("confidence")
                    enriched_order["pricing_breakdown"] = advised_price_data.get("breakdown")
                    enriched_order["pricing_reasoning"] = advised_price_data.get("reasoning")
                    enriched_order["cost_basis"] = advised_price_data.get("cost_basis")
                    enriched_order["acquisition_source"] = advised_price_data.get("acquisition_source")
                    enriched_order["cost_basis_source"] = advised_price_data.get("cost_basis_source")
                    enriched_order["price_difference_pct"] = advised_price_data.get("price_difference_pct")
                    # Profitability metrics
                    enriched_order["break_even_price"] = advised_price_data.get("break_even_price")
                    enriched_order["net_margin_pct_advised"] = advised_price_data.get("net_margin_pct_advised")
                    enriched_order["net_margin_pct_current"] = advised_price_data.get("net_margin_pct_current")
                    enriched_order["estimated_sell_days_advised"] = advised_price_data.get("estimated_sell_days_advised")
                    enriched_order["estimated_sell_days_current"] = advised_price_data.get("estimated_sell_days_current")
                    enriched_order["isk_per_day_advised"] = advised_price_data.get("isk_per_day_advised")
                    enriched_order["isk_per_day_current"] = advised_price_data.get("isk_per_day_current")
                    enriched_order["hold_signal"] = advised_price_data.get("hold_signal")
                    enriched_order["relist_risk"] = advised_price_data.get("relist_risk")

                enriched_orders.append(enriched_order)

        return enriched_orders
