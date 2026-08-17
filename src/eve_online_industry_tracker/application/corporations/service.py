from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from eve_online_industry_tracker.application.characters.realized_profit import (
    CorporationRealizedProfitLedgerService,
    summarize_realized_profit_rows,
)


class CorporationsService:
    def __init__(self, *, state: Any):
        self._state = state

    def list_corporations(self) -> Any:
        """Return all corporations: full data for director-managed corps,
        lightweight entries for corps where we only have regular members."""
        # Full corporations (with director access)
        full_corps = self._state.corp_manager.get_corporations()
        full_corp_ids = {c["corporation_id"] for c in full_corps if c.get("corporation_id")}

        # Build lightweight entries for non-director corporations
        lightweight_corps: list[dict[str, Any]] = []
        char_manager = self._state.char_manager
        chars_by_corp: dict[int, list[dict[str, Any]]] = {}

        for char in char_manager._character_list:
            cid = char.corporation_id
            if cid is None or cid in full_corp_ids:
                continue
            if cid not in chars_by_corp:
                chars_by_corp[cid] = []
            chars_by_corp[cid].append({
                "character_id": char.character_id,
                "character_name": char.character_name,
                "character_wallet_balance": char.wallet_balance,
                "titles": None,
            })

        for corp_id, members in chars_by_corp.items():
            # Get corp name from the first member
            sample_char = next(
                (c for c in char_manager._character_list if c.corporation_id == corp_id),
                None,
            )
            corp_name = sample_char.corporation_name if sample_char else f"Corporation {corp_id}"

            # Try to fetch public corp info via ESI
            corp_info: dict[str, Any] = {}
            try:
                corp_info = self._state.esi_service._public_esi_get(
                    f"/corporations/{corp_id}/"
                ) or {}
                if not isinstance(corp_info, dict):
                    corp_info = {}
            except Exception:
                pass

            lightweight_corps.append({
                "corporation_id": corp_id,
                "corporation_name": corp_info.get("name") or corp_name,
                "ticker": corp_info.get("ticker", ""),
                "description": corp_info.get("description", ""),
                "member_count": corp_info.get("member_count"),
                "creator_id": corp_info.get("creator_id"),
                "ceo_id": corp_info.get("ceo_id"),
                "ceo_name": None,
                "home_station_id": corp_info.get("home_station_id"),
                "shares": corp_info.get("shares"),
                "tax_rate": corp_info.get("tax_rate"),
                "url": corp_info.get("url"),
                "war_eligible": corp_info.get("war_eligible"),
                "image_url": f"https://images.evetech.net/corporations/{corp_id}/logo?size=128",
                "date_founded": corp_info.get("date_founded"),
                "wallets": None,
                "standings": None,
                "wallet_journal": [],
                "wallet_transactions": [],
                "structures": [],
                "members": members,
                "assets": [],
                "updated_at": None,
                "has_director_access": False,
            })

        # Tag full corps so the UI knows they have full access
        for corp in full_corps:
            corp["has_director_access"] = True

        return full_corps + lightweight_corps

    def list_assets(
        self,
        *,
        corporation_id: int | None = None,
    ) -> Any:
        return self._state.corp_manager.get_assets(corporation_id=corporation_id)

    def get_market_orders(
        self,
        *,
        refresh: bool = False,
        corporation_id: int | None = None,
    ) -> list[dict]:
        return self._state.corp_manager.get_market_orders(
            corporation_id=corporation_id,
            refresh=refresh,
        )

    def get_market_orders_enriched(
        self,
        *,
        refresh: bool = False,
        corporation_id: int | None = None,
    ) -> list[dict[str, Any]]:
        from eve_online_industry_tracker.application.market_analysis.pricing_suggestion_service import PricingSuggestionService
        from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService

        raw_corp_data = self._state.corp_manager.get_market_orders(
            corporation_id=corporation_id, refresh=refresh
        )

        director = self._state.char_manager.get_corp_director() or self._state.char_manager.get_main_character()
        fallback_character_id: int | None = director.character_id if director else None

        now = datetime.now(timezone.utc)
        enriched_orders: list[dict[str, Any]] = []
        pricing_svc = PricingSuggestionService(state=self._state)
        market_history_svc = MarketHistoryService(state=self._state)

        sell_type_ids: set[int] = set()
        buy_type_ids: set[int] = set()
        for corp_data in raw_corp_data:
            for order in (corp_data or {}).get("market_orders", []):
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
        try:
            for type_id in sell_type_ids:
                market_history_svc.fetch_and_store_history(type_id=type_id, region_id=10000002)
        except Exception:
            pass

        for corp_data in raw_corp_data:
            corp_orders = (corp_data or {}).get("market_orders", [])
            corp_name = (corp_data or {}).get("corporation_name")
            corp_id = int((corp_data or {}).get("corporation_id") or 0) or corporation_id

            for order in corp_orders:
                issued_raw = order.get("issued")
                try:
                    issued_dt = datetime.fromisoformat(str(issued_raw).replace("Z", "+00:00"))
                except Exception:
                    issued_dt = now

                duration_days_int = 0
                try:
                    duration_days_int = int(order.get("duration") or 0)
                except Exception:
                    pass

                expires_dt = issued_dt + timedelta(days=duration_days_int)
                expires_in_td = expires_dt - now
                if expires_in_td.total_seconds() > 0:
                    days = expires_in_td.days
                    hours = expires_in_td.seconds // 3600
                    mins = (expires_in_td.seconds % 3600) // 60
                    expires_in = f"{days}d {hours}h {mins}m"
                else:
                    expires_in = "Expired"

                type_id = order.get("type_id")
                try:
                    order_price = float(order.get("price") or 0)
                except Exception:
                    order_price = 0.0

                price_difference = 0.0
                price_status = "⚪N/A"
                if isinstance(type_id, int):
                    if order.get("is_buy_order"):
                        prices_list = buy_order_book.get(type_id, [])
                        if prices_list and all(isinstance(o, dict) for o in prices_list):
                            try:
                                highest_price = max(o.get("price", 0) for o in prices_list)
                                price_difference = order_price - float(highest_price)
                                price_status = "🟢Best price" if price_difference > 0 else "🔴Undercut"
                            except Exception:
                                pass
                    else:
                        prices_list = sell_order_book.get(type_id, [])
                        if prices_list and all(isinstance(o, dict) for o in prices_list):
                            try:
                                lowest_price = min(o.get("price", 0) for o in prices_list)
                                price_difference = order_price - float(lowest_price)
                                price_status = "🔴Undercut" if price_difference > 0 else "🟢Best price"
                            except Exception:
                                pass

                advised_price_data = None
                if not order.get("is_buy_order") and isinstance(type_id, int) and fallback_character_id is not None:
                    days_remaining_int: int | None = max(0, expires_in_td.days) if expires_in_td.total_seconds() > 0 else None
                    try:
                        advised_price_data = pricing_svc.suggest_price(
                            character_id=fallback_character_id,
                            type_id=type_id,
                            current_price=order_price,
                            quantity=int(order.get("volume_remain") or 0),
                            order_duration_days=duration_days_int or 90,
                            days_remaining=days_remaining_int,
                            corporation_id=corp_id,
                        )
                    except Exception:
                        advised_price_data = None

                enriched_order: dict[str, Any] = {
                    "order_id": order.get("order_id"),
                    "owner": corp_name,
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
                    "station": order.get("location_name") or f"Location {order.get('location_id', 'Unknown')}",
                    "region": order.get("region_name") or (f"Region {order.get('region_id')}" if order.get("region_id") else "Unknown"),
                    "is_buy_order": order.get("is_buy_order"),
                    "is_corporation": True,
                    "corporation_id": corp_id,
                    "type_group_id": order.get("type_group_id", -1),
                    "type_group_name": order.get("type_group_name", "Unknown"),
                    "type_category_id": order.get("type_category_id", -1),
                    "type_category_name": order.get("type_category_name", "Unknown"),
                    "is_blueprint_copy": False,
                }

                if advised_price_data:
                    enriched_order["advised_price"] = advised_price_data.get("advised_price")
                    enriched_order["advised_price_confidence"] = advised_price_data.get("confidence")
                    enriched_order["pricing_breakdown"] = advised_price_data.get("breakdown")
                    enriched_order["pricing_reasoning"] = advised_price_data.get("reasoning")
                    enriched_order["cost_basis"] = advised_price_data.get("cost_basis")
                    enriched_order["acquisition_source"] = advised_price_data.get("acquisition_source")
                    enriched_order["cost_basis_source"] = advised_price_data.get("cost_basis_source")
                    enriched_order["price_difference_pct"] = advised_price_data.get("price_difference_pct")
                    enriched_order["hub_price"] = advised_price_data.get("hub_price")
                    enriched_order["break_even_price"] = advised_price_data.get("break_even_price")
                    enriched_order["net_margin_pct_advised"] = advised_price_data.get("net_margin_pct_advised")
                    enriched_order["net_margin_pct_current"] = advised_price_data.get("net_margin_pct_current")
                    enriched_order["estimated_sell_days_advised"] = advised_price_data.get("estimated_sell_days_advised")
                    enriched_order["estimated_sell_days_current"] = advised_price_data.get("estimated_sell_days_current")
                    enriched_order["isk_per_day_advised"] = advised_price_data.get("isk_per_day_advised")
                    enriched_order["isk_per_day_current"] = advised_price_data.get("isk_per_day_current")
                    enriched_order["hold_signal"] = advised_price_data.get("hold_signal")
                    enriched_order["relist_risk"] = advised_price_data.get("relist_risk")
                    enriched_order["price_band"] = advised_price_data.get("price_band")
                    enriched_order["min_target_margin_pct"] = advised_price_data.get("min_target_margin_pct")
                    enriched_order["fill_rate_velocity"] = advised_price_data.get("fill_rate_velocity")
                    enriched_order["seller_concentration"] = advised_price_data.get("seller_concentration")
                    enriched_order["expiry_urgency"] = advised_price_data.get("expiry_urgency")
                    enriched_order["expiry_urgency_detail"] = advised_price_data.get("expiry_urgency_detail")

                enriched_orders.append(enriched_order)

        for order in enriched_orders:
            if order.get("is_buy_order") or not order.get("advised_price"):
                order["reprice_priority_score"] = 0.0
                continue
            score = 0.0
            if (order.get("net_margin_pct_current") or 0) < 0:
                score += 100.0
            adv_day = order.get("isk_per_day_advised") or 0
            cur_day = order.get("isk_per_day_current") or 0
            if adv_day > cur_day and adv_day > 0:
                daily_gain_m = (adv_day - cur_day) / 1_000_000
                score += min(50.0, daily_gain_m * 5)
            if order.get("expiry_urgency"):
                score += 40.0
            if (order.get("relist_risk") or {}).get("at_risk"):
                score += 25.0
            price = float(order.get("price") or 0)
            advised = float(order.get("advised_price") or price)
            if price > 0 and advised < price:
                undercut_pct = (price - advised) / price * 100
                score += min(20.0, undercut_pct * 2)
            order["reprice_priority_score"] = round(score, 1)

        return enriched_orders

    def get_realized_profit_ledger(
        self,
        *,
        refresh: bool = False,
        corporation_id: int | None = None,
    ) -> dict[str, Any]:
        if refresh:
            self._state.corp_manager.refresh_realized_profit_inputs(corporation_id=corporation_id)

        market_prices = self._state.esi_service.get_market_prices()
        ledger_service = CorporationRealizedProfitLedgerService(
            app_session=self._state.db_app.session,
            sde_session=self._state.db_sde.session,
            market_prices=market_prices if isinstance(market_prices, list) else [],
        )

        rows = ledger_service.list_rows(corporation_id=corporation_id)
        if refresh or not rows:
            rows = ledger_service.rebuild(corporation_id=corporation_id)

        return {
            "rows": rows,
            "summary": summarize_realized_profit_rows(rows),
        }
