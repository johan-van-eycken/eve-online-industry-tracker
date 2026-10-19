from __future__ import annotations

from dataclasses import asdict
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from eve_online_industry_tracker.application.characters.realized_profit import (
    CharacterRealizedProfitLedgerService,
    ledger_rebuild_lock,
    summarize_realized_profit_rows,
)
from eve_online_industry_tracker.config.admin_settings import AdminSettingInvalid
from eve_online_industry_tracker.application.market_analysis.pricing_suggestion_service import (
    PricingSuggestionService,
)
from eve_online_industry_tracker.application.market_analysis.market_history_service import (
    MarketHistoryService,
)
from eve_online_industry_tracker.application.market_pricing.order_values import (
    NA_STATUS,
    book_comparison,
    order_price_or_reason,
    order_total_price,
    readable_int,
    readable_number,
    refresh_market_history,
)
from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS
from eve_online_industry_tracker.infrastructure.oauth_character_repository import (
    OAuthCharacterRepository,
)


def _ledger_rebuild_lock(character_id: int) -> threading.Lock:
    """The one lock per character for a ledger rebuild (per process only:
    realized_profit.ledger_rebuild_lock)."""
    return ledger_rebuild_lock("character_id", int(character_id))


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
            admin_settings=getattr(self._state, "admin_settings", None),
        )

        rows = ledger_service.list_rows(character_id=character_id)
        if refresh or not rows:
            # Every rebuild holds the character's lock (two at once race on
            # UNIQUE(character_id, transaction_id)). Without a refresh the
            # emptiness is re-checked inside: the other request may have
            # just built it. A refresh rebuilds, one after the other.
            owner_ids = [int(character_id)] if character_id is not None else ledger_service.owner_ids()
            for owner_id in owner_ids:
                with _ledger_rebuild_lock(owner_id):
                    if not refresh and ledger_service.list_rows(character_id=owner_id):
                        continue
                    ledger_service.rebuild(character_id=owner_id)
            rows = ledger_service.list_rows(character_id=character_id)
        else:
            # A character whose rows predate a fee settings, skills or standings change, or
            # whose lower-bound cost has since been resolved (a re-snapshotted job), is rebuilt
            # here (local data only, no ESI), so the persisted ledger never shows stale fees.
            stale = ledger_service.stale_character_ids(rows) | ledger_service.stale_resolved_cost_owner_ids(rows)
            for stale_character_id in sorted(stale):
                # Two requests can find the same character stale; two rebuilds
                # race on UNIQUE(character_id, transaction_id). One rebuilds,
                # the other re-checks under the lock and finds it fresh.
                with _ledger_rebuild_lock(int(stale_character_id)):
                    current_rows = ledger_service.list_rows(character_id=int(stale_character_id))
                    still_stale = (
                        ledger_service.stale_character_ids(current_rows)
                        | ledger_service.stale_resolved_cost_owner_ids(current_rows)
                    )
                    if int(stale_character_id) in still_stale:
                        ledger_service.rebuild(character_id=stale_character_id)
            if stale:
                rows = ledger_service.list_rows(character_id=character_id)

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
        # {type_id: why its market history was not refreshed}: the pricing
        # suggestion of those sell orders then uses the stored history.
        history_unavailable: dict[int, str] = {}

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
            except AdminSettingInvalid:
                raise  # a stored invalid setting fails loudly, naming it
            except ESI_REQUEST_ERRORS as exc:
                # The orders still show, their price status "N/A" (no book to compare with).
                logging.warning("Market orders: buy order book unavailable, no price comparison: %s", exc)
                buy_order_book = {}

            try:
                if sell_type_ids:
                    sell_order_book = self._state.esi_service.get_type_sellprices(sorted(sell_type_ids))
            except AdminSettingInvalid:
                raise  # a stored invalid setting fails loudly, naming it
            except ESI_REQUEST_ERRORS as exc:
                logging.warning("Market orders: sell order book unavailable, no price comparison: %s", exc)
                sell_order_book = {}

            # Fetch and cache market history for sell orders
            history_unavailable = refresh_market_history(market_history_svc, sell_type_ids, label="Market orders")

        for character in refreshed_data:
            for order in (character or {}).get("market_orders", []):
                issued_raw = order.get("issued")
                try:
                    issued_dt = datetime.fromisoformat(str(issued_raw).replace("Z", "+00:00"))
                except ValueError:
                    issued_dt = now

                duration_days = order.get("duration")
                try:
                    duration_days_int = int(duration_days)
                except (TypeError, ValueError):
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

                price_difference: float | None = 0.0
                price_status = NA_STATUS

                type_id = order.get("type_id")
                order_price, price_unknown_reason = order_price_or_reason(order, label="Market order")
                if order_price is None:
                    price_difference = None

                if include_orderbook_comparison and isinstance(type_id, int):
                    book = (buy_order_book if order.get("is_buy_order") else sell_order_book)
                    price_status, price_difference = book_comparison(
                        order_price=order_price,
                        book=book.get(type_id, []) if isinstance(book, dict) else [],
                        is_buy_order=bool(order.get("is_buy_order")),
                    )

                # Best-effort: older cached orders may have region_id but no region_name
                # due to ID classification/enrichment issues at the time they were stored.
                region_name = order.get("region_name")
                if (not region_name) and isinstance(order.get("region_id"), int):
                    try:
                        region_info = self._state.esi_service.get_location_info(int(order.get("region_id")))
                        if isinstance(region_info, dict):
                            region_name = region_info.get("name")
                    except ESI_REQUEST_ERRORS:
                        region_name = None  # display only: falls back to "Region <id>"

                # Get pricing suggestion for sell orders
                advised_price_data = None
                pricing_error: str | None = None
                character_id = order.get("character_id") or (character or {}).get("character_id")
                corporation_id = int((character or {}).get("corporation_id")) if order.get("is_corporation") and (character or {}).get("corporation_id") else None
                if (not order.get("is_buy_order") and isinstance(type_id, int) and isinstance(character_id, int)
                        and order_price is not None):   # no suggestion from an unknown price
                    days_remaining_int: int | None = None
                    if expires_in_td.total_seconds() > 0:
                        days_remaining_int = max(0, expires_in_td.days)
                    try:
                        advised_price_data = pricing_svc.suggest_price(
                            character_id=character_id,
                            type_id=type_id,
                            current_price=order_price,
                            quantity=int(readable_int(order.get("volume_remain")) or 0),
                            order_duration_days=duration_days_int or 90,
                            days_remaining=days_remaining_int,
                            corporation_id=corporation_id,
                        )
                    except AdminSettingInvalid:
                        raise  # a stored invalid setting fails loudly, naming it
                    except (SQLAlchemyError, ValueError, ArithmeticError) as exc:
                        logging.warning(
                            "Pricing suggestion failed for order %s (type_id=%s): %s",
                            order.get("order_id"),
                            type_id,
                            exc,
                        )
                        advised_price_data = None
                        pricing_error = f"pricing suggestion unavailable: {exc}"

                enriched_order = {
                    "order_id": order.get("order_id"),
                    "owner": order.get("owner"),
                    "type_id": type_id,
                    "type_name": order.get("type_name"),
                    "price": order.get("price") if order_price is not None else None,
                    "price_unknown_reason": price_unknown_reason,
                    "history_unavailable_reason": (
                        history_unavailable.get(type_id)
                        if not order.get("is_buy_order") and isinstance(type_id, int) else None
                    ),
                    "price_status": price_status,
                    "price_difference": price_difference,
                    "volume": str(order.get("volume_remain")) + "/" + str(order.get("volume_total")),
                    "total_price": order_total_price(order_price, order),
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
                    "is_corporation": order.get("is_corporation", False),
                    "corporation_id": corporation_id,
                    "type_group_id": order.get("type_group_id", -1),
                    "type_group_name": order.get("type_group_name", "Unknown"),
                    "type_category_id": order.get("type_category_id", -1),
                    "type_category_name": order.get("type_category_name", "Unknown"),
                    "is_blueprint_copy": False,
                }

                if pricing_error:
                    enriched_order["pricing_error"] = pricing_error

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

        # Compute repricing priority score for sell orders
        for order in enriched_orders:
            if order.get("is_buy_order") or not order.get("advised_price"):
                order["reprice_priority_score"] = 0.0
                continue
            score = 0.0
            # Critical: losing money after fees
            if (order.get("net_margin_pct_current") or 0) < 0:
                score += 100.0
            # ISK/day improvement opportunity
            adv_day = order.get("isk_per_day_advised") or 0
            cur_day = order.get("isk_per_day_current") or 0
            if adv_day > cur_day and adv_day > 0:
                daily_gain_m = (adv_day - cur_day) / 1_000_000
                score += min(50.0, daily_gain_m * 5)
            # Expiry urgency
            if order.get("expiry_urgency"):
                score += 40.0
            # Relist risk
            if (order.get("relist_risk") or {}).get("at_risk"):
                score += 25.0
            # Being undercut at current price
            price = readable_number(order.get("price")) or 0.0
            advised = readable_number(order.get("advised_price")) or price
            if price > 0 and advised < price:
                undercut_pct = (price - advised) / price * 100
                score += min(20.0, undercut_pct * 2)
            order["reprice_priority_score"] = round(score, 1)

        return enriched_orders
