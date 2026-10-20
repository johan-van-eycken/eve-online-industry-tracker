from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.application.batch import PartialResult, batch_errors
from eve_online_industry_tracker.application.characters.realized_profit import (
    CorporationRealizedProfitLedgerService,
    ledger_rebuild_lock,
    summarize_realized_profit_rows,
)
from eve_online_industry_tracker.application.market_analysis.market_order_enrichment import (
    add_reprice_priority_scores,
    enrich_market_order,
    fetch_order_books,
    order_type_ids,
)
from eve_online_industry_tracker.application.market_pricing.order_values import refresh_market_history
from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS
from eve_online_industry_tracker.infrastructure.esi_versions import ESI_CORP_PUBLIC


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
                    ESI_CORP_PUBLIC.format(corporation_id=corp_id)
                ) or {}
                if not isinstance(corp_info, dict):
                    corp_info = {}
            except ESI_REQUEST_ERRORS as exc:
                # Display only: the entry falls back to the member's corporation name, no ticker.
                logging.warning("Public corporation info for %s unavailable: %s", corp_id, exc)

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

        buy_type_ids, sell_type_ids = order_type_ids(raw_corp_data)
        buy_order_book, sell_order_book = fetch_order_books(
            self._state.esi_service, buy_type_ids, sell_type_ids, label="Corporation market orders")
        # {type_id: why its market history was not refreshed}: the pricing
        # suggestion of those sell orders then uses the stored history.
        history_unavailable = refresh_market_history(
            market_history_svc, sell_type_ids, label="Corporation market orders")

        for corp_data in raw_corp_data:
            corp_orders = (corp_data or {}).get("market_orders", [])
            corp_name = (corp_data or {}).get("corporation_name")
            corp_id = int((corp_data or {}).get("corporation_id") or 0) or corporation_id

            for order in corp_orders:
                enriched_orders.append(enrich_market_order(
                    order,
                    now=now,
                    buy_order_book=buy_order_book,
                    sell_order_book=sell_order_book,
                    compare_with_books=True,
                    history_unavailable=history_unavailable,
                    pricing_svc=pricing_svc,
                    pricing_character_id=fallback_character_id,
                    corporation_id=corp_id,
                    owner=corp_name,
                    station=order.get("location_name") or f"Location {order.get('location_id', 'Unknown')}",
                    region_name=order.get("region_name"),
                    is_corporation=True,
                    price_label="Corporation market order",
                    pricing_failure_label="corporation order",
                ))

        add_reprice_priority_scores(enriched_orders)
        # The corporations whose orders could not be read are named, not left out silently.
        return PartialResult(enriched_orders, batch_errors(raw_corp_data))

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
        # Every rebuild holds the corporation's lock (per process only): two
        # at once race on UNIQUE(corporation_id, transaction_id). Without a
        # refresh the need is re-checked inside the lock: the other request
        # may have just rebuilt it. A refresh rebuilds, one after the other.
        if refresh or not rows:
            owner_ids = [int(corporation_id)] if corporation_id is not None else ledger_service.owner_ids()
            for owner_id in owner_ids:
                with ledger_rebuild_lock("corporation_id", owner_id):
                    if not refresh and ledger_service.list_rows(corporation_id=owner_id):
                        continue
                    ledger_service.rebuild(corporation_id=owner_id)
            rows = ledger_service.list_rows(corporation_id=corporation_id)
        else:
            # A lower-bound cost resolved since the rows were built (a
            # re-snapshotted job) makes the corporation's ledger stale.
            stale = ledger_service.stale_resolved_cost_owner_ids(rows)
            for stale_id in sorted(stale):
                with ledger_rebuild_lock("corporation_id", int(stale_id)):
                    current_rows = ledger_service.list_rows(corporation_id=int(stale_id))
                    if int(stale_id) in ledger_service.stale_resolved_cost_owner_ids(current_rows):
                        ledger_service.rebuild(corporation_id=int(stale_id))
            if stale:
                rows = ledger_service.list_rows(corporation_id=corporation_id)

        return {
            "rows": rows,
            "summary": summarize_realized_profit_rows(rows),
        }
