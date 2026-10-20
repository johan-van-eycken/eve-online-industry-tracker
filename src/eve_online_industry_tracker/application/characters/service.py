from __future__ import annotations

from dataclasses import asdict
import threading
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.application.batch import PartialResult, batch_errors
from eve_online_industry_tracker.application.characters.realized_profit import (
    CharacterRealizedProfitLedgerService,
    ledger_rebuild_lock,
    summarize_realized_profit_rows,
)
from eve_online_industry_tracker.application.market_analysis.pricing_suggestion_service import (
    PricingSuggestionService,
)
from eve_online_industry_tracker.application.market_analysis.market_history_service import (
    MarketHistoryService,
)
from eve_online_industry_tracker.application.market_analysis.market_order_enrichment import (
    add_reprice_priority_scores,
    enrich_market_order,
    fetch_order_books,
    order_type_ids,
)
from eve_online_industry_tracker.application.market_pricing.order_values import refresh_market_history
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
            buy_type_ids, sell_type_ids = order_type_ids(refreshed_data)
            buy_order_book, sell_order_book = fetch_order_books(
                self._state.esi_service, buy_type_ids, sell_type_ids, label="Market orders")
            # Fetch and cache market history for sell orders
            history_unavailable = refresh_market_history(market_history_svc, sell_type_ids, label="Market orders")

        for character in refreshed_data:
            for order in (character or {}).get("market_orders", []):
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

                character_id = order.get("character_id") or (character or {}).get("character_id")
                corporation_id = int((character or {}).get("corporation_id")) if order.get("is_corporation") and (character or {}).get("corporation_id") else None
                enriched_orders.append(enrich_market_order(
                    order,
                    now=now,
                    buy_order_book=buy_order_book,
                    sell_order_book=sell_order_book,
                    compare_with_books=include_orderbook_comparison,
                    history_unavailable=history_unavailable,
                    pricing_svc=pricing_svc,
                    pricing_character_id=character_id if isinstance(character_id, int) else None,
                    corporation_id=corporation_id,
                    owner=order.get("owner"),
                    station=(order.get("station_name") or order.get("location_name")
                             or f"Location {order.get('location_id', 'Unknown')}"),
                    region_name=region_name,
                    is_corporation=order.get("is_corporation", False),
                    price_label="Market order",
                    pricing_failure_label="order",
                ))

        # Compute repricing priority score for sell orders
        add_reprice_priority_scores(enriched_orders)
        # The owners whose orders could not be read are named, not left out silently.
        return PartialResult(enriched_orders, batch_errors(refreshed_data))
