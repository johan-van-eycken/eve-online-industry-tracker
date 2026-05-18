from __future__ import annotations

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
