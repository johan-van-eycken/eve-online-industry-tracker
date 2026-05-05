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
        return self._state.corp_manager.get_corporations()

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
