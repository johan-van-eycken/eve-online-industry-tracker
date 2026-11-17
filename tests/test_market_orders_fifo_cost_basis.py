"""A market order's cost basis is the owner's next FIFO lot in the realized-profit
ledger; without one the old chain applies. Fake ids."""
from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.application.characters import realized_profit as rp
from eve_online_industry_tracker.application.market_analysis.pricing_suggestion_service import PricingSuggestionService
from eve_online_industry_tracker.infrastructure.models import (
    CorporationAssetHistoryModel,
    CorporationIndustryJobsModel,
    CorporationModel,
)

CORP, MODULE, OTHER = 50, 12200, 12201


def _seed(app_session, *, with_job: bool = True) -> None:
    app_session.add(CorporationModel(corporation_id=CORP, corporation_name="Corp"))
    app_session.add(CorporationAssetHistoryModel(
        corporation_id=CORP, item_id=1, observed_at="2026-11-10T00:00:00Z", type_id=MODULE, quantity=40,
        acquisition_source="industry_build", acquisition_unit_cost=16_600_000.0))
    if with_job:
        app_session.add(CorporationIndustryJobsModel(
            corporation_id=CORP, job_id=675169231, status="delivered", activity_id=1,
            start_date="2026-10-10T00:00:00Z", end_date="2026-10-14T00:00:00Z",
            completed_date="2026-10-14T00:00:00Z", blueprint_type_id=MODULE + 1000, product_type_id=MODULE,
            runs=40, successful_runs=40, output_quantity=40, unit_build_cost=13_540_000.0))
    app_session.commit()


def _service(session_provider) -> PricingSuggestionService:
    return PricingSuggestionService(state=SimpleNamespace(), sessions=session_provider)


def test_a_corp_order_takes_the_next_fifo_lot_over_the_asset_history(session_provider) -> None:
    _seed(session_provider.app_session())

    assert _service(session_provider)._get_cost_basis(character_id=1, type_id=MODULE, corporation_id=CORP) == (
        13_540_000.0, "manufactured", "ledger_fifo", "fifo: industry job 675169231")


def test_without_a_fifo_lot_the_old_chain_applies(session_provider) -> None:
    _seed(session_provider.app_session(), with_job=False)

    cost, acquisition, source, detail = _service(session_provider)._get_cost_basis(
        character_id=1, type_id=MODULE, corporation_id=CORP)

    assert (cost, acquisition, source) == (16_600_000.0, "industry_build", "corp_asset_history")
    assert detail and "no open lot" in detail


def test_the_fifo_is_read_once_per_owner_per_request(session_provider, monkeypatch) -> None:
    _seed(session_provider.app_session())
    calls: list[tuple] = []
    real = rp.CorporationRealizedProfitLedgerService.next_fifo_unit_costs

    def counted(self, **kwargs):
        calls.append(tuple(sorted(kwargs.items())))
        return real(self, **kwargs)

    monkeypatch.setattr(rp.CorporationRealizedProfitLedgerService, "next_fifo_unit_costs", counted)
    service = _service(session_provider)

    for type_id in (MODULE, OTHER, MODULE):
        service._get_cost_basis(character_id=1, type_id=type_id, corporation_id=CORP)

    assert calls == [(("owner_id", CORP),)]


def test_a_failing_fifo_lookup_falls_back_to_the_old_chain(session_provider, monkeypatch, caplog) -> None:
    _seed(session_provider.app_session())

    def broken(self, **kwargs):
        raise OperationalError("SELECT", {}, Exception("database is locked"))

    monkeypatch.setattr(rp.CorporationRealizedProfitLedgerService, "next_fifo_unit_costs", broken)

    cost, _, source, _ = _service(session_provider)._get_cost_basis(character_id=1, type_id=MODULE, corporation_id=CORP)

    assert (cost, source) == (16_600_000.0, "corp_asset_history")
    assert "FIFO cost basis unavailable" in caplog.text
