"""The market-trend step of the product overview: relist cost, effective profit and the delivery projection."""
from __future__ import annotations

from types import MethodType, SimpleNamespace

import pytest

from eve_online_industry_tracker.application.industry import service as service_module
from eve_online_industry_tracker.application.industry.service import IndustryService
from industry_service_fakes import _blueprint_row, _build_service


class _History:
    def __init__(self, stats=None):
        self._stats = stats or {}

    def get_price_stats(self, *, type_id, **kw):
        return self._stats.get(type_id, {"has_data": False})


class _Pricing:
    def __init__(self, **kw):
        pass

    def _market_hub_context(self, hub):
        return {"region_id": 10000002}


def _fee_context(*, broker=0.012, applies=True):
    return {"rates": {"sales_tax_fraction": 0.0, "broker_fee_fraction": broker}, "broker_fee_applies": applies}


def _trend_service(app_session):
    return IndustryService(state=SimpleNamespace(esi_service=None, admin_settings=None),
                           sessions=SimpleNamespace(app_session=lambda: app_session))


def _priced_row(*, fee_context, units=1000, daily_volume=1.0, price=100.0, total_cost=50_000.0):
    """A row as the steps before the trends leave it: sale proceeds and profit already set."""
    return {
        "type_id": 5001,
        "market_unit_price": price,
        "quantity": units,
        "region_daily_volume_7d_avg": daily_volume,
        "manufacturing_job": {"runs": units, "product_quantity_per_run": 1, "total_cost": total_cost,
                              "time_seconds": 3600, "market_fee_context": fee_context},
    }


def _enriched(svc, row, history=None):
    rows = svc._enrich_product_rows_with_sale_proceeds([row], character_id=None, market_hub="jita",
                                                        product_price_side="sell",
                                                        fee_context=row["manufacturing_job"]["market_fee_context"])
    rows = svc._enrich_product_rows_with_profit_metrics(rows)
    svc._enrich_product_rows_with_market_trends(rows, market_history_svc=history or _History())
    return rows[0]


def test_the_overview_fills_profit_after_relist(monkeypatch, app_session):
    # The trend step reads profit_amount and net_proceeds, so it must run after the profit step.
    service = _build_service(
        blueprint_rows=[_blueprint_row()], profile=None, character_modifiers=None, trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )
    del service._enrich_product_rows_with_market_trends
    del service._enrich_product_rows_with_sale_proceeds
    service._sessions = SimpleNamespace(app_session=lambda: app_session)

    def priced(self, product_rows, **kw):
        for r in product_rows:
            r["market_unit_price"], r["manufacturing_job"]["total_cost"] = 100.0, 60.0
        return product_rows

    service._enrich_product_rows_with_material_prices = MethodType(priced, service)
    monkeypatch.setattr(service_module, "MarketHistoryService", lambda **kw: _History())

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=False, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1)

    assert rows and rows[0]["profit_amount"] == pytest.approx(40.0)
    assert rows[0]["effective_profit_with_relist"] == pytest.approx(rows[0]["profit_amount"])


def test_the_relist_cost_uses_the_rows_own_broker_fee(app_session):
    # 1000 units at 1/day: 1000 selling days, so 11 relists of 90 days at the row's 1.2% broker fee.
    row = _enriched(_trend_service(app_session), _priced_row(fee_context=_fee_context(broker=0.012)))
    assert row["relist_broker_fee_assumed"] is None
    assert row["relist_cost_per_unit"] == pytest.approx(100.0 * 0.012 * 11)
    assert row["effective_profit_with_relist"] == pytest.approx(row["profit_amount"] - 100.0 * 0.012 * 11 * 1000)
    assert row["effective_margin_with_relist_pct"] is not None


def test_selling_into_buy_orders_has_no_relist_cost(app_session):
    row = _enriched(_trend_service(app_session), _priced_row(fee_context=_fee_context(applies=False)))
    assert row["relist_cost_per_unit"] == 0.0
    assert row["effective_profit_with_relist"] == pytest.approx(row["profit_amount"])


def test_the_delivery_projection_uses_the_7d_vs_30d_trend(app_session):
    # 7d avg 11.5% above the 30d avg: 1%/day over the ~11.5 days between their midpoints.
    # The 42-week trend (+50%) must not drive it.
    history = _History({5001: {"has_data": True, "trend_pct": 50.0, "trend_30d_pct": 11.5}})
    row = _priced_row(fee_context=_fee_context())
    row["manufacturing_job"]["manufacturing_time_seconds"] = 2 * 86400
    row = _enriched(_trend_service(app_session), row, history)
    assert row["projected_price_at_delivery"] == pytest.approx(100.0 * 1.02)


def test_no_projection_without_30_days_of_history(app_session):
    history = _History({5001: {"has_data": True, "trend_pct": 50.0, "trend_30d_pct": None}})
    row = _priced_row(fee_context=_fee_context())
    row["manufacturing_job"]["manufacturing_time_seconds"] = 86400
    row = _enriched(_trend_service(app_session), row, history)
    assert row["projected_price_at_delivery"] is None and row["projected_margin_pct"] is None


def test_pipeline_and_realized_margin_are_summed_per_type_over_character_and_corp(app_session):
    from datetime import datetime, timedelta

    from eve_online_industry_tracker.db_models import (
        CharacterIndustryJobsModel, CharacterRealizedSalesLedgerModel, CorporationIndustryJobsModel,
        CorporationRealizedSalesLedgerModel,
    )
    from eve_online_industry_tracker.infrastructure.models import (
        CharacterMarketOrdersModel, CorporationMarketOrdersModel,
    )

    recent = (datetime.utcnow() - timedelta(days=10)).strftime("%Y-%m-%d")
    old = (datetime.utcnow() - timedelta(days=400)).strftime("%Y-%m-%d")
    app_session.add_all([
        CharacterIndustryJobsModel(character_id=1, job_id=1, product_type_id=5001, output_quantity=10, status="active"),
        CharacterIndustryJobsModel(character_id=1, job_id=2, product_type_id=5001, output_quantity=99, status="delivered"),
        CorporationIndustryJobsModel(corporation_id=9, job_id=3, product_type_id=5001, output_quantity=5, status="ready"),
        CharacterIndustryJobsModel(character_id=1, job_id=4, product_type_id=5002, output_quantity=7, status="active"),
        CharacterMarketOrdersModel(character_id=1, order_id=1, type_id=5001, volume_remain=20, is_buy_order=False),
        CharacterMarketOrdersModel(character_id=1, order_id=2, type_id=5001, volume_remain=99, is_buy_order=True),
        CorporationMarketOrdersModel(corporation_id=9, order_id=3, type_id=5001, volume_remain=30, is_buy_order=False),
        CharacterRealizedSalesLedgerModel(character_id=1, transaction_id=1, quantity=1, type_id=5001, date=recent,
                                          confidence="High", realized_margin_fraction=0.10),
        CorporationRealizedSalesLedgerModel(corporation_id=9, transaction_id=2, quantity=1, type_id=5001, date=recent,
                                            confidence="Medium", realized_margin_fraction=0.30),
        CharacterRealizedSalesLedgerModel(character_id=1, transaction_id=3, quantity=1, type_id=5001, date=recent,
                                          confidence="Low", realized_margin_fraction=0.90),
        CharacterRealizedSalesLedgerModel(character_id=1, transaction_id=4, quantity=1, type_id=5001, date=old,
                                          confidence="High", realized_margin_fraction=0.90),
        CharacterRealizedSalesLedgerModel(character_id=1, transaction_id=5, quantity=1, type_id=5002, date=recent,
                                          confidence="High", realized_margin_fraction=0.50),
    ])
    app_session.commit()
    rows = [{"type_id": 5001, "manufacturing_job": {}}, {"type_id": 5002, "manufacturing_job": {}},
            {"type_id": 5001, "manufacturing_job": {}}]

    _trend_service(app_session)._enrich_product_rows_with_market_trends(rows, market_history_svc=_History())

    first, second, duplicate = rows
    assert (first["pipeline_units_in_jobs"], first["pipeline_units_on_market"]) == (15, 50)
    assert first["historical_realized_margin_pct"] == pytest.approx(20.0)
    assert duplicate["pipeline_total_units"] == 65
    assert (second["pipeline_units_in_jobs"], second["pipeline_units_on_market"]) == (7, 0)
    assert second["historical_realized_margin_pct"] is None  # one sale: too few to average


def test_the_price_anomaly_reads_known_history_once(monkeypatch):
    from eve_online_industry_tracker.application.industry import market_enrichment

    reads = []

    class _CountingHistory:
        def get_price_stats(self, *, type_id, **kw):
            reads.append(type_id)
            return {"has_data": type_id == 34 or reads.count(type_id) > 1, "avg_7d": 5.0}

        def fetch_and_store_history(self, **kw):
            return None

    monkeypatch.setattr(market_enrichment, "MarketPricingService", _Pricing)
    monkeypatch.setattr(market_enrichment, "MarketHistoryService", lambda **kw: _CountingHistory())
    svc = IndustryService(state=SimpleNamespace(esi_service=None, admin_settings=None), sessions=None)

    rows = svc._enrich_product_rows_with_price_anomaly(
        [{"type_id": 34, "market_unit_price": 5.0}, {"type_id": 35, "market_unit_price": 5.0}], market_hub="jita")

    assert sorted(reads) == [34, 35, 35]  # 35 had no history: read, fetched, read again
    assert [r["history_7d_avg"] for r in rows] == [5.0, 5.0]
