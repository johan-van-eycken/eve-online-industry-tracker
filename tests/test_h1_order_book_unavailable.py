"""H1.4: a type whose order-book fetch failed (ESIService leaves it out of the
book and keeps the reason) has an unknown price and hub liquidity, None with
`price_unavailable_reason` / `liquidity_unavailable_reason`, never an empty
book read as 0 units; it is not cached. The industry service carries the
None through: the reason replaces the "thin market" note and the scoring
treats it as unknown. Fake values only."""
from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService

JITA_STATION, JITA_REGION = 60003760, 10000002
REASON = "ESI order book unavailable: fake 502"


class _Esi:
    def __init__(self, failed=None):
        self.failed = dict(failed or {})   # {(side, type_id): reason}

    def _book(self, side, type_ids):
        return {
            int(t): [{"location_id": JITA_STATION, "price": 5.0, "volume_remain": 100}]
            for t in type_ids if (side, int(t)) not in self.failed
        }

    def get_sell_order_book(self, type_ids, region_id=None):
        return self._book("sell", type_ids)

    def get_buy_order_book(self, type_ids, region_id=None):
        return self._book("buy", type_ids)

    def order_book_unavailable_reason(self, type_id, *, side="sell", region_id=None):
        assert region_id == JITA_REGION
        return self.failed.get((side, int(type_id)))


def _pricing(app_engine, esi):
    factory = sessionmaker(bind=app_engine, future=True)
    return MarketPricingService(
        state=SimpleNamespace(admin_settings=None, cfg_manager=None, esi_service=esi),
        sessions=SimpleNamespace(app_session=factory),
    )


def test_a_failed_fetch_gives_an_unknown_price_with_the_reason(app_engine):
    esi = _Esi({("sell", 35): REASON})
    out = _pricing(app_engine, esi).get_type_price_map(type_ids=[34, 35])
    assert out[34]["unit_price"] == 5.0 and out[34].get("price_unavailable_reason") is None
    assert out[35]["unit_price"] is None
    assert out[35]["price_unavailable_reason"] == REASON


def test_a_failed_price_fetch_is_not_cached(app_engine):
    esi = _Esi({("sell", 35): REASON})
    pricing = _pricing(app_engine, esi)
    pricing.get_type_price_map(type_ids=[35])
    esi.failed.clear()
    out = pricing.get_type_price_map(type_ids=[35])
    assert out[35]["unit_price"] == 5.0 and out[35]["cached"] is False


def test_a_type_with_no_orders_is_a_known_empty_book(app_engine):
    class _Empty(_Esi):
        def _book(self, side, type_ids):
            return {}
    out = _pricing(app_engine, _Empty()).get_hub_liquidity_map(type_ids=[36])
    assert out[36]["sell_volume_total"] == 0 and out[36]["liquidity_unavailable_reason"] is None


def test_a_failed_fetch_gives_unknown_liquidity_with_the_reason(app_engine):
    esi = _Esi({("sell", 35): REASON})
    out = _pricing(app_engine, esi).get_hub_liquidity_map(type_ids=[34, 35])
    assert out[34]["sell_volume_total"] == 100 and out[34]["liquidity_unavailable_reason"] is None
    assert out[35]["sell_volume_total"] is None and out[35]["sell_order_count"] is None
    assert out[35]["buy_volume_total"] == 100          # the buy side was fetched
    assert REASON in out[35]["liquidity_unavailable_reason"]
    assert "sell" in out[35]["liquidity_unavailable_reason"]


def test_a_failed_liquidity_fetch_is_not_cached(app_engine):
    esi = _Esi({("sell", 35): REASON})
    pricing = _pricing(app_engine, esi)
    pricing.get_hub_liquidity_map(type_ids=[35])
    esi.failed.clear()
    assert pricing.get_hub_liquidity_map(type_ids=[35])[35]["sell_volume_total"] == 100


# --- the industry service ---------------------------------------------------------------------

def _industry():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    svc = IndustryService.__new__(IndustryService)
    svc._state = SimpleNamespace(cfg_manager=None, esi_service=None, admin_settings=None)
    svc._sessions = None
    return svc


def test_the_market_activity_carries_unknown_liquidity_through(monkeypatch):
    from eve_online_industry_tracker.application.industry import service as service_module

    class _Pricing:
        def __init__(self, **kw):
            pass

        def get_region_daily_volume_map(self, *, type_ids, hub, unavailable=None):
            return {}

        def get_hub_liquidity_map(self, *, type_ids, hub):
            return {35: {"sell_volume_total": None, "sell_order_count": None, "buy_volume_total": 7,
                         "buy_order_count": 1, "liquidity_unavailable_reason": f"sell: {REASON}"}}

    monkeypatch.setattr(service_module, "MarketPricingService", _Pricing)
    (row,) = _industry()._enrich_product_rows_with_market_activity(
        [{"type_id": 35, "manufacturing_job": {"runs": 1}}], market_hub="jita")
    assert row["hub_sell_liquidity"] is None and row["hub_sell_order_count"] is None
    assert row["hub_buy_liquidity"] == 7
    assert row["liquidity_unavailable_reason"] == f"sell: {REASON}"
    assert row["manufacturing_job"]["hub_sell_liquidity"] is None
    assert row["manufacturing_job"]["liquidity_unavailable_reason"] == f"sell: {REASON}"


def test_unknown_liquidity_has_unknown_metrics():
    (row,) = _industry()._enrich_product_rows_with_liquidity_metrics([{
        "type_id": 35, "region_daily_volume_7d_avg": 50.0, "liquidity_unavailable_reason": REASON,
        "manufacturing_job": {"hub_sell_liquidity": None, "hub_sell_order_count": None,
                              "region_daily_volume_7d_avg": 50.0, "runs": 1},
    }])
    assert row["days_of_supply"] is None
    assert row["sell_through_rate"] is None
    assert row["liquidity_indicator"] == "Unknown"
    assert row["liquidity_score"] is None


def test_the_reason_replaces_the_thin_market_note(monkeypatch, tmp_path):
    from eve_online_industry_tracker.application.industry import service as service_module

    class _Pricing:
        def __init__(self, **kw):
            pass

        def _market_hub_context(self, hub):
            return {"region_id": JITA_REGION}

    class _History:
        def get_price_stats(self, **kw):
            return {"has_data": True, "avg_7d": 10.0, "avg_42w": 10.0}

        def fetch_and_store_history(self, **kw):
            return None

    monkeypatch.setattr(service_module, "MarketPricingService", _Pricing)
    monkeypatch.setattr(service_module, "MarketHistoryService", lambda **kw: _History())
    (row,) = _industry()._enrich_product_rows_with_price_anomaly(
        [{"type_id": 35, "market_unit_price": 10.0, "hub_sell_order_count": None,
          "liquidity_unavailable_reason": REASON}], market_hub="jita")
    reasons = row["price_anomaly_reasons"]
    assert not any("thin market" in r for r in reasons)
    assert any(REASON in r for r in reasons)


def test_the_pricing_confidence_names_unknown_liquidity():
    (row,) = _industry()._enrich_product_rows_with_pricing_confidence([{
        "type_id": 35, "market_unit_price": 10.0, "market_price_sample_size": 5,
        "market_price_fetched_at": 4102444800.0, "hub_sell_liquidity": None, "hub_sell_order_count": None,
        "liquidity_unavailable_reason": REASON, "region_daily_volume": 30, "region_daily_volume_7d_avg": 30.0,
        "manufacturing_job": {"runs": 1},
    }], product_price_side="sell")
    reasons = row["pricing_confidence_reasons"]
    assert any("unknown" in r and REASON in r for r in reasons)
    assert not any("No sell-side hub liquidity" in r for r in reasons)


def test_the_portfolio_candidate_keeps_unknown_liquidity_unknown():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    candidate = IndustryService._build_portfolio_candidate({
        "type_id": 35, "type_name": "Unknown Liquidity", "market_unit_price": 1_500_000.0,
        "market_price_sample_size": 5, "market_price_fetched_at": 4102444800.0,
        "hub_sell_liquidity": None, "hub_buy_liquidity": None, "hub_sell_order_count": None,
        "liquidity_unavailable_reason": REASON,
        "region_daily_volume": 100, "region_daily_volume_7d_avg": 100.0, "manufacturing_job": {"runs": 1},
    }, planning_horizon_hours=24.0)
    assert candidate["hub_sell_liquidity"] is None
    assert candidate["liquidity_unavailable_reason"] == REASON
    assert candidate["days_of_supply"] is None
    assert candidate["liquidity_score"] is None
