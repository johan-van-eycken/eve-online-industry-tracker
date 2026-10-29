"""G1 item 3b: MarketHistoryService.fetch_and_store_history returns why a type
has no fresh history (the ESI fetch failed, no ESI service), None on success,
and its callers show it: the price anomaly names it on the row instead of
silently skipping the historical check, and refresh_market_history returns
the reasons by type. Fake values throughout."""
from __future__ import annotations

import logging
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService
from eve_online_industry_tracker.application.market_pricing.order_values import refresh_market_history
from eve_online_industry_tracker.infrastructure.models import BaseApp, MarketHistoryModel

TYPE, REGION = 34, 10000002
GOOD = {"date": "2026-10-01", "average": 5.0, "highest": 6.0, "lowest": 4.0, "volume": 100, "order_count": 9}
REASON = "ESI market history unavailable: fake 502"


class _Esi:
    def __init__(self, history, failed=None, with_reason=True):
        self._history = history
        self._failed = failed or {}
        if with_reason:
            self.market_history_unavailable_reason = self._reason

    def get_market_history(self, type_ids, region_id=None):
        return {t: v for t, v in self._history.items() if t not in self._failed}

    def _reason(self, type_id, *, region_id=None):
        assert region_id == REGION
        return self._failed.get(int(type_id))


def _service(app_engine, esi):
    factory = sessionmaker(bind=app_engine, future=True)
    return MarketHistoryService(state=SimpleNamespace(esi_service=esi),
                                sessions=SimpleNamespace(app_session=factory)), factory


def _stored(factory):
    s = factory()
    try:
        return s.query(MarketHistoryModel).count()
    finally:
        s.close()


def test_a_stored_history_returns_no_reason(app_engine):
    svc, factory = _service(app_engine, _Esi({TYPE: [GOOD]}))
    assert svc.fetch_and_store_history(type_id=TYPE, region_id=REGION) is None
    assert _stored(factory) == 1


def test_a_real_empty_history_is_not_a_failure(app_engine):
    svc, _factory = _service(app_engine, _Esi({TYPE: []}))
    assert svc.fetch_and_store_history(type_id=TYPE, region_id=REGION) is None


def test_a_failed_fetch_returns_the_esi_reason_with_a_warning(app_engine, caplog):
    svc, factory = _service(app_engine, _Esi({TYPE: [GOOD]}, failed={TYPE: REASON}))
    with caplog.at_level(logging.WARNING):
        assert svc.fetch_and_store_history(type_id=TYPE, region_id=REGION) == REASON
    assert _stored(factory) == 0
    assert REASON in caplog.text and "type_id=34" in caplog.text


def test_a_type_left_out_without_a_known_reason_still_gets_one(app_engine):
    svc, _factory = _service(app_engine, _Esi({}, with_reason=False))
    reason = svc.fetch_and_store_history(type_id=TYPE, region_id=REGION)
    assert reason and "34" in reason


def test_no_esi_service_is_a_reason(app_engine):
    svc, _factory = _service(app_engine, None)
    assert "ESI" in svc.fetch_and_store_history(type_id=TYPE, region_id=REGION)


def test_refresh_market_history_returns_the_reasons_by_type(caplog):
    class _Svc:
        def fetch_and_store_history(self, *, type_id, region_id):
            return REASON if type_id == 34 else None

    with caplog.at_level(logging.WARNING):
        assert refresh_market_history(_Svc(), {34, 35}, label="Market orders") == {34: REASON}


def test_the_price_anomaly_names_a_failed_history_fetch(tmp_path, monkeypatch):
    from eve_online_industry_tracker.application.industry import market_enrichment
    from eve_online_industry_tracker.application.industry.service import IndustryService

    engine = create_engine(f"sqlite:///{tmp_path / 'app.db'}", future=True)   # the fetch runs on a worker thread
    BaseApp.metadata.create_all(engine)

    class _Pricing:
        def __init__(self, **kw):
            pass

        def _market_hub_context(self, hub):
            return {"region_id": REGION}

    history, _factory = _service(engine, _Esi({TYPE: [GOOD]}, failed={TYPE: REASON}))
    monkeypatch.setattr(market_enrichment, "MarketPricingService", _Pricing)
    monkeypatch.setattr(market_enrichment, "MarketHistoryService", lambda **kw: history)
    svc = IndustryService.__new__(IndustryService)
    svc._state = SimpleNamespace(esi_service=None, admin_settings=None)
    svc._sessions = None
    (row,) = svc._enrich_product_rows_with_price_anomaly(
        [{"type_id": TYPE, "market_unit_price": 10.0, "hub_sell_order_count": 9}], market_hub="jita")
    assert row["price_history_unavailable_reason"] == REASON
    assert any(REASON in r for r in row["price_anomaly_reasons"])
    engine.dispose()


def test_the_price_anomaly_with_a_history_has_no_unavailable_reason(tmp_path, monkeypatch):
    from eve_online_industry_tracker.application.industry import market_enrichment
    from eve_online_industry_tracker.application.industry.service import IndustryService

    engine = create_engine(f"sqlite:///{tmp_path / 'app.db'}", future=True)
    BaseApp.metadata.create_all(engine)

    class _Pricing:
        def __init__(self, **kw):
            pass

        def _market_hub_context(self, hub):
            return {"region_id": REGION}

    history, _factory = _service(engine, _Esi({TYPE: [{**GOOD, "date": "2099-01-01"}]}))
    monkeypatch.setattr(market_enrichment, "MarketPricingService", _Pricing)
    monkeypatch.setattr(market_enrichment, "MarketHistoryService", lambda **kw: history)
    svc = IndustryService.__new__(IndustryService)
    svc._state = SimpleNamespace(esi_service=None, admin_settings=None)
    svc._sessions = None
    (row,) = svc._enrich_product_rows_with_price_anomaly(
        [{"type_id": TYPE, "market_unit_price": 5.0, "hub_sell_order_count": 9}], market_hub="jita")
    assert row["price_history_unavailable_reason"] is None
    assert row["price_anomaly_reasons"] == []
    engine.dispose()


# --- item 3a: the region daily volume map -------------------------------------------------------

def _volume_map(esi, type_ids, unavailable=None):
    from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService

    MarketPricingService._region_volume_cache.clear()
    svc = MarketPricingService(state=SimpleNamespace(admin_settings=None, cfg_manager=None, esi_service=esi))
    return svc.get_region_daily_volume_map(type_ids=type_ids, unavailable=unavailable)


def test_a_failed_history_fetch_is_absent_with_a_reason_not_a_volume_of_0():
    from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService

    esi = _Esi({34: [GOOD], 35: [GOOD]}, failed={35: REASON})
    reasons: dict[int, str] = {}
    out = _volume_map(esi, [34, 35], reasons)
    assert out[34]["daily_volume"] == 100
    assert 35 not in out
    assert reasons == {35: REASON}
    # not cached: the next call fetches it again
    assert 35 not in MarketPricingService._region_volume_cache["jita:10000002"][1]
    MarketPricingService._region_volume_cache.clear()


def test_a_real_empty_history_is_still_a_known_volume():
    reasons: dict[int, str] = {}
    out = _volume_map(_Esi({34: []}), [34], reasons)
    assert out[34]["daily_volume_7d_sample_size"] == 0
    assert reasons == {}


def test_no_esi_service_gives_every_type_a_reason():
    reasons: dict[int, str] = {}
    assert _volume_map(None, [34, 35], reasons) == {}
    assert set(reasons) == {34, 35}


def test_the_volume_map_still_works_without_an_unavailable_dict():
    assert set(_volume_map(_Esi({34: [GOOD]}, failed={35: REASON}), [34, 35])) == {34}


def test_the_market_activity_shows_an_unknown_region_volume_as_unknown(monkeypatch):
    from eve_online_industry_tracker.application.industry import market_enrichment
    from eve_online_industry_tracker.application.industry.service import IndustryService

    class _Pricing:
        def __init__(self, **kw):
            pass

        def get_region_daily_volume_map(self, *, type_ids, hub, unavailable=None):
            unavailable[35] = REASON
            return {34: {"daily_volume": 100, "daily_volume_7d_avg": 90.0, "daily_volume_7d_sample_size": 7,
                         "daily_order_count": 3, "daily_volume_date": "2026-10-01"}}

        def get_hub_liquidity_map(self, *, type_ids, hub):
            return {}

    monkeypatch.setattr(market_enrichment, "MarketPricingService", _Pricing)
    svc = IndustryService.__new__(IndustryService)
    svc._state = SimpleNamespace()
    svc._sessions = None
    known, unknown = svc._enrich_product_rows_with_market_activity(
        [{"type_id": 34, "manufacturing_job": {"runs": 1}}, {"type_id": 35, "manufacturing_job": {"runs": 1}}], market_hub="jita")
    assert known["region_daily_volume"] == 100 and known["region_daily_volume_unavailable_reason"] is None
    assert unknown["region_daily_volume"] is None          # unknown, not 0
    assert unknown["region_daily_volume_7d_avg"] is None
    assert unknown["region_daily_order_count"] is None
    assert unknown["region_daily_volume_unavailable_reason"] == REASON
    assert unknown["manufacturing_job"]["region_daily_volume"] is None
    assert unknown["manufacturing_job"]["region_daily_volume_unavailable_reason"] == REASON


def _unknown_volume_row():
    return {
        "type_id": 35, "type_name": "Unknown Volume", "market_unit_price": 1_500_000.0,
        "market_price_sample_size": 5, "market_price_fetched_at": 4102444800.0,
        "hub_sell_liquidity": 1000, "hub_sell_order_count": 8,
        "region_daily_volume": None, "region_daily_volume_7d_avg": None,
        "region_daily_volume_unavailable_reason": REASON, "manufacturing_job": {"runs": 1},
    }


def test_the_pricing_confidence_names_an_unknown_region_volume_not_an_absent_one():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    svc = IndustryService.__new__(IndustryService)
    svc._state = SimpleNamespace(cfg_manager=None, esi_service=None)
    svc._sessions = None
    (row,) = svc._enrich_product_rows_with_pricing_confidence([_unknown_volume_row()], product_price_side="sell")
    reasons = row["pricing_confidence_reasons"]
    assert any("unknown" in r and REASON in r for r in reasons)
    assert not any("absent" in r for r in reasons)


def test_the_portfolio_candidate_keeps_an_unknown_region_volume_unknown():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    candidate = IndustryService._build_portfolio_candidate(_unknown_volume_row(), planning_horizon_hours=24.0)
    assert candidate["region_daily_volume"] is None
    assert candidate["region_daily_volume_unavailable_reason"] == REASON
    assert candidate["estimated_market_absorption_units"] == 0   # no volume known: nothing assumed sellable
