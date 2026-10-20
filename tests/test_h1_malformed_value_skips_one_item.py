"""H1 after H2's EsiRequestError: ValueError is no longer an ESI error, so
the per-item loops name it explicitly (a TypeError stays a bug and
propagates): a malformed value skips that one item, with its reason, and the
others are still processed."""
from __future__ import annotations

import logging
from types import SimpleNamespace

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import MarketDepthCollector
from eve_online_industry_tracker.application.market_pricing.order_values import refresh_market_history


def test_a_malformed_value_in_one_types_depth_skips_that_type_only(app_engine, monkeypatch, caplog):
    from eve_online_industry_tracker.application.market_analysis import market_history_service

    class _History:
        def __init__(self, **k):
            pass

        def fetch_and_store_history(self, *, type_id, region_id):
            if type_id == 34:
                raise ValueError("fake malformed history")
            return None

    monkeypatch.setattr(market_history_service, "MarketHistoryService", _History)
    state = SimpleNamespace(
        db_app=SimpleNamespace(Session=sessionmaker(bind=app_engine, future=True)),
        corp_manager=None, char_manager=None,
        esi_service=SimpleNamespace(get_sell_order_book=lambda ids, region_id=None: {
            t: [{"price": 5.0, "volume_remain": 10}] for t in ids}),
    )
    c = MarketDepthCollector(state=state)
    monkeypatch.setattr(c, "_get_type_ids_from_active_plan", lambda: [34, 35])
    monkeypatch.setattr(c, "_get_effective_velocity", lambda *a: None)
    real = c._process_type

    def _process(**kw):
        if kw["type_id"] == 34:
            raise ValueError("fake malformed order")
        return real(**kw)

    monkeypatch.setattr(c, "_process_type", _process)
    with caplog.at_level(logging.WARNING):
        c.run()
    with app_engine.connect() as conn:
        rows = {r[0] for r in conn.execute(text("SELECT type_id FROM market_depth_cache"))}
    assert rows == {35}
    assert "fake malformed history" in caplog.text and "fake malformed order" in caplog.text


def test_a_malformed_history_skips_that_type_only(caplog):
    class _Svc:
        def fetch_and_store_history(self, *, type_id, region_id):
            if type_id == 34:
                raise ValueError("fake malformed payload")
            return None

    with caplog.at_level(logging.WARNING):
        assert set(refresh_market_history(_Svc(), {34, 35}, label="Test")) == {34}
