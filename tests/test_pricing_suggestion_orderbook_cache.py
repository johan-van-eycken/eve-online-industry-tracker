"""The pricing suggestion reads the order book through the same view-cache repo
as market pricing, so a row of an older cache version is not used."""
from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_analysis import pricing_suggestion_service as ps
from eve_online_industry_tracker.infrastructure.persistence import market_orderbook_view_cache_repo as view_repo


def _service(factory):
    svc = ps.PricingSuggestionService.__new__(ps.PricingSuggestionService)
    svc._sessions = SimpleNamespace(app_session=factory)
    return svc


def _seed(factory):
    session = factory()
    view_repo.upsert_views(session, hub="jita", region_id=10000002, station_id=60003760, side="sell",
                           at_hub=True, views_by_type_id={34: [(6.0, 10), (5.0, 20)]}, depth=10)
    session.commit()
    session.close()


def test_the_cached_sell_levels_come_back_sorted_by_price(app_engine):
    factory = sessionmaker(bind=app_engine, future=True)
    _seed(factory)
    assert [tuple(lv) for lv in _service(factory)._get_orderbook_levels(type_id=34)] == [(5.0, 20), (6.0, 10)]


def test_a_row_of_an_older_cache_version_is_not_used(app_engine):
    factory = sessionmaker(bind=app_engine, future=True)
    _seed(factory)
    with app_engine.begin() as conn:
        conn.execute(text("UPDATE market_orderbook_view_cache SET version = 0"))
    assert _service(factory)._get_orderbook_levels(type_id=34) == []
