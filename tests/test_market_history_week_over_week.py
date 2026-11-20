"""The 7-day trend the planner pauses on is week over week (the last 7 days
against the 7 before), not the last 7 days against 42 weeks: a price that
settled lower months ago and is flat now is not "declining" (Deluge, plan 37)."""
from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService
from eve_online_industry_tracker.infrastructure.models import MarketHistoryModel

TYPE, REGION = 81046, 10000002


def _stats(app_engine, closes):
    factory = sessionmaker(bind=app_engine, future=True)
    s = factory()
    start = date.today() - timedelta(days=len(closes))
    for i, close in enumerate(closes):
        s.add(MarketHistoryModel(type_id=TYPE, region_id=REGION, date=(start + timedelta(days=i)).isoformat(),
                                 close=close, high=close, low=close, volume=10, order_count=1))
    s.commit()
    s.close()
    service = MarketHistoryService(state=SimpleNamespace(esi_service=None), sessions=SimpleNamespace(app_session=factory))
    return service.get_price_stats(type_id=TYPE, region_id=REGION)


def test_a_price_that_settled_lower_and_is_flat_is_not_declining_week_over_week(app_engine):
    stats = _stats(app_engine, [176.0] * 200 + [160.0] * 14)
    assert stats["trend_pct"] < -8                   # 7d vs 42w: far below the old level
    assert stats["trend_7d_wow_pct"] == 0.0          # but flat for two weeks


def test_a_real_drop_this_week_shows_week_over_week(app_engine):
    stats = _stats(app_engine, [160.0] * 7 + [144.0] * 7)
    assert round(stats["trend_7d_wow_pct"], 1) == -10.0


def test_under_two_weeks_of_history_there_is_no_week_over_week_trend(app_engine):
    assert _stats(app_engine, [160.0] * 10)["trend_7d_wow_pct"] is None
