"""Phase 4 build rules: margin gate, sell-time gate, market cap, capital return."""
from __future__ import annotations

import dataclasses

import pytest

from test_daily_planner_decisions import _make_admin, _make_overview_row, _make_pipeline, _make_scored

from eve_online_industry_tracker.application.daily_planner.item_decision_engine import (
    ItemDecisionEngine,
    market_cap_runs,
)


def _decide(scored=None, pipeline=None, row=None, **kw):
    return ItemDecisionEngine().decide(
        scored or _make_scored(), pipeline or _make_pipeline(), row or _make_overview_row(), _make_admin(), **kw,
    )


def test_margin_below_10_is_skipped():
    d = _decide(dataclasses.replace(_make_scored(), margin_pct=6.0))
    assert d.decision == "skip"
    assert d.decision_reason == "margin 6.0% below 10%"


def test_unknown_velocity_not_built():
    d = _decide(pipeline=dataclasses.replace(_make_pipeline(), velocity_unknown_reason="no sales"))
    assert d.decision == "skip"
    assert d.decision_reason.startswith("not built: sell velocity unknown")


def test_unknown_margin_not_built():
    d = _decide(dataclasses.replace(_make_scored(), margin_pct=0.0), margin_known=False)
    assert d.decision == "skip"
    assert d.decision_reason == "not built: margin unknown"


def test_market_full_is_watched():
    d = _decide(pipeline=_make_pipeline(total_pipeline_days=50.0, velocity=1.0))
    assert d.decision == "watch"
    assert d.decision_reason.startswith("market full")
    assert d.market_cap_runs == 0


def test_build_ranks_by_capital_return():
    scored = dataclasses.replace(_make_scored(), absolute_profit_per_batch=20_000_000)
    d = _decide(scored, _make_pipeline(total_pipeline_days=0.0, velocity=10.0))
    assert d.decision == "build"
    assert d.market_cap_runs == 30
    assert d.expected_days_to_sell == pytest.approx(30)
    assert d.capital_return_per_day == pytest.approx(0.2 / 30)
    assert d.decision_reason == "margin 15.0%, 30 days to sell, 0.67%/day on capital"


def test_low_isk_per_hour_still_builds():
    scored = dataclasses.replace(_make_scored(), isk_per_hour=400_000, adjusted_score=400_000, margin_pct=15.0)
    # 300 units at 15/day sell in 20 days
    d = _decide(scored, _make_pipeline(total_pipeline_days=0.0, velocity=15.0))
    assert d.decision == "build"
    assert d.expected_days_to_sell == pytest.approx(20)


def test_batch_cost_unknown_not_built():
    row = {**_make_overview_row(), "manufacturing_job": {"runs": 30, "total_cost": None}}
    d = _decide(row=row)
    assert (d.decision, d.decision_reason) == ("skip", "not built: batch cost unknown")


def test_batch_runs_unknown_not_built():
    row = {**_make_overview_row(), "manufacturing_job": {"total_cost": 1e8}}
    d = _decide(row=row)
    assert (d.decision, d.decision_reason) == ("skip", "not built: batch runs unknown")


def test_skip_keeps_declining_market_suffix():
    pipeline = _make_pipeline(price_trend_7d=-10.0)
    d = _decide(dataclasses.replace(_make_scored(), margin_pct=6.0), pipeline)
    assert d.decision_reason == "margin 6.0% below 10%; declining market (no active jobs)"


def test_market_cap_runs():
    assert market_cap_runs(batch_runs=30, units_per_run=1, velocity=0.5, pipeline_units=10, max_days=45) == 12
    assert market_cap_runs(batch_runs=30, units_per_run=1, velocity=0.5, pipeline_units=100, max_days=45) == 0
