"""The user's ruling on the planner thresholds (2026-10-21).

- Pipeline saturation is softened: max(floor, 1 - (days_of_supply - 3) /
  (full_days - 3)), floor 0.25 and full_days 30, two admin settings read at
  use (it was max(0, 1 - (days_of_supply - 3) / 11): zero at 14 days of
  supply, where days_of_supply is hub sell liquidity / the whole region's
  daily volume).
- planner_min_isk_per_hour 5M -> 2M and planner_min_profit_per_batch 20M ->
  10M by default.

Numbers are fake.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from test_admin_settings_wired import _manager
from test_daily_planner_scoring import _input_row, _make_pipeline

from eve_online_industry_tracker.application.daily_planner.item_decision_engine import ItemDecisionEngine
from eve_online_industry_tracker.application.daily_planner.profitability_scorer import ProfitabilityScorer
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA

SETTINGS = ADMIN_SETTINGS_SCHEMA["daily_planner"]["settings"]


def _saturation(days, **settings):
    return ProfitabilityScorer().score(
        _make_pipeline(days_of_supply=days), _input_row(), None, None, None, **settings,
    ).pipeline_saturation


@pytest.mark.parametrize(("key", "default", "low", "high"), [
    ("planner_saturation_floor", 0.25, 0.0, 1.0),
    ("planner_saturation_full_days", 30, 4, 365),
])
def test_the_saturation_settings(key, default, low, high):
    spec = SETTINGS[key]
    assert (spec["default"], spec["min"], spec["max"]) == (default, low, high)


def test_the_new_threshold_defaults():
    assert SETTINGS["planner_min_isk_per_hour"]["default"] == 2_000_000
    assert SETTINGS["planner_min_profit_per_batch"]["default"] == 10_000_000


@pytest.mark.parametrize(("days", "expected"), [
    (0.0, 1.0 + 3.0 / 27.0),     # below 3 days: a bonus, as before
    (3.0, 1.0),
    (14.0, 1.0 - 11.0 / 27.0),   # was 0.0
    (23.25, 0.25),               # reaches the floor
    (60.0, 0.25),                # never below it
])
def test_the_softened_saturation_by_default(days, expected):
    assert _saturation(days) == pytest.approx(expected)


def test_the_saturation_follows_the_settings_it_is_given():
    # floor 0, full at 14 days: the old curve.
    assert _saturation(14.0, saturation_floor=0.0, saturation_full_days=14) == pytest.approx(0.0)
    assert _saturation(8.5, saturation_floor=0.0, saturation_full_days=14) == pytest.approx(0.5)
    assert _saturation(60.0, saturation_floor=0.4, saturation_full_days=30) == pytest.approx(0.4)


def test_the_service_reads_the_saturation_settings_at_use(tmp_path):
    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService

    svc = DailyPlannerService.__new__(DailyPlannerService)
    svc._admin = _manager(tmp_path)
    assert svc._saturation_settings() == {"saturation_floor": 0.25, "saturation_full_days": 30}
    svc._admin.set("daily_planner", "planner_saturation_floor", 0.1)
    svc._admin.set("daily_planner", "planner_saturation_full_days", 60)
    assert svc._saturation_settings() == {"saturation_floor": 0.1, "saturation_full_days": 60}


@pytest.mark.parametrize(("key", "value"), [
    ("planner_saturation_floor", 1.5), ("planner_saturation_floor", -0.1),
    ("planner_saturation_full_days", 3), ("planner_saturation_full_days", 366),
])
def test_an_out_of_range_saturation_setting_is_refused(tmp_path, key, value):
    with pytest.raises(ValueError, match="is outside"):
        _manager(tmp_path).set("daily_planner", key, value)


def test_a_row_between_the_old_and_new_thresholds_is_built_by_default():
    """3M ISK/hour and 15M per batch: skipped under 5M / 20M, built under
    2M / 10M (no settings store: the schema defaults)."""
    scored = SimpleNamespace(
        type_id=1, adjusted_score=3_000_000.0, absolute_profit_per_batch=15_000_000.0, isk_per_hour=3_000_000.0,
        margin_pct=10.0, unscoreable_reason=None,
    )
    pipeline = SimpleNamespace(
        price_trend_7d_pct=0.0, momentum_signal=0.0, total_pipeline_days=0.0, competition_index=None,
        has_active_manufacturing_jobs=False, days_of_supply_current=1.0, effective_velocity=1.0,
        velocity_unknown_reason=None,
    )
    decision = ItemDecisionEngine().decide(scored, pipeline, {"type_id": 1, "type_name": "Thing"}, None)
    assert decision.decision == "build"
