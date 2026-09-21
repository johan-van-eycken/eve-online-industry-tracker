"""Tests for ItemDecisionEngine (Phase 4)."""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.daily_planner.item_decision_engine import ItemDecisionEngine
from eve_online_industry_tracker.application.daily_planner.models import PipelineState, ScoredItem


def _make_admin(
    min_isk=5_000_000,
    min_profit=20_000_000,
    competition_gate=4.0,
    momentum_pause=-6.0,
):
    admin = MagicMock()
    settings = {
        "planner_min_isk_per_hour": min_isk,
        "planner_min_profit_per_batch": min_profit,
        "planner_competition_index_gate": competition_gate,
        "planner_momentum_pause_threshold": momentum_pause,
    }
    admin.get.side_effect = lambda section, key: settings.get(key, None)
    return admin


def _make_pipeline(
    type_id: int = 1,
    total_pipeline_days: float = 1.0,
    competition_index: float | None = None,
    price_trend_7d: float = 0.0,
    momentum_signal: float = 0.0,
    has_active_jobs: bool = False,
    days_of_supply: float = 1.0,
) -> PipelineState:
    return PipelineState(
        type_id=type_id,
        effective_velocity=10.0,
        total_pipeline_days=total_pipeline_days,
        competition_index=competition_index,
        competitor_units=0,
        bpc_runs_available=0,
        momentum_signal=momentum_signal,
        days_of_supply_current=days_of_supply,
        price_trend_7d_pct=price_trend_7d,
        price_trend_30d_pct=None,
        has_active_manufacturing_jobs=has_active_jobs,
    )


def _make_scored(
    type_id: int = 1,
    adjusted_score: float = 10_000_000,
    absolute_profit: float = 50_000_000,
    unscoreable_reason: str | None = None,
) -> ScoredItem:
    return ScoredItem(
        type_id=type_id,
        adjusted_score=adjusted_score,
        absolute_profit_per_batch=absolute_profit,
        market_timing_factor=1.0,
        pipeline_saturation=1.0,
        accuracy_ema=1.0,
        velocity_multiplier=1.0,
        cost_multiplier=1.0,
        competition_factor=1.0,
        mineral_squeeze_penalty=1.0,
        confidence_tier_bonus=1.0,
        isk_per_hour=10_000_000,
        margin_pct=0.15,
        unscoreable_reason=unscoreable_reason,
    )


def _make_overview_row(type_id: int = 1) -> dict:
    return {
        "type_id": type_id,
        "type_name": "TestItem",
        "meta_group_id": 1,
        "blueprint_type_id": 1000 + type_id,
    }


class TestItemDecisionEngine:
    def setup_method(self):
        self.engine = ItemDecisionEngine()

    def test_build_all_conditions_met(self):
        """When all conditions met → decision='build'."""
        pipeline = _make_pipeline(total_pipeline_days=1.0, competition_index=None, price_trend_7d=0.0, momentum_signal=0.0)
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin()
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "build", f"Expected build, got {decision.decision}: {decision.decision_reason}"

    def test_watch_when_pipeline_saturated(self):
        """total_pipeline_days >= 3 → watch."""
        pipeline = _make_pipeline(total_pipeline_days=5.0, competition_index=None)
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin()
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "watch", f"Expected watch, got {decision.decision}"

    def test_watch_when_competition_flooded(self):
        """competition_index >= 4.0 → watch (even if pipeline < 3d)."""
        pipeline = _make_pipeline(total_pipeline_days=1.0, competition_index=4.5)
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin(competition_gate=4.0)
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "watch", f"Expected watch, got {decision.decision}"

    def test_competition_index_none_does_not_force_watch(self):
        """Phase A: competition_index=None → no competition gate applied → can still 'build'."""
        pipeline = _make_pipeline(total_pipeline_days=1.0, competition_index=None, price_trend_7d=0.0)
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin()
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "build", f"Expected build, got {decision.decision}: {decision.decision_reason}"

    def test_pause_requires_active_jobs(self):
        """Pause only when active jobs exist AND declining trend."""
        # With active jobs + declining trend → pause
        pipeline_with_jobs = _make_pipeline(
            total_pipeline_days=1.0,
            price_trend_7d=-10.0,
            momentum_signal=-10.0,
            has_active_jobs=True,
        )
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin()
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline_with_jobs, row, admin)
        assert decision.decision == "pause", f"Expected pause, got {decision.decision}"

    def test_no_pause_without_active_jobs(self):
        """Declining trend but NO active jobs → skip, not pause."""
        pipeline_no_jobs = _make_pipeline(
            total_pipeline_days=1.0,
            price_trend_7d=-10.0,
            momentum_signal=-10.0,
            has_active_jobs=False,
        )
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin()
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline_no_jobs, row, admin)
        # Declining trend + no active jobs → skip (from score check), or could also trigger skip
        # The key is: it should NOT be 'pause'
        assert decision.decision != "pause", f"Should not pause without active jobs"

    def test_skip_when_score_below_threshold(self):
        """adjusted_score <= min_isk → skip."""
        pipeline = _make_pipeline(total_pipeline_days=1.0, price_trend_7d=0.0)
        scored = _make_scored(adjusted_score=1_000_000, absolute_profit=50_000_000)  # below 5M threshold
        admin = _make_admin(min_isk=5_000_000)
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "skip", f"Expected skip, got {decision.decision}"

    def test_skip_when_profit_below_threshold(self):
        """absolute_profit <= min_profit → skip."""
        pipeline = _make_pipeline(total_pipeline_days=1.0, price_trend_7d=0.0)
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=5_000_000)  # below 20M threshold
        admin = _make_admin(min_profit=20_000_000)
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "skip", f"Expected skip, got {decision.decision}"

    def test_pause_on_trend_below_minus_8(self):
        """price_trend_7d < -8% + active jobs → pause (regardless of momentum)."""
        pipeline = _make_pipeline(
            total_pipeline_days=1.0,
            price_trend_7d=-9.0,
            momentum_signal=0.0,   # momentum OK, but trend alone triggers pause
            has_active_jobs=True,
        )
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin()
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "pause"

    def test_pause_on_momentum_threshold(self):
        """momentum_signal < -6 + active jobs → pause."""
        pipeline = _make_pipeline(
            total_pipeline_days=1.0,
            price_trend_7d=0.0,  # trend fine
            momentum_signal=-7.0,  # but momentum bad
            has_active_jobs=True,
        )
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin(momentum_pause=-6.0)
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "pause", f"Expected pause, got {decision.decision}"

    def test_watch_exactly_at_3_days(self):
        """total_pipeline_days exactly 3.0 → watch (>= 3)."""
        pipeline = _make_pipeline(total_pipeline_days=3.0)
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin()
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "watch"

    def test_build_just_below_3_days(self):
        """total_pipeline_days=2.99 → build (still < 3d)."""
        pipeline = _make_pipeline(total_pipeline_days=2.99)
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin()
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "build"

    def test_unscoreable_item_skips_with_reason_before_any_threshold_check(self):
        """An unscoreable ScoredItem must skip on that reason, not on the score/profit gate.

        Pipeline/admin values here would normally satisfy the 'build' path outright
        (high score, high profit, no saturation) -- proving the unscoreable check runs
        first, before any threshold comparison or /1e6 formatting.
        """
        pipeline = _make_pipeline(total_pipeline_days=1.0, competition_index=None, price_trend_7d=0.0)
        scored = _make_scored(
            adjusted_score=10_000_000,
            absolute_profit=50_000_000,
            unscoreable_reason="no isk/hour (missing cost basis or job time)",
        )
        admin = _make_admin(min_isk=5_000_000, min_profit=20_000_000)
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "skip"
        assert decision.decision_reason == "no isk/hour (missing cost basis or job time)"
