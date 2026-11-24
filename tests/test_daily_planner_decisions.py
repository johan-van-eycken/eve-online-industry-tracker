"""Tests for ItemDecisionEngine (Phase 4)."""
from __future__ import annotations

from unittest.mock import MagicMock

from eve_online_industry_tracker.application.daily_planner.item_decision_engine import ItemDecisionEngine
from eve_online_industry_tracker.application.daily_planner.models import PipelineState, ScoredItem


def _make_admin(
    min_isk=5_000_000,
    min_profit=20_000_000,
    momentum_pause=-6.0,
    min_margin=10.0,
    max_days=45.0,
):
    admin = MagicMock()
    settings = {
        "planner_momentum_pause_threshold": momentum_pause,
        "planner_min_margin_pct": min_margin,
        "planner_max_days_to_sell": max_days,
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
    velocity: float = 10.0,
) -> PipelineState:
    return PipelineState(
        type_id=type_id,
        effective_velocity=velocity,
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
        margin_pct=15.0,
        unscoreable_reason=unscoreable_reason,
    )


def _make_overview_row(type_id: int = 1) -> dict:
    return {
        "type_id": type_id,
        "type_name": "TestItem",
        "meta_group_id": 1,
        "blueprint_type_id": 1000 + type_id,
        "quantity": 300,
        "manufacturing_job": {"runs": 30, "total_cost": 100_000_000.0},
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

    def test_watch_when_market_full(self):
        """Pipeline already holds more than max_days of sales → watch (market full)."""
        pipeline = _make_pipeline(total_pipeline_days=50.0, competition_index=None)
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=50_000_000)
        admin = _make_admin()
        row = _make_overview_row()

        decision = self.engine.decide(scored, pipeline, row, admin)
        assert decision.decision == "watch", f"Expected watch, got {decision.decision}"

    def test_a_high_competition_index_no_longer_forces_watch(self):
        """The index divides the hub's whole sell stock by our own sales, so it
        flagged nearly every item; the sell-time rule decides instead."""
        pipeline = _make_pipeline(total_pipeline_days=1.0, competition_index=17186.3)
        decision = self.engine.decide(_make_scored(), pipeline, _make_overview_row(), _make_admin())
        assert decision.decision == "build", decision.decision_reason

    def test_no_build_when_the_return_on_capital_is_not_positive(self):
        """A loss at the realistic sale price is never built, whatever the margin says."""
        pipeline = _make_pipeline(total_pipeline_days=1.0)
        for profit in (-3_000_000, 0.0):
            decision = self.engine.decide(
                _make_scored(absolute_profit=profit), pipeline, _make_overview_row(), _make_admin(),
            )
            assert decision.decision == "skip", decision.decision_reason
            assert "no return on capital" in decision.decision_reason

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

    def test_low_score_no_longer_skips(self):
        """adjusted_score below the old ISK/hour threshold no longer gates: margin and sell time do."""
        pipeline = _make_pipeline(total_pipeline_days=1.0, price_trend_7d=0.0)
        scored = _make_scored(adjusted_score=1_000_000, absolute_profit=50_000_000)
        decision = self.engine.decide(scored, pipeline, _make_overview_row(), _make_admin(min_isk=5_000_000))
        assert decision.decision == "build", decision.decision_reason

    def test_low_profit_no_longer_skips(self):
        """absolute_profit below the old batch-profit threshold no longer gates."""
        pipeline = _make_pipeline(total_pipeline_days=1.0, price_trend_7d=0.0)
        scored = _make_scored(adjusted_score=10_000_000, absolute_profit=5_000_000)
        decision = self.engine.decide(scored, pipeline, _make_overview_row(), _make_admin(min_profit=20_000_000))
        assert decision.decision == "build", decision.decision_reason

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

    def test_watch_when_no_room_left_at_max_days(self):
        """Pipeline of 45 days at max 45 → not even one run fits → watch."""
        pipeline = _make_pipeline(total_pipeline_days=45.0)
        decision = self.engine.decide(_make_scored(), pipeline, _make_overview_row(), _make_admin())
        assert decision.decision == "watch"
        assert decision.market_cap_runs == 0

    def test_build_when_one_run_still_fits(self):
        """Pipeline of 44 days: 10 units of room = one run of 10 units → build, capped at 1 run."""
        pipeline = _make_pipeline(total_pipeline_days=44.0)
        decision = self.engine.decide(_make_scored(), pipeline, _make_overview_row(), _make_admin())
        assert decision.decision == "build"
        assert decision.market_cap_runs == 1

    def test_unscoreable_item_skips_with_reason_before_any_threshold_check(self):
        """An unscoreable ScoredItem must skip on that reason, not on the margin/sell-time rules.

        Pipeline/admin values here would normally satisfy the 'build' path outright
        (high margin, room on the market) -- proving the unscoreable check runs
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
        # 'unscoreable' is a distinct pipeline_stage from the ordinary 'watching'
        # skip -- this item was never priced, not deliberately deferred after
        # being priced -- so persisted build_plan_item rows can tell the two apart.
        assert decision.pipeline_stage == "unscoreable"


def test_the_decision_carries_the_pipelines_velocity_unknown_reason():
    import dataclasses
    reason = "no corp sales in 30 days and no market volume"
    pipeline = dataclasses.replace(_make_pipeline(), velocity_unknown_reason=reason)
    for scored in (_make_scored(), _make_scored(unscoreable_reason="no cost basis")):
        decision = ItemDecisionEngine().decide(
            scored=scored, pipeline=pipeline, overview_row=_make_overview_row(),
            admin_settings=_make_admin(),
        )
        assert decision.velocity_unknown_reason == reason
