"""Tests for ProfitabilityScorer (Phase 3)."""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.daily_planner.models import PipelineState
from eve_online_industry_tracker.application.daily_planner.profitability_scorer import ProfitabilityScorer


def _make_pipeline(
    type_id: int = 1,
    price_trend_7d: float = 0.0,
    price_trend_30d: float | None = 0.0,
    days_of_supply: float = 0.0,
    competition_index: float | None = None,
    effective_velocity: float = 10.0,
) -> PipelineState:
    trend_30d = price_trend_30d if price_trend_30d is not None else 0.0
    momentum_signal = price_trend_7d - (trend_30d / 4.0)
    return PipelineState(
        type_id=type_id,
        effective_velocity=effective_velocity,
        total_pipeline_days=days_of_supply,
        competition_index=competition_index,
        competitor_units=0,
        bpc_runs_available=0,
        momentum_signal=momentum_signal,
        days_of_supply_current=days_of_supply,
        price_trend_7d_pct=price_trend_7d,
        price_trend_30d_pct=price_trend_30d,
        has_active_manufacturing_jobs=False,
    )


def _make_overview_row(
    isk_per_hour: float = 10_000_000,
    margin_pct: float = 0.15,
    profit_amount: float = 50_000_000,
) -> dict:
    return {
        "type_id": 1,
        "type_name": "TestItem",
        "isk_per_hour": isk_per_hour,
        "profit_margin_fraction": margin_pct,
        "profit_amount": profit_amount,
    }


def _make_weights(
    accuracy_ema: float = 1.0,
    velocity_multiplier: float = 1.0,
    cost_multiplier: float = 1.0,
    confidence_tier: str = "low",
) -> MagicMock:
    w = MagicMock()
    w.accuracy_ema = accuracy_ema
    w.velocity_multiplier = velocity_multiplier
    w.cost_multiplier = cost_multiplier
    w.confidence_tier = confidence_tier
    return w


class TestProfitabilityScorer:
    """Verify each multiplier is applied correctly."""

    def setup_method(self):
        self.scorer = ProfitabilityScorer()

    def test_all_weights_one_returns_base_isk(self):
        """With all multipliers=1.0, adjusted_score should equal base ISK/hr × pipeline_saturation × market_timing."""
        pipeline = _make_pipeline(price_trend_7d=0.0, days_of_supply=3.0)
        row = _make_overview_row(isk_per_hour=10_000_000)
        weights = _make_weights()

        scored = self.scorer.score(pipeline, row, weights, market_depth=None, margin_correlation=None)

        # At 0% trend: base_factor = max(0.5, min(1.0, 1.0 + (0+5)*0.05)) = min(1.0, 1.25) = 1.0
        # momentum_signal=0, not < -3 → market_timing_factor = 1.0
        # days_supply=3: pipeline_saturation = max(0, 1-(3-3)/11) = 1.0
        # competition_index=None → competition_factor=1.0
        # confidence_tier=low → bonus=1.0
        # Expected: 10M * 1 * 1 * 1 * 1.0 * 1.0 * 1.0 * 1.0 * 1.0 = 10M
        assert abs(scored.adjusted_score - 10_000_000) < 1, f"Got {scored.adjusted_score}"

    def test_trend_minus_5_gives_base_factor_1_with_momentum_adj(self):
        """price_trend_7d=-5%, trend_30d=0 → momentum_signal=-5 < -3 → momentum_adj applied."""
        pipeline = _make_pipeline(price_trend_7d=-5.0, price_trend_30d=0.0, days_of_supply=3.0)
        row = _make_overview_row(isk_per_hour=10_000_000)
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        # base_factor = max(0.5, min(1.0, 1.0 + (-5+5)*0.05)) = 1.0
        # momentum_signal = -5 - 0/4 = -5 < -3
        # momentum_adj = max(0.8, 1.0 + (-5)/30) = max(0.8, 0.8333) = 0.8333
        # market_timing_factor = 1.0 * 0.8333 = 0.8333
        expected_momentum_adj = max(0.8, 1.0 + (-5.0) / 30.0)
        expected_mtf = 1.0 * expected_momentum_adj
        assert abs(scored.market_timing_factor - expected_mtf) < 0.001, (
            f"Expected market_timing_factor≈{expected_mtf:.4f}, got {scored.market_timing_factor}"
        )

    def test_trend_minus_10_gives_base_factor_0_75(self):
        """price_trend_7d=-10% → base_factor = max(0.5, min(1.0, 1.0 + (-10+5)*0.05)) = max(0.5, 0.75) = 0.75."""
        pipeline = _make_pipeline(price_trend_7d=-10.0, days_of_supply=3.0, price_trend_30d=0.0)
        row = _make_overview_row(isk_per_hour=10_000_000)
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        # base_factor = max(0.5, min(1.0, 1.0 + (-10+5)*0.05)) = max(0.5, 0.75) = 0.75
        # momentum_signal = -10 - 0/4 = -10 < -3
        # momentum_adj = max(0.8, 1.0 + (-10)/30) = max(0.8, 0.667) = 0.8
        # market_timing_factor = 0.75 * 0.8 = 0.6
        assert abs(scored.market_timing_factor - 0.6) < 0.01, f"Got {scored.market_timing_factor}"

    def test_trend_minus_15_gives_base_factor_0_5(self):
        """price_trend_7d=-15% → base_factor = max(0.5, 1.0+(-15+5)*0.05) = max(0.5, 0.5) = 0.5."""
        pipeline = _make_pipeline(price_trend_7d=-15.0, days_of_supply=3.0, price_trend_30d=0.0)
        row = _make_overview_row(isk_per_hour=10_000_000)
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        # base_factor = max(0.5, min(1.0, 1.0 + (-15+5)*0.05)) = max(0.5, 0.5) = 0.5
        # momentum_signal = -15 < -3
        # momentum_adj = max(0.8, 1 + (-15)/30) = max(0.8, 0.5) = 0.8
        # market_timing_factor = 0.5 * 0.8 = 0.4
        assert abs(scored.market_timing_factor - 0.4) < 0.01, f"Got {scored.market_timing_factor}"

    def test_momentum_adj_exactly_at_minus_6(self):
        """momentum_signal=-6 → momentum_adj = max(0.8, 1 + (-6/30)) = max(0.8, 0.8) = 0.8."""
        # momentum_signal = price_trend_7d - price_trend_30d/4
        # Set trend_7d=0, trend_30d=-24 → momentum = 0 - (-24/4) = 6... not right
        # Or: trend_7d=-6, trend_30d=0 → momentum = -6 - 0 = -6 ✓
        pipeline = _make_pipeline(price_trend_7d=-6.0, price_trend_30d=0.0, days_of_supply=3.0)
        row = _make_overview_row(isk_per_hour=10_000_000)
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        # base_factor = max(0.5, min(1.0, 1.0 + (-6+5)*0.05)) = max(0.5, 0.95) = 0.95
        # momentum_signal = -6 < -3 → momentum_adj = max(0.8, 1 + (-6/30)) = max(0.8, 0.8) = 0.8
        # market_timing_factor = 0.95 * 0.8 = 0.76
        assert abs(scored.market_timing_factor - 0.76) < 0.01, f"Got {scored.market_timing_factor}"

    def test_pipeline_saturation_at_0_days(self):
        """0 days supply → pipeline_saturation = 1 - (0-3)/11 = 1 + 3/11 ≈ 1.27."""
        pipeline = _make_pipeline(price_trend_7d=0.0, days_of_supply=0.0)
        row = _make_overview_row(isk_per_hour=10_000_000)
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        expected = max(0.0, 1.0 - (0.0 - 3.0) / 11.0)
        assert abs(scored.pipeline_saturation - expected) < 0.01, f"Got {scored.pipeline_saturation}"

    def test_pipeline_saturation_at_14_days(self):
        """14 days supply → pipeline_saturation = max(0, 1-(14-3)/11) = 0.0."""
        pipeline = _make_pipeline(price_trend_7d=0.0, days_of_supply=14.0)
        row = _make_overview_row(isk_per_hour=10_000_000)
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        assert abs(scored.pipeline_saturation - 0.0) < 0.01, f"Got {scored.pipeline_saturation}"

    def test_competition_factor_none_is_1(self):
        """competition_index=None (Phase A) → competition_factor=1.0."""
        pipeline = _make_pipeline(competition_index=None)
        row = _make_overview_row()
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        assert scored.competition_factor == 1.0

    def test_competition_factor_below_1(self):
        """competition_index=0.5 (< 1.0) → competition_factor=1.0."""
        pipeline = _make_pipeline(competition_index=0.5)
        row = _make_overview_row()
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        assert scored.competition_factor == 1.0

    def test_competition_factor_1_to_2(self):
        """competition_index=1.5 → competition_factor=0.90."""
        pipeline = _make_pipeline(competition_index=1.5)
        row = _make_overview_row()
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        assert scored.competition_factor == 0.90

    def test_competition_factor_2_to_4(self):
        """competition_index=3.0 → competition_factor=0.75."""
        pipeline = _make_pipeline(competition_index=3.0)
        row = _make_overview_row()
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        assert scored.competition_factor == 0.75

    def test_competition_factor_above_4(self):
        """competition_index=5.0 → competition_factor=0.60."""
        pipeline = _make_pipeline(competition_index=5.0)
        row = _make_overview_row()
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, None, None)
        assert scored.competition_factor == 0.60

    def test_confidence_tier_bonus_medium(self):
        """confidence_tier='medium' → bonus=1.05."""
        pipeline = _make_pipeline(days_of_supply=3.0, price_trend_7d=0.0)
        row = _make_overview_row(isk_per_hour=10_000_000)
        weights = _make_weights(confidence_tier="medium")
        scored = self.scorer.score(pipeline, row, weights, None, None)
        assert scored.confidence_tier_bonus == 1.05

    def test_confidence_tier_bonus_high(self):
        """confidence_tier='high' → bonus=1.15."""
        pipeline = _make_pipeline(days_of_supply=3.0, price_trend_7d=0.0)
        row = _make_overview_row(isk_per_hour=10_000_000)
        weights = _make_weights(confidence_tier="high")
        scored = self.scorer.score(pipeline, row, weights, None, None)
        assert scored.confidence_tier_bonus == 1.15

    def test_mineral_squeeze_penalty_1_when_no_correlation(self):
        """No correlation data → mineral_squeeze_penalty=1.0."""
        pipeline = _make_pipeline()
        row = _make_overview_row()
        weights = _make_weights()
        scored = self.scorer.score(pipeline, row, weights, market_depth=None, margin_correlation=None)
        assert scored.mineral_squeeze_penalty == 1.0

    def test_learning_weights_applied(self):
        """Verify accuracy_ema, velocity_multiplier, cost_multiplier are all multiplied in."""
        pipeline = _make_pipeline(price_trend_7d=0.0, days_of_supply=3.0)
        row = _make_overview_row(isk_per_hour=10_000_000)
        weights = _make_weights(accuracy_ema=0.9, velocity_multiplier=1.1, cost_multiplier=0.95)
        scored = self.scorer.score(pipeline, row, weights, None, None)
        # Expected: 10M * 0.9 * 1.1 * 0.95 * 1.0 * 1.0 * 1.0 * 1.0 * 1.0 = 9.405M
        expected = 10_000_000 * 0.9 * 1.1 * 0.95 * 1.0 * 1.0 * 1.0 * 1.0 * 1.0
        assert abs(scored.adjusted_score - expected) < 1000, f"Got {scored.adjusted_score}, expected {expected}"


def pytest_approx(value, rel=None, abs=None):
    """Helper that returns value for use in assertions (not a real pytest fixture here)."""
    # In this module we use direct assert with abs tolerance; this is a placeholder
    return value
