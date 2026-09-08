"""Tests for PipelineAnalyzer (Phase 2)."""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.daily_planner.pipeline_analyzer import PipelineAnalyzer


def _make_overview_row(
    type_id: int = 1,
    days_of_supply: float = 5.0,
    price_trend_7d: float = 0.0,
    price_trend_30d: float | None = 0.0,
    corp_stock_qty: float = 0.0,
    units_on_market: float = 0.0,
    blueprint_type_id: int = 1001,
) -> dict:
    row = {
        "type_id": type_id,
        "type_name": "TestItem",
        "days_of_supply": days_of_supply,
        "price_trend_7d_pct": price_trend_7d,
        "corp_stock_qty": corp_stock_qty,
        "units_on_market": units_on_market,
        "blueprint_type_id": blueprint_type_id,
    }
    if price_trend_30d is not None:
        row["price_trend_30d_pct"] = price_trend_30d
    return row


class TestPipelineAnalyzer:
    def setup_method(self):
        self.analyzer = PipelineAnalyzer()

    def _run(
        self,
        rows,
        industry_jobs=None,
        corp_assets=None,
        market_depth_cache=None,
        weights=None,
        sell_velocities=None,
    ):
        return self.analyzer.analyze(
            overview_rows=rows,
            industry_jobs=industry_jobs or [],
            corp_assets=corp_assets or [],
            market_depth_cache=market_depth_cache or {},
            weights=weights or {},
            sell_velocities=sell_velocities or {},
        )

    def test_effective_velocity_from_sell_velocity(self):
        """When sell_velocity > 0, effective_velocity = sell_velocity × velocity_multiplier."""
        rows = [_make_overview_row(type_id=1, days_of_supply=5.0)]
        weights = {1: MagicMock(velocity_multiplier=1.5)}
        velocities = {1: 10.0}

        states = self._run(rows, sell_velocities=velocities, weights=weights)
        assert len(states) == 1
        assert abs(states[0].effective_velocity - 15.0) < 0.01, f"Got {states[0].effective_velocity}"

    def test_effective_velocity_fallback_when_zero_velocity(self):
        """When sell_velocity=0, fallback to 1/days_of_supply."""
        rows = [_make_overview_row(type_id=1, days_of_supply=10.0)]
        velocities = {1: 0.0}

        states = self._run(rows, sell_velocities=velocities)
        assert len(states) == 1
        # 1/10 = 0.1
        assert abs(states[0].effective_velocity - 0.1) < 0.001, f"Got {states[0].effective_velocity}"

    def test_effective_velocity_fallback_floored_at_0_01(self):
        """effective_velocity is always >= 0.01."""
        # sell_velocity=0, days_of_supply=0 → both zero → floor at 0.01
        rows = [_make_overview_row(type_id=1, days_of_supply=0.0)]
        velocities = {1: 0.0}

        states = self._run(rows, sell_velocities=velocities)
        assert len(states) == 1
        assert states[0].effective_velocity >= 0.01, f"Got {states[0].effective_velocity}"
        assert abs(states[0].effective_velocity - 0.01) < 0.001

    def test_effective_velocity_minimum_floor_applies(self):
        """Even with velocity=0 and huge days_of_supply, floor at 0.01."""
        rows = [_make_overview_row(type_id=1, days_of_supply=1000.0)]
        velocities = {1: 0.0}

        states = self._run(rows, sell_velocities=velocities)
        # 1/1000 = 0.001 → floored to 0.01
        assert states[0].effective_velocity >= 0.01

    def test_momentum_signal_uses_7d_when_30d_is_none(self):
        """If price_trend_30d_pct is None, momentum_signal = price_trend_7d_pct alone."""
        rows = [_make_overview_row(type_id=1, price_trend_7d=-5.0, price_trend_30d=None)]
        velocities = {1: 10.0}

        states = self._run(rows, sell_velocities=velocities)
        assert len(states) == 1
        # momentum_signal = -5.0 - (0.0 / 4.0) = -5.0 (30d treated as 0.0)
        assert abs(states[0].momentum_signal - (-5.0)) < 0.01, f"Got {states[0].momentum_signal}"

    def test_momentum_signal_computed_correctly(self):
        """momentum_signal = price_trend_7d - price_trend_30d/4."""
        rows = [_make_overview_row(type_id=1, price_trend_7d=-8.0, price_trend_30d=-4.0)]
        velocities = {1: 10.0}

        states = self._run(rows, sell_velocities=velocities)
        # momentum = -8 - (-4/4) = -8 + 1 = -7
        assert abs(states[0].momentum_signal - (-7.0)) < 0.01, f"Got {states[0].momentum_signal}"

    def test_total_pipeline_days_calculation(self):
        """total_pipeline_days = (stock + mfg + market) / velocity."""
        # stock=50, market=30, velocity=10 → (80+0)/10 = 8.0
        rows = [_make_overview_row(type_id=1, corp_stock_qty=50.0, units_on_market=30.0)]
        velocities = {1: 10.0}

        states = self._run(rows, sell_velocities=velocities)
        assert abs(states[0].total_pipeline_days - 8.0) < 0.01, f"Got {states[0].total_pipeline_days}"

    def test_competition_index_from_cache(self):
        """competition_index read from market_depth_cache."""
        rows = [_make_overview_row(type_id=1)]
        velocities = {1: 10.0}
        cache_entry = MagicMock()
        cache_entry.competition_index = 2.5
        cache_entry.competitor_units = 1000

        states = self._run(rows, sell_velocities=velocities, market_depth_cache={1: cache_entry})
        assert states[0].competition_index == 2.5

    def test_competition_index_none_when_no_cache(self):
        """competition_index=None when no cache entry for type."""
        rows = [_make_overview_row(type_id=1)]
        velocities = {1: 10.0}

        states = self._run(rows, sell_velocities=velocities, market_depth_cache={})
        assert states[0].competition_index is None

    def test_has_active_manufacturing_jobs_true(self):
        """has_active_manufacturing_jobs=True when active mfg job exists."""
        rows = [_make_overview_row(type_id=1)]
        velocities = {1: 10.0}

        job = MagicMock()
        job.activity_id = 1  # manufacturing
        job.product_type_id = 1
        job.status = "active"
        job.runs = 5
        job.output_quantity = 5
        job.end_date = None

        states = self._run(rows, industry_jobs=[job], sell_velocities=velocities)
        assert states[0].has_active_manufacturing_jobs is True

    def test_has_active_manufacturing_jobs_false(self):
        """has_active_manufacturing_jobs=False when no mfg jobs."""
        rows = [_make_overview_row(type_id=1)]
        velocities = {1: 10.0}

        states = self._run(rows, industry_jobs=[], sell_velocities=velocities)
        assert states[0].has_active_manufacturing_jobs is False

    def test_multiple_rows_returns_multiple_states(self):
        """One PipelineState returned per overview row."""
        rows = [
            _make_overview_row(type_id=1),
            _make_overview_row(type_id=2),
            _make_overview_row(type_id=3),
        ]
        velocities = {1: 5.0, 2: 10.0, 3: 15.0}

        states = self._run(rows, sell_velocities=velocities)
        assert len(states) == 3
        type_ids = {s.type_id for s in states}
        assert type_ids == {1, 2, 3}

    def test_velocity_floor_applies_after_multiplier(self):
        """Even if velocity × multiplier < 0.01, floor applies."""
        rows = [_make_overview_row(type_id=1, days_of_supply=5.0)]
        # Very small velocity * small multiplier → should be floored
        weights = {1: MagicMock(velocity_multiplier=0.001)}
        velocities = {1: 0.001}  # 0.001 * 0.001 = 0.000001 < 0.01 → floor

        states = self._run(rows, sell_velocities=velocities, weights=weights)
        assert states[0].effective_velocity >= 0.01
