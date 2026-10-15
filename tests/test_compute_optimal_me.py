"""Tests for compute_optimal_me() and compute_optimal_te() in blueprints.py."""
from __future__ import annotations

import math
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.infrastructure.sde.blueprints import (
    compute_optimal_me,
    compute_optimal_te,
    te_adjusted_batch_time,
)


def _make_sde_session(blueprint_type_id: int, manufacturing_time: int, materials: list[dict]) -> MagicMock:
    """Build a mock SDE session returning a blueprint with the given materials."""
    blueprint = MagicMock()
    blueprint.blueprintTypeID = blueprint_type_id
    blueprint.activities = {
        "manufacturing": {
            "time": manufacturing_time,
            "materials": materials,
        }
    }

    session = MagicMock()
    query_mock = MagicMock()
    filter_mock = MagicMock()
    filter_mock.first.return_value = blueprint
    query_mock.filter.return_value = filter_mock
    session.query.return_value = query_mock
    return session


# ---------------------------------------------------------------------------
# compute_optimal_me
# ---------------------------------------------------------------------------

class TestComputeOptimalMe:
    def test_qty_10_optimal_me_is_10(self):
        """A material with qty=10: savings only appear at ME10 (ceil(9.0)=9 vs ceil(9.1)=10)."""
        session = _make_sde_session(123, 1000, [{"typeID": 34, "quantity": 10}])
        result = compute_optimal_me(123, session)
        assert result == 10, f"Expected 10, got {result}"

    def test_qty_100_optimal_me_is_10(self):
        """A material with qty=100: every ME level saves 1 unit until ME10."""
        # ME0=100, ME1=99, ME2=98, ..., ME10=90, ME11=89 (all improve)
        # But ME is capped at 10 so we search 1..10 — last useful is 10
        session = _make_sde_session(456, 1000, [{"typeID": 34, "quantity": 100}])
        result = compute_optimal_me(456, session)
        assert result == 10, f"Expected 10, got {result}"

    def test_qty_1_optimal_me_is_0(self):
        """A material with qty=1: ceil(1 * anything) = 1 — no savings ever."""
        session = _make_sde_session(789, 1000, [{"typeID": 34, "quantity": 1}])
        result = compute_optimal_me(789, session)
        assert result == 0, f"Expected 0, got {result}"

    def test_blueprint_not_found_returns_0(self):
        """Blueprint not found → return 0."""
        session = MagicMock()
        query_mock = MagicMock()
        filter_mock = MagicMock()
        filter_mock.first.return_value = None
        query_mock.filter.return_value = filter_mock
        session.query.return_value = query_mock

        result = compute_optimal_me(999, session)
        assert result == 0

    def test_no_materials_returns_0(self):
        """Blueprint with no materials → return 0."""
        session = _make_sde_session(111, 1000, [])
        result = compute_optimal_me(111, session)
        assert result == 0

    def test_multiple_materials_returns_max(self):
        """With two materials, both needing ME10, optimal is ME10."""
        # qty=20: improvements at ME5 (20→19) and ME10 (19→18) → last_useful_me=10
        # qty=10: only improvement at ME10 (10→9) → last_useful_me=10
        session = _make_sde_session(222, 1000, [
            {"typeID": 34, "quantity": 20},
            {"typeID": 35, "quantity": 10},
        ])
        result = compute_optimal_me(222, session)
        # Max of (10, 10) = 10
        assert result == 10, f"Expected 10, got {result}"

    def test_qty_20_optimal_me_is_10(self):
        """Verify qty=20 gives optimal ME=10 (improvements at both ME5 and ME10)."""
        session = _make_sde_session(333, 1000, [{"typeID": 34, "quantity": 20}])
        result = compute_optimal_me(333, session)
        # ME5: ceil(20*0.95)=ceil(19.0)=19 (improvement from 20)
        # ME10: ceil(20*0.90)=ceil(18.0)=18 (improvement from 19)
        # Last useful ME = 10 (final improvement within 1..10 range)
        assert result == 10, f"Expected 10, got {result}"

    def test_runs_raise_the_optimum_for_a_small_quantity(self):
        session = _make_sde_session(321, 1000, [{"typeID": 34, "quantity": 5}])
        assert compute_optimal_me(321, session) == 0
        assert compute_optimal_me(321, session, runs=10) == 10

    def test_one_unit_per_run_never_benefits_at_any_run_count(self):
        session = _make_sde_session(789, 1000, [{"typeID": 34, "quantity": 1}])
        assert compute_optimal_me(789, session, runs=100) == 0


# ---------------------------------------------------------------------------
# compute_optimal_te
# ---------------------------------------------------------------------------

class TestComputeOptimalTe:
    """compute_optimal_te returns EVE's TE percentage: 2% per research level,
    0..20 in steps of 2 (Task 8c). The threshold is the minimum time saved per
    level, as % of the base time, averaged over the levels up to the target."""

    def test_every_level_saves_the_full_two_percent_so_the_target_is_te_20(self):
        session = _make_sde_session(100, 10000, [])
        assert compute_optimal_te(100, session, time_savings_threshold_pct=1.0) == 20

    def test_default_threshold_targets_te_20(self):
        session = _make_sde_session(500, 1000, [])
        assert compute_optimal_te(500, session) == 20

    def test_a_threshold_of_exactly_two_percent_is_met_without_float_noise(self):
        # 100 s x (1 - 0.06) is 94.00000000000001 in floats; ceil would give 95.
        session = _make_sde_session(600, 100, [])
        assert compute_optimal_te(600, session, time_savings_threshold_pct=2.0) == 20

    def test_a_threshold_above_the_two_percent_step_researches_nothing(self):
        session = _make_sde_session(400, 1000, [])
        assert compute_optimal_te(400, session, time_savings_threshold_pct=2.5) == 0

    def test_ceil_noise_regression_a_10_second_blueprint_still_targets_te_20(self):
        """SDE simulation: 86 blueprints have a 10 s base time. Level 1 rounds
        ceil(9.8) back up to 10 s (no saving), which stopped the old loop at
        TE 0; level 10 saves 2 s = 20%, 2% per level."""
        session = _make_sde_session(700, 10, [])
        assert compute_optimal_te(700, session, time_savings_threshold_pct=1.0) == 20

    def test_the_target_is_always_an_even_percentage_in_0_to_20(self):
        for base_time in (1, 3, 7, 10, 15, 25, 33, 99, 101, 1234, 600000):
            for threshold in (0.1, 0.5, 1.0, 1.5, 2.0):
                result = compute_optimal_te(1, _make_sde_session(1, base_time, []), threshold)
                assert result % 2 == 0 and 0 <= result <= 20, (base_time, threshold, result)

    def test_blueprint_not_found_returns_0(self):
        """Blueprint not found -> return 0."""
        session = MagicMock()
        query_mock = MagicMock()
        filter_mock = MagicMock()
        filter_mock.first.return_value = None
        query_mock.filter.return_value = filter_mock
        session.query.return_value = query_mock

        result = compute_optimal_te(999, session)
        assert result == 0

    def test_zero_base_time_returns_0(self):
        """Blueprint with time=0 -> return 0."""
        session = _make_sde_session(300, 0, [])
        result = compute_optimal_te(300, session)
        assert result == 0


class TestTeAdjustedBatchTime:
    def test_te_is_a_percentage_applied_to_the_batch_time_with_one_ceiling(self):
        # 3 runs x 101 s at TE 20: ceil(303 x 0.8) = ceil(242.4) = 243.
        assert te_adjusted_batch_time(101, 3, 20) == 243

    def test_te_0_is_the_base_time(self):
        assert te_adjusted_batch_time(101, 3, 0) == 303

    def test_exact_integer_arithmetic(self):
        assert te_adjusted_batch_time(100, 1, 6) == 94
