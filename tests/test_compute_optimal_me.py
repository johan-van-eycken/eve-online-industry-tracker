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
    def test_large_base_time_threshold_never_reached(self):
        """base_time=10000: each TE saves 100 seconds = 1.0%; with threshold=1.0 (< 1.0 never true for ==1.0)."""
        # saved_pct = 1.0% at every level; 1.0 < 1.0 is False → returns 10
        session = _make_sde_session(100, 10000, [])
        result = compute_optimal_te(100, session, time_savings_threshold_pct=1.0)
        assert result == 10, f"Expected 10, got {result}"

    def test_small_base_time_threshold_exceeded_early(self):
        """base_time=10: each TE saves ceil-based; with threshold=15%, stops very early."""
        # base_time=10, threshold=15%
        # TE1: ceil(10*0.99)=10 → saved=(10-10)/10*100=0% < 15% → stop → return TE-1=0
        session = _make_sde_session(200, 10, [])
        result = compute_optimal_te(200, session, time_savings_threshold_pct=15.0)
        assert result == 0, f"Expected 0, got {result}"

    def test_blueprint_not_found_returns_0(self):
        """Blueprint not found → return 0."""
        session = MagicMock()
        query_mock = MagicMock()
        filter_mock = MagicMock()
        filter_mock.first.return_value = None
        query_mock.filter.return_value = filter_mock
        session.query.return_value = query_mock

        result = compute_optimal_te(999, session)
        assert result == 0

    def test_zero_base_time_returns_0(self):
        """Blueprint with time=0 → return 0."""
        session = _make_sde_session(300, 0, [])
        result = compute_optimal_te(300, session)
        assert result == 0

    def test_threshold_exactly_met(self):
        """base_time=1000, threshold=1.5%: each TE saves 10 = 1.0% < 1.5% → stops at TE1 → returns 0."""
        session = _make_sde_session(400, 1000, [])
        result = compute_optimal_te(400, session, time_savings_threshold_pct=1.5)
        assert result == 0, f"Expected 0, got {result}"

    def test_default_threshold(self):
        """Default threshold=1.0%: base_time=1000 → each TE saves 10/1000=1.0%, not < 1.0% → returns 10."""
        session = _make_sde_session(500, 1000, [])
        result = compute_optimal_te(500, session)
        assert result == 10, f"Expected 10, got {result}"
