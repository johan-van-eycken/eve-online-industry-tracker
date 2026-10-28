"""Tests for optimal_me_for_quantities() in blueprints.py. (TE has no optimum: it is
always researched to the max TE setting, Task 8c fix round 1.)"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.infrastructure.sde.blueprints import (
    optimal_me_for_quantities,
)


class TestOptimalMeForQuantities:
    def test_qty_10_optimal_me_is_10(self):
        """A material with qty=10: savings only appear at ME10 (ceil(9.0)=9 vs ceil(9.1)=10)."""
        assert optimal_me_for_quantities([10]) == 10

    def test_qty_100_optimal_me_is_10(self):
        """A material with qty=100: every ME level saves 1 unit until ME10."""
        assert optimal_me_for_quantities([100]) == 10

    def test_qty_1_optimal_me_is_0(self):
        """A material with qty=1: ceil(1 * anything) = 1 — no savings ever."""
        assert optimal_me_for_quantities([1]) == 0

    def test_no_materials_returns_0(self):
        assert optimal_me_for_quantities([]) == 0

    def test_multiple_materials_returns_max(self):
        # qty=20: improvements at ME5 (20→19) and ME10 (19→18); qty=10: only at ME10.
        assert optimal_me_for_quantities([20, 10]) == 10

    def test_qty_20_optimal_me_is_10(self):
        # ME5: ceil(20*0.95)=19, ME10: ceil(20*0.90)=18 -> last useful ME = 10
        assert optimal_me_for_quantities([20]) == 10

    def test_runs_raise_the_optimum_for_a_small_quantity(self):
        assert optimal_me_for_quantities([5]) == 0
        assert optimal_me_for_quantities([5], runs=10) == 10

    def test_one_unit_per_run_never_benefits_at_any_run_count(self):
        assert optimal_me_for_quantities([1], runs=100) == 0
