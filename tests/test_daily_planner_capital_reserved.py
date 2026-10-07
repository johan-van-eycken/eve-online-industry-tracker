"""Capital Reserved is the shopping list, not shopping list + job material costs."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from streamlit_ui.components.daily_planner import status_bar  # noqa: E402


def _a(action_type, cost, status="pending"):
    return {"action_type": action_type, "estimated_cost_isk": cost, "status": status}


def test_manufacture_material_cost_is_not_counted_on_top_of_buy_materials():
    actions = [
        _a("manufacture", 300.0),
        _a("sub_manufacture", 100.0),
        _a("buy_materials", 250.0),
        _a("buy_materials", 150.0),
    ]
    assert status_bar.compute_capital_reserved(actions) == 400.0


def test_buy_bpo_counts_and_non_pending_and_none_costs_are_ignored():
    actions = [
        _a("buy_bpo", 1000.0),
        _a("buy_materials", 50.0, status="done"),
        _a("buy_materials", None),
        _a("deliver", 9.0),
    ]
    assert status_bar.compute_capital_reserved(actions) == 1000.0


def test_empty_plan_reserves_nothing():
    assert status_bar.compute_capital_reserved([]) == 0


def test_capital_reserved_help_states_scope():
    h = status_bar.CAPITAL_RESERVED_HELP
    assert "shopping" in h.lower() and "install fees" in h and "0" in h
