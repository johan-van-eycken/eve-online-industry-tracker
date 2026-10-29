"""A copy row reads 'N copies × M runs'."""
from __future__ import annotations

from streamlit_ui.components.daily_planner.tab_actions import action_quantity_label


def test_a_copy_row_shows_copies_times_runs_per_copy():
    assert action_quantity_label({"action_type": "copy", "quantity": 2, "runs": 6}) == "2 copies × 3 runs"
    assert action_quantity_label({"action_type": "copy", "quantity": 1, "runs": 1}) == "1 copy × 1 run"


def test_other_rows_keep_their_labels():
    assert action_quantity_label({"action_type": "manufacture", "quantity": 10, "runs": 10}) == "×10 runs"
    assert action_quantity_label({"action_type": "buy_materials", "quantity": 1500, "runs": None}) == "×1,500"
    assert action_quantity_label({"action_type": "copy", "quantity": None, "runs": None}) == ""
