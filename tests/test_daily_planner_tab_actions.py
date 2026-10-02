"""tab_actions must tolerate action types the planner no longer produces."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from streamlit_ui.components.daily_planner.tab_actions import _character_level_actions  # noqa: E402


def test_legacy_relist_order_rows_are_not_shown_nor_block_day_complete():
    actions = [
        {"id": 1, "action_type": "manufacture", "status": "done"},
        {"id": 2, "action_type": "relist_order", "status": "pending"},  # persisted before removal
        {"id": 3, "action_type": "buy_materials", "status": "pending"},  # corp-level, Tab 2
    ]

    visible = _character_level_actions(actions)

    assert [a["id"] for a in visible] == [1]
