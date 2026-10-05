"""tab_actions must tolerate action types the planner no longer produces."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import logging  # noqa: E402

from streamlit_ui.components.daily_planner import tab_actions  # noqa: E402
from streamlit_ui.components.daily_planner.status_bar import (  # noqa: E402
    CHARACTER_ACTION_TYPES,
    pending_character_action_count,
)
from streamlit_ui.components.daily_planner.tab_actions import _character_level_actions  # noqa: E402


def test_legacy_relist_order_rows_are_not_shown_nor_block_day_complete():
    actions = [
        {"id": 1, "action_type": "manufacture", "status": "done"},
        {"id": 2, "action_type": "relist_order", "status": "pending"},  # persisted before removal
        {"id": 3, "action_type": "buy_materials", "status": "pending"},  # corp-level, Tab 2
    ]

    visible = _character_level_actions(actions)

    assert [a["id"] for a in visible] == [1]


def test_an_unknown_action_type_is_logged_once(monkeypatch, caplog):
    monkeypatch.setattr(tab_actions, "_WARNED_UNKNOWN_ACTION_TYPES", set())
    actions = [{"id": 2, "action_type": "relist_order", "status": "pending"},
               {"id": 3, "action_type": "buy_bpo", "status": "pending"}]
    with caplog.at_level(logging.WARNING):
        _character_level_actions(actions)
        _character_level_actions(actions)
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "relist_order" in warnings[0] and "buy_bpo" not in warnings[0]


def test_the_tab_renders_exactly_the_character_action_types():
    assert tuple(t for t, _, _ in tab_actions._ACTION_ORDER) == CHARACTER_ACTION_TYPES


def test_the_pending_count_ignores_legacy_and_corp_level_rows():
    actions = [{"action_type": "manufacture", "status": "pending"},
               {"action_type": "relist_order", "status": "pending"},
               {"action_type": "buy_materials", "status": "pending"},
               {"action_type": "invent", "status": "done"}]
    assert pending_character_action_count(actions) == 1
