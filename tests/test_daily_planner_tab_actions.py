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


def test_a_missing_action_type_is_logged_as_missing_not_as_an_empty_string(monkeypatch, caplog):
    monkeypatch.setattr(tab_actions, "_WARNED_UNKNOWN_ACTION_TYPES", set())
    actions = [{"id": 1, "status": "pending"},
               {"id": 2, "action_type": None, "status": "pending"},
               {"id": 3, "action_type": "relist_order", "status": "pending"}]
    with caplog.at_level(logging.WARNING):
        _character_level_actions(actions)
    (message,) = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert "<missing>, relist_order" in message
    assert "(, " not in message and ", ," not in message


def test_a_te_queued_after_me_is_not_todays_work():
    """Task 8c fix round 4: a TE research queued behind its BPO's ME job
    cannot start today: it neither counts as pending nor blocks day complete."""
    actions = [{"action_type": "me_research", "status": "done"},
               {"action_type": "te_research", "status": "pending", "queued_after_me": True}]
    assert pending_character_action_count(actions) == 0
    assert tab_actions._todays(actions) == actions[:1]
    assert tab_actions._is_queued(actions[1]) and not tab_actions._is_queued(actions[0])


# --- Task 8c fix round 5: "Mark started" on a queued TE once the ME ends -------------

def _queued_pair(me_completion, me_status="pending"):
    me = {"action_type": "me_research", "status": me_status, "type_id": 7, "character_id": 1,
          "estimated_completion": me_completion}
    te = {"action_type": "te_research", "status": "pending", "type_id": 7, "character_id": 1,
          "queued_after_me": True, "estimated_completion": "2026-01-03T00:00:00Z"}
    return me, te


def test_a_queued_te_can_be_started_once_the_me_research_has_ended():
    from datetime import datetime, timezone

    from streamlit_ui.components.daily_planner.status_bar import queued_te_startable

    now = datetime(2026, 1, 2, tzinfo=timezone.utc)
    me, te = _queued_pair("2026-01-01T12:00:00Z")
    assert queued_te_startable(te, [me, te], now) is True
    me, te = _queued_pair("2026-01-02T12:00:00Z")
    assert queued_te_startable(te, [me, te], now) is False
    # Skipped ME: the BPO is free now.
    me, te = _queued_pair("2026-01-02T12:00:00Z", me_status="skipped")
    assert queued_te_startable(te, [me, te], now) is True
    # An unknown ME end keeps it waiting.
    me, te = _queued_pair(None, me_status="done")
    assert queued_te_startable(te, [me, te], now) is False
    # Not queued: not this helper's case.
    assert queued_te_startable(me, [me, te], now) is False


def test_a_startable_queued_te_is_todays_work_again():
    from datetime import datetime, timezone

    now = datetime(2026, 1, 2, tzinfo=timezone.utc)
    me, te = _queued_pair("2026-01-01T12:00:00Z", me_status="done")
    assert tab_actions._todays([me, te], now) == [me, te]
    assert pending_character_action_count([me, te], now) == 1
    me, te = _queued_pair("2026-01-02T12:00:00Z", me_status="done")
    assert tab_actions._todays([me, te], now) == [me]
    assert pending_character_action_count([me, te], now) == 0
