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


def test_the_sections_follow_the_given_action_types_order():
    """FU-32: _ACTION_ORDER is derived from CHARACTER_ACTION_TYPES, so the
    order and the set live in one place; only the labels are local."""
    assert tab_actions._action_sections(("manufacture", "deliver")) == [
        ("manufacture", "MANUFACTURE", "🟢"),
        ("deliver", "DELIVER", "🔴"),
    ]
    assert tab_actions._ACTION_ORDER == tab_actions._action_sections(CHARACTER_ACTION_TYPES)


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
    me, te = _queued_pair("2026-01-01T12:00:00Z", me_status="done")
    assert queued_te_startable(te, [me, te], now) is True
    me, te = _queued_pair("2026-01-02T12:00:00Z", me_status="done")
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


def test_a_queued_te_waits_while_its_me_research_was_never_started():
    """Carry 14: the ME's estimated completion passing proves nothing while
    the ME action is still pending (never marked started): the BPO may still
    be free, or the ME job may have started late."""
    from datetime import datetime, timezone

    from streamlit_ui.components.daily_planner.status_bar import queued_te_startable

    now = datetime(2026, 1, 2, tzinfo=timezone.utc)
    me, te = _queued_pair("2026-01-01T12:00:00Z", me_status="pending")
    assert queued_te_startable(te, [me, te], now) is False
    assert pending_character_action_count([me, te], now) == 1   # the ME itself


# --- Task 10 fix round 1, minor 2: the ME job's real end, from the industry jobs ---

def _me_job(end_date, type_id=7):
    return {"activity_id": 4, "blueprint_type_id": type_id, "character_id": 1, "end_date": end_date}


def test_a_queued_te_follows_the_active_me_jobs_end_date_over_the_estimate():
    from datetime import datetime, timezone

    from streamlit_ui.components.daily_planner.status_bar import queued_te_status

    now = datetime(2026, 1, 2, tzinfo=timezone.utc)
    # The estimate has passed, but the real ME job (started late) runs until tomorrow.
    me, te = _queued_pair("2026-01-01T12:00:00Z", me_status="done")
    startable, note = queued_te_status(te, [me, te], now, jobs=[_me_job("2026-01-03T00:00:00Z")])
    assert startable is False and "from the industry jobs" in note
    # Fix round 2, item 3: an ME job of the type that ends before the estimate
    # may be a sibling BPO's: the TE waits for the later of the two.
    me, te = _queued_pair("2026-01-05T00:00:00Z", me_status="done")
    startable, note = queued_te_status(te, [me, te], now, jobs=[_me_job("2026-01-01T06:00:00Z")])
    assert startable is False and "estimate" in note
    later = datetime(2026, 1, 6, tzinfo=timezone.utc)
    assert queued_te_status(te, [me, te], later, jobs=[_me_job("2026-01-01T06:00:00Z")])[0] is True
    # Both passed: startable.
    me, te = _queued_pair("2026-01-01T00:00:00Z", me_status="done")
    assert queued_te_status(te, [me, te], now, jobs=[_me_job("2026-01-01T06:00:00Z")])[0] is True
    # Another blueprint's ME job does not count: the (future) estimate decides.
    me, te = _queued_pair("2026-01-05T00:00:00Z", me_status="done")
    startable, _ = queued_te_status(te, [me, te], now, jobs=[_me_job("2026-01-01T06:00:00Z", type_id=8)])
    assert startable is False


def test_without_an_active_me_job_the_estimate_is_used_and_the_note_says_so():
    from datetime import datetime, timezone

    from streamlit_ui.components.daily_planner.status_bar import queued_te_status

    now = datetime(2026, 1, 2, tzinfo=timezone.utc)
    me, te = _queued_pair("2026-01-01T12:00:00Z", me_status="done")
    startable, note = queued_te_status(te, [me, te], now, jobs=[])
    assert startable is True and "no active ME job" in note and "estimate" in note
    startable, note = queued_te_status(te, [me, te], now, jobs=None)
    assert startable is True and "estimate" in note and "not loaded" in note


def test_the_pending_count_uses_the_jobs_too():
    from datetime import datetime, timezone

    now = datetime(2026, 1, 2, tzinfo=timezone.utc)
    me, te = _queued_pair("2026-01-01T12:00:00Z", me_status="done")
    assert pending_character_action_count([me, te], now) == 1
    assert pending_character_action_count([me, te], now, jobs=[_me_job("2026-01-03T00:00:00Z")]) == 0
