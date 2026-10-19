"""Round H3, the Streamlit pages and the Flask routes: a failed or partial
read is named (a reason, a warning, an error or "unknown"), never a silent
0, default or overwrite. Fake values throughout."""
from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest


@pytest.fixture
def st_messages(monkeypatch):
    """Record st.warning / st.error / st.info / st.caption calls made by the Streamlit modules."""
    import streamlit as st

    messages: list[tuple[str, str]] = []
    for kind in ("warning", "error", "info", "caption"):
        monkeypatch.setattr(st, kind, lambda body, *a, _kind=kind, **k: messages.append((_kind, str(body))))
    return messages


# =============================================================================================
# 1. Industry profiles: "Save rigs" never overwrites the stored rigs with a partial rig list
# =============================================================================================

GOOD_RIG = 43920
LOST_RIG = 99999


def test_a_stored_rig_missing_from_the_list_stays_its_own_option():
    from streamlit_ui.pages import industry_profiles as page

    assert page._rig_select_option_ids([0, GOOD_RIG], stored_ids=[GOOD_RIG, LOST_RIG, 0]) == [0, GOOD_RIG, LOST_RIG]
    assert page._rig_select_option_ids([0, GOOD_RIG], stored_ids=[0, 0, 0]) == [0, GOOD_RIG]
    options = {0: "None", GOOD_RIG: "Good Rig"}
    assert page._rig_label(LOST_RIG, options) == f"Unknown rig (id {LOST_RIG})"
    assert page._rig_label(GOOD_RIG, options) == "Good Rig"


def test_save_rigs_is_blocked_while_the_rigs_failed_to_load():
    from streamlit_ui.pages import industry_profiles as page

    reason = page._rig_save_block_reason([0, 0, 0], rig_bonus_map={}, rigs_load_problem="HTTP 503")
    assert reason and "HTTP 503" in reason


def test_save_rigs_is_blocked_for_a_rig_without_its_bonuses():
    from streamlit_ui.pages import industry_profiles as page

    reason = page._rig_save_block_reason([GOOD_RIG, LOST_RIG, 0], rig_bonus_map={GOOD_RIG: {}}, rigs_load_problem=None)
    assert reason and str(LOST_RIG) in reason and str(GOOD_RIG) not in reason


def test_save_rigs_is_allowed_with_every_rig_known():
    from streamlit_ui.pages import industry_profiles as page

    assert page._rig_save_block_reason([GOOD_RIG, 0, 0], rig_bonus_map={GOOD_RIG: {}}, rigs_load_problem=None) is None


def test_the_rig_load_problem_is_returned(monkeypatch, st_messages):
    from streamlit_ui.pages import industry_profiles as page

    monkeypatch.setattr(page, "cached_api_get", lambda path, *a, **k: {"status": "error", "message": "HTTP 503"})
    rig_options, rig_bonus_map, rig_option_ids, problem = page._load_rig_catalog()
    assert rig_option_ids == [0] and rig_bonus_map == {}
    assert problem and "HTTP 503" in problem
    monkeypatch.setattr(page, "cached_api_get", lambda path, *a, **k: {
        "status": "success", "data": [{"type_id": GOOD_RIG, "name": "Standup M-Set Good Rig"}]})
    assert page._load_rig_catalog()[3] is None


def _render_profile(monkeypatch, *, rig_options, rig_bonus_map, rig_option_ids, rigs_load_problem):
    import streamlit as st
    from streamlit_ui.pages import industry_profiles as page

    buttons: dict[str, dict] = {}
    selects: dict[str, list] = {}
    puts: list = []

    def button(label, *a, key=None, **k):
        buttons[key or label] = {"label": label, **k}
        return key == "save_rigs_7" and not k.get("disabled")

    def selectbox(label, options=(), index=0, key=None, format_func=str, **k):
        selects[key or label] = [format_func(o) for o in options]
        return list(options)[index]

    monkeypatch.setattr(st, "button", button)
    monkeypatch.setattr(st, "selectbox", selectbox)
    monkeypatch.setattr(page, "api_put", lambda path, payload: puts.append(payload) or {"status": "success"})
    monkeypatch.setattr(page, "_rerun", lambda: None)
    profile = {"id": 7, "profile_name": "Fake Raitaru", "is_default": False, "location_name": "Fake",
               "location_type": None, "facility_type": "structure", "system_id": None,
               "rig_slot0_type_id": GOOD_RIG, "rig_slot1_type_id": LOST_RIG, "rig_slot2_type_id": None,
               "structure_rig_material_bonus": 0.02, "structure_rig_time_bonus": 0.2, "structure_rig_cost_bonus": 0.0}
    context = {"profile_warnings_by_id": {}, "system_cost_index_map": {}, "system_cost_index_reason": {},
               "solar_system_by_id": {}, "facility_bonuses_by_type_id": {}}
    page._render_existing_profiles([profile], rig_options=rig_options, rig_bonus_map=rig_bonus_map,
                                   rig_option_ids=rig_option_ids, profile_context=context,
                                   scc_rates=(0.04, 0.0), rigs_load_problem=rigs_load_problem)
    return buttons, selects, puts


def test_the_profile_keeps_a_lost_rig_and_disables_save_rigs(monkeypatch, st_messages):
    buttons, selects, puts = _render_profile(
        monkeypatch, rig_options={0: "None", GOOD_RIG: "Good Rig"}, rig_bonus_map={GOOD_RIG: {"effects": []}},
        rig_option_ids=[0, GOOD_RIG], rigs_load_problem=None)
    assert f"Unknown rig (id {LOST_RIG})" in selects["edit_rig1_7"]
    assert buttons["save_rigs_7"].get("disabled") is True
    assert puts == []
    assert any(kind == "warning" and str(LOST_RIG) in body for kind, body in st_messages)


def test_a_failed_rig_list_disables_save_rigs(monkeypatch, st_messages):
    buttons, selects, puts = _render_profile(
        monkeypatch, rig_options={0: "None"}, rig_bonus_map={}, rig_option_ids=[0],
        rigs_load_problem="Structure rigs could not be loaded (HTTP 503)")
    assert f"Unknown rig (id {GOOD_RIG})" in selects["edit_rig0_7"]
    assert buttons["save_rigs_7"].get("disabled") is True
    assert puts == []


def test_save_rigs_still_saves_when_every_rig_is_known(monkeypatch, st_messages):
    known = {GOOD_RIG: {"effects": []}, LOST_RIG: {"effects": []}}
    buttons, _selects, puts = _render_profile(
        monkeypatch, rig_options={0: "None", GOOD_RIG: "Good Rig", LOST_RIG: "Other Rig"}, rig_bonus_map=known,
        rig_option_ids=[0, GOOD_RIG, LOST_RIG], rigs_load_problem=None)
    assert not buttons["save_rigs_7"].get("disabled")
    assert puts and puts[0]["rig_slot0_type_id"] == GOOD_RIG and puts[0]["rig_slot1_type_id"] == LOST_RIG
