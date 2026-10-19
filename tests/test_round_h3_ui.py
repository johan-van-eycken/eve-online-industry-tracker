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


# =============================================================================================
# 2. Realized profit: a failed ledger read is an error, not "no sales", and is not cached
# =============================================================================================


@pytest.fixture
def realized_profit_api():
    from streamlit_ui.api import realized_profit as api

    api.fetch_realized_profit.clear()
    yield api
    api.fetch_realized_profit.clear()


def test_a_failing_realized_profit_api_raises_with_the_reason(monkeypatch, realized_profit_api):
    api = realized_profit_api
    monkeypatch.setattr(api, "api_get_or_error", lambda path, timeout_seconds=None: (None, "request failed: read timed out"))
    with pytest.raises(RuntimeError, match="read timed out"):
        api.fetch_realized_profit(owner_scope="character", owner_id=None)


def test_an_http_500_body_raises_its_message_and_is_not_cached(monkeypatch, realized_profit_api):
    api = realized_profit_api
    calls = []
    responses = [(None, "ledger build failed: fake DB lock"),
                 ({"status": "error", "message": "fake 200 error body"}, None),
                 ({"status": "success", "data": {"rows": []}}, None)]
    monkeypatch.setattr(api, "api_get_or_error", lambda path, timeout_seconds=None: calls.append(path) or responses.pop(0))
    with pytest.raises(RuntimeError, match="fake DB lock"):
        api.fetch_realized_profit(owner_scope="corporation", owner_id=None)
    with pytest.raises(RuntimeError, match="fake 200 error body"):
        api.fetch_realized_profit(owner_scope="corporation", owner_id=None)
    assert api.fetch_realized_profit(owner_scope="corporation", owner_id=None)["status"] == "success"
    assert len(calls) == 3  # the failures were not cached


def _render_realized_profit(monkeypatch, *, owner_option, fetch):
    import streamlit as st
    from streamlit_ui.pages import realized_profit as page

    monkeypatch.setattr(page, "require_aggrid", lambda: SimpleNamespace(js_code=lambda s: s))
    monkeypatch.setattr(page, "_character_name_map", lambda: {90000001: "Fake Pilot"})
    monkeypatch.setattr(page, "_owner_selector_options", lambda: [owner_option])
    monkeypatch.setattr(page, "fetch_realized_profit", fetch)
    monkeypatch.setattr(st, "button", lambda *a, **k: False)
    page.render()


@pytest.mark.parametrize("owner_option", [("all", 0, "All"), ("character", 90000001, "Fake Pilot")])
def test_realized_profit_render_stops_with_an_error_when_the_ledger_fails(monkeypatch, st_messages, owner_option):
    def fails(**kwargs):
        raise RuntimeError("HTTP 500")

    _render_realized_profit(monkeypatch, owner_option=owner_option, fetch=fails)
    errors = [body for kind, body in st_messages if kind == "error"]
    assert len(errors) == 1 and "HTTP 500" in errors[0]
    assert not any("No realized sales" in body for _kind, body in st_messages)


def test_the_assumed_fee_modes_are_the_backend_tuple():
    from eve_online_industry_tracker.application.characters import realized_profit as backend
    from streamlit_ui.pages import realized_profit as page

    assert page._ASSUMED_FEE_CAPTURE_MODES is backend.ASSUMED_FEE_CAPTURE_MODES


# =============================================================================================
# 3. Ore calculator: the facility's NPC station name note and problem are shown
# =============================================================================================


def test_the_station_name_problem_is_a_warning_and_the_note_a_caption():
    from streamlit_ui.pages import ore_calculator as page

    facility = {"id": 1, "name": "NPC station 60000001", "npc_station_name_problem": "no NPC station 60000001 in the SDE",
                "npc_station_name_note": None}
    assert page._facility_station_name_messages(facility) == [
        ("warning", "NPC station name: no NPC station 60000001 in the SDE")]
    facility = {"id": 1, "name": "Fakeburg VIII", "npc_station_name_problem": None,
                "npc_station_name_note": "the SDE name may lack the unique name; ESI unreachable"}
    assert page._facility_station_name_messages(facility) == [
        ("caption", "NPC station name: the SDE name may lack the unique name; ESI unreachable")]


@pytest.mark.parametrize("facility", [{}, {"id": 1, "name": "Fake"}, None,
                                      {"npc_station_name_problem": None, "npc_station_name_note": ""}])
def test_no_station_name_message_without_the_keys(facility):
    from streamlit_ui.pages import ore_calculator as page

    assert page._facility_station_name_messages(facility) == []


def test_the_ore_calculator_renders_the_station_name_messages():
    from streamlit_ui.pages import ore_calculator as page

    assert "_facility_station_name_messages(" in inspect.getsource(page.render)


# =============================================================================================
# 4 and 8. Daily planner status bar: unavailable inputs and a market data job that never started warn
# =============================================================================================


def _status_bar(monkeypatch, *, plan, market_status):
    import tests.test_daily_planner_status_bar_freshness as freshness
    from streamlit_ui.components.daily_planner import status_bar
    from streamlit_ui.state.daily_planner_page import DailyPlannerPageState

    fake = freshness._FakeSt()
    monkeypatch.setattr(status_bar, "st", fake)
    monkeypatch.setattr(status_bar, "get_status", lambda: {"status": "idle"})
    monkeypatch.setattr(status_bar, "get_plan", lambda: plan)
    monkeypatch.setattr(status_bar, "get_market_intel_status", lambda: market_status)
    status_bar.render_status_bar(DailyPlannerPageState(plan=plan))
    return fake


def test_unavailable_inputs_are_a_warning_not_a_caption(monkeypatch):
    import tests.test_daily_planner_status_bar_freshness as freshness

    plan = {**freshness._plan(0.95), "unavailable_inputs": {"margin_correlation": "fake margin read failed"}}
    fake = _status_bar(monkeypatch, plan=plan, market_status={})
    assert any("fake margin read failed" in w for w in fake.warnings)
    assert not any("fake margin read failed" in c for c in fake.captions)


def test_a_market_data_job_that_never_started_warns():
    from streamlit_ui.components.daily_planner import status_bar

    warning = status_bar.market_intel_warning({"running": False, "not_started": True, "last_error": None})
    assert warning and "Market data job not started" in warning
    assert status_bar.market_intel_warning({"running": True, "last_error": None}) is None
    assert status_bar.market_intel_warning({}) is None
    assert "fake 502" in status_bar.market_intel_warning({"last_error": "fake 502"})


def test_the_not_started_warning_reaches_the_status_bar(monkeypatch):
    import tests.test_daily_planner_status_bar_freshness as freshness

    fake = _status_bar(monkeypatch, plan=freshness._plan(0.95),
                       market_status={"running": False, "not_started": True, "last_error": None})
    assert any("Market data job not started" in w for w in fake.warnings)
