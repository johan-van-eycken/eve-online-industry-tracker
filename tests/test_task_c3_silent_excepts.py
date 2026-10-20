"""Task C3: the broad, silent excepts in src/flask_app and src/streamlit_ui.

A failure that hides missing data now logs and gives a reason (in the API
response or as an st.warning on the page); an unexpected exception now
propagates instead of being swallowed."""
from __future__ import annotations

import logging
import threading
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.errors import ServiceError


@pytest.fixture
def st_messages(monkeypatch):
    """Record st.warning / st.error calls made by the Streamlit modules."""
    import streamlit as st

    messages: list[tuple[str, str]] = []
    monkeypatch.setattr(st, "warning", lambda body, *a, **k: messages.append(("warning", str(body))))
    monkeypatch.setattr(st, "error", lambda body, *a, **k: messages.append(("error", str(body))))
    return messages


# --- flask_app: invalid request values give a 400 with the reason ----------------------------


def test_portfolio_plan_request_rejects_a_non_numeric_capital_limit():
    from flask_app.routes.industry import _portfolio_plan_request_from_payload

    with pytest.raises(ServiceError) as excinfo:
        _portfolio_plan_request_from_payload({"capital_limit_isk": "lots"})
    assert excinfo.value.status_code == 400
    assert "capital_limit_isk" in excinfo.value.message


def test_portfolio_plan_request_keeps_defaults_for_absent_values():
    from flask_app.routes.industry import _portfolio_plan_request_from_payload

    req = _portfolio_plan_request_from_payload({"capital_limit_isk": 5e9, "manufacturing_slots_available": 3})
    assert req.capital_limit_isk == 5e9
    assert req.manufacturing_slots_available == 3
    assert req.planning_horizon_hours == 24.0
    assert req.research_slots_available == 0


def test_candidate_directive_rejects_a_non_integer_override():
    from flask_app.routes.industry import _candidate_directives_payload

    with pytest.raises(ServiceError) as excinfo:
        _candidate_directives_payload([{"overview_row_id": "r1", "max_batches_override": "many"}])
    assert excinfo.value.status_code == 400
    assert "max_batches_override" in excinfo.value.message
    # Absent overrides stay None.
    [directive] = _candidate_directives_payload([{"overview_row_id": "r1"}])
    assert directive.max_batches_override is None


def test_portfolio_candidates_route_returns_400_for_a_bad_planning_horizon(monkeypatch):
    from flask_app.app import create_app
    import flask_app.routes.industry as routes

    monkeypatch.setattr(routes, "require_ready", lambda s: None)
    monkeypatch.setattr(routes, "require_sde_ready", lambda s: None)
    client = create_app().test_client()
    resp = client.get("/industry_products/1/portfolio_candidates?planning_horizon_hours=soon")
    assert resp.status_code == 400
    assert "planning_horizon_hours" in resp.get_json()["message"]


# --- flask_app: programming errors are no longer swallowed -----------------------------------


def test_register_thread_surfaces_a_state_without_a_thread_registry():
    from flask_app.background_jobs import register_thread

    with pytest.raises(AttributeError):
        register_thread(SimpleNamespace(), "t", threading.Thread(target=lambda: None))


def test_stop_background_jobs_tolerates_joining_the_current_thread():
    from flask_app.background_jobs import register_thread, stop_background_jobs
    from flask_app.state import AppState

    state = AppState()
    register_thread(state, "self", threading.current_thread())
    stop_background_jobs(state, join_timeout_seconds=0.1)  # join() raises RuntimeError: kept best-effort
    assert state.shutdown_event.is_set()


# --- streamlit_ui: API failures carry a reason -----------------------------------------------


def test_fetch_director_corporations_raises_with_the_api_reason(monkeypatch):
    import streamlit_ui.api.client as client
    from streamlit_ui.api.corporations import fetch_director_corporations

    monkeypatch.setattr(client, "api_get_or_error", lambda path, **k: (None, "HTTP 503"))
    with pytest.raises(RuntimeError, match="HTTP 503"):
        fetch_director_corporations()


def test_market_orders_warns_when_director_corporations_fail(monkeypatch, st_messages):
    import streamlit_ui.api.corporations as corps
    from streamlit_ui.pages import marketorders as page

    def boom():
        raise RuntimeError("HTTP 503")

    monkeypatch.setattr(corps, "fetch_director_corporations", boom)
    assert page._get_director_corp_ids() == []
    assert any(kind == "warning" and "HTTP 503" in body for kind, body in st_messages)


@pytest.mark.parametrize("fetch_name", ["fetch_blueprint_skill_qualification", "fetch_reorder_alerts", "fetch_type_track_record"])
def test_industry_builder_optional_fetches_raise_on_api_failure(monkeypatch, fetch_name):
    import streamlit_ui.api.client as client
    import streamlit_ui.api.industry_builder as api

    monkeypatch.setattr(client, "api_get", lambda path, **k: {"status": "error", "message": "backend down"})
    fn = getattr(api, fetch_name)
    fn.clear()
    with pytest.raises(RuntimeError, match="backend down"):
        fn((34,))


def test_portfolio_planner_warns_when_active_jobs_fail(monkeypatch, st_messages):
    from streamlit_ui.pages import portfolio_planner as page

    def boom():
        raise RuntimeError("Failed to load active industry jobs")

    monkeypatch.setattr(page, "fetch_active_industry_jobs", boom)
    # The user ruled: the page stops with an error (tests/test_final_f3_ui.py).
    assert page._fetch_jobs_data() is None
    assert any(kind == "error" and "active industry jobs" in body.lower() for kind, body in st_messages)


def test_portfolio_planner_optional_fetch_warns_with_the_reason(st_messages):
    from streamlit_ui.pages import portfolio_planner as page

    def boom(_ids):
        raise RuntimeError("backend down")

    assert page._fetch_optional(boom, (1,), what="skill qualification") == {}
    assert any(kind == "warning" and "skill qualification" in body and "backend down" in body
               for kind, body in st_messages)


def test_portfolio_planner_optional_fetch_lets_programming_errors_through():
    from streamlit_ui.pages import portfolio_planner as page

    def broken(_ids):
        raise TypeError("bug")

    with pytest.raises(TypeError):
        page._fetch_optional(broken, (1,), what="track record")


def test_realized_profit_warns_when_wallet_owners_fail(monkeypatch, st_messages):
    from streamlit_ui.pages import realized_profit as page

    def boom():
        raise RuntimeError("Failed to load characters")

    monkeypatch.setattr(page, "fetch_characters", boom)
    page._reset_load_warnings()
    assert page._character_payloads() == []
    assert page._character_name_map() == {}
    # The user ruled: an error that stops the page (tests/test_final_f3_ui.py), once per run.
    errors = [body for kind, body in st_messages if kind == "error"]
    assert len(errors) == 1
    assert "Failed to load characters" in errors[0]


def test_realized_profit_warns_when_director_corporations_fail(monkeypatch, st_messages):
    import streamlit_ui.api.corporations as corps
    from streamlit_ui.pages import realized_profit as page

    def boom():
        raise RuntimeError("HTTP 503")

    monkeypatch.setattr(corps, "fetch_director_corporations", boom)
    page._reset_load_warnings()
    assert page._corporation_payloads() == []
    assert any(kind == "error" and "HTTP 503" in body for kind, body in st_messages)


def test_corporation_master_wallet_balance_is_none_when_unreadable(caplog):
    from streamlit_ui.pages import corporations as page

    with caplog.at_level(logging.WARNING):
        assert page._extract_master_wallet_balance("{not json") is None
    assert any("wallet" in r.getMessage().lower() for r in caplog.records)
    assert page._extract_master_wallet_balance([{"division": 1, "balance": 12.5}]) == 12.5
    # No wallets at all: "no wallets reported", not ISK 0 (tests/test_final_f3_ui.py).
    assert page._extract_master_wallet_balance(None) is None


def test_corporation_wallet_label_says_unavailable_instead_of_zero():
    from streamlit_ui.pages import corporations as page

    assert "unavailable" in page._master_wallet_label(None).lower()
    assert page._master_wallet_label(1.0).startswith("ISK")


def test_location_info_reports_an_api_failure(monkeypatch):
    import streamlit_ui.components.assets_ui as assets_ui

    monkeypatch.setattr(assets_ui, "api_post", lambda path, payload: None)
    assets_ui.fetch_location_info.clear()
    out = assets_ui.fetch_location_info([60003760])
    assert out["status"] == "error"
    assert out["message"]


def test_apply_location_names_warns_when_names_fail(monkeypatch, st_messages):
    import pandas as pd
    import streamlit_ui.components.assets_ui as assets_ui

    monkeypatch.setattr(assets_ui, "fetch_location_info",
                        lambda ids: {"status": "error", "message": "lookup failed", "data": {}})
    assets_ui.apply_location_names(pd.DataFrame({"top_location_id": [60003760]}))
    assert any(kind == "warning" and "lookup failed" in body for kind, body in st_messages)


def test_wallet_location_names_warn_when_the_lookup_fails(monkeypatch, st_messages):
    import streamlit_ui.components.wallet_ui as wallet_ui

    monkeypatch.setattr(wallet_ui, "api_post", lambda path, payload: None)
    assert wallet_ui.fetch_location_name_map([60003760]) == {}
    assert any(kind == "warning" and "location" in body.lower() for kind, body in st_messages)


def test_page_preferences_log_an_unreadable_file(monkeypatch, tmp_path, caplog):
    import streamlit_ui.state.page_preferences as prefs

    path = tmp_path / "prefs.json"
    path.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(prefs, "_preferences_file_path", lambda: path)
    with caplog.at_level(logging.WARNING):
        assert prefs.load_page_preferences("x") == {}
    assert any("preferences" in r.getMessage().lower() for r in caplog.records)


# --- industry profiles ---------------------------------------------------------------------


def test_rig_options_warn_when_the_rigs_do_not_load(monkeypatch, st_messages):
    from streamlit_ui.pages import industry_profiles as page

    monkeypatch.setattr(page, "cached_api_get", lambda path, *a, **k: {"status": "error", "message": "SDE missing"})
    rig_options, rig_bonus_map, rig_option_ids = page._load_rig_options()
    assert rig_options == {0: "None"} and rig_option_ids == [0]
    assert any(kind == "warning" and "SDE missing" in body for kind, body in st_messages)


def test_rig_options_skip_a_malformed_rig_but_keep_the_rest(monkeypatch, st_messages):
    from streamlit_ui.pages import industry_profiles as page

    rigs = [{"type_id": "x", "name": "Broken"}, {"type_id": 43920, "name": "Standup M-Set Good Rig"}]
    monkeypatch.setattr(page, "cached_api_get", lambda path, *a, **k: {"status": "success", "data": rigs})
    rig_options, _, _ = page._load_rig_options()
    assert 43920 in rig_options


def test_manufacturing_cost_index_is_none_for_a_malformed_payload():
    from streamlit_ui.pages import industry_profiles as page

    assert page._extract_manufacturing_cost_index({"cost_indices": [{"activity": "manufacturing", "cost_index": "x"}]}) is None
    assert page._extract_manufacturing_cost_index({"cost_indices": [{"activity": "manufacturing", "cost_index": 0.05}]}) == 0.05
    # No manufacturing entry is unknown, not 0.0 (the user ruled).
    assert page._extract_manufacturing_cost_index({"cost_indices": []}) is None


def test_facility_bonuses_give_a_reason_when_they_do_not_load(monkeypatch):
    from streamlit_ui.pages import industry_profiles as page

    monkeypatch.setattr(page, "cached_api_get", lambda path, *a, **k: None)
    me, te, cost, reason = page._fetch_facility_bonus_pcts(35825)
    assert (me, te, cost) == (0.0, 0.0, 0.0)
    assert reason

    monkeypatch.setattr(page, "cached_api_get", lambda path, *a, **k: {
        "status": "success", "data": {"bonuses": {"material_reduction": 0.01, "time_reduction": 0.15, "cost_reduction": 0.03}}})
    me, te, cost, reason = page._fetch_facility_bonus_pcts(35825)
    assert reason is None
    assert (me, te, cost) == pytest.approx((1.0, 15.0, 3.0))


def test_profile_context_logs_when_structure_lists_fail(monkeypatch, caplog):
    from streamlit_ui.pages import industry_profiles as page

    def fake_get(path, *a, **k):
        if path.startswith("/industry_system_cost_index/"):
            return {"status": "success", "data": {"cost_indices": [{"activity": "manufacturing", "cost_index": 0.05}]}}
        if path.startswith("/public_structures/"):
            return {"status": "success", "data": [{"station_id": 1, "type_name": "Raitaru", "type_id": 35825}]}
        return None  # corporation structures and solar systems fail

    monkeypatch.setattr(page, "cached_api_get", fake_get)
    with caplog.at_level(logging.WARNING):
        context = page._load_profile_context(90000001, [{"system_id": 31000200}])
    # A failing corporation structure list no longer costs the public ones.
    assert context["public_structure_type_id_by_id"] == {1: 35825}
    assert any("corporation structures" in r.getMessage() for r in caplog.records)
