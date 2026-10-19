"""Final round F3, Streamlit side: the contract keys the backend groups add
(cost_lower_bound_reason, pricing_error, price_unknown_reason, lookup_error,
cost_index_unknown_reason) are rendered when present and tolerated when
absent; the user's rulings on the profile save and the failed active jobs;
and the small C3 points."""
from __future__ import annotations

import logging
import time
from types import SimpleNamespace

import pytest


@pytest.fixture
def st_messages(monkeypatch):
    """Record st.warning / st.error / st.dataframe calls made by the Streamlit modules."""
    import streamlit as st

    messages: list[tuple[str, str]] = []
    monkeypatch.setattr(st, "warning", lambda body, *a, **k: messages.append(("warning", str(body))))
    monkeypatch.setattr(st, "error", lambda body, *a, **k: messages.append(("error", str(body))))
    monkeypatch.setattr(st, "dataframe", lambda *a, **k: messages.append(("dataframe", "")))
    return messages


# =============================================================================================
# 1. Industry profile page
# =============================================================================================


def test_cost_index_with_a_manufacturing_entry_has_no_reason():
    from streamlit_ui.pages import industry_profiles as page

    payload = {"cost_indices": [{"activity": "manufacturing", "cost_index": 0.05}]}
    assert page._manufacturing_cost_index_and_reason(payload) == (0.05, None)
    assert page._extract_manufacturing_cost_index(payload) == 0.05


def test_a_missing_manufacturing_entry_is_unknown_not_zero():
    from streamlit_ui.pages import industry_profiles as page

    value, reason = page._manufacturing_cost_index_and_reason({"cost_indices": [{"activity": "copying", "cost_index": 0.01}]})
    assert value is None
    assert reason and "manufacturing" in reason
    assert page._extract_manufacturing_cost_index({"cost_indices": []}) is None
    assert page._cost_index_label(None) == "unknown"
    assert page._cost_index_label(0.0123) == "1.23%"


def test_the_backend_cost_index_reason_is_used_when_present():
    from streamlit_ui.pages import industry_profiles as page

    payload = {"cost_indices": [], "cost_index_unknown_reason": "system 30000142 has no manufacturing cost index"}
    value, reason = page._manufacturing_cost_index_and_reason(payload)
    assert value is None
    assert "30000142 has no manufacturing cost index" in reason


def test_a_lookup_error_on_the_cost_index_payload_is_the_reason():
    from streamlit_ui.pages import industry_profiles as page

    value, reason = page._manufacturing_cost_index_and_reason({"cost_indices": [], "lookup_error": "ESI timed out"})
    assert value is None
    assert "ESI timed out" in reason


def test_a_malformed_cost_index_is_unknown_with_a_reason():
    from streamlit_ui.pages import industry_profiles as page

    value, reason = page._manufacturing_cost_index_and_reason(
        {"cost_indices": [{"activity": "manufacturing", "cost_index": "x"}]})
    assert value is None and "malformed" in reason


def test_profile_context_warnings_name_each_contract_reason():
    from streamlit_ui.pages import industry_profiles as page

    context = {
        "id": 3,
        "cost_index_unknown_reason": "system 30000142 has no manufacturing cost index",
        "industry_system": {"solar_system_id": 30000142, "cost_indices": [], "lookup_error": "ESI service not initialized."},
        "facility": {"facility_id": 60003760, "tax": None, "lookup_error": "fake facility failure"},
    }
    warnings = page._profile_context_warnings(context)
    assert any("no manufacturing cost index" in w for w in warnings)
    assert any("ESI service not initialized." in w and "cost ind" in w.lower() for w in warnings)
    assert any("fake facility failure" in w and "facility" in w.lower() for w in warnings)


def test_profile_context_warnings_tolerate_absent_keys():
    from streamlit_ui.pages import industry_profiles as page

    assert page._profile_context_warnings(None) == []
    assert page._profile_context_warnings({}) == []
    assert page._profile_context_warnings({"id": 1, "industry_system": {"cost_indices": []},
                                           "facility": {"tax": 0.01}}) == []
    # A top-level lookup_error is shown too.
    assert page._profile_context_warnings({"lookup_error": "boom"}) == ["The industry profile context could not be read: boom"]


def test_profile_context_loads_the_contract_reasons_per_profile(monkeypatch, caplog):
    from streamlit_ui.pages import industry_profiles as page

    def fake_get(path, *a, **k):
        if path.startswith("/industry_system_cost_index/"):
            return {"status": "success", "data": {"cost_indices": [],
                                                  "cost_index_unknown_reason": "no manufacturing index in 31000200"}}
        if path == "/industry_profile_context/5":
            return {"status": "success", "data": {"id": 5, "facility": {"lookup_error": "facility gone"}}}
        if path == "/industry_profile_context/6":
            return {"status": "error", "message": "HTTP 503"}
        return {"status": "success", "data": []}

    monkeypatch.setattr(page, "cached_api_get", fake_get)
    context = page._load_profile_context(90000001, [{"id": 5, "system_id": 31000200}, {"id": 6, "system_id": None}])
    assert context["system_cost_index_map"][31000200] is None
    assert "no manufacturing index in 31000200" in context["system_cost_index_reason"][31000200]
    assert any("facility gone" in w for w in context["profile_warnings_by_id"][5])
    assert any("HTTP 503" in w for w in context["profile_warnings_by_id"][6])


def test_profile_save_is_blocked_while_the_facility_bonuses_do_not_load():
    from streamlit_ui.pages import industry_profiles as page

    reason = page._profile_save_block_reason(
        is_npc_station=False, structure_selected=True, structure_type_id=35825,
        bonus_reason="the structure type bonuses request failed (see the logs)")
    assert reason and "the structure type bonuses request failed" in reason

    # The structure's type is unknown: its bonuses cannot be loaded either.
    reason = page._profile_save_block_reason(
        is_npc_station=False, structure_selected=True, structure_type_id=None, bonus_reason=None)
    assert reason and "type" in reason


def test_profile_save_is_allowed_when_the_bonuses_load_or_do_not_apply():
    from streamlit_ui.pages import industry_profiles as page

    assert page._profile_save_block_reason(is_npc_station=False, structure_selected=True,
                                           structure_type_id=35825, bonus_reason=None) is None
    assert page._profile_save_block_reason(is_npc_station=True, structure_selected=True,
                                           structure_type_id=None, bonus_reason=None) is None
    # "No Station (Structure)": no structure, no facility bonus to load.
    assert page._profile_save_block_reason(is_npc_station=False, structure_selected=False,
                                           structure_type_id=None, bonus_reason=None) is None


def test_a_failed_facility_bonus_read_is_not_kept_in_the_cache(monkeypatch):
    import streamlit_ui.api.streamlit_client as streamlit_client
    from streamlit_ui.pages import industry_profiles as page

    responses = [{"status": "error", "message": "SDE busy"},
                 {"status": "success", "data": {"bonuses": {"material_reduction": 0.01}}}]
    monkeypatch.setattr(streamlit_client, "api_get", lambda path, timeout_seconds=None: responses.pop(0))
    page.cached_api_get.clear()
    try:
        assert page._fetch_facility_bonuses(35825) == ({}, "SDE busy")
        bonuses, reason = page._fetch_facility_bonuses(35825)  # the reload the blocked save asks for
        assert reason is None and bonuses["material_reduction"] == 0.01
    finally:
        page.cached_api_get.clear()


def test_rig_options_exclude_a_malformed_rig_and_warn(monkeypatch, st_messages, caplog):
    from streamlit_ui.pages import industry_profiles as page

    rigs = [{"type_id": "x", "name": "Broken"}, {"type_id": None, "name": "Also broken"},
            {"type_id": 43920, "name": "Standup M-Set Good Rig"}]
    monkeypatch.setattr(page, "cached_api_get", lambda path, *a, **k: {"status": "success", "data": rigs})
    with caplog.at_level(logging.WARNING):
        rig_options, rig_bonus_map, rig_option_ids = page._load_rig_options()
    assert set(rig_options) == {0, 43920}
    assert set(rig_bonus_map) == {43920}
    assert rig_option_ids == [0, 43920]
    assert any("skipped 2 structure rig(s)" in r.getMessage() for r in caplog.records)
    assert any(kind == "warning" and "2 structure rig(s)" in body for kind, body in st_messages)


# =============================================================================================
# 2. Active jobs that fail to load stop the Portfolio planner
# =============================================================================================


def test_portfolio_planner_stops_with_an_error_when_active_jobs_fail(monkeypatch, st_messages):
    from streamlit_ui.pages import portfolio_planner as page

    def boom():
        raise RuntimeError("Failed to load active industry jobs: HTTP 503")

    monkeypatch.setattr(page, "fetch_active_industry_jobs", boom)
    assert page._fetch_jobs_data() is None
    errors = [body for kind, body in st_messages if kind == "error"]
    assert len(errors) == 1 and "HTTP 503" in errors[0]
    assert not [body for kind, body in st_messages if kind == "warning"]


def test_portfolio_planner_render_shows_no_recommendations_without_active_jobs(monkeypatch, st_messages):
    from streamlit_ui.pages import portfolio_planner as page

    row = {"type_id": 34, "type_name": "Tritanium", "blueprint_type_id": 1}
    rendered = []
    fetched = []

    def boom():
        raise RuntimeError("HTTP 503")

    monkeypatch.setattr(page, "_render_page_about", lambda: None)
    monkeypatch.setattr(page, "_get_overview_rows", lambda: [row])
    monkeypatch.setattr(page, "_read_industry_builder_filter_state", lambda: {
        "enabled_meta_groups": set(), "have_skills_only": False, "positive_profit_only": False,
        "min_margin_pct": 0.0, "min_isk_per_hour": 0.0, "min_region_daily_volume": 0,
        "excluded_liquidity_indicators": set(), "excluded_anomaly_risks": set()})
    monkeypatch.setattr(page, "filter_overview_rows", lambda rows, *a: rows)
    monkeypatch.setattr(page, "_apply_hard_disqualifiers", lambda rows: (rows, []))
    monkeypatch.setattr(page, "_render_header_banner", lambda *a, **k: None)
    monkeypatch.setattr(page, "overview_refresh_is_active", lambda: False)
    monkeypatch.setattr(page, "fetch_active_industry_jobs", boom)
    monkeypatch.setattr(page, "_fetch_optional", lambda fn, ids, **k: fetched.append(k.get("what")) or {})
    monkeypatch.setattr(page, "_render_recommendations_table", lambda *a, **k: rendered.append(a))
    monkeypatch.setattr(page, "_render_shopping_list_tab", lambda *a, **k: rendered.append(a))
    page.render()
    assert rendered == []
    assert fetched == []  # no per-candidate fetches either
    assert any(kind == "error" and "HTTP 503" in body for kind, body in st_messages)


def test_portfolio_track_record_failure_warns_once_per_run(st_messages):
    from streamlit_ui.pages import portfolio_planner as page

    calls = []

    def boom(ids):
        calls.append(ids)
        raise RuntimeError("backend down")

    failed: set[str] = set()
    assert page._fetch_optional(boom, (1, 2), what="track record", failed=failed) == {}
    assert page._fetch_optional(boom, (3,), what="track record", failed=failed) == {}
    assert len(calls) == 1
    warnings = [body for kind, body in st_messages if kind == "warning"]
    assert len(warnings) == 1 and "backend down" in warnings[0]


def test_portfolio_track_record_is_reused_when_the_ids_were_already_fetched():
    from streamlit_ui.pages import portfolio_planner as page

    calls = []

    def fetch(ids):
        calls.append(ids)
        return {str(i): {"n": i} for i in ids}

    first = page._fetch_optional(fetch, (1, 2, 3), what="track record", failed=set())
    again = page._reuse_or_fetch(fetch, (2, 3), previous_ids=(1, 2, 3), previous=first,
                                 what="track record", failed=set())
    assert again == first
    assert calls == [(1, 2, 3)]
    more = page._reuse_or_fetch(fetch, (4,), previous_ids=(1, 2, 3), previous=first,
                                what="track record", failed=set())
    assert more == {"4": {"n": 4}}
    assert calls == [(1, 2, 3), (4,)]


# =============================================================================================
# 2b/4. Realized profit
# =============================================================================================


def test_realized_profit_stops_with_an_error_when_wallet_owners_fail(monkeypatch, st_messages):
    from streamlit_ui.pages import realized_profit as page

    def boom():
        raise RuntimeError("Failed to load characters")

    monkeypatch.setattr(page, "fetch_characters", boom)
    page._reset_load_warnings()
    assert page._character_payloads() == []
    assert page._character_name_map() == {}
    errors = [body for kind, body in st_messages if kind == "error"]
    assert len(errors) == 1 and "Failed to load characters" in errors[0]
    assert page._owner_load_failures() == ["characters"]


def test_realized_profit_render_stops_without_the_wallet_owners(monkeypatch, st_messages):
    import streamlit_ui.api.corporations as corps
    from streamlit_ui.pages import realized_profit as page

    def boom():
        raise RuntimeError("HTTP 503")

    fetched = []
    monkeypatch.setattr(page, "require_aggrid", lambda: SimpleNamespace(js_code=lambda s: s))
    monkeypatch.setattr(page, "fetch_characters", lambda: [])
    monkeypatch.setattr(corps, "fetch_director_corporations", boom)
    monkeypatch.setattr(page, "fetch_realized_profit", lambda **k: fetched.append(k) or {"status": "success"})
    page.render()
    assert fetched == []
    assert any(kind == "error" and "HTTP 503" in body for kind, body in st_messages)


def test_lower_bound_cost_summary_counts_the_sales_and_names_the_reasons():
    from streamlit_ui.pages import realized_profit as page

    rows = [
        {"transaction_id": 1, "cost_lower_bound_reason": "invention cost unknown: no decryptor choice"},
        {"transaction_id": 2, "cost_lower_bound_reason": "invention cost unknown: no decryptor choice"},
        {"transaction_id": 3, "cost_lower_bound_reason": "job cost unknown"},
        {"transaction_id": 4, "cost_lower_bound_reason": None},
        {"transaction_id": 5},
    ]
    warning = page._lower_bound_cost_warning(rows)
    assert warning.startswith("3 sales use a lower-bound cost: ")
    assert "invention cost unknown: no decryptor choice (2)" in warning
    assert "job cost unknown (1)" in warning


def test_lower_bound_cost_summary_is_none_without_the_key():
    from streamlit_ui.pages import realized_profit as page

    assert page._lower_bound_cost_warning([{"transaction_id": 1}, {"transaction_id": 2, "cost_lower_bound_reason": ""}]) is None
    assert page._lower_bound_cost_warning([]) is None


def test_the_ledger_table_marks_each_lower_bound_row():
    from streamlit_ui.pages import realized_profit as page

    rows = [
        {"date": "2026-10-01T00:00:00Z", "character_id": 1, "type_id": 34, "type_name": "Tritanium",
         "quantity": 1, "allocated_cost": 5.0, "cost_lower_bound_reason": "job cost unknown"},
        {"date": "2026-10-01T00:00:00Z", "character_id": 1, "type_id": 35, "type_name": "Pyerite",
         "quantity": 1, "allocated_cost": 5.0},
    ]
    frame = page._table_frame(rows, {1: "Pilot"}, owner_label="Character", owner_id_key="character_id")
    assert list(frame["Cost Lower Bound"]) == ["job cost unknown", ""]


# =============================================================================================
# 3. Market orders
# =============================================================================================


def test_order_pricing_issue_reads_both_contract_keys():
    from streamlit_ui.pages import marketorders as page

    assert page._order_pricing_issue({"pricing_error": "pricing suggestion unavailable: db locked"}) == \
        "pricing suggestion unavailable: db locked"
    assert page._order_pricing_issue({"price_unknown_reason": "order price unreadable"}) == "order price unreadable"
    both = page._order_pricing_issue({"pricing_error": "a", "price_unknown_reason": "b"})
    assert "a" in both and "b" in both
    assert page._order_pricing_issue({"pricing_error": "same", "price_unknown_reason": "same"}) == "same"
    assert page._order_pricing_issue({"advised_price": 9.0}) is None
    assert page._order_pricing_issue({"pricing_error": None, "price_unknown_reason": ""}) is None


def test_order_rows_carry_the_pricing_note():
    from streamlit_ui.pages import marketorders as page

    orders = [
        {"type_name": "Tritanium", "price": 5.0, "is_buy_order": False, "pricing_error": "pricing suggestion unavailable: x"},
        {"type_name": "Pyerite", "price": None, "is_buy_order": True, "price_unknown_reason": "order price unreadable"},
        {"type_name": "Mexallon", "price": 7.0, "is_buy_order": False, "advised_price": 6.5},
    ]
    sells, buys = page._build_order_rows(orders)
    assert sells[0]["Pricing Note"] == "pricing suggestion unavailable: x"
    assert "Advised Price" not in sells[0]
    assert buys[0]["Pricing Note"] == "order price unreadable"
    assert sells[1].get("Pricing Note", "") == ""


def test_pricing_issue_summary_counts_the_orders():
    from streamlit_ui.pages import marketorders as page

    orders = [
        {"type_name": "Tritanium", "pricing_error": "pricing suggestion unavailable: x"},
        {"type_name": "Pyerite", "price_unknown_reason": "order price unreadable"},
        {"type_name": "Mexallon", "advised_price": 6.5},
    ]
    warning = page._pricing_issues_warning(orders)
    assert warning.startswith("2 order(s) have no advised or known price")
    assert "Tritanium: pricing suggestion unavailable: x" in warning
    assert "Pyerite: order price unreadable" in warning
    assert page._pricing_issues_warning([{"type_name": "Mexallon"}]) is None


def test_pricing_analysis_warns_where_the_advised_price_is_shown(monkeypatch, st_messages):
    import streamlit as st
    from streamlit_ui.pages import marketorders as page

    monkeypatch.setattr(st, "info", lambda *a, **k: None)
    page._render_pricing_analysis({"type_name": "Tritanium", "price": 5.0, "pricing_error": "pricing suggestion unavailable: x"})
    assert any(kind == "warning" and "pricing suggestion unavailable: x" in body for kind, body in st_messages)


def test_market_orders_reads_the_director_corporations_once(monkeypatch, st_messages):
    import streamlit as st
    import streamlit_ui.api.corporations as corps
    from streamlit_ui.pages import marketorders as page

    calls = []

    def boom():
        calls.append(1)
        raise RuntimeError("HTTP 503")

    monkeypatch.setattr(corps, "fetch_director_corporations", boom)
    monkeypatch.setattr(page, "fetch_market_orders", lambda: {"data": [
        {"order_id": 1, "type_name": "Tritanium", "price": 5.0, "is_buy_order": False, "owner": "Pilot"}]})
    monkeypatch.setattr(page, "refresh_market_orders", lambda: None)
    monkeypatch.setattr(page, "clear_market_orders_cache", lambda: None)
    monkeypatch.setattr(page, "clear_corp_market_orders_cache", lambda: None)
    monkeypatch.setattr(page, "_rerun", lambda: None)
    monkeypatch.setattr(page, "_render_orders_grid", lambda *a, **k: None)
    monkeypatch.setattr(page, "_render_pricing_analysis", lambda *a, **k: None)
    monkeypatch.setattr(st, "button", lambda *a, **k: True)  # the refresh button is clicked
    page._render_market_orders(runtime=None, img_renderer=None)
    assert len(calls) == 1
    warnings = [body for kind, body in st_messages if kind == "warning" and "HTTP 503" in body]
    assert len(warnings) == 1


def test_director_corp_ids_success_path(monkeypatch, st_messages):
    import streamlit_ui.api.client as client
    import streamlit_ui.api.corporations as corps_api
    from streamlit_ui.pages import marketorders as page

    monkeypatch.setattr(client, "api_get_or_error", lambda path, **k: ({"status": "success", "data": [
        {"corporation_id": 98000001, "has_director_access": True},
        {"corporation_id": 98000002, "has_director_access": False},
        {"corporation_id": None, "has_director_access": True},
        "junk",
    ]}, None))
    assert [c["corporation_id"] for c in corps_api.fetch_director_corporations()] == [98000001]
    assert page._get_director_corp_ids() == [98000001]
    assert st_messages == []


# =============================================================================================
# 5. Status bar: one wording, a failure cached briefly
# =============================================================================================


@pytest.fixture
def market_intel_api(monkeypatch):
    from streamlit_ui.api import daily_planner as api

    api.get_market_intel_status.clear()
    yield api
    api.get_market_intel_status.clear()


def test_a_failed_market_intel_read_is_cached_briefly(monkeypatch, market_intel_api):
    api = market_intel_api
    calls = []
    now = [1000.0]

    def fails(path, **k):
        calls.append(path)
        return None, "request failed: read timed out"

    monkeypatch.setattr(api, "api_get_or_error", fails)
    monkeypatch.setattr(api, "_monotonic", lambda: now[0])
    with pytest.raises(RuntimeError, match="read timed out"):
        api.get_market_intel_status()
    now[0] += 10.0
    with pytest.raises(RuntimeError, match="read timed out"):
        api.get_market_intel_status()
    assert len(calls) == 1  # the rerun did not pay the timeout again
    now[0] += api.MARKET_INTEL_FAILURE_CACHE_SECONDS
    monkeypatch.setattr(api, "api_get_or_error",
                        lambda path, **k: calls.append(path) or ({"status": "success", "data": {"last_error": None}}, None))
    assert api.get_market_intel_status() == {"last_error": None}
    assert len(calls) == 2


def test_the_market_intel_failure_is_worded_once(monkeypatch, market_intel_api):
    import tests.test_daily_planner_status_bar_freshness as freshness
    from streamlit_ui.components.daily_planner import status_bar
    from streamlit_ui.state.daily_planner_page import DailyPlannerPageState

    api = market_intel_api
    monkeypatch.setattr(api, "api_get_or_error", lambda path, **k: (None, "HTTP 503"))
    fake = freshness._FakeSt()
    monkeypatch.setattr(status_bar, "st", fake)
    monkeypatch.setattr(status_bar, "get_status", lambda: {"status": "idle"})
    monkeypatch.setattr(status_bar, "get_plan", lambda: None)
    monkeypatch.setattr(status_bar, "get_market_intel_status", api.get_market_intel_status)
    status_bar.render_status_bar(DailyPlannerPageState(plan=None))
    [warning] = [w for w in fake.warnings if "HTTP 503" in w]
    assert warning.lower().count("could not be read") == 1
    assert "market intelligence status" not in warning.lower()


# =============================================================================================
# 6. Small C3 points
# =============================================================================================


def test_a_malformed_refresh_status_ends_the_poll_with_a_reason(monkeypatch):
    import streamlit as st
    import streamlit_ui.state.industry_snapshot_page as snapshot

    cleared = []
    st.session_state["industry_builder_refresh_job_id"] = "job-1"
    monkeypatch.setattr(snapshot, "clear_overview_refresh_job", lambda error_message=None: cleared.append(error_message))
    monkeypatch.setattr(snapshot, "fetch_product_overview_refresh_status",
                        lambda job_id: {"status": "running", "progress_fraction": "half"})
    monkeypatch.setattr(snapshot, "fetch_job_manager_status", lambda: {})
    snapshot._poll_overview_refresh_safely()
    assert len(cleared) == 1 and "malformed" in cleared[0] and "half" in cleared[0]


def test_a_runtime_error_still_ends_the_poll(monkeypatch):
    import streamlit_ui.state.industry_snapshot_page as snapshot

    cleared = []

    def fails(job_id):
        raise RuntimeError("HTTP 500")

    monkeypatch.setattr(snapshot, "clear_overview_refresh_job", lambda error_message=None: cleared.append(error_message))
    monkeypatch.setattr(snapshot, "poll_overview_refresh_job", lambda **k: fails("x"))
    snapshot._poll_overview_refresh_safely()
    assert cleared == ["HTTP 500"]


@pytest.mark.parametrize("fmt_name, missing", [
    ("format_isk", "N/A"), ("format_isk_short", "N/A"), ("format_decimal_eu", "-"),
    ("format_isk_eu", "-"), ("format_pct_eu", "-"),
])
def test_formatters_survive_an_overflowing_int(fmt_name, missing):
    import streamlit_ui.components.formatters as formatters

    assert getattr(formatters, fmt_name)(10 ** 400) == missing


def test_formatters_keep_formatting_normal_values():
    import streamlit_ui.components.formatters as formatters

    assert formatters.format_isk(1234.5) == "ISK 1.234,50"
    assert formatters.format_isk_short(2_500_000) == "2.50m"
    assert formatters.format_decimal_eu(1234.5) == "1.234,50"


class _BrokenBuilder:
    def __init__(self, exc):
        self._exc = exc

    def from_dataframe(self, df):
        raise self._exc


@pytest.mark.parametrize("exc", [KeyError("Item"), AttributeError("no configure_column")])
def test_shopping_grid_falls_back_to_a_plain_table(monkeypatch, st_messages, exc):
    import streamlit as st
    from streamlit_ui.components.daily_planner import tab_shopping

    monkeypatch.setattr(st, "markdown", lambda *a, **k: None)
    ag = SimpleNamespace(AgGrid=lambda *a, **k: None, GridOptionsBuilder=_BrokenBuilder(exc))
    rows = [{"Item": "Tritanium", "Quantity": 10, "Est. Unit Price": 5.0, "Est. Total": 50.0, "Note": "",
             "status": "pending"}]
    tab_shopping._render_materials_section("Materials", rows, False, ag)
    assert ("dataframe", "") in st_messages


@pytest.mark.parametrize("exc", [KeyError("Item"), AttributeError("no configure_column")])
def test_build_plan_grid_falls_back_to_a_plain_table(monkeypatch, st_messages, exc):
    from streamlit_ui.components.daily_planner import tab_build_plan
    from streamlit_ui.state.daily_planner_page import DailyPlannerPageState

    ag = SimpleNamespace(AgGrid=lambda *a, **k: None, GridOptionsBuilder=_BrokenBuilder(exc), JsCode=lambda s: s)
    monkeypatch.setattr(tab_build_plan, "import_aggrid", lambda: ag)
    plan = {"items": [{"type_id": 34, "type_name": "Tritanium", "decision": "build", "sample_count": 1}]}
    tab_build_plan.render_tab_build_plan(DailyPlannerPageState(plan=plan))
    assert ("dataframe", "") in st_messages


def test_location_info_does_not_cache_an_error(monkeypatch):
    import streamlit_ui.components.assets_ui as assets_ui

    responses = [None, {"status": "error", "message": "backend down"},
                 {"status": "success", "data": {"60003760": {"name": "Jita IV - Moon 4"}}}]
    monkeypatch.setattr(assets_ui, "api_post", lambda path, payload: responses.pop(0))
    assets_ui.fetch_location_info.clear()
    try:
        assert assets_ui.fetch_location_info([60003760])["status"] == "error"
        assert assets_ui.fetch_location_info([60003760])["message"] == "backend down"
        assert assets_ui.fetch_location_info([60003760])["status"] == "success"
        # A success is cached.
        assert assets_ui.fetch_location_info([60003760])["status"] == "success"
        assert responses == []
    finally:
        assets_ui.fetch_location_info.clear()


# =============================================================================================
# 8. Corporations page
# =============================================================================================


@pytest.mark.parametrize("wallets", [None, "", [], "[]"])
def test_no_wallets_reported_is_not_isk_zero(wallets):
    from streamlit_ui.pages import corporations as page

    balance, reason = page._master_wallet_balance_and_reason(wallets)
    assert balance is None
    assert reason == "No wallets reported"
    assert page._master_wallet_label(balance, reason) == "No wallets reported"


def test_a_missing_master_division_is_named():
    from streamlit_ui.pages import corporations as page

    balance, reason = page._master_wallet_balance_and_reason([{"division": 2, "balance": 5.0}])
    assert balance is None and "master wallet" in reason.lower()


def test_a_master_wallet_balance_is_shown_in_isk():
    from streamlit_ui.pages import corporations as page

    balance, reason = page._master_wallet_balance_and_reason('[{"division": 1, "balance": 12.5}]')
    assert (balance, reason) == (12.5, None)
    assert page._master_wallet_label(balance, reason).startswith("ISK")


def test_an_unknown_order_price_is_shown_as_unknown_not_zero(monkeypatch, st_messages):
    import streamlit as st
    from streamlit_ui.pages import marketorders as page

    metrics = []
    monkeypatch.setattr(st, "metric", lambda label, value, *a, **k: metrics.append((label, value)))
    monkeypatch.setattr(st, "caption", lambda *a, **k: None)
    monkeypatch.setattr(st, "write", lambda *a, **k: None)
    order = {"type_name": "Tritanium", "price": None, "advised_price": 6.0,
             "price_unknown_reason": "order price unreadable",
             "pricing_breakdown": {"components": {}}}
    page._render_pricing_analysis(order)
    assert ("Current Price", "unknown") in metrics
    assert not any(label == "Difference" for label, _ in metrics)
    assert any(kind == "warning" and "order price unreadable" in body for kind, body in st_messages)
    assert page._order_price_label(None) == "unknown"
    assert page._order_price_label(5.0) == "5.00"


def test_sell_stats_skip_an_unknown_price():
    from streamlit_ui.pages import marketorders as page

    stats = page._compute_sell_stats([
        {"price": None, "volume_remain": 10, "hub_price": 1.0, "cost_basis": 2.0, "cost_basis_source": "asset",
         "price_unknown_reason": "order price unreadable"},
        {"price": 3.0, "volume_remain": 10, "hub_price": 1.0, "cost_basis": 2.0, "cost_basis_source": "asset"},
    ])
    assert stats["below_cost_count"] == 2
    assert stats["below_cost_isk"] == 30.0
