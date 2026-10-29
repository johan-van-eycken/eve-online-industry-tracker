"""Round H3, the Streamlit pages and the Flask routes: a failed or partial
read is named (a reason, a warning, an error or "unknown"), never a silent
0, default or overwrite. Fake values throughout."""
from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest
from fakes import industry_client


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


# =============================================================================================
# 5. Grid JS formatters: an optional null_text ("unknown") where None means unknown
# =============================================================================================


def _js(code):
    return code  # JsCode stand-in: the formatter's source


@pytest.mark.parametrize("name", ["js_eu_number_formatter", "js_eu_isk_formatter", "js_eu_pct_formatter"])
def test_a_formatter_shows_the_null_text_for_a_missing_value(name):
    from streamlit_ui.components import aggrid_formatters as fmt

    plain = getattr(fmt, name)(JsCode=_js, locale="en-US", decimals=2)
    assert "unknown" not in plain
    with_text = getattr(fmt, name)(JsCode=_js, locale="en-US", decimals=2, null_text="unknown")
    assert '"unknown"' in with_text
    flagged = getattr(fmt, name)(JsCode=_js, locale="en-US", decimals=2, null_text="unknown",
                                 null_text_flag_field="_is_product_row")
    assert "params.data._is_product_row" in flagged and '"unknown"' in flagged


def test_the_null_text_is_a_safe_js_string():
    from streamlit_ui.components import aggrid_formatters as fmt

    code = fmt.js_eu_number_formatter(JsCode=_js, locale="en-US", decimals=0, null_text="it's \"odd\"")
    assert '"it\'s \\"odd\\""' in code


class _RecordingBuilder:
    def __init__(self):
        self.columns: dict[str, dict] = {}

    def from_dataframe(self, df):
        return self

    def configure_column(self, col, **kwargs):
        self.columns.setdefault(col, {}).update(kwargs)

    def __getattr__(self, name):
        return lambda *a, **k: None


def test_the_industry_builder_shows_unknown_for_its_unknown_columns(monkeypatch):
    from streamlit_ui.pages import industry_builder as page

    builder = _RecordingBuilder()
    runtime = SimpleNamespace(js_code=_js, locale="en-US", grid_options_builder=builder,
                              aggrid=lambda *a, **k: {"selected_rows": []})
    monkeypatch.setattr(page, "render_aggrid_table", lambda *a, **k: {"selected_rows": []}, raising=False)
    try:
        page._render_overview_grid(runtime=runtime, filtered_overview_rows=[_product_row()])
    except Exception:  # the grid call itself is not under test; the column setup ran before it
        pass
    for col in ("Material Cost", "Job Cost", "Profit", "Region Daily Volume", "Market Unit Price",
                "Hub Buy Liquidity", "Hub Sell Liquidity"):
        formatter = builder.columns[col]["valueFormatter"]
        assert '"unknown"' in formatter and "_is_product_row" in formatter, col
    assert '"unknown"' not in builder.columns["Gross Sale Value"]["valueFormatter"]


def test_the_market_orders_grid_shows_unknown_for_price_and_total():
    from streamlit_ui.pages import marketorders as page

    import pandas as pd

    builder = _RecordingBuilder()
    runtime = SimpleNamespace(js_code=_js, locale="en-US", grid_options_builder=builder)
    df = pd.DataFrame([{"Icon": "", "Type": "Fake", "Price": None, "Total Price": None, "Escrow Remaining": 1.0}])
    try:
        page._render_orders_grid(df, runtime=runtime, img_renderer=None,
                                 isk_cols=["Price", "Total Price", "Escrow Remaining"], min_volume=False, key="k")
    except Exception:  # the grid call itself is not under test
        pass
    assert '"unknown"' in builder.columns["Price"]["valueFormatter"]
    assert '"unknown"' in builder.columns["Total Price"]["valueFormatter"]
    assert '"unknown"' not in builder.columns["Escrow Remaining"]["valueFormatter"]


# =============================================================================================
# H1 interface: liquidity_unavailable_reason / price_unavailable_reason in the Industry Builder
# =============================================================================================

LIQ_REASON = "order book for 10000002 unavailable: fake 503"
PRICE_REASON = "hub price unknown: fake order book fetch failed"


def _product_row(*, liquidity_reason=None, price_reason=None, on_job=False):
    job = {"job_tree": {"node_type": "product", "activity": "product", "type_id": 5002, "children": [
        {"node_type": "material", "label": "Tritanium", "type_id": 34}]},
        "material_cost": None, "total_job_cost": None}
    row = {"overview_row_id": "r1", "type_id": 5002, "type_name": "Fake Module", "pricing_confidence": "Low",
           "hub_buy_liquidity": None if liquidity_reason else 10.0,
           "hub_sell_liquidity": None if liquidity_reason else 20.0,
           "market_unit_price": None if price_reason else 1000.0,
           "profit_amount": None, "region_daily_volume": None, "manufacturing_job": job}
    target = job if on_job else row
    if liquidity_reason:
        target["liquidity_unavailable_reason"] = liquidity_reason
    if price_reason:
        target["price_unavailable_reason"] = price_reason
    return row


def test_the_hub_liquidity_and_price_notes_name_their_reason():
    from streamlit_ui.state import industry_builder_ui as ui

    row = _product_row(liquidity_reason=LIQ_REASON, price_reason=PRICE_REASON)
    assert ui.hub_liquidity_note(row) == f"hub liquidity unknown: {LIQ_REASON}"
    assert ui.hub_price_note(row) == f"hub price unknown: {PRICE_REASON}"
    on_job = _product_row(liquidity_reason=LIQ_REASON, price_reason=PRICE_REASON, on_job=True)
    assert LIQ_REASON in ui.hub_liquidity_note(on_job) and PRICE_REASON in ui.hub_price_note(on_job)


def test_the_notes_are_empty_without_the_keys():
    from streamlit_ui.state import industry_builder_ui as ui

    row = _product_row()
    assert ui.hub_liquidity_note(row) == "" and ui.hub_price_note(row) == ""
    assert ui.hub_liquidity_note({}) == "" and ui.hub_price_note({"manufacturing_job": None}) == ""


def test_the_product_grid_row_carries_the_notes_next_to_their_columns():
    from streamlit_ui.state import industry_builder_ui as ui

    product, material = ui.flatten_overview_job_tree_rows(
        [_product_row(liquidity_reason=LIQ_REASON, price_reason=PRICE_REASON)])
    assert product["Hub Buy Liquidity"] is None and product["Market Unit Price"] is None
    assert product["Hub Liquidity Note"] == f"hub liquidity unknown: {LIQ_REASON}"
    assert product["Price Note"] == f"hub price unknown: {PRICE_REASON}"
    assert product["_is_product_row"] is True and material["_is_product_row"] is False
    assert material["Hub Liquidity Note"] == "" and material["Price Note"] == ""
    keys = list(product)
    assert keys.index("Hub Liquidity Note") == keys.index("Hub Sell Orders") + 1
    assert keys.index("Price Note") == keys.index("Market Unit Price") + 1
    plain, _ = ui.flatten_overview_job_tree_rows([_product_row()])
    assert plain["Hub Liquidity Note"] == "" and plain["Price Note"] == "" and plain["Hub Buy Liquidity"] == 10.0


def test_the_drilldown_and_debug_payload_show_the_hub_reasons():
    from streamlit_ui.pages import industry_builder as page
    from streamlit_ui.state import industry_builder_ui as ui

    source = inspect.getsource(page)
    assert "hub_liquidity_note(selected_row)" in source and "hub_price_note(selected_row)" in source
    preview = ui.build_debug_payload_preview(
        _product_row(liquidity_reason=LIQ_REASON, price_reason=PRICE_REASON, on_job=True))
    assert preview["manufacturing_job"]["liquidity_unavailable_reason"] == LIQ_REASON
    assert preview["manufacturing_job"]["price_unavailable_reason"] == PRICE_REASON


def test_the_hidden_columns_include_the_product_row_flag():
    from streamlit_ui.pages import industry_builder as page

    assert '"_is_product_row"' in inspect.getsource(page._render_overview_grid)


# =============================================================================================
# 6. Portfolio and the overview filters: unknown is not unprofitable, not 0, and never hidden silently
# =============================================================================================


def _candidate(type_id, *, profit=1_000_000.0, isk=2_000_000.0, roc=0.5, margin=0.2, volume=500, **extra):
    return {"type_id": type_id, "type_name": f"Fake {type_id}", "meta_group_name": "Tech I",
            "profit_amount": profit, "isk_per_hour": isk, "return_on_capital": roc,
            "profit_margin_fraction": margin, "region_daily_volume": volume,
            "liquidity_indicator": "High", "pricing_confidence": "High", "manufacturing_job": {}, **extra}


def test_an_unknown_profit_is_disqualified_as_unknown_not_unprofitable():
    from streamlit_ui.pages import portfolio_planner as page

    eligible, disqualified = page._apply_hard_disqualifiers(
        [_candidate(1, profit=None), _candidate(2, profit=-5.0), _candidate(3)])
    assert [r["type_id"] for r in eligible] == [3]
    assert [(r["type_id"], reason) for r, reason in disqualified] == [(1, "Profit unknown"), (2, "Unprofitable")]


def test_scoring_ignores_unknown_values_in_the_normalisation():
    from streamlit_ui.pages import portfolio_planner as page

    ctx = page._compute_normalization_context([_candidate(1, isk=None), _candidate(2, isk=5.0), _candidate(3, isk=9.0)])
    assert ctx["isk_min"] == 5.0 and ctx["isk_max"] == 9.0


def test_a_row_with_an_unknown_score_input_is_unknown_and_ranked_last():
    from streamlit_ui.pages import portfolio_planner as page

    rows = [_candidate(1, isk=None), _candidate(2, isk=1_000_000.0), _candidate(3, isk=3_000_000.0)]
    ranked = page._rank_candidates(rows, objective="Balanced")
    assert [r["type_id"] for r, _ in ranked] == [3, 2, 1]
    assert ranked[-1][1] is None and all(isinstance(score, float) for _r, score in ranked[:2])
    by_isk = page._rank_candidates(rows, objective="Max ISK/hr")
    assert [r["type_id"] for r, _ in by_isk] == [3, 2, 1] and by_isk[-1][1] is None
    assert page._apply_track_record_multiplier(None, rows[0], {"1": {"status": "proven"}}) is None
    assert any("ISK/hour unknown" in w for w in page._warnings(rows[0]))


def test_the_recommendations_table_does_not_show_an_unknown_as_zero(monkeypatch):
    import streamlit as st
    from streamlit_ui.pages import portfolio_planner as page

    frames = []
    monkeypatch.setattr(st, "dataframe", lambda df, *a, **k: frames.append(df))
    monkeypatch.setattr(st, "expander", lambda *a, **k: _NullContext())
    monkeypatch.setattr(st, "columns", lambda spec, *a, **k: [_NullContext() for _ in range(spec if isinstance(spec, int) else len(spec))])
    page._render_recommendations_table([(_candidate(1, isk=None, roc=None), None)])
    row = frames[0].iloc[0]
    assert row["ISK/Hr (M)"] is None or row["ISK/Hr (M)"] != row["ISK/Hr (M)"]  # None or NaN, not 0
    assert row["ROC %"] is None or row["ROC %"] != row["ROC %"]


class _NullContext:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _filter_args(rows, *, positive=False, min_margin=0.0, min_isk=0.0, min_volume=0):
    from streamlit_ui.state import industry_builder_ui as ui

    return (rows, (ui.get_meta_group_name(rows[0]),), False, positive, min_margin, min_isk, min_volume, (), ())


def test_the_hidden_notes_name_each_unknown_minimum_filter():
    from streamlit_ui.state import industry_builder_ui as ui

    rows = [_candidate(1), _candidate(2, isk=None), _candidate(3, margin=None), _candidate(4, profit=None),
            _candidate(5, isk=None, volume=None, region_daily_volume_unavailable_reason="fake 502"),
            _candidate(6, isk=1.0)]  # a known low ISK/h is not counted
    args = _filter_args(rows, positive=True, min_margin=10.0, min_isk=1000.0, min_volume=10)
    assert [r["type_id"] for r in ui.filter_overview_rows(*args)] == [1]
    notes = ui.unknown_value_hidden_notes(*args)
    assert "2 rows hidden: ISK/hour unknown" in notes
    assert "1 row hidden: profit margin unknown" in notes
    assert "1 row hidden: profit unknown" in notes
    assert "1 row hidden: region daily volume unknown (fake 502)" in notes
    assert ui.unknown_region_volume_hidden_note(*args) == "1 row hidden: region daily volume unknown (fake 502)"


def test_no_hidden_notes_without_active_minimums():
    from streamlit_ui.state import industry_builder_ui as ui

    rows = [_candidate(1), _candidate(2, isk=None, margin=None, volume=None)]
    args = _filter_args(rows)
    assert [r["type_id"] for r in ui.filter_overview_rows(*args)] == [1, 2]  # unknown passes a 0 minimum, as before
    assert ui.unknown_value_hidden_notes(*args) == []


def test_a_row_hidden_by_a_known_value_is_not_counted_for_its_unknown_one():
    from streamlit_ui.state import industry_builder_ui as ui

    rows = [_candidate(1), _candidate(2, isk=None, margin=0.01)]  # margin known and below the minimum
    assert ui.unknown_value_hidden_notes(*_filter_args(rows, min_margin=10.0, min_isk=1000.0)) == []


def test_the_market_orders_stat_cards_count_the_unknown_orders():
    from streamlit_ui.pages import marketorders as page

    real = {"cost_basis_source": "market_buy"}
    orders = [
        {**real, "price": 100.0, "total_price": 1000.0, "net_margin_pct_current": 10.0, "isk_per_day_current": 50.0,
         "advised_price": 90.0, "net_margin_pct_advised": 5.0, "isk_per_day_advised": 40.0},
        {**real, "price": None, "total_price": None, "net_margin_pct_current": None, "isk_per_day_current": None},
        {"cost_basis_source": "market_order_fallback", "price": 50.0, "total_price": 500.0,
         "net_margin_pct_current": 20.0, "isk_per_day_current": 10.0},  # no real cost: its profit is unknown
    ]
    stats = page._compute_sell_stats(orders)
    assert stats["est_profit_current"] == pytest.approx(100.0) and stats["est_profit_current_unknown"] == 2
    assert stats["est_profit_advised_unknown"] == 2
    assert stats["total_isk_day_current"] == 50.0 and stats["total_isk_day_current_unknown"] == 2
    assert stats["total_isk_day_advised_unknown"] == 2
    assert stats["avg_margin_current"] == 10.0 and stats["avg_margin_current_unknown"] == 2


def test_a_stat_card_value_names_its_unknown_count():
    from streamlit_ui.pages import marketorders as page

    assert page._with_unknown("10.0K ISK", 2) == "10.0K ISK + 2 unknown"
    assert page._with_unknown("10.0K ISK", 0) == "10.0K ISK"
    assert page._with_unknown(None, 3) == "unknown (3)"
    assert page._with_unknown(None, 0) == "—"


def test_the_sell_stats_box_renders_the_unknown_counts(monkeypatch):
    from streamlit_ui.pages import marketorders as page

    cards: list[tuple[str, str]] = []
    monkeypatch.setattr(page, "_render_stat_card_grid", lambda c: cards.extend(c))
    monkeypatch.setattr(page.st, "columns", lambda spec, *a, **k: [_NullContext(), _NullContext()])
    stats = page._compute_sell_stats([
        {"cost_basis_source": "market_buy", "price": 100.0, "total_price": 1000.0, "net_margin_pct_current": 10.0,
         "isk_per_day_current": 50.0},
        {"cost_basis_source": "market_buy", "price": None, "total_price": None}])
    page._render_sell_stats_box(stats)
    by_label = dict(cards)
    for label in ("Est. Profit (Current)", "Est. ISK/day (Current)", "Avg Margin % (Current)"):
        assert "+ 1 unknown" in by_label[label], label
    assert by_label["Est. Profit (Advised)"] == "unknown (2)"


def test_the_builder_and_portfolio_pages_show_every_hidden_note():
    from streamlit_ui.pages import industry_builder, portfolio_planner

    for page in (industry_builder, portfolio_planner):
        assert "unknown_value_hidden_notes(" in inspect.getsource(page)


# =============================================================================================
# 9. Wallet location names: one request, a failure memo (not re-asked on every rerun), no error caching
# =============================================================================================


@pytest.fixture
def wallet_ui(monkeypatch):
    import streamlit_ui.components.wallet_ui as wallet_ui

    now = [1000.0]
    monkeypatch.setattr(wallet_ui, "_monotonic", lambda: now[0])
    wallet_ui.reset_location_names_cache()
    yield SimpleNamespace(module=wallet_ui, now=now)
    wallet_ui.reset_location_names_cache()


def test_wallet_location_names_are_fetched_once_and_cached(monkeypatch, wallet_ui, st_messages):
    calls = []
    monkeypatch.setattr(wallet_ui.module, "api_post", lambda path, payload: calls.append(payload) or {
        "status": "success", "data": {"60003760": {"name": "Jita IV - Moon 4"}}})
    assert wallet_ui.module.fetch_location_name_map([60003760, "60003760", None]) == {60003760: "Jita IV - Moon 4"}
    assert wallet_ui.module.fetch_location_name_map([60003760]) == {60003760: "Jita IV - Moon 4"}
    assert calls == [{"location_ids": [60003760]}]


def test_a_failed_location_lookup_is_memoised_briefly_and_then_asked_again(monkeypatch, wallet_ui, st_messages):
    calls = []
    monkeypatch.setattr(wallet_ui.module, "api_post", lambda path, payload: calls.append(payload) or None)
    assert wallet_ui.module.fetch_location_name_map([60003760]) == {}
    wallet_ui.now[0] += 5.0
    assert wallet_ui.module.fetch_location_name_map([60003760]) == {}
    assert len(calls) == 1  # the rerun did not ask again
    warnings = [body for kind, body in st_messages if kind == "warning"]
    assert len(warnings) == 2 and all("Location names could not be loaded" in w for w in warnings)
    wallet_ui.now[0] += wallet_ui.module.LOCATION_NAMES_FAILURE_MEMO_SECONDS
    monkeypatch.setattr(wallet_ui.module, "api_post", lambda path, payload: calls.append(payload) or {
        "status": "success", "data": {"60003760": {"name": "Jita IV - Moon 4"}}})
    assert wallet_ui.module.fetch_location_name_map([60003760]) == {60003760: "Jita IV - Moon 4"}
    assert len(calls) == 2  # the failure was not cached


def test_an_error_body_is_a_failure_with_its_reason(monkeypatch, wallet_ui, st_messages):
    monkeypatch.setattr(wallet_ui.module, "api_post",
                        lambda path, payload: {"status": "error", "message": "fake ESI down"})
    assert wallet_ui.module.fetch_location_name_map([60003760]) == {}
    assert any(kind == "warning" and "fake ESI down" in body for kind, body in st_messages)


# =============================================================================================
# 10. Admin settings: the NPC station caption is cached, a failed lookup is not
# =============================================================================================


def test_the_station_caption_is_cached_and_its_failure_is_not(monkeypatch):
    from streamlit_ui.pages import admin_settings as page

    calls = []
    responses = [(None, "HTTP 503"),
                 ({"data": {"station_id": 60003760, "name": "Jita IV - Moon 4", "problem": None}}, None)]
    monkeypatch.setattr(page, "api_get_or_error", lambda path: calls.append(path) or responses.pop(0))
    page._station_name_data.clear()
    try:
        caption, problem = page._station_caption(60003760)
        assert caption is None and "HTTP 503" in problem
        assert page._station_caption(60003760) == ("NPC station 60003760: Jita IV - Moon 4", None)
        assert page._station_caption(60003760) == ("NPC station 60003760: Jita IV - Moon 4", None)
        assert len(calls) == 2  # the failure was asked again, the success was cached
    finally:
        page._station_name_data.clear()


# =============================================================================================
# 11. Industry Jobs page: the build cost and the invention cost reason, where the payload has them
# =============================================================================================


def test_the_job_cost_lines_show_the_build_cost_and_the_reason():
    from streamlit_ui.pages import industry_jobs as page

    job = {"total_build_cost": 12_000_000.0, "unit_build_cost": 1_200_000.0,
           "invention_cost_unknown_reason": "invention cost unknown: fake missing datacore price"}
    lines = page._job_cost_lines(job)
    assert any("Build cost" in line and "12" in line for line in lines)
    assert any("fake missing datacore price" in line for line in lines)


def test_the_job_cost_lines_tolerate_absent_keys():
    from streamlit_ui.pages import industry_jobs as page

    assert page._job_cost_lines({"cost": 1000.0}) == []
    assert page._job_cost_lines({"total_build_cost": None, "invention_cost_unknown_reason": None}) == []


def test_the_table_has_the_cost_columns_only_when_the_payload_has_them():
    from streamlit_ui.pages import industry_jobs as page

    plain = page._table_rows([{"activity_name": "Manufacturing", "cost": 10.0}])
    assert "Build Cost" not in plain[0] and "Cost Note" not in plain[0]
    rows = page._table_rows([{"activity_name": "Manufacturing", "cost": 10.0, "total_build_cost": None,
                              "invention_cost_unknown_reason": "fake reason"},
                             {"activity_name": "Manufacturing", "cost": 10.0}])
    assert rows[0]["Build Cost"] is None and rows[0]["Cost Note"] == "fake reason"
    assert rows[1]["Build Cost"] is None and rows[1]["Cost Note"] == ""


# =============================================================================================
# 12. Routes: a malformed type_ids or a non-positive industry_profile_id is a 400
# =============================================================================================


@pytest.mark.parametrize("path", [
    "/industry/blueprint_skill_qualification?blueprint_type_ids=1,x",
    "/industry/reorder_alerts?type_ids=34,,abc",
    "/industry/track_record?type_ids=1.5",
])
def test_malformed_type_ids_are_a_400(industry_client, path):
    resp = industry_client.get(path)
    assert resp.status_code == 400
    assert "type_ids" in resp.get_json()["message"]


def test_well_formed_type_ids_still_reach_the_service(industry_client, monkeypatch):
    import flask_app.routes.industry as routes

    seen = []

    class _FakeService:
        def __init__(self, state):
            pass

        def get_type_track_record(self, *, type_ids, today_iso):
            seen.append(type_ids)
            return {34: {"status": "proven"}}

    monkeypatch.setattr(routes, "IndustryService", _FakeService)
    body = industry_client.get("/industry/track_record?type_ids=34, 35").get_json()
    assert body["data"] == {"34": {"status": "proven"}} and seen == [[34, 35]]
    assert industry_client.get("/industry/track_record?type_ids=").get_json()["data"] == {}


@pytest.mark.parametrize("value", ["0", "-3", "abc", "2.5"])
def test_a_non_positive_or_malformed_industry_profile_id_query_is_a_400(industry_client, value):
    resp = industry_client.get(f"/industry_products/1?industry_profile_id={value}")
    assert resp.status_code == 400
    assert "industry_profile_id" in resp.get_json()["message"]


@pytest.mark.parametrize("path", ["/industry_products/1/refresh", "/industry_products/1/portfolio_candidates/start"])
@pytest.mark.parametrize("value", [0, -1, True, "x", 2.5])
def test_a_non_positive_or_malformed_industry_profile_id_payload_is_a_400(industry_client, path, value):
    resp = industry_client.post(path, json={"industry_profile_id": value})
    assert resp.status_code == 400
    assert "industry_profile_id" in resp.get_json()["message"]


def test_the_industry_profile_id_param():
    from eve_online_industry_tracker.application.errors import ServiceError
    from flask_app.routes.industry import _industry_profile_id_param

    assert _industry_profile_id_param({}) is None
    assert _industry_profile_id_param({"industry_profile_id": ""}) is None
    assert _industry_profile_id_param({"industry_profile_id": None}) is None
    assert _industry_profile_id_param({"industry_profile_id": "7"}) == 7
    assert _industry_profile_id_param({"industry_profile_id": 7}) == 7
    with pytest.raises(ServiceError) as excinfo:
        _industry_profile_id_param({"industry_profile_id": 0})
    assert excinfo.value.status_code == 400


def test_the_ui_sends_no_industry_profile_id_for_none_selected(monkeypatch):
    from streamlit_ui.api import industry_builder as api

    sent = []
    monkeypatch.setattr(api, "api_post", lambda path, payload: sent.append(payload) or {"status": "success", "data": {}})
    common = dict(maximize_bp_runs=False, group_identical_bpcs=True, build_from_bpc=True,
                  have_blueprint_source_only=True, include_reactions=False, market_hub="jita",
                  material_price_side="sell", product_price_side="sell", owned_blueprints_scope="all_characters",
                  character_id=1)
    api.start_product_overview_refresh(industry_profile_id=0, **common)
    api.start_portfolio_candidates_refresh(industry_profile_id=0, planning_horizon_hours=24.0, **common)
    assert [payload["industry_profile_id"] for payload in sent] == [None, None]


# =============================================================================================
# 14. Admin settings page: a stored invalid value is shown and an unchanged save does not overwrite it
# =============================================================================================


class _AdminFakeSt:
    def __init__(self, *, click_save=True, choices=None):
        self.errors: list[str] = []
        self.buttons: dict[str, dict] = {}
        self.inputs: dict[str, object] = {}
        self.click_save = click_save
        self.choices = choices or {}

    def __getattr__(self, name):
        return lambda *a, **k: None

    def error(self, body, *a, **k):
        self.errors.append(str(body))

    def columns(self, spec):
        return [_NullContext() for _ in range(spec if isinstance(spec, int) else len(spec))]

    def number_input(self, label, *, value=None, key=None, **k):
        self.inputs[key] = value
        return self.choices.get(key, value)

    def checkbox(self, label, *, value=False, key=None, **k):
        self.inputs[key] = value
        return self.choices.get(key, value)

    def selectbox(self, label, *, options, index=0, key=None, **k):
        self.inputs[key] = options[index]
        return self.choices.get(key, options[index])

    def text_input(self, label, *, value="", key=None, **k):
        self.inputs[key] = value
        return self.choices.get(key, value)

    def button(self, label, *a, disabled=False, **k):
        self.buttons[label] = {"disabled": disabled}
        return label == "Save Settings" and self.click_save and not disabled

    def rerun(self):
        pass


_ADMIN_SCHEMA = {"market_fees": {"label": "Market fees", "settings": {
    "relist_count": {"type": "int", "default": 2, "min": 0, "max": 10, "label": "Relists"},
    "hub": {"type": "select", "default": "jita", "label": "Hub",
            "options": [{"value": "jita", "label": "Jita"}, {"value": "amarr", "label": "Amarr"}]},
    "enabled": {"type": "bool", "default": True, "label": "Enabled"},
    "note": {"type": "str", "default": "", "label": "Note"},
}}}
_ADMIN_VALUES = {"market_fees": {"relist_count": 50, "hub": "atlantis", "enabled": 1.0, "note": "keep"}}
_ADMIN_INVALID = {"market_fees": {"relist_count": {"value": 50, "problem": "50 is above the maximum 10"},
                                  "hub": {"value": "atlantis", "problem": "not an option"},
                                  "enabled": {"value": 1.0, "problem": "not a boolean"}}}


def _render_admin(monkeypatch, fake):
    from streamlit_ui.pages import admin_settings as page

    puts = []
    monkeypatch.setattr(page, "st", fake)
    monkeypatch.setattr(page, "api_get", lambda path: {"status": "success", "data": {
        "schema": _ADMIN_SCHEMA, "values": _ADMIN_VALUES, "invalid": _ADMIN_INVALID}})
    monkeypatch.setattr(page, "api_put_or_error", lambda path, payload: puts.append(payload) or ({"status": "success"}, None))
    monkeypatch.setattr(page, "api_post", lambda path, payload: pytest.fail("no reset expected"))
    page.render()
    return puts


def test_the_admin_page_shows_each_invalid_value_and_an_unchanged_save_writes_nothing(monkeypatch):
    fake = _AdminFakeSt(click_save=True)
    puts = _render_admin(monkeypatch, fake)
    for needle in ("50", "'atlantis'", "1.0"):
        assert any(needle in error for error in fake.errors), needle
    assert fake.inputs["admin_market_fees_relist_count"] is None       # not clamped to 10, not the default
    # The stored invalid entry stays selected (the selectbox shows labels).
    assert fake.inputs["admin_market_fees_hub"] == "Invalid stored value: 'atlantis'"
    assert fake.inputs["admin_market_fees_enabled"] == "Invalid stored value: 1.0"
    assert fake.buttons["Save Settings"]["disabled"] is True
    assert puts == []


def test_a_save_of_another_setting_leaves_the_invalid_values_alone(monkeypatch):
    fake = _AdminFakeSt(click_save=True, choices={"admin_market_fees_note": "changed"})
    puts = _render_admin(monkeypatch, fake)
    assert puts == [{"market_fees": {"note": "changed"}}]


# --- the admin settings notice -----------------------------------------------------------------
#
# Round J1.4: an admin settings notice that starts with "ERROR:" (an
# unreadable admin_settings.json) renders as st.error, the others as
# st.warning. Fake values only.

def test_an_error_notice_renders_as_an_error_and_others_as_warnings(monkeypatch):
    from streamlit_ui.pages import admin_settings as page

    shown = []
    fake = SimpleNamespace(
        error=lambda body, *a, **k: shown.append(("error", body)),
        warning=lambda body, *a, **k: shown.append(("warning", body)),
    )
    monkeypatch.setattr(page, "st", fake)
    monkeypatch.setattr(page, "api_get", lambda path: {"status": "success", "data": {
        "notices": ["ERROR: admin settings file is unreadable; ...", "industry/x was out of range"],
        "schema": {}, "values": {}, "invalid": {},
    }})
    page._load_settings()
    assert shown == [
        ("error", "ERROR: admin settings file is unreadable; ..."),
        ("warning", "industry/x was out of range"),
    ]
