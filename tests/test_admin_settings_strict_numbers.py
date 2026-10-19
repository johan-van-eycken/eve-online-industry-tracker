"""Every numeric admin setting is strict, like reprocessing/tax since PR #14.

- Saving a value outside its range, off its step grid, a non-numeric string
  or a bool is refused (ValueError naming category/key and the range: the
  settings route's 400).
- A stored invalid value is kept as stored, logged as an ERROR at load,
  listed by invalid() (the Admin Settings page and /health show it) and
  raises AdminSettingInvalid naming it where it is read, except the settings
  marked startup_critical (ESI pacing, the market refresh loop): those read
  as the schema default, loudly, so the ESI client keeps working.

Fake values throughout.
"""
from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.config.admin_settings import (
    ADMIN_SETTINGS_SCHEMA,
    AdminSettingInvalid,
    AdminSettingsManager,
)


def _manager(tmp_path, payload=None) -> AdminSettingsManager:
    path = tmp_path / "admin_settings.json"
    if payload is not None:
        path.write_text(json.dumps(payload), encoding="utf-8")
    return AdminSettingsManager(path)


def _numeric_settings():
    return [(c, k, spec) for c, cat in ADMIN_SETTINGS_SCHEMA.items() for k, spec in cat["settings"].items()
            if spec["type"] in ("int", "float")]


def _out_of_range(spec):
    if "max" in spec:
        return spec["max"] + (1 if spec["type"] == "int" else 0.5)
    return spec["min"] - 1


STARTUP_CRITICAL = {
    ("performance", "esi_pagination_sleep_seconds"),
    ("esi_resilience", "esi_error_budget_low_watermark"),
    ("esi_resilience", "esi_error_budget_max_sleep_seconds"),
    ("esi_resilience", "esi_error_budget_pacing_sleep_seconds"),
    ("daily_planner", "planner_market_refresh_interval_hours"),
}


# --- saving ----------------------------------------------------------------------------

@pytest.mark.parametrize("category, key, spec", [s for s in _numeric_settings() if "min" in s[2] or "max" in s[2]],
                         ids=lambda v: v if isinstance(v, str) else "")
def test_saving_any_numeric_setting_out_of_range_is_refused_naming_it_and_its_range(tmp_path, category, key, spec):
    mgr = _manager(tmp_path)
    before = mgr.get(category, key)
    with pytest.raises(ValueError) as err:
        mgr.set(category, key, _out_of_range(spec))
    message = str(err.value)
    assert f"{category}/{key}" in message
    assert repr(spec.get("min")) in message and repr(spec.get("max")) in message
    with pytest.raises(ValueError, match=f"{category}/{key}"):
        mgr.set_bulk({category: {key: _out_of_range(spec)}})
    assert mgr.get(category, key) == before
    assert not (tmp_path / "admin_settings.json").exists()   # nothing persisted


@pytest.mark.parametrize("value", ["lots", "", True, False, [5], {"v": 5}, None, float("nan"), float("inf")])
def test_saving_a_non_number_or_a_bool_is_refused_not_coerced(tmp_path, value):
    mgr = _manager(tmp_path)
    for category, key in (("industry", "max_blueprint_material_efficiency"), ("industry", "scc_surcharge")):
        with pytest.raises(ValueError, match=f"{category}/{key}"):
            mgr.set(category, key, value)


def test_saving_a_fractional_value_for_an_int_setting_is_refused(tmp_path):
    mgr = _manager(tmp_path)
    with pytest.raises(ValueError, match="industry/portfolio_order_count_threshold.*not an integer"):
        mgr.set("industry", "portfolio_order_count_threshold", 5.5)
    mgr.set("industry", "portfolio_order_count_threshold", 6.0)   # an integral float is the integer
    assert mgr.get("industry", "portfolio_order_count_threshold") == 6


def test_saving_an_off_step_te_is_refused_not_snapped(tmp_path):
    mgr = _manager(tmp_path)
    with pytest.raises(ValueError, match="industry/max_blueprint_time_efficiency.*step"):
        mgr.set("industry", "max_blueprint_time_efficiency", 15)
    mgr.set("industry", "max_blueprint_time_efficiency", 16)
    assert mgr.get("industry", "max_blueprint_time_efficiency") == 16


def test_numeric_strings_and_the_bounds_themselves_are_accepted(tmp_path):
    mgr = _manager(tmp_path)
    mgr.set("daily_planner", "planner_bpo_strong_buy_days", " 45 ")
    assert mgr.get("daily_planner", "planner_bpo_strong_buy_days") == 45
    mgr.set("industry", "scc_surcharge", "0.04")
    assert mgr.get("industry", "scc_surcharge") == 0.04
    mgr.set("market_defaults", "default_sales_tax", 0.2)
    mgr.set("daily_planner", "planner_mineral_correlation_threshold", -1)
    assert mgr.get("daily_planner", "planner_mineral_correlation_threshold") == -1.0


def test_a_bulk_save_with_one_bad_value_saves_nothing(tmp_path):
    mgr = _manager(tmp_path)
    with pytest.raises(ValueError, match="industry/scc_surcharge"):
        mgr.set_bulk({"industry": {"broker_fee_base": 0.02, "scc_surcharge": 0.9}})
    assert mgr.get("industry", "broker_fee_base") == ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["broker_fee_base"]["default"]
    assert not (tmp_path / "admin_settings.json").exists()


def test_the_settings_route_answers_400_naming_the_setting_and_range(tmp_path):
    from flask_app.app import create_app

    app = create_app()
    app.extensions["app_state"] = SimpleNamespace(admin_settings=_manager(tmp_path), db_app=None, db_sde=None, db_oauth=None)
    response = app.test_client().put("/admin_settings", json={"market_defaults": {"default_sales_tax": 0.9}})
    assert response.status_code == 400
    message = response.get_json()["message"]
    assert "market_defaults/default_sales_tax" in message and "0.0" in message and "0.2" in message


# --- a stored invalid value ---------------------------------------------------------------

def test_a_stored_out_of_range_value_is_kept_logged_and_raises_where_read(tmp_path, caplog):
    with caplog.at_level(logging.ERROR):
        mgr = _manager(tmp_path, {"industry": {"scc_surcharge": 0.9}})
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any("industry/scc_surcharge" in m and "0.9" in m for m in errors)
    with pytest.raises(AdminSettingInvalid) as err:
        mgr.get("industry", "scc_surcharge")
    assert "industry/scc_surcharge=0.9" in err.value.message and "Admin Settings" in err.value.message
    assert isinstance(err.value, ServiceError) and err.value.status_code == 500
    assert not isinstance(err.value, (KeyError, AttributeError, ValueError))   # no fallback wrapper swallows it
    assert mgr.get_all()["industry"]["scc_surcharge"] == 0.9                  # shown as stored, not replaced
    (problem,) = [p for p in mgr.invalid()["industry"].values()]
    assert problem["value"] == 0.9 and "0.0 to 0.2" in problem["problem"] and not problem["startup_critical"]


@pytest.mark.parametrize("raw", ["lots", True, None, [1]])
def test_a_stored_mistyped_value_is_invalid_not_coerced(tmp_path, raw, caplog):
    with caplog.at_level(logging.ERROR):
        mgr = _manager(tmp_path, {"industry": {"max_blueprint_material_efficiency": raw}})
    assert any("industry/max_blueprint_material_efficiency" in r.getMessage()
               for r in caplog.records if r.levelno == logging.ERROR)
    with pytest.raises(AdminSettingInvalid, match="industry/max_blueprint_material_efficiency"):
        mgr.get("industry", "max_blueprint_material_efficiency")


def test_saving_a_valid_value_clears_the_stored_problem(tmp_path):
    mgr = _manager(tmp_path, {"industry": {"scc_surcharge": 0.9}})
    mgr.set("industry", "scc_surcharge", 0.05)
    assert mgr.get("industry", "scc_surcharge") == 0.05 and mgr.invalid() == {}
    mgr = _manager(tmp_path, {"industry": {"scc_surcharge": 0.9}})
    mgr.reset_to_defaults()
    assert mgr.invalid() == {}


def test_the_startup_critical_settings_are_the_esi_pacing_and_the_refresh_loop():
    marked = {(c, k) for c, k, spec in _numeric_settings() if spec.get("startup_critical")}
    assert marked == STARTUP_CRITICAL


@pytest.mark.parametrize("category, key", sorted(STARTUP_CRITICAL))
def test_a_stored_invalid_startup_critical_value_reads_as_the_default_loudly(tmp_path, caplog, category, key):
    spec = ADMIN_SETTINGS_SCHEMA[category]["settings"][key]
    with caplog.at_level(logging.ERROR):
        mgr = _manager(tmp_path, {category: {key: _out_of_range(spec)}})
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any(f"{category}/{key}" in m and "default" in m for m in errors)
    assert mgr.get(category, key) == spec["default"]
    assert mgr.invalid()[category][key]["startup_critical"] is True
    assert mgr.get_all()[category][key] == _out_of_range(spec)


def test_the_esi_limiter_keeps_working_with_an_invalid_pacing_setting(tmp_path):
    from eve_online_industry_tracker.infrastructure.esi_client import _EsiErrorRateLimiter

    limiter = _EsiErrorRateLimiter()
    limiter._admin_settings = _manager(tmp_path, {"esi_resilience": {"esi_error_budget_pacing_sleep_seconds": 99}})
    limiter.update_from_headers({"X-Esi-Error-Limit-Remain": "2", "X-Esi-Error-Limit-Reset": "30"})
    assert limiter.suggested_sleep_seconds() == pytest.approx(0.2)   # the schema default, not 99


def test_the_problems_are_listed_for_health(tmp_path):
    mgr = _manager(tmp_path, {"industry": {"scc_surcharge": 0.9},
                              "esi_resilience": {"esi_error_budget_max_sleep_seconds": 5}})
    problems = mgr.problems()
    assert len(problems) == 2
    assert any("industry/scc_surcharge=0.9" in p for p in problems)
    assert any("esi_resilience/esi_error_budget_max_sleep_seconds=5" in p and "default 60" in p for p in problems)


def test_health_shows_the_invalid_settings(tmp_path):
    from eve_online_industry_tracker.application.admin.service import AdminService

    state = SimpleNamespace(init_state="Ready", init_warnings=[], sde_warnings=[], db_sde=None,
                            admin_settings=_manager(tmp_path, {"industry": {"scc_surcharge": 0.9}}))
    payload, status = AdminService(state=state).health_payload()
    assert status == 200
    assert any("industry/scc_surcharge=0.9" in w for w in payload["warnings"])


def test_the_settings_route_sends_the_invalid_settings(tmp_path):
    from flask_app.app import create_app

    app = create_app()
    app.extensions["app_state"] = SimpleNamespace(admin_settings=_manager(tmp_path, {"industry": {"scc_surcharge": 0.9}}),
                                                 db_app=None, db_sde=None, db_oauth=None)
    data = app.test_client().get("/admin_settings").get_json()["data"]
    assert data["values"]["industry"]["scc_surcharge"] == 0.9
    assert "0.0 to 0.2" in data["invalid"]["industry"]["scc_surcharge"]["problem"]


# --- where the settings are read ----------------------------------------------------------

def test_the_market_intelligence_readers_do_not_swallow_an_invalid_setting(tmp_path):
    from eve_online_industry_tracker.application.market_intelligence.margin_correlator import MarginCorrelator
    from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import MarketDepthCollector

    mgr = _manager(tmp_path, {"daily_planner": {"planner_price_outlier_sigma": 50,
                                                "planner_mineral_correlation_min_days": 1,
                                                "planner_mineral_correlation_threshold": 0.5}})
    collector = MarketDepthCollector.__new__(MarketDepthCollector)
    collector._admin = mgr
    with pytest.raises(AdminSettingInvalid, match="planner_price_outlier_sigma"):
        collector._get_outlier_sigma()
    correlator = MarginCorrelator(state=SimpleNamespace(), admin_settings=mgr)
    with pytest.raises(AdminSettingInvalid, match="planner_mineral_correlation_min_days"):
        correlator._get_min_days()
    with pytest.raises(AdminSettingInvalid, match="planner_mineral_correlation_threshold"):
        correlator._get_threshold()


def test_the_daily_planner_reader_does_not_swallow_an_invalid_setting(tmp_path):
    from eve_online_industry_tracker.application.daily_planner.admin_setting import admin_setting

    mgr = _manager(tmp_path, {"daily_planner": {"planner_history_days": 2}})
    with pytest.raises(AdminSettingInvalid, match="daily_planner/planner_history_days=2"):
        admin_setting(mgr, "daily_planner", "planner_history_days", 90)


# --- the settings page --------------------------------------------------------------------

def test_the_settings_page_shows_a_stored_invalid_number_as_an_error_not_a_clamped_value():
    from streamlit_ui.pages.admin_settings import _number_choice

    spec = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["max_blueprint_time_efficiency"]
    args, problem = _number_choice(spec, 99, {"value": 99, "problem": "99 is outside 0 to 20",
                                              "startup_critical": False})
    assert args["value"] is None                      # nothing pre-filled that a save would write over it
    assert "99" in problem and "0 to 20" in problem and "fail" in problem
    args, problem = _number_choice(spec, 18, None)
    assert args["value"] == 18 and problem is None
    args, problem = _number_choice(ADMIN_SETTINGS_SCHEMA["esi_resilience"]["settings"]["esi_max_retries"], 4, None)
    assert args["value"] == 4 and args["min_value"] == 1 and args["max_value"] == 10


def test_the_settings_page_says_a_startup_critical_setting_uses_its_default():
    from streamlit_ui.pages.admin_settings import _number_choice

    spec = ADMIN_SETTINGS_SCHEMA["esi_resilience"]["settings"]["esi_error_budget_max_sleep_seconds"]
    _, problem = _number_choice(spec, 5, {"value": 5, "problem": "5 is outside 10 to 300", "startup_critical": True})
    assert "default" in problem and "60" in problem


def test_the_settings_page_flags_an_out_of_range_value_even_without_the_servers_word():
    from streamlit_ui.pages.admin_settings import _number_choice

    spec = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["max_blueprint_time_efficiency"]
    for stored in (99, -5, "junk", True):
        args, problem = _number_choice(spec, stored, None)
        assert args["value"] is None and problem


def test_every_schema_number_setting_gets_valid_number_input_args():
    from streamlit_ui.pages.admin_settings import _number_choice

    for _, _, spec in _numeric_settings():
        for stored in (spec["default"], 10**9, -(10**9), None, "x"):
            args, problem = _number_choice(spec, stored, None)
            if args["value"] is None:
                assert problem or spec.get("optional")
                continue
            if args["min_value"] is not None:
                assert args["value"] >= args["min_value"]
            if args["max_value"] is not None:
                assert args["value"] <= args["max_value"]


def test_the_settings_page_only_saves_an_invalid_setting_when_a_value_is_entered():
    from streamlit_ui.pages.admin_settings import NO_CHANGE, _pending_value

    assert _pending_value(new=None, current=99, problem="99 is outside") is NO_CHANGE   # left alone: kept
    assert _pending_value(new=18, current=99, problem="99 is outside") == 18
    assert _pending_value(new=18, current=18, problem=None) is NO_CHANGE
    assert _pending_value(new=16, current=18, problem=None) == 16
    assert _pending_value(new=None, current=0.02, problem=None) is None             # clearing an optional one
