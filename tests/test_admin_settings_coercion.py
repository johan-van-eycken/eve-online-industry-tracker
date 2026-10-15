# tests/test_admin_settings_coercion.py
"""Task 8c fix round 1: stored admin settings are coerced against the schema
on load. An out-of-range or mistyped stored value used to pass straight
through: it changed behaviour silently and crashed st.number_input on the
settings page. A key the schema no longer has (the removed TE threshold) is
ignored with an INFO log. Fake values throughout."""
from __future__ import annotations

import json
import logging

import pytest

from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA, AdminSettingsManager


def _manager(tmp_path, payload) -> AdminSettingsManager:
    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return AdminSettingsManager(path)


def test_an_out_of_range_stored_value_is_clamped_with_a_warning(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    mgr = _manager(tmp_path, {"industry": {"max_blueprint_time_efficiency": 99}})
    assert mgr.get("industry", "max_blueprint_time_efficiency") == 20
    warning = next(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
    assert "industry/max_blueprint_time_efficiency" in warning
    assert "99" in warning and "20" in warning


def test_a_mistyped_stored_value_gets_the_spec_type(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    mgr = _manager(tmp_path, {"daily_planner": {"planner_bpo_strong_buy_days": "45"}})
    value = mgr.get("daily_planner", "planner_bpo_strong_buy_days")
    assert value == 45 and not isinstance(value, str)
    assert any("planner_bpo_strong_buy_days" in r.getMessage() for r in caplog.records)


def test_an_unusable_stored_value_falls_back_to_the_default(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    mgr = _manager(tmp_path, {"industry": {"max_blueprint_time_efficiency": "lots"}})
    assert mgr.get("industry", "max_blueprint_time_efficiency") == 20
    assert any("lots" in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)


def test_a_valid_stored_value_loads_unchanged_without_a_warning(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    mgr = _manager(tmp_path, {"industry": {"max_blueprint_time_efficiency": 18,
                                           "industry_hangar_flag": "CorpSAG1"}})
    assert mgr.get("industry", "max_blueprint_time_efficiency") == 18
    assert mgr.get("industry", "industry_hangar_flag") == "CorpSAG1"
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_the_removed_te_threshold_key_is_ignored_with_an_info_log(tmp_path, caplog):
    assert "planner_optimal_te_threshold_pct" not in ADMIN_SETTINGS_SCHEMA["daily_planner"]["settings"]
    caplog.set_level(logging.INFO)
    mgr = _manager(tmp_path, {"daily_planner": {"planner_optimal_te_threshold_pct": 1.0}})
    infos = [r for r in caplog.records if "planner_optimal_te_threshold_pct" in r.getMessage()]
    assert infos and all(r.levelno == logging.INFO for r in infos)
    assert "planner_optimal_te_threshold_pct" not in mgr.get_all()["daily_planner"]
    with pytest.raises(KeyError):
        mgr.get("daily_planner", "planner_optimal_te_threshold_pct")


def test_the_te_max_setting_is_a_percentage_0_to_20():
    spec = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["max_blueprint_time_efficiency"]
    assert spec["min"] == 0 and spec["max"] == 20
    assert "%" in spec["help"] and "0" in spec["help"] and "20" in spec["help"]


# --- the settings page -------------------------------------------------------------

def test_the_settings_page_number_input_args_never_put_the_value_out_of_range():
    from streamlit_ui.pages.admin_settings import _number_input_args

    int_spec = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["max_blueprint_time_efficiency"]
    assert _number_input_args(int_spec, 99)["value"] == 20
    assert _number_input_args(int_spec, -5)["value"] == 0
    assert _number_input_args(int_spec, "junk")["value"] == int_spec["default"]
    args = _number_input_args(int_spec, 18)
    assert args["value"] == 18 and args["min_value"] == 0 and args["max_value"] == 20

    float_spec = {"type": "float", "default": 1.0, "min": 0.5, "max": 2.0, "step": 0.1, "format": "%.1f"}
    args = _number_input_args(float_spec, 7.0)
    assert args["value"] == 2.0 and isinstance(args["value"], float)
    assert args["format"] == "%.1f" and args["step"] == 0.1


def test_every_schema_number_setting_gets_valid_number_input_args():
    from streamlit_ui.pages.admin_settings import _number_input_args

    for category in ADMIN_SETTINGS_SCHEMA.values():
        for spec in category["settings"].values():
            if spec["type"] not in ("int", "float"):
                continue
            for stored in (spec["default"], 10**9, -(10**9), None, "x"):
                args = _number_input_args(spec, stored)
                if args["min_value"] is not None:
                    assert args["value"] >= args["min_value"]
                if args["max_value"] is not None:
                    assert args["value"] <= args["max_value"]
