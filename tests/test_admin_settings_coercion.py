# tests/test_admin_settings_coercion.py
"""Task 8c fix round 1: stored admin settings are checked against the schema
on load. An out-of-range or mistyped stored value used to pass straight
through: it changed behaviour silently and crashed st.number_input on the
settings page. Since the strict-settings follow-up a stored invalid number is
neither clamped nor replaced by the default: it is kept, logged as an ERROR
and fails where it is read (tests/test_admin_settings_strict_numbers.py). A
key the schema no longer has (the removed TE threshold) is ignored with an
INFO log. Fake values throughout."""
from __future__ import annotations

import json
import logging

import pytest

from eve_online_industry_tracker.config.admin_settings import (
    ADMIN_SETTINGS_SCHEMA,
    AdminSettingInvalid,
    AdminSettingsManager,
)


def _manager(tmp_path, payload) -> AdminSettingsManager:
    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return AdminSettingsManager(path)


def test_an_out_of_range_stored_value_is_kept_with_an_error_not_clamped(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    mgr = _manager(tmp_path, {"industry": {"max_blueprint_time_efficiency": 99}})
    with pytest.raises(AdminSettingInvalid, match="industry/max_blueprint_time_efficiency=99"):
        mgr.get("industry", "max_blueprint_time_efficiency")
    error = next(r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
    assert "industry/max_blueprint_time_efficiency" in error
    assert "99" in error and "0 to 20" in error


def test_a_mistyped_stored_value_gets_the_spec_type(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    mgr = _manager(tmp_path, {"daily_planner": {"planner_bpo_strong_buy_days": "45"}})
    value = mgr.get("daily_planner", "planner_bpo_strong_buy_days")
    assert value == 45 and not isinstance(value, str)
    assert any("planner_bpo_strong_buy_days" in r.getMessage() for r in caplog.records)


def test_an_unusable_stored_value_is_kept_with_an_error_not_the_default(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    mgr = _manager(tmp_path, {"industry": {"max_blueprint_time_efficiency": "lots"}})
    with pytest.raises(AdminSettingInvalid, match="lots"):
        mgr.get("industry", "max_blueprint_time_efficiency")
    assert any("lots" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)


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
# (the strict number inputs: tests/test_admin_settings_strict_numbers.py)

def test_the_settings_page_number_input_args_keep_a_valid_value_and_the_spec():
    from streamlit_ui.pages.admin_settings import _number_choice

    int_spec = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["max_blueprint_time_efficiency"]
    args, problem = _number_choice(int_spec, 18, None)
    assert problem is None
    assert args["value"] == 18 and args["min_value"] == 0 and args["max_value"] == 20

    float_spec = {"type": "float", "default": 1.0, "min": 0.5, "max": 2.0, "step": 0.1, "format": "%.1f"}
    args, problem = _number_choice(float_spec, 1, None)
    assert args["value"] == 1.0 and isinstance(args["value"], float) and problem is None
    assert args["format"] == "%.1f" and args["step"] == 0.1


# --- fix round 2: step and bool strings ----------------------------------------------

def test_an_odd_stored_te_is_kept_with_an_error_not_snapped(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    mgr = _manager(tmp_path, {"industry": {"max_blueprint_time_efficiency": 15}})
    with pytest.raises(AdminSettingInvalid, match="industry/max_blueprint_time_efficiency=15"):
        mgr.get("industry", "max_blueprint_time_efficiency")
    error = next(r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
    assert "max_blueprint_time_efficiency" in error and "15" in error and "step of 2" in error


def test_every_int_default_is_on_its_step_grid():
    for category in ADMIN_SETTINGS_SCHEMA.values():
        for key, spec in category["settings"].items():
            if spec["type"] == "int" and "step" in spec:
                assert (spec["default"] - spec.get("min", 0)) % spec["step"] == 0, key


@pytest.mark.parametrize("raw, expected", [
    ("false", False), ("0", False), ("no", False), ("off", False), ("False", False),
    ("true", True), ("1", True), ("yes", True), (0, False), (1, True), (True, True),
])
def test_bool_strings_parse(raw, expected):
    spec = {"type": "bool", "default": True}
    assert AdminSettingsManager._coerce(raw, spec) is expected


def test_an_unparseable_bool_string_is_kept_with_an_error_not_the_default(tmp_path, caplog):
    # Since the strict bool/select follow-up (tests/test_admin_settings_strict_bool_select.py).
    key = next(
        (c, k) for c, cat in ADMIN_SETTINGS_SCHEMA.items()
        for k, spec in cat["settings"].items() if spec["type"] == "bool"
    )
    caplog.set_level(logging.WARNING)
    mgr = _manager(tmp_path, {key[0]: {key[1]: "maybe"}})
    with pytest.raises(AdminSettingInvalid, match="maybe"):
        mgr.get(*key)
    assert any("maybe" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
