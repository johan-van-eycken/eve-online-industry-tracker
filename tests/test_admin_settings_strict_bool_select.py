"""Bool and select admin settings are strict too, like the numbers
(tests/test_admin_settings_strict_numbers.py).

- Saving a value that is not a boolean, or not one of a select's options, is
  refused (ValueError naming category/key: the settings route's 400).
- A stored invalid value is kept as stored, logged as an ERROR at load,
  listed by invalid()/problems() (the Admin Settings page and /health) and
  raises AdminSettingInvalid where it is read. No bool or select is read on
  a startup-critical path (an ESI request, a daemon loop), so none is
  startup_critical.
- daily_planner/planner_market_hub is a strict select of the hubs the market
  depth collector knows (Jita), no longer a free string with a silent except.

Fake values throughout.
"""
from __future__ import annotations

import logging

import pytest

from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA, AdminSettingInvalid
from fakes import _manager


def _settings_of_type(*types):
    return [(c, k, spec) for c, cat in ADMIN_SETTINGS_SCHEMA.items() for k, spec in cat["settings"].items()
            if spec["type"] in types]


BOOL = ("industry", "invention_decryptor_selection_enabled")
HANGAR = ("industry", "industry_hangar_flag")
HUB = ("daily_planner", "planner_market_hub")


# --- the schema --------------------------------------------------------------------------

def test_every_setting_is_a_number_bool_or_select():
    types = {spec["type"] for cat in ADMIN_SETTINGS_SCHEMA.values() for spec in cat["settings"].values()}
    assert types <= {"int", "float", "bool", "select"}   # no free "str" left: each type is checked


def test_no_bool_or_select_is_startup_critical():
    # None is read on an ESI request or by a daemon loop (the market intelligence loop reads
    # planner_market_hub per cycle, and a failed cycle is logged and kept as its last_error).
    assert [(c, k) for c, k, spec in _settings_of_type("bool", "select") if spec.get("startup_critical")] == []


def test_the_market_hub_is_a_strict_select_of_the_collector_hubs():
    from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import HUB_REGION

    spec = ADMIN_SETTINGS_SCHEMA[HUB[0]]["settings"][HUB[1]]
    assert spec["type"] == "select" and spec["strict"] and spec["default"] == "jita"
    assert [opt["value"] for opt in spec["options"]] == sorted(HUB_REGION)


# --- saving ------------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["maybe", 2, 0.5, None, [], {"on": True}])
def test_saving_an_invalid_bool_is_refused(tmp_path, raw):
    mgr = _manager(tmp_path)
    with pytest.raises(ValueError, match="industry/invention_decryptor_selection_enabled"):
        mgr.set(*BOOL, raw)
    assert mgr.get(*BOOL) is True


@pytest.mark.parametrize("raw, expected", [(True, True), (False, False), ("off", False), ("yes", True),
                                           (0, False), (1, True)])
def test_saving_a_bool_reads_it(tmp_path, raw, expected):
    mgr = _manager(tmp_path)
    mgr.set(*BOOL, raw)
    assert mgr.get(*BOOL) is expected


@pytest.mark.parametrize("key, raw", [(HANGAR, "CorpSAGInvalid"), (HANGAR, 3), (HUB, "amarr"), (HUB, "Jita"),
                                      (HUB, "")])
def test_saving_a_value_that_is_not_an_option_is_refused(tmp_path, key, raw):
    mgr = _manager(tmp_path)
    with pytest.raises(ValueError, match=f"{key[0]}/{key[1]}"):
        mgr.set(*key, raw)


def test_one_invalid_select_saves_none_of_a_bulk_update(tmp_path):
    mgr = _manager(tmp_path)
    with pytest.raises(ValueError, match="industry_hangar_flag"):
        mgr.set_bulk({"industry": {"invention_decryptor_selection_enabled": False,
                                   "industry_hangar_flag": "CorpSAG9"}})
    assert mgr.get(*BOOL) is True


def test_the_hangar_no_filter_option_still_takes_its_empty_spellings(tmp_path):
    mgr = _manager(tmp_path)
    for raw in (None, "", "null", "None"):
        mgr.set(*HANGAR, "CorpSAG2")
        mgr.set(*HANGAR, raw)
        assert mgr.get(*HANGAR) is None


# --- a stored invalid value --------------------------------------------------------------

@pytest.mark.parametrize("key, raw", [(BOOL, "maybe"), (BOOL, 7), (HANGAR, "CorpSAG9"), (HUB, "amarr")])
def test_a_stored_invalid_value_is_kept_listed_and_raises_at_use(tmp_path, caplog, key, raw):
    category, name = key
    with caplog.at_level(logging.WARNING):
        mgr = _manager(tmp_path, {category: {name: raw}})
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any(f"{category}/{name}" in m for m in errors)
    with pytest.raises(AdminSettingInvalid, match=f"{category}/{name}"):
        mgr.get(category, name)
    entry = mgr.invalid()[category][name]
    assert entry["value"] == raw and entry["startup_critical"] is False
    assert mgr.get_all()[category][name] == raw                     # shown as stored, not the default
    assert any(f"{category}/{name}={raw!r}" in p for p in mgr.problems())


def test_saving_a_valid_value_clears_a_stored_invalid_one(tmp_path):
    mgr = _manager(tmp_path, {"industry": {"invention_decryptor_selection_enabled": "maybe"}})
    mgr.set(*BOOL, False)
    assert mgr.get(*BOOL) is False and mgr.invalid() == {}


# --- the market depth collector ------------------------------------------------------------

def test_the_collector_raises_on_an_invalid_hub(tmp_path):
    from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import (
        MarketDepthCollector,
    )

    collector = MarketDepthCollector.__new__(MarketDepthCollector)
    collector._admin = _manager(tmp_path, {"daily_planner": {"planner_market_hub": "amarr"}})
    with pytest.raises(AdminSettingInvalid, match="planner_market_hub"):
        collector._get_hub_and_region()
    collector._admin = _manager(tmp_path, {"daily_planner": {"planner_market_hub": "jita"}})
    assert collector._get_hub_and_region() == ("jita", 10000002)
    collector._admin = None
    assert collector._get_hub_and_region() == ("jita", 10000002)


def test_the_collector_does_not_swallow_a_failing_settings_store():
    from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import (
        MarketDepthCollector,
    )

    class _Broken:
        def get(self, category, key):
            raise RuntimeError("settings store down")

    collector = MarketDepthCollector.__new__(MarketDepthCollector)
    collector._admin = _Broken()
    with pytest.raises(RuntimeError, match="settings store down"):
        collector._get_hub_and_region()


# --- the settings page ---------------------------------------------------------------------

def test_the_page_shows_a_stored_invalid_bool_as_its_own_choice():
    from streamlit_ui.pages.admin_settings import _bool_choices

    spec = ADMIN_SETTINGS_SCHEMA[BOOL[0]]["settings"][BOOL[1]]
    assert _bool_choices(spec, True, None) is None                   # valid: the checkbox
    values, labels, index, problem = _bool_choices(
        spec, "maybe", {"value": "maybe", "problem": "not a boolean: 'maybe'", "startup_critical": False})
    assert values[index] == "maybe" and "maybe" in labels[index]
    assert set(values[1:]) == {True, False}
    assert "maybe" in problem and "fail with an error" in problem


# --- review round 1 ------------------------------------------------------------------------

def test_an_empty_string_is_not_a_boolean(tmp_path):
    # M2: "" used to read as False.
    with pytest.raises(ValueError, match="not a boolean"):
        _manager(tmp_path).set(*BOOL, "")
    mgr = _manager(tmp_path, {"industry": {"invention_decryptor_selection_enabled": ""}})
    with pytest.raises(AdminSettingInvalid):
        mgr.get(*BOOL)


@pytest.mark.parametrize("raw", ["Jita", "JITA", " jita "])
def test_a_case_variant_of_an_option_is_refused_with_a_hint(tmp_path, raw):
    # M5: still refused (the planner queries by the exact hub), but the error names the option.
    with pytest.raises(ValueError, match="did you mean 'jita'"):
        _manager(tmp_path).set(*HUB, raw)
    mgr = _manager(tmp_path, {"daily_planner": {"planner_market_hub": raw}})
    assert "did you mean 'jita'" in mgr.invalid()["daily_planner"]["planner_market_hub"]["problem"]


def test_a_value_unlike_every_option_has_no_hint(tmp_path):
    with pytest.raises(ValueError) as excinfo:
        _manager(tmp_path).set(*HUB, "amarr")
    assert "did you mean" not in str(excinfo.value)


@pytest.mark.parametrize("stored", [1.0, 0.0, 1, "maybe"])
def test_a_stored_invalid_bool_is_fixed_by_picking_on_or_off(stored):
    # M1: picking On over a stored 1.0 was "no change" (True == 1.0), so it could never be fixed.
    from streamlit_ui.pages.admin_settings import NO_CHANGE, _choice_to_save

    assert _choice_to_save(True, stored, problem="invalid") is True
    assert _choice_to_save(False, stored, problem="invalid") is False
    assert _choice_to_save(stored, stored, problem="invalid") is NO_CHANGE    # the invalid entry kept
    assert _choice_to_save(True, True, problem=None) is NO_CHANGE
    assert _choice_to_save(False, True, problem=None) is False
