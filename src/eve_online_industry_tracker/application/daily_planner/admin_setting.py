"""The daily planner's one admin-settings reader.

Every `_adm` helper in this package delegates here, and settings outside the
daily_planner section (industry.invention_probability_floor) are read with it
directly. Literal section/key reads are pinned to the schema by
tests/test_daily_planner_fail_loud.py.
"""
from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA

#: The fallback when none is given: the setting's schema default (no
#: duplicated constant that can drift from it).
SCHEMA_DEFAULT: Any = object()


def schema_default(section: str, key: str) -> Any:
    """The schema default of admin setting section/key."""
    return ADMIN_SETTINGS_SCHEMA[section]["settings"][key]["default"]


def admin_setting(admin_settings: Any, section: str, key: str, fallback: Any = SCHEMA_DEFAULT) -> Any:
    """admin_settings.get(section, key), or `fallback` (by default the
    schema default) when there is no settings store. AttributeError covers
    stub/None admin objects; KeyError is what AdminSettingsManager.get raises
    for an unknown key."""
    try:
        return admin_settings.get(section, key)
    except (AttributeError, KeyError):
        return schema_default(section, key) if fallback is SCHEMA_DEFAULT else fallback
