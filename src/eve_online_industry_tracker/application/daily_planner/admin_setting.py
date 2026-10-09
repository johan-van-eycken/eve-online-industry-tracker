"""The daily planner's one admin-settings reader.

Every `_adm` helper in this package delegates here, and settings outside the
daily_planner section (industry.invention_probability_floor) are read with it
directly. Literal section/key reads are pinned to the schema by
tests/test_daily_planner_fail_loud.py.
"""
from __future__ import annotations

from typing import Any


def admin_setting(admin_settings: Any, section: str, key: str, fallback: Any) -> Any:
    """admin_settings.get(section, key), or `fallback` when there is no
    settings store. AttributeError covers stub/None admin objects; KeyError is
    what AdminSettingsManager.get raises for an unknown key."""
    try:
        return admin_settings.get(section, key)
    except (AttributeError, KeyError):
        return fallback
