"""The admin settings a Streamlit page reads to render (thresholds), from
/admin_settings at each render so a change on the settings page applies on
the next rerun. A stored invalid value or a failed read is named in the
problems the page shows; the schema default is used for it meanwhile."""
from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA
from streamlit_ui.api.client import api_get_or_error

PLANNER_PAGE_KEYS: tuple[str, ...] = (
    "planner_freshness_warning_level",       # the "prices drifted slightly" caption
    "planner_max_plan_age_hours",            # expired mode
    "planner_market_refresh_interval_hours", # the market data age shown as fresh
)


def settings_values(category: str, keys: tuple[str, ...]) -> tuple[dict[str, float], list[str]]:
    """({key: value}, problems) for these number settings of `category`."""
    specs = ADMIN_SETTINGS_SCHEMA[category]["settings"]
    values = {key: float(specs[key]["default"]) for key in keys}
    body, failure = api_get_or_error("/admin_settings")
    if failure:
        return values, [f"Admin settings could not be read ({failure}): the {category} defaults are used"]
    data = (body or {}).get("data") or {}
    stored: dict[str, Any] = (data.get("values") or {}).get(category) or {}
    invalid: dict[str, Any] = (data.get("invalid") or {}).get(category) or {}
    problems: list[str] = []
    for key in keys:
        if key in invalid:
            entry = invalid[key] or {}
            problems.append(f"admin setting {category}/{key}={entry.get('value')!r} is invalid "
                            f"({entry.get('problem')}): the default {specs[key]['default']!r} is used")
        elif stored.get(key) is not None:
            values[key] = float(stored[key])
    return values, problems


def planner_page_settings() -> tuple[dict[str, float], list[str]]:
    return settings_values("daily_planner", PLANNER_PAGE_KEYS)
