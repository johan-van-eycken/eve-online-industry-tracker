from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_ui.api.client import api_get


@st.cache_data(ttl=60, show_spinner=False)
def fetch_active_industry_jobs(*, character_id: int | None = None) -> dict[str, Any]:
    """Return {"jobs": [...], "slot_capacities": {char_id_str: {...}}}."""
    path = "/industry_active_jobs"
    if character_id is not None:
        path += f"?character_id={int(character_id)}"

    response = api_get(path) or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to load active industry jobs")

    data = response.get("data") or {}
    if isinstance(data, dict):
        return data
    # Backward compat if data is a list (old format)
    return {"jobs": data if isinstance(data, list) else [], "slot_capacities": {}}


def clear_active_jobs_cache() -> None:
    fetch_active_industry_jobs.clear()
