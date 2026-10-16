from __future__ import annotations

import streamlit as st

from streamlit_ui.api.client import api_get, api_get_or_error


@st.cache_data(ttl=3600)
def fetch_all_materials() -> list[str]:
    response = api_get("/materials") or {}
    materials = response.get("data") or []
    if not isinstance(materials, list):
        raise RuntimeError("Unexpected materials response from Flask API")
    return [str(entry.get("name") or "") for entry in materials if isinstance(entry, dict) and entry.get("name")]


def fetch_all_facilities() -> list[dict]:
    """The configured reprocessing facility (admin settings). Not cached: it
    changes with the settings. Raises with the API's reason on failure (an
    unset NPC station, a missing tax, an SDE that needs a re-import)."""
    response, problem = api_get_or_error("/facilities")
    if problem:
        raise RuntimeError(problem)
    facilities = (response or {}).get("data") or []
    if not isinstance(facilities, list):
        raise RuntimeError("Unexpected facilities response from Flask API")
    return [facility for facility in facilities if isinstance(facility, dict)]