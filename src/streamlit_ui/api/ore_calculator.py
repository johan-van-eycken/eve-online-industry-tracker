from __future__ import annotations

import streamlit as st

from streamlit_ui.api.client import api_get


@st.cache_data(ttl=3600)
def fetch_all_materials() -> list[str]:
    response = api_get("/materials") or {}
    materials = response.get("data") or []
    if not isinstance(materials, list):
        raise RuntimeError("Unexpected materials response from Flask API")
    return [str(entry.get("name") or "") for entry in materials if isinstance(entry, dict) and entry.get("name")]


@st.cache_data(ttl=3600)
def fetch_all_facilities() -> list[dict]:
    response = api_get("/facilities") or {}
    facilities = response.get("data") or []
    if not isinstance(facilities, list):
        raise RuntimeError("Unexpected facilities response from Flask API")
    return [facility for facility in facilities if isinstance(facility, dict)]