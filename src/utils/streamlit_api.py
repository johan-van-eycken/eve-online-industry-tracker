from __future__ import annotations

import streamlit as st

from utils.flask_api import api_get


@st.cache_data(ttl=300)
def cached_api_get(path: str, timeout_seconds: float | None = None):
    return api_get(path, timeout_seconds=timeout_seconds)