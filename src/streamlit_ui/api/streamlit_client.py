from __future__ import annotations

import streamlit as st

from streamlit_ui.api.client import api_get


class _FailedResponse(RuntimeError):
    """Raised inside the cached fetch so st.cache_data does not keep a failure."""

    def __init__(self, response):
        super().__init__("API request failed")
        self.response = response


@st.cache_data(ttl=300)
def _cached_api_get(path: str, timeout_seconds: float | None = None):
    response = api_get(path, timeout_seconds=timeout_seconds)
    if response is None or (isinstance(response, dict) and response.get("status") == "error"):
        raise _FailedResponse(response)
    return response


def cached_api_get(path: str, timeout_seconds: float | None = None):
    """api_get, cached for 5 minutes; a failed response (None or an error
    dict) is returned but not cached, so the next run asks again."""
    try:
        return _cached_api_get(path, timeout_seconds=timeout_seconds)
    except _FailedResponse as exc:
        return exc.response


cached_api_get.clear = _cached_api_get.clear  # type: ignore[attr-defined]
