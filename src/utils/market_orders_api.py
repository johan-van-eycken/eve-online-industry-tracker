from __future__ import annotations

from typing import Any

import streamlit as st

from utils.flask_api import api_get


@st.cache_data(ttl=3600)
def fetch_market_orders() -> dict[str, Any]:
    response = api_get("/characters/market_orders?refresh=0", timeout_seconds=60) or {}
    return response if isinstance(response, dict) else {}


def refresh_market_orders() -> None:
    response = api_get("/characters/market_orders?refresh=1", timeout_seconds=120) or {}
    if isinstance(response, dict) and response.get("status") not in {None, "success"}:
        raise RuntimeError(response.get("message") or "Failed to refresh market orders")


def clear_market_orders_cache() -> None:
    fetch_market_orders.clear()