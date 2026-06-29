from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_ui.api.client import api_get


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


@st.cache_data(ttl=3600)
def fetch_corp_market_orders(corporation_id: int) -> dict[str, Any]:
    response = api_get(f"/corporations/{corporation_id}/market_orders?refresh=0", timeout_seconds=60) or {}
    return response if isinstance(response, dict) else {}


def refresh_corp_market_orders(corporation_id: int) -> None:
    response = api_get(f"/corporations/{corporation_id}/market_orders?refresh=1", timeout_seconds=120) or {}
    if isinstance(response, dict) and response.get("status") not in {None, "success"}:
        raise RuntimeError(response.get("message") or "Failed to refresh corporation market orders")


def clear_corp_market_orders_cache() -> None:
    fetch_corp_market_orders.clear()