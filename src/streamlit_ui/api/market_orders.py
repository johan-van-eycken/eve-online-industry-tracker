from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_ui.api.client import api_get_or_error


def _get(path: str, timeout_seconds: int, what: str) -> dict[str, Any]:
    """The API reply, or RuntimeError with the API's message (e.g. an
    invalid admin setting the order books read), so the page shows why
    instead of an empty order list. A failure is not cached."""
    response, problem = api_get_or_error(path, timeout_seconds=timeout_seconds)
    if problem:
        raise RuntimeError(f"{what}: {problem}")
    if not isinstance(response, dict):
        raise RuntimeError(f"{what}: unexpected response from the Flask API")
    if response.get("status") not in {None, "success"}:
        raise RuntimeError(f"{what}: {response.get('message') or 'failed'}")
    return response


@st.cache_data(ttl=3600)
def fetch_market_orders() -> dict[str, Any]:
    return _get("/characters/market_orders?refresh=0", 60, "Market orders")


def refresh_market_orders() -> None:
    _get("/characters/market_orders?refresh=1", 120, "Failed to refresh market orders")


def clear_market_orders_cache() -> None:
    fetch_market_orders.clear()


@st.cache_data(ttl=3600)
def fetch_corp_market_orders(corporation_id: int) -> dict[str, Any]:
    return _get(f"/corporations/{corporation_id}/market_orders?refresh=0", 60, "Corporation market orders")


@st.cache_data(ttl=3600)
def fetch_corp_market_orders_enriched(corporation_id: int) -> list[Any]:
    response = _get(f"/corporations/{corporation_id}/market_orders/enriched?refresh=0", 120,
                    "Corporation market orders")
    data = response.get("data") or []
    return data if isinstance(data, list) else []


def refresh_corp_market_orders(corporation_id: int) -> None:
    _get(f"/corporations/{corporation_id}/market_orders?refresh=1", 120,
         "Failed to refresh corporation market orders")


def clear_corp_market_orders_cache() -> None:
    fetch_corp_market_orders.clear()
    fetch_corp_market_orders_enriched.clear()
