from __future__ import annotations

import time
from typing import Any

import streamlit as st

from streamlit_ui.api.client import api_get_or_error
from streamlit_ui.components.batch_errors import batch_errors


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


#: How long a partial reply (some characters failed, `errors` non-empty) is
#: kept: a complete reply is cached for the hour, a partial one is fetched
#: again after this, so a failed character's orders come back soon.
PARTIAL_REPLY_TTL_SECONDS = 60


@st.cache_data(ttl=3600)
def _fetch_market_orders_reply() -> dict[str, Any]:
    """{"response": the API reply, "fetched_at": epoch seconds}."""
    return {"response": _get("/characters/market_orders?refresh=0", 60, "Market orders"),
            "fetched_at": time.time()}


def fetch_market_orders() -> dict[str, Any]:
    """Every character's orders: cached for an hour, a partial reply (with
    `errors`) for PARTIAL_REPLY_TTL_SECONDS only. A failure is not cached."""
    cached = _fetch_market_orders_reply()
    if batch_errors(cached["response"]) and time.time() - cached["fetched_at"] > PARTIAL_REPLY_TTL_SECONDS:
        _fetch_market_orders_reply.clear()
        cached = _fetch_market_orders_reply()
    return cached["response"]


def refresh_market_orders() -> dict[str, str]:
    """Refresh every character's orders; {character name: error} of each
    whose refresh failed (the others were refreshed)."""
    return batch_errors(_get("/characters/market_orders?refresh=1", 120, "Failed to refresh market orders"))


def clear_market_orders_cache() -> None:
    _fetch_market_orders_reply.clear()


@st.cache_data(ttl=3600)
def fetch_corp_market_orders(corporation_id: int) -> dict[str, Any]:
    return _get(f"/corporations/{corporation_id}/market_orders?refresh=0", 60, "Corporation market orders")


@st.cache_data(ttl=3600)
def fetch_corp_market_orders_enriched(corporation_id: int) -> list[Any]:
    response = _get(f"/corporations/{corporation_id}/market_orders/enriched?refresh=0", 120,
                    "Corporation market orders")
    data = response.get("data") or []
    return data if isinstance(data, list) else []


def refresh_corp_market_orders(corporation_id: int) -> dict[str, str]:
    """{corporation name: error} of a partial refresh (none for one corporation:
    its failure raises)."""
    return batch_errors(_get(f"/corporations/{corporation_id}/market_orders?refresh=1", 120,
                             "Failed to refresh corporation market orders"))


def clear_corp_market_orders_cache() -> None:
    fetch_corp_market_orders.clear()
    fetch_corp_market_orders_enriched.clear()
