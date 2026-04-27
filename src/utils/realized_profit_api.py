from __future__ import annotations

from typing import Any

import streamlit as st

from utils.flask_api import api_get


def _query_string(*, owner_scope: str, owner_id: int | None, refresh: bool) -> str:
    parts: list[str] = [f"refresh={1 if refresh else 0}"]
    if owner_id is not None:
        key = "character_id" if str(owner_scope) == "character" else "corporation_id"
        parts.append(f"{key}={int(owner_id)}")
    return "&".join(parts)


def _path(owner_scope: str) -> str:
    return "/characters/realized_profit" if str(owner_scope) == "character" else "/corporations/realized_profit"


@st.cache_data(ttl=300, show_spinner=False)
def fetch_realized_profit(*, owner_scope: str = "character", owner_id: int | None = None) -> dict[str, Any]:
    response = api_get(
        f"{_path(owner_scope)}?{_query_string(owner_scope=owner_scope, owner_id=owner_id, refresh=False)}",
        timeout_seconds=90,
    ) or {}
    return response if isinstance(response, dict) else {}


def refresh_realized_profit(*, owner_scope: str = "character", owner_id: int | None = None) -> dict[str, Any]:
    response = api_get(
        f"{_path(owner_scope)}?{_query_string(owner_scope=owner_scope, owner_id=owner_id, refresh=True)}",
        timeout_seconds=180,
    ) or {}
    if isinstance(response, dict) and response.get("status") not in {None, "success"}:
        raise RuntimeError(response.get("message") or "Failed to refresh realized profit ledger")
    return response if isinstance(response, dict) else {}


def clear_realized_profit_cache() -> None:
    fetch_realized_profit.clear()