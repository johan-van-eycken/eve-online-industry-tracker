from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_ui.api.client import api_get_or_error


def _query_string(*, owner_scope: str, owner_id: int | None, refresh: bool) -> str:
    parts: list[str] = [f"refresh={1 if refresh else 0}"]
    if owner_id is not None:
        key = "character_id" if str(owner_scope) == "character" else "corporation_id"
        parts.append(f"{key}={int(owner_id)}")
    return "&".join(parts)


def _path(owner_scope: str) -> str:
    return "/characters/realized_profit" if str(owner_scope) == "character" else "/corporations/realized_profit"


def _ledger_response(*, owner_scope: str, owner_id: int | None, refresh: bool, timeout_seconds: float,
                     what: str) -> dict[str, Any]:
    """The ledger response; raises RuntimeError with the API's reason on a
    failed request, an HTTP error, an error body or a malformed body."""
    body, failure = api_get_or_error(
        f"{_path(owner_scope)}?{_query_string(owner_scope=owner_scope, owner_id=owner_id, refresh=refresh)}",
        timeout_seconds=timeout_seconds,
    )
    if failure:
        raise RuntimeError(f"{what} failed ({owner_scope}): {failure}")
    if not isinstance(body, dict):
        raise RuntimeError(f"{what} failed ({owner_scope}): the response is not an object")
    if body.get("status") not in {None, "success"}:
        raise RuntimeError(f"{what} failed ({owner_scope}): {body.get('message') or body.get('status')}")
    return body


@st.cache_data(ttl=300, show_spinner=False)
def fetch_realized_profit(*, owner_scope: str = "character", owner_id: int | None = None) -> dict[str, Any]:
    """The cached ledger. A failure raises, and st.cache_data keeps no result
    of a call that raised, so the next run asks again."""
    return _ledger_response(owner_scope=owner_scope, owner_id=owner_id, refresh=False, timeout_seconds=90,
                            what="Loading the realized profit ledger")


def refresh_realized_profit(*, owner_scope: str = "character", owner_id: int | None = None) -> dict[str, Any]:
    return _ledger_response(owner_scope=owner_scope, owner_id=owner_id, refresh=True, timeout_seconds=180,
                            what="Refreshing the realized profit ledger")


def clear_realized_profit_cache() -> None:
    fetch_realized_profit.clear()
