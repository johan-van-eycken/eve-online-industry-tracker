from __future__ import annotations

import logging
import time
from typing import Any

import requests as _requests  # pyright: ignore[reportMissingModuleSource]

import streamlit as st

from streamlit_ui.api.client import api_get, api_get_or_error, api_post
from flask_app.settings import api_base, api_request_timeout_seconds


def get_plan() -> dict[str, Any]:
    """GET /planner/plan — returns {"plan": {...}, "items": [...], "actions": [...]}."""
    response = api_get("/planner/plan") or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to load daily plan")
    data = response.get("data") or {}
    return data if isinstance(data, dict) else {}


def get_status() -> dict[str, Any]:
    """GET /planner/status — polled on every rerun; no caching."""
    response = api_get("/planner/status") or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to load planner status")
    data = response.get("data") or {}
    return data if isinstance(data, dict) else {}


def compute_plan() -> dict[str, Any]:
    """POST /planner/compute — triggers background plan computation."""
    response = api_post("/planner/compute", {}) or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to trigger plan computation")
    data = response.get("data") or {}
    return data if isinstance(data, dict) else {}


def mark_action_done(action_id: int) -> dict[str, Any]:
    """POST /planner/action/done — mark a daily action as done."""
    response = api_post("/planner/action/done", {"action_id": int(action_id)}) or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or f"Failed to mark action {action_id} done")
    data = response.get("data") or {}
    return data if isinstance(data, dict) else {}


def set_action_status(action_id: int, status: str) -> dict[str, Any]:
    """PATCH /planner/action/{id} — update action status (e.g. 'skipped', 'pending')."""
    timeout = api_request_timeout_seconds()
    url = f"{api_base()}/planner/action/{int(action_id)}"
    try:
        response = _requests.patch(url, json={"status": str(status)}, timeout=timeout)
    except _requests.exceptions.RequestException as exc:
        logging.error("PATCH /planner/action/%d failed: %s", action_id, exc)
        return {}
    if not (200 <= response.status_code < 300):
        logging.error(
            "PATCH /planner/action/%d failed with %s: %s",
            action_id,
            response.status_code,
            response.text,
        )
        return {}
    try:
        body = response.json()
    except ValueError:
        return {}
    if body.get("status") != "success":
        raise RuntimeError(body.get("message") or f"Failed to set action {action_id} status")
    data = body.get("data") or {}
    return data if isinstance(data, dict) else {}


#: How long a failed market intelligence status read is remembered: the
#: status bar renders on every rerun, and an unreachable backend would make
#: each rerun wait for the request timeout again.
MARKET_INTEL_FAILURE_CACHE_SECONDS = 30.0

#: (monotonic time of the failure, reason) of the last failed read, per process.
_market_intel_failure: tuple[float, str] | None = None


def _monotonic() -> float:
    return time.monotonic()


@st.cache_data(ttl=60, show_spinner=False)
def _fetch_market_intel_status() -> dict[str, Any]:
    """The successful read, cached for 60 s (st.cache_data never caches the raise)."""
    body, error = api_get_or_error("/planner/market_intel/status")
    if error is not None or not isinstance(body, dict) or body.get("status") != "success":
        reason = error or (body.get("message") if isinstance(body, dict) else None) or "unexpected response"
        raise RuntimeError(str(reason))
    data = body.get("data")
    return data if isinstance(data, dict) else {}


def get_market_intel_status() -> dict[str, Any]:
    """GET /planner/market_intel/status — market intelligence job status.

    Raises RuntimeError with the API's reason alone (the status bar words the
    message) when it cannot be read. A failure is remembered for
    MARKET_INTEL_FAILURE_CACHE_SECONDS, so the reruns in between raise the
    same reason without paying the request timeout again."""
    global _market_intel_failure
    if _market_intel_failure is not None:
        failed_at, reason = _market_intel_failure
        if _monotonic() - failed_at < MARKET_INTEL_FAILURE_CACHE_SECONDS:
            raise RuntimeError(reason)
        _market_intel_failure = None
    try:
        return _fetch_market_intel_status()
    except RuntimeError as exc:
        _market_intel_failure = (_monotonic(), str(exc))
        raise


def _clear_market_intel_status() -> None:
    global _market_intel_failure
    _market_intel_failure = None
    _fetch_market_intel_status.clear()


get_market_intel_status.clear = _clear_market_intel_status  # type: ignore[attr-defined]


@st.cache_data(ttl=300, show_spinner=False)
def get_analytics() -> dict[str, Any]:
    """GET /planner/analytics — self-learning accuracy stats (rarely changes; cached 5 min)."""
    response = api_get("/planner/analytics") or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to load planner analytics")
    data = response.get("data") or {}
    return data if isinstance(data, dict) else {}
