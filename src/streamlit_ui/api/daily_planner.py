from __future__ import annotations

import logging
from typing import Any

import requests as _requests  # pyright: ignore[reportMissingModuleSource]

import streamlit as st

from streamlit_ui.api.client import api_get, api_post
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


@st.cache_data(ttl=300, show_spinner=False)
def get_analytics() -> dict[str, Any]:
    """GET /planner/analytics — self-learning accuracy stats (rarely changes; cached 5 min)."""
    response = api_get("/planner/analytics") or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to load planner analytics")
    data = response.get("data") or {}
    return data if isinstance(data, dict) else {}
