from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
import os
import time
from typing import Any

import requests
import streamlit as st

from streamlit_ui.api.client import api_get, api_post
from streamlit_ui.components.webpage_ui import render_job_status_panel


@dataclass(frozen=True)
class PageSpec:
    label: str
    module_path: str


NAVIGATION_PAGES = (
    PageSpec("Characters", "streamlit_ui.pages.characters"),
    PageSpec("Corporations", "streamlit_ui.pages.corporations"),
    PageSpec("Industry Builder", "streamlit_ui.pages.industry_builder"),
    PageSpec("Industry Slots", "streamlit_ui.pages.industry_jobs"),
    PageSpec("Portfolio Planner", "streamlit_ui.pages.portfolio_planner"),
    PageSpec("Realized Profit", "streamlit_ui.pages.realized_profit"),
    PageSpec("Market Orders", "streamlit_ui.pages.marketorders"),
    PageSpec("Ore Calculator", "streamlit_ui.pages.ore_calculator"),
    PageSpec("Settings", "streamlit_ui.pages.industry_profiles"),
)

ADMIN_PAGE_MODULES = {
    "Application Settings": "streamlit_ui.pages.admin_settings",
    "Database Maintenance": "streamlit_ui.pages.database_maintenance",
    "ESI Monitoring": "streamlit_ui.pages.esi_monitoring",
}

ADMIN_ACTIONS = ["", "Application Settings", "Database Maintenance", "Restart Flask App", "Restart Streamlit", "Public Structures Scan", "ESI Monitoring"]


def _render_module_page(module_path: str) -> None:
    module = import_module(module_path)
    render_fn = getattr(module, "render", None)
    if not callable(render_fn):
        raise RuntimeError(f"Module {module_path} does not expose a callable render()")
    render_fn()


def _fetch_public_structures_scan_status() -> dict[str, Any]:
    response = api_get("/public_structures_status") or {}
    if response.get("status") not in {None, "success"}:
        raise RuntimeError(response.get("message") or "Failed to load public structures status")
    data = response.get("data") or {}
    global_scan = data.get("global_scan") or {}
    return global_scan if isinstance(global_scan, dict) else {}


def _render_public_structures_scan_status() -> None:
    try:
        global_scan = _fetch_public_structures_scan_status()
    except Exception as exc:
        st.error(f"Failed to load public structures status: {exc}")
        return

    total_ids = global_scan.get("total_ids")
    cursor = global_scan.get("cursor")
    progress_fraction: float | None = None
    progress_label: str | None = None
    if isinstance(total_ids, int) and isinstance(cursor, int) and total_ids > 0:
        progress_fraction = max(0.0, min(1.0, float(cursor) / float(total_ids)))
        progress_label = f"Progress: {cursor}/{total_ids} ({progress_fraction * 100:.1f}%)"

    render_job_status_panel(
        title="Global public structures scan",
        is_running=bool(global_scan.get("running")),
        progress_fraction=progress_fraction,
        progress_text=progress_label,
        metrics={
            "Running": "yes" if global_scan.get("running") else "no",
            "Attempted": str(global_scan.get("attempted")) if global_scan.get("attempted") is not None else "-",
            "Rows written": str(global_scan.get("rows_written")) if global_scan.get("rows_written") is not None else "-",
        },
        error_message=str(global_scan.get("error") or "") or None,
        success_message=("Scan finished." if (not global_scan.get("running")) and global_scan.get("finished_at") and not global_scan.get("error") else None),
        details=global_scan,
    )


def _render_restart_flask_page() -> None:
    st.subheader("Restart Flask App")
    st.caption("This restarts the backend API process. The frontend will reconnect when Flask is available again.")

    if st.button("Confirm Restart", type="primary"):
        try:
            api_get("/shutdown")
            st.success("Flask app is restarting. Please wait a few seconds.")
            st.info("The page will automatically reconnect when Flask is back online.")
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
            st.success("Flask app is restarting. Please wait a few seconds.")
            st.info("The page will automatically reconnect when Flask is back online.")
        except Exception as exc:
            st.error(f"Error restarting Flask app: {exc}")


def _render_restart_streamlit_page() -> None:
    st.subheader("Restart Streamlit")
    st.caption(
        "This stops the current Streamlit process. If you started the app via the supervisor "
        "(`python -m eve_online_industry_tracker`), it should automatically restart Streamlit."
    )
    st.warning("Any in-memory UI state will be lost.")

    if st.button("Confirm Restart Streamlit", type="primary"):
        st.success("Restarting Streamlit...")
        time.sleep(0.5)
        os._exit(0)


def _render_public_structures_scan_page() -> None:
    st.subheader("Public Structures Scan")

    controls = st.columns([1, 1, 2, 2])
    with controls[0]:
        start_scan = st.button("Start scan", type="primary")
    with controls[1]:
        st.button("Refresh now")
    with controls[2]:
        auto_refresh = st.checkbox(
            "Auto-refresh",
            value=True,
            help="Uses Streamlit fragments when supported to avoid a full-page rerun.",
        )
    with controls[3]:
        refresh_interval_s = st.slider("Refresh interval (s)", min_value=1, max_value=10, value=2, step=1)

    if start_scan:
        try:
            response = api_post("/public_structures_scan/start", {}) or {}
            started = bool((response.get("meta") or {}).get("started"))
            if started:
                st.success("Global public structures scan started in the background.")
            else:
                st.info("Global public structures scan is already running.")
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException) as exc:
            st.error(f"Failed to contact Flask API: {exc}")
        except Exception as exc:
            st.error(f"Error starting public structures scan: {exc}")

    fragment = getattr(st, "fragment", None)
    if auto_refresh and callable(fragment):
        @st.fragment(run_every=f"{int(refresh_interval_s)}s")
        def _status_fragment() -> None:
            _render_public_structures_scan_status()

        _status_fragment()
        st.caption("Live updates: fragment refresh (no full-page rerun).")
        return

    if auto_refresh and not callable(fragment):
        st.info("Auto-refresh without full reruns requires Streamlit fragments. Upgrade Streamlit or use 'Refresh now'.")

    _render_public_structures_scan_status()


def _render_admin_page(choice_admin: str) -> None:
    if choice_admin == "Restart Flask App":
        _render_restart_flask_page()
        return
    if choice_admin == "Restart Streamlit":
        _render_restart_streamlit_page()
        return
    if choice_admin == "Public Structures Scan":
        _render_public_structures_scan_page()
        return

    module_path = ADMIN_PAGE_MODULES.get(choice_admin)
    if module_path:
        _render_module_page(module_path)


def render_application() -> None:
    st.set_page_config(page_title="EVE Online Industry Tracker", layout="wide")
    st.title("EVE Online Industry Tracker")

    navigation_labels = [page.label for page in NAVIGATION_PAGES]
    selected_navigation = st.sidebar.selectbox("Navigation", navigation_labels, key="app_shell_navigation")
    selected_admin = st.sidebar.selectbox("Admin", ADMIN_ACTIONS, key="app_shell_admin")

    if selected_admin:
        _render_admin_page(selected_admin)
        return

    page_spec = next((page for page in NAVIGATION_PAGES if page.label == selected_navigation), None)
    if page_spec is not None:
        _render_module_page(page_spec.module_path)