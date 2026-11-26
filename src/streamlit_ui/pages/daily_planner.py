from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_ui.api.admin_settings import planner_page_settings
from streamlit_ui.state.daily_planner_page import get_page_state
from streamlit_ui.components.daily_planner.status_bar import load_active_jobs, render_status_bar
from streamlit_ui.components.daily_planner.tab_actions import (
    _character_level_actions,
    render_tab_actions,
    step_summaries,
)
from streamlit_ui.components.daily_planner.tab_shopping import render_tab_shopping
from streamlit_ui.components.daily_planner.tab_build_plan import decision_counts, render_tab_build_plan
from streamlit_ui.components.daily_planner.tab_analytics import render_tab_analytics

VIEW_ACTIONS, VIEW_SHOPPING, VIEW_BUILD, VIEW_ANALYTICS = "actions", "shopping", "build", "analytics"
_VIEW_KEY = "dp_view"


def view_tiles(plan: dict[str, Any], *, jobs: list[dict[str, Any]] | None = None) -> list[tuple[str, str, str, bool]]:
    """(title, grey line, view it opens, done) per tile: one per view."""
    actions = plan.get("actions") or []
    (deliver, buy, start) = step_summaries(_character_level_actions(actions), actions, jobs=jobs)
    counts = decision_counts(plan.get("items") or [])
    build_line = " · ".join(f"{n:,} {d}" for d, n in counts.items()) or "no items"
    return [
        ("Today's actions", f"deliver: {deliver[1]} · start: {start[1]}", VIEW_ACTIONS, deliver[2] and start[2]),
        ("Buy materials", buy[1].replace(" → Shopping List", ""), VIEW_SHOPPING, buy[2]),
        ("Build Plan", build_line, VIEW_BUILD, False),
        ("Analytics", "plan vs. realized, learning", VIEW_ANALYTICS, False),
    ]


def _render_view_tiles(plan: dict[str, Any], active: str, *, jobs: list[dict[str, Any]] | None) -> None:
    # The open view's tiles get the accent border (the old tab underline colour).
    st.markdown(
        f"<style>[class*='st-key-dp_tile_'][class*='_{active}'] {{border-color: #ff4b4b !important;}}</style>",
        unsafe_allow_html=True,
    )
    tiles = view_tiles(plan, jobs=jobs)
    for i, (col, (title, sub, view, done)) in enumerate(zip(st.columns(len(tiles)), tiles)):
        with col.container(border=True, key=f"dp_tile_{i}_{view}"):
            label = f"**{title}**" + (" :green[:material/check_circle:]" if done else "")
            if st.button(label, key=f"dp_tile_btn_{i}", type="tertiary"):
                st.session_state[_VIEW_KEY] = view
                st.rerun()
            st.caption(sub)


def render(**_kwargs) -> None:
    render_daily_planner(**_kwargs)


def render_daily_planner(**_kwargs) -> None:
    state = get_page_state()
    # One /admin_settings read per render, shared by the status bar and the actions tab.
    planner_settings, settings_problems = planner_page_settings()
    render_status_bar(state, planner_settings, settings_problems)

    if state.plan is None:
        st.info("No plan yet — click **Recompute Plan** to generate your first daily plan.")
        return

    # Read once per render: a queued TE starts on its ME job's real end.
    jobs = load_active_jobs() if state.plan.get("actions") else None
    view = st.session_state.get(_VIEW_KEY, VIEW_ACTIONS)
    _render_view_tiles(state.plan, view, jobs=jobs)

    if view == VIEW_SHOPPING:
        render_tab_shopping(state)
    elif view == VIEW_BUILD:
        render_tab_build_plan(state)
    elif view == VIEW_ANALYTICS:
        render_tab_analytics(state)
    else:
        render_tab_actions(state, planner_settings, jobs=jobs)
