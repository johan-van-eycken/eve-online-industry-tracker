from __future__ import annotations

import streamlit as st

from streamlit_ui.state.daily_planner_page import get_page_state
from streamlit_ui.components.daily_planner.status_bar import render_status_bar
from streamlit_ui.components.daily_planner.tab_actions import render_tab_actions
from streamlit_ui.components.daily_planner.tab_shopping import render_tab_shopping
from streamlit_ui.components.daily_planner.tab_build_plan import render_tab_build_plan
from streamlit_ui.components.daily_planner.tab_analytics import render_tab_analytics


def render(**_kwargs) -> None:
    render_daily_planner(**_kwargs)


def render_daily_planner(**_kwargs) -> None:
    state = get_page_state()
    render_status_bar(state)

    if state.plan is None:
        st.info("No plan yet — click **Recompute Plan** to generate your first daily plan.")
        return

    tab1, tab2, tab3, tab4 = st.tabs(["Today's Actions", "Shopping List", "Build Plan", "Analytics"])
    with tab1:
        render_tab_actions(state)
    with tab2:
        render_tab_shopping(state)
    with tab3:
        render_tab_build_plan(state)
    with tab4:
        render_tab_analytics(state)
