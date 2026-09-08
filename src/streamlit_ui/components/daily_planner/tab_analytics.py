from __future__ import annotations

import streamlit as st

from streamlit_ui.state.daily_planner_page import DailyPlannerPageState


def render_tab_analytics(page_state: DailyPlannerPageState) -> None:
    """Phase A stub — analytics available in Phase C."""
    st.info("Analytics available after manufacturing batches complete and sell (Phase C).")
