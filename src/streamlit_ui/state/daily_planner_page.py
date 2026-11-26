from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DailyPlannerPageState:
    plan: dict | None = None
    status: str = "idle"                                    # mirrors compute job status
    recompute_confirmed: bool = False                       # for the confirmation dialog


def get_page_state() -> DailyPlannerPageState:
    import streamlit as st

    if "daily_planner" not in st.session_state:
        st.session_state["daily_planner"] = DailyPlannerPageState()
    return st.session_state["daily_planner"]
