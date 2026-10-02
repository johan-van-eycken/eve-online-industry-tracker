from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DailyPlannerPageState:
    plan: dict | None = None
    status: str = "idle"                                    # mirrors compute job status
    active_tab: int = 0
    force_include: set[int] = field(default_factory=set)   # type_ids; session-only
    force_exclude: set[int] = field(default_factory=set)   # type_ids; session-only
    recompute_confirmed: bool = False                       # for the confirmation dialog


def get_page_state() -> DailyPlannerPageState:
    import streamlit as st

    if "daily_planner" not in st.session_state:
        st.session_state["daily_planner"] = DailyPlannerPageState()
    return st.session_state["daily_planner"]
