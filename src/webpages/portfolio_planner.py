from __future__ import annotations

import streamlit as st

from utils.industry_snapshot_page import prepare_shared_industry_snapshot_page
from utils.portfolio_planner_page import render_portfolio_planner


def render() -> None:
    try:
        page_context = prepare_shared_industry_snapshot_page(
            title="Portfolio Planner",
            intro_caption=(
                "Portfolio Planner uses the same Industry Builder snapshot and backend settings, "
                "but organizes them into planning context, advanced snapshot settings, planner inputs, and candidate analysis."
            ),
            refresh_caption="Refresh the snapshot after changing planning context or advanced snapshot settings.",
            refresh_button_label="Refresh Snapshot",
            refresh_button_key="portfolio_planner_refresh_overview",
            no_rows_message="No manufacturable product rows are available yet.",
            use_collapsed_advanced_snapshot_section=True,
            context_heading="Planning Context",
            advanced_snapshot_heading="Advanced Snapshot Settings",
        )
    except Exception as exc:
        st.error(str(exc))
        return
    if page_context is None:
        return

    render_portfolio_planner(
        default_character_id_value=page_context.default_character_id_value,
        default_owned_blueprint_scope=page_context.default_owned_blueprint_scope,
    )