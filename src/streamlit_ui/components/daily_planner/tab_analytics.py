from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_ui.api.daily_planner import get_analytics
from streamlit_ui.state.daily_planner_page import DailyPlannerPageState


def render_tab_analytics(page_state: DailyPlannerPageState) -> None:
    """Render the Analytics tab — self-learning stats, competition, correlation, invention."""
    try:
        analytics = get_analytics() or {}
    except Exception as exc:
        st.error(f"Failed to load analytics: {exc}")
        return

    items: list[dict] = analytics.get("items") or []
    plan_history: list[dict] = analytics.get("plan_history") or []

    # ------------------------------------------------------------------
    # Section 1: Self-Learning Accuracy
    # ------------------------------------------------------------------
    with st.expander("Self-Learning Accuracy", expanded=True):
        all_zero_samples = all(int(i.get("sample_count") or 0) == 0 for i in items)
        if not items or all_zero_samples:
            st.info(
                "No completed batches yet — accuracy data appears after manufacturing jobs "
                "complete and sell."
            )
        else:
            rows = []
            for i in items:
                ema = float(i.get("accuracy_ema") or 1.0)
                if ema >= 1.05:
                    ema_display = f"{ema:.3f} ▲"
                elif ema < 0.95:
                    ema_display = f"{ema:.3f} ▼"
                else:
                    ema_display = f"{ema:.3f}"
                rows.append({
                    "Item": str(i.get("type_name") or i.get("type_id") or ""),
                    "Samples": int(i.get("sample_count") or 0),
                    "Confidence": str(i.get("confidence_tier") or "low"),
                    "Accuracy EMA": ema_display,
                    "Velocity ×": f"{float(i.get('velocity_multiplier') or 1.0):.3f}",
                    "Cost ×": f"{float(i.get('cost_multiplier') or 1.0):.3f}",
                    "Decision": str(i.get("decision") or ""),
                })
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    # ------------------------------------------------------------------
    # Section 2: Competition Monitor
    # ------------------------------------------------------------------
    with st.expander("Competition Monitor", expanded=True):
        items_with_comp = [i for i in items if i.get("competition_index") is not None]
        if not items_with_comp:
            st.info(
                "No market depth data yet — run MarketIntelligenceJob first "
                "(starts automatically every 3h)."
            )
        else:
            rows = []
            for i in items_with_comp:
                ci = float(i.get("competition_index") or 0.0)
                if ci > 4.0:
                    ci_badge = "🔴"
                elif ci >= 2.0:
                    ci_badge = "🟡"
                else:
                    ci_badge = "🟢"
                supply_days = ci * 30
                rows.append({
                    "Item": str(i.get("type_name") or i.get("type_id") or ""),
                    "Decision": str(i.get("decision") or ""),
                    "Competition Index": f"{ci_badge} {ci:.2f}",
                    "Supply Days (est.)": f"{supply_days:.0f}d",
                    "Competitor Units": int(i.get("competitor_units") or 0),
                    "Market Last Updated": str(i.get("market_snapshot_at") or "—"),
                })
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    # ------------------------------------------------------------------
    # Section 3: Margin–Mineral Correlation
    # ------------------------------------------------------------------
    with st.expander("Margin–Mineral Correlation", expanded=True):
        items_with_corr = [
            i for i in items if int(i.get("correlation_data_points") or 0) > 0
        ]
        if not items_with_corr:
            st.info(
                "Correlation data appears after 30+ days of market history and "
                "realized profit data."
            )
        else:
            rows = []
            for i in items_with_corr:
                squeeze = bool(i.get("is_squeeze_sensitive"))
                squeeze_display = "⚠ Yes" if squeeze else "No"
                pearson = i.get("pearson_correlation")
                pearson_display = f"{float(pearson):.3f}" if pearson is not None else "—"
                rows.append({
                    "Item": str(i.get("type_name") or i.get("type_id") or ""),
                    "Pearson r": pearson_display,
                    "Squeeze Sensitive": squeeze_display,
                    "Data Points": int(i.get("correlation_data_points") or 0),
                })
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    # ------------------------------------------------------------------
    # Section 4: Invention Success Rates
    # ------------------------------------------------------------------
    with st.expander("Invention Success Rates", expanded=True):
        items_with_inv = [i for i in items if i.get("invention_details") is not None]
        if not items_with_inv:
            st.info("Invention data appears after T2 invention jobs complete.")
        else:
            rows = []
            for i in items_with_inv:
                det = i["invention_details"]
                actual = float(det.get("actual_rate") or 0.0)
                theoretical = float(det.get("theoretical_rate") or 0.0)
                attempts = int(det.get("attempts") or 0)
                if actual < theoretical - 0.10:
                    status = "⚠ Below expected"
                else:
                    status = "OK"
                rows.append({
                    "Item": str(i.get("type_name") or i.get("type_id") or ""),
                    "Theoretical %": f"{theoretical * 100:.1f}%",
                    "Actual %": f"{actual * 100:.1f}%",
                    "Attempts": attempts,
                    "Status": status,
                })
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    # ------------------------------------------------------------------
    # Section 5: Plan History
    # ------------------------------------------------------------------
    with st.expander("Plan History", expanded=True):
        if not plan_history:
            st.info("No plan history yet.")
        else:
            rows = [
                {
                    "Created At": str(p.get("created_at") or ""),
                    "Status": str(p.get("status") or ""),
                    "Freshness": f"{float(p.get('freshness_score') or 1.0) * 100:.0f}%",
                }
                for p in plan_history
            ]
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
