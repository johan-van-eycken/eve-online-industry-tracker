from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_ui.api.daily_planner import get_analytics
from streamlit_ui.state.daily_planner_page import DailyPlannerPageState


def analytics_unavailable_caption(analytics: dict | None) -> str | None:
    """Caption naming the analytics reads that failed (RM-35), or None.

    Without it a DB error reads as the sections' "no data yet" messages."""
    unavailable = (analytics or {}).get("unavailable") or {}
    if not isinstance(unavailable, dict) or not unavailable:
        return None
    return "Some analytics could not be read: " + "; ".join(str(r) for r in unavailable.values())


def section_empty_message(analytics: dict | None, key: str, default: str) -> tuple[str, bool]:
    """(message, read failed) for an Analytics section with no rows.

    A failed read gives its reason instead of the "no data yet" default, so
    the section does not name a false cause for a DB error (RM-35)."""
    unavailable = (analytics or {}).get("unavailable") or {}
    reason = unavailable.get(key) if isinstance(unavailable, dict) else None
    return (str(reason), True) if reason else (default, False)


def market_depth_stale_caption(items: list[dict] | None) -> str | None:
    """Caption naming the items whose market depth row is stale (the collector
    kept the previous snapshot) and why, or None when every row is fresh."""
    stale = [i for i in (items or []) if i.get("market_stale_reason")]
    if not stale:
        return None
    return "Stale market depth (previous snapshot kept): " + "; ".join(
        f"{i.get('type_name') or i.get('type_id')}: {i.get('market_stale_reason')}"
        + (f" (snapshot {i.get('market_snapshot_at')})" if i.get("market_snapshot_at") else "")
        for i in stale
    )


def competition_rows(items: list[dict] | None) -> list[dict]:
    """The Competition Monitor rows: every item with a market depth row. An
    unknown competition index (no velocity) shows as unknown, not 0; a stale
    row names its reason next to its snapshot time."""
    rows = []
    for i in items or []:
        if i.get("competitor_units") is None:
            continue   # no market depth row
        raw_ci = i.get("competition_index")
        if raw_ci is None:
            ci_display, supply_display = "unknown (no sales velocity)", "unknown"
        else:
            ci = float(raw_ci)
            badge = "🔴" if ci > 4.0 else ("🟡" if ci >= 2.0 else "🟢")
            ci_display, supply_display = f"{badge} {ci:.2f}", f"{ci * 30:.0f}d"
        updated = str(i.get("market_snapshot_at") or "—")
        if i.get("market_stale_reason"):
            updated = f"{updated} (stale: {i.get('market_stale_reason')})"
        rows.append({
            "Item": str(i.get("type_name") or i.get("type_id") or ""),
            "Decision": str(i.get("decision") or ""),
            "Competition Index": ci_display,
            "Supply Days (est.)": supply_display,
            "Competitor Units": int(i.get("competitor_units") or 0),
            "Market Last Updated": updated,
        })
    return rows


def _render_empty_section(analytics: dict, key: str, default: str) -> None:
    message, failed = section_empty_message(analytics, key, default)
    if failed:
        st.warning(message)
    else:
        st.info(message)


def render_tab_analytics(page_state: DailyPlannerPageState) -> None:
    """Render the Analytics tab — self-learning stats, competition, correlation, invention."""
    try:
        analytics = get_analytics() or {}
    except Exception as exc:
        st.error(f"Failed to load analytics: {exc}")
        return

    items: list[dict] = analytics.get("items") or []
    plan_history: list[dict] = analytics.get("plan_history") or []

    unavailable_caption = analytics_unavailable_caption(analytics)
    if unavailable_caption:
        st.caption(unavailable_caption)

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
        rows = competition_rows(items)
        if not rows:
            _render_empty_section(
                analytics, "market_depth",
                "No market depth data yet — run MarketIntelligenceJob first "
                "(starts automatically every 3h).",
            )
        else:
            stale_caption = market_depth_stale_caption(items)
            if stale_caption:
                st.caption(stale_caption)
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    # ------------------------------------------------------------------
    # Section 3: Margin–Mineral Correlation
    # ------------------------------------------------------------------
    with st.expander("Margin–Mineral Correlation", expanded=True):
        items_with_corr = [
            i for i in items if int(i.get("correlation_data_points") or 0) > 0
        ]
        if not items_with_corr:
            _render_empty_section(
                analytics, "margin_correlation",
                "Correlation data appears after 30+ days of market history and "
                "realized profit data.",
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
            _render_empty_section(
                analytics, "invention_details",
                "Invention data appears after T2 invention jobs complete.",
            )
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
                    "Freshness": (
                        f"{float(p['freshness_score']) * 100:.0f}%"
                        if p.get("freshness_score") is not None else "unknown"
                    ),
                }
                for p in plan_history
            ]
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
