from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from streamlit_ui.state.daily_planner_page import DailyPlannerPageState
from streamlit_ui.components.aggrid_import import import_aggrid
from streamlit_ui.components.daily_planner.status_bar import _fmt_isk

# ---------------------------------------------------------------------------
# Decision colour mapping (for AG-Grid cellStyle)
# ---------------------------------------------------------------------------

_DECISION_COLORS: dict[str, str] = {
    "build": "#27ae60",   # green
    "watch": "#e67e22",   # amber
    "skip":  "#c0392b",   # red
    "pause": "#7f8c8d",   # grey
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _decision_label(decision: str) -> str:
    return {
        "build": "Build",
        "watch": "Watch",
        "skip":  "Skip",
        "pause": "Pause",
    }.get(decision, decision.title())


def _confidence_label(tier: str | None) -> str:
    if tier == "high":
        return "High (20+)"
    if tier == "medium":
        return "Medium (5–19)"
    if tier == "low":
        return "Low (<5)"
    return tier or "—"


def _fmt_pct(v: Any) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):.1f}%"
    except (TypeError, ValueError):
        return "—"


def _fmt_num(v: Any, decimals: int = 1) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):,.{decimals}f}"
    except (TypeError, ValueError):
        return "—"


# ---------------------------------------------------------------------------
# Score breakdown drill-down
# ---------------------------------------------------------------------------

def _render_score_breakdown(item: dict[str, Any]) -> None:
    """Render the scoring component breakdown for a single build plan item."""
    st.markdown("**Score Breakdown**")

    isk_per_hour = item.get("isk_per_hour")
    margin_pct = item.get("margin_pct")
    days_of_supply = item.get("days_of_supply_current")
    momentum = item.get("momentum_signal")
    competition_idx = item.get("competition_index")

    cols = st.columns(2)
    with cols[0]:
        st.caption(f"ISK/hour: {_fmt_isk(isk_per_hour)}")
        st.caption(f"Margin: {_fmt_pct(margin_pct)}")
        st.caption(f"Days of supply: {_fmt_num(days_of_supply)} days")
        st.caption(f"Momentum signal: {_fmt_num(momentum)}")
    with cols[1]:
        st.caption(f"Competition index: {_fmt_num(competition_idx) if competition_idx is not None else '— (Phase A)'}")
        st.caption(f"Priority score: {_fmt_num(item.get('priority_score'))}")
        st.caption(f"Confidence tier: {_confidence_label(item.get('confidence_tier'))}")
        st.caption(f"Sample count: {item.get('sample_count') or 0}")

    decision_reason = item.get("decision_reason")
    if decision_reason:
        st.caption(f"Reason: {decision_reason}")


# ---------------------------------------------------------------------------
# All-watch fallback
# ---------------------------------------------------------------------------

def _render_all_watch_fallback(items: list[dict[str, Any]]) -> None:
    """Show top 5 watch items when nothing is on build."""
    watch_items = [i for i in items if str(i.get("decision") or "") == "watch"]
    if not watch_items:
        st.info("No items evaluated for this plan yet.")
        return

    watch_items.sort(key=lambda i: float(i.get("priority_score") or 0.0), reverse=True)
    top5 = watch_items[:5]

    st.info("No items on build today. Top 5 watch items:")
    for item in top5:
        name = str(item.get("type_name") or item.get("type_id") or "Unknown")
        score = float(item.get("priority_score") or 0.0)
        reason = str(item.get("decision_reason") or "")
        label = f"**{name}** — score {score:.1f}"
        if reason:
            label += f" ({reason})"
        st.markdown(f"- {label}")


# ---------------------------------------------------------------------------
# Main tab renderer
# ---------------------------------------------------------------------------

def render_tab_build_plan(page_state: DailyPlannerPageState) -> None:
    if page_state.plan is None:
        st.info("No plan yet — click **Recompute Plan** to generate your first daily plan.")
        return

    items: list[dict[str, Any]] = page_state.plan.get("items") or []

    if not items:
        st.info("No items in this plan.")
        return

    # Check all-watch / all-skip fallback
    has_build = any(str(i.get("decision") or "") == "build" for i in items)
    if not has_build:
        _render_all_watch_fallback(items)

    # ------------------------------------------------------------------
    # Decision filter
    # ------------------------------------------------------------------
    all_decisions = sorted({str(i.get("decision") or "unknown") for i in items})
    selected_decisions = st.multiselect(
        "Filter by decision",
        options=all_decisions,
        default=all_decisions,
        key="build_plan_decision_filter",
    )

    # ------------------------------------------------------------------
    # Force include / exclude (session-only)
    # ------------------------------------------------------------------
    all_type_names = {
        int(i.get("type_id") or 0): str(i.get("type_name") or str(i.get("type_id") or "Unknown"))
        for i in items
        if i.get("type_id")
    }

    with st.expander("Force Include / Exclude (session-only, resets on recompute)", expanded=False):
        st.caption("Session-only overrides. These do not change the persisted plan — they reset when you recompute.")
        fi_col, fe_col = st.columns(2)
        with fi_col:
            st.markdown("**Force Include** (add to build regardless of score)")
            include_names = st.multiselect(
                "Force include",
                options=list(all_type_names.values()),
                default=[
                    all_type_names[tid]
                    for tid in page_state.force_include
                    if tid in all_type_names
                ],
                key="force_include_select",
                label_visibility="collapsed",
            )
            page_state.force_include = {
                tid for tid, name in all_type_names.items() if name in include_names
            }
        with fe_col:
            st.markdown("**Force Exclude** (remove from build)")
            exclude_names = st.multiselect(
                "Force exclude",
                options=list(all_type_names.values()),
                default=[
                    all_type_names[tid]
                    for tid in page_state.force_exclude
                    if tid in all_type_names
                ],
                key="force_exclude_select",
                label_visibility="collapsed",
            )
            page_state.force_exclude = {
                tid for tid, name in all_type_names.items() if name in exclude_names
            }

    # ------------------------------------------------------------------
    # Build display dataframe
    # ------------------------------------------------------------------
    filtered_items = [
        i for i in items
        if str(i.get("decision") or "unknown") in selected_decisions
    ]

    display_rows = []
    for item in filtered_items:
        type_id = int(item.get("type_id") or 0)
        decision = str(item.get("decision") or "")
        name = str(item.get("type_name") or str(type_id) or "Unknown")

        # Squeeze sensitive flag
        is_squeeze = bool(item.get("is_squeeze_sensitive"))
        if is_squeeze:
            name = name + " ⚠"

        # Force overrides
        if type_id in page_state.force_include:
            decision_display = f"{_decision_label(decision)} ★"
        elif type_id in page_state.force_exclude:
            decision_display = f"{_decision_label(decision)} ✗"
        else:
            decision_display = _decision_label(decision)

        # Competition index — Phase A: show "—" since MIJ not running
        comp_index = item.get("competition_index")  # None in Phase A
        competitor_supply_days = (
            round(float(comp_index) * 30, 1) if comp_index is not None else None
        )

        display_rows.append({
            "Item": name,
            "Decision": decision_display,
            "_decision_raw": decision,
            "Priority Score": round(float(item.get("priority_score") or 0.0), 1),
            "ISK/hour": item.get("isk_per_hour"),
            "Margin %": item.get("margin_pct"),
            "Days of Supply": item.get("days_of_supply_current"),
            "Comp. Index": _fmt_num(comp_index) if comp_index is not None else "—",
            "Comp. Supply Days": _fmt_num(competitor_supply_days) if competitor_supply_days is not None else "—",
            "Momentum": _fmt_num(item.get("momentum_signal")),
            "Confidence": _confidence_label(item.get("confidence_tier")),
            "Samples": int(item.get("sample_count") or 0),
            "Reason": str(item.get("decision_reason") or ""),
        })

    df = pd.DataFrame(display_rows)

    # ------------------------------------------------------------------
    # Render grid
    # ------------------------------------------------------------------
    ag = import_aggrid()
    AgGrid = ag.AgGrid if ag else None
    GridOptionsBuilder = ag.GridOptionsBuilder if ag else None
    JsCode = ag.JsCode if ag else None

    if AgGrid is not None and GridOptionsBuilder is not None and JsCode is not None:
        try:
            decision_cell_style = JsCode(
                """
                function(params) {
                    var colors = {
                        'Build': '#27ae60',
                        'Watch': '#e67e22',
                        'Skip':  '#c0392b',
                        'Pause': '#7f8c8d'
                    };
                    var dec = params.value ? params.value.replace(/[★✗]/g, '').trim() : '';
                    var color = colors[dec] || null;
                    if (!color) return {};
                    return {color: color, fontWeight: '600'};
                }
                """
            )
            gb = GridOptionsBuilder.from_dataframe(df.drop(columns=["_decision_raw"]))
            gb.configure_default_column(resizable=True, sortable=True, filter=True)
            gb.configure_column("Item", minWidth=200)
            gb.configure_column("Decision", cellStyle=decision_cell_style, maxWidth=130)
            gb.configure_column("Priority Score", type=["numericColumn"], maxWidth=120)
            gb.configure_column("ISK/hour", type=["numericColumn"], maxWidth=120, valueFormatter="value ? (value / 1e6).toFixed(1) + 'M' : '—'")
            gb.configure_column("Margin %", maxWidth=100)
            gb.configure_column("Days of Supply", maxWidth=120)
            gb.configure_column("Comp. Index", maxWidth=110)
            gb.configure_column("Comp. Supply Days", maxWidth=130)
            gb.configure_column("Momentum", maxWidth=100)
            gb.configure_column("Samples", maxWidth=90)
            gb.configure_selection("single", use_checkbox=False)
            go = gb.build()
            AgGrid(
                df.drop(columns=["_decision_raw"]),
                gridOptions=go,
                height=min(38 * len(display_rows) + 60, 500),
                fit_columns_on_grid_load=False,
                allow_unsafe_jscode=True,
                key="build_plan_grid",
            )
        except Exception:
            st.dataframe(
                df.drop(columns=["_decision_raw"]),
                hide_index=True,
                width="stretch",
            )
    else:
        st.dataframe(
            df.drop(columns=["_decision_raw"]),
            hide_index=True,
            width="stretch",
        )

    st.caption(
        "Phase A: Competition Index and Competitor Supply Days show '—' until Market Intelligence Job runs. "
        "⚠ badge = margin historically sensitive to mineral price rises."
    )

    # ------------------------------------------------------------------
    # Score breakdown drill-down (per item expanders)
    # ------------------------------------------------------------------
    if filtered_items:
        st.markdown("---")
        st.markdown("**Score Breakdown** — select an item to expand:")
        selected_name = st.selectbox(
            "Item",
            options=["(select)"] + [str(i.get("type_name") or i.get("type_id") or "Unknown") for i in filtered_items],
            key="build_plan_breakdown_select",
            label_visibility="collapsed",
        )
        if selected_name and selected_name != "(select)":
            for item in filtered_items:
                if str(item.get("type_name") or item.get("type_id") or "Unknown") == selected_name:
                    _render_score_breakdown(item)
                    break
