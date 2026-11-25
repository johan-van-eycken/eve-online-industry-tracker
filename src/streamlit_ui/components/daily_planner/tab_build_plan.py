from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import streamlit as st
from streamlit.errors import StreamlitAPIException

from streamlit_ui.state.daily_planner_page import DailyPlannerPageState
from streamlit_ui.components.aggrid_import import import_aggrid
from streamlit_ui.components.daily_planner.status_bar import _fmt_isk
from streamlit_ui.components.stat_tiles import render_stat_tiles

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
        return f"{float(v):.2f}%"
    except (TypeError, ValueError):
        return "—"


def _round2(v: Any) -> float | None:
    """A numeric table cell at 2 decimals; stays a number so the grid sorts it."""
    try:
        return None if v is None else round(float(v), 2)
    except (TypeError, ValueError):
        return None


def _fmt_num(v: Any, decimals: int = 2) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):,.{decimals}f}"
    except (TypeError, ValueError):
        return "—"


# ---------------------------------------------------------------------------
# Score breakdown drill-down
# ---------------------------------------------------------------------------

def bpo_analysis_caption(item: dict[str, Any]) -> str | None:
    """One line on the BPO investment analysis for the score breakdown, or None.

    A completed analysis shows its result, and a skipped one shows why, so
    blank BPO fields are never ambiguous between "skipped" and "not attempted".
    """
    reason = item.get("bpo_analysis_skip_reason")
    if reason:
        return f"BPO analysis skipped: {reason}"
    if item.get("bpo_market_price") is None:
        return None
    break_even = item.get("break_even_days")
    break_even_text = f"{float(break_even):.0f}d break-even" if break_even is not None else "no break-even"
    return (
        f"BPO {_fmt_isk(item.get('bpo_market_price'))}: {break_even_text}, "
        f"saves {_fmt_isk(item.get('projected_annual_savings'))}/yr"
    )


def _render_score_breakdown(item: dict[str, Any]) -> None:
    """The selected item's "why the planner chose this" panel."""
    with st.container(border=True):
        name = str(item.get("type_name") or item.get("type_id") or "Unknown")
        st.markdown(f"#### {name} · {_decision_label(str(item.get('decision') or ''))}")
        decision_reason = item.get("decision_reason")
        if decision_reason:
            st.caption(str(decision_reason))
        cols = st.columns(4)
        cols[0].metric("Margin", _fmt_pct(item.get("margin_pct")))
        cols[1].metric("Days of supply", _fmt_num(item.get("days_of_supply_current")))
        cols[2].metric("ISK/hour", _fmt_isk(item.get("isk_per_hour")))
        cols[3].metric("Momentum", _fmt_num(item.get("momentum_signal")))
        competition_idx = item.get("competition_index")
        st.caption(
            f"Priority score {_fmt_num(item.get('priority_score'))} · "
            f"competition index {_fmt_num(competition_idx) if competition_idx is not None else '— (Phase A)'} · "
            f"confidence {_confidence_label(item.get('confidence_tier'))} · "
            f"{item.get('sample_count') or 0} sample(s)"
        )
        bpo_caption = bpo_analysis_caption(item)
        if bpo_caption:
            st.caption(bpo_caption)


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
        label = f"**{name}** — score {score:.2f}"
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
    # Decision tiles and filters
    # ------------------------------------------------------------------
    counts = decision_counts(items)
    render_stat_tiles([(_decision_label(d), f"{n:,}", None) for d, n in counts.items()])

    all_decisions = list(counts)
    pill_col, search_col = st.columns([3, 2], vertical_alignment="bottom")
    with pill_col:
        selected_decisions = st.pills(
            "Decision", options=all_decisions, default=all_decisions, selection_mode="multi",
            format_func=_decision_label, key="build_plan_decision_filter",
        ) or []
    with search_col:
        search = st.text_input("Search", placeholder="item name or reason", key="build_plan_search")

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
        if str(i.get("decision") or "unknown") in selected_decisions and matches_search(i, search)
    ]

    display_rows = []
    for item in filtered_items:
        type_id = int(item.get("type_id") or 0)
        decision = str(item.get("decision") or "")
        name = _display_name(item)

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
            round(float(comp_index) * 30, 2) if comp_index is not None else None
        )

        display_rows.append({
            "Item": name,
            "Decision": decision_display,
            "_decision_raw": decision,
            "Priority Score": round(float(item.get("priority_score") or 0.0), 2),
            "ISK/hour": item.get("isk_per_hour"),
            "Margin %": _round2(item.get("margin_pct")),
            "Days of Supply": _round2(item.get("days_of_supply_current")),
            "Comp. Index": _fmt_num(comp_index) if comp_index is not None else "—",
            "Comp. Supply Days": _fmt_num(competitor_supply_days) if competitor_supply_days is not None else "—",
            "Momentum": _fmt_num(item.get("momentum_signal")),
            "Confidence": _confidence_label(item.get("confidence_tier")),
            "Samples": int(item.get("sample_count") or 0),
            "Reason": str(item.get("decision_reason") or ""),
        })

    df = pd.DataFrame(display_rows)
    if df.empty:
        st.info("No items match this filter.")
        return
    # Phase A: the competition columns stay "—" until the Market Intelligence Job runs.
    for column in ("Comp. Index", "Comp. Supply Days"):
        if (df[column] == "—").all():
            df = df.drop(columns=[column])

    # ------------------------------------------------------------------
    # Render grid (a click on a row opens its breakdown below)
    # ------------------------------------------------------------------
    selected_name: str | None = None
    ag = import_aggrid()
    AgGrid = ag.AgGrid if ag else None
    GridOptionsBuilder = ag.GridOptionsBuilder if ag else None
    JsCode = ag.JsCode if ag else None
    grid_df = df.drop(columns=["_decision_raw"])

    if AgGrid is not None and GridOptionsBuilder is not None and JsCode is not None:
        try:
            decision_cell_style = JsCode(
                """
                function(params) {
                    var colors = {
                        'Build': '#34d399',
                        'Watch': '#fbbf24',
                        'Skip':  '#f87171',
                        'Pause': '#a1a1aa'
                    };
                    var dec = params.value ? params.value.replace(/[★✗]/g, '').trim() : '';
                    var color = colors[dec] || null;
                    if (!color) return {};
                    return {color: color, fontWeight: '600'};
                }
                """
            )
            two_decimals = "value == null ? '—' : value.toFixed(2)"
            gb = GridOptionsBuilder.from_dataframe(grid_df)
            gb.configure_default_column(resizable=True, sortable=True, filter=True)
            gb.configure_column("Item", minWidth=220, pinned="left")
            gb.configure_column("Decision", cellStyle=decision_cell_style, maxWidth=120)
            gb.configure_column("Priority Score", type=["numericColumn"], maxWidth=120, valueFormatter=two_decimals)
            gb.configure_column("ISK/hour", type=["numericColumn"], maxWidth=120,
                                valueFormatter="value == null ? '—' : (value / 1e6).toFixed(2) + 'M'")
            gb.configure_column("Margin %", type=["numericColumn"], maxWidth=110,
                                valueFormatter="value == null ? '—' : value.toFixed(2) + '%'")
            gb.configure_column("Days of Supply", type=["numericColumn"], maxWidth=130, valueFormatter=two_decimals)
            gb.configure_column("Momentum", type=["numericColumn"], maxWidth=110)
            gb.configure_column("Samples", type=["numericColumn"], maxWidth=90)
            gb.configure_column("Reason", minWidth=320)
            gb.configure_selection("single", use_checkbox=False)
            response = AgGrid(
                grid_df,
                gridOptions=gb.build(),
                height=min(38 * len(display_rows) + 60, 560),
                fit_columns_on_grid_load=False,
                allow_unsafe_jscode=True,
                theme="streamlit",
                key="build_plan_grid",
            )
            selected_name = _selected_item_name(getattr(response, "selected_rows", None)
                                                if not isinstance(response, dict) else response.get("selected_rows"))
        except (TypeError, ValueError, KeyError, AttributeError, StreamlitAPIException) as exc:
            # Cosmetic: the plain table shows the same rows.
            logging.warning("Build plan grid failed, showing a plain table: %s", exc)
            selected_name = _plain_table(grid_df)
    else:
        selected_name = _plain_table(grid_df)

    st.caption("⚠ after a name = margin historically sensitive to mineral price rises. "
               "Click a row for why the planner chose it.")

    if selected_name:
        for item in filtered_items:
            if _display_name(item) == selected_name:
                _render_score_breakdown(item)
                break


def _display_name(item: dict[str, Any]) -> str:
    name = str(item.get("type_name") or str(int(item.get("type_id") or 0)) or "Unknown")
    return name + " ⚠" if item.get("is_squeeze_sensitive") else name


def _selected_item_name(selected: Any) -> str | None:
    """The Item of AgGrid's selected row; it comes back as a DataFrame or a
    list of dicts depending on the st_aggrid version, or None."""
    if selected is None:
        return None
    if isinstance(selected, pd.DataFrame):
        return None if selected.empty else str(selected.iloc[0]["Item"])
    if isinstance(selected, list) and selected and isinstance(selected[0], dict):
        return str(selected[0].get("Item"))
    return None


def _plain_table(df: pd.DataFrame) -> str | None:
    """The table without AgGrid; returns the clicked row's Item."""
    event = st.dataframe(
        df, hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row",
        key="build_plan_table",
        column_config={
            "Priority Score": st.column_config.NumberColumn(format="%.2f"),
            "ISK/hour": st.column_config.NumberColumn(format="%.2f"),
            "Margin %": st.column_config.NumberColumn(format="%.2f%%"),
            "Days of Supply": st.column_config.NumberColumn(format="%.2f"),
        },
    )
    rows = event.selection.rows if event is not None else []
    return str(df.iloc[rows[0]]["Item"]) if rows else None


def decision_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    """{decision: count}, build/watch/skip/pause first, any other after."""
    order = ("build", "watch", "skip", "pause")
    counts: dict[str, int] = {}
    for item in items:
        d = str(item.get("decision") or "unknown")
        counts[d] = counts.get(d, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (order.index(kv[0]) if kv[0] in order else len(order), kv[0])))


def matches_search(item: dict[str, Any], search: str | None) -> bool:
    """Case-insensitive match on the item name or its decision reason."""
    needle = (search or "").strip().lower()
    if not needle:
        return True
    return needle in str(item.get("type_name") or "").lower() or needle in str(item.get("decision_reason") or "").lower()
