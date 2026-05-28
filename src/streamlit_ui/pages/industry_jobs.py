from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd
import streamlit as st

from streamlit_ui.api.characters import build_character_options, fetch_characters, filter_industry_characters
from streamlit_ui.api.industry_jobs import clear_active_jobs_cache, fetch_active_industry_jobs
from streamlit_ui.components.aggrid_formatters import js_eu_isk_formatter, js_eu_number_formatter, js_icon_cell_renderer
from streamlit_ui.components.formatters import blueprint_image_url, format_isk_eu, type_icon_url
from streamlit_ui.components.webpage_ui import AgGridRuntime, render_aggrid_table, require_aggrid


_ACTIVITY_ICONS = {
    "Manufacturing": "\u2692\ufe0f",
    "Reaction": "\u2697\ufe0f",
    "Copying": "\U0001f4cb",
    "ME Research": "\U0001f4d0",
    "TE Research": "\u23f1\ufe0f",
    "Invention": "\U0001f4a1",
}

_ACTIVITY_COLORS = {
    "Manufacturing": "#4CAF50",
    "Reaction": "#FF9800",
    "Copying": "#2196F3",
    "ME Research": "#9C27B0",
    "TE Research": "#00BCD4",
    "Invention": "#E91E63",
}


def _parse_iso(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _format_time_remaining(end_date: Any) -> str:
    dt = _parse_iso(end_date)
    if dt is None:
        return "Unknown"
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = dt - now
    total_seconds = int(delta.total_seconds())
    if total_seconds <= 0:
        return "Ready"
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _format_duration(seconds: int) -> str:
    if seconds <= 0:
        return "-"
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _progress_fraction(start_date: Any, end_date: Any) -> float:
    start_dt = _parse_iso(start_date)
    end_dt = _parse_iso(end_date)
    if start_dt is None or end_dt is None:
        return 0.0
    now = datetime.now(timezone.utc)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    total = (end_dt - start_dt).total_seconds()
    elapsed = (now - start_dt).total_seconds()
    if total <= 0:
        return 1.0
    return max(0.0, min(1.0, elapsed / total))


def _render_slot_card(job: dict[str, Any]) -> None:
    """Render a single industry slot as an EVE-like card."""
    activity_name = job.get("activity_name", "Unknown")
    icon = _ACTIVITY_ICONS.get(activity_name, "\u2699\ufe0f")
    color = _ACTIVITY_COLORS.get(activity_name, "#666666")
    product_name = job.get("product_name") or job.get("blueprint_name") or "Unknown"
    product_type_id = job.get("product_type_id", 0)
    blueprint_type_id = job.get("blueprint_type_id", 0)
    runs = job.get("runs", 0)
    cost = job.get("cost", 0)
    time_remaining = _format_time_remaining(job.get("end_date"))
    progress = _progress_fraction(job.get("start_date"), job.get("end_date"))
    duration_str = _format_duration(job.get("duration_seconds", 0))

    if activity_name in {"Copying", "Invention"}:
        img_url = blueprint_image_url(blueprint_type_id, is_bpc=True, size=64) or ""
    elif activity_name in {"ME Research", "TE Research"}:
        img_url = blueprint_image_url(blueprint_type_id, is_bpc=False, size=64) or ""
    else:
        img_url = type_icon_url(product_type_id, size=64) or ""

    progress_pct = int(progress * 100)
    is_ready = time_remaining == "Ready"
    progress_color = "#4CAF50" if is_ready else color
    ready_label = '<b style="color: #4CAF50;">READY</b>' if is_ready else f"Remaining: <b>{time_remaining}</b>"

    st.markdown(
        f"""<div style="border: 1px solid {color}; border-radius: 6px; padding: 10px; margin-bottom: 8px;
                        background: linear-gradient(135deg, rgba(30,30,40,0.9), rgba(20,20,30,0.95));
                        box-shadow: 0 2px 4px rgba(0,0,0,0.3);">
            <div style="display: flex; align-items: center; gap: 12px;">
                <img src="{img_url}" width="48" height="48" style="border-radius: 4px; border: 1px solid #444;"
                     onerror="this.style.display='none'"/>
                <div style="flex: 1;">
                    <div style="font-weight: bold; font-size: 14px; color: #eee;">
                        {icon} {product_name}
                    </div>
                    <div style="font-size: 12px; color: #aaa; margin-top: 2px;">
                        {activity_name} &bull; {runs} run{"s" if runs != 1 else ""} &bull; Duration: {duration_str}
                    </div>
                    <div style="margin-top: 6px; background: #333; border-radius: 3px; height: 14px; overflow: hidden;">
                        <div style="width: {progress_pct}%; height: 100%; background: {progress_color};
                                    transition: width 0.3s; border-radius: 3px;
                                    display: flex; align-items: center; justify-content: center;
                                    font-size: 10px; color: white; font-weight: bold;">
                            {progress_pct}%
                        </div>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-top: 4px; font-size: 11px; color: #999;">
                        <span>{ready_label}</span>
                        <span>Install cost: <b>{format_isk_eu(cost)}</b></span>
                    </div>
                </div>
            </div>
        </div>""",
        unsafe_allow_html=True,
    )


def _render_summary_metrics(jobs: list[dict[str, Any]]) -> None:
    """Render summary metrics for all active jobs."""
    total_jobs = len(jobs)
    activity_counts: dict[str, int] = {}
    total_cost = 0.0
    ready_count = 0

    for job in jobs:
        activity = job.get("activity_name", "Unknown")
        activity_counts[activity] = activity_counts.get(activity, 0) + 1
        total_cost += float(job.get("cost", 0) or 0)
        if _format_time_remaining(job.get("end_date")) == "Ready":
            ready_count += 1

    cols = st.columns(4)
    cols[0].metric("Total Active Slots", total_jobs)
    cols[1].metric("Ready for Delivery", ready_count)
    cols[2].metric("Total Install Cost", format_isk_eu(total_cost))
    cols[3].metric("Activity Types", len(activity_counts))

    # Activity breakdown bar
    if activity_counts:
        breakdown_parts = []
        for activity, count in sorted(activity_counts.items(), key=lambda x: -x[1]):
            icon = _ACTIVITY_ICONS.get(activity, "")
            color = _ACTIVITY_COLORS.get(activity, "#666")
            breakdown_parts.append(
                f'<span style="color: {color}; margin-right: 16px;">{icon} {activity}: <b>{count}</b></span>'
            )
        st.markdown(
            '<div style="padding: 8px 0; font-size: 13px;">' + "".join(breakdown_parts) + "</div>",
            unsafe_allow_html=True,
        )


def render() -> None:
    st.title("Industry Slots")
    st.caption("Active industry jobs across all characters. Shows occupied manufacturing, research, invention, and reaction slots.")

    try:
        characters = fetch_characters()
    except Exception as exc:
        st.error(f"Failed to load characters: {exc}")
        return

    industry_characters = filter_industry_characters(characters)
    char_options = build_character_options(industry_characters)

    # Controls row
    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 2, 1])

    with ctrl_col1:
        char_filter_options = ["All Characters"] + [f"{name} ({cid})" for cid, name in char_options.items()]
        selected_char_label = st.selectbox("Character", char_filter_options, key="industry_slots_char_filter")

    selected_character_id: int | None = None
    if selected_char_label and selected_char_label != "All Characters":
        # Extract character ID from "Name (ID)" format
        try:
            selected_character_id = int(selected_char_label.rsplit("(", 1)[-1].rstrip(")"))
        except (ValueError, IndexError):
            pass

    with ctrl_col2:
        view_mode = st.selectbox("View", ["Slot Cards", "Table View"], key="industry_slots_view_mode")

    with ctrl_col3:
        st.write("")  # spacing
        if st.button("Refresh", key="industry_slots_refresh"):
            clear_active_jobs_cache()
            st.rerun()

    # Fetch data
    try:
        payload = fetch_active_industry_jobs(character_id=selected_character_id)
    except Exception as exc:
        st.error(f"Failed to load active industry jobs: {exc}")
        return

    jobs = payload.get("jobs") or []
    slot_capacities = payload.get("slot_capacities") or {}

    # Filter to only industry-enabled characters
    industry_char_ids = set(char_options.keys())
    jobs = [j for j in jobs if j.get("character_id") in industry_char_ids]
    slot_capacities = {k: v for k, v in slot_capacities.items() if int(k) in industry_char_ids}

    if not jobs:
        # Still show slot capacities even with no active jobs
        if slot_capacities:
            _render_slot_capacity_overview(jobs, slot_capacities, char_options)
        else:
            st.info("No active industry jobs found." + (" Try selecting 'All Characters' or refresh character data." if selected_character_id else ""))
        return

    _render_summary_metrics(jobs)

    if slot_capacities:
        _render_slot_capacity_overview(jobs, slot_capacities, char_options)

    st.divider()

    if view_mode == "Slot Cards":
        _render_card_view(jobs, char_options, slot_capacities)
    else:
        _render_table_view(jobs)


def _count_used_slots(jobs: list[dict[str, Any]], char_id: int) -> dict[str, int]:
    """Count used slots per category for a character."""
    mfg = 0
    research = 0
    reaction = 0
    for job in jobs:
        if job.get("character_id") != char_id:
            continue
        activity = job.get("activity_name", "")
        if activity == "Manufacturing":
            mfg += 1
        elif activity == "Reaction":
            reaction += 1
        elif activity in ("ME Research", "TE Research", "Copying", "Invention"):
            research += 1
    return {"manufacturing": mfg, "research": research, "reaction": reaction}


def _render_slot_capacity_overview(
    jobs: list[dict[str, Any]],
    slot_capacities: dict[str, Any],
    char_options: dict[int, str],
) -> None:
    """Render slot usage vs capacity per character."""
    if not slot_capacities:
        return

    st.markdown("**Slot Availability**")
    cols = st.columns(min(len(slot_capacities), 4))

    for idx, (char_id_str, caps) in enumerate(sorted(
        slot_capacities.items(),
        key=lambda x: char_options.get(int(x[0]), x[0]),
    )):
        char_id = int(char_id_str)
        char_name = char_options.get(char_id, str(char_id))
        used = _count_used_slots(jobs, char_id)

        mfg_max = caps.get("manufacturing_max", 1)
        res_max = caps.get("research_max", 1)
        react_max = caps.get("reaction_max", 1)
        mfg_used = used["manufacturing"]
        res_used = used["research"]
        react_used = used["reaction"]

        mfg_free = max(0, mfg_max - mfg_used)
        res_free = max(0, res_max - res_used)
        react_free = max(0, react_max - react_used)

        with cols[idx % len(cols)]:
            st.markdown(f"**{char_name}**")

            def _slot_bar(label: str, used_count: int, max_count: int, color: str) -> str:
                pct = int((used_count / max_count) * 100) if max_count > 0 else 0
                free = max(0, max_count - used_count)
                status_color = "#4CAF50" if free > 0 else "#F44336"
                return (
                    f'<div style="margin: 3px 0;">'
                    f'<span style="font-size: 12px; color: #ccc;">{label}: '
                    f'<b style="color: {status_color};">{used_count}/{max_count}</b>'
                    f' ({free} free)</span>'
                    f'<div style="background: #333; border-radius: 3px; height: 8px; margin-top: 2px;">'
                    f'<div style="width: {pct}%; height: 100%; background: {color}; border-radius: 3px;"></div>'
                    f'</div></div>'
                )

            html = _slot_bar("Manufacturing", mfg_used, mfg_max, _ACTIVITY_COLORS["Manufacturing"])
            html += _slot_bar("Research/Inv", res_used, res_max, _ACTIVITY_COLORS["ME Research"])
            html += _slot_bar("Reactions", react_used, react_max, _ACTIVITY_COLORS["Reaction"])
            st.markdown(html, unsafe_allow_html=True)


def _render_card_view(jobs: list[dict[str, Any]], char_options: dict[int, str], slot_capacities: dict[str, Any]) -> None:
    """Render EVE-like slot cards grouped by character."""
    # Group jobs by character
    jobs_by_char: dict[int, list[dict[str, Any]]] = {}
    for job in jobs:
        cid = job.get("character_id", 0)
        if cid not in jobs_by_char:
            jobs_by_char[cid] = []
        jobs_by_char[cid].append(job)

    for char_id, char_jobs in sorted(jobs_by_char.items(), key=lambda x: char_options.get(x[0], str(x[0]))):
        char_name = char_options.get(char_id, str(char_id))
        active_count = len(char_jobs)
        ready_count = sum(1 for j in char_jobs if _format_time_remaining(j.get("end_date")) == "Ready")

        # Slot capacity info for header
        caps = slot_capacities.get(str(char_id)) or {}
        used = _count_used_slots(jobs, char_id)
        capacity_parts: list[str] = []
        if caps:
            mfg_max = caps.get("manufacturing_max", 0)
            res_max = caps.get("research_max", 0)
            react_max = caps.get("reaction_max", 0)
            if mfg_max:
                capacity_parts.append(f"MFG {used['manufacturing']}/{mfg_max}")
            if res_max:
                capacity_parts.append(f"Research {used['research']}/{res_max}")
            if react_max:
                capacity_parts.append(f"React {used['reaction']}/{react_max}")

        header_suffix = f" ({ready_count} ready)" if ready_count > 0 else ""
        capacity_text = f" | {' | '.join(capacity_parts)}" if capacity_parts else ""
        st.subheader(f"{char_name} - {active_count} active slot{'s' if active_count != 1 else ''}{header_suffix}")
        if capacity_text:
            st.caption(f"Slot usage:{capacity_text}")

        # Group by activity type within character
        jobs_by_activity: dict[str, list[dict[str, Any]]] = {}
        for job in char_jobs:
            activity = job.get("activity_name", "Unknown")
            if activity not in jobs_by_activity:
                jobs_by_activity[activity] = []
            jobs_by_activity[activity].append(job)

        # Render in activity order
        activity_order = ["Manufacturing", "Reaction", "ME Research", "TE Research", "Copying", "Invention"]
        for activity in activity_order:
            activity_jobs = jobs_by_activity.get(activity)
            if not activity_jobs:
                continue

            icon = _ACTIVITY_ICONS.get(activity, "")
            color = _ACTIVITY_COLORS.get(activity, "#666")
            st.markdown(
                f'<div style="font-size: 13px; font-weight: bold; color: {color}; margin: 8px 0 4px 0;">'
                f'{icon} {activity} ({len(activity_jobs)} slot{"s" if len(activity_jobs) != 1 else ""})</div>',
                unsafe_allow_html=True,
            )

            # Render in 2-column layout for wider slots
            col1, col2 = st.columns(2)
            for idx, job in enumerate(sorted(activity_jobs, key=lambda j: j.get("end_date") or "")):
                with col1 if idx % 2 == 0 else col2:
                    _render_slot_card(job)

        st.divider()


def _render_table_view(jobs: list[dict[str, Any]]) -> None:
    """Render an AG-Grid table view of active jobs."""
    runtime = require_aggrid()

    rows = []
    for job in jobs:
        progress = _progress_fraction(job.get("start_date"), job.get("end_date"))
        rows.append({
            "Icon": (
                blueprint_image_url(job.get("blueprint_type_id"), is_bpc=True, size=32)
                if job.get("activity_name") in {"Copying", "Invention"}
                else blueprint_image_url(job.get("blueprint_type_id"), is_bpc=False, size=32)
                if job.get("activity_name") in {"ME Research", "TE Research"}
                else type_icon_url(job.get("product_type_id"), size=32)
            ) or "",
            "Character": job.get("character_name", ""),
            "Activity": job.get("activity_name", ""),
            "Product": job.get("product_name") or job.get("blueprint_name") or "",
            "Runs": job.get("runs", 0),
            "Progress": round(progress * 100, 1),
            "Time Remaining": _format_time_remaining(job.get("end_date")),
            "Duration": _format_duration(job.get("duration_seconds", 0)),
            "Install Cost": float(job.get("cost", 0) or 0),
            "Start": str(job.get("start_date") or "")[:16],
            "End": str(job.get("end_date") or "")[:16],
        })

    df = pd.DataFrame(rows)

    render_aggrid_table(
        df,
        runtime=runtime,
        key="industry_slots_table",
        image_cols=["Icon"],
        image_renderer=js_icon_cell_renderer(JsCode=runtime.js_code),
        isk_cols=["Install Cost"],
        number_cols_0=["Runs"],
        number_cols_2=["Progress"],
        height=min(700, 40 + len(rows) * 35),
        empty_message="No active industry jobs.",
    )
