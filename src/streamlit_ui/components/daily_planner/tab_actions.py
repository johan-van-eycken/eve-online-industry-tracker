from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import streamlit as st

from streamlit_ui.api.daily_planner import mark_action_done, set_action_status
from streamlit_ui.state.daily_planner_page import DailyPlannerPageState
from streamlit_ui.components.daily_planner.status_bar import (
    PLANNER_MAX_PLAN_AGE_HOURS,
    _fmt_isk,
    _parse_dt,
)

# ---------------------------------------------------------------------------
# Action type ordering and display config
# ---------------------------------------------------------------------------

# Action types that are corp-level and do NOT appear in Tab 1
_CORP_LEVEL_ACTIONS = frozenset({"buy_materials", "buy_bpo"})

# Ordered sections for Tab 1
_ACTION_ORDER: list[tuple[str, str, str]] = [
    ("deliver",          "DELIVER",          "🔴"),
    ("invent",           "INVENT",           "🟡"),
    ("copy",             "COPY",             "🟣"),
    ("me_research",      "ME RESEARCH",      "🔧"),
    ("te_research",      "TE RESEARCH",      "🔧"),
    ("sub_manufacture",  "SUB-MANUFACTURE",  "🔧"),
    ("manufacture",      "MANUFACTURE",      "🟢"),
]

# Which action types are "START" actions (require materials to be ready)
_START_ACTIONS = frozenset({
    "manufacture", "sub_manufacture", "invent", "copy", "me_research", "te_research"
})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_completion(estimated_completion: Any) -> str:
    if not estimated_completion:
        return ""
    dt = _parse_dt(estimated_completion)
    if dt is None:
        return str(estimated_completion)
    now_utc = datetime.now(tz=timezone.utc)
    delta = dt - now_utc
    total_secs = int(delta.total_seconds())
    if total_secs <= 0:
        return "ready"
    days = total_secs // 86400
    hours = (total_secs % 86400) // 3600
    mins = (total_secs % 3600) // 60
    if days > 0:
        rel = f"est. {days}d {hours}h"
    elif hours > 0:
        rel = f"est. {hours}h {mins}m"
    else:
        rel = f"est. {mins}m"
    abs_str = dt.strftime("(%a %d %b %H:%M EVE)")
    return f"{rel} {abs_str}"


def _character_level_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Actions shown in Tab 1: the types this tab renders.

    Corp-level buy actions belong to Tab 2. Any other type (e.g. one persisted
    by an older planner version) is dropped here so it neither crashes the tab
    nor keeps "Day complete" from ever showing as an invisible pending row.
    """
    rendered = {action_type for action_type, _, _ in _ACTION_ORDER}
    return [a for a in actions if str(a.get("action_type") or "") in rendered]


def _has_pending_buys(plan: dict[str, Any]) -> bool:
    """True if any current_job buy_materials actions are still pending."""
    actions = plan.get("actions") or []
    return any(
        str(a.get("action_type") or "") == "buy_materials"
        and str(a.get("shopping_category") or "") == "current_job"
        and str(a.get("status") or "pending") == "pending"
        for a in actions
    )


def _reload_plan(page_state: DailyPlannerPageState) -> None:
    """Reload the plan from the API into page state."""
    from streamlit_ui.api.daily_planner import get_plan
    try:
        data = get_plan() or {}
        if data.get("plan") is not None:
            page_state.plan = data
    except Exception as exc:
        st.error(f"Failed to reload plan: {exc}")


# ---------------------------------------------------------------------------
# Action row renderer
# ---------------------------------------------------------------------------

def _render_action_row(
    action: dict[str, Any],
    page_state: DailyPlannerPageState,
    readiness_ok: bool,
) -> None:
    action_id = int(action.get("id") or 0)
    status = str(action.get("status") or "pending")
    action_type = str(action.get("action_type") or "")
    type_name = str(action.get("type_name") or "Unknown")
    quantity = action.get("quantity")
    runs = action.get("runs")
    cost = action.get("estimated_cost_isk")
    completion = action.get("estimated_completion")
    notes = str(action.get("notes") or "")
    is_override = bool(action.get("is_force_override"))

    # Build label
    qty_label = ""
    if runs is not None:
        qty_label = f"×{runs} runs"
    elif quantity is not None:
        qty_label = f"×{quantity:,}" if isinstance(quantity, int) else f"×{quantity}"

    cost_label = _fmt_isk(float(cost)) if cost is not None else ""
    completion_label = _fmt_completion(completion)

    # Row layout
    if status == "done":
        st.markdown(f"~~{type_name} {qty_label}~~ ✅")
        return
    if status == "skipped":
        st.markdown(f"~~{type_name} {qty_label}~~ *(skipped)*")
        return

    # Pending action
    cols = st.columns([0.5, 0.6, 4, 1.5, 1.5, 1.5, 1])
    with cols[0]:
        if st.button("✅", key=f"done_{action_id}", help="Mark as done"):
            try:
                mark_action_done(action_id)
                _reload_plan(page_state)
                st.rerun()
            except Exception as exc:
                st.error(f"Failed: {exc}")

    with cols[1]:
        if st.button("Skip", key=f"skip_{action_id}", help="Skip this action"):
            try:
                set_action_status(action_id, "skipped")
                _reload_plan(page_state)
                st.rerun()
            except Exception as exc:
                st.error(f"Failed: {exc}")

    with cols[2]:
        label = type_name
        if qty_label:
            label += f" {qty_label}"
        if is_override:
            label += " ★ manual override"
        st.markdown(label)

    with cols[3]:
        if cost_label:
            st.caption(cost_label)

    with cols[4]:
        if completion_label:
            st.caption(completion_label)

    with cols[5]:
        if action_type in _START_ACTIONS:
            if readiness_ok:
                st.caption("✅ Ready")
            else:
                st.caption("🛒 Buy first")

    with cols[6]:
        pass  # spacer

    if notes:
        st.caption(f"  ↳ {notes}")


# ---------------------------------------------------------------------------
# Workflow progress header
# ---------------------------------------------------------------------------

def _render_workflow_header(actions: list[dict[str, Any]]) -> None:
    deliver_done = all(
        str(a.get("status") or "pending") in ("done", "skipped")
        for a in actions
        if str(a.get("action_type") or "") == "deliver"
    ) and any(str(a.get("action_type") or "") == "deliver" for a in actions)

    start_done = all(
        str(a.get("status") or "pending") in ("done", "skipped")
        for a in actions
        if str(a.get("action_type") or "") in _START_ACTIONS
    ) and any(str(a.get("action_type") or "") in _START_ACTIONS for a in actions)

    step1 = "Step 1: Deliver ✓" if deliver_done else "Step 1: Deliver"
    step2 = "Step 2: Buy materials → [Shopping List]"
    step3 = "Step 3: Start jobs ✓" if start_done else "Step 3: Start jobs"

    st.markdown(
        f"**{step1}** &nbsp;→&nbsp; {step2} &nbsp;→&nbsp; **{step3}**"
    )
    st.markdown("---")


# ---------------------------------------------------------------------------
# Character section renderer
# ---------------------------------------------------------------------------

def _render_character_section(
    char_name: str,
    char_actions: list[dict[str, Any]],
    page_state: DailyPlannerPageState,
    readiness_ok: bool,
) -> None:
    # Group by action type
    by_type: dict[str, list[dict[str, Any]]] = {}
    for a in char_actions:
        at = str(a.get("action_type") or "")
        by_type.setdefault(at, []).append(a)

    pending_count = sum(
        1 for a in char_actions
        if str(a.get("status") or "pending") == "pending"
    )
    label = f"{char_name} — {pending_count} pending"

    with st.expander(label, expanded=pending_count > 0):
        for action_type, section_label, section_emoji in _ACTION_ORDER:
            section_actions = by_type.get(action_type, [])
            if not section_actions:
                continue

            st.markdown(f"**{section_emoji} {section_label}**")

            # "Mark All Delivered Done" convenience button
            if action_type == "deliver":
                pending_delivers = [
                    a for a in section_actions
                    if str(a.get("status") or "pending") == "pending"
                ]
                if pending_delivers:
                    if st.button(
                        f"Mark All Delivered Done ({len(pending_delivers)})",
                        key=f"mark_all_deliver_{char_name}",
                    ):
                        errors: list[str] = []
                        for a in pending_delivers:
                            try:
                                mark_action_done(int(a.get("id") or 0))
                            except Exception as exc:
                                errors.append(str(exc))
                        if errors:
                            st.error(f"Some actions failed: {'; '.join(errors)}")
                        _reload_plan(page_state)
                        st.rerun()

            for action in section_actions:
                _render_action_row(action, page_state, readiness_ok)

            st.markdown("")  # spacer between sections


# ---------------------------------------------------------------------------
# Main tab renderer
# ---------------------------------------------------------------------------

def render_tab_actions(page_state: DailyPlannerPageState) -> None:
    if page_state.plan is None:
        st.info("No plan yet — click **Recompute Plan** to generate your first daily plan.")
        return

    plan_meta = page_state.plan.get("plan") or {}
    created_at = plan_meta.get("created_at")
    plan_expired = False
    if created_at:
        dt = _parse_dt(created_at)
        if dt:
            age_hours = (datetime.now(tz=timezone.utc) - dt).total_seconds() / 3600
            plan_expired = age_hours > PLANNER_MAX_PLAN_AGE_HOURS

    if plan_expired:
        st.warning(
            "This plan is outdated and actions are no longer valid. "
            "Recompute to generate today's actions."
        )
        st.caption(
            "Jobs that completed while you were away have been tracked automatically. "
            "Recomputing will plan from your current slot availability, stock, and market state — "
            "no manual reconciliation needed."
        )
        return

    all_actions: list[dict[str, Any]] = page_state.plan.get("actions") or []

    char_actions = _character_level_actions(all_actions)

    if not char_actions:
        st.info("No actions for today. The plan has no pending character-level tasks.")
        return

    # Check if day is complete
    all_done = all(
        str(a.get("status") or "pending") in ("done", "skipped")
        for a in char_actions
    )
    if all_done:
        # Day complete banner
        jobs_started = sum(
            1 for a in char_actions
            if str(a.get("action_type") or "") in _START_ACTIONS
            and str(a.get("status") or "pending") == "done"
        )
        isk_committed = sum(
            float(a.get("estimated_cost_isk") or 0.0)
            for a in char_actions
            if str(a.get("status") or "pending") == "done"
        )
        next_completion: str = ""
        future_completions = [
            _parse_dt(a.get("estimated_completion"))
            for a in char_actions
            if a.get("estimated_completion") and str(a.get("action_type") or "") in _START_ACTIONS
        ]
        valid_completions = [d for d in future_completions if d is not None]
        if valid_completions:
            soonest = min(valid_completions)
            next_completion = f" · Next job completing {soonest.strftime('%a %d %b %H:%M EVE')}"
        st.success(
            f"Day complete — see you tomorrow! "
            f"{jobs_started} jobs started, {_fmt_isk(isk_committed)} committed{next_completion}."
        )
        return

    # Workflow progress header
    _render_workflow_header(char_actions)

    # Determine readiness: if any current_job buy_materials actions are pending, not ready
    readiness_ok = not _has_pending_buys(page_state.plan)

    if not readiness_ok:
        st.caption(
            "🛒 Some materials still need to be purchased — go to **Shopping List** tab first. "
            "Readiness based on stock at plan time — re-check in-game before submitting."
        )
    else:
        st.caption("Readiness based on stock at plan time — re-check in-game before submitting.")

    # Group by character
    char_groups: dict[str, list[dict[str, Any]]] = {}
    for action in char_actions:
        name = str(action.get("character_name") or action.get("character_id") or "Unknown")
        char_groups.setdefault(name, []).append(action)

    for char_name, actions in sorted(char_groups.items()):
        _render_character_section(char_name, actions, page_state, readiness_ok)
