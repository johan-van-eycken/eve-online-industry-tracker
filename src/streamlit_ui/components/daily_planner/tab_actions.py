from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import streamlit as st

from streamlit_ui.api.admin_settings import planner_page_defaults
from streamlit_ui.api.daily_planner import mark_action_done, set_action_status
from streamlit_ui.state.daily_planner_page import DailyPlannerPageState
from streamlit_ui.components.daily_planner.status_bar import (
    CHARACTER_ACTION_TYPES,
    CORP_LEVEL_ACTION_TYPES,
    _fmt_isk,
    _parse_dt,
    free_slots_by_character,
    is_waiting,
    plan_summary,
    queued_te_status,
)

logger = logging.getLogger(__name__)

# Unknown action types already reported this process; Streamlit reruns the
# page on every interaction, and one warning per type is enough.
_WARNED_UNKNOWN_ACTION_TYPES: set[str] = set()

# ---------------------------------------------------------------------------
# Action type ordering and display config
# ---------------------------------------------------------------------------

# Section label and marker per action type. The order and the set of types
# come from status_bar.CHARACTER_ACTION_TYPES, the single source.
# The colour is a Streamlit markdown colour (:red[...]).
_SECTION_DISPLAY: dict[str, tuple[str, str]] = {
    "deliver":         ("DELIVER",         "red"),
    "invent":          ("INVENT",          "orange"),
    "copy":            ("COPY",            "violet"),
    "me_research":     ("ME RESEARCH",     "blue"),
    "te_research":     ("TE RESEARCH",     "blue"),
    "sub_manufacture": ("SUB-MANUFACTURE", "gray"),
    "manufacture":     ("MANUFACTURE",     "green"),
}


def _action_sections(action_types: tuple[str, ...]) -> list[tuple[str, str, str]]:
    """(action type, label, colour) per type, in the given order. A type with
    no display entry raises KeyError at import, not a silently missing section."""
    return [(t, *_SECTION_DISPLAY[t]) for t in action_types]


# Ordered sections for Tab 1
_ACTION_ORDER: list[tuple[str, str, str]] = _action_sections(CHARACTER_ACTION_TYPES)

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
    by an older planner version) is dropped here, so it neither crashes the
    tab nor keeps "Day complete" from showing. It is logged once per type, so
    a type the planner starts producing without a section here is noticed.
    """
    rendered = set(CHARACTER_ACTION_TYPES)
    unknown = {
        str(a.get("action_type") or "") for a in actions
    } - rendered - CORP_LEVEL_ACTION_TYPES - _WARNED_UNKNOWN_ACTION_TYPES
    if unknown:
        logger.warning(
            "Daily planner actions tab: not showing action type(s) %s "
            "(not produced by this planner version)",
            ", ".join(sorted(t or "<missing>" for t in unknown)),
        )
        _WARNED_UNKNOWN_ACTION_TYPES.update(unknown)
    return [a for a in actions if str(a.get("action_type") or "") in rendered]


def _is_queued(action: dict[str, Any]) -> bool:
    """A TE research queued behind the ME research on the same BPO:
    EVE runs one job per blueprint, so it cannot start today
    and does not count as today's work."""
    return bool(action.get("queued_after_me"))


def _todays(
    actions: list[dict[str, Any]], now: datetime | None = None, *, jobs: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """The actions that can be done today: every action but a queued one
    whose ME research has not ended yet (once it has, the
    queued TE is today's work, with a "Mark started" button). `jobs`: the
    active industry jobs, whose ME job end_date decides (status_bar.queued_te_status)."""
    return [a for a in actions if not is_waiting(a, actions, now, jobs=jobs)]


# The shopping list category each start action waits on; copies and research use no materials.
_BUY_CATEGORY: dict[str, str] = {
    "manufacture": "current_job", "sub_manufacture": "current_job", "invent": "invention_input",
}


PendingBuys = dict[str, dict[int, str]]


def pending_buy_lines(actions: list[dict[str, Any]]) -> PendingBuys:
    """{shopping category: {type_id: item name}} of the buy lines still pending."""
    lines: PendingBuys = {}
    for a in actions:
        if str(a.get("action_type") or "") == "buy_materials" and str(a.get("status") or "pending") == "pending":
            lines.setdefault(str(a.get("shopping_category") or ""), {})[int(a.get("type_id") or 0)] = str(
                a.get("type_name") or "Unknown")
    return lines


def waiting_on(action: dict[str, Any], pending_buys: PendingBuys) -> list[str]:
    """The pending buy lines (item names) this job waits on: the materials it
    was short of at plan time that are not bought yet. A plan from before the
    link (no short_type_ids) waits on every pending line of its kind."""
    pending = pending_buys.get(_BUY_CATEGORY.get(str(action.get("action_type") or ""), ""), {})
    short = action.get("short_type_ids")
    if short is None:
        return list(pending.values())
    ids = [int(t) for t in str(short).split(",") if t.strip()]
    return [pending[t] for t in ids if t in pending]


def needs_purchase(action: dict[str, Any], pending_buys: PendingBuys) -> bool:
    return bool(waiting_on(action, pending_buys))


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

def action_quantity_label(action: dict[str, Any]) -> str:
    """The row's quantity label. A copy is one job of
    `quantity` copies whose `runs` are the copied runs in all, so it reads
    "N copies × M runs" (M runs per copy)."""
    quantity = action.get("quantity")
    runs = action.get("runs")
    if str(action.get("action_type") or "") == "copy" and isinstance(quantity, int) and quantity > 0 \
            and isinstance(runs, int) and runs > 0:
        per_copy = runs // quantity
        return (f"{quantity} {'copy' if quantity == 1 else 'copies'} × "
                f"{per_copy} {'run' if per_copy == 1 else 'runs'}")
    if runs is not None:
        return f"×{runs} runs"
    if quantity is not None:
        return f"×{quantity:,}" if isinstance(quantity, int) else f"×{quantity}"
    return ""


#: Today's Actions filter choices.
FILTER_TODO, FILTER_ALL, FILTER_READY, FILTER_BUY = "Todo's", "All", "Ready now", "Needs purchase"


def short_notes(notes: str, keep: int = 3) -> tuple[str, str | None]:
    """(one line, full text or None): the planner's "; "-joined reason parts
    as "a · b · c", cut after `keep` parts; the full text comes back only
    when something was cut (it goes in the tooltip)."""
    parts = [p.strip() for p in notes.split(";") if p.strip()]
    if len(parts) <= keep:
        return " · ".join(parts), None
    return " · ".join(parts[:keep]) + " …", " · ".join(parts)


def matches_filter(action: dict[str, Any], mode: str, pending_buys: PendingBuys) -> bool:
    """Whether the row shows under the filter: "Todo's" is what is neither
    done nor skipped, "Ready now" what can be done without buying, "Needs
    purchase" the jobs still waiting on the shopping list."""
    if mode == FILTER_ALL:
        return True
    if mode == FILTER_TODO:
        return str(action.get("status") or "pending") == "pending"
    needs_buy = needs_purchase(action, pending_buys)
    return needs_buy if mode == FILTER_BUY else not needs_buy


def step_summaries(
    char_actions: list[dict[str, Any]], all_actions: list[dict[str, Any]], *,
    jobs: list[dict[str, Any]] | None = None,
) -> list[tuple[str, str, bool]]:
    """(title, grey line, done) for the three daily steps."""
    def _pending(rows):
        return [a for a in rows if str(a.get("status") or "pending") == "pending"]

    delivers = [a for a in char_actions if str(a.get("action_type") or "") == "deliver"]
    buys = [a for a in all_actions if str(a.get("action_type") or "") == "buy_materials"]
    starts = [a for a in _todays(char_actions, jobs=jobs) if str(a.get("action_type") or "") in _START_ACTIONS]
    pending_buys = _pending(buys)
    buy_isk = sum(float(a.get("estimated_cost_isk") or 0.0) for a in pending_buys)
    pilots = {a.get("character_id") for a in _pending(starts)}
    return [
        ("Deliver", f"{len(_pending(delivers))} job(s) ready" if _pending(delivers) else "nothing to deliver",
         not _pending(delivers)),
        ("Buy materials", f"{len(pending_buys)} item(s) · {_fmt_isk(buy_isk)} → Shopping List" if pending_buys
         else "everything bought", not pending_buys),
        ("Start jobs", f"{len(_pending(starts))} job(s) on {len(pilots)} pilot(s)" if _pending(starts)
         else "all started", bool(starts) and not _pending(starts)),
    ]


def _status_button(action_id: int, page_state: DailyPlannerPageState, *, label: str, key: str,
                   help_text: str, skip: bool = False) -> None:
    if st.button(label, key=key, help=help_text):
        try:
            if skip:
                set_action_status(action_id, "skipped")
            else:
                mark_action_done(action_id)
            _reload_plan(page_state)
            st.rerun()
        except Exception as exc:
            st.error(f"Failed: {exc}")


def _render_action_row(
    action: dict[str, Any],
    page_state: DailyPlannerPageState,
    pending_buys: PendingBuys,
    siblings: list[dict[str, Any]] | None = None,
    *,
    jobs: list[dict[str, Any]] | None = None,
    show_reasons: bool = True,
) -> None:
    action_id = int(action.get("id") or 0)
    status = str(action.get("status") or "pending")
    action_type = str(action.get("action_type") or "")
    type_name = str(action.get("type_name") or "Unknown")
    cost = action.get("estimated_cost_isk")
    notes = str(action.get("notes") or "")
    qty_label = action_quantity_label(action)
    cost_label = _fmt_isk(float(cost)) if cost is not None else ""
    completion_label = _fmt_completion(action.get("estimated_completion"))

    if status in ("done", "skipped"):
        st.markdown(f":gray[~~{type_name} {qty_label}~~ · {'done' if status == 'done' else 'skipped'}]")
        return

    detail_parts: list[str] = []
    badge: tuple[str, str] | None = None
    done_label, done_help = ":material/check:", "Mark as done"
    if _is_queued(action):
        startable, queue_note = queued_te_status(action, siblings or [action], jobs=jobs)
        if not startable:
            # Not startable yet: no done/skip buttons.
            st.markdown(f"**{type_name}** :gray[{qty_label}]")
            st.caption(" · ".join(p for p in ("queued behind the ME research on this BPO", completion_label,
                                              queue_note) if p))
            return
        badge = ("Start now", "blue")
        done_label, done_help = ":material/play_arrow:", "The ME research on this BPO has ended: mark this TE research as started"
        detail_parts.append(queue_note or "")
    elif action_type in _START_ACTIONS:
        missing = waiting_on(action, pending_buys)
        badge = ("Buy first", "orange") if missing else ("Ready", "green")
        if missing:
            detail_parts.append("buy: " + ", ".join(missing[:3]) + (f" +{len(missing) - 3}" if len(missing) > 3 else ""))
    else:
        badge = ("Ready", "green")

    cols = st.columns([0.7, 0.7, 5, 2.2], vertical_alignment="top")
    with cols[0]:
        _status_button(action_id, page_state, label=done_label, key=f"done_{action_id}", help_text=done_help)
    with cols[1]:
        _status_button(action_id, page_state, label=":material/skip_next:", key=f"skip_{action_id}",
                       help_text="Skip this action", skip=True)
    with cols[2]:
        st.markdown(f"**{type_name}** :gray[{qty_label}]")
        if show_reasons:
            short, full = short_notes(notes)
            line = " · ".join(p for p in (short, completion_label, *detail_parts) if p)
            if line:
                st.caption(line, help=full)
    with cols[3]:
        if cost_label:
            st.markdown(f"`{cost_label}`")
        if badge:
            st.badge(badge[0], color=badge[1])


# ---------------------------------------------------------------------------
# Character section renderer
# ---------------------------------------------------------------------------

def _render_character_section(
    char_name: str,
    char_actions: list[dict[str, Any]],
    page_state: DailyPlannerPageState,
    pending_buys: PendingBuys,
    *,
    jobs: list[dict[str, Any]] | None = None,
    free_slots: str | None = None,
    mode: str = FILTER_ALL,
    show_reasons: bool = True,
) -> None:
    by_type: dict[str, list[dict[str, Any]]] = {}
    for a in char_actions:
        by_type.setdefault(str(a.get("action_type") or ""), []).append(a)

    todays_pending = [a for a in _todays(char_actions, jobs=jobs) if str(a.get("status") or "pending") == "pending"]
    pending_isk = sum(float(a.get("estimated_cost_isk") or 0.0) for a in todays_pending)

    with st.container(border=True):
        st.markdown(f"#### {char_name}")
        st.caption(f"{len(todays_pending)} pending · {_fmt_isk(pending_isk)}"
                   + (f" · free slots: {free_slots}" if free_slots else ""))
        shown_any = False
        for action_type, section_label, color in _ACTION_ORDER:
            section_actions = [a for a in by_type.get(action_type, []) if matches_filter(a, mode, pending_buys)]
            if not section_actions:
                continue
            shown_any = True
            pending = [a for a in section_actions if str(a.get("status") or "pending") == "pending"]
            head, button_col = st.columns([3, 2], vertical_alignment="center")
            head.markdown(f":{color}[**{section_label}**] :gray[{len(pending)}]")

            if action_type == "deliver" and pending:
                with button_col:
                    if st.button(f"All delivered ({len(pending)})", key=f"mark_all_deliver_{char_name}"):
                        errors: list[str] = []
                        for a in pending:
                            try:
                                mark_action_done(int(a.get("id") or 0))
                            except RuntimeError as exc:
                                errors.append(str(exc))
                        if errors:
                            st.error(f"Some actions failed: {'; '.join(errors)}")
                        _reload_plan(page_state)
                        st.rerun()

            for action in section_actions:
                _render_action_row(action, page_state, pending_buys, char_actions, jobs=jobs,
                                   show_reasons=show_reasons)
        if not shown_any:
            st.caption("Nothing under this filter.")


# ---------------------------------------------------------------------------
# Main tab renderer
# ---------------------------------------------------------------------------

def render_tab_actions(
    page_state: DailyPlannerPageState, planner_settings: dict[str, float] | None = None, *,
    jobs: list[dict[str, Any]] | None = None,
) -> None:
    """planner_settings: the page's one admin settings read (None: the schema defaults).
    jobs: the active industry jobs the page read (None: unknown, the planner's estimate is used)."""
    if planner_settings is None:
        planner_settings = planner_page_defaults()
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
            # daily_planner/planner_max_plan_age_hours (the status bar shows a problem reading it)
            plan_expired = age_hours > planner_settings["planner_max_plan_age_hours"]

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
        for a in _todays(char_actions, jobs=jobs)
    )
    if all_done:
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

    pending_buys = pending_buy_lines(all_actions)

    filter_col, reasons_col = st.columns([3, 1], vertical_alignment="center")
    with filter_col:
        mode = st.segmented_control(
            "Show", [FILTER_TODO, FILTER_ALL, FILTER_READY, FILTER_BUY], default=FILTER_TODO,
            key="dp_actions_filter", label_visibility="collapsed",
        ) or FILTER_TODO
    with reasons_col:
        show_reasons = st.toggle("Show reasons", value=True, key="dp_actions_reasons")
    st.caption("Readiness is based on stock at plan time — re-check in-game before submitting.")

    char_groups: dict[str, list[dict[str, Any]]] = {}
    for action in char_actions:
        name = str(action.get("character_name") or action.get("character_id") or "Unknown")
        char_groups.setdefault(name, []).append(action)

    free = free_slots_by_character(plan_summary(plan_meta).get("free_slots"))
    names = sorted(char_groups)
    for start in range(0, len(names), 2):
        for col, char_name in zip(st.columns(2), names[start:start + 2]):
            with col:
                _render_character_section(char_name, char_groups[char_name], page_state, pending_buys,
                                          jobs=jobs, free_slots=free.get(char_name), mode=mode,
                                          show_reasons=show_reasons)
