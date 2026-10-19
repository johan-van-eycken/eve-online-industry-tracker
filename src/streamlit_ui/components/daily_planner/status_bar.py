from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import streamlit as st

from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA
from streamlit_ui.api.admin_settings import planner_page_defaults
from streamlit_ui.api.daily_planner import compute_plan, get_market_intel_status, get_plan, get_status
from streamlit_ui.state.daily_planner_page import DailyPlannerPageState

logger = logging.getLogger(__name__)

# The plan's expiry, the market data age shown as fresh and the freshness
# warning level are admin settings (daily_planner/planner_max_plan_age_hours,
# planner_market_refresh_interval_hours, planner_freshness_warning_level), read
# once per render by pages/daily_planner.py and passed in (render_status_bar).

#: Action types Tab 1 (tab_actions) renders, in workflow order. Defined here,
#: not in tab_actions, because tab_actions imports this module.
CHARACTER_ACTION_TYPES: tuple[str, ...] = (
    "deliver", "invent", "copy", "me_research", "te_research", "sub_manufacture", "manufacture",
)
#: Corp-level action types: shown in Tab 2 (shopping), never in Tab 1.
CORP_LEVEL_ACTION_TYPES: frozenset[str] = frozenset({"buy_materials", "buy_bpo"})


#: Action types that spend ISK on the market: the shopping list.
_CAPITAL_ACTION_TYPES: frozenset[str] = frozenset({"buy_materials", "buy_bpo"})
#: Action types that install a job and so pay an install fee (D4, A2).
_JOB_ACTION_TYPES: frozenset[str] = frozenset(
    {"manufacture", "sub_manufacture", "invent", "copy", "me_research", "te_research"}
)


CAPITAL_RESERVED_HELP = (
    "ISK the plan needs: pending purchases (materials and BPO purchases) plus the "
    "estimated install fee of every pending job (manufacturing, invention, copying, "
    "ME/TE research). Shown as unknown while a purchase has no price or a job's "
    "install fee could not be estimated."
)


@dataclass(frozen=True)
class CapitalReserved:
    """The plan's capital need. `total` is exact only when both counts are 0."""

    total: float
    unpriced_purchases: int
    unknown_fee_jobs: int
    #: (reason, job count) for the unknown fees whose cause was recorded,
    #: sorted by reason. The rest of unknown_fee_jobs has no recorded cause:
    #: an old plan or product overview, which a refresh / recompute fixes.
    unknown_fee_reasons: tuple[tuple[str, int], ...] = ()
    #: The known fees of pending jobs queued behind another job on the same
    #: blueprint (a TE research after its BPO's ME research): in `total`,
    #: since the fee is paid once the job starts, and named in the caption.
    queued_fee: float = 0.0
    queued_jobs: int = 0

    @property
    def stale_fee_jobs(self) -> int:
        return self.unknown_fee_jobs - sum(n for _, n in self.unknown_fee_reasons)


def compute_capital_reserved(actions: list[dict[str, Any]]) -> CapitalReserved:
    """ISK needed to execute the plan (D4): the pending shopping list plus the
    pending jobs' estimated install fees.

    manufacture / sub_manufacture / invent actions also carry the cost of the
    inputs the buy_materials rows purchase, so only their install fee is
    added; adding estimated_cost_isk would count every input twice. An
    unpriced purchase or an unknown fee is counted, never read as 0. A row
    without the fee field (a plan from before the column) counts as unknown.
    """
    total = 0.0
    unpriced_purchases = 0
    unknown_fee_jobs = 0
    queued_fee = 0.0
    queued_jobs = 0
    reasons: dict[str, int] = {}
    for action in actions:
        if str(action.get("status") or "pending") != "pending":
            continue
        action_type = str(action.get("action_type") or "")
        if action_type in _CAPITAL_ACTION_TYPES:
            cost = action.get("estimated_cost_isk")
            if cost is None:
                unpriced_purchases += 1
            else:
                total += float(cost)
        elif action_type in _JOB_ACTION_TYPES:
            fee = action.get("estimated_job_fee_isk")
            if fee is None:
                unknown_fee_jobs += 1
                reason = action.get("estimated_job_fee_unknown_reason")
                if isinstance(reason, str) and reason.strip():
                    reasons[reason] = reasons.get(reason, 0) + 1
            else:
                total += float(fee)
                if action.get("queued_after_me"):
                    queued_fee += float(fee)
                    queued_jobs += 1
    return CapitalReserved(
        total, unpriced_purchases, unknown_fee_jobs, tuple(sorted(reasons.items())),
        queued_fee=queued_fee, queued_jobs=queued_jobs,
    )


#: Appended to the caption when a job's fee is unknown with no recorded
#: cause: a plan or product overview from before the fee estimates (or
#: their reasons) carries none, which a refresh fixes. An unknown with a
#: recorded cause (a missing index, an unpriced material) names the cause
#: instead: a refresh would not fix it.
STALE_FEE_HINT = (
    " (if the plan or the product overview predates the fee estimates, "
    "refresh the product overview and recompute the plan)"
)


def capital_reserved_display(reserved: CapitalReserved) -> tuple[str, str | None]:
    """(metric value, caption or None) for the Capital Reserved metric. A2:
    one exact amount, or "unknown" with what is missing -- never a partial
    sum or a lower bound."""
    missing: list[str] = []
    if reserved.unpriced_purchases:
        missing.append(f"{reserved.unpriced_purchases} purchase(s) have no price")
    if reserved.unknown_fee_jobs:
        fee_part = f"install fee unknown for {reserved.unknown_fee_jobs} job(s)"
        if reserved.unknown_fee_reasons:
            fee_part += ": " + "; ".join(f"{reason} ({n} job(s))" for reason, n in reserved.unknown_fee_reasons)
        missing.append(fee_part)
    if missing:
        caption = "; ".join(missing)
        return "unknown", caption + (STALE_FEE_HINT if reserved.stale_fee_jobs > 0 else "")
    queued = (
        f"incl. {_fmt_isk(reserved.queued_fee)} for {reserved.queued_jobs} queued job(s)"
        if reserved.queued_jobs else None
    )
    return (_fmt_isk(reserved.total) if reserved.total else "—"), queued


#: ESI activity id of ME research.
from eve_online_industry_tracker.domain.industry_activities import ACTIVITY_ME_RESEARCH as ME_RESEARCH_ACTIVITY_ID  # noqa: E402


def queued_te_status(
    action: dict[str, Any], actions: list[dict[str, Any]], now: datetime | None = None,
    *, jobs: list[dict[str, Any]] | None = None,
) -> tuple[bool, str | None]:
    """(startable, note) for a TE research queued behind the ME research on
    its BPO (queued_after_me).

    Startable when the ME action on the same BPO (type_id) for the same
    pilot was skipped, or was marked started (done) and its job has ended.
    A pending ME was never started, so the TE waits (carry 14). The end is
    the later of the planner's estimate and the latest active ME-research
    job's end_date on that blueprint type, from `jobs` (the action rows do
    not name the BPO's item, so an earlier job may be a sibling BPO's and
    must not start it early; fix round 2). Without such a job only the
    estimate is used, and the note says so. With no such ME action
    nothing says it must wait. (False, None) for an action that is not
    queued."""
    if not action.get("queued_after_me"):
        return False, None
    now = now or datetime.now(tz=timezone.utc)
    me = next((
        a for a in actions
        if str(a.get("action_type") or "") == "me_research"
        and a.get("type_id") == action.get("type_id")
        and a.get("character_id") == action.get("character_id")
    ), None)
    me_status = str(me.get("status") or "pending") if me is not None else None
    if me is None or me_status == "skipped":
        return True, None
    if me_status != "done":
        return False, "the ME research on this BPO is not marked started yet"
    me_jobs = [
        j for j in jobs or []
        if isinstance(j, dict) and j.get("activity_id") == ME_RESEARCH_ACTIVITY_ID
        and j.get("blueprint_type_id") == action.get("type_id")
    ]
    me_end = _parse_dt(me.get("estimated_completion"))
    if me_jobs:
        ends = [_parse_dt(j.get("end_date")) for j in me_jobs]
        if any(end is None for end in ends):
            return False, "the active ME job's end time is unknown"
        job_end = max(e for e in ends if e is not None)
        if me_end is not None and me_end > job_end:
            return me_end <= now, (
                f"waits for the planner's estimate of the ME end ({me_end.strftime('%a %d %b %H:%M')} EVE): "
                "the ME job found on this blueprint type ends earlier and may be another BPO's"
            )
        return job_end <= now, f"ME job ends {job_end.strftime('%a %d %b %H:%M')} EVE (from the industry jobs)"
    why = "industry jobs not loaded" if jobs is None else "no active ME job found for this BPO"
    return me_end is not None and me_end <= now, f"{why}: using the planner's estimate of the ME end"


def queued_te_startable(
    action: dict[str, Any], actions: list[dict[str, Any]], now: datetime | None = None,
    *, jobs: list[dict[str, Any]] | None = None,
) -> bool:
    """queued_te_status's startable flag."""
    return queued_te_status(action, actions, now, jobs=jobs)[0]


def is_waiting(
    action: dict[str, Any], actions: list[dict[str, Any]], now: datetime | None = None,
    *, jobs: list[dict[str, Any]] | None = None,
) -> bool:
    """A queued action that cannot be started yet: not today's work."""
    return bool(action.get("queued_after_me")) and not queued_te_startable(action, actions, now, jobs=jobs)


def pending_character_action_count(
    actions: list[dict[str, Any]], now: datetime | None = None, *, jobs: list[dict[str, Any]] | None = None,
) -> int:
    """Pending actions a pilot still has to do. Rows of a type this planner
    no longer produces (e.g. a legacy relist_order) do not count."""
    return sum(
        1 for a in actions
        if str(a.get("status") or "pending") == "pending"
        and str(a.get("action_type") or "") in CHARACTER_ACTION_TYPES
        # A TE research queued behind its BPO's ME job cannot be done
        # until that job ends.
        and not is_waiting(a, actions, now, jobs=jobs)
    )


def load_active_jobs() -> list[dict[str, Any]] | None:
    """The active industry jobs for the queued-TE check, or None when they
    cannot be loaded (the check then uses the planner's estimate and says so)."""
    from streamlit_ui.api.industry_jobs import fetch_active_industry_jobs  # noqa: PLC0415

    try:
        jobs = fetch_active_industry_jobs().get("jobs")
    except Exception:  # noqa: BLE001 - UI read; the note names the fallback
        logger.warning("Daily planner: active industry jobs not loaded; queued TE uses the estimate", exc_info=True)
        return None
    return [j for j in jobs if isinstance(j, dict)] if isinstance(jobs, list) else None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_dt(s: Any) -> datetime | None:
    if not s:
        return None
    if isinstance(s, datetime):
        return s if s.tzinfo is not None else s.replace(tzinfo=timezone.utc)
    try:
        ts = str(s).rstrip("Z")
        if "." in ts:
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        try:
            parsed = datetime.fromisoformat(str(s))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _fmt_isk(v: float | None) -> str:
    if v is None:
        return "—"
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f}B ISK"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M ISK"
    if v >= 1_000:
        return f"{v / 1_000:.1f}K ISK"
    return f"{v:,.0f} ISK"


def wallet_snapshot(plan_meta: dict[str, Any]) -> float | None:
    """The plan's corp wallet snapshot, or None when it was unknown at compute time."""
    raw = plan_meta.get("corp_wallet_snapshot")
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    # nan/inf are not balances; "unknown" is the honest reading.
    return value if math.isfinite(value) else None


def _fmt_age(hours: float) -> str:
    if hours < 1.0:
        return f"{int(hours * 60)}m ago"
    if hours < 24.0:
        return f"{hours:.1f}h ago"
    return f"{hours / 24:.1f}d ago"


def freshness_summary(plan_data: dict[str, Any] | None,
                      warning_level: float | None = None) -> tuple[str, float | None]:
    """(label, score) for the Freshness metric; score is None when unknown.

    Reads the LIVE top-level `freshness_score` that get_active_plan recomputes
    on every GET, not `plan.freshness_score`: Phase 9 writes that one against
    the same market cache the plan was computed from, so it is always 1.0.
    A missing score is unknown, never 100%. So is a score with 0 comparable
    items: compute_freshness_stats returns 1.0 when nothing could be checked.
    """
    data = plan_data or {}
    raw = data.get("freshness_score")
    comparable = data.get("freshness_comparable_items")
    total = data.get("freshness_total_items")
    counts = (
        f"{int(comparable)}/{int(total)} items"
        if isinstance(comparable, (int, float)) and isinstance(total, (int, float))
        else None
    )
    if raw is None:
        return "unknown", None
    try:
        score = float(raw)
    except (TypeError, ValueError):
        return "unknown", None
    if comparable == 0:
        return f"unknown · {counts} comparable" if counts else "unknown", None

    if warning_level is None:   # daily_planner/planner_freshness_warning_level's schema default
        warning_level = float(ADMIN_SETTINGS_SCHEMA["daily_planner"]["settings"]
                              ["planner_freshness_warning_level"]["default"])
    pct = int(score * 100)
    if score >= warning_level:
        label = f"{pct}% (fresh)"
    elif score >= 0.75:
        label = f"{pct}% (mild drift)"
    else:
        label = f"{pct}% (stale)"
    return (f"{label} · {counts}" if counts else label), score


def unavailable_inputs_caption(plan_data: dict[str, Any] | None) -> str | None:
    """Caption naming the scoring inputs whose read failed in the last compute
    (RM-35), or None when every read succeeded. The plan was still scored;
    each reason says it was scored without that input."""
    unavailable = (plan_data or {}).get("unavailable_inputs") or {}
    if not isinstance(unavailable, dict) or not unavailable:
        return None
    # A compute-time snapshot: a later read may well succeed.
    return "When this plan was computed, some scoring inputs could not be read: " + "; ".join(
        str(reason) for reason in unavailable.values()
    )


def compute_failure_banner(status_data: dict[str, Any], *, has_plan: bool) -> str | None:
    """Text for the red banner shown while the last compute is failed, else None.

    Shown on every render while the backend reports "failed", not only on the
    poll that saw running -> failed: otherwise a page reload hides the failure
    and the user reads the previous plan as if it were today's.
    """
    if str(status_data.get("status") or "") != "failed":
        return None
    error = status_data.get("error") or "no error message was recorded (see the backend log)"
    message = f"Plan computation failed: {error}"
    if has_plan:
        message += "\n\nThe plan shown below is the previous one, not a fresh computation."
    return message


# ---------------------------------------------------------------------------
# Plan diff summary
# ---------------------------------------------------------------------------

def _render_plan_diff(
    prior_decisions: dict[str, str],
    new_items: list[dict[str, Any]],
) -> None:
    """Show a one-line summary of items that changed decision vs prior plan."""
    newly_build: list[str] = []
    moved_watch: list[str] = []
    moved_skip: list[str] = []

    for item in new_items:
        tid = str(item.get("type_id") or "")
        name = str(item.get("type_name") or tid)
        new_dec = str(item.get("decision") or "")
        old_dec = prior_decisions.get(tid)
        if old_dec is None or old_dec == new_dec:
            continue
        reason = item.get("decision_reason") or ""
        if new_dec == "build" and old_dec != "build":
            newly_build.append(name)
        elif new_dec == "watch":
            moved_watch.append(f"{name}: {reason}" if reason else name)
        elif new_dec == "skip":
            moved_skip.append(f"{name}: {reason}" if reason else name)

    parts: list[str] = []
    if newly_build:
        parts.append(f"{len(newly_build)} newly on build ({', '.join(newly_build[:3])}{'...' if len(newly_build) > 3 else ''})")
    if moved_watch:
        parts.append(f"{len(moved_watch)} moved to watch ({', '.join(moved_watch[:2])}{'...' if len(moved_watch) > 2 else ''})")
    if moved_skip:
        parts.append(f"{len(moved_skip)} moved to skip ({', '.join(moved_skip[:2])}{'...' if len(moved_skip) > 2 else ''})")

    if parts:
        st.success(f"Plan updated — {'; '.join(parts)}.")
    else:
        st.success("Plan updated — no decision changes vs prior plan.")


# ---------------------------------------------------------------------------
# Main render function
# ---------------------------------------------------------------------------

def render_status_bar(page_state: DailyPlannerPageState, planner_settings: dict[str, float] | None = None,
                      settings_problems: list[str] | tuple[str, ...] = ()) -> None:
    """Render the always-visible status bar at the top of the Daily Planner page.
    planner_settings/settings_problems: the page's one admin settings read
    (None: the schema defaults)."""

    # ------------------------------------------------------------------
    # 1. Poll compute status
    # ------------------------------------------------------------------
    try:
        status_data = get_status() or {}
    except Exception as exc:
        st.error(f"Failed to reach planner backend: {exc}")
        status_data = {}

    compute_status = str(status_data.get("status") or "idle")

    # ------------------------------------------------------------------
    # 2. Detect transition: was running, now done/failed
    # ------------------------------------------------------------------
    just_completed = (compute_status not in ("running",)) and (page_state.status == "running")
    if just_completed:
        page_state.status = "idle"
        page_state.recompute_confirmed = False
        # A failure is rendered by the persistent banner below, not here.
        if compute_status != "failed":
            # Reload plan and store prior decisions for diff
            try:
                new_data = get_plan() or {}
                if new_data.get("plan") is not None:
                    if page_state.plan is not None:
                        prior = {
                            str(item.get("type_id") or ""): str(item.get("decision") or "")
                            for item in (page_state.plan.get("items") or [])
                        }
                        st.session_state["_dp_prior_decisions"] = prior
                        st.session_state["_dp_show_diff"] = True
                    page_state.plan = new_data
            except Exception as exc:
                st.error(f"Failed to reload plan after compute: {exc}")

    # ------------------------------------------------------------------
    # 3. Initial plan load (first visit, or after hard refresh)
    # ------------------------------------------------------------------
    if page_state.plan is None and compute_status != "running":
        try:
            data = get_plan() or {}
            if data.get("plan") is not None:
                page_state.plan = data
        except Exception as exc:
            st.error(f"Failed to load plan: {exc}")

    # ------------------------------------------------------------------
    # 3b. Failed compute: red banner, on every render until the next run
    # ------------------------------------------------------------------
    failure_banner = compute_failure_banner(status_data, has_plan=page_state.plan is not None)
    if failure_banner is not None:
        st.error(failure_banner)

    # ------------------------------------------------------------------
    # 4. Computing state: show spinner and schedule re-poll
    # ------------------------------------------------------------------
    if compute_status == "running":
        page_state.status = "running"
        bar_col, btn_col = st.columns([8, 2])
        with bar_col:
            st.markdown("**Computing plan...** This may take 6–12 seconds.")
        with btn_col:
            st.button("Recompute Plan", disabled=True, key="_recompute_during_compute")
        with st.spinner("Running plan computation..."):
            time.sleep(1)
        st.rerun()
        return

    # ------------------------------------------------------------------
    # 5. Onboarding / warmup banner
    # ------------------------------------------------------------------
    if page_state.plan is not None:
        items = page_state.plan.get("items") or []
        if items:
            max_sample = max((int(i.get("sample_count") or 0) for i in items), default=0)
            if max_sample < 5:
                st.info(
                    "The planner is in warmup mode — all items start with default weights. "
                    "Scores improve as manufacturing batches complete and sell. "
                    "Expect meaningful learning after 5+ completed batches per item."
                )

    # ------------------------------------------------------------------
    # 6. Plan diff banner (after recompute)
    # ------------------------------------------------------------------
    if st.session_state.get("_dp_show_diff") and page_state.plan is not None:
        prior = st.session_state.get("_dp_prior_decisions") or {}
        new_items = page_state.plan.get("items") or []
        _render_plan_diff(prior, new_items)
        if st.button("Dismiss", key="_dp_dismiss_diff"):
            st.session_state["_dp_show_diff"] = False
            st.rerun()

    # ------------------------------------------------------------------
    # 7. Plan metrics
    # ------------------------------------------------------------------
    plan_meta: dict[str, Any] = {}
    if page_state.plan:
        plan_meta = page_state.plan.get("plan") or {}

    if planner_settings is None:
        planner_settings = planner_page_defaults()
    for problem in settings_problems:
        st.warning(problem)

    # Live freshness (top level of the GET payload), None when unknown.
    freshness_display, freshness_score = freshness_summary(
        page_state.plan, warning_level=planner_settings["planner_freshness_warning_level"])
    corp_wallet = wallet_snapshot(plan_meta)
    created_at = plan_meta.get("created_at")

    plan_age_hours: float | None = None
    plan_expired = False
    if created_at:
        created_dt = _parse_dt(created_at)
        if created_dt:
            now_utc = datetime.now(tz=timezone.utc)
            plan_age_hours = (now_utc - created_dt).total_seconds() / 3600
            plan_expired = plan_age_hours > planner_settings["planner_max_plan_age_hours"]

    # Expired banner
    if plan_expired and plan_age_hours is not None:
        days = plan_age_hours / 24
        st.error(
            f"Plan is {days:.1f} day(s) old and actions are no longer valid. "
            "Recompute to generate today's actions.\n\n"
            "Jobs that completed while you were away have been tracked automatically. "
            "Recomputing will plan from your current slot availability, stock, and market state."
        )

    # Metrics row
    if plan_meta:
        c1, c2, c3, c4, c5 = st.columns(5)

        with c1:
            st.metric("Freshness", freshness_display)
        with c2:
            age_str = _fmt_age(plan_age_hours) if plan_age_hours is not None else "—"
            st.metric("Last Computed", age_str)
        with c3:
            st.metric("Corp Wallet", _fmt_isk(corp_wallet))
        with c4:
            actions = (page_state.plan or {}).get("actions") or []
            value, caption = capital_reserved_display(compute_capital_reserved(actions))
            st.metric("Capital Reserved", value, help=CAPITAL_RESERVED_HELP)
            if caption:
                st.caption(caption)
        with c5:
            try:
                market_status = get_market_intel_status() or {}
            except Exception:
                market_status = {}
            last_completed = market_status.get("last_completed_at")
            market_dt = _parse_dt(last_completed)
            if market_dt is None:
                market_label = "—"
            else:
                now_utc = datetime.now(tz=timezone.utc)
                market_age_hours = (now_utc - market_dt).total_seconds() / 3600
                age_text = _fmt_age(market_age_hours)
                if market_age_hours < planner_settings["planner_market_refresh_interval_hours"]:
                    market_label = f"{age_text} ✓"
                elif market_age_hours < 6.0:
                    market_label = f"{age_text} ⚠"
                else:
                    market_label = f"{age_text} 🔴"
            st.metric("Market Data", market_label)

    unavailable_caption = unavailable_inputs_caption(page_state.plan)
    if unavailable_caption:
        st.caption(unavailable_caption)

    # Freshness warning
    if page_state.plan and freshness_score is not None and freshness_score < 0.75:
        st.warning("Market prices have drifted significantly — consider recomputing.")
    elif (page_state.plan and freshness_score is not None
          and freshness_score < planner_settings["planner_freshness_warning_level"]):
        st.caption("Some prices have drifted slightly — recomputing is recommended.")

    # ------------------------------------------------------------------
    # 8. Recompute button (with confirmation when pending actions exist)
    # ------------------------------------------------------------------
    actions = (page_state.plan or {}).get("actions") or []
    pending_char_actions = pending_character_action_count(
        actions, jobs=load_active_jobs() if actions else None,
    )

    stale = freshness_score is not None and freshness_score < 0.75
    button_type = "primary" if (plan_expired or stale or page_state.plan is None) else "secondary"

    if st.session_state.get("_dp_confirm_recompute") and pending_char_actions > 0:
        # Confirmation dialog
        st.warning(
            f"You have {pending_char_actions} action(s) not yet marked done. "
            "Recomputing will replace today's action list. Continue?"
        )
        col_cancel, col_confirm = st.columns([1, 1])
        with col_cancel:
            if st.button("Cancel", key="_recompute_cancel"):
                st.session_state["_dp_confirm_recompute"] = False
                st.rerun()
        with col_confirm:
            if st.button("Recompute", type="primary", key="_recompute_confirmed"):
                st.session_state["_dp_confirm_recompute"] = False
                _trigger_compute(page_state)
    else:
        if st.button("Recompute Plan", type=button_type, key="_recompute_plan_btn"):
            if pending_char_actions > 0 and not plan_expired:
                st.session_state["_dp_confirm_recompute"] = True
                st.rerun()
            else:
                _trigger_compute(page_state)

    st.markdown("---")


def _trigger_compute(page_state: DailyPlannerPageState) -> None:
    try:
        compute_plan()
        page_state.status = "running"
        st.rerun()
    except Exception as exc:
        st.error(f"Failed to trigger recompute: {exc}")
