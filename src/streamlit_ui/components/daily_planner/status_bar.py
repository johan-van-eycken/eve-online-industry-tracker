from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import streamlit as st

from streamlit_ui.api.daily_planner import compute_plan, get_market_intel_status, get_plan, get_status
from streamlit_ui.state.daily_planner_page import DailyPlannerPageState

# Default from spec: plan expires after 36 hours
PLANNER_MAX_PLAN_AGE_HOURS: float = 36.0

# Market intel refresh interval: green if fresher than this
PLANNER_MARKET_REFRESH_INTERVAL_HOURS: float = 3.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_dt(s: Any) -> datetime | None:
    if not s:
        return None
    try:
        ts = str(s).rstrip("Z")
        if "." in ts:
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


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


def _fmt_age(hours: float) -> str:
    if hours < 1.0:
        return f"{int(hours * 60)}m ago"
    if hours < 24.0:
        return f"{hours:.1f}h ago"
    return f"{hours / 24:.1f}d ago"


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

def render_status_bar(page_state: DailyPlannerPageState) -> None:
    """Render the always-visible status bar at the top of the Daily Planner page."""

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

    freshness_score = float(plan_meta.get("freshness_score") or 1.0)
    corp_wallet = float(plan_meta.get("corp_wallet_snapshot") or 0.0)
    created_at = plan_meta.get("created_at")

    plan_age_hours: float | None = None
    plan_expired = False
    if created_at:
        created_dt = _parse_dt(created_at)
        if created_dt:
            now_utc = datetime.now(tz=timezone.utc)
            plan_age_hours = (now_utc - created_dt).total_seconds() / 3600
            plan_expired = plan_age_hours > PLANNER_MAX_PLAN_AGE_HOURS

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

        freshness_pct = int(freshness_score * 100)
        if freshness_score >= 0.90:
            freshness_display = f"{freshness_pct}% (fresh)"
        elif freshness_score >= 0.75:
            freshness_display = f"{freshness_pct}% (mild drift)"
        else:
            freshness_display = f"{freshness_pct}% (stale)"

        with c1:
            st.metric("Freshness", freshness_display)
        with c2:
            age_str = _fmt_age(plan_age_hours) if plan_age_hours is not None else "—"
            st.metric("Last Computed", age_str)
        with c3:
            st.metric("Corp Wallet", _fmt_isk(corp_wallet))
        with c4:
            # Capital reserved: sum of estimated_cost_isk for pending buy/job actions
            actions = (page_state.plan or {}).get("actions") or []
            capital_reserved = sum(
                float(a.get("estimated_cost_isk") or 0.0)
                for a in actions
                if str(a.get("status") or "pending") == "pending"
                and str(a.get("action_type") or "") in (
                    "buy_materials", "buy_bpo", "manufacture", "sub_manufacture"
                )
            )
            st.metric("Capital Reserved", _fmt_isk(capital_reserved) if capital_reserved else "—")
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
                if market_age_hours < PLANNER_MARKET_REFRESH_INTERVAL_HOURS:
                    market_label = f"{age_text} ✓"
                elif market_age_hours < 6.0:
                    market_label = f"{age_text} ⚠"
                else:
                    market_label = f"{age_text} 🔴"
            st.metric("Market Data", market_label)

    # Freshness warning
    if page_state.plan and freshness_score < 0.75:
        st.warning("Market prices have drifted significantly — consider recomputing.")
    elif page_state.plan and freshness_score < 0.90:
        st.caption("Some prices have drifted slightly — recomputing is recommended.")

    # ------------------------------------------------------------------
    # 8. Recompute button (with confirmation when pending actions exist)
    # ------------------------------------------------------------------
    actions = (page_state.plan or {}).get("actions") or []
    pending_char_actions = sum(
        1 for a in actions
        if str(a.get("status") or "pending") == "pending"
        and str(a.get("action_type") or "") not in ("buy_materials", "buy_bpo")
    )

    button_type = "primary" if (plan_expired or freshness_score < 0.75 or page_state.plan is None) else "secondary"

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
