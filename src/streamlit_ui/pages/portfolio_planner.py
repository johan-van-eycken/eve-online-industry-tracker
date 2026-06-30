from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, cast

import pandas as pd
import streamlit as st

from streamlit_ui.api.industry_builder import (
    clear_industry_builder_caches,
    fetch_blueprint_skill_qualification,
    fetch_reorder_alerts,
    start_product_overview_refresh,
)
from streamlit_ui.api.industry_jobs import fetch_active_industry_jobs
from streamlit_ui.api.industry_profiles import build_industry_profile_options
from streamlit_ui.state.industry_builder_page import (
    fetch_industry_profiles_cached,
    overview_refresh_is_active,
    start_overview_refresh_job,
)
from streamlit_ui.state.industry_builder_ui import filter_overview_rows
from streamlit_ui.shopping_list import aggregate_shopping_list
from streamlit_ui.state.industry_snapshot_page import (
    _refresh_status_fragment,
    load_character_context,
)
from streamlit_ui.eve_constants import ACTIVITY_LAB_IDS, ACTIVITY_REACTION_IDS

_ALL_META_GROUPS = {"Tech I", "Tech II", "Tech III", "Faction", "Storyline", "Other"}
_DEFAULT_META_GROUPS_ON = {"Tech I", "Tech II", "Faction", "Storyline", "Other"}

_HARD_DISQUALIFIERS = [
    (lambda r: bool(r.get("blueprint_sde_fallback")), "No owned blueprint (SDE fallback)"),
    (lambda r: str(r.get("price_anomaly_risk") or "").strip() == "High", "High price anomaly risk"),
    (lambda r: str(r.get("pricing_confidence") or "").strip().lower() == "low", "Low pricing confidence"),
    (lambda r: float(r.get("profit_amount") or 0.0) <= 0.0, "Unprofitable"),
]

_LIQUIDITY_SCORES: dict[str, float] = {
    "Very High": 1.0,
    "High": 0.8,
    "Medium": 0.6,
    "Low": 0.3,
    "Very Low": 0.0,
    "Unknown": 0.0,
}

_CONFIDENCE_SCORES: dict[str, float] = {
    "High": 1.0,
    "Medium": 0.5,
    "Low": 0.0,
}


def _row_blueprint_type_id(row: dict) -> int:
    """Extract blueprint_type_id from an overview/candidate row via the manufacturing_job.blueprint_sde path."""
    return int(((row.get("manufacturing_job") or {}).get("blueprint_sde") or {}).get("blueprint_type_id") or 0)


# ---------------------------------------------------------------------------
# Data acquisition
# ---------------------------------------------------------------------------

def _get_overview_rows() -> list[dict[str, Any]]:
    rows = st.session_state.get("industry_builder_overview_rows")
    return rows if isinstance(rows, list) else []


def _read_industry_builder_filter_state() -> dict[str, Any]:
    enabled_meta_groups: set[str] = set()
    for name in _ALL_META_GROUPS:
        form_key = f"form_meta_{name}"
        default = name in _DEFAULT_META_GROUPS_ON
        if bool(st.session_state.get(form_key, default)):
            enabled_meta_groups.add(name)

    return {
        "enabled_meta_groups": enabled_meta_groups or _DEFAULT_META_GROUPS_ON,
        "have_skills_only": bool(st.session_state.get("industry_builder_have_skills_only", True)),
        "positive_profit_only": bool(st.session_state.get("industry_builder_positive_profit_only", True)),
        "min_margin_pct": float(st.session_state.get("industry_builder_min_margin_pct") or 0.0),
        "min_isk_per_hour": float(st.session_state.get("industry_builder_min_isk_per_hour") or 0.0),
        "min_region_daily_volume": int(st.session_state.get("industry_builder_min_region_daily_volume") or 0),
        "excluded_liquidity_indicators": list(st.session_state.get("industry_builder_liquidity_exclude") or ["Very Low", "Unknown"]),
        "excluded_anomaly_risks": list(st.session_state.get("industry_builder_anomaly_exclude") or ["High"]),
    }


# ---------------------------------------------------------------------------
# Hard disqualifiers
# ---------------------------------------------------------------------------

def _apply_hard_disqualifiers(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], str]]]:
    eligible: list[dict[str, Any]] = []
    disqualified: list[tuple[dict[str, Any], str]] = []
    for row in rows:
        reason: str | None = None
        for predicate, message in _HARD_DISQUALIFIERS:
            if predicate(row):
                reason = message
                break
        if reason is not None:
            disqualified.append((row, reason))
        else:
            eligible.append(row)
    return eligible, disqualified


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _compute_normalization_context(rows: list[dict[str, Any]]) -> dict[str, float]:
    def safe_float(v: Any) -> float:
        try:
            return float(v or 0.0)
        except (TypeError, ValueError):
            return 0.0

    isk_vals = [safe_float(r.get("isk_per_hour")) for r in rows]
    profit_vals = [safe_float(r.get("profit_amount")) for r in rows]
    roc_vals = [min(safe_float(r.get("return_on_capital")), 5.0) for r in rows]
    return {
        "isk_min": min(isk_vals, default=0.0),
        "isk_max": max(isk_vals, default=1.0),
        "profit_min": min(profit_vals, default=0.0),
        "profit_max": max(profit_vals, default=1.0),
        "roc_min": min(roc_vals, default=0.0),
        "roc_max": max(roc_vals, default=1.0),
    }


def _normalize(value: float, lo: float, hi: float) -> float:
    span = hi - lo
    if span <= 0.0:
        return 0.0
    return max(0.0, min(1.0, (value - lo) / span))


def _score_row(row: dict[str, Any], ctx: dict[str, float]) -> float:
    def sf(v: Any) -> float:
        try:
            return float(v or 0.0)
        except (TypeError, ValueError):
            return 0.0

    isk_score = _normalize(sf(row.get("isk_per_hour")), ctx["isk_min"], ctx["isk_max"])
    roc_score = _normalize(min(sf(row.get("return_on_capital")), 5.0), ctx["roc_min"], ctx["roc_max"])
    profit_score = _normalize(sf(row.get("profit_amount")), ctx["profit_min"], ctx["profit_max"])
    liq_score = _LIQUIDITY_SCORES.get(str(row.get("liquidity_indicator") or "Unknown").strip(), 0.0)
    conf_score = _CONFIDENCE_SCORES.get(str(row.get("pricing_confidence") or "Low").strip().title(), 0.0)

    composite = (
        0.30 * isk_score
        + 0.25 * roc_score
        + 0.20 * profit_score
        + 0.15 * liq_score
        + 0.10 * conf_score
    )

    anomaly = str(row.get("price_anomaly_risk") or "None").strip()
    if anomaly == "Medium":
        composite *= 0.80
    elif anomaly == "Low":
        composite *= 0.95

    if bool(row.get("fragile_margin")):
        composite *= 0.85
    if bool(row.get("material_contention")):
        composite *= 0.90
    if row.get("manufacture_window_ok") is False:
        composite *= 0.90

    me = row.get("blueprint_me")
    if me is not None:
        try:
            if int(me) < 5:
                composite *= 0.95
        except (TypeError, ValueError):
            pass

    return round(composite * 100.0, 1)


SCORING_OBJECTIVES = ["Balanced", "Max ISK/hr", "Capital Turnover"]


def _rank_candidates(
    rows: list[dict[str, Any]],
    objective: str = "Balanced",
) -> list[tuple[dict[str, Any], float]]:
    if not rows:
        return []
    if objective == "Max ISK/hr":
        def _isk_key(row: dict) -> float:
            try:
                return float(row.get("isk_per_hour") or 0.0)
            except (TypeError, ValueError):
                return 0.0
        scored = [(row, round(_isk_key(row) / 1_000_000, 1)) for row in rows]
        scored.sort(key=lambda pair: pair[1], reverse=True)
        return scored
    if objective == "Capital Turnover":
        def _ct_key(row: dict) -> float:
            try:
                return float(row.get("isk_per_cycle_day") or 0.0)
            except (TypeError, ValueError):
                return 0.0
        scored = [(row, round(_ct_key(row) / 1_000_000, 1)) for row in rows]
        scored.sort(key=lambda pair: pair[1], reverse=True)
        return scored
    # Default: Balanced composite
    ctx = _compute_normalization_context(rows)
    scored = [(row, _score_row(row, ctx)) for row in rows]
    scored.sort(key=lambda pair: pair[1], reverse=True)
    return scored


# ---------------------------------------------------------------------------
# Explanation generation
# ---------------------------------------------------------------------------

def _good_signals(row: dict[str, Any], score: float) -> list[str]:
    signals: list[str] = []

    def sf(v: Any) -> float:
        try:
            return float(v or 0.0)
        except (TypeError, ValueError):
            return 0.0

    isk_per_hour = sf(row.get("isk_per_hour"))
    roc = sf(row.get("return_on_capital"))
    margin = sf(row.get("profit_margin_fraction"))
    liquidity = str(row.get("liquidity_indicator") or "Unknown").strip()
    anomaly = str(row.get("price_anomaly_risk") or "None").strip()
    confidence = str(row.get("pricing_confidence") or "Low").strip().title()
    dos = row.get("days_of_supply")
    manufacture_window_ok = row.get("manufacture_window_ok")
    me = row.get("blueprint_me")

    if isk_per_hour >= 100_000_000:
        signals.append(f"Very high ISK/hr ({isk_per_hour / 1_000_000:.0f}M/hr)")
    elif isk_per_hour >= 50_000_000:
        signals.append(f"Strong ISK/hr ({isk_per_hour / 1_000_000:.0f}M/hr)")

    if roc >= 2.0:
        signals.append(f"Extraordinary ROC ({roc * 100:.0f}%) — minimal capital required")
    elif roc >= 0.5:
        signals.append(f"Strong ROC ({roc * 100:.0f}%)")

    if liquidity in ("Very High", "High"):
        dos_suffix = f" ({dos:.1f}d)" if dos is not None else ""
        signals.append(f"{liquidity} liquidity{dos_suffix}")

    if margin >= 0.25 and not bool(row.get("fragile_margin")):
        signals.append(f"Strong margin buffer ({margin * 100:.1f}%)")

    if manufacture_window_ok is True:
        signals.append("Manufacture window safe")

    if anomaly in ("None", ""):
        signals.append("No price anomaly")

    if confidence == "High":
        signals.append("High pricing confidence")

    if me is not None:
        try:
            if int(me) == 10:
                signals.append("ME10 blueprint")
        except (TypeError, ValueError):
            pass

    return signals


def _warnings(row: dict[str, Any]) -> list[str]:
    warns: list[str] = []

    def sf(v: Any) -> float:
        try:
            return float(v or 0.0)
        except (TypeError, ValueError):
            return 0.0

    anomaly = str(row.get("price_anomaly_risk") or "None").strip()
    liquidity = str(row.get("liquidity_indicator") or "Unknown").strip()
    manufacture_window_ok = row.get("manufacture_window_ok")
    me = row.get("blueprint_me")
    prep_pct = row.get("prep_time_fraction_pct")
    margin = sf(row.get("profit_margin_fraction"))

    if anomaly == "Medium":
        reasons = row.get("price_anomaly_reasons")
        detail = ""
        if isinstance(reasons, list) and reasons:
            detail = f" ({reasons[0]})"
        warns.append(f"Medium price anomaly risk{detail}")

    if bool(row.get("fragile_margin")):
        warns.append(f"Fragile margin ({margin * 100:.1f}%) — one undercut wipes the profit")

    if liquidity in ("Low", "Very Low", "Unknown"):
        warns.append(f"{liquidity} liquidity — may be slow to sell")

    if bool(row.get("material_contention")):
        warns.append("Material contention — owned stock may be allocated to a higher-ranked product first")

    if manufacture_window_ok is False:
        warns.append("Manufacture window at risk — market may restock before build completes")

    if me is not None:
        try:
            if int(me) < 5:
                warns.append(f"Low ME ({int(me)}) — higher material cost than ME10")
        except (TypeError, ValueError):
            pass

    if prep_pct is not None:
        try:
            if float(prep_pct) >= 50.0:
                warns.append(f"High prep time ({float(prep_pct):.0f}%) — most slot time is overhead, not production")
        except (TypeError, ValueError):
            pass

    return warns


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _render_no_snapshot_prompt() -> None:
    st.info(
        "No manufacturing data loaded yet. "
        "Go to Industry Builder in the sidebar and click Refresh Overview first. "
        "The recommendations will appear here automatically once loaded."
    )


def _render_header_banner(
    all_rows: list[dict[str, Any]],
    filtered_rows: list[dict[str, Any]],
    eligible_rows: list[dict[str, Any]],
    overview_meta: dict[str, Any],
) -> None:
    if overview_refresh_is_active():
        _refresh_status_fragment()
        return

    pricing_batch = cast(dict[str, Any], overview_meta.get("pricing_batch") or overview_meta)
    generated_at = str(pricing_batch.get("generated_at") or "")
    hub = str(
        pricing_batch.get("market_hub_label")
        or pricing_batch.get("market_hub")
        or "N/A"
    )

    banner_col, refresh_col = st.columns([9, 1])
    with banner_col:
        st.caption(
            f"{len(all_rows)} products in snapshot · "
            f"Hub: {hub} · "
            f"{len(filtered_rows)} pass Industry Builder filters · "
            f"{len(eligible_rows)} eligible for scoring"
        )
        st.caption("Filter settings are inherited from your Industry Builder configuration. Change them there and Refresh Overview to update.")
    with refresh_col:
        if st.button(
            "Refresh Snapshot",
            key="portfolio_planner_refresh_snapshot",
            disabled=overview_refresh_is_active(),
            use_container_width=True,
        ):
            try:
                (
                    _characters,
                    character_options,
                    default_character_id_value,
                    owned_blueprint_scope_options,
                    _owned_blueprint_scope_labels,
                    default_owned_blueprint_scope,
                ) = load_character_context()
                selected_char_id = int(
                    st.session_state.get("industry_builder_character_id", default_character_id_value)
                )
                industry_profiles = fetch_industry_profiles_cached(character_id=selected_char_id)
                _profile_options, _profile_labels, default_profile_id = build_industry_profile_options(
                    cast(list[dict[str, Any]], industry_profiles)
                )
                clear_industry_builder_caches()
                st.session_state["industry_builder_overview_rows"] = []
                st.session_state["industry_builder_overview_meta"] = {}
                start_overview_refresh_job(
                    default_character_id_value=default_character_id_value,
                    default_industry_profile_id=default_profile_id,
                    default_owned_blueprint_scope=default_owned_blueprint_scope,
                    reactions_allowed_for_profile=True,
                    start_refresh_fn=start_product_overview_refresh,
                )
            except Exception as exc:
                st.error(f"Failed to start snapshot refresh: {exc}")
                return
            st.rerun()


_LAB_ACTIVITY_IDS = ACTIVITY_LAB_IDS
_REACTION_ACTIVITY_IDS = ACTIVITY_REACTION_IDS


def _fetch_jobs_data() -> dict[str, Any]:
    """Fetch active industry jobs; returns empty structure on error."""
    try:
        return fetch_active_industry_jobs()
    except Exception:
        return {"jobs": [], "slot_capacities": {}}


def _parse_end_date(end_date: Any) -> datetime | None:
    """Parse an ISO-formatted end_date string into a UTC datetime, or None."""
    if not end_date:
        return None
    try:
        s = str(end_date).rstrip("Z")
        if "." in s:
            dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f")
        else:
            dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _render_slot_summary_header(jobs_data: dict[str, Any]) -> None:
    """Render a compact per-character slot summary at the top of the page."""
    jobs: list[dict[str, Any]] = jobs_data.get("jobs") or []
    slot_capacities: dict[str, Any] = jobs_data.get("slot_capacities") or {}

    if not slot_capacities:
        return

    now_utc = datetime.now(tz=timezone.utc)
    soon_cutoff = now_utc + timedelta(hours=24)

    # Collect all character IDs present in capacities
    char_ids = list(slot_capacities.keys())

    # Build per-character job counts and upcoming job data
    char_used_mfg: dict[str, int] = {c: 0 for c in char_ids}
    char_used_lab: dict[str, int] = {c: 0 for c in char_ids}
    char_used_reaction: dict[str, int] = {c: 0 for c in char_ids}
    char_upcoming: dict[str, list[datetime]] = {c: [] for c in char_ids}

    for job in jobs:
        cid_key = str(job.get("character_id") or "")
        if cid_key not in slot_capacities:
            continue
        activity_id = job.get("activity_id")
        if activity_id == 1:
            char_used_mfg[cid_key] += 1
        elif activity_id in _LAB_ACTIVITY_IDS:
            char_used_lab[cid_key] += 1
        elif activity_id in _REACTION_ACTIVITY_IDS:
            char_used_reaction[cid_key] += 1

        end_dt = _parse_end_date(job.get("end_date"))
        if end_dt is not None and now_utc <= end_dt <= soon_cutoff:
            char_upcoming[cid_key].append(end_dt)

    # Resolve character names from the jobs list (best-effort)
    char_name_map: dict[str, str] = {}
    for job in jobs:
        cid_key = str(job.get("character_id") or "")
        if cid_key and cid_key not in char_name_map:
            name = job.get("character_name") or cid_key
            char_name_map[cid_key] = str(name)

    st.markdown("#### Industry Slots")
    cols = st.columns(max(len(char_ids), 1))
    for idx, cid_key in enumerate(char_ids):
        caps = slot_capacities.get(cid_key) or {}
        mfg_max = int(caps.get("manufacturing_max") or 0)
        lab_max = int(caps.get("research_max") or 0)
        used_mfg = char_used_mfg.get(cid_key, 0)
        used_lab = char_used_lab.get(cid_key, 0)
        free_mfg = max(mfg_max - used_mfg, 0)
        free_lab = max(lab_max - used_lab, 0)

        name = char_name_map.get(cid_key) or cid_key

        with cols[idx]:
            st.markdown(f"**{name}**")

            # Manufacturing slots
            st.caption(f"Manufacturing: {used_mfg}/{mfg_max} used, {free_mfg} free")
            if mfg_max > 0:
                st.progress(min(used_mfg / mfg_max, 1.0))

            # Lab slots
            st.caption(f"Lab: {used_lab}/{lab_max} used, {free_lab} free")
            if lab_max > 0:
                st.progress(min(used_lab / lab_max, 1.0))

            # Reaction slots (only shown when character has reaction capacity)
            react_max = int(caps.get("reaction_max") or 0)
            if react_max > 0:
                used_react = char_used_reaction.get(cid_key, 0)
                free_react = max(react_max - used_react, 0)
                st.caption(f"Reactions: {used_react}/{react_max} used, {free_react} free")
                st.progress(min(used_react / react_max, 1.0))

            # Upcoming completions within 24h
            upcoming = sorted(char_upcoming.get(cid_key, []))
            if upcoming:
                soonest = upcoming[0]
                delta = soonest - now_utc
                total_mins = max(int(delta.total_seconds() / 60), 0)
                hours_left = total_mins // 60
                mins_left = total_mins % 60
                st.caption(f"⏰ +{len(upcoming)} in {hours_left}h {mins_left}m")

    st.markdown("---")


def _build_active_type_ids(jobs_data: dict[str, Any]) -> set[int]:
    """Return the set of product_type_ids currently being built."""
    jobs: list[dict[str, Any]] = jobs_data.get("jobs") or []
    result: set[int] = set()
    for job in jobs:
        pid = job.get("product_type_id")
        if pid:
            try:
                result.add(int(pid))
            except (TypeError, ValueError):
                pass
    return result


def _bpc_label(row: dict) -> str:
    status = row.get("bpc_status")
    count = int(row.get("bpc_count") or 0)
    if status == "bpo":        return "— BPO"
    if status == "stocked":    return f"✅ {count}"
    if status == "low_with_bpo": return f"⚠️ {count}"
    if status == "low":        return f"⚠️ {count}"
    if status == "needed":     return "🔴 Copy"
    if status == "invent":     return "🔴 Invent"
    return "?"


def _compute_character_assignment(
    candidates: list[dict],
    skill_qual: dict[str, dict[str, bool]],
    jobs_data: dict,
) -> dict[str, str]:
    """
    Returns {overview_row_id: character_name} assignment for each candidate.

    Algorithm:
    1. For each candidate, look up blueprint_type_id in skill_qual.
    2. Qualified characters = those with skill_qual[bp_type_id][char_id] == True.
    3. If no one qualifies -> "?" (unassignable).
    4. Among qualified: pick the one with most free manufacturing slots.
    5. Tiebreak: more active jobs of same manufacturing_group category.
    6. Tiebreak: lowest character_id (deterministic).
    """
    jobs: list[dict[str, Any]] = jobs_data.get("jobs") or []
    slot_capacities: dict[str, Any] = jobs_data.get("slot_capacities") or {}

    # Compute free manufacturing slots per character
    char_used_mfg: dict[str, int] = {c: 0 for c in slot_capacities}
    for job in jobs:
        cid_key = str(job.get("character_id") or "")
        if cid_key in slot_capacities and job.get("activity_id") == 1:
            char_used_mfg[cid_key] += 1

    def free_mfg_slots(cid_str: str) -> int:
        caps = slot_capacities.get(cid_str) or {}
        mfg_max = int(caps.get("manufacturing_max") or 0)
        used = char_used_mfg.get(cid_str, 0)
        return max(0, mfg_max - used)

    # Compute active mfg jobs per character (activity_id == 1 only)
    char_active_mfg: dict[str, int] = {}
    for job in jobs:
        if job.get("activity_id") != 1:
            continue
        cid_key = str(job.get("character_id") or "")
        char_active_mfg[cid_key] = char_active_mfg.get(cid_key, 0) + 1

    # Resolve character names from jobs
    char_name_map: dict[str, str] = {}
    for job in jobs:
        cid_key = str(job.get("character_id") or "")
        if cid_key and cid_key not in char_name_map:
            char_name_map[cid_key] = str(job.get("character_name") or cid_key)

    assignment: dict[str, str] = {}
    for candidate in candidates:
        overview_row_id = str(candidate.get("overview_row_id") or "")
        if not overview_row_id:
            continue

        # Determine blueprint_type_id from the row
        bp_type_id: int = 0
        try:
            bp_type_id = _row_blueprint_type_id(candidate)
        except (TypeError, ValueError):
            bp_type_id = 0

        bp_type_id_str = str(bp_type_id)
        char_quals = skill_qual.get(bp_type_id_str) or {}

        qualified_char_ids = [
            cid_str
            for cid_str, qualifies in char_quals.items()
            if qualifies
        ]

        if not qualified_char_ids:
            assignment[overview_row_id] = "?"
            continue

        def sort_key(cid_str: str) -> tuple:
            free = free_mfg_slots(cid_str)
            active_mfg = char_active_mfg.get(cid_str, 0)
            try:
                cid_int = int(cid_str)
            except (TypeError, ValueError):
                cid_int = 0
            # More free slots is better (desc), more active mfg jobs is better (desc),
            # lower char_id is tiebreaker (asc)
            return (-free, -active_mfg, cid_int)

        best_char_id = min(qualified_char_ids, key=sort_key)
        assignment[overview_row_id] = char_name_map.get(best_char_id) or best_char_id

    return assignment


def _reorder_tag(row: dict, reorder_alerts: dict) -> str:
    type_id = str(int(row.get("type_id") or 0))
    alert = reorder_alerts.get(type_id) or {}
    urgency = alert.get("urgency")
    if urgency == "urgent":  return "🔴 Restock now"
    if urgency == "soon":    return "🟡 Restock soon"
    if urgency == "ok":      return "🟢 OK"
    return ""   # no_data or missing → don't show


_SCORE_COLUMN_LABELS: dict[str, str] = {
    "Balanced": "Score",
    "Max ISK/hr": "ISK/Hr (M)",
    "Capital Turnover": "Cycle ISK/d (M)",
}


def _render_recommendations_table(ranked: list[tuple[dict[str, Any], float]], active_type_ids: set[int] | None = None, assignment: dict[str, str] | None = None, reorder_alerts: dict | None = None, objective: str = "Balanced") -> None:
    if not ranked:
        st.info("No eligible products to recommend with current filters and disqualification rules.")
        return

    st.markdown(f"### Top 15 Manufacturing Recommendations — sorted by {objective}")

    def sf(v: Any) -> float:
        try:
            return float(v or 0.0)
        except (TypeError, ValueError):
            return 0.0

    score_col_label = _SCORE_COLUMN_LABELS.get(objective, "Score")
    table_rows = []
    for rank, (row, score) in enumerate(ranked, start=1):
        warns = _warnings(row)
        warning_str = "; ".join(f"⚠ {w}" for w in warns) if warns else ""
        dos = row.get("days_of_supply")

        # Check if this product is currently being built
        is_active = False
        if active_type_ids:
            prod_type_id = row.get("product_type_id") or row.get("type_id")
            if prod_type_id:
                try:
                    is_active = int(prod_type_id) in active_type_ids
                except (TypeError, ValueError):
                    pass
        active_flag = "⚠️ Building" if is_active else ""

        # Resolve assignment for this row
        overview_row_id = str(row.get("overview_row_id") or "")
        assigned_char = (assignment or {}).get(overview_row_id, "") if overview_row_id else ""

        table_rows.append({
            "#": rank,
            "Product": str(row.get("type_name") or row.get("type_id") or "Unknown"),
            score_col_label: score,
            "Assign to": assigned_char,
            "Profit (M)": round(sf(row.get("profit_amount")) / 1_000_000, 1),
            "ISK/Hr (M)": round(sf(row.get("isk_per_hour")) / 1_000_000, 1),
            "Margin %": round(sf(row.get("profit_margin_fraction")) * 100, 1),
            "ROC %": round(sf(row.get("return_on_capital")) * 100, 0),
            "Liquidity": str(row.get("liquidity_indicator") or "Unknown"),
            "DOS": round(dos, 1) if dos is not None else None,
            "BPC": _bpc_label(row),
            "Active?": active_flag,
            "Reorder": _reorder_tag(row, reorder_alerts or {}),
            "Warnings": warning_str,
        })

    df = pd.DataFrame(table_rows)
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.markdown("---")
    st.markdown("**Details per recommendation**")
    for rank, (row, score) in enumerate(ranked, start=1):
        type_name = str(row.get("type_name") or row.get("type_id") or "Unknown")
        with st.expander(f"#{rank} {type_name} — details"):
            good = _good_signals(row, score)
            warns = _warnings(row)

            detail_col, metrics_col = st.columns([3, 2])
            with detail_col:
                if good:
                    st.markdown("**Why chosen**")
                    for sig in good:
                        st.markdown(f"- {sig}")
                if warns:
                    st.markdown("**Watch out**")
                    for w in warns:
                        st.markdown(f"- {w}")

            with metrics_col:
                mj = cast(dict[str, Any], row.get("manufacturing_job") or {})
                time_secs = sf(mj.get("time_seconds"))
                prep_pct = row.get("prep_time_fraction_pct")
                me = row.get("blueprint_me")
                ci = row.get("manufacturing_cost_index")
                anomaly_reasons = row.get("price_anomaly_reasons")

                st.caption(f"**Score:** {score}")
                st.caption(f"**Build time:** {time_secs / 3600:.1f}h" if time_secs > 0 else "**Build time:** —")
                if me is not None:
                    st.caption(f"**Blueprint ME:** {me}")
                if prep_pct is not None:
                    st.caption(f"**Prep time:** {float(prep_pct):.0f}%")
                if ci is not None:
                    st.caption(f"**Cost index:** {float(ci) * 100:.2f}%")
                if isinstance(anomaly_reasons, list) and anomaly_reasons:
                    st.caption(f"**Anomaly:** {'; '.join(anomaly_reasons)}")


def _render_excluded_section(disqualified: list[tuple[dict[str, Any], str]]) -> None:
    if not disqualified:
        return
    with st.expander(f"Excluded products ({len(disqualified)})"):
        rows = [
            {
                "Product": str(row.get("type_name") or row.get("type_id") or "Unknown"),
                "Reason": reason,
            }
            for row, reason in disqualified
        ]
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


def _render_page_about() -> None:
    def _content() -> None:
        st.caption(
            "Automatically scores and ranks your manufactureable products using a composite of "
            "ISK/hour, return on capital, absolute profit, market liquidity, and pricing confidence. "
            "Applies multiplicative penalties for anomaly risk, fragile margins, and material contention."
        )
        st.caption(
            "Filter settings are inherited from the Industry Builder. "
            "Change your meta group, profit, or quality filters there and Refresh Overview — "
            "the recommendations here will update automatically."
        )
        st.caption(
            "Hard disqualifiers (SDE fallback blueprints, High anomaly risk, Low pricing confidence, "
            "and unprofitable products) are excluded from scoring entirely and listed in the Excluded section."
        )

    if hasattr(st, "popover"):
        with st.popover("?", help="About the Portfolio Planner"):
            _content()
    else:
        with st.expander("?", expanded=False):
            _content()


# ---------------------------------------------------------------------------
# Shopping List tab
# ---------------------------------------------------------------------------

def _render_shopping_list_tab(overview_rows: list[dict[str, Any]]) -> None:
    st.markdown("### Jita Shopping List")
    st.caption(
        "Select the items you plan to build. "
        "Quantities reflect what the Industry Builder has already computed "
        "(owned stock in the Industry hangar is already subtracted via the sourcing strategy)."
    )

    if not overview_rows:
        st.info("No overview data loaded. Refresh from the Industry Builder first.")
        return

    # Build label → row mapping for multiselect
    options: list[str] = []
    label_to_row: dict[str, dict[str, Any]] = {}
    for row in overview_rows:
        type_name = str(row.get("type_name") or row.get("type_id") or "Unknown")
        batches = max(1, int(row.get("max_batches_total") or 1))
        label = f"{type_name} (×{batches})"
        # Handle duplicates (shouldn't happen but be defensive)
        unique_label = label
        counter = 2
        while unique_label in label_to_row:
            unique_label = f"{label} [{counter}]"
            counter += 1
        options.append(unique_label)
        label_to_row[unique_label] = row

    selected_labels: list[str] = st.multiselect(
        "Items to build",
        options=options,
        default=[],
        key="shopping_list_selected_items",
        help="Select one or more products. Quantities are scaled by the recommended batch count.",
    )

    if not selected_labels:
        st.info("Select items above to generate a shopping list.")
        return

    selected_rows = [label_to_row[lbl] for lbl in selected_labels]
    shopping_items = aggregate_shopping_list(selected_rows)

    if not shopping_items:
        st.info("No buy-sourced materials found for the selected items.")
        return

    # Build display DataFrame
    def _fmt_isk(v: float | None) -> str:
        if v is None:
            return "—"
        if v >= 1_000_000_000:
            return f"{v / 1_000_000_000:.2f}B"
        if v >= 1_000_000:
            return f"{v / 1_000_000:.2f}M"
        if v >= 1_000:
            return f"{v / 1_000:.1f}K"
        return f"{v:,.0f}"

    table_rows = []
    for item in shopping_items:
        unit_price = item["unit_price"]
        buy_qty = item["buy"]
        total_isk = buy_qty * unit_price if unit_price is not None else None
        table_rows.append({
            "Item": item["type_name"],
            "Need": f"{item['need']:,}",
            "Owned (est.)": f"{item['need'] - buy_qty:,}",
            "Buy": f"{buy_qty:,}",
            "Unit Price": _fmt_isk(unit_price),
            "Total ISK": _fmt_isk(total_isk),
        })

    df = pd.DataFrame(table_rows)
    st.dataframe(df, hide_index=True, use_container_width=True)
    st.caption(
        "* Owned quantities are estimated from the Industry Builder's material sourcing calculations. "
        "Run a corp asset sync to update."
    )

    # Summary line
    total_isk_sum = sum(
        item["buy"] * item["unit_price"]
        for item in shopping_items
        if item["unit_price"] is not None and item["buy"] > 0
    )
    distinct_items = len(shopping_items)
    fully_stocked = sum(1 for item in shopping_items if item["buy"] == 0)
    no_price_count = sum(1 for item in shopping_items if item["unit_price"] is None and item["buy"] > 0)
    summary = (
        f"**Total ISK to buy:** {_fmt_isk(total_isk_sum)} · "
        f"**Distinct items:** {distinct_items} · "
        f"**Already fully stocked:** {fully_stocked}"
    )
    if no_price_count:
        summary += f" · ⚠️ {no_price_count} item(s) have no price — excluded from total"
    st.markdown(summary)

    # Clipboard block
    st.markdown("---")
    st.markdown("**Copy to clipboard** (paste in-game or in a spreadsheet):")
    lines = ["=== Jita Shopping List ==="]
    for item in shopping_items:
        buy_qty = item["buy"]
        if buy_qty <= 0:
            continue
        lines.append(f"{item['type_name']} x {buy_qty:,}")
    lines.append(f"Total: ~{_fmt_isk(total_isk_sum)} ISK")
    st.code("\n".join(lines), language="text")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def render() -> None:
    header_col, about_col = st.columns([20, 1])
    with header_col:
        st.subheader("Portfolio Planner")
    with about_col:
        st.write("")
        _render_page_about()

    overview_rows = _get_overview_rows()
    overview_meta = cast(dict[str, Any], st.session_state.get("industry_builder_overview_meta") or {})

    if not overview_rows:
        _render_no_snapshot_prompt()
        return

    fs = _read_industry_builder_filter_state()
    filtered_rows = filter_overview_rows(
        overview_rows,
        tuple(sorted(fs["enabled_meta_groups"])),
        fs["have_skills_only"],
        fs["positive_profit_only"],
        fs["min_margin_pct"],
        fs["min_isk_per_hour"],
        fs["min_region_daily_volume"],
        tuple(sorted(fs["excluded_liquidity_indicators"])),
        tuple(sorted(fs["excluded_anomaly_risks"])),
    )

    eligible_rows, disqualified = _apply_hard_disqualifiers(filtered_rows)

    scoring_objective = st.selectbox(
        "Sort by",
        SCORING_OBJECTIVES,
        key="portfolio_scoring_objective",
    )

    ranked = _rank_candidates(eligible_rows, objective=scoring_objective)

    _render_header_banner(overview_rows, filtered_rows, eligible_rows, overview_meta)

    if overview_refresh_is_active():
        return

    # Fetch active jobs once for the slot summary header and the "Active?" column
    jobs_data = _fetch_jobs_data()
    _render_slot_summary_header(jobs_data)
    active_type_ids = _build_active_type_ids(jobs_data)

    # Compute character assignment for recommendations
    top_candidates = [row for row, _ in ranked[:15]]
    blueprint_type_ids_for_qual: tuple[int, ...] = tuple(
        sorted({
            _row_blueprint_type_id(row)
            for row in top_candidates
            if _row_blueprint_type_id(row) > 0
        })
    )
    skill_qual: dict[str, dict[str, bool]] = {}
    if blueprint_type_ids_for_qual:
        try:
            skill_qual = fetch_blueprint_skill_qualification(blueprint_type_ids_for_qual)
        except Exception:
            skill_qual = {}
    assignment = _compute_character_assignment(top_candidates, skill_qual, jobs_data)

    candidate_type_ids = tuple(sorted({
        int(c.get("type_id") or 0)
        for c in top_candidates
        if int(c.get("type_id") or 0) > 0
    }))
    reorder_alerts: dict[str, dict] = {}
    if candidate_type_ids:
        try:
            reorder_alerts = fetch_reorder_alerts(candidate_type_ids)
        except Exception:
            reorder_alerts = {}

    tab_recommendations, tab_shopping = st.tabs(["Recommendations", "Shopping List"])

    with tab_recommendations:
        _render_recommendations_table(ranked[:15], active_type_ids=active_type_ids, assignment=assignment, reorder_alerts=reorder_alerts, objective=scoring_objective)
        _render_excluded_section(disqualified)

    with tab_shopping:
        _render_shopping_list_tab(overview_rows)
