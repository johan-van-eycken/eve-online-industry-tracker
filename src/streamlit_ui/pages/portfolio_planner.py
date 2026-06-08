from __future__ import annotations

from typing import Any, cast

import pandas as pd
import streamlit as st

from streamlit_ui.api.industry_builder import (
    clear_industry_builder_caches,
    start_product_overview_refresh,
)
from streamlit_ui.api.industry_profiles import build_industry_profile_options
from streamlit_ui.state.industry_builder_page import (
    fetch_industry_profiles_cached,
    overview_refresh_is_active,
    start_overview_refresh_job,
)
from streamlit_ui.state.industry_builder_ui import filter_overview_rows
from streamlit_ui.state.industry_snapshot_page import (
    _refresh_status_fragment,
    load_character_context,
)

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


def _rank_candidates(rows: list[dict[str, Any]]) -> list[tuple[dict[str, Any], float]]:
    if not rows:
        return []
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


def _render_recommendations_table(ranked: list[tuple[dict[str, Any], float]]) -> None:
    if not ranked:
        st.info("No eligible products to recommend with current filters and disqualification rules.")
        return

    st.markdown("### Top 15 Manufacturing Recommendations")

    def sf(v: Any) -> float:
        try:
            return float(v or 0.0)
        except (TypeError, ValueError):
            return 0.0

    table_rows = []
    for rank, (row, score) in enumerate(ranked, start=1):
        warns = _warnings(row)
        warning_str = "; ".join(f"⚠ {w}" for w in warns) if warns else ""
        dos = row.get("days_of_supply")
        table_rows.append({
            "#": rank,
            "Product": str(row.get("type_name") or row.get("type_id") or "Unknown"),
            "Score": score,
            "Profit (M)": round(sf(row.get("profit_amount")) / 1_000_000, 1),
            "ISK/Hr (M)": round(sf(row.get("isk_per_hour")) / 1_000_000, 1),
            "Margin %": round(sf(row.get("profit_margin_fraction")) * 100, 1),
            "ROC %": round(sf(row.get("return_on_capital")) * 100, 0),
            "Liquidity": str(row.get("liquidity_indicator") or "Unknown"),
            "DOS": round(dos, 1) if dos is not None else None,
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
    ranked = _rank_candidates(eligible_rows)

    _render_header_banner(overview_rows, filtered_rows, eligible_rows, overview_meta)

    if overview_refresh_is_active():
        return

    _render_recommendations_table(ranked[:15])
    _render_excluded_section(disqualified)
