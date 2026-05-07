from __future__ import annotations

from collections import Counter
import time
from datetime import datetime, timezone
from typing import Any, cast

import pandas as pd
import streamlit as st

from utils.industry_builder_api import (
    fetch_portfolio_candidates_refresh_status,
    fetch_portfolio_plan,
    start_portfolio_candidates_refresh,
)
from utils.industry_builder_page import current_overview_request_params
from utils.webpage_ui import render_aggrid_table, require_aggrid


_PORTFOLIO_CANDIDATES_JOB_ID_KEY = "industry_builder_portfolio_candidates_job_id"
_PORTFOLIO_CANDIDATES_PROGRESS_FRACTION_KEY = "industry_builder_portfolio_candidates_progress_fraction"
_PORTFOLIO_CANDIDATES_PROGRESS_LABEL_KEY = "industry_builder_portfolio_candidates_progress_label"
_PORTFOLIO_CANDIDATES_ERROR_KEY = "industry_builder_portfolio_candidates_error"
_PORTFOLIO_CANDIDATES_CREATED_AT_KEY = "industry_builder_portfolio_candidates_created_at"
_PORTFOLIO_CANDIDATES_UPDATED_AT_KEY = "industry_builder_portfolio_candidates_updated_at"
_PORTFOLIO_CANDIDATES_PROGRESS_META_KEY = "industry_builder_portfolio_candidates_progress_meta"
_PORTFOLIO_PLAN_REQUEST_KEY = "industry_builder_portfolio_plan_request"
_PORTFOLIO_CANDIDATE_DIRECTIVES_KEY = "industry_builder_portfolio_candidate_directives"


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return float(parsed)


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _optional_int(value: Any) -> int | None:
    parsed = _safe_int(value)
    return parsed if parsed is not None and parsed > 0 else None


def _selected_string_list(key: str) -> list[str]:
    values = st.session_state.get(key) or []
    if not isinstance(values, list):
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def _candidate_row_id(candidate: dict[str, Any]) -> str:
    return str(candidate.get("overview_row_id") or "").strip()


def _candidate_label(candidate: dict[str, Any]) -> str:
    type_name = str(candidate.get("type_name") or candidate.get("type_id") or "Candidate").strip()
    blueprint_source = _title_label(str(candidate.get("blueprint_source_kind") or ""))
    row_id = _candidate_row_id(candidate)
    suffix = row_id[-10:] if row_id else "n/a"
    if blueprint_source:
        return f"{type_name} | {blueprint_source} | {suffix}"
    return f"{type_name} | {suffix}"


def _normalize_directive_value(value: Any) -> int | None:
    parsed = _optional_int(value)
    return int(parsed) if parsed is not None else None


def _candidate_directive_state(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    raw_state = st.session_state.get(_PORTFOLIO_CANDIDATE_DIRECTIVES_KEY) or {}
    candidate_ids = {_candidate_row_id(candidate) for candidate in candidates if _candidate_row_id(candidate)}
    normalized: dict[str, dict[str, Any]] = {}
    for overview_row_id in candidate_ids:
        raw_entry = raw_state.get(overview_row_id) if isinstance(raw_state, dict) else {}
        raw_entry = raw_entry if isinstance(raw_entry, dict) else {}
        exclude = bool(raw_entry.get("exclude", False))
        normalized[overview_row_id] = {
            "force_include": bool(raw_entry.get("force_include", False)) and not exclude,
            "exclude": exclude,
            "lock_required": bool(raw_entry.get("lock_required", False)) and not exclude,
            "max_batches_override": _normalize_directive_value(raw_entry.get("max_batches_override")),
            "target_batches_override": _normalize_directive_value(raw_entry.get("target_batches_override")),
            "target_units_override": _normalize_directive_value(raw_entry.get("target_units_override")),
        }
    st.session_state[_PORTFOLIO_CANDIDATE_DIRECTIVES_KEY] = normalized
    return normalized


def _candidate_directive_payload(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    directives = _candidate_directive_state(candidates)
    payload: list[dict[str, Any]] = []
    for overview_row_id, directive in directives.items():
        entry = {
            "overview_row_id": overview_row_id,
            "force_include": bool(directive.get("force_include", False)),
            "exclude": bool(directive.get("exclude", False)),
            "lock_required": bool(directive.get("lock_required", False)),
            "max_batches_override": _normalize_directive_value(directive.get("max_batches_override")),
            "target_batches_override": _normalize_directive_value(directive.get("target_batches_override")),
            "target_units_override": _normalize_directive_value(directive.get("target_units_override")),
        }
        if any(
            [
                entry["force_include"],
                entry["exclude"],
                entry["lock_required"],
                entry["max_batches_override"] is not None,
                entry["target_batches_override"] is not None,
                entry["target_units_override"] is not None,
            ]
        ):
            payload.append(entry)
    return payload


def _active_operator_tokens(candidates: list[dict[str, Any]]) -> list[str]:
    directives = _candidate_directive_payload(candidates)
    if not directives:
        return []
    forced_count = sum(1 for entry in directives if bool(entry.get("force_include")))
    excluded_count = sum(1 for entry in directives if bool(entry.get("exclude")))
    locked_count = sum(1 for entry in directives if bool(entry.get("lock_required")))
    override_count = sum(
        1
        for entry in directives
        if entry.get("max_batches_override") is not None
        or entry.get("target_batches_override") is not None
        or entry.get("target_units_override") is not None
    )
    tokens: list[str] = []
    if forced_count > 0:
        tokens.append(f"Forced includes: {forced_count}")
    if excluded_count > 0:
        tokens.append(f"Excluded: {excluded_count}")
    if locked_count > 0:
        tokens.append(f"Locked: {locked_count}")
    if override_count > 0:
        tokens.append(f"Overrides: {override_count}")
    return tokens


def _operator_candidate_count(candidates: list[dict[str, Any]]) -> int:
    directives = _candidate_directive_payload(candidates)
    return sum(1 for entry in directives if bool(entry.get("force_include")) or bool(entry.get("lock_required")))


def _loaded_snapshot_meta(candidates_payload: dict[str, Any]) -> dict[str, Any]:
    return cast(dict[str, Any], candidates_payload.get("meta") or {})


def _loaded_snapshot_id(candidates_payload: dict[str, Any]) -> str:
    return str(_loaded_snapshot_meta(candidates_payload).get("snapshot_id") or "")


def _loaded_snapshot_horizon_hours(candidates_payload: dict[str, Any]) -> float:
    summary = cast(dict[str, Any], _loaded_snapshot_meta(candidates_payload).get("summary") or {})
    try:
        return float(summary.get("planning_horizon_hours") or 0.0)
    except Exception:
        return 0.0


def _current_planning_horizon_hours() -> float:
    try:
        return float(st.session_state.get("industry_builder_portfolio_horizon_hours", 24.0) or 24.0)
    except Exception:
        return 24.0


def _snapshot_matches_current_horizon(candidates_payload: dict[str, Any]) -> bool:
    snapshot_horizon = _loaded_snapshot_horizon_hours(candidates_payload)
    current_horizon = _current_planning_horizon_hours()
    if snapshot_horizon <= 0.0:
        return False
    return abs(snapshot_horizon - current_horizon) < 1e-9


def _portfolio_plan_request_payload(
    *,
    character_id: int,
    candidate_snapshot_id: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "character_id": int(character_id),
        "candidate_snapshot_id": str(candidate_snapshot_id),
        "planning_horizon_hours": _current_planning_horizon_hours(),
        "capital_limit_isk": float(st.session_state.get("industry_builder_portfolio_capital_limit_isk", 0.0) or 0.0),
        "manufacturing_slots_available": int(st.session_state.get("industry_builder_portfolio_slots_available", 1) or 1),
        "objective": str(st.session_state.get("industry_builder_portfolio_objective", "balanced") or "balanced"),
        "positive_profit_only": bool(st.session_state.get("industry_builder_portfolio_candidate_positive_profit_only", False)),
        "min_margin_pct": float(st.session_state.get("industry_builder_portfolio_candidate_min_margin_pct", 0.0) or 0.0),
        "min_isk_per_hour": float(st.session_state.get("industry_builder_portfolio_candidate_min_isk_per_hour", 0.0) or 0.0),
        "min_region_daily_volume": int(st.session_state.get("industry_builder_portfolio_candidate_min_region_daily_volume", 0) or 0),
        "minimum_pricing_confidence": str(st.session_state.get("industry_builder_portfolio_minimum_confidence", "low") or "low"),
        "candidate_categories": _selected_string_list("industry_builder_portfolio_candidate_categories"),
        "candidate_meta_groups": _selected_string_list("industry_builder_portfolio_candidate_meta_groups"),
        "candidate_pricing_confidences": [value.lower() for value in _selected_string_list("industry_builder_portfolio_candidate_confidences")],
        "candidate_blueprint_sources": [value.lower() for value in _selected_string_list("industry_builder_portfolio_candidate_blueprint_sources")],
        "min_owned_input_coverage_pct": float(
            st.session_state.get("industry_builder_portfolio_candidate_min_owned_input_coverage_pct", 0.0) or 0.0
        ),
        "candidate_directives": _candidate_directive_payload(candidates),
    }


def _stored_plan_request() -> dict[str, Any]:
    value = st.session_state.get(_PORTFOLIO_PLAN_REQUEST_KEY) or {}
    return cast(dict[str, Any], value if isinstance(value, dict) else {})


def _plan_request_is_current(current_request: dict[str, Any]) -> bool:
    return _stored_plan_request() == current_request


def _title_label(value: str) -> str:
    return str(value or "").replace("_", " ").strip().title()


def _snapshot_summary_text(candidates_payload: dict[str, Any]) -> str:
    meta = cast(dict[str, Any], candidates_payload.get("meta") or {})
    pricing_batch = cast(dict[str, Any], meta.get("pricing_batch") or {})
    request_params = cast(dict[str, Any], meta.get("snapshot_request_params") or {})
    snapshot_id = str(meta.get("snapshot_id") or "")
    generated_at = str(pricing_batch.get("generated_at") or "N/A")
    market_hub = str(
        pricing_batch.get("market_hub_label")
        or pricing_batch.get("market_hub")
        or request_params.get("market_hub")
        or "N/A"
    )
    return "Snapshot {snapshot} | Pricing {generated} | Hub {hub}".format(
        snapshot=snapshot_id[:8] if snapshot_id else "ad hoc",
        generated=generated_at,
        hub=market_hub,
    )


def _active_candidate_scope_tokens() -> list[str]:
    tokens: list[str] = []
    selected_categories = _selected_string_list("industry_builder_portfolio_candidate_categories")
    selected_meta_groups = _selected_string_list("industry_builder_portfolio_candidate_meta_groups")
    selected_confidences = _selected_string_list("industry_builder_portfolio_candidate_confidences")
    selected_blueprint_sources = _selected_string_list("industry_builder_portfolio_candidate_blueprint_sources")
    if selected_categories:
        tokens.append(f"Categories: {len(selected_categories)}")
    if selected_meta_groups:
        tokens.append(f"Meta groups: {len(selected_meta_groups)}")
    if selected_confidences:
        tokens.append(f"Confidence: {', '.join(selected_confidences)}")
    if selected_blueprint_sources:
        tokens.append(f"Blueprint source: {', '.join(selected_blueprint_sources)}")
    if bool(st.session_state.get("industry_builder_portfolio_candidate_positive_profit_only", False)):
        tokens.append("Positive profit only")
    min_margin_pct = float(st.session_state.get("industry_builder_portfolio_candidate_min_margin_pct", 0.0) or 0.0)
    if min_margin_pct > 0.0:
        tokens.append(f"Min margin {min_margin_pct:.1f}%")
    min_isk_per_hour = float(st.session_state.get("industry_builder_portfolio_candidate_min_isk_per_hour", 0.0) or 0.0)
    if min_isk_per_hour > 0.0:
        tokens.append(f"Min ISK/hour {min_isk_per_hour:,.0f}")
    min_region_daily_volume = int(st.session_state.get("industry_builder_portfolio_candidate_min_region_daily_volume", 0) or 0)
    if min_region_daily_volume > 0:
        tokens.append(f"Min daily volume {min_region_daily_volume}")
    min_owned_input_coverage_pct = float(
        st.session_state.get("industry_builder_portfolio_candidate_min_owned_input_coverage_pct", 0.0) or 0.0
    )
    if min_owned_input_coverage_pct > 0.0:
        tokens.append(f"Min owned inputs {min_owned_input_coverage_pct:.0f}%")
    return tokens


def _apply_candidate_directive_grid_edits(
    candidates: list[dict[str, Any]],
    *,
    grid_response: Any,
) -> dict[str, dict[str, Any]]:
    directives = _candidate_directive_state(candidates)
    response_data = None
    if isinstance(grid_response, dict):
        response_data = grid_response.get("data")
    if response_data is None:
        return directives

    if isinstance(response_data, pd.DataFrame):
        rows = response_data.to_dict(orient="records")
    elif isinstance(response_data, list):
        rows = response_data
    else:
        return directives

    for row in rows:
        if not isinstance(row, dict):
            continue
        overview_row_id = str(row.get("Overview Row ID") or "").strip()
        if not overview_row_id or overview_row_id not in directives:
            continue
        exclude = bool(row.get("Excluded", False))
        directives[overview_row_id]["exclude"] = exclude
        directives[overview_row_id]["force_include"] = bool(row.get("Forced", False)) and not exclude
        directives[overview_row_id]["lock_required"] = bool(row.get("Locked", False)) and not exclude
        directives[overview_row_id]["max_batches_override"] = _normalize_directive_value(row.get("Max Batches Override"))
        directives[overview_row_id]["target_batches_override"] = _normalize_directive_value(row.get("Target Batches Override"))
        directives[overview_row_id]["target_units_override"] = _normalize_directive_value(row.get("Target Units Override"))

    st.session_state[_PORTFOLIO_CANDIDATE_DIRECTIVES_KEY] = directives
    return directives


def _planner_decisions_summary_frame(decisions: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {"Decision": "Forced includes", "Count": len(cast(list[dict[str, Any]], decisions.get("forced_includes") or []))},
        {"Decision": "Exclusions", "Count": len(cast(list[dict[str, Any]], decisions.get("exclusions") or []))},
        {"Decision": "Locked items", "Count": len(cast(list[dict[str, Any]], decisions.get("locked_items") or []))},
        {"Decision": "Override items", "Count": len(cast(list[dict[str, Any]], decisions.get("override_items") or []))},
        {"Decision": "Unfulfilled locked", "Count": len(cast(list[dict[str, Any]], decisions.get("unfulfilled_locked_items") or []))},
    ]
    return pd.DataFrame([row for row in rows if int(row["Count"]) > 0])


def _skip_reason_summary_frame(skipped_items: list[dict[str, Any]]) -> pd.DataFrame:
    reason_counts: Counter[str] = Counter()
    for item in skipped_items:
        if not isinstance(item, dict):
            continue
        reason_text = str(item.get("reason") or "").strip()
        if not reason_text:
            continue
        for reason in [part.strip() for part in reason_text.split(";") if part.strip()]:
            reason_counts[reason] += 1
    rows = [{"Reason": reason, "Count": count} for reason, count in reason_counts.most_common()]
    return pd.DataFrame(rows)


def _parse_iso_timestamp(value: Any) -> datetime | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except Exception:
        return None


def _format_elapsed_seconds(value: float | None) -> str:
    if value is None:
        return "N/A"
    total_seconds = max(0, int(value))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _portfolio_candidates_elapsed_seconds() -> float | None:
    created_at = _parse_iso_timestamp(st.session_state.get(_PORTFOLIO_CANDIDATES_CREATED_AT_KEY))
    if created_at is None:
        return None
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - created_at).total_seconds())


def _ensure_portfolio_candidate_job_state() -> None:
    defaults = {
        _PORTFOLIO_CANDIDATES_JOB_ID_KEY: "",
        _PORTFOLIO_CANDIDATES_PROGRESS_FRACTION_KEY: 0.0,
        _PORTFOLIO_CANDIDATES_PROGRESS_LABEL_KEY: "",
        _PORTFOLIO_CANDIDATES_ERROR_KEY: None,
        _PORTFOLIO_CANDIDATES_CREATED_AT_KEY: None,
        _PORTFOLIO_CANDIDATES_UPDATED_AT_KEY: None,
        _PORTFOLIO_CANDIDATES_PROGRESS_META_KEY: {},
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _clear_portfolio_candidate_job(*, error_message: str | None = None) -> None:
    st.session_state[_PORTFOLIO_CANDIDATES_JOB_ID_KEY] = ""
    st.session_state[_PORTFOLIO_CANDIDATES_ERROR_KEY] = error_message
    st.session_state[_PORTFOLIO_CANDIDATES_PROGRESS_META_KEY] = {}


def _portfolio_candidate_job_is_active() -> bool:
    return bool(str(st.session_state.get(_PORTFOLIO_CANDIDATES_JOB_ID_KEY) or ""))


def _portfolio_candidate_job_view() -> dict[str, Any]:
    return {
        "is_active": _portfolio_candidate_job_is_active(),
        "progress_fraction": float(st.session_state.get(_PORTFOLIO_CANDIDATES_PROGRESS_FRACTION_KEY) or 0.0),
        "progress_label": str(st.session_state.get(_PORTFOLIO_CANDIDATES_PROGRESS_LABEL_KEY) or ""),
        "error_message": st.session_state.get(_PORTFOLIO_CANDIDATES_ERROR_KEY),
        "created_at": st.session_state.get(_PORTFOLIO_CANDIDATES_CREATED_AT_KEY),
        "updated_at": st.session_state.get(_PORTFOLIO_CANDIDATES_UPDATED_AT_KEY),
        "progress_meta": st.session_state.get(_PORTFOLIO_CANDIDATES_PROGRESS_META_KEY) or {},
    }


def _poll_portfolio_candidate_job() -> str:
    job_id = str(st.session_state.get(_PORTFOLIO_CANDIDATES_JOB_ID_KEY) or "")
    if not job_id:
        return "idle"

    refresh_status = fetch_portfolio_candidates_refresh_status(job_id)
    st.session_state[_PORTFOLIO_CANDIDATES_PROGRESS_FRACTION_KEY] = max(
        0.0,
        min(1.0, float(refresh_status.get("progress_fraction") or 0.0)),
    )
    st.session_state[_PORTFOLIO_CANDIDATES_PROGRESS_LABEL_KEY] = str(
        refresh_status.get("progress_label") or "Loading portfolio candidates..."
    )
    st.session_state[_PORTFOLIO_CANDIDATES_CREATED_AT_KEY] = refresh_status.get("created_at")
    st.session_state[_PORTFOLIO_CANDIDATES_UPDATED_AT_KEY] = refresh_status.get("updated_at")
    st.session_state[_PORTFOLIO_CANDIDATES_PROGRESS_META_KEY] = refresh_status.get("progress_meta") or {}

    status = str(refresh_status.get("status") or "")
    if status == "completed":
        result = refresh_status.get("result") or []
        result_meta = refresh_status.get("result_meta") or {}
        payload_meta = dict(result_meta) if isinstance(result_meta, dict) else {}
        payload_meta["snapshot_id"] = str(refresh_status.get("job_id") or job_id)
        payload_meta["snapshot_created_at"] = refresh_status.get("created_at")
        payload_meta["snapshot_updated_at"] = refresh_status.get("updated_at")
        payload_meta["snapshot_request_params"] = refresh_status.get("request_params") or {}
        payload_meta["snapshot_result_count"] = int(refresh_status.get("result_count") or 0)
        st.session_state["industry_builder_portfolio_candidates_payload"] = {
            "candidates": result if isinstance(result, list) else [],
            "meta": payload_meta,
        }
        _clear_portfolio_candidate_job()
        return "completed"

    if status == "failed":
        _clear_portfolio_candidate_job(
            error_message=str(refresh_status.get("error_message") or "Portfolio candidate build failed")
        )
        return "failed"

    return "running"


def _start_candidate_job(
    *,
    default_character_id_value: int,
    default_owned_blueprint_scope: str,
) -> None:
    request_params = current_overview_request_params(
        default_character_id_value=default_character_id_value,
        default_owned_blueprint_scope=default_owned_blueprint_scope,
    )
    refresh_job = start_portfolio_candidates_refresh(
        maximize_bp_runs=bool(request_params.get("maximize_bp_runs", False)),
        group_identical_bpcs=bool(request_params.get("group_identical_bpcs", True)),
        build_from_bpc=bool(request_params.get("build_from_bpc", True)),
        have_blueprint_source_only=bool(request_params.get("have_blueprint_source_only", True)),
        include_reactions=bool(request_params.get("include_reactions", False)),
        market_hub=str(request_params.get("market_hub") or "jita"),
        material_price_side=str(request_params.get("material_price_side") or "sell"),
        product_price_side=str(request_params.get("product_price_side") or "sell"),
        industry_profile_id=_optional_int(request_params.get("industry_profile_id")),
        owned_blueprints_scope=str(request_params.get("owned_blueprints_scope") or default_owned_blueprint_scope),
        character_id=int(request_params.get("character_id") or default_character_id_value),
        planning_horizon_hours=float(st.session_state.get("industry_builder_portfolio_horizon_hours", 24.0) or 24.0),
    )
    refresh_job_id = str(refresh_job.get("job_id") or "")
    if not refresh_job_id:
        raise RuntimeError("Portfolio candidates job did not return a job_id")

    st.session_state[_PORTFOLIO_CANDIDATES_JOB_ID_KEY] = refresh_job_id
    st.session_state[_PORTFOLIO_CANDIDATES_PROGRESS_FRACTION_KEY] = 0.0
    st.session_state[_PORTFOLIO_CANDIDATES_PROGRESS_LABEL_KEY] = "Starting portfolio candidate build..."
    st.session_state[_PORTFOLIO_CANDIDATES_ERROR_KEY] = None
    st.session_state[_PORTFOLIO_CANDIDATES_CREATED_AT_KEY] = refresh_job.get("created_at")
    st.session_state[_PORTFOLIO_CANDIDATES_UPDATED_AT_KEY] = refresh_job.get("updated_at")
    st.session_state[_PORTFOLIO_CANDIDATES_PROGRESS_META_KEY] = refresh_job.get("progress_meta") or {}


def _render_portfolio_candidate_progress(job_view: dict[str, Any]) -> None:
    progress_meta = cast(dict[str, Any], job_view.get("progress_meta") or {})
    current_step = int(progress_meta.get("step") or 0)
    step_count = int(progress_meta.get("step_count") or 0)
    stage = _title_label(str(progress_meta.get("stage") or "working"))

    st.markdown("**Loading portfolio candidates**")
    st.caption("The backend is building candidate rows from the current snapshot settings. This page will keep polling automatically until the result is ready.")

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric("Elapsed", _format_elapsed_seconds(_portfolio_candidates_elapsed_seconds()))
    metric_col_2.metric(
        "Current Step",
        f"{current_step}/{step_count}" if current_step > 0 and step_count > 0 else "Queued",
    )
    metric_col_3.metric("Current Stage", stage or "Working")

    st.progress(
        int(max(0.0, min(1.0, float(job_view.get("progress_fraction") or 0.0))) * 100),
        text=str(job_view.get("progress_label") or "Loading portfolio candidates..."),
    )

    rows_hint = progress_meta.get("rows")
    candidate_count_hint = progress_meta.get("candidate_count")
    hint_parts: list[str] = []
    if rows_hint is not None:
        hint_parts.append(f"Rows: {rows_hint}")
    if candidate_count_hint is not None:
        hint_parts.append(f"Candidates: {candidate_count_hint}")
    if hint_parts:
        st.caption(" | ".join(hint_parts))


def _candidate_table_frame(candidates: list[dict[str, Any]], *, directives: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        directive = directives.get(_candidate_row_id(candidate), {})
        rows.append(
            {
                "Overview Row ID": _candidate_row_id(candidate),
                "Type Name": candidate.get("type_name"),
                "Category": candidate.get("category_name"),
                "Meta Group": candidate.get("meta_group_name"),
                "Profit / Batch": _safe_float(candidate.get("profit_amount")),
                "Effective Profit / Batch": _safe_float(candidate.get("effective_profit_per_batch")),
                "ISK / Hour": _safe_float(candidate.get("isk_per_hour")),
                "Effective ISK / Hour": _safe_float(candidate.get("effective_isk_per_hour")),
                "Margin": _safe_float(candidate.get("profit_margin_fraction")),
                "Capital / Batch": _safe_float(candidate.get("cash_outlay_per_batch")),
                "Slot Hours / Batch": _safe_float(candidate.get("slot_hours_per_batch")),
                "Mfg Slot Hours": _safe_float(candidate.get("manufacturing_slot_hours_per_batch")),
                "Prep Slot Hours": _safe_float(candidate.get("preparation_slot_hours_per_batch")),
                "Region Daily Volume": _safe_int(candidate.get("region_daily_volume")),
                "Market Absorption Units": _safe_int(candidate.get("estimated_market_absorption_units")),
                "Max Batches": _safe_int(candidate.get("max_batches_total")),
                "Blueprint Source": _title_label(str(candidate.get("blueprint_source_kind") or "")),
                "Owned Input Coverage": _safe_float(candidate.get("owned_input_coverage_fraction")),
                "Pricing Confidence": _title_label(str(candidate.get("pricing_confidence") or "")),
                "Portfolio Eligible": bool(candidate.get("is_portfolio_candidate", False)),
                "Forced": bool(directive.get("force_include", False)),
                "Excluded": bool(directive.get("exclude", False)),
                "Locked": bool(directive.get("lock_required", False)),
                "Max Batches Override": _safe_int(directive.get("max_batches_override")),
                "Target Batches Override": _safe_int(directive.get("target_batches_override")),
                "Target Units Override": _safe_int(directive.get("target_units_override")),
            }
        )
    return pd.DataFrame(rows)


def _candidate_matches_filters(candidate: dict[str, Any]) -> bool:
    selected_categories = cast(list[str], st.session_state.get("industry_builder_portfolio_candidate_categories") or [])
    selected_meta_groups = cast(list[str], st.session_state.get("industry_builder_portfolio_candidate_meta_groups") or [])
    selected_confidences = cast(list[str], st.session_state.get("industry_builder_portfolio_candidate_confidences") or [])
    selected_blueprint_sources = cast(
        list[str],
        st.session_state.get("industry_builder_portfolio_candidate_blueprint_sources") or [],
    )
    candidate_category = str(candidate.get("category_name") or "")
    candidate_meta_group = str(candidate.get("meta_group_name") or "")
    candidate_confidence = _title_label(str(candidate.get("pricing_confidence") or ""))
    candidate_blueprint_source = _title_label(str(candidate.get("blueprint_source_kind") or ""))

    if selected_categories and candidate_category not in selected_categories:
        return False
    if selected_meta_groups and candidate_meta_group not in selected_meta_groups:
        return False
    if selected_confidences and candidate_confidence not in selected_confidences:
        return False
    if selected_blueprint_sources and candidate_blueprint_source not in selected_blueprint_sources:
        return False

    positive_profit_only = bool(st.session_state.get("industry_builder_portfolio_candidate_positive_profit_only", False))
    if positive_profit_only and float(candidate.get("profit_amount") or 0.0) <= 0.0:
        return False

    min_margin_pct = float(st.session_state.get("industry_builder_portfolio_candidate_min_margin_pct", 0.0) or 0.0)
    if float(candidate.get("profit_margin_fraction") or 0.0) < (min_margin_pct / 100.0):
        return False

    min_isk_per_hour = float(st.session_state.get("industry_builder_portfolio_candidate_min_isk_per_hour", 0.0) or 0.0)
    if float(candidate.get("isk_per_hour") or 0.0) < min_isk_per_hour:
        return False

    min_region_daily_volume = int(st.session_state.get("industry_builder_portfolio_candidate_min_region_daily_volume", 0) or 0)
    if int(candidate.get("region_daily_volume") or 0) < min_region_daily_volume:
        return False

    min_owned_input_coverage = float(
        st.session_state.get("industry_builder_portfolio_candidate_min_owned_input_coverage_pct", 0.0) or 0.0
    )
    if float(candidate.get("owned_input_coverage_fraction") or 0.0) < (min_owned_input_coverage / 100.0):
        return False

    return True


def _filtered_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [candidate for candidate in candidates if isinstance(candidate, dict) and _candidate_matches_filters(candidate)]


def _render_candidate_filters(candidates: list[dict[str, Any]]) -> None:
    category_options = sorted({str(candidate.get("category_name") or "") for candidate in candidates if str(candidate.get("category_name") or "")})
    meta_group_options = sorted({str(candidate.get("meta_group_name") or "") for candidate in candidates if str(candidate.get("meta_group_name") or "")})
    confidence_options = sorted({_title_label(str(candidate.get("pricing_confidence") or "")) for candidate in candidates if str(candidate.get("pricing_confidence") or "")})
    blueprint_source_options = sorted({_title_label(str(candidate.get("blueprint_source_kind") or "")) for candidate in candidates if str(candidate.get("blueprint_source_kind") or "")})

    with st.expander("Candidate Scope Filters", expanded=False):
        st.caption("These filters narrow both the visible candidate table and the candidate set used by Build Portfolio Plan.")
        filter_col_1, filter_col_2, filter_col_3, filter_col_4 = st.columns(4)
        with filter_col_1:
            st.multiselect(
                "Category",
                options=category_options,
                key="industry_builder_portfolio_candidate_categories",
            )
            st.checkbox(
                "Positive profit only",
                key="industry_builder_portfolio_candidate_positive_profit_only",
            )
        with filter_col_2:
            st.multiselect(
                "Meta Group",
                options=meta_group_options,
                key="industry_builder_portfolio_candidate_meta_groups",
            )
            st.number_input(
                "Min Margin (%)",
                min_value=0.0,
                step=0.5,
                key="industry_builder_portfolio_candidate_min_margin_pct",
            )
        with filter_col_3:
            st.multiselect(
                "Pricing Confidence",
                options=confidence_options,
                key="industry_builder_portfolio_candidate_confidences",
            )
            st.number_input(
                "Min ISK / Hour",
                min_value=0.0,
                step=100000.0,
                key="industry_builder_portfolio_candidate_min_isk_per_hour",
            )
            st.number_input(
                "Min Owned Input Coverage (%)",
                min_value=0.0,
                max_value=100.0,
                step=5.0,
                key="industry_builder_portfolio_candidate_min_owned_input_coverage_pct",
                help="Higher values keep only candidates that already cover more of their required inputs from owned inventory.",
            )
        with filter_col_4:
            st.multiselect(
                "Blueprint Source",
                options=blueprint_source_options,
                key="industry_builder_portfolio_candidate_blueprint_sources",
            )
            st.number_input(
                "Min Region Daily Volume",
                min_value=0,
                step=1,
                key="industry_builder_portfolio_candidate_min_region_daily_volume",
            )


def _render_candidate_workspace(candidates_payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = cast(list[dict[str, Any]], candidates_payload.get("candidates") or [])
    if not candidates:
        st.info("No portfolio candidates are available for the current snapshot and planner settings.")
        return []

    _render_candidate_filters(candidates)
    directives = _candidate_directive_state(candidates)
    filtered_candidates = _filtered_candidates(candidates)

    scope_tokens = _active_candidate_scope_tokens()
    if scope_tokens:
        st.caption("Active scope: " + " | ".join(scope_tokens))
    else:
        st.caption("Active scope: All loaded candidates")
    operator_tokens = _active_operator_tokens(candidates)
    if operator_tokens:
        st.caption("Operator intent: " + " | ".join(operator_tokens))
    st.caption("Edit `Forced`, `Excluded`, `Locked`, and override columns directly in the candidate table. Exclusions win over forced or locked settings.")

    metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)
    metric_col_1.metric("Candidates", len(candidates))
    metric_col_2.metric("In Scope", len(filtered_candidates))
    metric_col_3.metric(
        "Eligible In Scope",
        sum(1 for candidate in filtered_candidates if bool(candidate.get("is_portfolio_candidate", False))),
    )
    metric_col_4.metric(
        "Filtered Out",
        max(0, len(candidates) - len(filtered_candidates)),
    )

    candidate_df = _candidate_table_frame(filtered_candidates, directives=directives)
    runtime = require_aggrid()
    grid_response = render_aggrid_table(
        candidate_df,
        runtime=runtime,
        key="industry_builder_portfolio_candidates_grid",
        isk_cols=["Profit / Batch", "Effective Profit / Batch", "ISK / Hour", "Effective ISK / Hour", "Capital / Batch"],
        pct_cols=["Margin", "Owned Input Coverage"],
        number_cols_0=["Region Daily Volume", "Market Absorption Units", "Max Batches"],
        number_cols_2=["Slot Hours / Batch", "Mfg Slot Hours", "Prep Slot Hours"],
        column_configs={
            "Overview Row ID": {"hide": True},
            "Type Name": {"minWidth": 220, "pinned": "left"},
            "Category": {"minWidth": 130},
            "Meta Group": {"minWidth": 130},
            "Blueprint Source": {"minWidth": 180},
            "Pricing Confidence": {"minWidth": 140},
            "Portfolio Eligible": {"minWidth": 130},
            "Forced": {
                "editable": True,
                "cellRenderer": "agCheckboxCellRenderer",
                "cellEditor": "agCheckboxCellEditor",
                "width": 95,
            },
            "Excluded": {
                "editable": True,
                "cellRenderer": "agCheckboxCellRenderer",
                "cellEditor": "agCheckboxCellEditor",
                "width": 95,
            },
            "Locked": {
                "editable": True,
                "cellRenderer": "agCheckboxCellRenderer",
                "cellEditor": "agCheckboxCellEditor",
                "width": 95,
            },
            "Max Batches Override": {"editable": True, "type": ["numericColumn", "numberColumnFilter"], "width": 120},
            "Target Batches Override": {"editable": True, "type": ["numericColumn", "numberColumnFilter"], "width": 130},
            "Target Units Override": {"editable": True, "type": ["numericColumn", "numberColumnFilter"], "width": 120},
        },
        auto_size_columns=True,
        empty_message="No portfolio candidates match the current candidate scope filters.",
        editable=True,
        return_grid_response=True,
    )
    _apply_candidate_directive_grid_edits(candidates, grid_response=grid_response)
    return filtered_candidates


def render_portfolio_planner(
    *,
    default_character_id_value: int,
    default_owned_blueprint_scope: str,
) -> None:
    planner_defaults = {
        "industry_builder_portfolio_capital_limit_isk": 2_000_000_000.0,
        "industry_builder_portfolio_slots_available": 10,
        "industry_builder_portfolio_horizon_hours": 24.0,
        "industry_builder_portfolio_objective": "balanced",
        "industry_builder_portfolio_minimum_confidence": "low",
        "industry_builder_portfolio_candidates_payload": {},
        "industry_builder_portfolio_plan": {},
        _PORTFOLIO_PLAN_REQUEST_KEY: {},
        _PORTFOLIO_CANDIDATE_DIRECTIVES_KEY: {},
        "industry_builder_portfolio_candidate_categories": [],
        "industry_builder_portfolio_candidate_meta_groups": [],
        "industry_builder_portfolio_candidate_confidences": [],
        "industry_builder_portfolio_candidate_blueprint_sources": [],
        "industry_builder_portfolio_candidate_positive_profit_only": False,
        "industry_builder_portfolio_candidate_min_margin_pct": 0.0,
        "industry_builder_portfolio_candidate_min_isk_per_hour": 0.0,
        "industry_builder_portfolio_candidate_min_region_daily_volume": 0,
        "industry_builder_portfolio_candidate_min_owned_input_coverage_pct": 0.0,
    }
    for key, value in planner_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    _ensure_portfolio_candidate_job_state()

    if _portfolio_candidate_job_is_active():
        try:
            _poll_portfolio_candidate_job()
        except Exception as exc:
            _clear_portfolio_candidate_job(error_message=str(exc))

    candidate_job_view = _portfolio_candidate_job_view()

    if candidate_job_view.get("error_message"):
        st.error(str(candidate_job_view.get("error_message")))

    if bool(candidate_job_view.get("is_active")):
        _render_portfolio_candidate_progress(candidate_job_view)
        time.sleep(1.0)
        st.rerun()

    candidates_payload = cast(dict[str, Any], st.session_state.get("industry_builder_portfolio_candidates_payload") or {})
    plan = cast(dict[str, Any], st.session_state.get("industry_builder_portfolio_plan") or {})

    st.markdown("**1. Load Candidate Snapshot**")
    st.caption("Build one explicit candidate snapshot from the current planning context and advanced snapshot settings. Portfolio planning will use this exact snapshot.")

    load_col, snapshot_col = st.columns([1, 3])
    with load_col:
        if st.button("Load Candidate Snapshot", key="industry_builder_load_portfolio_candidates"):
            try:
                st.session_state["industry_builder_portfolio_candidates_payload"] = {}
                st.session_state["industry_builder_portfolio_plan"] = {}
                st.session_state[_PORTFOLIO_PLAN_REQUEST_KEY] = {}
                st.session_state[_PORTFOLIO_CANDIDATE_DIRECTIVES_KEY] = {}
                _start_candidate_job(
                    default_character_id_value=default_character_id_value,
                    default_owned_blueprint_scope=default_owned_blueprint_scope,
                )
                st.rerun()
            except Exception as exc:
                st.error(f"Failed to load portfolio candidates: {exc}")
    with snapshot_col:
        if not candidates_payload:
            st.info("No candidate snapshot loaded yet.")
        else:
            meta = _loaded_snapshot_meta(candidates_payload)
            summary = cast(dict[str, Any], meta.get("summary") or {})
            st.info(_snapshot_summary_text(candidates_payload))
            metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)
            metric_col_1.metric("Candidate Rows", int(summary.get("candidate_count") or 0))
            metric_col_2.metric("Portfolio Eligible", int(summary.get("portfolio_candidate_count") or 0))
            metric_col_3.metric("Planning Horizon", f"{float(summary.get('planning_horizon_hours') or 0.0):,.0f} h")
            metric_col_4.metric("Snapshot Rows", int(meta.get("snapshot_result_count") or 0))

    st.markdown("**2. Narrow Candidate Scope**")
    st.caption("Use these filters to decide which rows remain eligible for the plan. This scope changes both the visible table and the backend plan input.")
    filtered_candidates = _render_candidate_workspace(candidates_payload) if candidates_payload else []
    if not candidates_payload:
        st.info("Load a candidate snapshot to review and narrow the scope.")
    elif not filtered_candidates:
        st.info("No candidates remain in the current scope. Adjust the candidate scope filters or reload the snapshot.")
    elif not _snapshot_matches_current_horizon(candidates_payload):
        st.warning(
            "The loaded snapshot was built for {snapshot_horizon:.0f}h, but the current planning horizon is {current_horizon:.0f}h. Reload the candidate snapshot before building the plan.".format(
                snapshot_horizon=_loaded_snapshot_horizon_hours(candidates_payload),
                current_horizon=_current_planning_horizon_hours(),
            )
        )

    loaded_snapshot_id = _loaded_snapshot_id(candidates_payload)
    current_plan_request: dict[str, Any] = {}
    if loaded_snapshot_id:
        request_params = current_overview_request_params(
            default_character_id_value=default_character_id_value,
            default_owned_blueprint_scope=default_owned_blueprint_scope,
        )
        current_plan_request = _portfolio_plan_request_payload(
            character_id=int(request_params.get("character_id") or default_character_id_value),
            candidate_snapshot_id=loaded_snapshot_id,
            candidates=cast(list[dict[str, Any]], candidates_payload.get("candidates") or []),
        )

    st.markdown("**3. Build Portfolio Plan**")
    st.caption("Constraints define the feasible plan. Objective defines how the in-scope candidates are ranked before capital and slot hours are allocated.")

    constraints_col, preference_col, action_col = st.columns([2, 1, 1])
    with constraints_col:
        st.markdown("**Constraints**")
        st.number_input(
            "Capital Limit (ISK)",
            min_value=0.0,
            step=100_000_000.0,
            key="industry_builder_portfolio_capital_limit_isk",
        )
        st.number_input(
            "Manufacturing Slots",
            min_value=1,
            step=1,
            key="industry_builder_portfolio_slots_available",
        )
        st.number_input(
            "Planning Horizon (Hours)",
            min_value=1.0,
            step=1.0,
            key="industry_builder_portfolio_horizon_hours",
        )
        st.selectbox(
            "Minimum Pricing Confidence",
            options=["low", "medium", "high"],
            key="industry_builder_portfolio_minimum_confidence",
        )
    with preference_col:
        st.markdown("**Ranking Preference**")
        st.selectbox(
            "Objective",
            options=["balanced", "max_profit", "max_isk_per_hour"],
            key="industry_builder_portfolio_objective",
        )
    with action_col:
        st.markdown("**Run**")
        build_disabled = (
            bool(candidate_job_view.get("is_active"))
            or not loaded_snapshot_id
            or (not filtered_candidates and _operator_candidate_count(cast(list[dict[str, Any]], candidates_payload.get("candidates") or [])) <= 0)
            or not _snapshot_matches_current_horizon(candidates_payload)
        )
        if st.button(
            "Build Portfolio Plan",
            key="industry_builder_build_portfolio_plan",
            disabled=build_disabled,
        ):
            try:
                st.session_state["industry_builder_portfolio_plan"] = fetch_portfolio_plan(
                    **current_plan_request
                )
                st.session_state[_PORTFOLIO_PLAN_REQUEST_KEY] = dict(current_plan_request)
            except Exception as exc:
                st.error(f"Failed to build portfolio plan: {exc}")

        if not loaded_snapshot_id:
            st.caption("Load a candidate snapshot first.")
        elif not filtered_candidates and _operator_candidate_count(cast(list[dict[str, Any]], candidates_payload.get("candidates") or [])) <= 0:
            st.caption("Adjust the candidate scope so at least one row remains in scope.")
        elif not _snapshot_matches_current_horizon(candidates_payload):
            st.caption("Reload the candidate snapshot so it matches the current planning horizon.")

    if not plan:
        st.info("No portfolio plan has been built yet.")
        return

    if not _plan_request_is_current(current_plan_request):
        st.warning(
            "The visible plan is stale. Candidate scope or planner inputs changed after the last build. Rebuild the portfolio plan to refresh the recommendation."
        )

    st.info(
        "Plan provenance: snapshot {snapshot} | pricing {pricing} | scope {scope}".format(
            snapshot=str(plan.get("candidate_snapshot_id") or "ad hoc")[:8] or "ad hoc",
            pricing=str(((plan.get("pricing_batch") or {}).get("generated_at") or "N/A")),
            scope=int(plan.get("candidate_scope_count") or 0),
        )
    )

    metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)
    metric_col_1.metric("Selected Items", int(plan.get("selected_count") or 0))
    metric_col_2.metric("Expected Profit", f"{float(plan.get('total_expected_profit') or 0.0):,.0f} ISK")
    metric_col_3.metric("Capital Committed", f"{float(plan.get('capital_committed') or 0.0):,.0f} ISK")
    metric_col_4.metric("Slot Hours", f"{float(plan.get('slot_hours_committed') or 0.0):,.1f}")

    st.caption(
        "Objective: {objective} | Horizon: {horizon:.0f}h | Candidate scope: {candidates} | Min confidence: {confidence}".format(
            objective=str(plan.get("objective") or "balanced"),
            horizon=float(plan.get("planning_horizon_hours") or 0.0),
            candidates=int(plan.get("candidate_scope_count") or 0),
            confidence=str(plan.get("minimum_pricing_confidence") or "low"),
        )
    )
    if _plan_request_is_current(current_plan_request):
        st.caption("The final plan uses the current planner inputs above, the current backend optimizer, and the active candidate scope filters.")
    else:
        st.caption("The visible plan was built from an older planner state. Rebuild to apply the current planner inputs and candidate scope.")

    operator_decisions = cast(dict[str, Any], plan.get("operator_decisions") or {})
    decisions_summary_df = _planner_decisions_summary_frame(operator_decisions)
    if not decisions_summary_df.empty:
        st.markdown("**Planner decisions**")
        st.dataframe(decisions_summary_df, width="stretch", hide_index=True)
    unfulfilled_locked_items = cast(list[dict[str, Any]], operator_decisions.get("unfulfilled_locked_items") or [])
    if unfulfilled_locked_items:
        st.warning("One or more locked items could not fit in the final plan. See the planner decisions details below.")
    if operator_decisions:
        with st.expander("Planner decisions details", expanded=False):
            for section_title, section_key in [
                ("Forced includes", "forced_includes"),
                ("Exclusions", "exclusions"),
                ("Locked items", "locked_items"),
                ("Overrides", "override_items"),
                ("Unfulfilled locked items", "unfulfilled_locked_items"),
            ]:
                section_rows = cast(list[dict[str, Any]], operator_decisions.get(section_key) or [])
                if not section_rows:
                    continue
                st.markdown(f"**{section_title}**")
                st.dataframe(section_rows, width="stretch", hide_index=True)

    selected_items = cast(list[dict[str, Any]], plan.get("selected_items") or [])
    if selected_items:
        st.markdown("**Recommended queue**")
        st.dataframe(selected_items, width="stretch", hide_index=True)

    skipped_items = cast(list[dict[str, Any]], plan.get("skipped_items") or [])
    if skipped_items:
        skipped_summary_df = _skip_reason_summary_frame(skipped_items)
        if not skipped_summary_df.empty:
            st.markdown("**Skipped summary**")
            st.dataframe(skipped_summary_df, width="stretch", hide_index=True)
        with st.expander("Skipped items", expanded=False):
            st.dataframe(skipped_items, width="stretch", hide_index=True)