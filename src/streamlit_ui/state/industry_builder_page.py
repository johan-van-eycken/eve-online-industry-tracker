from __future__ import annotations

from typing import Any, Callable, cast

import streamlit as st

from streamlit_ui.api.industry_profiles import fetch_industry_profiles
from streamlit_ui.state.session_state import ensure_state_defaults, ensure_valid_state_value


_REFRESH_JOB_ID_KEY = "industry_builder_refresh_job_id"
_REFRESH_PROGRESS_FRACTION_KEY = "industry_builder_refresh_progress_fraction"
_REFRESH_PROGRESS_LABEL_KEY = "industry_builder_refresh_progress_label"
_REFRESH_ERROR_KEY = "industry_builder_refresh_error"
_REFRESH_CREATED_AT_KEY = "industry_builder_refresh_created_at"
_REFRESH_UPDATED_AT_KEY = "industry_builder_refresh_updated_at"
_REFRESH_PROGRESS_META_KEY = "industry_builder_refresh_progress_meta"
_PREFERENCES_NAMESPACE = "industry_builder"
_MISC_SETTING_DEFAULTS: dict[str, bool] = {
    "industry_builder_maximize_bp_runs_pending": True,
    "industry_builder_group_identical_bpcs": False,
    "industry_builder_build_from_bpc": True,
    "industry_builder_have_blueprint_source_only": True,
    "industry_builder_have_skills_only": True,
    "industry_builder_include_reactions": False,
}
_MARKET_SETTING_DEFAULTS: dict[str, str] = {
    "industry_builder_market_hub": "jita",
    "industry_builder_material_price_side": "sell",
    "industry_builder_product_price_side": "sell",
}
_PROFIT_FILTER_DEFAULTS: dict[str, Any] = {
    "industry_builder_positive_profit_only": True,
    "industry_builder_min_margin_pct": 0.0,
    "industry_builder_min_isk_per_hour": 0.0,
    "industry_builder_min_region_daily_volume": 0,
}


@st.cache_data(ttl=300, show_spinner=False)
def fetch_industry_profiles_cached(*, character_id: int) -> list[dict[str, Any]]:
    profiles = fetch_industry_profiles(character_id=int(character_id))
    return cast(list[dict[str, Any]], profiles)


def default_character_id(characters: list[dict[str, Any]], character_options: dict[int, str]) -> int:
    character_ids = list(character_options.keys())
    selected_default_character_id = character_ids[0]
    for character in characters:
        if not isinstance(character, dict):
            continue
        if not bool(character.get("is_main")):
            continue
        try:
            main_character_id = int(character.get("character_id") or 0)
        except Exception:
            continue
        if main_character_id in character_options:
            selected_default_character_id = main_character_id
            break
    return selected_default_character_id


def ensure_selection_state(
    *,
    character_options: dict[int, str],
    default_character_id_value: int,
    owned_blueprint_scope_options: list[str],
    default_owned_blueprint_scope: str,
) -> None:
    ensure_valid_state_value(
        "industry_builder_owned_blueprints_scope",
        default_owned_blueprint_scope,
        valid_values=owned_blueprint_scope_options,
        coerce=str,
    )
    ensure_valid_state_value(
        "industry_builder_character_id",
        int(default_character_id_value),
        valid_values=list(character_options.keys()),
        coerce=int,
    )
    ensure_state_defaults(
        {
            "industry_builder_owned_blueprints_scope_applied": default_owned_blueprint_scope,
            "industry_builder_character_id_applied": int(default_character_id_value),
            "industry_builder_industry_profile_id_applied": 0,
        }
    )




def ensure_refresh_state() -> None:
    ensure_state_defaults(
        {
            _REFRESH_JOB_ID_KEY: "",
            _REFRESH_PROGRESS_FRACTION_KEY: 0.0,
            _REFRESH_PROGRESS_LABEL_KEY: "",
            _REFRESH_ERROR_KEY: None,
            _REFRESH_CREATED_AT_KEY: None,
            _REFRESH_UPDATED_AT_KEY: None,
            _REFRESH_PROGRESS_META_KEY: {},
        }
    )


def ensure_overview_refresh_state() -> None:
    ensure_refresh_state()
    ensure_state_defaults({"industry_builder_overview_meta": {}})


def resolve_profile_security_status(
    *,
    industry_profiles: list[dict[str, Any]],
    selected_industry_profile_id: int,
    solar_system_security_map: dict[int, float],
) -> float | None:
    selected_profile_system_id: int | None = None
    for profile in industry_profiles:
        if int(profile.get("id") or 0) != int(selected_industry_profile_id):
            continue
        try:
            selected_profile_system_id = int(profile.get("system_id") or 0) or None
        except Exception:
            selected_profile_system_id = None
        break

    if not selected_profile_system_id:
        return None
    return float(solar_system_security_map.get(selected_profile_system_id, 0.0))


def current_overview_request_params(
    *,
    default_character_id_value: int,
    default_owned_blueprint_scope: str,
) -> dict[str, Any]:
    return {
        "maximize_bp_runs": bool(st.session_state.get("industry_builder_maximize_bp_runs_applied", False)),
        "group_identical_bpcs": bool(st.session_state.get("industry_builder_group_identical_bpcs_applied", True)),
        "build_from_bpc": bool(st.session_state.get("industry_builder_build_from_bpc_applied", True)),
        "have_blueprint_source_only": bool(
            st.session_state.get("industry_builder_have_blueprint_source_only_applied", True)
        ),
        "include_reactions": bool(st.session_state.get("industry_builder_include_reactions_applied", False)),
        "market_hub": str(st.session_state.get("industry_builder_market_hub_applied", "jita") or "jita"),
        "material_price_side": str(
            st.session_state.get("industry_builder_material_price_side_applied", "sell") or "sell"
        ),
        "product_price_side": str(
            st.session_state.get("industry_builder_product_price_side_applied", "sell") or "sell"
        ),
        "industry_profile_id": int(st.session_state.get("industry_builder_industry_profile_id_applied", 0)) or None,
        "owned_blueprints_scope": str(
            st.session_state.get("industry_builder_owned_blueprints_scope_applied", default_owned_blueprint_scope)
        ),
        "character_id": int(st.session_state.get("industry_builder_character_id_applied", default_character_id_value)),
    }


def apply_pending_overview_request_params(
    *,
    default_character_id_value: int,
    default_industry_profile_id: int,
    default_owned_blueprint_scope: str,
    reactions_allowed_for_profile: bool,
) -> dict[str, Any]:
    st.session_state["industry_builder_maximize_bp_runs_applied"] = bool(
        st.session_state.get("industry_builder_maximize_bp_runs_pending", False)
    )
    st.session_state["industry_builder_group_identical_bpcs_applied"] = bool(
        st.session_state.get("industry_builder_group_identical_bpcs", True)
    )
    st.session_state["industry_builder_build_from_bpc_applied"] = bool(
        st.session_state.get("industry_builder_build_from_bpc", True)
    )
    st.session_state["industry_builder_have_blueprint_source_only_applied"] = bool(
        st.session_state.get("industry_builder_have_blueprint_source_only", True)
    )
    st.session_state["industry_builder_include_reactions_applied"] = (
        bool(st.session_state.get("industry_builder_include_reactions", False)) and reactions_allowed_for_profile
    )
    st.session_state["industry_builder_market_hub_applied"] = str(
        st.session_state.get("industry_builder_market_hub", "jita") or "jita"
    )
    st.session_state["industry_builder_material_price_side_applied"] = str(
        st.session_state.get("industry_builder_material_price_side", "sell") or "sell"
    )
    st.session_state["industry_builder_product_price_side_applied"] = str(
        st.session_state.get("industry_builder_product_price_side", "sell") or "sell"
    )
    st.session_state["industry_builder_owned_blueprints_scope_applied"] = str(
        st.session_state.get("industry_builder_owned_blueprints_scope", default_owned_blueprint_scope)
    )
    st.session_state["industry_builder_character_id_applied"] = int(
        st.session_state.get("industry_builder_character_id", default_character_id_value)
    )
    st.session_state["industry_builder_industry_profile_id_applied"] = int(
        st.session_state.get("industry_builder_industry_profile_id", default_industry_profile_id)
    )
    return current_overview_request_params(
        default_character_id_value=default_character_id_value,
        default_owned_blueprint_scope=default_owned_blueprint_scope,
    )


def start_overview_refresh_job(
    *,
    default_character_id_value: int,
    default_industry_profile_id: int,
    default_owned_blueprint_scope: str,
    reactions_allowed_for_profile: bool,
    start_refresh_fn: Callable[..., dict[str, Any]],
) -> None:
    refresh_params = apply_pending_overview_request_params(
        default_character_id_value=default_character_id_value,
        default_industry_profile_id=default_industry_profile_id,
        default_owned_blueprint_scope=default_owned_blueprint_scope,
        reactions_allowed_for_profile=reactions_allowed_for_profile,
    )
    refresh_job = start_refresh_fn(**refresh_params)
    refresh_job_id = str(refresh_job.get("job_id") or "")
    if not refresh_job_id:
        raise RuntimeError("Refresh job did not return a job_id")

    st.session_state[_REFRESH_JOB_ID_KEY] = refresh_job_id
    st.session_state[_REFRESH_PROGRESS_FRACTION_KEY] = 0.0
    st.session_state[_REFRESH_PROGRESS_LABEL_KEY] = "Starting overview refresh..."
    st.session_state[_REFRESH_ERROR_KEY] = None
    st.session_state[_REFRESH_CREATED_AT_KEY] = refresh_job.get("created_at")
    st.session_state[_REFRESH_UPDATED_AT_KEY] = refresh_job.get("updated_at")
    st.session_state[_REFRESH_PROGRESS_META_KEY] = refresh_job.get("progress_meta") or {}


def clear_overview_refresh_job(*, error_message: str | None = None) -> None:
    st.session_state[_REFRESH_JOB_ID_KEY] = ""
    st.session_state[_REFRESH_ERROR_KEY] = error_message
    st.session_state[_REFRESH_PROGRESS_META_KEY] = {}


def poll_overview_refresh_job(
    *,
    fetch_status_fn: Callable[[str], dict[str, Any]],
    fetch_job_manager_status_fn: Callable[[], dict[str, Any]],
) -> str:
    refresh_job_id = str(st.session_state.get(_REFRESH_JOB_ID_KEY) or "")
    if not refresh_job_id:
        return "idle"

    refresh_status = fetch_status_fn(refresh_job_id)
    if refresh_status is None:
        return "running"
    progress_fraction = float(refresh_status.get("progress_fraction") or 0.0)
    progress_label = str(refresh_status.get("progress_label") or "Refreshing overview...")
    st.session_state[_REFRESH_PROGRESS_FRACTION_KEY] = max(0.0, min(1.0, progress_fraction))
    st.session_state[_REFRESH_PROGRESS_LABEL_KEY] = progress_label
    st.session_state[_REFRESH_CREATED_AT_KEY] = refresh_status.get("created_at")
    st.session_state[_REFRESH_UPDATED_AT_KEY] = refresh_status.get("updated_at")
    st.session_state[_REFRESH_PROGRESS_META_KEY] = refresh_status.get("progress_meta") or {}

    status = str(refresh_status.get("status") or "")
    if status == "completed":
        result = refresh_status.get("result") or []
        st.session_state["industry_builder_overview_rows"] = result if isinstance(result, list) else []
        st.session_state["industry_builder_overview_meta"] = refresh_status.get("result_meta") or {}
        st.session_state["industry_builder_job_manager_status"] = fetch_job_manager_status_fn()
        clear_overview_refresh_job()
        return "completed"

    if status == "failed":
        clear_overview_refresh_job(error_message=str(refresh_status.get("error_message") or "Refresh job failed"))
        return "failed"

    return "running"


def overview_refresh_is_active() -> bool:
    return bool(str(st.session_state.get(_REFRESH_JOB_ID_KEY) or ""))


def overview_refresh_view() -> dict[str, Any]:
    return {
        "is_active": overview_refresh_is_active(),
        "progress_fraction": float(st.session_state.get(_REFRESH_PROGRESS_FRACTION_KEY) or 0.0),
        "progress_label": str(st.session_state.get(_REFRESH_PROGRESS_LABEL_KEY) or ""),
        "error_message": st.session_state.get(_REFRESH_ERROR_KEY),
        "created_at": st.session_state.get(_REFRESH_CREATED_AT_KEY),
        "updated_at": st.session_state.get(_REFRESH_UPDATED_AT_KEY),
        "progress_meta": st.session_state.get(_REFRESH_PROGRESS_META_KEY) or {},
    }