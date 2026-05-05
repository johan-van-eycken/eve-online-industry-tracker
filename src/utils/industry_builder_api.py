from __future__ import annotations

from typing import Any

import streamlit as st

from utils.flask_api import api_get, api_post
from utils.streamlit_api import cached_api_get


@st.cache_data(ttl=300, show_spinner=False)
def fetch_product_overview(
    *,
    force_refresh: bool = False,
    maximize_bp_runs: bool = False,
    group_identical_bpcs: bool = True,
    build_from_bpc: bool = True,
    have_blueprint_source_only: bool = True,
    include_reactions: bool = False,
    market_hub: str = "jita",
    material_price_side: str = "sell",
    product_price_side: str = "sell",
    industry_profile_id: int | None = None,
    owned_blueprints_scope: str = "all_characters",
    character_id: int,
) -> list[dict[str, Any]]:
    path = (
        f"/industry_products/{int(character_id)}"
        f"?maximize_bp_runs={1 if maximize_bp_runs else 0}"
        f"&group_identical_bpcs={1 if group_identical_bpcs else 0}"
        f"&build_from_bpc={1 if build_from_bpc else 0}"
        f"&have_blueprint_source_only={1 if have_blueprint_source_only else 0}"
        f"&include_reactions={1 if include_reactions else 0}"
        f"&market_hub={market_hub}"
        f"&material_price_side={material_price_side}"
        f"&product_price_side={product_price_side}"
        f"&owned_blueprints_scope={owned_blueprints_scope}"
    )
    if industry_profile_id is not None and int(industry_profile_id) > 0:
        path += f"&industry_profile_id={int(industry_profile_id)}"
    if force_refresh:
        path += "&refresh=1"

    response = api_get(path, timeout_seconds=120 if force_refresh else 60) or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to load industry product overview")

    data = response.get("data") or []
    return data if isinstance(data, list) else []


@st.cache_data(ttl=30, show_spinner=False)
def fetch_job_manager_status() -> dict[str, Any]:
    response = api_get("/industry_job_manager/status") or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to load industry job manager status")

    data = response.get("data") or {}
    return data if isinstance(data, dict) else {}


def start_product_overview_refresh(
    *,
    maximize_bp_runs: bool,
    group_identical_bpcs: bool,
    build_from_bpc: bool,
    have_blueprint_source_only: bool,
    include_reactions: bool,
    market_hub: str,
    material_price_side: str,
    product_price_side: str,
    industry_profile_id: int | None,
    owned_blueprints_scope: str,
    character_id: int,
) -> dict[str, Any]:
    response = api_post(
        f"/industry_products/{int(character_id)}/refresh",
        {
            "force_refresh": True,
            "maximize_bp_runs": bool(maximize_bp_runs),
            "group_identical_bpcs": bool(group_identical_bpcs),
            "build_from_bpc": bool(build_from_bpc),
            "have_blueprint_source_only": bool(have_blueprint_source_only),
            "include_reactions": bool(include_reactions),
            "market_hub": str(market_hub),
            "material_price_side": str(material_price_side),
            "product_price_side": str(product_price_side),
            "industry_profile_id": int(industry_profile_id) if industry_profile_id is not None else None,
            "owned_blueprints_scope": str(owned_blueprints_scope),
        },
    ) or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to start industry product overview refresh")

    data = response.get("data") or {}
    return data if isinstance(data, dict) else {}


def fetch_product_overview_refresh_status(job_id: str) -> dict[str, Any]:
    response = api_get(f"/industry_products/refresh/{job_id}", timeout_seconds=30) or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to load industry product overview refresh status")

    data = response.get("data") or {}
    return data if isinstance(data, dict) else {}


@st.cache_data(ttl=60, show_spinner=False)
def fetch_portfolio_candidates(
    *,
    maximize_bp_runs: bool = False,
    group_identical_bpcs: bool = True,
    build_from_bpc: bool = True,
    have_blueprint_source_only: bool = True,
    include_reactions: bool = False,
    market_hub: str = "jita",
    material_price_side: str = "sell",
    product_price_side: str = "sell",
    industry_profile_id: int | None = None,
    owned_blueprints_scope: str = "all_characters",
    character_id: int = 0,
    planning_horizon_hours: float = 24.0,
) -> dict[str, Any]:
    path = (
        f"/industry_products/{int(character_id)}/portfolio_candidates"
        f"?maximize_bp_runs={1 if maximize_bp_runs else 0}"
        f"&group_identical_bpcs={1 if group_identical_bpcs else 0}"
        f"&build_from_bpc={1 if build_from_bpc else 0}"
        f"&have_blueprint_source_only={1 if have_blueprint_source_only else 0}"
        f"&include_reactions={1 if include_reactions else 0}"
        f"&market_hub={market_hub}"
        f"&material_price_side={material_price_side}"
        f"&product_price_side={product_price_side}"
        f"&owned_blueprints_scope={owned_blueprints_scope}"
        f"&planning_horizon_hours={planning_horizon_hours}"
    )
    if industry_profile_id is not None and int(industry_profile_id) > 0:
        path += f"&industry_profile_id={int(industry_profile_id)}"
    response = api_get(path, timeout_seconds=60) or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to load industry portfolio candidates")
    data = response.get("data") or []
    meta = response.get("meta") or {}
    return {
        "candidates": data if isinstance(data, list) else [],
        "meta": meta if isinstance(meta, dict) else {},
    }


@st.cache_data(ttl=60, show_spinner=False)
def fetch_portfolio_plan(
    *,
    maximize_bp_runs: bool,
    group_identical_bpcs: bool,
    build_from_bpc: bool,
    have_blueprint_source_only: bool,
    include_reactions: bool,
    market_hub: str,
    material_price_side: str,
    product_price_side: str,
    industry_profile_id: int | None,
    owned_blueprints_scope: str,
    character_id: int,
    planning_horizon_hours: float,
    capital_limit_isk: float,
    manufacturing_slots_available: int,
    objective: str,
    positive_profit_only: bool,
    min_margin_pct: float,
    min_isk_per_hour: float,
    min_region_daily_volume: int,
    minimum_pricing_confidence: str,
) -> dict[str, Any]:
    response = api_post(
        f"/industry_products/{int(character_id)}/portfolio_plan",
        {
            "maximize_bp_runs": bool(maximize_bp_runs),
            "group_identical_bpcs": bool(group_identical_bpcs),
            "build_from_bpc": bool(build_from_bpc),
            "have_blueprint_source_only": bool(have_blueprint_source_only),
            "include_reactions": bool(include_reactions),
            "market_hub": str(market_hub),
            "material_price_side": str(material_price_side),
            "product_price_side": str(product_price_side),
            "industry_profile_id": int(industry_profile_id) if industry_profile_id is not None else None,
            "owned_blueprints_scope": str(owned_blueprints_scope),
            "planning_horizon_hours": float(planning_horizon_hours),
            "capital_limit_isk": float(capital_limit_isk),
            "manufacturing_slots_available": int(manufacturing_slots_available),
            "objective": str(objective),
            "positive_profit_only": bool(positive_profit_only),
            "min_margin_pct": float(min_margin_pct),
            "min_isk_per_hour": float(min_isk_per_hour),
            "min_region_daily_volume": int(min_region_daily_volume),
            "minimum_pricing_confidence": str(minimum_pricing_confidence),
        },
    ) or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to build portfolio plan")
    data = response.get("data") or {}
    return data if isinstance(data, dict) else {}


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_solar_system_security_map() -> dict[int, float]:
    response = cached_api_get("/solar_systems") or {}
    if response.get("status") != "success":
        raise RuntimeError(response.get("message") or "Failed to load solar systems")

    data = response.get("data") or []
    out: dict[int, float] = {}
    for entry in data:
        if not isinstance(entry, dict):
            continue
        try:
            system_id = int(entry.get("id") or 0)
            security_status = float(entry.get("security_status") or 0.0)
        except Exception:
            continue
        if system_id > 0:
            out[system_id] = security_status
    return out


def clear_industry_builder_caches() -> None:
    fetch_product_overview.clear()
    fetch_portfolio_candidates.clear()
    fetch_portfolio_plan.clear()
