from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.config.settings import public_structures_cache_ttl_seconds as _public_structures_cache_ttl_seconds
from eve_online_industry_tracker.infrastructure import blueprints_service
from eve_online_industry_tracker.infrastructure import industry_builder_service
from eve_online_industry_tracker.infrastructure import public_structures_cache_service
from eve_online_industry_tracker.infrastructure.persistence import corporation_structures_repo
from eve_online_industry_tracker.infrastructure.persistence import industry_profiles_repo
from eve_online_industry_tracker.infrastructure.sde.locations import get_npc_stations, get_solar_systems
from eve_online_industry_tracker.infrastructure.sde.rig_effects import get_rig_effects_for_type_ids
from eve_online_industry_tracker.infrastructure.sde.types import get_type_data
from eve_online_industry_tracker.infrastructure.submanufacturing_planner_service import plan_submanufacturing_tree


def get_blueprint_assets(
    db_app_session: Any,
    esi_service: Any,
    *,
    sde_session: Any,
    language: str,
    include_unowned: bool,
) -> list[dict]:
    return blueprints_service.get_blueprint_assets(
        db_app_session,
        esi_service=esi_service,
        sde_session=sde_session,
        language=language,
        include_unowned=include_unowned,
    )


def enrich_blueprints_for_character(
    all_blueprints: list[dict],
    character: Any,
    *,
    esi_service: Any,
    industry_profile: Any,
    manufacturing_system_cost_index: float,
    copying_system_cost_index: float,
    research_me_system_cost_index: float,
    research_te_system_cost_index: float,
    surcharge_rate_total_fraction: float,
    owned_blueprint_type_ids: set[int],
    owned_blueprint_best_by_type_id: dict[int, dict],
    include_submanufacturing: bool,
    submanufacturing_blueprint_type_id: int | None,
    progress_callback=None,
    maximize_blueprint_runs: bool,
    rig_payload: list[dict],
    db_app_session: Any,
    db_sde_session: Any,
    language: str,
    pricing_preferences: dict | None = None,
    prefer_inventory_consumption: bool | None = None,
    assume_bpo_copy_overhead: bool | None = None,
    esi_market_prices: list[dict] | None = None,
    market_price_map: dict[int, dict[str, float | None]] | None = None,
) -> Any:
    # Compatibility shim: `IndustryService` passes these newer kwargs.
    # - `prefer_inventory_consumption` controls planner behavior (consume on-hand FIFO first).
    # - The others are currently unused by the builder service but accepted to avoid crashes.
    _ = assume_bpo_copy_overhead
    _ = esi_market_prices
    _ = market_price_map

    prefer_inventory_consumption_b = True if prefer_inventory_consumption is None else bool(prefer_inventory_consumption)
    # FIFO lots/valuation remain enabled; the preference only affects whether inventory is forced
    # to be consumed before making build-vs-buy decisions.
    use_fifo_inventory_costing = True
    return industry_builder_service.enrich_blueprints_for_character(
        all_blueprints,
        character,
        esi_service=esi_service,
        industry_profile=industry_profile,
        manufacturing_system_cost_index=manufacturing_system_cost_index,
        copying_system_cost_index=copying_system_cost_index,
        research_me_system_cost_index=research_me_system_cost_index,
        research_te_system_cost_index=research_te_system_cost_index,
        surcharge_rate_total_fraction=surcharge_rate_total_fraction,
        owned_blueprint_type_ids=owned_blueprint_type_ids,
        owned_blueprint_best_by_type_id=owned_blueprint_best_by_type_id,
        include_submanufacturing=include_submanufacturing,
        submanufacturing_blueprint_type_id=submanufacturing_blueprint_type_id,
        progress_callback=progress_callback,
        maximize_blueprint_runs=maximize_blueprint_runs,
        rig_payload=rig_payload,
        db_app_session=db_app_session,
        db_sde_session=db_sde_session,
        language=language,
        use_fifo_inventory_costing=use_fifo_inventory_costing,
        prefer_inventory_consumption=prefer_inventory_consumption_b,
        pricing_preferences=(pricing_preferences if isinstance(pricing_preferences, dict) else None),
    )


def get_cached_public_structures(*, state: Any, system_id: int, ttl_seconds: int) -> tuple[list[dict], bool]:
    return public_structures_cache_service.get_cached_public_structures(
        state=state,
        system_id=system_id,
        ttl_seconds=int(ttl_seconds),
    )


def trigger_refresh_public_structures_for_system(*, state: Any, system_id: int) -> bool:
    return public_structures_cache_service.trigger_refresh_public_structures_for_system(
        state=state,
        system_id=int(system_id),
    )


def public_structures_cache_ttl_seconds() -> int:
    return int(_public_structures_cache_ttl_seconds())


def corporation_structures_list_by_corporation_id(db_app_session: Any, corporation_id: int) -> list[Any]:
    return corporation_structures_repo.list_by_corporation_id(db_app_session, corporation_id)


def industry_profile_get_by_id(db_app_session: Any, profile_id: int) -> Any:
    return industry_profiles_repo.get_by_id(db_app_session, profile_id)


def industry_profile_get_default_for_character_id(db_app_session: Any, character_id: int) -> Any:
    return industry_profiles_repo.get_default_for_character_id(db_app_session, character_id)


def industry_profile_list_by_character_id(db_app_session: Any, character_id: int) -> list[Any]:
    return industry_profiles_repo.list_by_character_id(db_app_session, character_id)


def industry_profile_create(db_app_session: Any, data: dict) -> int:
    return industry_profiles_repo.create(db_app_session, data)


def industry_profile_update(db_app_session: Any, profile_id: int, data: dict) -> None:
    industry_profiles_repo.update(db_app_session, profile_id, data)


def industry_profile_delete(db_app_session: Any, profile_id: int) -> None:
    industry_profiles_repo.delete(db_app_session, profile_id)
