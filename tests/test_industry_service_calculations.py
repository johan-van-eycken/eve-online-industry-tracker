from __future__ import annotations

from types import MethodType, SimpleNamespace
from typing import Any, cast

import pytest

from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService


def _build_service(
    *,
    blueprint_rows: list[dict],
    profile: dict | None,
    character_modifiers: dict | None,
    trained_skill_levels: dict[int, int],
    owned_assets: tuple[list[object], list[object], dict[int, str], dict[int, str], dict[int, str]],
    owned_item_inventory: tuple[dict[int, int], dict[int, float]] | None = None,
    adjusted_price_map: dict[int, dict],
) -> IndustryService:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace(esi_service=None)  # type: ignore[attr-defined]
    service._sessions = None  # type: ignore[attr-defined]

    class DummyManager:
        def get_blueprint_overview(self, *, force_refresh: bool = False) -> list[dict]:
            return blueprint_rows

    service._ensure_industry_job_manager = MethodType(lambda self: DummyManager(), service)  # type: ignore[attr-defined]
    service._get_character_trained_skill_levels = MethodType(  # type: ignore[attr-defined]
        lambda self, *, character_id: trained_skill_levels,
        service,
    )
    service._resolve_industry_profile_context = MethodType(  # type: ignore[attr-defined]
        lambda self, *, character_id, industry_profile_id: profile,
        service,
    )
    service._get_character_industry_modifier_payload = MethodType(  # type: ignore[attr-defined]
        lambda self, *, character_id: character_modifiers,
        service,
    )
    service._get_owned_blueprint_assets = MethodType(  # type: ignore[attr-defined]
        lambda self, *, owned_blueprints_scope: owned_assets,
        service,
    )
    service._get_owned_item_inventory = MethodType(  # type: ignore[attr-defined]
        lambda self, *, owned_blueprints_scope, **kwargs: owned_item_inventory or ({}, {}),
        service,
    )
    service._get_adjusted_market_price_map = MethodType(  # type: ignore[attr-defined]
        lambda self: adjusted_price_map,
        service,
    )
    service._compact_owned_blueprint_asset = MethodType(  # type: ignore[attr-defined]
        lambda self, asset, **kwargs: {} if asset is None else {"item_id": getattr(asset, "item_id", None)},
        service,
    )
    service._enrich_product_rows_with_material_prices = MethodType(  # type: ignore[attr-defined]
        lambda self, product_rows, progress_callback=None, **kwargs: product_rows,
        service,
    )
    service._enrich_product_rows_with_market_activity = MethodType(  # type: ignore[attr-defined]
        lambda self, product_rows, **kwargs: product_rows,
        service,
    )
    service._enrich_product_rows_with_price_anomaly = MethodType(  # type: ignore[attr-defined]
        lambda self, product_rows, **kwargs: product_rows,
        service,
    )
    service._enrich_product_rows_with_pricing_confidence = MethodType(  # type: ignore[attr-defined]
        lambda self, product_rows, **kwargs: product_rows,
        service,
    )
    service._enrich_product_rows_with_sale_proceeds = MethodType(  # type: ignore[attr-defined]
        lambda self, product_rows, **kwargs: product_rows,
        service,
    )
    # `_enrich_product_rows_with_market_trends` needs a live DB session
    # (`self._sessions.app_session()`) to pull pipeline/order-book/historical
    # data. These tests are calculation tests (profit, ME/TE, invention,
    # recursive build-vs-buy) and assert on none of the market-trend fields
    # it writes (price_trend_*, pipeline_*, margin_buffer_pct, etc.), so it is
    # stubbed out here rather than backed by a real session, matching the
    # other enrichment stubs above. It mutates `rows` in place and returns
    # None, so the stub does the same (a no-op).
    service._enrich_product_rows_with_market_trends = MethodType(  # type: ignore[attr-defined]
        lambda self, rows, **kwargs: None,
        service,
    )
    return service


def _blueprint_row() -> dict:
    return {
        "blueprint_type_id": 9001,
        "blueprint": {"type_id": 9001, "type_name": "Test Blueprint", "base_price": 1000.0},
        "manufacturing_job": {
            "materials": [
                {
                    "type_id": 34,
                    "type_name": "Tritanium",
                    "quantity": 10,
                    "base_price": 5.0,
                    "group_name": "Mineral",
                    "category_name": "Material",
                }
            ],
            "skill_entries": [],
            "time_seconds": 100,
            "max_production_limit": 10,
            "products": [
                {
                    "type_id": 5001,
                    "type_name": "Test Module",
                    "quantity": 1,
                    "base_price": 100.0,
                    "group_name": "Shield Boosters",
                    "category_name": "Module",
                    "meta_group_name": "Tech I",
                }
            ],
        },
        "copying_job": {"time_seconds": 80},
        "research_material_job": {"time_seconds": 105},
        "research_time_job": {"time_seconds": 105},
    }


def _expired_blueprint_rows() -> list[dict]:
    valid_row = _blueprint_row()
    expired_row = _blueprint_row()
    expired_row["blueprint_type_id"] = 9002
    expired_row["blueprint"] = {"type_id": 9002, "type_name": "Expired Blueprint", "base_price": 1000.0}
    expired_row["manufacturing_job"] = {
        **dict(expired_row["manufacturing_job"]),
        "products": [
            {
                "type_id": 88202,
                "type_name": "Expired Sanctified Vidette Filament",
                "quantity": 1,
                "base_price": 100.0,
                "group_name": "Filament",
                "category_name": "Abyssal",
                "meta_group_name": "Tech I",
            }
        ],
    }
    return [valid_row, expired_row]


def _recursive_t2_blueprint_rows() -> list[dict]:
    return [
        {
            "blueprint_type_id": 1001,
            "blueprint": {"type_id": 1001, "type_name": "T1 Source Blueprint", "base_price": 1000.0},
            "manufacturing_job": {
                "materials": [
                    {
                        "type_id": 34,
                        "type_name": "Tritanium",
                        "quantity": 5,
                        "base_price": 5.0,
                        "group_name": "Mineral",
                        "category_name": "Material",
                    }
                ],
                "skill_entries": [],
                "time_seconds": 50,
                "max_production_limit": 10,
                "products": [{"type_id": 6001, "type_name": "T1 Source Item", "quantity": 1}],
            },
            "copying_job": {"time_seconds": 20},
            "invention_job": {
                "materials": [
                    {
                        "type_id": 204,
                        "type_name": "Datacore",
                        "quantity": 2,
                        "base_price": 100.0,
                        "group_name": "Datacores",
                        "category_name": "Material",
                    }
                ],
                "skill_entries": [],
                "time_seconds": 40,
                "products": [
                    {
                        "probability_pct": 50.0,
                        "quantity": 1,
                        "product": {"type_id": 9002, "type_name": "T2 Blueprint"},
                    }
                ],
            },
        },
        {
            "blueprint_type_id": 9002,
            "blueprint": {"type_id": 9002, "type_name": "T2 Blueprint", "base_price": 5000.0},
            "manufacturing_job": {
                "materials": [
                    {
                        "type_id": 7001,
                        "type_name": "Reacted Material",
                        "quantity": 2,
                        "base_price": 50.0,
                        "group_name": "Advanced Components",
                        "category_name": "Material",
                    }
                ],
                "skill_entries": [],
                "time_seconds": 100,
                "max_production_limit": 10,
                "products": [
                    {
                        "type_id": 5002,
                        "type_name": "T2 Module",
                        "quantity": 1,
                        "base_price": 1000.0,
                        "group_name": "Shield Boosters",
                        "category_name": "Module",
                        "meta_group_name": "Tech II",
                    }
                ],
            },
        },
        {
            "blueprint_type_id": 8001,
            "blueprint": {"type_id": 8001, "type_name": "Reaction Formula", "base_price": 2500.0},
            "reaction_job": {
                "materials": [
                    {
                        "type_id": 35,
                        "type_name": "Pyerite",
                        "quantity": 3,
                        "base_price": 8.0,
                        "group_name": "Mineral",
                        "category_name": "Material",
                    }
                ],
                "skill_entries": [],
                "time_seconds": 30,
                "products": [
                    {
                        "type_id": 7001,
                        "type_name": "Reacted Material",
                        "quantity": 1,
                        "base_price": 50.0,
                        "group_name": "Reaction",
                        "category_name": "Material",
                    }
                ],
            },
        },
    ]


def _recursive_t2_with_t1_material_blueprint_rows() -> list[dict]:
    return [
        {
            "blueprint_type_id": 1001,
            "blueprint": {"type_id": 1001, "type_name": "T1 Source Blueprint", "base_price": 1000.0},
            "manufacturing_job": {
                "materials": [
                    {
                        "type_id": 34,
                        "type_name": "Tritanium",
                        "quantity": 5,
                        "base_price": 5.0,
                        "group_name": "Mineral",
                        "category_name": "Material",
                    }
                ],
                "skill_entries": [],
                "time_seconds": 50,
                "max_production_limit": 10,
                "products": [
                    {
                        "type_id": 6001,
                        "type_name": "T1 Source Item",
                        "quantity": 1,
                        "base_price": 500.0,
                        "group_name": "Cannons",
                        "category_name": "Module",
                        "meta_group_name": "Tech I",
                    }
                ],
            },
            "copying_job": {"time_seconds": 20},
            "invention_job": {
                "materials": [
                    {
                        "type_id": 204,
                        "type_name": "Datacore",
                        "quantity": 2,
                        "base_price": 100.0,
                        "group_name": "Datacores",
                        "category_name": "Material",
                    }
                ],
                "skill_entries": [],
                "time_seconds": 40,
                "products": [
                    {
                        "probability_pct": 50.0,
                        "quantity": 1,
                        "product": {"type_id": 9002, "type_name": "T2 Blueprint"},
                    }
                ],
            },
        },
        {
            "blueprint_type_id": 9002,
            "blueprint": {"type_id": 9002, "type_name": "T2 Blueprint", "base_price": 5000.0},
            "manufacturing_job": {
                "materials": [
                    {
                        "type_id": 6001,
                        "type_name": "T1 Source Item",
                        "quantity": 2,
                        "base_price": 500.0,
                        "group_name": "Cannons",
                        "category_name": "Module",
                        "meta_group_name": "Tech I",
                    }
                ],
                "skill_entries": [],
                "time_seconds": 100,
                "max_production_limit": 10,
                "products": [
                    {
                        "type_id": 5002,
                        "type_name": "T2 Module",
                        "quantity": 1,
                        "base_price": 1000.0,
                        "group_name": "Cannons",
                        "category_name": "Module",
                        "meta_group_name": "Tech II",
                    }
                ],
            },
        },
    ]


def _count_activity_nodes(node: dict, activity: str) -> int:
    total = 1 if str(node.get("activity") or "") == activity else 0
    for child in node.get("children") or []:
        if isinstance(child, dict):
            total += _count_activity_nodes(child, activity)
    return total


def _find_first_activity_node(node: dict, activity: str) -> dict | None:
    if str(node.get("activity") or "") == activity:
        return node
    for child in node.get("children") or []:
        if isinstance(child, dict):
            match = _find_first_activity_node(child, activity)
            if isinstance(match, dict):
                return match
    return None


def test_owned_bpo_with_build_from_bpc_adds_copy_chain() -> None:
    original_asset = SimpleNamespace(
        type_id=9001,
        item_id=42,
        is_blueprint_copy=False,
        blueprint_material_efficiency=10,
        blueprint_time_efficiency=20,
        blueprint_runs=None,
    )
    profile = {
        "installation_cost_modifier": 0.10,
        "material_efficiency_bonus": 0.02,
        "time_efficiency_bonus": 0.10,
        "facility_cost_bonus": 0.0,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "copying", "cost_index": 0.03},
        ],
        "structure_rigs": [],
    }
    character_modifiers = {
        "modifier_skills": [
            {"type_name": "Industry", "trained_skill_level": 5},
            {"type_name": "Advanced Industry", "trained_skill_level": 5},
            {"type_name": "Science", "trained_skill_level": 5},
        ],
        "implants": [],
    }
    service = _build_service(
        blueprint_rows=[_blueprint_row()],
        profile=profile,
        character_modifiers=character_modifiers,
        trained_skill_levels={},
        owned_assets=([], [original_asset], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True,
        have_blueprint_source_only=True,
        maximize_bp_runs=False,
        character_id=1,
    )

    assert len(rows) == 1
    manufacturing_job = rows[0]["manufacturing_job"]
    assert manufacturing_job["blueprint_source_kind"] == "copied_from_owned_blueprint_original"
    assert manufacturing_job["blueprint_material_efficiency"] == 10
    assert manufacturing_job["blueprint_time_efficiency"] == 20
    assert manufacturing_job["materials"]["34"]["quantity"] == 9
    assert manufacturing_job["activity_breakdown"]["copying"]["duration_seconds"] > 0
    assert manufacturing_job["time_seconds"] > manufacturing_job["manufacturing_time_seconds"]
    assert manufacturing_job["total_job_cost"] > manufacturing_job["manufacturing_job_cost"]


def test_blueprint_sde_fallback_adds_me_te_research_chain() -> None:
    profile = {
        "installation_cost_modifier": 0.10,
        "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0,
        "facility_cost_bonus": 0.0,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "researching_material_efficiency", "cost_index": 0.02},
            {"activity": "researching_time_efficiency", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    character_modifiers = {
        "modifier_skills": [
            {"type_name": "Industry", "trained_skill_level": 5},
            {"type_name": "Advanced Industry", "trained_skill_level": 5},
            {"type_name": "Research", "trained_skill_level": 5},
            {"type_name": "Metallurgy", "trained_skill_level": 5},
        ],
        "implants": [],
    }
    service = _build_service(
        blueprint_rows=[_blueprint_row()],
        profile=profile,
        character_modifiers=character_modifiers,
        trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=False,
        have_blueprint_source_only=False,
        maximize_bp_runs=False,
        character_id=1,
    )

    assert len(rows) == 1
    manufacturing_job = rows[0]["manufacturing_job"]
    assert manufacturing_job["blueprint_source_kind"] == "blueprint_sde_fallback"
    assert manufacturing_job["blueprint_material_efficiency"] == 10
    assert manufacturing_job["blueprint_time_efficiency"] == 20
    assert manufacturing_job["activity_breakdown"]["research_material"]["duration_seconds"] > 0
    assert manufacturing_job["activity_breakdown"]["research_time"]["duration_seconds"] > 0
    assert manufacturing_job["time_seconds"] > manufacturing_job["manufacturing_time_seconds"]
    assert manufacturing_job["total_job_cost"] > manufacturing_job["manufacturing_job_cost"]


def test_product_overview_excludes_expired_product_names() -> None:
    service = _build_service(
        blueprint_rows=_expired_blueprint_rows(),
        profile=None,
        character_modifiers=None,
        trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}, 88202: {"adjusted_price": 100.0}},
    )

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=False,
        have_blueprint_source_only=False,
        maximize_bp_runs=False,
        character_id=1,
    )

    assert len(rows) == 1
    assert rows[0]["type_name"] == "Test Module"


def test_t2_recursive_plan_adds_invention_and_reaction_chains() -> None:
    profile = {
        "installation_cost_modifier": 0.10,
        "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0,
        "facility_cost_bonus": 0.0,
        "system_security_status": 0.1,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "reaction", "cost_index": 0.04},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    character_modifiers = {
        "modifier_skills": [
            {"type_name": "Industry", "trained_skill_level": 5},
            {"type_name": "Advanced Industry", "trained_skill_level": 5},
            {"type_name": "Science", "trained_skill_level": 5},
        ],
        "implants": [],
    }
    service = _build_service(
        blueprint_rows=_recursive_t2_blueprint_rows(),
        profile=profile,
        character_modifiers=character_modifiers,
        trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        adjusted_price_map={
            34: {"adjusted_price": 5.0},
            35: {"adjusted_price": 8.0},
            204: {"adjusted_price": 100.0},
            7001: {"adjusted_price": 50.0},
            5002: {"adjusted_price": 1000.0},
        },
    )

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True,
        have_blueprint_source_only=False,
        include_reactions=True,
        maximize_bp_runs=False,
        character_id=1,
    )

    manufacturing_job = next(row["manufacturing_job"] for row in rows if row.get("type_id") == 5002)
    assert manufacturing_job["time_seconds"] > manufacturing_job["manufacturing_time_seconds"]
    assert manufacturing_job["total_job_cost"] > manufacturing_job["manufacturing_job_cost"]
    assert "invention" in manufacturing_job["activity_breakdown"]
    assert "reaction:7001" in manufacturing_job["recursive_activity_breakdown"]
    assert "35" in manufacturing_job["procurement_materials"]
    assert "7001" not in manufacturing_job["procurement_materials"]


def test_high_sec_profile_suppresses_reaction_recursion() -> None:
    profile = {
        "installation_cost_modifier": 0.10,
        "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0,
        "facility_cost_bonus": 0.0,
        "system_security_status": 0.8,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "reaction", "cost_index": 0.04},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    service = _build_service(
        blueprint_rows=_recursive_t2_blueprint_rows(),
        profile=profile,
        character_modifiers={"modifier_skills": [], "implants": []},
        trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        adjusted_price_map={35: {"adjusted_price": 8.0}, 204: {"adjusted_price": 100.0}, 7001: {"adjusted_price": 50.0}},
    )

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True,
        have_blueprint_source_only=False,
        include_reactions=True,
        maximize_bp_runs=False,
        character_id=1,
    )

    manufacturing_job = next(row["manufacturing_job"] for row in rows if row.get("type_id") == 5002)
    assert "reaction:7001" not in manufacturing_job["recursive_activity_breakdown"]
    assert "7001" in manufacturing_job["procurement_materials"]


def test_recursive_plan_prefers_take_when_owned_quantity_available() -> None:
    profile = {
        "installation_cost_modifier": 0.10,
        "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0,
        "facility_cost_bonus": 0.0,
        "system_security_status": 0.1,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "reaction", "cost_index": 0.04},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    service = _build_service(
        blueprint_rows=_recursive_t2_blueprint_rows(),
        profile=profile,
        character_modifiers={"modifier_skills": [], "implants": []},
        trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        owned_item_inventory=({7001: 2}, {7001: 42.0}),
        adjusted_price_map={35: {"adjusted_price": 8.0}, 204: {"adjusted_price": 100.0}, 7001: {"adjusted_price": 50.0}},
    )

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True,
        have_blueprint_source_only=False,
        include_reactions=True,
        maximize_bp_runs=False,
        character_id=1,
    )

    manufacturing_job = next(row["manufacturing_job"] for row in rows if row.get("type_id") == 5002)
    assert "reaction:7001" not in manufacturing_job["recursive_activity_breakdown"]
    assert manufacturing_job["procurement_materials"]["7001"]["unit_price"] == 42.0


def test_recursive_plan_prefers_buy_when_market_cheaper(monkeypatch) -> None:
    profile = {
        "installation_cost_modifier": 0.10,
        "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0,
        "facility_cost_bonus": 0.0,
        "system_security_status": 0.1,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "reaction", "cost_index": 0.04},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    monkeypatch.setattr(
        MarketPricingService,
        "get_type_price_map",
        lambda self, *, type_ids, hub="jita", side="sell", progress_callback=None: {
            7001: {"unit_price": 10.0, "price_source": "test_market"}
        },
    )
    service = _build_service(
        blueprint_rows=_recursive_t2_blueprint_rows(),
        profile=profile,
        character_modifiers={"modifier_skills": [], "implants": []},
        trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        adjusted_price_map={35: {"adjusted_price": 8.0}, 204: {"adjusted_price": 100.0}, 7001: {"adjusted_price": 50.0}},
    )

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True,
        have_blueprint_source_only=False,
        include_reactions=True,
        maximize_bp_runs=False,
        character_id=1,
    )

    manufacturing_job = next(row["manufacturing_job"] for row in rows if row.get("type_id") == 5002)
    assert "reaction:7001" not in manufacturing_job["recursive_activity_breakdown"]
    assert manufacturing_job["procurement_materials"]["7001"]["unit_price"] == 10.0


def test_recursive_plan_marks_unknown_owned_cost_basis_when_inventory_has_no_cost() -> None:
    profile = {
        "installation_cost_modifier": 0.10,
        "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0,
        "facility_cost_bonus": 0.0,
        "system_security_status": 0.1,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "reaction", "cost_index": 0.04},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    service = _build_service(
        blueprint_rows=_recursive_t2_blueprint_rows(),
        profile=profile,
        character_modifiers={"modifier_skills": [], "implants": []},
        trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        owned_item_inventory=({7001: 2}, {}),
        adjusted_price_map={35: {"adjusted_price": 8.0}, 204: {"adjusted_price": 100.0}, 7001: {"adjusted_price": 50.0}},
    )

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True,
        have_blueprint_source_only=False,
        include_reactions=True,
        maximize_bp_runs=False,
        character_id=1,
    )

    manufacturing_job = next(row["manufacturing_job"] for row in rows if row.get("type_id") == 5002)
    material = manufacturing_job["procurement_materials"]["7001"]
    assert material["sourcing_strategy"] == "take"
    assert material["owned_cost_basis_known"] is False
    assert material["uses_unknown_owned_cost_basis"] is True


def test_nested_unowned_bpc_build_includes_copying_row(monkeypatch) -> None:
    profile = {
        "installation_cost_modifier": 0.10,
        "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0,
        "facility_cost_bonus": 0.0,
        "system_security_status": 0.1,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    monkeypatch.setattr(
        MarketPricingService,
        "get_material_sell_price_map",
        lambda self, *, material_type_ids, progress_callback=None: {
            6001: {"unit_price": 10_000.0, "price_source": "test_market"}
        },
    )
    service = _build_service(
        blueprint_rows=_recursive_t2_with_t1_material_blueprint_rows(),
        profile=profile,
        character_modifiers={"modifier_skills": [], "implants": []},
        trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        adjusted_price_map={
            34: {"adjusted_price": 5.0},
            204: {"adjusted_price": 100.0},
            6001: {"adjusted_price": 500.0},
            5002: {"adjusted_price": 1000.0},
        },
    )

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True,
        have_blueprint_source_only=False,
        include_reactions=False,
        maximize_bp_runs=False,
        character_id=1,
    )

    manufacturing_job = next(row["manufacturing_job"] for row in rows if row.get("type_id") == 5002)
    job_tree = manufacturing_job["job_tree"]

    assert _count_activity_nodes(job_tree, "copying") == 2


def test_invention_material_rows_use_planner_actions() -> None:
    profile = {
        "installation_cost_modifier": 0.10,
        "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0,
        "facility_cost_bonus": 0.0,
        "system_security_status": 0.1,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "reaction", "cost_index": 0.04},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    service = _build_service(
        blueprint_rows=_recursive_t2_blueprint_rows(),
        profile=profile,
        character_modifiers={"modifier_skills": [], "implants": []},
        trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        adjusted_price_map={
            34: {"adjusted_price": 5.0},
            35: {"adjusted_price": 8.0},
            204: {"adjusted_price": 100.0},
            7001: {"adjusted_price": 50.0},
            5002: {"adjusted_price": 1000.0},
        },
    )

    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True,
        have_blueprint_source_only=False,
        include_reactions=True,
        maximize_bp_runs=False,
        character_id=1,
    )

    manufacturing_job = next(row["manufacturing_job"] for row in rows if row.get("type_id") == 5002)
    invention_node = _find_first_activity_node(manufacturing_job["job_tree"], "invention")

    assert isinstance(invention_node, dict)
    materials_node = next(
        child for child in (invention_node.get("children") or []) if isinstance(child, dict) and child.get("activity") == "materials"
    )
    material_children = [
        child for child in (materials_node.get("children") or []) if isinstance(child, dict) and child.get("node_type") == "material"
    ]

    assert material_children
    assert all(str(child.get("recommendation_action") or "") == "buy" for child in material_children)


def test_owned_blueprint_copy_cost_falls_back_to_average_price() -> None:
    asset = cast(Any, SimpleNamespace(
        blueprint_runs=100,
        acquisition_total_cost=None,
        acquisition_unit_cost=None,
        type_average_price=500_000.0,
        type_adjusted_price=None,
    ))

    allocated_cost = IndustryService._owned_blueprint_copy_consumption_cost(asset, consumed_runs=25)

    assert allocated_cost == 125_000.0


def test_owned_blueprint_copy_cost_falls_back_to_adjusted_price() -> None:
    asset = cast(Any, SimpleNamespace(
        blueprint_runs=40,
        acquisition_total_cost=None,
        acquisition_unit_cost=None,
        type_average_price=None,
        type_adjusted_price=80_000.0,
    ))

    allocated_cost = IndustryService._owned_blueprint_copy_consumption_cost(asset, consumed_runs=10)

    assert allocated_cost == 20_000.0

# --- invention inputs in the T2 row's cost ---------------------------------------
# Fake numbers. The T1 source blueprint 1001 invents into T2 blueprint 9002 at
# 50% with 10 runs per invented BPC, consuming 2 datacores (type 204) per
# attempt. The T2 blueprint needs 10 Tritanium (34) per run. Prices: 34 -> 5,
# 204 -> 100 (sell side, from MarketPricingService), no profile, no skills, so
# the row's ME and every reduction are 0.


def _invented_t2_blueprint_rows() -> list[dict]:
    return [
        {
            "blueprint_type_id": 1001,
            "blueprint": {"type_id": 1001, "type_name": "T1 Source Blueprint", "base_price": 1000.0},
            "manufacturing_job": {
                "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 3, "category_name": "Material"}],
                "skill_entries": [],
                "time_seconds": 50,
                "max_production_limit": 10,
                "products": [{"type_id": 6001, "type_name": "T1 Source Item", "quantity": 1, "meta_group_name": "Tech I"}],
            },
            "copying_job": {"time_seconds": 20},
            "invention_job": {
                "materials": [{"type_id": 204, "type_name": "Datacore", "quantity": 2, "category_name": "Material"}],
                "skill_entries": [],
                "time_seconds": 40,
                "products": [
                    {"probability_pct": 50.0, "quantity": 10, "product": {"type_id": 9002, "type_name": "T2 Blueprint"}}
                ],
            },
        },
        {
            "blueprint_type_id": 9002,
            "blueprint": {"type_id": 9002, "type_name": "T2 Blueprint", "base_price": 5000.0},
            "manufacturing_job": {
                "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 10, "category_name": "Material"}],
                "skill_entries": [],
                "time_seconds": 100,
                "max_production_limit": 10,
                "products": [{"type_id": 5002, "type_name": "T2 Module", "quantity": 1, "meta_group_name": "Tech II"}],
            },
        },
    ]


_INVENTION_FAKE_PRICES = {34: 5.0, 204: 100.0}


def _invention_overview(monkeypatch, *, owned_assets=([], [], {}, {}, {}), maximize_bp_runs=False, owned_item_inventory=None):
    monkeypatch.setattr(
        MarketPricingService,
        "get_type_price_map",
        lambda self, *, type_ids, hub="jita", side="sell", progress_callback=None: {
            int(t): {"unit_price": _INVENTION_FAKE_PRICES[int(t)], "price_source": "market_test"}
            for t in type_ids if int(t) in _INVENTION_FAKE_PRICES
        },
    )
    service = _build_service(
        blueprint_rows=_invented_t2_blueprint_rows(),
        profile=None,
        character_modifiers={"modifier_skills": [], "implants": []},
        trained_skill_levels={},
        owned_assets=owned_assets,
        owned_item_inventory=owned_item_inventory,
        adjusted_price_map={t: {"adjusted_price": p} for t, p in _INVENTION_FAKE_PRICES.items()},
    )
    # The real pricing step: it is what writes material_cost / total_cost.
    service._enrich_product_rows_with_material_prices = MethodType(  # type: ignore[attr-defined]
        IndustryService._enrich_product_rows_with_material_prices, service,
    )
    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True,
        have_blueprint_source_only=False,
        maximize_bp_runs=maximize_bp_runs,
        character_id=1,
    )
    return {row["type_id"]: row for row in rows}


def test_invented_t2_row_cost_includes_expected_invention_inputs(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch)[5002]["manufacturing_job"]

    # 1 run: Tritanium 10 x 5 = 50. Invention per run =
    # (2 datacores x 100) / (0.5 probability x 10 runs per BPC) = 40.
    assert mj["runs"] == 1
    assert mj["material_cost"] == pytest.approx(90.0)
    assert mj["invention_material_cost"] == pytest.approx(40.0)
    assert mj["total_cost"] == pytest.approx((mj["total_job_cost"] or 0.0) + 90.0)
    assert mj["expected_invention_materials"]["204"]["quantity"] == pytest.approx(0.4)
    assert mj["expected_invention_materials"]["204"]["quantity_per_attempt"] == 2
    assert mj["expected_invention_materials"]["204"]["unit_price"] == 100.0


def test_invented_t2_invention_cost_scales_with_runs(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True)[5002]["manufacturing_job"]

    # 10 runs: Tritanium 100 x 5 = 500; invention 40 per run x 10 = 400.
    assert mj["runs"] == 10
    assert mj["invention_material_cost"] == pytest.approx(400.0)
    assert mj["material_cost"] == pytest.approx(900.0)


def test_invention_inputs_use_owned_cost_like_other_materials(monkeypatch) -> None:
    # Owned datacores at 60 each are taken before buying, as for every material.
    mj = _invention_overview(
        monkeypatch, owned_item_inventory=({204: 100}, {204: 60.0}),
    )[5002]["manufacturing_job"]

    assert mj["expected_invention_materials"]["204"]["unit_price"] == 60.0
    assert mj["invention_material_cost"] == pytest.approx(24.0)  # 2 x 60 / (0.5 x 10)


def test_invention_inputs_are_not_in_the_batch_materials(monkeypatch) -> None:
    # The planner buys datacores from its own invent action; `materials` (what
    # the manufacture action buys) and `procurement_materials` stay free of them.
    mj = _invention_overview(monkeypatch)[5002]["manufacturing_job"]

    assert "204" not in mj["materials"]
    assert "204" not in mj["procurement_materials"]


def test_t1_row_cost_is_unchanged_by_invention_inputs(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch)[6001]["manufacturing_job"]

    assert mj["material_cost"] == 15.0  # 3 Tritanium x 5, nothing else
    assert "invention_material_cost" not in mj
    assert "expected_invention_materials" not in mj


def test_t2_row_from_owned_bpc_has_no_invention_cost(monkeypatch) -> None:
    bpc = SimpleNamespace(
        type_id=9002, item_id=77, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    mj = _invention_overview(monkeypatch, owned_assets=([bpc], [], {}, {}, {}))[5002]["manufacturing_job"]

    assert mj["blueprint_source_kind"] == "owned_blueprint_copy"
    assert mj["material_cost"] == 50.0
    assert "invention_material_cost" not in mj


def test_shopping_list_buys_datacores_once_for_an_invented_t2_row(monkeypatch) -> None:
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction
    from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder
    from eve_online_industry_tracker.application.industry import overview_row as orow

    row = _invention_overview(monkeypatch)[5002]
    assert row["manufacturing_job"]["invention_material_cost"] == pytest.approx(40.0)

    def action(action_type, materials=None):
        return AssignedAction(
            type_id=5002, type_name="T2 Module", action_type=action_type,
            character_id=1, character_name="Pilot", quantity=None, runs=None,
            estimated_cost_isk=None, estimated_profit_isk=None,
            estimated_completion=None, notes=None, materials=materials,
        )

    blueprint_data = {
        1001: {
            "manufacturing": {"materials": [], "products": [{"type_id": 6001, "quantity": 1}]},
            "invention": {
                "materials": [{"type_id": 204, "type_name": "Datacore", "quantity": 2}],
                "products": [{"type_id": 9002, "quantity": 10}],
            },
        },
        9002: {
            "manufacturing": {
                "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 10}],
                "products": [{"type_id": 5002, "quantity": 1}],
            },
        },
    }

    class _Resolver:
        def is_blueprint(self, type_id):
            return False

        def prefetch(self, type_ids):
            return None

    class _Admin:
        def get(self, section, key, default=None):
            return default

    items = ShoppingListBuilder().build(
        assigned_actions=[action("manufacture", orow.get_batch_materials(row)), action("invent")],
        corp_assets=[], market_depth_cache={34: {"vwap_5d": 5.0}, 204: {"vwap_5d": 100.0}},
        admin_settings=_Admin(), blueprint_data=blueprint_data, meta_resolver=_Resolver(),
    )

    datacores = [i for i in items if i.type_id == 204]
    assert len(datacores) == 1
    assert datacores[0].quantity == 2  # one attempt, from the invent action only
    assert datacores[0].shopping_category == "invention_input"
