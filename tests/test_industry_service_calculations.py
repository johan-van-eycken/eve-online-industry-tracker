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


def _invention_service(
    monkeypatch, *, owned_assets=([], [], {}, {}, {}), owned_item_inventory=None,
    prices=None, profile=None, blueprint_rows=None, real_confidence=False, adm_overrides=None,
):
    prices = dict(_INVENTION_FAKE_PRICES if prices is None else prices)
    monkeypatch.setattr(
        MarketPricingService,
        "get_type_price_map",
        lambda self, *, type_ids, hub="jita", side="sell", progress_callback=None: {
            int(t): {"unit_price": prices[int(t)], "price_source": "market_test"}
            for t in type_ids if int(t) in prices
        },
    )
    service = _build_service(
        blueprint_rows=blueprint_rows or _invented_t2_blueprint_rows(),
        profile=profile,
        character_modifiers={"modifier_skills": [], "implants": []},
        trained_skill_levels={},
        owned_assets=owned_assets,
        owned_item_inventory=owned_item_inventory,
        adjusted_price_map={t: {"adjusted_price": p} for t, p in prices.items()},
    )
    # The real pricing step: it is what writes material_cost / total_cost.
    service._enrich_product_rows_with_material_prices = MethodType(  # type: ignore[attr-defined]
        IndustryService._enrich_product_rows_with_material_prices, service,
    )
    if adm_overrides:
        service._adm = MethodType(  # type: ignore[attr-defined]
            lambda self, category, key, fallback=None: adm_overrides.get(key, fallback), service,
        )
    if real_confidence:
        monkeypatch.setattr(MarketPricingService, "orderbook_depth", lambda self: 5)
        monkeypatch.setattr(MarketPricingService, "material_price_cache_ttl_seconds", lambda self: 3600)
        service._enrich_product_rows_with_pricing_confidence = MethodType(  # type: ignore[attr-defined]
            IndustryService._enrich_product_rows_with_pricing_confidence, service,
        )
    return service


def _invention_overview(monkeypatch, *, maximize_bp_runs=False, build_from_bpc=True, **kwargs):
    service = _invention_service(monkeypatch, **kwargs)
    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=build_from_bpc,
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
    # The row carries the Portfolio buy line for the datacores; the daily
    # planner must not read it (it buys them via the invent action only).
    assert row["manufacturing_job"]["invention_procurement_materials"]["204"]["quantity"] == 4
    assert 204 not in (orow.get_batch_materials(row) or {})

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


# --- fix round 1: fees on the same expected-attempt model, pricing gaps -------

# Only invention has a cost index (2%), no surcharge or reductions, so the
# row's total_job_cost is exactly the invention fee: per-attempt EIV of the
# inputs (2 datacores x adjusted 100 = 200) x 0.02 x expected attempts.
_INVENTION_ONLY_PROFILE = {
    "installation_cost_modifier": 0.0,
    "material_efficiency_bonus": 0.0,
    "time_efficiency_bonus": 0.0,
    "facility_cost_bonus": 0.0,
    "system_cost_indices": [{"activity": "invention", "cost_index": 0.02}],
    "structure_rigs": [],
}


def _rows_with(*, probability_pct: float, bpc_runs: int, t2_limit: int) -> list[dict]:
    rows = _invented_t2_blueprint_rows()
    product = rows[0]["invention_job"]["products"][0]
    product["probability_pct"] = probability_pct
    product["quantity"] = bpc_runs
    rows[1]["manufacturing_job"]["max_production_limit"] = t2_limit
    return rows


def test_one_run_invention_fee_is_amortized_not_a_whole_attempt(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, profile=_INVENTION_ONLY_PROFILE)[5002]["manufacturing_job"]

    # attempts = 1 run / (0.5 x 10) = 0.2 (was ceil(1/0.5) = 2 whole attempts -> 8.0).
    invention = mj["activity_breakdown"]["invention"]
    assert invention["runs"] == pytest.approx(0.2)
    assert invention["total_job_cost"] == pytest.approx(200 * 0.02 * 0.2)  # 0.8
    assert mj["total_job_cost"] == pytest.approx(0.8)
    assert mj["total_cost"] == pytest.approx(0.8 + 90.0)


def test_fees_and_inputs_use_sde_runs_per_bpc_not_the_t2_limit(monkeypatch) -> None:
    # 10-run invented BPC, 300-run T2 limit, 300 runs, 34%:
    # attempts = 300 / (0.34 x 10) = 88.235... (the T2 limit would give ceil(1/0.34) = 3).
    rows = _rows_with(probability_pct=34.0, bpc_runs=10, t2_limit=300)
    mj = _invention_overview(
        monkeypatch, profile=_INVENTION_ONLY_PROFILE, blueprint_rows=rows, maximize_bp_runs=True,
    )[5002]["manufacturing_job"]

    attempts = 300 / (0.34 * 10)
    invention = mj["activity_breakdown"]["invention"]
    assert mj["runs"] == 300
    assert invention["runs_per_invented_blueprint"] == 10
    assert invention["runs"] == pytest.approx(attempts)
    assert invention["total_job_cost"] == pytest.approx(200 * 0.02 * attempts)
    assert mj["invention_material_cost"] == pytest.approx(2 * 100 * attempts)
    assert mj["material_cost"] == pytest.approx(300 * 10 * 5 + 2 * 100 * attempts)


def test_job_tree_adds_up_to_the_row_cost(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, profile=_INVENTION_ONLY_PROFILE)[5002]["manufacturing_job"]

    tree = mj["job_tree"]
    assert tree["material_cost"] == pytest.approx(mj["material_cost"])
    assert tree["total_job_cost"] == pytest.approx(mj["total_job_cost"])
    invention_node = _find_first_activity_node(tree, "invention")
    assert invention_node is not None and invention_node["runs"] == pytest.approx(0.2)


def test_partly_owned_bpc_amortizes_only_the_missing_runs(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    bpc = SimpleNamespace(
        type_id=9002, item_id=78, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=4,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    service._get_owned_blueprint_assets = MethodType(  # type: ignore[attr-defined]
        lambda self, *, owned_blueprints_scope: ([bpc], [], {}, {}, {}), service,
    )
    ctx = service._build_planning_context(
        force_refresh=False, build_from_bpc=True, include_reactions=False, maximize_bp_runs=False,
        group_identical_bpcs=True, have_blueprint_source_only=False, market_hub="jita",
        material_price_side="sell", product_price_side="sell", industry_profile_id=None,
        owned_blueprints_scope="all_characters", character_id=1, progress_callback=None,
    )
    t2_row = _invented_t2_blueprint_rows()[1]
    row = service._build_single_product_row(
        ctx, row=t2_row, raw_product=t2_row["manufacturing_job"]["products"][0],
        row_index=1, product_index=1, blueprint_copy_assets=[bpc],
        blueprint_original_asset=None, effective_runs=10,
    )
    assert row is not None
    service._enrich_product_rows_with_material_prices([row])
    mj = row["manufacturing_job"]

    # 4 runs from the owned BPC, 6 invented: 2 x 100 x 6 / (0.5 x 10) = 240.
    assert mj["expected_invention_materials"]["204"]["invented_runs"] == 6
    assert mj["invention_material_cost"] == pytest.approx(240.0)
    assert mj["material_cost"] == pytest.approx(10 * 10 * 5 + 240.0)


def test_owned_t2_bpo_row_has_no_invention_cost(monkeypatch) -> None:
    bpo = SimpleNamespace(
        type_id=9002, item_id=79, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    profile = {
        "installation_cost_modifier": 0.0, "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0, "facility_cost_bonus": 0.0,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    mj = _invention_overview(monkeypatch, owned_assets=([], [bpo], {}, {}, {}), profile=profile)[5002]["manufacturing_job"]

    assert mj["blueprint_source_kind"] == "copied_from_owned_blueprint_original"
    assert "invention" not in mj["activity_breakdown"]
    assert "invention_material_cost" not in mj
    assert mj["material_cost"] == pytest.approx(50.0)
    # Manufacturing only: 50 EIV x 0.05 = 2.5 (the fixture T2 blueprint has no
    # copy time, and the 2% invention index must add nothing).
    assert mj["total_job_cost"] == pytest.approx(2.5)
    assert mj["elapsed_time_seconds"] == mj["time_seconds"]


_ASSUMES_T2_BPO_REASON = "costed as if a T2 BPO is owned (build_from_bpc off)"


def test_t2_row_without_build_from_bpc_is_flagged_as_assuming_an_owned_t2_bpo(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, build_from_bpc=False, real_confidence=True)[5002]["manufacturing_job"]

    assert mj["assumes_owned_t2_bpo"] is True
    assert _ASSUMES_T2_BPO_REASON in mj["pricing_confidence_reasons"]
    # Labelling only: still no invention in the cost.
    assert "invention_material_cost" not in mj
    assert "invention" not in mj["activity_breakdown"]


def test_t2_row_with_build_from_bpc_is_not_flagged(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, build_from_bpc=True, real_confidence=True)[5002]["manufacturing_job"]

    assert not mj.get("assumes_owned_t2_bpo")
    assert _ASSUMES_T2_BPO_REASON not in mj["pricing_confidence_reasons"]


def test_t1_row_without_build_from_bpc_is_not_flagged(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, build_from_bpc=False, real_confidence=True)[6001]["manufacturing_job"]

    assert not mj.get("assumes_owned_t2_bpo")
    assert _ASSUMES_T2_BPO_REASON not in mj["pricing_confidence_reasons"]


def test_owned_t2_bpo_without_build_from_bpc_is_not_flagged(monkeypatch) -> None:
    bpo = SimpleNamespace(
        type_id=9002, item_id=79, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    mj = _invention_overview(
        monkeypatch, build_from_bpc=False, real_confidence=True, owned_assets=([], [bpo], {}, {}, {}),
    )[5002]["manufacturing_job"]

    assert not mj.get("assumes_owned_t2_bpo")
    assert _ASSUMES_T2_BPO_REASON not in mj["pricing_confidence_reasons"]


def test_unpriced_invention_input_lowers_confidence_and_is_named(monkeypatch, caplog) -> None:
    # No sell price and no adjusted price for the datacore anywhere.
    with caplog.at_level("WARNING"):
        rows = _invention_overview(monkeypatch, prices={34: 5.0}, real_confidence=True)
    mj = rows[5002]["manufacturing_job"]
    priced = _invention_overview(monkeypatch, real_confidence=True)[5002]["manufacturing_job"]

    assert mj["expected_invention_materials"]["204"]["unit_price"] is None
    assert mj["invention_material_cost"] is None
    assert mj["material_cost"] == pytest.approx(50.0)
    assert mj["material_type_count"] == 2 and mj["priced_material_count"] == 1
    assert priced["material_pricing_confidence"] == "High"
    assert mj["material_pricing_confidence"] != "High"
    assert any("Datacore" in reason for reason in mj["pricing_confidence_reasons"])
    assert any("invention input 204" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")


def test_invention_input_without_a_plan_price_falls_back_to_the_market_map(monkeypatch) -> None:
    service = _invention_service(monkeypatch, prices={34: 5.0})
    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    row = next(r for r in rows if r["type_id"] == 5002)
    assert row["manufacturing_job"]["expected_invention_materials"]["204"]["unit_price"] is None

    # The pricing step's own market lookup now has the datacore.
    monkeypatch.setattr(
        MarketPricingService, "get_type_price_map",
        lambda self, *, type_ids, hub="jita", side="sell", progress_callback=None: {
            204: {"unit_price": 100.0, "price_source": "market_fallback"},
        },
    )
    service._enrich_product_rows_with_material_prices([row])
    mj = row["manufacturing_job"]

    assert mj["expected_invention_materials"]["204"]["unit_price"] == 100.0
    assert mj["expected_invention_materials"]["204"]["price_source"] == "market_fallback"
    assert mj["expected_invention_materials"]["204"]["line_total"] == pytest.approx(40.0)
    assert mj["invention_material_cost"] == pytest.approx(40.0)


def test_missing_sde_runs_and_probability_log_warnings(monkeypatch, caplog) -> None:
    rows = _invented_t2_blueprint_rows()
    product = rows[0]["invention_job"]["products"][0]
    product["probability_pct"] = 0.0
    product["quantity"] = 0
    with caplog.at_level("WARNING"):
        mj = _invention_overview(monkeypatch, blueprint_rows=rows)[5002]["manufacturing_job"]

    messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any("no SDE success probability" in m for m in messages)
    assert any("no SDE runs per invented BPC" in m for m in messages)
    invention = mj["activity_breakdown"]["invention"]
    assert invention["success_probability"] == pytest.approx(0.01)  # the admin floor's default
    assert invention["runs_per_invented_blueprint"] == 10  # the T2 limit fallback


# --- fix round 2: elapsed time, breakdown sums, zero-probability guard ----------


def test_activity_breakdown_fees_sum_to_total_job_cost(monkeypatch) -> None:
    profile = {
        "installation_cost_modifier": 0.1, "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0, "facility_cost_bonus": 0.0,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    mj = _invention_overview(monkeypatch, profile=profile)[5002]["manufacturing_job"]

    fees = [
        float(entry["total_job_cost"])
        for entry in mj["activity_breakdown"].values()
        if isinstance(entry, dict) and entry.get("total_job_cost") is not None
    ]
    assert {"manufacturing", "invention", "invention_source_copying"} <= set(mj["activity_breakdown"])
    assert sum(fees) == pytest.approx(mj["total_job_cost"])


def test_elapsed_time_uses_whole_invention_jobs(monkeypatch) -> None:
    # No profile, no skills: no time reductions. 1 run, p = 0.5, 10 runs per BPC.
    mj = _invention_overview(monkeypatch)[5002]["manufacturing_job"]
    invention = mj["activity_breakdown"]["invention"]
    source_copy = mj["activity_breakdown"]["invention_source_copying"]

    # Amortized slot time (ISK/h): manufacturing 100 + invention 40 x 0.2 + copy 20 x 0.2.
    assert mj["time_seconds"] == 100 + 8 + 4
    # Elapsed: 1 BPC needed, ceil(1 / 0.5) = 2 whole attempts of 40 s in sequence,
    # 2 source-copy runs of 20 s, then manufacturing 100 s.
    assert invention["job_duration_seconds"] == 40
    assert invention["whole_attempts"] == 2
    assert invention["elapsed_duration_seconds"] == 80
    assert source_copy["job_duration_seconds"] == 20
    assert source_copy["elapsed_duration_seconds"] == 40
    assert mj["elapsed_time_seconds"] == 100 + 80 + 40
    node = _find_first_activity_node(mj["job_tree"], "invention")
    assert node is not None
    assert node["job_duration_seconds"] == 40 and node["duration_is_amortized"] is True


def test_manufacture_window_reads_elapsed_not_amortized_time() -> None:
    row = {
        "days_of_supply": 0.5,
        "manufacturing_job": {"time_seconds": 3600.0, "elapsed_time_seconds": 2 * 86400.0},
    }
    out = IndustryService._enrich_product_rows_with_manufacturing_signals(
        cast(Any, object.__new__(IndustryService)), [row],
    )
    # Amortized 1 h would say it sells in time; the real 2 days do not.
    assert out[0]["manufacture_window_ok"] is False


def test_zero_probability_and_zero_floor_never_divides(monkeypatch, caplog) -> None:
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    with caplog.at_level("WARNING"):
        out = _invention_overview(
            monkeypatch, blueprint_rows=rows, real_confidence=True,
            adm_overrides={"invention_probability_floor": 0.0},
        )
    mj = out[5002]["manufacturing_job"]

    assert "no invention success probability" in mj["invention_cost_unknown_reason"]
    assert mj["activity_breakdown"]["invention"]["total_job_cost"] is None
    assert "invention_material_cost" not in mj
    assert mj["elapsed_time_seconds"] is None
    assert mj["material_pricing_confidence"] == "Low"
    assert any("Invention cost unknown" in r for r in mj["pricing_confidence_reasons"])
    assert any("invention cost left unknown" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")
    # A T1 row in the same refresh is untouched.
    assert out[6001]["manufacturing_job"]["material_cost"] == pytest.approx(15.0)


def test_zero_floor_does_not_break_an_owned_bpc_row(monkeypatch) -> None:
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    bpc = SimpleNamespace(
        type_id=9002, item_id=80, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    mj = _invention_overview(
        monkeypatch, blueprint_rows=rows, owned_assets=([bpc], [], {}, {}, {}),
        adm_overrides={"invention_probability_floor": 0.0},
    )[5002]["manufacturing_job"]

    assert mj["material_cost"] == pytest.approx(50.0)
    assert "invention_cost_unknown_reason" not in mj


# --- nested T2 sub-builds: expected attempts, no division by zero ---------------
# Fake numbers. Parent blueprint 9100 (unowned, no invention path) builds 7100
# from 1 "T2 Component" (5002) per run. 5002 is the T2 module of the invention
# fixture above, so building it in the nested plan needs an invented 9002 BPC:
# p = 0.5, 10 runs per BPC, 2 datacores (204) per attempt, invention 40 s,
# source copy 20 s. The market price of 5002 is high, so the plan builds it.


def _nested_invention_blueprint_rows(*, probability_pct: float = 50.0) -> list[dict]:
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = probability_pct
    rows.append({
        "blueprint_type_id": 9100,
        "blueprint": {"type_id": 9100, "type_name": "Parent Blueprint", "base_price": 1000.0},
        "manufacturing_job": {
            "materials": [{"type_id": 5002, "type_name": "T2 Module", "quantity": 1, "category_name": "Module"}],
            "skill_entries": [],
            "time_seconds": 200,
            "max_production_limit": 10,
            "products": [{"type_id": 7100, "type_name": "Parent Item", "quantity": 1, "meta_group_name": "Tech I"}],
        },
    })
    return rows


_NESTED_FAKE_PRICES = {34: 5.0, 204: 100.0, 5002: 10_000.0}


def test_nested_t2_component_uses_expected_invention_attempts(monkeypatch) -> None:
    rows = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, profile=_INVENTION_ONLY_PROFILE,
    )
    mj = rows[7100]["manufacturing_job"]

    # 1 parent run -> 1 component run -> 1 / (0.5 x 10) = 0.2 expected attempts
    # (was ceil(1 / 0.5) = 2 whole attempts: 4 datacores, 400 ISK, fee 8.0).
    nested = mj["recursive_activity_breakdown"]["manufacturing:5002"]
    assert nested["recommended_action"] == "build"
    invention = nested["nested"]["invention:9002"]
    assert invention["attempts"] == pytest.approx(0.2)
    assert invention["whole_attempts"] == 2
    assert invention["job_cost"] == pytest.approx(200 * 0.02 * 0.2)  # 0.8

    inputs = [m for m in mj["expected_invention_materials"].values() if m["type_id"] == 204]
    assert len(inputs) == 1
    assert inputs[0]["quantity"] == pytest.approx(0.4)
    assert inputs[0]["quantity_per_attempt"] == 2
    assert inputs[0]["expected_attempts"] == pytest.approx(0.2)
    assert inputs[0]["line_total"] == pytest.approx(40.0)
    # The buy list gets whole attempts: 2 attempts x 2 datacores, all bought.
    buy = mj["invention_procurement_materials"]["204"]
    assert (buy["quantity"], buy["take_quantity"], buy["buy_quantity"]) == (4, 0, 4)

    # Tritanium 10 x 5 from the component, plus 40 of amortized datacores.
    assert mj["invention_material_cost"] == pytest.approx(40.0)
    assert mj["material_cost"] == pytest.approx(90.0)
    assert mj["total_job_cost"] == pytest.approx(0.8)
    assert mj["total_cost"] == pytest.approx(90.8)

    # Slot time: parent 200 + component 100 + invention 40 x 0.2 + copy 20 x 0.2.
    assert mj["time_seconds"] == 200 + 100 + 8 + 4
    # Elapsed: 2 whole attempts of 40 s and 2 source-copy runs of 20 s in sequence.
    assert mj["elapsed_time_seconds"] == 200 + 100 + 80 + 40

    # The top-level T2 row in the same refresh is unchanged.
    t2 = rows[5002]["manufacturing_job"]
    assert t2["material_cost"] == pytest.approx(90.0)
    assert t2["total_job_cost"] == pytest.approx(0.8)
    assert t2["elapsed_time_seconds"] == 100 + 80 + 40


def test_nested_t2_zero_probability_and_zero_floor_never_divides(monkeypatch, caplog) -> None:
    with caplog.at_level("WARNING"):
        rows = _invention_overview(
            monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(probability_pct=0.0),
            prices=_NESTED_FAKE_PRICES, profile=_INVENTION_ONLY_PROFILE, real_confidence=True,
            adm_overrides={"invention_probability_floor": 0.0},
        )
    assert any("invention cost left unknown" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")

    # The top-level T2 row: invention cost unknown, never divided by zero.
    t2 = rows[5002]["manufacturing_job"]
    assert "no invention success probability" in t2["invention_cost_unknown_reason"]
    assert not t2.get("expected_invention_materials")
    assert t2["elapsed_time_seconds"] is None
    assert t2["material_pricing_confidence"] == "Low"
    assert any("Invention cost unknown" in r for r in t2["pricing_confidence_reasons"])
    # A T1 row in the same refresh is untouched.
    assert rows[6001]["manufacturing_job"]["material_cost"] == pytest.approx(15.0)


def test_nested_sub_build_with_unknown_invention_cost_is_bought(monkeypatch) -> None:
    # Unknown odds leave the sub-build's estimated_total_cost without the
    # invention, so it looked cheaper than its 10,000 ISK market price and
    # was built with no datacores bought. It is bought instead.
    rows = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(probability_pct=0.0),
        prices=_NESTED_FAKE_PRICES, profile=_INVENTION_ONLY_PROFILE,
        adm_overrides={"invention_probability_floor": 0.0},
    )
    mj = rows[7100]["manufacturing_job"]

    assert "manufacturing:5002" not in mj["recursive_activity_breakdown"]
    assert mj["procurement_materials"]["5002"]["quantity"] == 1
    assert mj["material_cost"] == pytest.approx(10_000.0)
    assert "invention_cost_unknown_reason" not in mj
    assert "invention_procurement_materials" not in mj
    assert "invention_procurement_materials_per_extra_batch" not in mj


# --- fix round 1: whole-attempt invention buy list ------------------------------


def _shopping_line(row: dict, type_id: int) -> dict | None:
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

    return next((line for line in aggregate_shopping_list([row]) if line["type_id"] == type_id), None)


def test_top_level_t2_shopping_list_buys_datacores_for_whole_attempts(monkeypatch) -> None:
    row = _invention_overview(monkeypatch)[5002]
    mj = row["manufacturing_job"]

    # 1 run, p 0.5: 2 whole attempts x 2 datacores. Cost stays amortized (40),
    # so the buy line is not priced into material_cost a second time.
    assert mj["invention_procurement_materials"]["204"]["quantity"] == 4
    assert mj["material_cost"] == pytest.approx(90.0)
    assert "204" not in mj["procurement_materials"]
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)


def test_nested_t2_shopping_list_buys_datacores_for_whole_attempts(monkeypatch) -> None:
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
    )[7100]

    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)


def test_owned_datacores_reduce_the_invention_buy(monkeypatch) -> None:
    row = _invention_overview(monkeypatch, owned_item_inventory=({204: 3}, {204: 60.0}))[5002]

    buy = row["manufacturing_job"]["invention_procurement_materials"]["204"]
    assert (buy["quantity"], buy["take_quantity"], buy["buy_quantity"]) == (4, 3, 1)
    assert buy["sourcing_strategy"] == "split"
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 1)


def _invented_parent_rows() -> list[dict]:
    """The nested fixture, with the parent blueprint 9100 itself invented from a
    second T1 source (1002) using the same datacore, 2 per attempt, p 0.5."""
    rows = _nested_invention_blueprint_rows()
    source = _invented_t2_blueprint_rows()[0]
    source["blueprint_type_id"] = 1002
    source["blueprint"] = {"type_id": 1002, "type_name": "Parent Source Blueprint", "base_price": 1000.0}
    source["manufacturing_job"]["products"] = [{"type_id": 6002, "type_name": "Parent Source Item", "quantity": 1}]
    source["invention_job"]["products"][0]["product"] = {"type_id": 9100, "type_name": "Parent Blueprint"}
    return [*rows, source]


@pytest.mark.parametrize(
    ("component_price", "take", "buy"),
    [
        # Component bought: its evaluated invention spends no stock, so the
        # parent's own 4 datacores take all 3 owned and buy 1.
        (1.0, 3, 1),
        # Component built: 4 nested + 4 top-level datacores, 3 taken, 5 bought.
        (10_000.0, 3, 5),
    ],
)
def test_evaluating_a_nested_invention_does_not_spend_stock(monkeypatch, component_price, take, buy) -> None:
    row = _invention_overview(
        monkeypatch, blueprint_rows=_invented_parent_rows(),
        prices={**_NESTED_FAKE_PRICES, 5002: component_price},
        owned_item_inventory=({204: 3}, {204: 60.0}),
    )[7100]
    mj = row["manufacturing_job"]

    built = mj["recursive_activity_breakdown"].get("manufacturing:5002", {}).get("recommended_action") == "build"
    assert built is (component_price > 1.0)
    line = mj["invention_procurement_materials"]["204"]
    assert (line["take_quantity"], line["buy_quantity"]) == (take, buy)
    assert isinstance(line["take_quantity"], int) and isinstance(line["buy_quantity"], int)


# --- Task H: the shopping list spends owned stock once over all batches -------


def test_partly_owned_manufacturing_material_reports_take_and_buy(monkeypatch) -> None:
    # 3 of the 10 Tritanium are owned. The procurement line merges the take
    # and buy legs, and must still say which part is owned.
    row = _invention_overview(monkeypatch, owned_item_inventory=({34: 3}, {34: 1.0}))[5002]

    line = row["manufacturing_job"]["procurement_materials"]["34"]
    assert (line["quantity"], line["take_quantity"], line["buy_quantity"]) == (10, 3, 7)
    shopping = _shopping_line(row, 34)
    assert shopping is not None and (shopping["need"], shopping["buy"]) == (10, 7)


def test_shopping_list_spends_owned_material_once_over_batches(monkeypatch) -> None:
    row = _invention_overview(monkeypatch, owned_item_inventory=({34: 3}, {34: 1.0}))[5002]
    row["max_batches_total"] = 5

    shopping = _shopping_line(row, 34)
    assert shopping is not None and (shopping["need"], shopping["buy"]) == (50, 47)


def test_shopping_list_spends_owned_datacores_once_over_batches(monkeypatch) -> None:
    # 3 owned, 4 datacores per batch, 5 batches: 4 x 5 - 3 = 17.
    row = _invention_overview(monkeypatch, owned_item_inventory=({204: 3}, {204: 60.0}))[5002]
    row["max_batches_total"] = 5

    shopping = _shopping_line(row, 204)
    assert shopping is not None and (shopping["need"], shopping["buy"]) == (20, 17)


def test_two_products_sharing_owned_material_subtract_it_once(monkeypatch) -> None:
    # 5002 needs 10 Tritanium, 6001 needs 3; 3 owned. Each row is planned
    # against the full stock, so both take 3; the list needs 13 - 3 = 10.
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

    rows = _invention_overview(monkeypatch, owned_item_inventory=({34: 3}, {34: 1.0}))
    line = next(item for item in aggregate_shopping_list([rows[5002], rows[6001]]) if item["type_id"] == 34)
    assert (line["need"], line["buy"]) == (13, 10)


# --- round 3 final wave I1: datacores for the batches past owned T2 BPCs ------


def _owned_t2_bpc(*, runs: int, item_id: int = 81) -> SimpleNamespace:
    return SimpleNamespace(
        type_id=9002, item_id=item_id, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=runs,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )


def test_owned_bpc_covering_batch_one_still_buys_datacores_for_later_batches(monkeypatch) -> None:
    # A 10-run owned BPC covers batch 1 (1 run). Every later batch invents its
    # run: 1 run / 10 per BPC -> 1 BPC, p 0.5 -> 2 whole attempts x 2 = 4.
    row = _invention_overview(monkeypatch, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}))[5002]
    mj = row["manufacturing_job"]
    assert "invention_procurement_materials" not in mj
    extra = mj["invention_procurement_materials_per_extra_batch"]["204"]
    assert (extra["quantity"], extra["take_quantity"], extra["buy_quantity"]) == (4, 0, 4)

    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (16, 16)  # 4 x (5 - 1)


def test_partly_owned_bpc_buys_full_batch_datacores_for_later_batches(monkeypatch) -> None:
    # 2-run invented BPCs, a 4-run owned BPC, 10 runs per batch, 3 batches.
    # Batch 1 invents 6 runs: 3 BPCs / 0.5 = 6 attempts x 2 = 12 datacores.
    # Later batches invent 10 runs: 5 BPCs / 0.5 = 10 attempts x 2 = 20 each.
    rows = _rows_with(probability_pct=50.0, bpc_runs=2, t2_limit=10)
    service = _invention_service(monkeypatch, blueprint_rows=rows)
    bpc = _owned_t2_bpc(runs=4, item_id=82)
    service._get_owned_blueprint_assets = MethodType(  # type: ignore[attr-defined]
        lambda self, *, owned_blueprints_scope: ([bpc], [], {}, {}, {}), service,
    )
    ctx = service._build_planning_context(
        force_refresh=False, build_from_bpc=True, include_reactions=False, maximize_bp_runs=False,
        group_identical_bpcs=True, have_blueprint_source_only=False, market_hub="jita",
        material_price_side="sell", product_price_side="sell", industry_profile_id=None,
        owned_blueprints_scope="all_characters", character_id=1, progress_callback=None,
    )
    t2_row = rows[1]
    row = service._build_single_product_row(
        ctx, row=t2_row, raw_product=t2_row["manufacturing_job"]["products"][0],
        row_index=1, product_index=1, blueprint_copy_assets=[bpc],
        blueprint_original_asset=None, effective_runs=10,
    )
    assert row is not None
    mj = row["manufacturing_job"]
    assert mj["invention_procurement_materials"]["204"]["quantity"] == 12
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 20

    row["max_batches_total"] = 3
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (52, 52)  # 12 + 2 x 20


def test_no_owned_bpc_datacore_buy_is_unchanged(monkeypatch) -> None:
    row = _invention_overview(monkeypatch)[5002]
    mj = row["manufacturing_job"]
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4

    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (20, 20)


def test_owned_datacores_are_netted_once_with_an_extra_batch_list(monkeypatch) -> None:
    # 3 owned: batch 1 takes them, the extra-batch lines take 0. 20 - 3 = 17.
    row = _invention_overview(monkeypatch, owned_item_inventory=({204: 3}, {204: 60.0}))[5002]
    extra = row["manufacturing_job"]["invention_procurement_materials_per_extra_batch"]["204"]
    assert (extra["take_quantity"], extra["buy_quantity"]) == (0, 4)

    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (20, 17)


def test_nested_invention_is_in_the_extra_batch_list(monkeypatch) -> None:
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
    )[7100]
    assert row["manufacturing_job"]["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4

    row["max_batches_total"] = 3
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (12, 12)


def test_build_from_bpc_off_writes_no_invention_buy_lists(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, build_from_bpc=False)[5002]["manufacturing_job"]
    assert "invention_procurement_materials" not in mj
    assert "invention_procurement_materials_per_extra_batch" not in mj


def test_extra_batch_list_does_not_repeat_the_unknown_odds_warning(monkeypatch, caplog) -> None:
    # No owned BPC: the extra batch reuses batch 1's attempt count, so the
    # unknown-odds WARNING is logged once, as before the extra-batch list.
    with caplog.at_level("WARNING"):
        _invention_overview(
            monkeypatch, blueprint_rows=_rows_with(probability_pct=0.0, bpc_runs=10, t2_limit=10),
            adm_overrides={"invention_probability_floor": 0.0},
        )
    unknown = [r for r in caplog.records if "invention cost left unknown" in r.getMessage()]
    assert len(unknown) == 1
