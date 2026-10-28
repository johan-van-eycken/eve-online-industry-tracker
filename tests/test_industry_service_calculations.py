from __future__ import annotations

from types import MethodType, SimpleNamespace
from typing import Any, cast
import pytest
from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService
from eve_online_industry_tracker.application.industry.shopping_list import LATER_BATCH_PLAN_VERSION
from industry_service_fakes import (
    _ACCELERANT_LIKE,
    _FEES_KNOWN_PROFILE,
    _NESTED_FAKE_PRICES,
    _blueprint_row,
    _build_service,
    _decryptor_overview,
    _decryptor_service,
    _find_first_activity_node,
    _invented_parent_rows,
    _invented_t2_blueprint_rows,
    _invention_overview,
    _invention_service,
    _later_plan,
    _nested_invention_blueprint_rows,
    _owned_t2_bpc,
    _owned_t2_bpc_me,
    _planning_context,
    _rows_with,
    _shopping_line,
)


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
    # A lowsec Athanor: a Standup reactor fits it (the reaction gate reads the
    # SDE fitting data from structure_service_fits).
    from eve_online_industry_tracker.domain import industry_facility as fac

    reactor_fit = {"structure_type_ids": [], "structure_group_ids": [1406], "disallow_in_high_sec": True,
                   "disallow_in_empire_space": False, "requires_sov_upgrade": False}
    profile = {
        "structure_type_id": 35835,
        "location_type": "structure",
        "structure_service_fits": fac.structure_service_fits(35835, 1406, {45537: reactor_fit}),
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
    # 1-run batches share 10-run BPCs (fix round 1, I2): 15 batches = 15 runs
    # = 2 BPCs x 2 attempts x 2 datacores = 8 (was 4 per batch, 60). 3 owned,
    # netted once: 8 - 3 = 5.
    row = _invention_overview(monkeypatch, owned_item_inventory=({204: 3}, {204: 60.0}))[5002]
    row["max_batches_total"] = 15

    shopping = _shopping_line(row, 204)
    assert shopping is not None and (shopping["need"], shopping["buy"]) == (8, 5)


def test_two_products_sharing_owned_material_subtract_it_once(monkeypatch) -> None:
    # 5002 needs 10 Tritanium, 6001 needs 3; 3 owned. Each row is planned
    # against the full stock, so both take 3; the list needs 13 - 3 = 10.
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

    rows = _invention_overview(monkeypatch, owned_item_inventory=({34: 3}, {34: 1.0}))
    line = next(item for item in aggregate_shopping_list([rows[5002], rows[6001]]) if item["type_id"] == 34)
    assert (line["need"], line["buy"]) == (13, 10)


# --- round 5 Task 4: a partly owned BPC batch is two jobs (D3) ----------------


def _owned_t2_bpo(*, me: int, te: int, item_id: int = 95) -> SimpleNamespace:
    return SimpleNamespace(
        type_id=9002, item_id=item_id, is_blueprint_copy=False,
        blueprint_material_efficiency=me, blueprint_time_efficiency=te, blueprint_runs=None,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )


def test_the_runs_past_an_owned_bpc_are_copied_from_the_bpo_at_its_me(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    bpc = _owned_t2_bpc(runs=4, item_id=96)
    bpo = _owned_t2_bpo(me=10, te=20)
    service._get_owned_blueprint_assets = MethodType(  # type: ignore[attr-defined]
        lambda self, *, owned_blueprints_scope: ([bpc], [bpo], {}, {}, {}), service,
    )
    ctx = _planning_context(service)
    t2_row = _invented_t2_blueprint_rows()[1]
    row = service._build_single_product_row(
        ctx, row=t2_row, raw_product=t2_row["manufacturing_job"]["products"][0],
        row_index=1, product_index=1, blueprint_copy_assets=[bpc],
        blueprint_original_asset=None, effective_runs=10,
    )
    assert row is not None
    mj = row["manufacturing_job"]

    # 4 runs at ME 0 = 40; 6 copied runs at the BPO's ME 10 = ceil(60 x 0.9) = 54.
    assert mj["materials"]["34"]["quantity"] == 94
    assert [s["source"] for s in mj["job_segments"]] == ["owned_blueprint_copy", "copied_from_owned_blueprint_original"]
    # Time: 4 x 100 s at TE 0 = 400; 6 x 100 s at TE 20 = 480.
    assert [s["duration_seconds"] for s in mj["job_segments"]] == [400, 480]
    assert mj["manufacturing_time_seconds"] == 880


def test_a_fully_covered_batch_is_one_job(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}))[5002]["manufacturing_job"]
    assert len(mj["job_segments"]) == 1
    assert mj["job_segments"][0]["materials"] == {"34": 10}


def test_a_nested_partly_owned_bpc_invents_the_rest_as_a_second_job(monkeypatch) -> None:
    # 10 parent runs need 10 components. A 4-run owned 9002 BPC (no BPO)
    # builds 4 at ME 0, and the other 6 are a second job on an invented BPC
    # (was: bought). Tritanium 40 + ceil(60 x 0.98) = 59.
    # Invention for 6 runs: ceil(6 / 10) = 1 BPC, ceil(1 / 0.5) = 2 attempts, 4 datacores.
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, owned_assets=([_owned_t2_bpc(runs=4)], [], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]

    assert mj["recursive_activity_breakdown"]["manufacturing:5002"]["quantity"] == 10
    assert "5002" not in mj["procurement_materials"]
    assert mj["procurement_materials"]["34"]["quantity"] == 99
    assert mj["invention_procurement_materials"]["204"]["quantity"] == 4


def test_a_nested_partly_owned_bpc_with_a_bpo_copies_the_rest_at_the_bpo_me(monkeypatch) -> None:
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=4)], [_owned_t2_bpo(me=10, te=20)], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]

    assert mj["procurement_materials"]["34"]["quantity"] == 94  # 40 + ceil(60 x 0.9)
    assert "invention_procurement_materials" not in mj


def test_a_nested_partly_owned_bpc_with_unknown_odds_keeps_building_the_owned_runs(monkeypatch) -> None:
    # p 0 and floor 0: the invented second job can't be costed, and an unknown
    # invention cost makes the parent buy the whole child. So the D3 second
    # job falls back to buying the 6 runs past the owned BPC, and the 4 owned
    # runs are still built at ME 0 (40 Tritanium), not bought with the rest.
    rows = _nested_invention_blueprint_rows(probability_pct=0.0)
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=rows, prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=4)], [], {}, {}, {}),
        adm_overrides={"invention_probability_floor": 0.0},
    )[7100]
    mj = row["manufacturing_job"]

    assert mj["recursive_activity_breakdown"]["manufacturing:5002"]["quantity"] == 4
    assert mj["procurement_materials"]["5002"]["quantity"] == 6
    assert mj["procurement_materials"]["34"]["quantity"] == 40
    # Later batches have no owned runs: the sub-build would be invented with
    # unknown odds, so all 10 components are bought (FU-60), nothing flagged.
    assert _later_plan(mj, 2)["procurement_materials"]["5002"]["quantity"] == 10
    assert "invention_cost_unknown_reason" not in mj


# --- round 5 Task 4 fix round 1 ------------------------------------------------


def test_unknown_odds_past_an_owned_bpc_warn_once_and_name_the_bought_runs(monkeypatch, caplog) -> None:
    rows = _nested_invention_blueprint_rows(probability_pct=0.0)
    with caplog.at_level("WARNING"):
        row = _invention_overview(
            monkeypatch, maximize_bp_runs=True, blueprint_rows=rows, prices=_NESTED_FAKE_PRICES,
            owned_assets=([_owned_t2_bpc(runs=4)], [], {}, {}, {}),
            adm_overrides={"invention_probability_floor": 0.0},
        )[7100]
    mj = row["manufacturing_job"]

    floor_warnings = [
        r for r in caplog.records
        if "no SDE success probability; using the admin floor" in r.getMessage() and "9002" in r.getMessage()
    ]
    assert len(floor_warnings) == 1
    # Batch 1 itself buys the 6 runs past the owned BPC: said for batch 1 too.
    assert any(
        "T2 Blueprint sub-build: 6 runs past the owned BPC bought, invention odds unknown" in n
        for n in mj["invention_chance_notes"]
    )


def test_the_activity_cost_cache_tells_required_skill_levels_apart(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    ctx = _planning_context(service)

    def _time_reduction(level: int) -> float:
        return service._plan_activity_costs(
            ctx, activity="manufacturing", base_time_seconds=100, runs=10, process_value=None,
            manufacturing_group=None,
            required_skill_entries=[{"type_id": 3380, "type_name": "Fake Construction", "trained_skill_level": level}],
        )["time_reduction"]

    # Same (base time, runs, group, ME, TE); a required skill at 5 gives 5 %,
    # at 1 gives 1 %. Was: the second call got the first one's cached 5 %.
    assert _time_reduction(5) == pytest.approx(0.05)
    assert _time_reduction(1) == pytest.approx(0.01)


def test_a_nested_invented_second_job_carries_the_decryptor_me_and_te(monkeypatch) -> None:
    # Accelerant-like (+2 ME, +10 TE): the invented job runs at ME 4 / TE 14.
    # Owned 4 runs at ME 0 / TE 0: 40 Tritanium, 4 x 100 = 400 s.
    # Invented 6 runs: ceil(60 x 0.96) = ceil(57.6) = 58 Tritanium,
    # 6 x 100 x 0.86 = 516 s. Total 98 Tritanium, 916 s of manufacturing.
    service = _decryptor_service(
        monkeypatch, [_ACCELERANT_LIKE], prices={**_NESTED_FAKE_PRICES, 34201: 50.0},
        blueprint_rows=_nested_invention_blueprint_rows(),
        owned_assets=([_owned_t2_bpc(runs=4)], [], {}, {}, {}), profile=_FEES_KNOWN_PROFILE,
    )
    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=True, character_id=1,
    )
    mj = {r["type_id"]: r for r in rows}[7100]["manufacturing_job"]
    child = mj["recursive_activity_breakdown"]["manufacturing:5002"]

    segments = child["job_segments"]
    assert [(s["runs"], s["blueprint_material_efficiency"], s["blueprint_time_efficiency"], s["source"]) for s in segments] == [
        (4, 0, 0, "owned_blueprint_copy"), (6, 4, 14, "invented_blueprint_copy"),
    ]
    assert [s["materials"]["34"] for s in segments] == [40, 58]
    assert [s["duration_seconds"] for s in segments] == [400, 516]
    assert mj["procurement_materials"]["34"]["quantity"] == 98


def _jobs(plan_or_mj: dict) -> list[tuple]:
    return [
        (s["runs"], s["blueprint_material_efficiency"], s["blueprint_time_efficiency"], s["source"])
        for s in plan_or_mj["job_segments"]
    ]


def test_later_batches_are_sized_at_the_invented_me_not_the_owned_bpcs(monkeypatch) -> None:
    # A 10-run ME 4 BPC covers batch 1: ceil(100 x 0.96) = 96 Tritanium.
    # Batches 2..N invent at ME 2: ceil(100 x 0.98) = 98 each (was 96, an under-buy).
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, owned_assets=([_owned_t2_bpc_me(runs=10, me=4)], [], {}, {}, {}),
    )[5002]
    mj = row["manufacturing_job"]
    assert mj["materials"]["34"]["quantity"] == 96
    later = _later_plan(mj, 2)
    assert _jobs(later) == [(10, 2, 4, "invented_blueprint_copy")]
    extra = later["procurement_materials"]["34"]
    assert (extra["quantity"], int(extra.get("take_quantity") or 0)) == (98, 0)
    assert mj["extra_batch_schedule"]["lead"] == [] and len(mj["extra_batch_schedule"]["cycle"]) == 1
    # 10 runs / 10 per BPC = 1 BPC, 2 whole attempts, 4 datacores per later batch.
    assert later["invention_procurement_materials"]["204"]["quantity"] == 4

    row["max_batches_total"] = 5
    line = _shopping_line(row, 34)
    assert line is not None and (line["need"], line["buy"]) == (96 + 4 * 98, 96 + 4 * 98)
    # 50 runs - 10 owned = 40 invented = 4 BPCs x 2 attempts x 2 datacores.
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (16, 16)


def test_nothing_owned_spent_writes_no_schedule(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True)[5002]["manufacturing_job"]
    assert "extra_batch_schedule" not in mj
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4


def test_one_run_batches_keep_the_owned_bpc_me_until_its_runs_are_spent(monkeypatch) -> None:
    # 1-run batches on a 10-run ME 4 BPC: batches 2..10 still run on it
    # (as the I2 datacore plan assumes), batch 11 on invents at ME 2.
    mj = _invention_overview(
        monkeypatch, owned_assets=([_owned_t2_bpc_me(runs=10, me=4)], [], {}, {}, {}),
    )[5002]["manufacturing_job"]
    assert _jobs(mj) == [(1, 4, 0, "owned_blueprint_copy")]
    assert [count for _plan_index, count in mj["extra_batch_schedule"]["lead"]] == [9]
    assert _jobs(_later_plan(mj, 10)) == [(1, 4, 0, "owned_blueprint_copy")]
    assert _jobs(_later_plan(mj, 11)) == [(1, 2, 4, "invented_blueprint_copy")]


def test_later_batches_rebuild_a_nested_component_without_the_owned_bpc(monkeypatch) -> None:
    # Batch 1 builds the 10 components on an owned 10-run ME 0 BPC: 100
    # Tritanium, nothing invented. Later batches invent them: ceil(100 x 0.98)
    # = 98 Tritanium and 1 BPC = 2 attempts = 4 datacores, no component bought.
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]
    assert mj["procurement_materials"]["34"]["quantity"] == 100
    later = _later_plan(mj, 2)
    assert later["procurement_materials"]["34"]["quantity"] == 98
    assert "5002" not in later["procurement_materials"]
    assert later["invention_procurement_materials"]["204"]["quantity"] == 4

    row["max_batches_total"] = 3
    line = _shopping_line(row, 34)
    assert line is not None and (line["need"], line["buy"]) == (100 + 2 * 98, 100 + 2 * 98)
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (2 * 4, 2 * 4)


def test_later_batches_of_a_partly_owned_nested_bpc_invent_every_run(monkeypatch) -> None:
    # Replaces the round-4 "invents the built runs" test. Batch 1: 4 runs on
    # the owned BPC + 6 invented (Task 4): 40 + 59 = 99. Later batches: all 10
    # invented, one job: ceil(100 x 0.98) = 98 (was 99, the rounding gap);
    # 1 BPC, 2 attempts, 4 datacores.
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, owned_assets=([_owned_t2_bpc(runs=4)], [], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]
    assert mj["procurement_materials"]["34"]["quantity"] == 99
    later = _later_plan(mj, 2)
    assert later["procurement_materials"]["34"]["quantity"] == 98
    assert later["invention_procurement_materials"]["204"]["quantity"] == 4

    row["max_batches_total"] = 3
    line = _shopping_line(row, 34)
    assert line is not None and (line["need"], line["buy"]) == (99 + 2 * 98, 99 + 2 * 98)
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4 + 2 * 4, 4 + 2 * 4)


def test_a_later_batch_split_over_two_invented_bpcs_is_two_jobs(monkeypatch) -> None:
    # Accelerant-like: 11-run BPCs at ME 4 / TE 14, p 0.5 x 1.2 = 0.6.
    # 10-run batches share them: batch 1 runs 0-9 on BPC 1 (one job, 96),
    # batch 2 is 1 run on BPC 1 + 9 on BPC 2: ceil(9.6) + ceil(86.4) = 10 + 87
    # = 97, one ceiling per job (one 10-run job would say 96, an under-buy).
    row = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE])[5002]
    mj = row["manufacturing_job"]
    assert mj["invention_decryptor"]["type_id"] == 34201
    assert mj["materials"]["34"]["quantity"] == 96
    assert _jobs(_later_plan(mj, 2)) == [(1, 4, 14, "invented_blueprint_copy"), (9, 4, 14, "invented_blueprint_copy")]
    assert [s["materials"]["34"] for s in _later_plan(mj, 2)["job_segments"]] == [10, 87]
    assert [s["duration_seconds"] for s in _later_plan(mj, 2)["job_segments"]] == [86, 774]
    assert _later_plan(mj, 2)["procurement_materials"]["34"]["quantity"] == 97
    # Batch 7 starts at run 60, offset 60 mod 11 = 5 into BPC 6: 6 runs left, then 4.
    assert [job[0] for job in _jobs(_later_plan(mj, 7))] == [6, 4]
    # Offsets repeat every 11 batches: batch 13 starts where batch 2 did.
    assert _jobs(_later_plan(mj, 13)) == _jobs(_later_plan(mj, 2))
    assert len(mj["extra_batch_schedule"]["cycle"]) == 11

    # 11 batches: batch 1 96; batches 2..10 split [a, 10 - a] = 97 each except
    # a = 5 (48 + 48 = 96); batch 11 fits BPC 10 (96).
    row["max_batches_total"] = 11
    line = _shopping_line(row, 34)
    assert line is not None and line["need"] == 96 + 8 * 97 + 96 + 96
    # 110 runs / 11 = 10 BPCs x ceil(1 / 0.6) = 2 attempts = 20 decryptors.
    line = _shopping_line(row, 34201)
    assert line is not None and line["need"] == 20


def test_a_batch_longer_than_an_invented_bpc_is_one_job_per_bpc(monkeypatch) -> None:
    # 2-run invented BPCs, 10-run batch: 5 jobs, ceil(20 x 0.98) = 20 each =
    # 100 Tritanium (one 10-run job would say 98). Every batch is the same.
    rows = _rows_with(probability_pct=50.0, bpc_runs=2, t2_limit=10)
    mj = _invention_overview(monkeypatch, blueprint_rows=rows, maximize_bp_runs=True)[5002]["manufacturing_job"]
    assert _jobs(mj) == [(2, 2, 4, "invented_blueprint_copy")] * 5
    assert mj["materials"]["34"]["quantity"] == 100
    assert mj["manufacturing_time_seconds"] == 5 * 192
    assert "extra_batch_schedule" not in mj


def test_a_nested_invented_sub_build_is_one_job_per_bpc(monkeypatch) -> None:
    # 4-run invented component BPCs, 10 components: jobs 4 + 4 + 2 at ME 2:
    # ceil(39.2) + ceil(39.2) + ceil(19.6) = 40 + 40 + 20 = 100 (was 98).
    # 3 BPCs / 0.5 = 6 attempts x 2 = 12 datacores.
    rows = _nested_invention_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["quantity"] = 4
    mj = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=rows, prices=_NESTED_FAKE_PRICES,
    )[7100]["manufacturing_job"]
    child = mj["recursive_activity_breakdown"]["manufacturing:5002"]
    assert [s["runs"] for s in child["job_segments"]] == [4, 4, 2]
    assert mj["procurement_materials"]["34"]["quantity"] == 100
    assert mj["invention_procurement_materials"]["204"]["quantity"] == 12


def test_grouped_owned_bpcs_are_one_job_each(monkeypatch) -> None:
    # Two 5-run ME 3 BPCs grouped into one 10-run batch: ceil(48.5) x 2 = 98
    # (one 10-run job would say ceil(97) = 97).
    bpcs = [_owned_t2_bpc_me(runs=5, me=3, item_id=91), _owned_t2_bpc_me(runs=5, me=3, item_id=92)]
    mj = _invention_overview(
        monkeypatch, maximize_bp_runs=True, owned_assets=(bpcs, [], {}, {}, {}),
    )[5002]["manufacturing_job"]
    assert _jobs(mj) == [(5, 3, 0, "owned_blueprint_copy")] * 2
    assert mj["materials"]["34"]["quantity"] == 98


def test_row_reductions_of_a_two_job_batch_are_runs_weighted_and_labelled(monkeypatch) -> None:
    # 4 runs at ME 0 / TE 0 and 6 copied at the BPO's ME 10 / TE 20:
    # material (4 x 0 + 6 x 0.10) / 10 = 0.06, time (4 x 0 + 6 x 0.20) / 10 = 0.12.
    service = _invention_service(monkeypatch)
    bpc = _owned_t2_bpc(runs=4, item_id=96)
    bpo = _owned_t2_bpo(me=10, te=20)
    service._get_owned_blueprint_assets = MethodType(  # type: ignore[attr-defined]
        lambda self, *, owned_blueprints_scope: ([bpc], [bpo], {}, {}, {}), service,
    )
    ctx = _planning_context(service)
    t2_row = _invented_t2_blueprint_rows()[1]
    row = service._build_single_product_row(
        ctx, row=t2_row, raw_product=t2_row["manufacturing_job"]["products"][0],
        row_index=1, product_index=1, blueprint_copy_assets=[bpc],
        blueprint_original_asset=None, effective_runs=10,
    )
    assert row is not None
    mj = row["manufacturing_job"]
    assert mj["material_reduction"] == pytest.approx(0.06)
    assert mj["time_reduction"] == pytest.approx(0.12)
    assert mj["reduction_basis"] == "runs_weighted_job_segments"
    assert mj["materials"]["34"]["material_reduction"] == pytest.approx(0.06)
    assert mj["activity_breakdown"]["manufacturing"]["material_reduction"] == pytest.approx(0.06)
    # A one-job batch keeps its single reduction and no label.
    single = _invention_overview(monkeypatch)[5002]["manufacturing_job"]
    assert "reduction_basis" not in single


# --- round 5 Task 5 fix round 1 ------------------------------------------------


def test_every_build_from_bpc_row_is_stamped_with_the_later_batch_plan_version(monkeypatch) -> None:
    from eve_online_industry_tracker.application.industry.shopping_list import LATER_BATCH_PLAN_VERSION

    assert _invention_overview(monkeypatch)[5002]["manufacturing_job"]["later_batch_plan_version"] == LATER_BATCH_PLAN_VERSION
    assert "later_batch_plan_version" not in _invention_overview(monkeypatch, build_from_bpc=False)[5002]["manufacturing_job"]


def test_spare_nested_bpc_runs_after_batch_one_are_noted(monkeypatch) -> None:
    # 1-run batches: batch 1 uses 1 of the nested BPC's 10 runs; later batches
    # are planned without the other 9 (over-buy only), and the row says so.
    mj = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}), real_confidence=True,
    )[7100]["manufacturing_job"]
    assert any(
        "later batches assume nested BPC runs are spent" in n and "T2 Blueprint" in n and "9 runs" in n
        for n in mj["later_batch_notes"]
    )
    assert any("later batches assume nested BPC runs are spent" in r for r in mj["pricing_confidence_reasons"])


def test_a_nested_bpc_spent_by_batch_one_needs_no_note(monkeypatch) -> None:
    mj = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
    )[7100]["manufacturing_job"]
    assert "later_batch_notes" not in mj


def test_layout_datacore_plan_and_decryptor_cycle_share_one_runs_per_bpc(monkeypatch) -> None:
    # One source for the runs per invented BPC: patched to 7, every call site
    # uses 7 - the top-level and nested run layouts, the invention costs and
    # shared-BPC plan, and the decryptor candidates' cycle runs.
    sites: list[int | None] = []

    def seven(target_entry, *, max_production_limit, decryptor):
        sites.append(decryptor.type_id)
        return 7

    monkeypatch.setattr(IndustryService, "_runs_per_invented_blueprint", staticmethod(seven))
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE])[5002]["manufacturing_job"]
    assert [s["runs"] for s in mj["job_segments"]] == [7, 3]  # top-level layout
    assert mj["invention_bpc_plan"]["runs_per_invented_blueprint"] == 7  # invention costs
    assert mj["activity_breakdown"]["invention"]["runs_per_invented_blueprint"] == 7
    # Decryptor cycle: each candidate is built for 7 runs, so 7 units.
    candidates = mj["invention_decryptor"]["candidates"]
    assert {c["type_id"] for c in candidates} == {None, 34201}
    assert {c["units_per_cycle"] for c in candidates} == {7}
    assert 34201 in sites and None in sites

    nested = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
    )[7100]["manufacturing_job"]
    child = nested["recursive_activity_breakdown"]["manufacturing:5002"]
    assert [s["runs"] for s in child["job_segments"]] == [7, 3]  # nested layout


def test_missing_sde_runs_per_bpc_still_warn_once(monkeypatch, caplog) -> None:
    rows = _rows_with(probability_pct=50.0, bpc_runs=0, t2_limit=10)
    with caplog.at_level("WARNING"):
        mj = _invention_overview(monkeypatch, blueprint_rows=rows, maximize_bp_runs=True)[5002]["manufacturing_job"]
    assert mj["invention_bpc_plan"]["runs_per_invented_blueprint"] == 10
    assert sum("no SDE runs per invented BPC" in r.getMessage() for r in caplog.records) == 1


def test_fully_invented_jobs_are_labelled_invented(monkeypatch) -> None:
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE])[5002]["manufacturing_job"]
    assert mj["blueprint_source_kind"] == "unowned_blueprint_copy"
    assert {s["source"] for s in mj["job_segments"]} == {"invented_blueprint_copy"}
    assert {s["source"] for s in _later_plan(mj, 2)["job_segments"]} == {"invented_blueprint_copy"}


def test_reduction_labels_on_nested_and_activity_breakdowns(monkeypatch) -> None:
    # Nested: 4 owned runs at ME 0 + 6 invented at ME 2: (6 x 0.02) / 10 = 0.012.
    mj = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, owned_assets=([_owned_t2_bpc(runs=4)], [], {}, {}, {}),
    )[7100]["manufacturing_job"]
    child = mj["recursive_activity_breakdown"]["manufacturing:5002"]
    assert child["reduction_basis"] == "runs_weighted_job_segments"
    assert child["material_reduction"] == pytest.approx(0.012)
    # Top level: the two-job BPO row labels activity_breakdown.manufacturing too.
    service = _invention_service(monkeypatch)
    bpc = _owned_t2_bpc(runs=4, item_id=96)
    service._get_owned_blueprint_assets = MethodType(  # type: ignore[attr-defined]
        lambda self, *, owned_blueprints_scope: ([bpc], [_owned_t2_bpo(me=10, te=20)], {}, {}, {}), service,
    )
    t2_row = _invented_t2_blueprint_rows()[1]
    row = service._build_single_product_row(
        _planning_context(service), row=t2_row, raw_product=t2_row["manufacturing_job"]["products"][0],
        row_index=1, product_index=1, blueprint_copy_assets=[bpc], blueprint_original_asset=None, effective_runs=10,
    )
    assert row is not None
    assert row["manufacturing_job"]["activity_breakdown"]["manufacturing"]["reduction_basis"] == "runs_weighted_job_segments"


# --- round 5 Task 5 fix round 2: the version stamp is a validity check ---------


def test_an_overview_cached_before_the_later_batch_plans_is_rebuilt(monkeypatch) -> None:
    import hashlib
    import json

    service = _invention_service(monkeypatch)
    kwargs = dict(build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1)
    # The cache entry an overview from before this change left: the input
    # hash without the plan version, rows without the stamp.
    old_hash = hashlib.sha256(json.dumps({
        "maximize_bp_runs": False, "group_identical_bpcs": True, "build_from_bpc": True,
        "have_blueprint_source_only": False, "include_reactions": False, "market_hub": "jita",
        "material_price_side": "sell", "product_price_side": "sell", "industry_profile_id": None,
        "owned_blueprints_scope": "all_characters", "character_id": 1,
    }, sort_keys=True).encode()).hexdigest()
    service._state._overview_result_cache = {"hash": old_hash, "rows": [{"type_id": 1, "stale": True}]}
    assert service.get_cached_overview_rows() is None

    rows = service.industry_manufacturing_product_overview(**kwargs)
    assert all("stale" not in r for r in rows)
    assert all(r["manufacturing_job"]["later_batch_plan_version"] == LATER_BATCH_PLAN_VERSION for r in rows)
    # The rebuilt overview is cached and served again.
    service._state._overview_result_cache["rows"][0]["cache_marker"] = True
    assert any(r.get("cache_marker") for r in service.industry_manufacturing_product_overview(**kwargs))
    assert service.get_cached_overview_rows() is not None


# --- round 5 Task 6: nested-plan hygiene (C5, C6, C7) ---------------------------


def _parent_with_cheap_component_rows() -> list[dict]:
    """Parent 7200 needs 1 component 6200 and 5 Tritanium per run; the
    component's own blueprint needs 10 Tritanium. The component is cheaper to
    buy (1 ISK) than to build."""
    return [
        {
            "blueprint_type_id": 9200,
            "blueprint": {"type_id": 9200, "type_name": "Parent Blueprint", "base_price": 1000.0},
            "manufacturing_job": {
                "materials": [
                    {"type_id": 6200, "type_name": "Component", "quantity": 1, "category_name": "Commodity"},
                    {"type_id": 34, "type_name": "Tritanium", "quantity": 5, "category_name": "Material"},
                ],
                "skill_entries": [], "time_seconds": 100, "max_production_limit": 10,
                "products": [{"type_id": 7200, "type_name": "Parent", "quantity": 1, "meta_group_name": "Tech I"}],
            },
        },
        {
            "blueprint_type_id": 9300,
            "blueprint": {"type_id": 9300, "type_name": "Component Blueprint", "base_price": 1000.0},
            "manufacturing_job": {
                "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 10, "category_name": "Material"}],
                "skill_entries": [], "time_seconds": 100, "max_production_limit": 10,
                "products": [{"type_id": 6200, "type_name": "Component", "quantity": 1, "meta_group_name": "Tech I"}],
            },
        },
    ]


def test_a_rejected_sub_build_spends_no_stock(monkeypatch) -> None:
    # 10 Tritanium owned. Evaluating the component build took all 10, then the
    # component was bought anyway and the parent's own 5 were bought too (FU-61).
    row = _invention_overview(
        monkeypatch, blueprint_rows=_parent_with_cheap_component_rows(),
        prices={34: 5.0, 6200: 1.0}, owned_item_inventory=({34: 10}, {34: 1.0}),
    )[7200]
    procurement = row["manufacturing_job"]["procurement_materials"]

    assert procurement["6200"]["quantity"] == 1                       # bought
    assert (procurement["34"]["take_quantity"], procurement["34"]["buy_quantity"]) == (5, 0)


def test_owned_datacores_are_valued_once_across_a_rows_inventions(monkeypatch) -> None:
    # Parent 9100 (invented) and its component 5002 (invented) each need 4
    # amortized datacores over 10 runs; 5 are owned at 60, the market is 100.
    # The top level takes 4 at 60 (240); the component gets 1 at 60 + 3 at 100
    # (360). Was 4 x 60 twice (480): the owned datacores were counted twice (FU-63).
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_invented_parent_rows(),
        prices=_NESTED_FAKE_PRICES, owned_item_inventory=({204: 5}, {204: 60.0}),
    )[7100]
    mj = row["manufacturing_job"]

    assert mj["recursive_activity_breakdown"]["manufacturing:5002"]["recommended_action"] == "build"
    assert mj["invention_material_cost"] == pytest.approx(4 * 60 + 1 * 60 + 3 * 100)


def test_fractional_source_copy_runs_carry_to_the_next_invention() -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
    source_row = {"blueprint_type_id": 1001, "copying_job": {"time_seconds": 20},
                  "manufacturing_job": {"materials": [{"type_id": 34, "quantity": 3}]}}
    runs = {1001: 1}
    compute = lambda **kw: {"duration_seconds": 0, "total_job_cost": None}  # noqa: E731
    costs = {"runs": 0.3, "whole_attempts": 1}

    service._invention_source_copy_costs(
        compute_activity=compute, invention_source_row=source_row, invention_costs=costs,
        available_blueprint_copy_runs_by_type_id=runs, adjusted_price_map={}, enabled=True,
    )
    assert runs[1001] == pytest.approx(0.7)
    _, missing = service._invention_source_copy_costs(
        compute_activity=compute, invention_source_row=source_row, invention_costs=costs,
        available_blueprint_copy_runs_by_type_id=runs, adjusted_price_map={}, enabled=True,
    )
    assert missing == 0  # 0.7 runs left cover 0.3 attempts (was truncated to 0, so 0.3 missing)
    assert runs[1001] == pytest.approx(0.4)


# --- round 5 Task 6 fix round 1: whole-run budget, fractional write-back -------


def _source_copy_elapsed_seconds(service, runs: dict, attempts: float, whole_attempts: int) -> int:
    """One invention's source-copy elapsed time: 10 s per whole missing run."""
    source_row = {"blueprint_type_id": 1001, "copying_job": {"time_seconds": 10},
                  "manufacturing_job": {"materials": [{"type_id": 34, "quantity": 3}]}}
    compute = lambda **kw: {"duration_seconds": 10 * kw["runs"], "total_job_cost": None}  # noqa: E731
    costs, _ = service._invention_source_copy_costs(
        compute_activity=compute, invention_source_row=source_row,
        invention_costs={"runs": attempts, "whole_attempts": whole_attempts},
        available_blueprint_copy_runs_by_type_id=runs, adjusted_price_map={}, enabled=True,
    )
    return costs["elapsed_duration_seconds"]


def _bare_service() -> IndustryService:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
    return service


def test_whole_source_copy_runs_are_budgeted_across_inventions() -> None:
    # 2 owned runs, three inventions of 1 whole attempt (0.5 expected) each:
    # the third must copy 1 whole run. floor(2 - 0.5 - 0.5) = 1 said 0.
    service, runs = _bare_service(), {1001: 2}
    elapsed = [_source_copy_elapsed_seconds(service, runs, 0.5, 1) for _ in range(3)]
    assert elapsed == [0, 0, 10]
    assert runs[1001] == pytest.approx(0.5)  # the expected-value remainder is kept


def test_whole_source_copy_runs_spent_by_more_whole_than_expected_attempts() -> None:
    # 0.6 expected / 2 whole, then 0.6 / 1: both owned whole runs go to the
    # first, so the second copies 1 run (floor(1.4) = 1 said 0).
    service, runs = _bare_service(), {1001: 2}
    assert _source_copy_elapsed_seconds(service, runs, 0.6, 2) == 0
    assert _source_copy_elapsed_seconds(service, runs, 0.6, 1) == 10


def test_a_sub_build_keeps_the_fractional_bpc_run_remainder(monkeypatch) -> None:
    # 1.7 runs left on the T1 source (an invention used 0.3). A 1-run
    # manufacturing sub-build takes 1 whole run; 0.7 stays for later
    # inventions (was int(1.7) - 1 = 0).
    service = _invention_service(monkeypatch)
    runs: dict = {1001: 1.7}
    service._plan_blueprint_chain_for_quantity(
        blueprint_row=_invented_t2_blueprint_rows()[0], activity="manufacturing",
        desired_product_type_id=6001, required_quantity=1, build_from_bpc=True, include_reactions=False,
        selected_industry_profile=None, selected_character_modifiers=None, character_skill_levels_by_name={},
        adjusted_market_price_map={}, sell_price_map=None, blueprint_copy_assets_by_type_id={},
        available_blueprint_copy_runs_by_type_id=runs, available_owned_item_quantity_by_type_id={},
        invention_valuation_stock={}, owned_item_unit_cost_by_type_id=None,
        blueprint_original_assets_by_type_id={}, manufacturing_row_by_product_type_id={},
        reaction_row_by_product_type_id={}, invention_row_by_blueprint_type_id={},
        invention_context=cast(Any, None),
    )
    assert runs[1001] == pytest.approx(0.7)


# --- round 5 Task 7: rows publish the stock they were planned against ----------


def test_a_row_publishes_the_stock_of_its_buy_line_types(monkeypatch) -> None:
    # Only the types on its buy lists (Tritanium, datacores), not the hangar.
    mj = _invention_overview(
        monkeypatch, owned_item_inventory=({34: 3, 204: 7, 99999: 5}, {34: 1.0, 204: 60.0}),
    )[5002]["manufacturing_job"]
    assert mj["owned_stock_by_type_id"] == {"204": 7, "34": 3}


def test_a_row_publishes_zero_for_a_buy_line_type_it_does_not_own(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, owned_item_inventory=({34: 3}, {34: 1.0}))[5002]["manufacturing_job"]
    assert mj["owned_stock_by_type_id"] == {"204": 0, "34": 3}


def test_every_row_of_one_overview_publishes_the_same_stock(monkeypatch) -> None:
    # One pool per overview: each row is planned against a copy of it.
    rows = _invention_overview(monkeypatch, owned_item_inventory=({34: 12, 204: 3}, {34: 1.0, 204: 60.0}))
    assert rows[5002]["manufacturing_job"]["owned_stock_by_type_id"]["34"] == 12
    assert rows[6001]["manufacturing_job"]["owned_stock_by_type_id"]["34"] == 12


def test_owned_datacores_net_against_later_batches_when_an_owned_bpc_covers_batch_one(monkeypatch) -> None:
    # FU-75 / C8: 3 datacores in the hangar, a 10-run owned BPC covers batch 1
    # (and batches 2..10). 25 one-run batches: runs 11..25 = 15 runs = 2
    # invented BPCs x 2 whole attempts x 2 datacores = 8 needed. No row took
    # the 3 owned (no batch-1 invention list), so the take bound bought 8.
    row = _invention_overview(
        monkeypatch, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
        owned_item_inventory=({204: 3}, {204: 60.0}),
    )[5002]
    assert "invention_procurement_materials" not in row["manufacturing_job"]
    assert row["manufacturing_job"]["owned_stock_by_type_id"]["204"] == 3
    row["max_batches_total"] = 25
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (8, 5)


def test_two_products_sharing_a_material_net_the_real_stock_once_over_batches(monkeypatch) -> None:
    # C9: 12 Tritanium and 3 datacores owned. 5002 (10 Tritanium a run) at 15
    # one-run batches: batch 1 takes 10, batches 2..15 need 10 each. 6001 (3 a
    # run) at 4 batches: batch 1 takes 3. Need 150 + 12 = 162; the 12 owned
    # count once: buy 150 (the take bound said 10 owned, buy 152). Datacores:
    # batch 1 invents 1 BPC (2 attempts x 2 = 4, takes the 3), the 15 runs need
    # 2 BPCs, so 4 more: 8 - 3 = 5.
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

    rows = _invention_overview(monkeypatch, owned_item_inventory=({34: 12, 204: 3}, {34: 1.0, 204: 60.0}))
    rows[5002]["max_batches_total"] = 15
    rows[6001]["max_batches_total"] = 4
    lines = {item["type_id"]: item for item in aggregate_shopping_list([rows[5002], rows[6001]])}
    assert (lines[34]["need"], lines[34]["buy"]) == (162, 150)
    assert (lines[204]["need"], lines[204]["buy"]) == (8, 5)


def test_every_row_of_one_overview_publishes_one_stock_pool_id(monkeypatch) -> None:
    rows = _invention_overview(monkeypatch, owned_item_inventory=({34: 12, 204: 3}, {34: 1.0, 204: 60.0}))
    pool_ids = {row["manufacturing_job"]["owned_stock_pool_id"] for row in rows.values()}
    assert len(pool_ids) == 1 and all(isinstance(p, str) and p for p in pool_ids)
    other = _invention_overview(monkeypatch, owned_item_inventory=({34: 11, 204: 3}, {34: 1.0, 204: 60.0}))
    assert other[5002]["manufacturing_job"]["owned_stock_pool_id"] not in pool_ids


# --- round 5 Task 9: visibility --------------------------------------------------


def test_an_invention_node_with_unknown_odds_says_unknown_not_take(monkeypatch) -> None:
    rows = _rows_with(probability_pct=0.0, bpc_runs=10, t2_limit=10)
    mj = _invention_overview(
        monkeypatch, blueprint_rows=rows, adm_overrides={"invention_probability_floor": 0.0},
    )[5002]["manufacturing_job"]
    node = _find_first_activity_node(mj["job_tree"], "invention")
    assert node is not None
    assert node["recommendation_action"] == "unknown"
    assert "no invention success probability" in node["cost_unknown_reason"]


def test_a_source_copy_covered_by_owned_runs_is_still_listed_for_its_elapsed_time(monkeypatch) -> None:
    source_bpc = SimpleNamespace(
        type_id=1001, item_id=98, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=1,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    mj = _invention_overview(monkeypatch, owned_assets=([source_bpc], [], {}, {}, {}))[5002]["manufacturing_job"]
    copying = mj["activity_breakdown"]["invention_source_copying"]
    assert copying["duration_seconds"] == 0
    assert copying["elapsed_duration_seconds"] == 20
    assert mj["time_seconds"] == 96 + 8          # no copy slot time
    assert mj["elapsed_time_seconds"] == 96 + 80 + 20


def test_the_producer_picks_the_lowest_blueprint_id_for_a_shared_product() -> None:
    from eve_online_industry_tracker.application.industry.service import IndustryService

    def _row(bp_id):
        return {"blueprint_type_id": bp_id, "manufacturing_job": {"products": [{"type_id": 5}]}}

    high, low = _row(200), _row(100)
    manufacturing, _, _ = IndustryService._build_blueprint_row_indexes([high, low])
    assert manufacturing[5] is low
