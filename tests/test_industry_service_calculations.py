from __future__ import annotations

from types import MethodType, SimpleNamespace

from eve_online_industry_tracker.application.industry.service import IndustryService


def _build_service(
    *,
    blueprint_rows: list[dict],
    profile: dict | None,
    character_modifiers: dict | None,
    trained_skill_levels: dict[int, int],
    owned_assets: tuple[list[object], list[object], dict[int, str], dict[int, str], dict[int, str]],
    adjusted_price_map: dict[int, dict],
) -> IndustryService:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
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
    service._get_adjusted_market_price_map = MethodType(  # type: ignore[attr-defined]
        lambda self: adjusted_price_map,
        service,
    )
    service._compact_owned_blueprint_asset = MethodType(  # type: ignore[attr-defined]
        lambda self, asset, **kwargs: {} if asset is None else {"item_id": getattr(asset, "item_id", None)},
        service,
    )
    service._enrich_product_rows_with_material_prices = MethodType(  # type: ignore[attr-defined]
        lambda self, product_rows, progress_callback=None: product_rows,
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
    assert "invention:9002" in manufacturing_job["recursive_activity_breakdown"]
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