from __future__ import annotations

from types import MethodType, SimpleNamespace
from typing import Any, cast

import pytest

from eve_online_industry_tracker.application.industry.invention_model import NO_DECRYPTOR, DecryptorOption
from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService

# Public SDE ids: High Energy Physics, Caldari Starship Engineering, Caldari Encryption Methods.
_HARNESS_INVENTION_SKILLS = (
    {"type_id": 11433, "type_name": "High Energy Physics", "group_id": 270},
    {"type_id": 11454, "type_name": "Caldari Starship Engineering", "group_id": 270},
    {"type_id": 21790, "type_name": "Caldari Encryption Methods", "group_id": 270},
)


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
    # No SDE in the harness: plan without decryptors, without the "no SDE
    # session" WARNING on every refresh (that branch has its own test).
    service._load_decryptor_options = MethodType(  # type: ignore[attr-defined]
        lambda self: (NO_DECRYPTOR,), service,
    )
    # Invention chance: one loaded, untrained pilot (factor exactly 1.0, so
    # every chance is the SDE base chance and no number moves) and identifiable
    # invention skills, like real SDE rows. No skills-unknown WARNING on every
    # refresh; the fallback branches have their own tests, which override these.
    for row in blueprint_rows:
        invention_job = row.get("invention_job") if isinstance(row, dict) else None
        if isinstance(invention_job, dict) and not invention_job.get("skill_entries"):
            invention_job["skill_entries"] = [dict(s) for s in _HARNESS_INVENTION_SKILLS]
    service._get_inventor_skill_levels = MethodType(  # type: ignore[attr-defined]
        lambda self: {1: {}}, service,
    )
    service._load_encryption_skill_type_ids = MethodType(  # type: ignore[attr-defined]
        lambda self: frozenset({21790}), service,
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
    # The NPC market fee context (resolved once per refresh in the planning
    # context): no sales tax or broker fee unless a test sets them.
    service._resolve_npc_market_fee_context = MethodType(  # type: ignore[attr-defined]
        lambda self, **kwargs: {"rates": {"sales_tax_fraction": 0.0, "broker_fee_fraction": 0.0},
                                "broker_fee_applies": True},
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
    fee_context=None,
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
    if fee_context is not None:
        service._resolve_npc_market_fee_context = MethodType(  # type: ignore[attr-defined]
            lambda self, **kwargs: fee_context, service,
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

    # 10 runs at ME 2: Tritanium 98 x 5 = 490; invention 40 per run x 10 = 400.
    assert mj["runs"] == 10
    assert mj["invention_material_cost"] == pytest.approx(400.0)
    assert mj["material_cost"] == pytest.approx(890.0)


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

    def action(action_type, materials=None, runs=None):
        return AssignedAction(
            type_id=5002, type_name="T2 Module", action_type=action_type,
            character_id=1, character_name="Pilot", quantity=None, runs=runs,
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
        # The invent action carries the producer's whole attempts (A4: the
        # best inventor's, CharacterAssigner sets them).
        assigned_actions=[
            action("manufacture", orow.get_batch_materials(row)),
            action("invent", runs=orow.get_invention_costs(row)["whole_attempts"]),
        ],
        corp_assets=[], market_depth_cache={34: {"vwap_5d": 5.0}, 204: {"vwap_5d": 100.0}},
        admin_settings=_Admin(), blueprint_data=blueprint_data, meta_resolver=_Resolver(),
    )

    datacores = [i for i in items if i.type_id == 204]
    assert len(datacores) == 1
    # Whole attempts x 2, from the invent action only: the Portfolio line's 4.
    assert datacores[0].quantity == 4
    assert datacores[0].shopping_category == "invention_input"


# --- fix round 1: fees on the same expected-attempt model, pricing gaps -------

# Only invention has a cost index (2%), no facility tax or reductions, so the
# row's total_job_cost is exactly the invention fee (job_cost.py): job value
# per attempt = 0.02 x the invented T2 blueprint's per-run EIV (10 Tritanium x
# adjusted 5 = 50) = 1.0, times (index 0.02 + SCC 0.04), times the attempts.
_SCC = 0.04
_T2_INVENTION_FEE_PER_ATTEMPT = 0.02 * 50 * (0.02 + _SCC)  # 0.06
_INVENTION_ONLY_PROFILE = {
    "installation_cost_modifier": 0.0, "facility_tax": 0.0,
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
    assert invention["total_job_cost"] == pytest.approx(_T2_INVENTION_FEE_PER_ATTEMPT * 0.2)  # 0.012
    assert mj["total_job_cost"] == pytest.approx(0.012)
    assert mj["total_cost"] == pytest.approx(0.012 + 90.0)


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
    assert invention["total_job_cost"] == pytest.approx(_T2_INVENTION_FEE_PER_ATTEMPT * attempts)
    assert mj["invention_material_cost"] == pytest.approx(2 * 100 * attempts)
    # Tritanium at ME 2: 300 runs x 10 x 0.98 = 2940 units x 5.
    assert mj["material_cost"] == pytest.approx(2940 * 5 + 2 * 100 * attempts)


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
    # D3: two jobs. 4 runs at the owned ME 0 = 40 Tritanium; 6 runs on the
    # invented BPC at ME 2 = ceil(60 x 0.98) = 59. Was 100 at ME 0 for all 10.
    assert mj["materials"]["34"]["quantity"] == 99
    assert mj["material_cost"] == pytest.approx(99 * 5 + 240.0)
    segments = mj["job_segments"]
    assert [(s["runs"], s["blueprint_material_efficiency"], s["source"]) for s in segments] == [
        (4, 0, "owned_blueprint_copy"), (6, 2, "invented_blueprint_copy"),
    ]
    assert [s["materials"]["34"] for s in segments] == [40, 59]
    # Time per job: 4 x 100 s at TE 0, 6 x 100 s at TE 4 = 576.
    assert mj["manufacturing_time_seconds"] == 400 + 576


def test_owned_t2_bpo_row_has_no_invention_cost(monkeypatch) -> None:
    bpo = SimpleNamespace(
        type_id=9002, item_id=79, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    profile = {
        "installation_cost_modifier": 0.0, "facility_tax": 0.0, "material_efficiency_bonus": 0.0,
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
    # Manufacturing only: 50 EIV x (0.05 + SCC 0.04) = 4.5 (the fixture T2
    # blueprint has no copy time, and the 2% invention index must add nothing).
    assert mj["total_job_cost"] == pytest.approx(4.5)
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

    # Amortized slot time (ISK/h): manufacturing 96 (TE 4) + invention 40 x 0.2 + copy 20 x 0.2.
    assert mj["time_seconds"] == 96 + 8 + 4
    # Elapsed: 1 BPC needed, ceil(1 / 0.5) = 2 whole attempts of 40 s in sequence,
    # 2 source-copy runs of 20 s, then manufacturing 96 (TE 4).
    assert invention["job_duration_seconds"] == 40
    assert invention["whole_attempts"] == 2
    assert invention["elapsed_duration_seconds"] == 80
    assert source_copy["job_duration_seconds"] == 20
    assert source_copy["elapsed_duration_seconds"] == 40
    assert mj["elapsed_time_seconds"] == 96 + 80 + 40
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
    # Batch 1 is fine; only the batches past the owned BPC runs are unknown
    # (Task I R3, see test_unknown_odds_with_owned_bpc_covering_batch_one_...).
    assert mj["invention_cost_unknown_reason"].startswith("batches past the owned T2 BPC runs")


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
    assert invention["job_cost"] == pytest.approx(_T2_INVENTION_FEE_PER_ATTEMPT * 0.2)  # 0.012

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
    assert mj["total_job_cost"] == pytest.approx(0.012)
    assert mj["total_cost"] == pytest.approx(90.012)

    # Slot time: parent 200 + component 96 (1 run x 100 s, TE 4) + invention
    # 40 x 0.2 + copy 20 x 0.2.
    assert mj["time_seconds"] == 200 + 96 + 8 + 4
    # Elapsed: 2 whole attempts of 40 s and 2 source-copy runs of 20 s in sequence.
    assert mj["elapsed_time_seconds"] == 200 + 96 + 80 + 40

    # The top-level T2 row in the same refresh is unchanged.
    t2 = rows[5002]["manufacturing_job"]
    assert t2["material_cost"] == pytest.approx(90.0)
    assert t2["total_job_cost"] == pytest.approx(0.012)
    assert t2["elapsed_time_seconds"] == 96 + 80 + 40  # manufacturing 96 (TE 4)


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


# --- round 3 final wave I1: datacores for the batches past owned T2 BPCs ------


def _owned_t2_bpc(*, runs: int, item_id: int = 81) -> SimpleNamespace:
    return SimpleNamespace(
        type_id=9002, item_id=item_id, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=runs,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )


def test_owned_bpc_covering_batch_one_still_buys_datacores_for_later_batches(monkeypatch) -> None:
    # A 10-run owned BPC covers batch 1 (1 run) and the next 9 one-run
    # batches (fix round 1, I2: later batches use what an owned BPC covers).
    # Past those 10 runs the batches invent: 1 BPC (10 runs), p 0.5 -> 2
    # whole attempts x 2 = 4 datacores.
    row = _invention_overview(monkeypatch, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}))[5002]
    mj = row["manufacturing_job"]
    assert "invention_procurement_materials" not in mj
    plan = mj["invention_bpc_plan"]
    assert (plan["owned_blueprint_runs"], plan["runs_per_invented_blueprint"], plan["whole_attempts_per_blueprint"]) == (10, 10, 2)

    row["max_batches_total"] = 5
    assert _shopping_line(row, 204) is None  # 5 runs, all on the owned BPC
    row["max_batches_total"] = 15
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)  # runs 11..15: 1 BPC


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
    # One later batch on its own (the no-BPC-plan fallback list): 20.
    later = _later_plan(mj, 2)
    assert later["invention_procurement_materials"]["204"]["quantity"] == 20

    row["max_batches_total"] = 3
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (52, 52)  # 12 + 2 x 20


def test_one_run_batches_share_invented_bpc_runs(monkeypatch) -> None:
    # Fix round 1, I2: no owned BPC, 1-run batches, 10-run BPCs at p 0.5.
    # 5 batches = 5 runs = 1 BPC = 2 attempts = 4 datacores (was 4 per batch, 20);
    # 25 batches = 3 BPCs = 6 attempts = 12.
    row = _invention_overview(monkeypatch)[5002]
    mj = row["manufacturing_job"]
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4

    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)
    row["max_batches_total"] = 25
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (12, 12)


def test_full_bpc_batches_without_a_decryptor_buy_what_they_did_before(monkeypatch) -> None:
    # 10-run batches on 10-run BPCs: 1 BPC = 2 attempts = 4 datacores per batch.
    row = _invention_overview(monkeypatch, maximize_bp_runs=True)[5002]
    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (20, 20)


def test_owned_datacores_are_netted_once_with_an_extra_batch_list(monkeypatch) -> None:
    # 3 owned: batch 1 takes them, the later-batch lines take 0. 10-run
    # batches: 4 datacores each, 5 batches = 20 - 3 = 17.
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, owned_item_inventory=({204: 3}, {204: 60.0}),
    )[5002]
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


# --- round 4 Task I R1: nested owned BPC in batch 1, datacores for batches 2..N -


def test_nested_owned_bpc_in_batch_one_still_buys_nested_datacores_for_later_batches(monkeypatch) -> None:
    # The parent 7100 builds 1 T2 component (5002) per run. A 10-run owned 9002
    # BPC covers batch 1's component run, so batch 1 invents nothing. Every
    # later batch builds that run again without the BPC and invents it:
    # 1 run / 10 per BPC -> 1 BPC, p 0.5 -> 2 whole attempts x 2 = 4 datacores.
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]
    nested = mj["recursive_activity_breakdown"]["manufacturing:5002"]
    assert nested["recommended_action"] == "build"
    assert "invention:9002" not in nested["nested"]
    assert "invention_procurement_materials" not in mj
    later = _later_plan(mj, 2)
    extra = later["invention_procurement_materials"]["204"]
    assert (extra["quantity"], extra["take_quantity"], extra["buy_quantity"]) == (4, 0, 4)
    assert later["procurement_materials"]["34"]["quantity"] == 10

    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (16, 16)  # 4 x (5 - 1)


def test_nested_without_owned_bpc_extra_batch_datacores_are_unchanged(monkeypatch) -> None:
    # No owned BPC: batch 1 already invents the run (4 datacores), and so does
    # every later batch: 4 + 4 x 4 = 20.
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
    )[7100]
    mj = row["manufacturing_job"]
    assert mj["invention_procurement_materials"]["204"]["quantity"] == 4
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4

    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (20, 20)


def test_nested_sub_build_bought_in_batch_one_adds_no_extra_batch_datacores(monkeypatch) -> None:
    # The component is cheaper to buy (1 ISK), so it is bought in batch 1 and
    # the extra-batch list gets nothing for it, owned BPC or not.
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(),
        prices={**_NESTED_FAKE_PRICES, 5002: 1.0},
        owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]
    assert "manufacturing:5002" not in mj["recursive_activity_breakdown"]
    assert "invention_procurement_materials_per_extra_batch" not in mj
    # The rejected sub-build spends no owned BPC run (FU-61), so every later
    # batch repeats batch 1: no schedule, the component bought each batch.
    assert "extra_batch_schedule" not in mj
    assert mj["procurement_materials"]["5002"]["quantity"] == 1
    row["max_batches_total"] = 5
    component = _shopping_line(row, 5002)
    assert component is not None and component["buy"] == 5
    assert _shopping_line(row, 204) is None


# --- round 4 Task I R3: unknown odds past an owned BPC are flagged, never 0 ----


def _unknown_need_rows(rows: list[dict]) -> dict:
    from eve_online_industry_tracker.application.industry.shopping_list import invention_need_unknown_rows

    return invention_need_unknown_rows(rows)


def test_unknown_odds_with_owned_bpc_covering_batch_one_flag_the_later_batches(monkeypatch, caplog) -> None:
    # p 0 and floor 0: batch 1 runs on the owned 10-run BPC and needs no
    # invention, but batches 2..N must invent with unknown odds.
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    with caplog.at_level("WARNING"):
        row = _invention_overview(
            monkeypatch, blueprint_rows=rows, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
            real_confidence=True, adm_overrides={"invention_probability_floor": 0.0},
        )[5002]
    mj = row["manufacturing_job"]

    assert mj["material_cost"] == pytest.approx(50.0)  # batch 1 itself is unchanged
    assert "no invention success probability" in mj["invention_cost_unknown_reason"]
    assert "past the owned T2 BPC runs" in mj["invention_cost_unknown_reason"]
    assert mj["invention_datacore_need_unknown"]["from_batch"] == 2
    assert mj["material_pricing_confidence"] == "Low"
    assert mj["overall_pricing_confidence"] == "Low"
    # Batch 1 runs on the owned BPC, so its cost is exact: not the
    # "T2 cost is understated" wording (Task I fix round 1).
    assert mj["invention_datacore_need_unknown"]["batch_one_cost_exact"] is True
    assert any(
        "Datacore need unknown for batches past the owned BPC runs" in r
        and "batch 1 cost is exact, later batches will cost more" in r
        for r in mj["pricing_confidence_reasons"]
    )
    assert not any("Invention cost unknown" in r for r in mj["pricing_confidence_reasons"])
    # One WARNING for the row, not "invention cost left unknown" as well.
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert sum("datacore need unknown" in m for m in warnings) == 1
    assert not any("invention cost left unknown" in m for m in warnings)
    # No made-up datacore quantity anywhere.
    assert "invention_procurement_materials" not in mj
    assert "invention_procurement_materials_per_extra_batch" not in mj

    row["max_batches_total"] = 5
    assert _shopping_line(row, 204) is None
    flagged = _unknown_need_rows([row])
    assert list(flagged) == ["T2 Module"]
    assert "no invention success probability" in flagged["T2 Module"]
    # One batch runs on the owned BPC only: nothing unknown to flag.
    row["max_batches_total"] = 1
    assert _unknown_need_rows([row]) == {}


def test_unknown_odds_with_nested_owned_bpc_covering_batch_one_buy_the_component_later(monkeypatch, caplog) -> None:
    # Batch 1 builds the component on the owned BPC (a known cost). Later
    # batches would have to invent it with unknown odds, so they buy it, the
    # same rule as batch 1 (FU-60): no datacore guess and no flag.
    with caplog.at_level("WARNING"):
        row = _invention_overview(
            monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(probability_pct=0.0),
            prices=_NESTED_FAKE_PRICES, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
            real_confidence=True, adm_overrides={"invention_probability_floor": 0.0},
        )[7100]
    mj = row["manufacturing_job"]

    assert mj["recursive_activity_breakdown"]["manufacturing:5002"]["recommended_action"] == "build"
    assert _later_plan(mj, 2)["procurement_materials"]["5002"]["quantity"] == 1
    assert "invention_datacore_need_unknown" not in mj
    assert "invention_cost_unknown_reason" not in mj
    assert not any(m.startswith("Product 7100: datacore need unknown")
                   for m in (r.getMessage() for r in caplog.records if r.levelname == "WARNING"))

    row["max_batches_total"] = 3
    line = _shopping_line(row, 5002)
    assert line is not None and line["need"] == 2
    assert _unknown_need_rows([row]) == {}


def test_unknown_odds_without_owned_bpc_flag_every_batch(monkeypatch) -> None:
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    row = _invention_overview(
        monkeypatch, blueprint_rows=rows, adm_overrides={"invention_probability_floor": 0.0},
    )[5002]
    assert row["manufacturing_job"]["invention_datacore_need_unknown"]["from_batch"] == 1
    assert _shopping_line(row, 204) is None
    assert list(_unknown_need_rows([row])) == ["T2 Module"]


def test_known_odds_rows_are_not_flagged(monkeypatch) -> None:
    rows = _invention_overview(monkeypatch, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}))
    rows[5002]["max_batches_total"] = 5
    assert "invention_datacore_need_unknown" not in rows[5002]["manufacturing_job"]
    assert _unknown_need_rows(list(rows.values())) == {}



def test_batch_one_unknown_odds_keep_the_understated_wording(monkeypatch, caplog) -> None:
    # No owned BPC: batch 1 itself invents with unknown odds, so the T2 cost
    # really leaves the invention out. One WARNING (the batch-1 evaluation's).
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    with caplog.at_level("WARNING"):
        mj = _invention_overview(
            monkeypatch, blueprint_rows=rows, real_confidence=True,
            adm_overrides={"invention_probability_floor": 0.0},
        )[5002]["manufacturing_job"]
    assert mj["invention_datacore_need_unknown"]["batch_one_cost_exact"] is False
    assert mj["material_pricing_confidence"] == "Low"
    assert any(
        "Invention cost unknown" in r and "so it is understated" in r for r in mj["pricing_confidence_reasons"]
    )
    assert not any("batch 1 cost is exact" in r for r in mj["pricing_confidence_reasons"])
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert sum("invention cost left unknown" in m for m in warnings) == 1
    assert not any("datacore need unknown" in m for m in warnings)


# --- round 5 Task 1: invented BPC baseline ME 2 / TE 4, decryptor data -------

_ACCELERANT_LIKE = DecryptorOption(
    type_id=34201, type_name="Accelerant Decryptor", probability_multiplier=1.2,
    me_modifier=2, te_modifier=10, max_run_modifier=1,
)


def _planning_context(service, *, maximize_bp_runs=False):
    return service._build_planning_context(
        force_refresh=False, build_from_bpc=True, include_reactions=False, maximize_bp_runs=maximize_bp_runs,
        group_identical_bpcs=True, have_blueprint_source_only=False, market_hub="jita",
        material_price_side="sell", product_price_side="sell", industry_profile_id=None,
        owned_blueprints_scope="all_characters", character_id=1, progress_callback=None,
    )


def test_an_invented_t2_row_is_built_at_me_2_te_4(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True)[5002]["manufacturing_job"]

    assert mj["blueprint_source_kind"] == "unowned_blueprint_copy"
    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (2, 4)
    assert mj["materials"]["34"]["quantity"] == 98  # ceil(10 runs x 10 x 0.98), was 100
    assert mj["manufacturing_time_seconds"] == 960  # 10 x 100 s x 0.96, was 1000


def test_an_unowned_t1_bpc_without_an_invention_path_stays_me_0(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True)[6001]["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (0, 0)
    assert mj["materials"]["34"]["quantity"] == 30


def test_a_t2_blueprint_nothing_invents_is_not_given_the_invented_baseline(monkeypatch) -> None:
    rows = _invented_t2_blueprint_rows()[1:]  # the T2 blueprint without its invention source
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True, blueprint_rows=rows)[5002]["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (0, 0)
    assert "invention" not in mj["activity_breakdown"]
    assert mj["materials"]["34"]["quantity"] == 100


def test_a_nested_invented_component_is_built_at_me_2_without_a_copy_job(monkeypatch) -> None:
    rows = _nested_invention_blueprint_rows()
    # A real T2 blueprint has an SDE copy time. The invention supplies the BPC,
    # so no copy of the T2 blueprint may be costed (FU-58).
    rows[1]["copying_job"] = {"time_seconds": 30}
    out = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=rows,
        prices=_NESTED_FAKE_PRICES, profile=_INVENTION_ONLY_PROFILE,
    )
    mj = out[7100]["manufacturing_job"]
    nested = mj["recursive_activity_breakdown"]["manufacturing:5002"]

    assert nested["recommended_action"] == "build"
    assert "copying" not in nested["nested"]
    assert "invention:9002" in nested["nested"]
    # The parent (no invention path, ME 0) needs 10 components; the component
    # job runs on the invented BPC at ME 2: ceil(10 x 10 x 0.98) = 98 Tritanium.
    assert mj["procurement_materials"]["34"]["quantity"] == 98


def test_decryptor_options_are_loaded_and_priced_like_datacores(monkeypatch) -> None:
    service = _invention_service(monkeypatch, prices={**_INVENTION_FAKE_PRICES, 34201: 50.0})
    service._load_decryptor_options = MethodType(  # type: ignore[attr-defined]
        lambda self: (NO_DECRYPTOR, _ACCELERANT_LIKE), service,
    )
    ctx = _planning_context(service)

    assert ctx.decryptor_options == (NO_DECRYPTOR, _ACCELERANT_LIKE)
    assert ctx.material_price_map[34201]["unit_price"] == 50.0
    assert ctx.blueprint_row_by_type_id[9002]["blueprint_type_id"] == 9002
    # Task 3: the context now answers with the blueprint's chosen decryptor
    # (no sell price for 5002 here, so the cheapest per unit: Accelerant).
    assert ctx.invention_context.decryptor_for_blueprint(9002) is _ACCELERANT_LIKE


def test_without_an_sde_session_there_are_no_decryptors_and_a_warning(caplog) -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
    service._sessions = None  # type: ignore[attr-defined]
    with caplog.at_level("WARNING"):
        assert service._load_decryptor_options() == (NO_DECRYPTOR,)
    assert any("no decryptor data" in r.getMessage() for r in caplog.records)


# --- round 5 Task 1, fix round 1: one decryptor source, one invention predicate ---


def _top_level_t2_row(service, ctx, **kwargs):
    t2_row = _invented_t2_blueprint_rows()[1]
    return service._build_single_product_row(
        ctx, row=t2_row, raw_product=t2_row["manufacturing_job"]["products"][0],
        row_index=1, product_index=1, blueprint_copy_assets=[],
        blueprint_original_asset=None, effective_runs=10, **kwargs,
    )


def test_the_top_level_row_takes_its_decryptor_from_the_invention_context(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    ctx = _planning_context(service)
    asked: list[int] = []
    ctx.invention_context.decryptor_for_blueprint = lambda bp: (asked.append(bp), _ACCELERANT_LIKE)[1]

    mj = _top_level_t2_row(service, ctx)["manufacturing_job"]

    # ME 2 + 2 = 4, TE 4 + 10 = 14: the context's decryptor, as in the nested planner.
    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (4, 14)
    assert mj["materials"]["34"]["quantity"] == 96  # ceil(100 x 0.96)
    assert asked == [9002]


def test_an_explicit_decryptor_overrides_the_invention_context(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    ctx = _planning_context(service)
    ctx.invention_context.decryptor_for_blueprint = lambda bp: _ACCELERANT_LIKE

    mj = _top_level_t2_row(service, ctx, decryptor=NO_DECRYPTOR)["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (2, 4)


def test_a_row_that_does_not_invent_never_asks_for_a_decryptor(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    ctx = _planning_context(service)

    def _must_not_be_asked(bp):
        raise AssertionError(f"asked a decryptor for {bp}")

    ctx.invention_context.decryptor_for_blueprint = _must_not_be_asked
    t1_row = _invented_t2_blueprint_rows()[0]
    row = service._build_single_product_row(
        ctx, row=t1_row, raw_product=t1_row["manufacturing_job"]["products"][0],
        row_index=0, product_index=0, blueprint_copy_assets=[],
        blueprint_original_asset=None, effective_runs=10,
    )
    assert row is not None
    assert row["manufacturing_job"]["blueprint_material_efficiency"] == 0


def test_a_source_row_without_an_invention_job_dict_is_not_an_invention_path(monkeypatch, caplog) -> None:
    # The nested planner requires invention_job to be a dict; the top level now
    # uses the same predicate (it used to coerce a None job to {} and invent).
    service = _invention_service(monkeypatch)
    ctx = _planning_context(service)
    ctx.invention_row_by_blueprint_type_id[9002] = {"blueprint_type_id": 1001, "invention_job": None}

    with caplog.at_level("WARNING"):
        mj = _top_level_t2_row(service, ctx)["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (0, 0)
    assert mj["materials"]["34"]["quantity"] == 100
    # No invention, so no chance and no base-chance fallback for it.
    assert "invention" not in mj["activity_breakdown"]
    assert "invention_chance" not in mj
    assert "invention_chance_notes" not in mj
    assert not any("Invention chance" in r.getMessage() for r in caplog.records)


# --- round 5 Task 2: invention chance from the corp's best inventor -----------
# Fake skill levels only.

_INVENTION_SKILLS = [
    {"type_id": 11433, "type_name": "High Energy Physics", "group_id": 270},
    {"type_id": 11454, "type_name": "Caldari Starship Engineering", "group_id": 270},
    {"type_id": 21790, "type_name": "Caldari Encryption Methods", "group_id": 270},
]


def _skilled_overview(monkeypatch, *, levels, encryption=frozenset({21790}), real_confidence=False,
                      skill_entries=None, **kwargs):
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["skill_entries"] = [dict(s) for s in (skill_entries or _INVENTION_SKILLS)]
    service = _invention_service(monkeypatch, blueprint_rows=rows, real_confidence=real_confidence, **kwargs)
    service._get_inventor_skill_levels = MethodType(lambda self: levels, service)  # type: ignore[attr-defined]
    service._load_encryption_skill_type_ids = MethodType(lambda self: encryption, service)  # type: ignore[attr-defined]
    out = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    return {row["type_id"]: row for row in out}


def test_the_best_inventors_skills_raise_the_chance(monkeypatch) -> None:
    levels = {1: {11433: 4, 11454: 4, 21790: 4}, 2: {11433: 5, 11454: 5, 21790: 3}}
    mj = _skilled_overview(monkeypatch, levels=levels)[5002]["manufacturing_job"]

    factor = 1 + 10 / 30 + 3 / 40  # pilot 2 beats pilot 1 (1 + 8/30 + 4/40)
    p = 0.5 * factor
    chance = mj["invention_chance"]
    assert chance["inventor_character_id"] == 2
    assert chance["skill_factor"] == pytest.approx(factor)
    assert chance["effective_probability"] == pytest.approx(p)
    assert mj["activity_breakdown"]["invention"]["success_probability"] == pytest.approx(p)
    # 1 run / 10 per BPC / p attempts, 2 datacores x 100 each.
    assert mj["invention_material_cost"] == pytest.approx(2 * 100 * (1 / 10) / p)
    assert chance["skills_unknown_reason"] is None
    assert chance["note"] is None


def test_an_untrained_corp_is_a_known_factor_of_one(monkeypatch) -> None:
    mj = _skilled_overview(monkeypatch, levels={1: {}})[5002]["manufacturing_job"]

    assert mj["invention_chance"]["skill_factor"] == 1.0
    assert mj["invention_chance"]["skills_unknown_reason"] is None
    assert mj["invention_material_cost"] == pytest.approx(40.0)


def test_unloaded_skills_fall_back_to_the_base_chance_with_a_reason(monkeypatch, caplog) -> None:
    with caplog.at_level("WARNING"):
        mj = _skilled_overview(monkeypatch, levels={}, real_confidence=True)[5002]["manufacturing_job"]

    assert mj["invention_chance"]["skill_factor"] is None
    assert mj["invention_chance"]["skills_unknown_reason"] == "no corp character has its skills loaded"
    assert mj["invention_material_cost"] == pytest.approx(40.0)  # base 0.5
    assert any("SDE base chance" in r for r in mj["pricing_confidence_reasons"])
    assert sum("no corp character has its skills loaded" in r.getMessage()
               for r in caplog.records if r.levelname == "WARNING") == 1


def test_a_blueprint_without_an_encryption_skill_uses_the_science_term_with_a_row_note(monkeypatch) -> None:
    """A3: the formula with the science term only, not the base chance."""
    levels = {1: {11433: 4, 11454: 4, 21790: 5}}
    mj = _skilled_overview(
        monkeypatch, levels=levels, skill_entries=_INVENTION_SKILLS[:2], real_confidence=True,
    )[5002]["manufacturing_job"]

    p = 0.5 * (1 + 8 / 30)
    chance = mj["invention_chance"]
    assert chance["skill_factor"] == pytest.approx(1 + 8 / 30)
    assert chance["encryption_skill_type_id"] is None
    assert chance["skills_unknown_reason"] is None
    assert "no encryption skill" in chance["note"]
    assert mj["invention_material_cost"] == pytest.approx(2 * 100 * (1 / 10) / p)
    # The note reaches the UI as an informational reason line ...
    assert f"Invention chance: {chance['note']}." in mj["pricing_confidence_reasons"]
    assert not any("SDE base chance" in r for r in mj["pricing_confidence_reasons"])
    # ... and does not change the confidence level.
    with_encryption = _skilled_overview(monkeypatch, levels=levels, real_confidence=True)[5002]["manufacturing_job"]
    assert mj["overall_pricing_confidence"] == with_encryption["overall_pricing_confidence"]
    assert mj["material_pricing_confidence"] == with_encryption["material_pricing_confidence"]


def test_the_admin_floor_still_applies_to_the_skilled_chance(monkeypatch) -> None:
    """A non-zero base whose skilled chance stays under the floor:
    0.02 x (1 + 10/30 + 5/40) = 0.0292 < 0.05."""
    rows_levels = {1: {11433: 5, 11454: 5, 21790: 5}}
    service_rows = _invented_t2_blueprint_rows()
    service_rows[0]["invention_job"]["products"][0]["probability_pct"] = 2.0
    service_rows[0]["invention_job"]["skill_entries"] = [dict(s) for s in _INVENTION_SKILLS]
    service = _invention_service(monkeypatch, blueprint_rows=service_rows, adm_overrides={"invention_probability_floor": 0.05})
    service._get_inventor_skill_levels = MethodType(lambda self: rows_levels, service)  # type: ignore[attr-defined]
    service._load_encryption_skill_type_ids = MethodType(lambda self: frozenset({21790}), service)  # type: ignore[attr-defined]
    rows = {r["type_id"]: r for r in service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )}
    mj = rows[5002]["manufacturing_job"]
    assert mj["invention_chance"]["probability"] == pytest.approx(0.02 * (1 + 10 / 30 + 5 / 40))
    assert mj["invention_chance"]["effective_probability"] == pytest.approx(0.05)
    assert mj["activity_breakdown"]["invention"]["success_probability"] == pytest.approx(0.05)


def test_inventor_skills_come_from_every_loaded_corp_character() -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace(char_manager=SimpleNamespace(get_characters=lambda: [  # type: ignore[attr-defined]
        {"character_id": 1, "skills": {"skills": [
            {"skill_id": 11433, "trained_skill_level": 4}, {"skill_id": 21790, "trained_skill_level": 2},
        ]}},
        {"character_id": 2, "skills": {}},          # not fetched: unknown, left out
        {"character_id": 3},                        # not fetched either
        {"character_id": 4, "industry_enabled": False, "skills": {"skills": [  # excluded from industry
            {"skill_id": 11433, "trained_skill_level": 5},
        ]}},
        {"character_id": 5, "industry_enabled": True, "skills": {"skills": [
            {"skill_id": 11433, "trained_skill_level": 1},
        ]}},
    ]))
    assert service._get_inventor_skill_levels() == {1: {11433: 4, 21790: 2}, 5: {11433: 1}}


def test_no_character_manager_means_no_inventor_skills() -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
    assert service._get_inventor_skill_levels() == {}


def _nested_skilled_overview(monkeypatch, *, levels, real_confidence=False):
    service = _invention_service(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, profile=_INVENTION_ONLY_PROFILE, real_confidence=real_confidence,
    )
    service._get_inventor_skill_levels = MethodType(lambda self: levels, service)  # type: ignore[attr-defined]
    out = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    return {row["type_id"]: row for row in out}


def test_a_nested_invention_uses_the_best_inventors_chance(monkeypatch) -> None:
    mj = _nested_skilled_overview(monkeypatch, levels={1: {11433: 4, 11454: 4, 21790: 4}})[7100]["manufacturing_job"]

    factor = 1 + 8 / 30 + 4 / 40
    invention = mj["recursive_activity_breakdown"]["manufacturing:5002"]["nested"]["invention:9002"]
    assert invention["success_probability"] == pytest.approx(0.5 * factor)
    assert invention["invention_chance"]["skill_factor"] == pytest.approx(factor)
    assert invention["invention_chance"]["inventor_character_id"] == 1
    # 1 component run / 10 per BPC / p.
    assert invention["attempts"] == pytest.approx(0.1 / (0.5 * factor))
    assert "invention_chance_notes" not in mj


def test_a_nested_skills_fallback_is_a_row_reason(monkeypatch) -> None:
    mj = _nested_skilled_overview(monkeypatch, levels={}, real_confidence=True)[7100]["manufacturing_job"]

    invention = mj["recursive_activity_breakdown"]["manufacturing:5002"]["nested"]["invention:9002"]
    assert invention["success_probability"] == pytest.approx(0.5)
    assert invention["invention_chance"]["skills_unknown_reason"] == "no corp character has its skills loaded"
    assert len(mj["invention_chance_notes"]) == 1
    assert "sub-build: uses the SDE base chance (no corp character has its skills loaded)" in mj["invention_chance_notes"][0]
    assert f"Invention chance: {mj['invention_chance_notes'][0]}." in mj["pricing_confidence_reasons"]


def _overview_with_real_skill_loader(monkeypatch, characters):
    """The real _get_inventor_skill_levels over a fake character manager."""
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["skill_entries"] = [dict(s) for s in _INVENTION_SKILLS]
    service = _invention_service(monkeypatch, blueprint_rows=rows, real_confidence=True)
    del service._get_inventor_skill_levels  # back to the class method
    service._state.char_manager = SimpleNamespace(get_characters=lambda: characters)  # type: ignore[attr-defined]
    out = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    return {row["type_id"]: row for row in out}[5002]["manufacturing_job"]


_SKILLED = {"skills": [{"skill_id": 11433, "trained_skill_level": 5}]}


@pytest.mark.parametrize(("characters", "reason"), [
    # Every character with loaded skills is excluded from industry.
    ([{"character_id": 1, "industry_enabled": False, "skills": _SKILLED},
      {"character_id": 2, "industry_enabled": True, "skills": {}}],
     "every corp character with loaded skills is excluded from industry"),
    # Nobody has loaded skills (an excluded pilot without skills changes nothing).
    ([{"character_id": 1, "industry_enabled": True, "skills": {}},
      {"character_id": 2, "industry_enabled": False}],
     "no corp character has its skills loaded"),
])
def test_an_empty_inventor_list_names_why(monkeypatch, caplog, characters, reason) -> None:
    with caplog.at_level("WARNING"):
        mj = _overview_with_real_skill_loader(monkeypatch, characters)

    assert mj["invention_chance"]["skills_unknown_reason"] == reason
    assert mj["invention_chance"]["skill_factor"] is None
    assert mj["invention_material_cost"] == pytest.approx(40.0)  # base 0.5
    assert f"Invention chance uses the SDE base chance ({reason})" in " ".join(mj["pricing_confidence_reasons"])
    assert sum(reason in r.getMessage() for r in caplog.records if r.levelname == "WARNING") == 1


# --- round 5 Task 3: the best decryptor per invented item (amendment A1) ------
# Highest profit per invention cycle: the expected attempts for one successful
# BPC plus the runs that BPC yields. Fixture: p 0.5 (no skills loaded: base
# chance), 10 runs per BPC, 2 datacores x 100 per attempt, 10 Tritanium x 5 per
# run, no fees. Fake prices.
# No decryptor:            10 runs, ME 2 -> 98 Trit = 490; 2 attempts x 200 = 400.
#                          890 per cycle, 89.00 per unit.
# Accelerant-like at 50:   11 runs, ME 4 -> 106 Trit = 530; 1/0.6 attempts x 250
#                          = 416.67. 946.67 per cycle, 86.06 per unit (cheapest).
# Attainment-like at 1000: 14 runs, ME 1 -> 139 Trit = 695; 1/0.9 attempts x 1200
#                          = 1333.33. 2028.33 per cycle, 144.88 per unit.
# At a 500 ISK sell price the profit per cycle is 4110 / 4553.33 / 4971.67.

_ATTAINMENT_LIKE = DecryptorOption(
    type_id=34202, type_name="Attainment Decryptor", probability_multiplier=1.8,
    me_modifier=-1, te_modifier=4, max_run_modifier=4,
)
_SYMMETRY_LIKE = DecryptorOption(
    type_id=34206, type_name="Symmetry Decryptor", probability_multiplier=1.0,
    me_modifier=1, te_modifier=8, max_run_modifier=2,
)
_DECRYPTOR_PRICES = {**_INVENTION_FAKE_PRICES, 34201: 50.0, 34202: 1000.0, 34206: 1.0}
_ACC_CYCLE_COST = 530 + (1 / 0.6) * (2 * 100 + 50)
_ATT_CYCLE_COST = 695 + (1 / 0.9) * (2 * 100 + 1000)


def _decryptor_service(monkeypatch, options, *, prices=None, **kwargs):
    service = _invention_service(monkeypatch, prices=prices or _DECRYPTOR_PRICES, **kwargs)
    service._load_decryptor_options = MethodType(  # type: ignore[attr-defined]
        lambda self: (NO_DECRYPTOR, *options), service,
    )
    return service


def _decryptor_overview(monkeypatch, options, *, prices=None, maximize_bp_runs=True, **kwargs):
    service = _decryptor_service(monkeypatch, options, prices=prices, **kwargs)
    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False,
        maximize_bp_runs=maximize_bp_runs, character_id=1,
    )
    return {row["type_id"]: row for row in rows}


def test_the_highest_profit_per_cycle_wins_over_the_cheapest_per_unit(monkeypatch) -> None:
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE, _ATTAINMENT_LIKE], prices={**_DECRYPTOR_PRICES, 5002: 500.0},
    )[5002]["manufacturing_job"]

    choice = mj["invention_decryptor"]
    assert choice["type_id"] == 34202
    assert choice["criterion"] == "profit_per_cycle" and choice["fallback_reason"] is None
    assert choice["profit_per_cycle"] == pytest.approx(14 * 500 - _ATT_CYCLE_COST)
    assert choice["baseline_profit_per_cycle"] == pytest.approx(10 * 500 - 890)
    candidates = {c["type_id"]: c for c in choice["candidates"]}
    assert candidates[34201]["profit_per_cycle"] == pytest.approx(11 * 500 - _ACC_CYCLE_COST)
    assert candidates[34201]["cost_per_unit"] == pytest.approx(_ACC_CYCLE_COST / 11)  # the cheapest
    assert [candidates[t]["units_per_cycle"] for t in (None, 34201, 34202)] == [10, 11, 14]
    # The row is built on the chosen Attainment BPC: ME 2 - 1, 14 runs, p 0.9.
    assert mj["blueprint_material_efficiency"] == 1
    assert mj["activity_breakdown"]["invention"]["runs_per_invented_blueprint"] == 14
    assert mj["activity_breakdown"]["invention"]["success_probability"] == pytest.approx(0.9)


def test_an_unknown_sell_price_picks_the_cheapest_per_unit_and_says_so(monkeypatch) -> None:
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE, _ATTAINMENT_LIKE])[5002]["manufacturing_job"]

    choice = mj["invention_decryptor"]
    assert choice["type_id"] == 34201
    assert choice["criterion"] == "cost_per_unit"
    assert choice["fallback_reason"] == "sell price unknown: chose lowest cost per unit"
    assert choice["reason"].endswith("; sell price unknown: chose lowest cost per unit")
    assert choice["cost_per_unit"] == pytest.approx(_ACC_CYCLE_COST / 11)
    assert choice["baseline_cost_per_unit"] == pytest.approx(89.0)
    costs = {c["type_id"]: c["cost_per_unit"] for c in choice["candidates"]}
    assert costs[34202] == pytest.approx(_ATT_CYCLE_COST / 14)


def test_the_chosen_decryptor_sets_me_te_runs_and_buys_one_per_attempt(monkeypatch) -> None:
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE])[5002]["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (4, 14)
    assert mj["materials"]["34"]["quantity"] == 96
    invention = mj["activity_breakdown"]["invention"]
    assert invention["runs_per_invented_blueprint"] == 11
    assert invention["success_probability"] == pytest.approx(0.6)
    attempts = 10 / 11 / 0.6
    assert mj["expected_invention_materials"]["34201"]["quantity"] == pytest.approx(attempts)
    assert mj["invention_material_cost"] == pytest.approx(attempts * (2 * 100 + 50))
    # Whole attempts: ceil(10 / 11) = 1 BPC, ceil(1 / 0.6) = 2 attempts.
    buy = mj["invention_procurement_materials"]
    assert (buy["204"]["quantity"], buy["34201"]["quantity"]) == (4, 2)
    # Batch 12 starts on a fresh BPC (110 runs = 10 BPCs): one 10-run job,
    # the same whole attempts on its own.
    extra = _later_plan(mj, 12)["invention_procurement_materials"]
    assert (extra["204"]["quantity"], extra["34201"]["quantity"]) == (4, 2)
    # The fee's job value is 0.02 x the invented blueprint's per-run EIV (50)
    # per attempt: neither the datacores nor the decryptor are part of it.
    assert invention["estimated_item_value"] == pytest.approx(0.02 * 50 * attempts)


def test_a_run_modifier_that_crosses_a_bpc_boundary_halves_the_whole_attempts(monkeypatch) -> None:
    # 12 runs: 2 BPCs at 10 runs each (4 attempts, 8 datacores) without a
    # decryptor; one 12-run BPC with the +2 decryptor (2 attempts, 4 + 2).
    rows = _rows_with(probability_pct=50.0, bpc_runs=10, t2_limit=12)
    mj = _decryptor_overview(monkeypatch, [_SYMMETRY_LIKE], blueprint_rows=rows)[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["type_id"] == 34206
    buy = mj["invention_procurement_materials"]
    assert (buy["204"]["quantity"], buy["34206"]["quantity"]) == (4, 2)


def test_a_decryptor_without_a_market_price_is_never_chosen(monkeypatch) -> None:
    prices = {k: v for k, v in _DECRYPTOR_PRICES.items() if k != 34201}
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE], prices=prices)[5002]["manufacturing_job"]

    choice = mj["invention_decryptor"]
    assert choice["type_id"] is None
    assert choice["candidates"][1]["skip_reason"] == "no market price for Accelerant Decryptor"
    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (2, 4)


def test_no_decryptor_data_plans_without_a_decryptor_and_says_why(monkeypatch) -> None:
    mj = _decryptor_overview(monkeypatch, [])[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["type_id"] is None
    assert mj["invention_decryptor"]["reason"] == "no decryptor: the SDE gave no decryptor data"


def test_decryptor_selection_off_plans_without_a_decryptor(monkeypatch) -> None:
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE],
        adm_overrides={"invention_decryptor_selection_enabled": False},
    )[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["reason"] == "no decryptor: decryptor selection is off (admin setting)"
    assert mj["blueprint_material_efficiency"] == 2
    assert "34201" not in mj["expected_invention_materials"]


def test_a_blueprint_without_invention_gets_no_decryptor_payload(monkeypatch) -> None:
    out = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=_invented_t2_blueprint_rows()[1:])
    assert "invention_decryptor" not in out[5002]["manufacturing_job"]
    assert "invention_chance" not in out[5002]["manufacturing_job"]
    t1 = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE])[6001]["manufacturing_job"]
    assert "invention_decryptor" not in t1  # the T1 source row invents nothing


def test_unknown_invention_odds_compare_no_decryptor_and_warn_once(monkeypatch, caplog) -> None:
    rows = _rows_with(probability_pct=0.0, bpc_runs=10, t2_limit=10)
    with caplog.at_level("WARNING"):
        mj = _decryptor_overview(
            monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=rows,
            adm_overrides={"invention_probability_floor": 0.0},
        )[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["type_id"] is None
    assert "unknown" in mj["invention_decryptor"]["reason"]
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert sum("invention cost left unknown" in m for m in warnings) == 1


def test_a_nested_invented_component_uses_its_blueprints_decryptor(monkeypatch) -> None:
    # 5002 sells for 10,000: Accelerant earns 11 x 10,000 - 946.67 per cycle
    # against 10 x 10,000 - 890 without a decryptor.
    out = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=_nested_invention_blueprint_rows(),
        prices={**_NESTED_FAKE_PRICES, 34201: 50.0},
    )
    mj = out[7100]["manufacturing_job"]

    # The parent's 10 components are built on an Accelerant BPC (ME 4).
    assert mj["procurement_materials"]["34"]["quantity"] == 96
    assert any(e["type_id"] == 34201 for e in mj["expected_invention_materials"].values())
    assert mj["invention_procurement_materials"]["34201"]["quantity"] == 2
    assert "invention_decryptor" not in mj  # the parent itself is not invented
    assert out[5002]["manufacturing_job"]["invention_decryptor"]["type_id"] == 34201
    assert out[5002]["manufacturing_job"]["invention_decryptor"]["criterion"] == "profit_per_cycle"


def test_the_decryptor_choice_is_made_once_per_blueprint(monkeypatch) -> None:
    service = _decryptor_service(monkeypatch, [_ACCELERANT_LIKE])
    ctx = _planning_context(service)
    calls: list[int] = []
    real = service._evaluate_decryptors
    service._evaluate_decryptors = lambda c, bp: (calls.append(bp), real(c, bp))[1]  # type: ignore[method-assign]

    first = service._decryptor_choice_for(ctx, 9002)
    assert ctx.invention_context.decryptor_for_blueprint(9002) is first.option
    assert service._decryptor_choice_for(ctx, 9002) is first
    assert calls == [9002]


def test_the_planners_shopping_list_buys_one_decryptor_per_attempt() -> None:
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction
    from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder

    invent = AssignedAction(
        type_id=5002, type_name="T2 Module", action_type="invent", character_id=1, character_name="Pilot",
        quantity=None, runs=1, estimated_cost_isk=None, estimated_profit_isk=None,
        estimated_completion=None, notes=None,
        decryptor_type_id=34201, decryptor_type_name="Accelerant Decryptor",
    )
    blueprint_data = {
        1001: {"manufacturing": {"materials": [], "products": [{"type_id": 6001, "quantity": 1}]},
               "invention": {"materials": [{"type_id": 204, "type_name": "Datacore", "quantity": 2}],
                             "products": [{"type_id": 9002, "quantity": 10}]}},
        9002: {"manufacturing": {"materials": [], "products": [{"type_id": 5002, "quantity": 1}]}},
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
        assigned_actions=[invent], corp_assets=[],
        market_depth_cache={204: {"vwap_5d": 100.0}, 34201: {"vwap_5d": 50.0}},
        admin_settings=_Admin(), blueprint_data=blueprint_data, meta_resolver=_Resolver(),
    )
    by_type = {i.type_id: i for i in items}
    assert by_type[204].quantity == 2
    assert by_type[34201].quantity == 1
    assert by_type[34201].shopping_category == "invention_input"


# --- Task 3 fix round 1: net profit per cycle, candidate skip reasons ---------


def _fees(tax, broker, applies=True):
    return {"rates": {"sales_tax_fraction": tax, "broker_fee_fraction": broker}, "broker_fee_applies": applies}


def test_cycle_profit_is_net_of_sales_tax_and_broker_fee(monkeypatch) -> None:
    # Sell price 60. Gross: 10 x 60 - 890 = -290 vs 11 x 60 - 946.67 = -286.67,
    # so Accelerant. Net of 5% tax + 5% broker: 10 x 54 - 890 = -350 vs
    # 11 x 54 - 946.67 = -352.67, so no decryptor.
    prices = {**_DECRYPTOR_PRICES, 5002: 60.0}
    gross = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE], prices=prices)[5002]["manufacturing_job"]
    net = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], prices=prices, fee_context=_fees(0.05, 0.05),
    )[5002]["manufacturing_job"]

    assert gross["invention_decryptor"]["type_id"] == 34201
    choice = net["invention_decryptor"]
    assert choice["type_id"] is None
    assert choice["criterion"] == "profit_per_cycle"
    assert choice["baseline_profit_per_cycle"] == pytest.approx(10 * 60 * 0.9 - 890)
    assert choice["candidates"][1]["profit_per_cycle"] == pytest.approx(11 * 60 * 0.9 - _ACC_CYCLE_COST)


def test_a_broker_fee_that_does_not_apply_is_not_deducted(monkeypatch) -> None:
    # Instant sale (buy side): only the 5% tax. 10 x 57 - 890 = -320 vs
    # 11 x 57 - 946.67 = -319.67, so Accelerant.
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], prices={**_DECRYPTOR_PRICES, 5002: 60.0},
        fee_context=_fees(0.05, 0.05, applies=False),
    )[5002]["manufacturing_job"]
    assert mj["invention_decryptor"]["profit_per_cycle"] == pytest.approx(11 * 57 - _ACC_CYCLE_COST)
    assert mj["invention_decryptor"]["type_id"] == 34201


def test_an_unknown_sales_tax_falls_back_to_cost_per_unit_and_names_the_fees(monkeypatch) -> None:
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], prices={**_DECRYPTOR_PRICES, 5002: 500.0},
        fee_context=_fees(None, None),
    )[5002]["manufacturing_job"]

    choice = mj["invention_decryptor"]
    assert choice["type_id"] == 34201  # cheapest per unit
    assert choice["criterion"] == "cost_per_unit"
    assert choice["fallback_reason"] == (
        "sales tax unknown (no character to read Accounting from), so net proceeds are unknown: "
        "chose lowest cost per unit"
    )


def test_the_market_fee_context_is_resolved_once_per_refresh(monkeypatch) -> None:
    service = _decryptor_service(monkeypatch, [_ACCELERANT_LIKE], prices={**_DECRYPTOR_PRICES, 5002: 500.0})
    calls: list[dict] = []
    service._resolve_npc_market_fee_context = MethodType(  # type: ignore[attr-defined]
        lambda self, **kwargs: (calls.append(kwargs), _fees(0.05, 0.05))[1], service,
    )
    # The real sale-proceeds step, so it must reuse the planning context's fees.
    service._enrich_product_rows_with_sale_proceeds = MethodType(  # type: ignore[attr-defined]
        IndustryService._enrich_product_rows_with_sale_proceeds, service,
    )
    rows = {r["type_id"]: r for r in service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=True, character_id=1,
    )}
    assert calls == [{"character_id": 1, "market_hub": "jita", "product_price_side": "sell"}]
    assert rows[5002]["net_proceeds"] == pytest.approx(10 * 500 * 0.9)


def test_a_candidate_without_a_cost_says_why(monkeypatch) -> None:
    # M4: Tritanium has no price, so no candidate has a comparable cost.
    prices = {k: v for k, v in _DECRYPTOR_PRICES.items() if k != 34}
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE], prices=prices)[5002]["manufacturing_job"]
    candidates = mj["invention_decryptor"]["candidates"]
    assert [c["skip_reason"] for c in candidates] == ["an input has no price: Tritanium"] * 2

    rows = _rows_with(probability_pct=0.0, bpc_runs=10, t2_limit=10)
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=rows, adm_overrides={"invention_probability_floor": 0.0},
    )[5002]["manufacturing_job"]
    assert mj["invention_decryptor"]["candidates"][0]["skip_reason"] == (
        "invention cost unknown: no invention success probability (SDE and admin floor are both 0)"
    )


# --- Task 3 fix round 1, I2: later batches share invented BPC runs ------------
# Damage Control II (public SDE data: 2047 invents 2049 at 34%, 10 runs per
# BPC, 2 + 2 datacores per attempt). FAKE prices. 10-run batches.


def _m(type_id, name, quantity):
    return {"type_id": type_id, "type_name": name, "quantity": quantity, "category_name": "Material"}


def _dc2_rows() -> list[dict]:
    return [
        {"blueprint_type_id": 2047, "blueprint": {"type_id": 2047, "type_name": "Damage Control I Blueprint"},
         "manufacturing_job": {"materials": [_m(34, "Tritanium", 1062), _m(35, "Pyerite", 2), _m(36, "Mexallon", 53)],
                               "skill_entries": [], "time_seconds": 900, "max_production_limit": 200,
                               "products": [{"type_id": 2046, "type_name": "Damage Control I", "quantity": 1,
                                             "meta_group_name": "Tech I"}]},
         "copying_job": {"time_seconds": 720},
         "invention_job": {"materials": [_m(20416, "Datacore - Nanite Engineering", 2),
                                         _m(20415, "Datacore - Molecular Engineering", 2)],
                           "skill_entries": [], "time_seconds": 13800,
                           "products": [{"probability_pct": 34.0, "quantity": 10,
                                         "product": {"type_id": 2049, "type_name": "Damage Control II Blueprint"}}]}},
        {"blueprint_type_id": 2049, "blueprint": {"type_id": 2049, "type_name": "Damage Control II Blueprint"},
         "manufacturing_job": {"materials": [
             _m(11553, "Oscillator Capacitor Unit", 1), _m(11399, "Morphite", 1), _m(3689, "Mechanical Parts", 5),
             _m(11695, "Plasma Pulse Generator", 1), _m(11475, "R.A.M.- Armor/Hull Tech", 1),
             _m(3828, "Construction Blocks", 3), _m(11542, "Fernite Carbide Composite Armor Plate", 1),
             _m(11540, "Quantum Microprocessor", 1), _m(2046, "Damage Control I", 1)],
             "skill_entries": [], "time_seconds": 2340, "max_production_limit": 10,
             "products": [{"type_id": 2048, "type_name": "Damage Control II", "quantity": 1,
                           "meta_group_name": "Tech II"}]}},
    ]


_DC2_FAKE_PRICES = {
    34: 4.0, 35: 10.0, 36: 80.0, 2046: 30_000.0, 20416: 50_000.0, 20415: 50_000.0,
    11553: 40_000.0, 11399: 10_000.0, 3689: 9_000.0, 11695: 30_000.0, 11475: 2_000.0, 3828: 9_000.0,
    11542: 35_000.0, 11540: 45_000.0, 2048: 1_000_000.0, 34203: 250_000.0,
}
_AUGMENTATION = DecryptorOption(
    type_id=34203, type_name="Augmentation Decryptor", probability_multiplier=0.6,
    me_modifier=-2, te_modifier=2, max_run_modifier=9,
)


def _dc2_row(monkeypatch, options):
    return _decryptor_overview(monkeypatch, options, prices=_DC2_FAKE_PRICES, blueprint_rows=_dc2_rows())[2048]


def test_dc2_with_augmentation_buys_30_attempts_for_10_batches_not_50(monkeypatch) -> None:
    # 19-run BPCs at p 0.204: 1 BPC = ceil(1 / 0.204) = 5 whole attempts.
    # 10 batches x 10 runs = 100 runs = ceil(100 / 19) = 6 BPCs = 30 attempts
    # (per-batch it was 10 x 5 = 50).
    row = _dc2_row(monkeypatch, [_AUGMENTATION])
    mj = row["manufacturing_job"]
    assert mj["invention_decryptor"]["type_id"] == 34203
    plan = mj["invention_bpc_plan"]
    assert (plan["batch_runs"], plan["runs_per_invented_blueprint"], plan["whole_attempts_per_blueprint"]) == (10, 19, 5)
    assert (plan["owned_blueprint_runs"], plan["batch_one_invented_blueprints"]) == (0, 1)

    row["max_batches_total"] = 10
    for type_id in (20416, 20415):
        line = _shopping_line(row, type_id)
        assert line is not None and (line["need"], line["buy"]) == (60, 60)  # 2 x 30 attempts
    line = _shopping_line(row, 34203)
    assert line is not None and (line["need"], line["buy"]) == (30, 30)


def test_dc2_without_a_decryptor_buys_what_it_did_before(monkeypatch) -> None:
    # 10-run BPCs at p 0.34: 1 BPC per batch, 3 attempts: 10 x 3 = 30 attempts.
    row = _dc2_row(monkeypatch, [])
    row["max_batches_total"] = 10
    for type_id in (20416, 20415):
        line = _shopping_line(row, type_id)
        assert line is not None and (line["need"], line["buy"]) == (60, 60)
    assert row["manufacturing_job"]["invention_procurement_materials_per_extra_batch"]["20416"]["quantity"] == 6


def test_one_batch_buys_only_batch_ones_attempts(monkeypatch) -> None:
    row = _dc2_row(monkeypatch, [_AUGMENTATION])
    row["max_batches_total"] = 1
    line = _shopping_line(row, 34203)
    assert line is not None and (line["need"], line["buy"]) == (5, 5)


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
        owned_assets=([_owned_t2_bpc(runs=4)], [], {}, {}, {}),
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


# --- round 5 Task 5: batches 2..N from their own plans (C1) --------------------
# A later batch has no owned BPC run batch 1 spent, so it is planned again:
# the row's own job at the next blueprint source (invented ME/TE with the
# decryptor), every sub-build afresh, and one ME rounding per job. Invented
# BPC runs are shared across batches, so a batch can be split over two BPCs.


def _later_plan(mj: dict, batch: int) -> dict:
    """The extra_batch_schedule plan that batch `batch` (>= 2) uses."""
    schedule = mj["extra_batch_schedule"]
    k = batch - 2
    for plan_index, count in schedule["lead"]:
        if k < count:
            return schedule["plans"][plan_index]
        k -= count
    return schedule["plans"][schedule["cycle"][k % len(schedule["cycle"])]]


def _jobs(plan_or_mj: dict) -> list[tuple]:
    return [
        (s["runs"], s["blueprint_material_efficiency"], s["blueprint_time_efficiency"], s["source"])
        for s in plan_or_mj["job_segments"]
    ]


def _owned_t2_bpc_me(*, runs: int, me: int, item_id: int = 97) -> SimpleNamespace:
    bpc = _owned_t2_bpc(runs=runs, item_id=item_id)
    bpc.blueprint_material_efficiency = me
    return bpc


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
    assert all(r["manufacturing_job"]["later_batch_plan_version"] == 1 for r in rows)
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


# --- round 5 Task 8: fees per attempt, per copy run and per research level (D4, A2) ---


# Fake rates: facility tax 6% + SCC 4% = 10% on manufacturing, copy and
# invention job values; 6% + research SCC 2% = 8% on research job values.
_FEE_PROFILE = {
    "installation_cost_modifier": 0.1, "facility_tax": 0.06, "material_efficiency_bonus": 0.0,
    "time_efficiency_bonus": 0.0, "facility_cost_bonus": 0.0,
    "system_cost_indices": [
        {"activity": "manufacturing", "cost_index": 0.05},
        {"activity": "copying", "cost_index": 0.03},
        {"activity": "invention", "cost_index": 0.02},
        {"activity": "researching_material_efficiency", "cost_index": 0.02},
        {"activity": "researching_time_efficiency", "cost_index": 0.04},
    ],
    "structure_rigs": [],
}


def test_the_invention_publishes_its_fee_per_attempt_and_per_source_copy_run(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, profile=_FEE_PROFILE)[5002]["manufacturing_job"]
    invention = mj["activity_breakdown"]["invention"]
    # One attempt: job value 0.02 x the T2 blueprint's EIV 50 = 1.0, x (index
    # 0.02 + surcharge 0.10) = 0.12.
    assert invention["job_cost_per_attempt"] == pytest.approx(0.12)
    # One source-copy run: job value 0.02 x source EIV 3 x 5 = 0.3, x (0.03 + 0.10).
    assert invention["source_copy_job_cost_per_run"] == pytest.approx(0.039)
    # The expected-value fee is the per-attempt fee x the expected attempts.
    assert invention["total_job_cost"] == pytest.approx(0.12 * invention["runs"])


def _owned_bpo_manufacturing_job(*, level_one_seconds: int = 105, modifier_skills: list | None = None) -> dict:
    original_asset = SimpleNamespace(
        type_id=9001, item_id=42, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
    )
    blueprint_row = _blueprint_row()
    blueprint_row["research_material_job"] = {"time_seconds": level_one_seconds}
    blueprint_row["research_time_job"] = {"time_seconds": level_one_seconds}
    service = _build_service(
        blueprint_rows=[blueprint_row], profile=_FEE_PROFILE,
        character_modifiers={"modifier_skills": modifier_skills or [], "implants": []}, trained_skill_levels={},
        owned_assets=([], [original_asset], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )
    (row,) = service.industry_manufacturing_product_overview(
        build_from_bpc=False, have_blueprint_source_only=True, maximize_bp_runs=False, character_id=1,
    )
    mj = row["manufacturing_job"]
    assert mj["blueprint_source_kind"] == "owned_blueprint_original"
    return mj


def _owned_bpo_research_table(*, level_one_seconds: int = 105) -> dict:
    return _owned_bpo_manufacturing_job(level_one_seconds=level_one_seconds)["research_job_cost_by_level"]


def test_an_owned_bpo_row_publishes_its_cumulative_research_fee_per_level() -> None:
    table = _owned_bpo_research_table()
    # Per-run EIV 10 x 5 = 50. EVE's research job value: EIV x 0.02 x the
    # dimensionless level multiplier (rank-1 table seconds / 105); fee = job
    # value x (index + the 8 % research surcharge).
    me = table["research_material"]
    assert len(me) == 11 and me[0] == 0.0
    assert me[1] == pytest.approx(50 * 0.02 * (105 / 105) * (0.02 + 0.08))
    assert me[5] == pytest.approx(50 * 0.02 * (3360 / 105) * (0.02 + 0.08))
    assert me[10] == pytest.approx(50 * 0.02 * (256000 / 105) * (0.02 + 0.08))
    te = table["research_time"]
    assert te[5] == pytest.approx(50 * 0.02 * (3360 / 105) * (0.04 + 0.08))


def test_the_research_fee_uses_the_level_multiplier_not_the_rank_scaled_seconds() -> None:
    """Fix round 1: the old formula multiplied by the rank-scaled duration in
    SECONDS, so a rank-20 blueprint's ME 0 -> 10 fee was 20 x 105 = 2100x
    the real one. Real ESI jobs (same facility and activity, a week apart)
    cost the same per ISK of EIV at rank 20 and rank 40: the fee does not
    scale with rank, only the duration does. Fake EIV 50, fake index 0.02."""
    rank_one = _owned_bpo_research_table(level_one_seconds=105)
    rank_twenty = _owned_bpo_research_table(level_one_seconds=20 * 105)
    expected = 50 * 0.02 * (256000 / 105) * (0.02 + 0.08)
    assert rank_one["research_material"][10] == pytest.approx(expected)
    assert rank_twenty["research_material"][10] == pytest.approx(expected)


def test_an_owned_bpo_row_publishes_its_cumulative_research_duration_per_level() -> None:
    """Task 8c fix round 1: the research-time model the fee uses (rank x the
    rank-1 level table, then the skill / structure time reductions), per
    level, so the planner can show a research action's duration for its
    exact level span. Fake rank 20, no skills, no structure time bonus."""
    durations = _owned_bpo_manufacturing_job(level_one_seconds=20 * 105)["research_duration_seconds_by_level"]
    te = durations["research_time"]
    assert len(te) == 11 and te[0] == 0
    assert te[5] == 20 * 3360
    # TE 0 -> 20 is levels 0 -> 10.
    assert te[10] - te[0] == 20 * 256000
    assert durations["research_material"][10] == 20 * 256000


def test_the_unskilled_research_duration_leaves_the_research_skills_out() -> None:
    """Fix round 2: the planner applies the ASSIGNED pilot's skills to the
    unskilled table. The producer character here has Research 5 (-25% TE
    research time); the unskilled table leaves that out. Fake rank 20."""
    mj = _owned_bpo_manufacturing_job(level_one_seconds=20 * 105, modifier_skills=[
        {"type_name": "Research", "trained_skill_level": 5},
    ])
    skilled = mj["research_duration_seconds_by_level"]["research_time"]
    unskilled = mj["research_duration_seconds_by_level_unskilled"]["research_time"]
    assert unskilled[10] == 20 * 256000
    assert skilled[10] == 20 * 256000 * 3 // 4
    # ME research has no Research bonus: both tables agree.
    assert mj["research_duration_seconds_by_level"]["research_material"] == (
        mj["research_duration_seconds_by_level_unskilled"]["research_material"]
    )


def test_a_blueprint_without_research_time_publishes_an_unknown_duration() -> None:
    durations = _owned_bpo_manufacturing_job(level_one_seconds=0)["research_duration_seconds_by_level"]
    assert durations["research_time"] is None


def test_a_row_without_an_owned_bpo_publishes_no_research_fee_table(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, profile=_FEE_PROFILE)[5002]["manufacturing_job"]
    assert "research_job_cost_by_level" not in mj


def test_a_bpc_variant_of_an_owned_bpo_publishes_the_research_fee_table_too() -> None:
    """The planner schedules ME/TE research whenever the corp owns the BPO,
    also when the kept row builds from an owned BPC of it: that row carries
    the table as well (else the research fee would read unknown)."""
    original_asset = SimpleNamespace(
        type_id=9001, item_id=42, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
    )
    copy_asset = SimpleNamespace(
        type_id=9001, item_id=43, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    service = _build_service(
        blueprint_rows=[_blueprint_row()], profile=_FEE_PROFILE,
        character_modifiers={"modifier_skills": [], "implants": []}, trained_skill_levels={},
        owned_assets=([copy_asset], [original_asset], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )
    (row,) = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=True, maximize_bp_runs=False, character_id=1,
    )
    mj = row["manufacturing_job"]
    assert mj["blueprint_source_kind"] != "owned_blueprint_original"
    me = mj["research_job_cost_by_level"]["research_material"]
    assert me[5] == pytest.approx(50 * 0.02 * (3360 / 105) * (0.02 + 0.08))


def test_an_owned_t2_bpc_covering_batch_one_still_publishes_the_invention_fees(monkeypatch) -> None:
    """Batch 1 invents nothing, but the planner's invent / copy actions for
    the later batches still need the fee rates (D4)."""
    from eve_online_industry_tracker.application.industry import overview_row as orow

    row = _invention_overview(
        monkeypatch, profile=_FEE_PROFILE, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
    )[5002]
    plan = row["manufacturing_job"]["invention_bpc_plan"]
    assert plan["job_cost_per_attempt"] == pytest.approx(0.12)
    assert plan["source_copy_job_cost_per_run"] == pytest.approx(0.039)
    assert orow.get_invention_fee_per_attempt(row) == pytest.approx(0.12)
    assert orow.get_source_copy_fee_per_run(row) == pytest.approx(0.039)


# --- round 5 Task 8 fix round 1: a component's per-run job fee on its parent ---


def _parent_of_component_row() -> dict:
    """Blueprint 9100 builds 6001 from 2 x the component 5001 per run (fake)."""
    parent = _blueprint_row()
    parent["blueprint_type_id"] = 9100
    parent["blueprint"] = {"type_id": 9100, "type_name": "Parent Blueprint", "base_price": 1000.0}
    parent["manufacturing_job"] = {
        **parent["manufacturing_job"],
        "materials": [{"type_id": 5001, "type_name": "Test Module", "quantity": 2, "base_price": 100.0,
                       "group_name": "Shield Boosters", "category_name": "Module"}],
        "products": [{"type_id": 6001, "type_name": "Parent Thing", "quantity": 1, "base_price": 500.0,
                      "group_name": "Shield Boosters", "category_name": "Module", "meta_group_name": "Tech I"}],
    }
    return parent


def test_a_parent_publishes_its_components_job_fee_per_run() -> None:
    """The component 5001: per-run EIV 10 Tritanium x 5 = 50, manufacturing
    index 0.05, + the 10 % surcharge on the job value = 7.5 per run, the same figure as the
    component's own row (one formula, _plan_activity_costs)."""
    service = _build_service(
        blueprint_rows=[_blueprint_row(), _parent_of_component_row()], profile=_FEE_PROFILE,
        character_modifiers={"modifier_skills": [], "implants": []}, trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}, 6001: {"adjusted_price": 500.0}},
    )
    rows = {
        int(r["type_id"]): r for r in service.industry_manufacturing_product_overview(
            build_from_bpc=False, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
        )
    }
    entry = rows[6001]["manufacturing_job"]["materials"]["5001"]
    assert entry["component_job_fee_per_run"] == pytest.approx(50 * (0.05 + 0.10))
    own = rows[5001]["manufacturing_job"]
    assert entry["component_job_fee_per_run"] == pytest.approx(own["manufacturing_job_cost"] / own["runs"])
    # A raw material has no blueprint: no fee key.
    assert "component_job_fee_per_run" not in rows[5001]["manufacturing_job"]["materials"]["34"]


# --- Task 8b fix round 1: an unknown fee input is unknown with its reason ---


def test_a_profile_without_a_facility_tax_leaves_every_fee_unknown_with_the_reason(monkeypatch, caplog) -> None:
    import logging

    profile = {k: v for k, v in _FEE_PROFILE.items() if k not in ("facility_tax", "installation_cost_modifier")}
    with caplog.at_level(logging.WARNING):
        mj = _invention_overview(monkeypatch, profile=profile)[5002]["manufacturing_job"]
    assert mj["manufacturing_job_cost"] is None
    assert "facility tax" in mj["manufacturing_job_cost_unknown_reason"]
    invention = mj["activity_breakdown"]["invention"]
    assert invention["job_cost_per_attempt"] is None
    assert "facility tax" in invention["job_cost_per_attempt_unknown_reason"]
    assert invention["source_copy_job_cost_per_run"] is None
    assert "facility tax" in invention["source_copy_job_cost_per_run_unknown_reason"]
    assert any("facility tax" in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)


def test_a_missing_research_index_is_the_published_reason() -> None:
    profile = {**_FEE_PROFILE, "system_cost_indices": [
        e for e in _FEE_PROFILE["system_cost_indices"] if e["activity"] != "researching_material_efficiency"
    ]}
    original_asset = SimpleNamespace(
        type_id=9001, item_id=42, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
    )
    blueprint_row = _blueprint_row()
    blueprint_row["research_material_job"] = {"time_seconds": 105}
    blueprint_row["research_time_job"] = {"time_seconds": 105}
    service = _build_service(
        blueprint_rows=[blueprint_row], profile=profile,
        character_modifiers={"modifier_skills": [], "implants": []}, trained_skill_levels={},
        owned_assets=([], [original_asset], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )
    (row,) = service.industry_manufacturing_product_overview(
        build_from_bpc=False, have_blueprint_source_only=True, maximize_bp_runs=False, character_id=1,
    )
    mj = row["manufacturing_job"]
    assert mj["research_job_cost_by_level"]["research_material"] is None
    assert "research_material cost index" in mj["research_job_cost_by_level_unknown_reason"]["research_material"]
    assert mj["research_job_cost_by_level"]["research_time"] is not None
    assert "research_time" not in mj["research_job_cost_by_level_unknown_reason"]


def test_a_partly_priced_component_has_no_fee_and_says_why() -> None:
    """A material without an adjusted price used to give a partial EIV, so a
    low fee that looked exact."""
    blueprint_row = _blueprint_row()
    blueprint_row["manufacturing_job"]["materials"].append(
        {"type_id": 35, "type_name": "Pyerite", "quantity": 4, "group_name": "Mineral", "category_name": "Material"},
    )
    service = _build_service(
        blueprint_rows=[blueprint_row], profile=_FEE_PROFILE,
        character_modifiers={"modifier_skills": [], "implants": []}, trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        # No adjusted, average or SDE base price for 35.
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )
    (row,) = service.industry_manufacturing_product_overview(
        build_from_bpc=False, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    mj = row["manufacturing_job"]
    assert mj["manufacturing_job_cost"] is None
    assert "EIV" in mj["manufacturing_job_cost_unknown_reason"]


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
