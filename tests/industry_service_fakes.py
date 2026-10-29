"""Shared fakes for the IndustryService producer tests (moved from test_industry_service_calculations.py)."""

from __future__ import annotations

from types import MethodType, SimpleNamespace
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA
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
    busy_blueprint_item_ids: set[int] | None = None,
) -> IndustryService:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace(esi_service=None)  # type: ignore[attr-defined]
    service._sessions = None  # type: ignore[attr-defined]
    # Blueprints in active industry jobs (none unless a test says so).
    service._active_job_blueprint_item_ids = MethodType(  # type: ignore[attr-defined]
        lambda self: set(busy_blueprint_item_ids or ()), service,
    )

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
    # Invention chance: one loaded, untrained pilot and identifiable invention
    # skills, like real SDE rows. The pilot cannot run the invention (Task 15
    # I3), so every chance is the SDE base chance and no number moves; the
    # skilled branches have their own tests, which override these.
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


def _find_first_activity_node(node: dict, activity: str) -> dict | None:
    if str(node.get("activity") or "") == activity:
        return node
    for child in node.get("children") or []:
        if isinstance(child, dict):
            match = _find_first_activity_node(child, activity)
            if isinstance(match, dict):
                return match
    return None

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

# Task 15 fix round 1 (minor 1): a sub-build whose job fees are unknown is
# bought, so the harness's default profile prices every fee (a test passes
# profile=None for "no profile"). Every index 1e-9, no facility tax: each fee
# is the SCC surcharge on the job value.
_FEES_KNOWN_PROFILE = {
    "facility_tax": 0.0, "installation_cost_modifier": 0.0,
    "material_efficiency_bonus": 0.0, "time_efficiency_bonus": 0.0, "facility_cost_bonus": 0.0,
    "system_cost_indices": [
        {"activity": activity, "cost_index": 1e-9}
        for activity in ("manufacturing", "copying", "invention", "research_material", "research_time", "reaction")
    ],
    "structure_rigs": [],
}


def _invention_service(
    monkeypatch, *, owned_assets=([], [], {}, {}, {}), owned_item_inventory=None,
    prices=None, profile=_FEES_KNOWN_PROFILE, blueprint_rows=None, real_confidence=False, adm_overrides=None,
    fee_context=None, busy_blueprint_item_ids=None,
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
        busy_blueprint_item_ids=busy_blueprint_item_ids,
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
            lambda self, category, key: adm_overrides.get(key, ADMIN_SETTINGS_SCHEMA[category]["settings"][key]["default"]),
            service,
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
_INVENTION_ONLY_PROFILE = {
    "installation_cost_modifier": 0.0, "facility_tax": 0.0,
    "material_efficiency_bonus": 0.0,
    "time_efficiency_bonus": 0.0,
    "facility_cost_bonus": 0.0,
    "system_cost_indices": [{"activity": "invention", "cost_index": 0.02}],
    "structure_rigs": [],
}


# _INVENTION_ONLY_PROFILE for a row with a sub-build: the sub-build's other
# fees must be priced too, or it is bought (Task 15 fix round 1, minor 1).
# The other indices are 1e-9, so each other fee is ~ the 4 % SCC on its value.
_NESTED_FEES_PROFILE = {
    **_INVENTION_ONLY_PROFILE,
    "system_cost_indices": [
        *_INVENTION_ONLY_PROFILE["system_cost_indices"],
        *({"activity": activity, "cost_index": 1e-9} for activity in ("manufacturing", "copying")),
    ],
}


def _rows_with(*, probability_pct: float, bpc_runs: int, t2_limit: int) -> list[dict]:
    rows = _invented_t2_blueprint_rows()
    product = rows[0]["invention_job"]["products"][0]
    product["probability_pct"] = probability_pct
    product["quantity"] = bpc_runs
    rows[1]["manufacturing_job"]["max_production_limit"] = t2_limit
    return rows


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


# --- fix round 1: whole-attempt invention buy list ------------------------------


def _shopping_line(row: dict, type_id: int) -> dict | None:
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

    return next((line for line in aggregate_shopping_list([row]) if line["type_id"] == type_id), None)


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


# --- round 3 final wave I1: datacores for the batches past owned T2 BPCs ------


def _owned_t2_bpc(*, runs: int, item_id: int = 81) -> SimpleNamespace:
    return SimpleNamespace(
        type_id=9002, item_id=item_id, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=runs,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )


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
_DECRYPTOR_PRICES = {**_INVENTION_FAKE_PRICES, 34201: 50.0, 34202: 1000.0, 34206: 1.0}


def _decryptor_service(monkeypatch, options, *, prices=None, **kwargs):
    # No profile unless a test sets one: the decryptor figures carry no fees.
    kwargs.setdefault("profile", None)
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


def _owned_t2_bpc_me(*, runs: int, me: int, item_id: int = 97) -> SimpleNamespace:
    bpc = _owned_t2_bpc(runs=runs, item_id=item_id)
    bpc.blueprint_material_efficiency = me
    return bpc


def _profile_service(app_session, **profile):
    from eve_online_industry_tracker.infrastructure.models import IndustryProfilesModel

    row = IndustryProfilesModel(character_id=90000001, profile_name="Fake profile", is_default=True, **profile)
    app_session.add(row)
    app_session.commit()
    sessions = SimpleNamespace(app_session=lambda: app_session, sde_session=lambda: None)
    svc = IndustryService(state=SimpleNamespace(esi_service=None, admin_settings=None, db_sde=None),
                          sessions=sessions)
    svc.solar_systems = lambda: []
    return svc, row.id
