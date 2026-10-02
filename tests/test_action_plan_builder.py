# tests/test_action_plan_builder.py
"""Regression tests for Defect 1 (task-18a remediation batch):

ActionPlanBuilder._build_deliver_actions read `character_name`,
`installer_name`, `product_type_name` and `type_name` off industry job rows
via `_job_attr` -- none of those four are real columns on either job ORM
model (CorporationIndustryJobsModel / CharacterIndustryJobsModel), so every
DELIVER row silently got character_name=None and type_name="". Fix: resolve
the pilot name from a character_id/installer_id -> name map (built the same
way CharacterAssigner builds it for slot accounting), and the item name from
TypeMetadataResolver.type_name(product_type_id), added for this fix.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.action_plan_builder import (
    ActionPlanBuilder,
)
from eve_online_industry_tracker.application.industry.type_metadata import TypeMetadataResolver


def _past(hours: int = 1) -> datetime:
    return (datetime.now(tz=timezone.utc) - timedelta(hours=hours)).replace(tzinfo=None)


class _RecordingLoader:
    """Mirrors tests/test_type_metadata.py's _FakeLoader: records every batch
    of type_ids it was asked to load, so a test can assert it was invoked
    exactly once (batched), not once per deliver row."""

    def __init__(self, data):
        self._data = data
        self.calls: list[list[int]] = []

    def __call__(self, session, language, type_ids):
        self.calls.append(sorted(type_ids))
        return {tid: self._data[tid] for tid in type_ids if tid in self._data}


def _job(**kwargs):
    """A minimal stand-in for a CorporationIndustryJobsModel/
    CharacterIndustryJobsModel row -- a plain object with only real columns
    plus whatever the test wants to set. No `character_name`,
    `installer_name`, `product_type_name` or `type_name` attribute exists,
    matching the real ORM models exactly (getattr(..., None) is what
    `_job_attr` falls back to for those)."""
    defaults = dict(
        status="ready",
        end_date=_past(),
        character_id=None,
        installer_id=None,
        product_type_id=None,
        type_id=None,
        runs=1,
        output_quantity=None,
        activity_id=1,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_deliver_action_resolves_pilot_name_via_installer_id_not_a_phantom_field():
    """installer_name/character_name are not real columns -- the pilot name
    must come from the character_name_map (installer_id -> name), the way
    CharacterAssigner already resolves it for slot accounting."""
    job = _job(installer_id=42, product_type_id=999)
    resolver = TypeMetadataResolver(
        sde_session_provider=lambda: None,
        loader=_RecordingLoader({999: {"type_id": 999, "type_name": "Warrior II"}}),
    )

    rows = ActionPlanBuilder().build(
        plan_id=1,
        assigned_actions=[],
        shopping_items=[],
        pricing_suggestions=[],
        industry_jobs=[job],
        admin_settings=SimpleNamespace(),
        character_name_map={42: "Test Pilot"},
        meta_resolver=resolver,
    )

    deliver_rows = [r for r in rows if r.action_type == "deliver"]
    assert len(deliver_rows) == 1
    assert deliver_rows[0].character_id == 42
    assert deliver_rows[0].character_name == "Test Pilot"


def test_deliver_action_resolves_item_name_via_meta_resolver_not_a_phantom_field():
    """product_type_name/type_name are not real columns -- the item name must
    come from TypeMetadataResolver.type_name(product_type_id), not be blank.
    A wrong implementation (still reading the phantom fields) produces
    type_name="" here -- indistinguishable from a rendering bug in Tab 1."""
    job = _job(installer_id=42, product_type_id=999)
    resolver = TypeMetadataResolver(
        sde_session_provider=lambda: None,
        loader=_RecordingLoader({999: {"type_id": 999, "type_name": "Warrior II"}}),
    )

    rows = ActionPlanBuilder().build(
        plan_id=1,
        assigned_actions=[],
        shopping_items=[],
        pricing_suggestions=[],
        industry_jobs=[job],
        admin_settings=SimpleNamespace(),
        character_name_map={42: "Test Pilot"},
        meta_resolver=resolver,
    )

    deliver_rows = [r for r in rows if r.action_type == "deliver"]
    assert deliver_rows[0].type_name == "Warrior II"
    assert deliver_rows[0].type_name != ""


def test_unresolvable_item_name_falls_back_to_an_identifiable_placeholder_not_blank():
    """When the SDE has nothing for this type_id, the fallback must stay
    identifiable (f"type_{type_id}"), never an empty string."""
    job = _job(installer_id=42, product_type_id=424242)
    resolver = TypeMetadataResolver(sde_session_provider=lambda: None, loader=_RecordingLoader({}))

    rows = ActionPlanBuilder().build(
        plan_id=1,
        assigned_actions=[],
        shopping_items=[],
        pricing_suggestions=[],
        industry_jobs=[job],
        admin_settings=SimpleNamespace(),
        character_name_map={},
        meta_resolver=resolver,
    )

    deliver_rows = [r for r in rows if r.action_type == "deliver"]
    assert deliver_rows[0].type_name == "type_424242"


def test_deliver_item_names_are_prefetched_in_one_batch_not_per_job():
    """Mirrors index_blueprint_assets' prewarm test (test_daily_planner_service_helpers.py):
    without an up-front batched prefetch, meta_resolver.type_name() self-heals
    a per-id cache miss inside the deliver loop, opening one SDE session per
    distinct product_type_id instead of one query total."""
    jobs = [
        _job(installer_id=1, product_type_id=100),
        _job(installer_id=1, product_type_id=200),
        _job(installer_id=1, product_type_id=100),
    ]
    loader = _RecordingLoader({
        100: {"type_id": 100, "type_name": "Tritanium"},
        200: {"type_id": 200, "type_name": "Pyerite"},
    })
    resolver = TypeMetadataResolver(sde_session_provider=lambda: None, loader=loader)

    ActionPlanBuilder().build(
        plan_id=1,
        assigned_actions=[],
        shopping_items=[],
        pricing_suggestions=[],
        industry_jobs=jobs,
        admin_settings=SimpleNamespace(),
        character_name_map={1: "Test Pilot"},
        meta_resolver=resolver,
    )

    assert loader.calls == [[100, 200]]


def test_no_character_name_map_or_resolver_still_uses_honest_fallbacks():
    """Backward-compat: when Phase 8 is called without the new kwargs (e.g. an
    older caller or a stub in another test), the deliver row must not raise
    and must still avoid an empty type_name."""
    job = _job(installer_id=42, product_type_id=999)

    rows = ActionPlanBuilder().build(
        plan_id=1,
        assigned_actions=[],
        shopping_items=[],
        pricing_suggestions=[],
        industry_jobs=[job],
        admin_settings=SimpleNamespace(),
    )

    deliver_rows = [r for r in rows if r.action_type == "deliver"]
    assert deliver_rows[0].character_name is None
    assert deliver_rows[0].type_name == "type_999"


# --- buy_bpo rows (Task 18, ruling R4) -----------------------------------------
# Before Task 16 every BPO opportunity was a zero-saving "hold", so emitting a
# buy_bpo row for every one of them, keyed by the *product* type_id, was
# invisible. With real savings it would tell the user to buy the product for
# every analysed item.

def _opp(recommendation, **overrides):
    opp = {
        "type_id": 12345,                 # the product
        "type_name": "Hobgoblin I",
        "bp_type_id": 999,                # the blueprint to buy
        "bp_type_name": "Hobgoblin I Blueprint",
        "bpo_market_price": 20_000_000.0,
        "break_even_days": 20.0,
        "projected_annual_savings": 365_000_000.0,
        "recommendation": recommendation,
        "as_invention_enabler": False,
    }
    opp.update(overrides)
    return opp


def _buy_bpo_rows(opportunities, meta_resolver=None):
    rows = ActionPlanBuilder().build(
        plan_id=1,
        assigned_actions=[],
        shopping_items=[],
        pricing_suggestions=[],
        industry_jobs=[],
        admin_settings=SimpleNamespace(),
        bpo_opportunities=opportunities,
        meta_resolver=meta_resolver,
    )
    return [r for r in rows if r.action_type == "buy_bpo"]


def test_a_hold_opportunity_is_not_a_buy_action():
    assert _buy_bpo_rows([_opp("hold")]) == []


def test_strong_buy_and_consider_become_buy_actions_for_the_blueprint():
    rows = _buy_bpo_rows([_opp("strong_buy"), _opp("consider", bp_type_id=888,
                                                   bp_type_name="Other Blueprint")])
    assert [(r.type_id, r.type_name) for r in rows] == [
        (999, "Hobgoblin I Blueprint"),
        (888, "Other Blueprint"),
    ]
    assert rows[0].estimated_cost_isk == 20_000_000.0
    assert "Hobgoblin I" in rows[0].notes  # says which product it is for
    assert "strong_buy" in rows[0].notes


def test_an_unnamed_blueprint_gets_an_identifiable_name_not_the_product_name():
    (row,) = _buy_bpo_rows([_opp("strong_buy", bp_type_name="")])
    assert row.type_id == 999
    assert row.type_name == "type_999"


def test_chain_planner_puts_the_blueprint_name_on_the_opportunity():
    from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner
    from eve_online_industry_tracker.application.daily_planner.models import ItemDecision

    class _Admin:
        def get(self, section, key):
            raise KeyError(key)

    row = {"type_id": 12345, "type_name": "Hobgoblin I", "quantity": 10,
           "manufacturing_job": {
               "runs": 10, "material_cost": 5000.0,
               "blueprint_material_efficiency": 0,
               "blueprint_source_kind": "owned_blueprint_copy",
               "blueprint_sde": {"blueprint_type_id": 999},
               "materials": {"34": {"type_id": 34, "quantity": 9000, "unit_price": 5.0}},
           }}
    decision = ItemDecision(
        type_id=12345, type_name="Hobgoblin I", decision="build", decision_reason="t",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=2.0, meta_group_id=1,
        pipeline_stage="manufacturing", overview_row=row,
    )
    phase1 = {
        "bpo_assets_by_type_id": {},
        "bpc_assets_by_type_id": {999: [object()]},
        "blueprint_data": {999: {
            "type_name": "Hobgoblin I Blueprint",
            "manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                              "materials": [{"type_id": 34, "quantity": 1000}]},
        }},
        "market_depth_cache": {999: {"spot_sell_price": 20_000.0}},
    }
    plan = ChainPlanner(None, None, _Admin()).plan_chain([decision], phase1)

    (opp,) = plan.bpo_opportunities
    assert opp["bp_type_id"] == 999
    assert opp["bp_type_name"] == "Hobgoblin I Blueprint"
