# tests/test_chain_planner_keying.py
"""ChainPlanner keying: product type_ids and blueprint type_ids are different
id spaces, and every crossing between them has to go through an explicit index.

Before this was fixed the chain planner never executed any of its branches:
the entry point read a top-level `blueprint_type_id` that no producer writes,
sub-manufacture looked up product ids in a blueprint-keyed map, and the owned
BPO's ME/TE was read from attributes the asset model does not have.
"""
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.chain_planner import (
    ChainPlanner,
    build_product_to_blueprint_index,
)
from eve_online_industry_tracker.application.daily_planner.models import ItemDecision

BLUEPRINT_DATA = {
    999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                            "materials": [{"type_id": 34, "quantity": 100}]}},
    888: {"manufacturing": {"products": [{"type_id": 54321, "quantity": 10}],
                            "materials": []}},
}


class _AdminStub:
    def get(self, section, key, default=None):
        raise KeyError(key)  # forces ChainPlanner's own fallbacks


def _planner(*, optimal_me=10, optimal_te=20):
    planner = ChainPlanner(industry_service=None, session_provider=None, admin_settings=_AdminStub())
    planner._compute_optimal_me = lambda bp_type_id: optimal_me
    planner._compute_optimal_te = lambda bp_type_id, threshold: optimal_te
    return planner


def _row(*, type_id=12345, blueprint_type_id=999, **job):
    """An overview row shaped like the producer's: blueprint_type_id is nested."""
    manufacturing_job = {"runs": 1, "blueprint_sde": {"blueprint_type_id": blueprint_type_id}}
    manufacturing_job.update(job)
    return {"type_id": type_id, "type_name": "Thing", "quantity": 1,
            "manufacturing_job": manufacturing_job}


def _decision(row, *, meta_group_id=1, **overrides):
    base = dict(
        type_id=int(row["type_id"]), type_name="Thing", decision="build",
        decision_reason="test", adjusted_score=1.0, absolute_profit_per_batch=1.0,
        isk_per_hour=1.0, margin_pct=1.0, days_of_supply_current=1.0,
        effective_velocity=1.0, meta_group_id=meta_group_id,
        pipeline_stage="manufacturing", overview_row=row,
    )
    base.update(overrides)
    return ItemDecision(**base)


def _bpo(type_id, *, me, te):
    """Shaped like CorporationAssetsModel: the columns are blueprint_*-prefixed."""
    return SimpleNamespace(type_id=type_id, is_blueprint_copy=False,
                           blueprint_material_efficiency=me, blueprint_time_efficiency=te)


# --- the index -----------------------------------------------------------------

def test_index_maps_product_type_id_to_blueprint_type_id():
    index = build_product_to_blueprint_index(BLUEPRINT_DATA)
    assert index == {12345: 999, 54321: 888}


def test_blueprints_without_products_are_skipped():
    index = build_product_to_blueprint_index({777: {"manufacturing": {"products": []}}})
    assert index == {}


def test_malformed_entries_do_not_raise():
    index = build_product_to_blueprint_index({
        1: {}, 2: {"manufacturing": None}, 3: {"manufacturing": {"products": "nope"}},
    })
    assert index == {}


def test_blueprint_for_product_requires_an_owned_bpo():
    # Corp owns blueprint 888, which produces material 54321.
    bpo_assets_by_blueprint_type_id = {888: [object()]}
    planner = _planner()
    index = build_product_to_blueprint_index(BLUEPRINT_DATA)

    assert planner._blueprint_for_product(54321, index, bpo_assets_by_blueprint_type_id) == 888
    assert planner._blueprint_for_product(12345, index, bpo_assets_by_blueprint_type_id) is None


# --- the entry point -----------------------------------------------------------

def test_entry_point_reads_the_nested_blueprint_type_id():
    """Owned BPO at ME5/TE10 with optimal ME10/TE20 -> both research jobs flagged.

    This only happens if plan_chain resolves bp_type_id=999 from
    manufacturing_job.blueprint_sde, because the BPO index is keyed by 999.
    """
    row = _row()
    phase1 = {"bpo_assets_by_type_id": {999: [_bpo(999, me=5, te=10)]},
              "blueprint_data": BLUEPRINT_DATA}

    _planner().plan_chain([_decision(row)], phase1)

    assert row["needs_me_research"] is True
    assert row["me_current"] == 5
    assert row["me_research_target"] == 10
    assert row["needs_te_research"] is True
    assert row["te_current"] == 10
    assert row["te_research_target"] == 20


def test_a_fully_researched_bpo_schedules_no_research():
    """The asset model's ME/TE columns are blueprint_material_efficiency /
    blueprint_time_efficiency; reading `material_efficiency` gave 0 for every
    BPO and would have scheduled research on already-researched blueprints."""
    row = _row()
    phase1 = {"bpo_assets_by_type_id": {999: [_bpo(999, me=10, te=20)]},
              "blueprint_data": BLUEPRINT_DATA}

    _planner().plan_chain([_decision(row)], phase1)

    assert "needs_me_research" not in row
    assert "needs_te_research" not in row


def test_an_owned_bpo_with_unknown_me_schedules_no_research():
    """No ME/TE on the asset is unknown, not zero: don't invent a research job."""
    row = _row()
    phase1 = {"bpo_assets_by_type_id": {999: [_bpo(999, me=None, te=None)]},
              "blueprint_data": BLUEPRINT_DATA}

    _planner().plan_chain([_decision(row)], phase1)

    assert "needs_me_research" not in row
    assert "needs_te_research" not in row


# --- sub-manufacture -----------------------------------------------------------

SUB_BLUEPRINT_DATA = {
    # Top-level blueprint: 1 run of 12345 needs 100 x material 54321.
    999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                            "materials": [{"type_id": 54321, "quantity": 100,
                                           "type_name": "Widget"}]}},
    # Owned blueprint 888: 1 run makes 10 x 54321 from 2 x Tritanium (34).
    888: {"manufacturing": {"products": [{"type_id": 54321, "quantity": 10}],
                            "materials": [{"type_id": 34, "quantity": 2}]}},
}


def test_sub_manufacture_builds_a_material_whose_blueprint_is_owned():
    row = _row()
    phase1 = {
        "bpo_assets_by_type_id": {888: [_bpo(888, me=10, te=20)]},
        "blueprint_data": SUB_BLUEPRINT_DATA,
        "market_depth_cache": {34: {"vwap_5d": 1.0}, 54321: {"vwap_5d": 1.0}},
    }

    plan = _planner().plan_chain([_decision(row)], phase1)

    subs = [d for d in plan.decisions if d.is_sub_component]
    assert len(subs) == 1
    sub = subs[0]
    assert sub.type_id == 54321
    assert sub.type_name == "Widget"
    # 10 runs x 2 Tritanium x 1 ISK = 20 ISK, versus 100 x 1 ISK on the market.
    assert sub.overview_row["sub_manufacture_cost"] == 20.0
    assert sub.overview_row["market_buy_cost"] == 100.0
    # Downstream phases need the blueprint, not just the product.
    assert sub.overview_row["blueprint_type_id"] == 888


def test_sub_manufacture_buys_a_material_whose_blueprint_is_not_owned():
    row = _row()
    phase1 = {
        "bpo_assets_by_type_id": {},
        "blueprint_data": SUB_BLUEPRINT_DATA,
        "market_depth_cache": {34: {"vwap_5d": 1.0}, 54321: {"vwap_5d": 1.0}},
    }

    plan = _planner().plan_chain([_decision(row)], phase1)

    assert [d for d in plan.decisions if d.is_sub_component] == []


# --- service: loading blueprint data -------------------------------------------

def test_service_loads_blueprint_data_for_nested_ids_and_owned_bpos(monkeypatch):
    """The service read the same phantom top-level key, so blueprint_data was
    always {}. It also has to load the corp's owned BPOs: sub-manufacture
    only ever builds from an owned BPO, and those are rarely overview rows."""
    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService
    from eve_online_industry_tracker.infrastructure.sde import blueprints as sde_blueprints

    seen: list[list[int]] = []

    def fake_loader(session, language, blueprint_type_ids):
        seen.append(sorted(blueprint_type_ids))
        return {}

    monkeypatch.setattr(sde_blueprints, "get_blueprint_manufacturing_data", fake_loader)
    session = SimpleNamespace(close=lambda: None)
    fake_self = SimpleNamespace(_session_provider=SimpleNamespace(sde_session=lambda: session))

    DailyPlannerService._get_blueprint_data(
        fake_self, [_row(blueprint_type_id=999), {"type_id": 1}], extra_blueprint_type_ids={888},
    )

    assert seen == [[888, 999]]


# --- invention: which T1 BPO feeds a T2 blueprint ------------------------------

from eve_online_industry_tracker.application.daily_planner.chain_planner import (  # noqa: E402
    build_invention_source_index,
)

DATACORE = {"type_id": 20410, "quantity": 2}
INVENTION_BLUEPRINT_DATA = {
    # T1 blueprint 999 invents into T2 blueprint 1999.
    999: {"manufacturing": {"products": [{"type_id": 12344, "quantity": 1}], "materials": []},
          "invention": {"products": [{"type_id": 1999, "quantity": 1}],
                        "materials": [DATACORE]}},
    # T2 blueprint 1999 makes the T2 product 12345; it has no invention activity.
    1999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}], "materials": []},
           "invention": {"products": [], "materials": []}},
}


def test_invention_source_index_maps_invented_blueprint_to_its_t1_source():
    assert build_invention_source_index(INVENTION_BLUEPRINT_DATA) == {1999: 999}


def test_invention_source_index_tolerates_malformed_entries():
    assert build_invention_source_index({1: {}, 2: {"invention": None},
                                         3: {"invention": {"products": "x"}}}) == {}


def _t2_row():
    return _row(type_id=12345, blueprint_type_id=1999)


def test_owned_t1_bpo_marks_the_t2_item_for_copy_and_invention():
    row = _t2_row()
    decision = _decision(row, meta_group_id=2)
    phase1 = {"bpo_assets_by_type_id": {999: [_bpo(999, me=10, te=20)]},
              "blueprint_data": INVENTION_BLUEPRINT_DATA}

    _planner().plan_chain([decision], phase1)

    assert row["has_t1_bpo"] is True
    assert row["needs_invention"] is True
    assert row["t1_blueprint_type_id"] == 999
    # Datacores come from the T1 blueprint's invention activity.
    assert row["invention_materials"] == [DATACORE]
    assert "needs_t1_bpo" not in row
    assert decision.pipeline_stage == "copying"


def test_unowned_t1_bpo_means_one_must_be_acquired():
    row = _t2_row()
    decision = _decision(row, meta_group_id=2)
    phase1 = {"bpo_assets_by_type_id": {}, "blueprint_data": INVENTION_BLUEPRINT_DATA}

    _planner().plan_chain([decision], phase1)

    assert row.get("has_t1_bpo") is not True
    assert row["needs_t1_bpo"] is True
    assert row["t1_blueprint_type_id"] == 999
    assert decision.pipeline_stage == "invention"


def test_owned_t1_bpo_and_assigner_produce_a_copy_of_the_t1_blueprint():
    """End to end across the phase-5/phase-6 handoff."""
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        CharacterAssigner,
    )

    row = _t2_row()
    phase1 = {"bpo_assets_by_type_id": {999: [_bpo(999, me=10, te=20)]},
              "blueprint_data": INVENTION_BLUEPRINT_DATA}
    plan = _planner().plan_chain([_decision(row, meta_group_id=2)], phase1)

    chars = SimpleNamespace(list_characters=lambda: [{
        "character_id": 1, "character_name": "Pilot",
        "skills": {"skills": [{"skill_name": "Laboratory Operation", "trained_skill_level": 5}]},
    }])
    actions = CharacterAssigner().assign(plan, [], chars, _AdminStub())

    copies = [a for a in actions if a.action_type == "copy"]
    assert [a.type_id for a in copies] == [999]
