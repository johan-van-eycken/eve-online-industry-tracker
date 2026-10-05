# tests/test_chain_planner_keying.py
"""ChainPlanner keying: product type_ids and blueprint type_ids are different
id spaces, and every crossing between them has to go through an explicit index.

Before this was fixed the chain planner never executed any of its branches:
the entry point read a top-level `blueprint_type_id` that no producer writes,
sub-manufacture looked up product ids in a blueprint-keyed map, and the owned
BPO's ME/TE was read from attributes the asset model does not have.
"""
from __future__ import annotations

import logging
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
    planner._compute_optimal_me = lambda bp_type_id, runs=1: optimal_me
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


def _batch_materials(**qty_by_type_id):
    """manufacturing_job.materials as the producer writes it: keyed by
    str(type_id), quantity already scaled by runs and ME/structure."""
    return {
        str(t): {"type_id": int(t), "quantity": q, "type_name": "Widget"}
        for t, q in qty_by_type_id.items()
    }


def test_sub_manufacture_builds_a_material_whose_blueprint_is_owned():
    row = _row(materials=_batch_materials(**{"54321": 100}))
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
    # 10 runs x 2 Tritanium at ME10 = ceil(18) = 18 x 1 ISK, versus 100 x 1 ISK on the market.
    assert sub.overview_row["sub_manufacture_cost"] == 18.0
    assert sub.overview_row["market_buy_cost"] == 100.0
    # Downstream phases need the blueprint, not just the product.
    assert sub.overview_row["blueprint_type_id"] == 888


def test_sub_manufacture_buys_a_material_whose_blueprint_is_not_owned():
    row = _row(materials=_batch_materials(**{"54321": 100}))
    phase1 = {
        "bpo_assets_by_type_id": {},
        "blueprint_data": SUB_BLUEPRINT_DATA,
        "market_depth_cache": {34: {"vwap_5d": 1.0}, 54321: {"vwap_5d": 1.0}},
    }

    plan = _planner().plan_chain([_decision(row)], phase1)

    assert [d for d in plan.decisions if d.is_sub_component] == []


_SUB_PHASE1 = {
    "bpo_assets_by_type_id": {888: [_bpo(888, me=10, te=20)]},
    "blueprint_data": SUB_BLUEPRINT_DATA,
    "market_depth_cache": {34: {"vwap_5d": 1.0}, 54321: {"vwap_5d": 1.0}},
}


def test_sub_manufacture_quantity_is_the_parents_batch_quantity_not_one_sde_run():
    """F4: the SDE says 100 per run. A 20-run parent at ME10 needs 1800, which
    is what the producer wrote under manufacturing_job.materials."""
    row = _row(runs=20, materials=_batch_materials(**{"54321": 1800}))

    plan = _planner().plan_chain([_decision(row)], _SUB_PHASE1)

    (sub,) = [d for d in plan.decisions if d.is_sub_component]
    assert sub.overview_row["quantity_needed"] == 1800
    # 180 runs x 2 Tritanium at ME10 = 324 x 1 ISK, versus 1800 x 1 ISK on the market.
    assert sub.overview_row["sub_manufacture_cost"] == 324.0
    assert sub.overview_row["market_buy_cost"] == 1800.0


def test_sub_manufacture_requests_for_one_material_are_merged_across_parents():
    a = _row(type_id=12345, materials=_batch_materials(**{"54321": 300}))
    b = _row(type_id=22222, materials=_batch_materials(**{"54321": 200}))

    plan = _planner().plan_chain([_decision(a), _decision(b)], _SUB_PHASE1)

    subs = [d for d in plan.decisions if d.is_sub_component]
    assert len(subs) == 1
    assert subs[0].overview_row["quantity_needed"] == 500
    assert subs[0].overview_row["requested_by_type_ids"] == [12345, 22222]


def test_no_producer_batch_materials_means_no_sub_manufacture(caplog):
    """Without the producer's scaled quantities there is no correct amount to
    build; one SDE run is not a stand-in for it."""
    row = _row()  # no manufacturing_job.materials

    with caplog.at_level("WARNING"):
        plan = _planner().plan_chain([_decision(row)], _SUB_PHASE1)

    assert [d for d in plan.decisions if d.is_sub_component] == []
    assert any("batch materials" in r.getMessage() for r in caplog.records)


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
    monkeypatch.setattr(sde_blueprints, "get_invention_source_blueprint_ids", lambda session, ids: {})
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


# --- BPO investment: the exact, ceil-rounded ME saving --------------------------
# The old analysis read three phantom keys, so it always computed a 0 saving and
# recommended "hold". It also used a flat 10% of material cost, which overstates
# the saving whenever ceil() rounding eats the ME reduction.

from eve_online_industry_tracker.infrastructure.sde.blueprints import (  # noqa: E402
    me_adjusted_batch_quantity,
    me_adjusted_quantity,
    optimal_me_for_quantities,
)


def test_me_adjusted_quantity_rounds_up_per_material():
    assert me_adjusted_quantity(1000, 0) == 1000
    assert me_adjusted_quantity(1000, 10) == 900
    assert me_adjusted_quantity(5, 10) == 5      # ceil(4.5)
    assert me_adjusted_quantity(1, 10) == 1


def test_optimal_me_for_quantities():
    assert optimal_me_for_quantities([1000]) == 10
    assert optimal_me_for_quantities([5]) == 0
    assert optimal_me_for_quantities([5, 30]) == 10   # ceil(27) at ME10 < ceil(27.3) at ME9
    assert optimal_me_for_quantities([]) == 0


def _bpo_row(*, me_current, unit_price=5.0, material_cost=5000.0, quantity=10, runs=10,
             source_kind="owned_blueprint_copy"):
    """10 runs, 1 unit per run; one material, type 34, priced at unit_price."""
    row = _row(runs=runs, material_cost=material_cost,
               blueprint_material_efficiency=me_current, blueprint_source_kind=source_kind,
               materials={"34": {"type_id": 34, "quantity": 9000, "unit_price": unit_price}})
    row["quantity"] = quantity
    return row


def _bpo_bp_data(base_qty):
    return {999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                                    "materials": [{"type_id": 34, "quantity": base_qty}]}}}


def _analyse(row, bp_data, *, velocity=2.0, bpo_price=20_000.0, bpc_owned=True):
    decision = _decision(row, effective_velocity=velocity)
    phase1 = {
        "bpo_assets_by_type_id": {},
        "bpc_assets_by_type_id": {999: [object()]} if bpc_owned else {},
        "blueprint_data": bp_data,
        "market_depth_cache": {999: {"spot_sell_price": bpo_price}},
    }
    plan = _planner().plan_chain([decision], phase1)
    return decision, plan.bpo_opportunities


def test_a_large_quantity_blueprint_gets_the_exact_isk_saving():
    """1000 base units: ME0 1000 vs ME10 900 -> 100 units x 5 ISK = 500 ISK/run.
    1 unit per run at 2 units/day -> 2 runs/day -> 1000 ISK/day.
    20,000 ISK BPO -> 20-day break-even -> strong_buy (<= 30d)."""
    decision, opps = _analyse(_bpo_row(me_current=0), _bpo_bp_data(1000))

    assert decision.bpo_analysis_skip_reason is None
    assert decision.break_even_days == 20.0
    assert decision.projected_annual_savings == 365_000.0
    assert decision.bpo_investment_recommended is True
    assert decision.bpo_market_price == 20_000.0
    assert len(opps) == 1
    assert opps[0]["recommendation"] == "strong_buy"
    assert opps[0]["bp_type_id"] == 999


def test_runs_per_day_uses_units_per_run_not_runs_per_batch():
    """Same item, 10 units per run (quantity 100 over 10 runs): 2 units/day is
    0.2 runs/day -> 100 ISK/day -> 200-day break-even -> hold."""
    decision, opps = _analyse(_bpo_row(me_current=0, quantity=100), _bpo_bp_data(1000))

    assert decision.break_even_days == 200.0
    assert decision.projected_annual_savings == 36_500.0
    assert decision.bpo_investment_recommended is False
    assert opps[0]["recommendation"] == "hold"


def test_a_single_run_of_a_small_material_saves_nothing_from_me_research():
    """1 run of 5 base units: ceil(5 x 0.9) = 5, so no saving."""
    decision, opps = _analyse(_bpo_row(me_current=0, runs=1, quantity=1), _bpo_bp_data(5))
    assert decision.projected_annual_savings == 0.0
    assert decision.break_even_days is None
    assert decision.bpo_investment_recommended is False
    assert opps[0]["recommendation"] == "hold"


def test_an_already_researched_blueprint_saves_nothing():
    decision, opps = _analyse(_bpo_row(me_current=10), _bpo_bp_data(1000))

    assert decision.projected_annual_savings == 0.0
    assert decision.bpo_investment_recommended is False
    assert opps[0]["recommendation"] == "hold"


def _assert_skipped(decision, opps, fragment):
    assert opps == []
    assert decision.bpo_investment_recommended is None
    assert decision.projected_annual_savings is None
    assert decision.bpo_analysis_skip_reason is not None
    assert fragment in decision.bpo_analysis_skip_reason


def test_no_priced_materials_skips_the_analysis_with_a_reason():
    decision, opps = _analyse(_bpo_row(me_current=0, material_cost=None), _bpo_bp_data(1000))
    _assert_skipped(decision, opps, "material cost")


def test_an_unpriced_material_skips_the_analysis_with_a_reason():
    decision, opps = _analyse(_bpo_row(me_current=0, unit_price=None), _bpo_bp_data(1000))
    _assert_skipped(decision, opps, "unit price")


def test_missing_base_quantities_skip_the_analysis_with_a_reason():
    bp_data = {999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                                       "materials": []}}}
    decision, opps = _analyse(_bpo_row(me_current=0), bp_data)
    _assert_skipped(decision, opps, "base material")


def test_unknown_blueprint_me_skips_the_analysis_with_a_reason():
    decision, opps = _analyse(_bpo_row(me_current=None), _bpo_bp_data(1000))
    _assert_skipped(decision, opps, "unknown current blueprint ME")


def test_a_missing_batch_run_count_skips_the_analysis_with_a_reason():
    row = _bpo_row(me_current=0)
    del row["manufacturing_job"]["runs"]
    decision, opps = _analyse(row, _bpo_bp_data(1000))
    _assert_skipped(decision, opps, "unknown batch run count (manufacturing_job.runs)")


def test_a_non_positive_batch_run_count_skips_the_analysis_with_a_reason():
    decision, opps = _analyse(_bpo_row(me_current=0, runs=0), _bpo_bp_data(1000))
    _assert_skipped(decision, opps, "non-positive batch run count (0)")


def test_no_batch_units_skips_the_analysis_with_a_reason():
    decision, opps = _analyse(_bpo_row(me_current=0, quantity=0), _bpo_bp_data(1000))
    _assert_skipped(decision, opps, "no batch units (quantity=0)")


def test_an_owned_bpo_without_a_batch_run_count_schedules_no_me_research(caplog):
    """No manufacturing_job.runs: the optimum is not computed at a run count
    guessed from quantity; ME research is not scheduled and a WARNING says why."""
    asked = []
    planner = _planner()
    planner._compute_optimal_me = lambda bp_type_id, runs=1: asked.append(runs) or 10
    row = _row()
    row["quantity"] = 50
    del row["manufacturing_job"]["runs"]
    phase1 = {"bpo_assets_by_type_id": {999: [_bpo(999, me=0, te=0)]}, "blueprint_data": BLUEPRINT_DATA}

    with caplog.at_level("WARNING"):
        planner.plan_chain([_decision(row)], phase1)

    assert asked == []
    assert "needs_me_research" not in row
    assert row["needs_te_research"] is True   # TE does not depend on the run count
    assert any("run count" in r.getMessage() and r.levelno == logging.WARNING
               for r in caplog.records)


def test_an_assumed_sde_fallback_me_skips_the_analysis_with_a_reason():
    """With no owned blueprint the producer assumes max ME; that is not a
    measured level to compute a saving from."""
    row = _bpo_row(me_current=10, source_kind="blueprint_sde_fallback")
    decision, opps = _analyse(row, _bpo_bp_data(1000), bpc_owned=False)
    _assert_skipped(decision, opps, "blueprint_sde_fallback")


def test_no_bpo_market_price_skips_the_analysis_with_a_reason():
    row = _bpo_row(me_current=0)
    decision = _decision(row)
    phase1 = {"bpc_assets_by_type_id": {999: [object()]},
              "blueprint_data": _bpo_bp_data(1000), "market_depth_cache": {}}
    plan = _planner().plan_chain([decision], phase1)
    _assert_skipped(decision, plan.bpo_opportunities, "market price")


def test_an_invention_enabler_is_not_valued_as_an_me_saving():
    """ME on a T1 BPO does not change T2 material quantities; that value is
    not modelled, so it is skipped rather than reported as a 'hold'."""
    row = _t2_row()
    decision = _decision(row, meta_group_id=2)
    phase1 = {"bpo_assets_by_type_id": {}, "blueprint_data": INVENTION_BLUEPRINT_DATA,
              "market_depth_cache": {999: {"spot_sell_price": 1.0},
                                     1999: {"spot_sell_price": 1.0}}}
    plan = _planner().plan_chain([decision], phase1)
    _assert_skipped(decision, plan.bpo_opportunities, "invention")


def test_the_batch_saving_counts_both_materials_at_the_batch_ceiling():
    """10 runs, ME0 -> ME10 (optimal at 10 runs):
      * 5 x 10 = 50 units x 1000 ISK: 50 -> 45, saves 5 x 1000 = 5000 ISK
      * 1000 x 10 = 10000 units x 5 ISK: 10000 -> 9000, saves 1000 x 5 = 5000 ISK
    10,000 ISK per 10-unit batch. At 2 units/day that is 0.2 batches/day,
    2000 ISK/day, so a 20,000 ISK BPO breaks even in exactly 10 days."""
    row = _row(runs=10, material_cost=100_000.0,
               blueprint_material_efficiency=0, blueprint_source_kind="owned_blueprint_copy",
               materials={"34": {"type_id": 34, "quantity": 50, "unit_price": 1000.0},
                          "35": {"type_id": 35, "quantity": 9000, "unit_price": 5.0}})
    row["quantity"] = 10
    bp_data = {999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                                       "materials": [{"type_id": 34, "quantity": 5},
                                                     {"type_id": 35, "quantity": 1000}]}}}

    assert _planner()._me_saving_isk_per_batch(row=row, bp_data=bp_data[999]) == (10_000.0, None)

    decision, opps = _analyse(row, bp_data)
    assert decision.bpo_analysis_skip_reason is None
    assert decision.break_even_days == 10.0
    assert decision.projected_annual_savings == 730_000.0
    assert opps[0]["recommendation"] == "strong_buy"


def test_optimal_me_research_target_is_computed_at_the_batch_run_count():
    """The owned-BPO branch must ask for the optimum at the row's run count."""
    seen = {}
    planner = _planner()
    planner._compute_optimal_me = lambda bp_type_id, runs=1: seen.setdefault("runs", runs) and 10
    row = _row(runs=20)
    phase1 = {"bpo_assets_by_type_id": {999: [_bpo(999, me=0, te=0)]}, "blueprint_data": BLUEPRINT_DATA}
    planner.plan_chain([_decision(row)], phase1)
    assert seen["runs"] == 20


def test_the_ceiling_is_taken_over_the_batch_not_per_run():
    """5 units/run x 100 runs at ME10: per run ceil(4.5) = 5 saves nothing,
    the batch ceil(450) = 450 saves 50 units."""
    assert 100 * (me_adjusted_quantity(5, 0) - me_adjusted_quantity(5, 10)) == 0
    assert me_adjusted_batch_quantity(5, 100, 0) - me_adjusted_batch_quantity(5, 100, 10) == 50
    assert me_adjusted_batch_quantity(5, 1, 10) == 5
    assert me_adjusted_batch_quantity(5, 1000, 10) == 4500
    assert me_adjusted_batch_quantity(1, 1000, 10) == 1000   # never below one unit per run
    assert me_adjusted_batch_quantity(0, 10, 10) == 0
    assert me_adjusted_batch_quantity(5, 0, 10) == 0


def test_the_batch_quantity_matches_the_producers_rounding_exactly():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    for base in (1, 2, 5, 7, 10, 33, 100, 1000, 2500):
        for runs in (1, 2, 3, 10, 20, 100, 250, 1000):
            for me in range(11):
                reduction = IndustryService._combine_reductions([me / 100.0])
                expected = IndustryService._round_material_quantity(
                    float(base * runs) * max(0.0, 1.0 - reduction), minimum_quantity=runs,
                )
                assert me_adjusted_batch_quantity(base, runs, me) == expected, (base, runs, me)


def test_optimal_me_depends_on_the_run_count():
    assert optimal_me_for_quantities([5]) == 0
    assert optimal_me_for_quantities([5], runs=10) == 10
    assert optimal_me_for_quantities([1], runs=100) == 0   # the one-unit-per-run floor


def test_a_100_run_batch_of_a_small_material_has_a_real_saving():
    """Base 5 x 100 runs, ME0 -> ME10: 500 -> 450 units, 50 x 5 ISK = 250 ISK
    per batch. 2 units/day over 100-unit batches is 0.02 batches/day, so
    5 ISK/day and a 4000-day break-even ('hold'), not 'saves nothing'."""
    row = _bpo_row(me_current=0, runs=100, quantity=100)
    decision, opps = _analyse(row, _bpo_bp_data(5))
    assert decision.break_even_days == 4000.0
    assert decision.projected_annual_savings == 1825.0
    assert opps[0]["recommendation"] == "hold"


def test_an_unknown_sell_velocity_skips_the_analysis_with_a_reason():
    row = _bpo_row(me_current=0)
    decision = _decision(
        row, effective_velocity=0.01,
        velocity_unknown_reason="no corp sales in 30 days and no days-of-supply estimate",
    )
    phase1 = {"bpc_assets_by_type_id": {999: [object()]}, "blueprint_data": _bpo_bp_data(1000),
              "market_depth_cache": {999: {"spot_sell_price": 20_000.0}}}
    plan = _planner().plan_chain([decision], phase1)
    _assert_skipped(decision, plan.bpo_opportunities, "sell velocity unknown")


def test_a_zero_sell_velocity_skips_the_analysis_with_a_reason():
    decision, opps = _analyse(_bpo_row(me_current=0), _bpo_bp_data(1000), velocity=0.0)
    _assert_skipped(decision, opps, "zero sell velocity")


def test_a_slow_measured_velocity_is_used_as_is_not_floored():
    """5000 ISK saved per 10-unit batch at 0.02 units/day: 10 ISK/day, so a
    20,000 ISK BPO breaks even in 2000 days. The old 0.033 floor said ~1212."""
    decision, _ = _analyse(_bpo_row(me_current=0), _bpo_bp_data(1000), velocity=0.02)
    assert decision.break_even_days == 2000.0
    assert decision.projected_annual_savings == 3650.0


# --- sub-manufacture: corp stock, owned BPO ME, carried batch -----------------

from eve_online_industry_tracker.application.daily_planner.models import ChainPlan  # noqa: E402


def _sub_phase1(*, me=10, stock=None, blueprint_data=None):
    return {
        "bpo_assets_by_type_id": {888: [_bpo(888, me=me, te=20)]},
        "blueprint_data": blueprint_data or SUB_BLUEPRINT_DATA,
        "market_depth_cache": {34: {"vwap_5d": 1.0}, 54321: {"vwap_5d": 1.0}},
        "corp_material_stock": stock or {},
    }


def _subs(phase1, **row_kwargs):
    row = _row(materials=_batch_materials(**{"54321": 100}), **row_kwargs)
    plan = _planner().plan_chain([_decision(row)], phase1)
    return [d for d in plan.decisions if d.is_sub_component]


def test_sub_manufacture_records_the_me_adjusted_batch_it_will_run():
    (sub,) = _subs(_sub_phase1())
    assert sub.overview_row["sub_runs"] == 10
    assert sub.overview_row["sub_batch_materials"] == {34: 18}


def test_the_sub_job_inputs_use_the_owned_bpos_me():
    (sub,) = _subs(_sub_phase1(me=0))
    assert sub.overview_row["sub_batch_materials"] == {34: 20}
    assert sub.overview_row["sub_manufacture_cost"] == 20.0


def test_an_owned_bpo_with_unknown_me_is_not_sub_built(caplog):
    with caplog.at_level("WARNING"):
        assert _subs(_sub_phase1(me=None)) == []
    assert any("unknown ME" in r.getMessage() for r in caplog.records)


def test_a_component_fully_in_corp_stock_is_not_sub_built():
    assert _subs(_sub_phase1(stock={54321: 100})) == []
    assert _subs(_sub_phase1(stock={54321: 250})) == []


def test_only_the_part_not_in_corp_stock_is_sub_built():
    """Need 100, stock 60: build 40 = 4 runs; 2 x 4 at ME10 = ceil(7.2) = 8."""
    (sub,) = _subs(_sub_phase1(stock={54321: 60}))
    assert sub.overview_row["quantity_requested"] == 100
    assert sub.overview_row["corp_stock_used"] == 60
    assert sub.overview_row["quantity_needed"] == 40
    assert sub.overview_row["sub_runs"] == 4
    assert sub.overview_row["sub_batch_materials"] == {34: 8}
    assert sub.overview_row["market_buy_cost"] == 40.0


def test_a_sub_blueprint_with_no_materials_is_not_a_free_build(caplog):
    blueprint_data = {**SUB_BLUEPRINT_DATA, 888: {"manufacturing": {
        "products": [{"type_id": 54321, "quantity": 10}], "materials": []}}}
    with caplog.at_level("WARNING"):
        assert _subs(_sub_phase1(blueprint_data=blueprint_data)) == []
    assert any("no SDE materials" in r.getMessage() for r in caplog.records)


def test_a_sub_blueprint_that_does_not_make_the_component_is_not_used(caplog):
    blueprint_data = {**SUB_BLUEPRINT_DATA, 888: {"manufacturing": {
        "products": [{"type_id": 77777, "quantity": 10}],
        "materials": [{"type_id": 34, "quantity": 2}]}}}
    # The index maps nothing to 888 for 54321 now, so there is no request at all.
    assert _subs(_sub_phase1(blueprint_data=blueprint_data)) == []
    # The guard itself: asked to build 54321 from a blueprint that makes
    # something else, _sub_batch refuses and says why.
    with caplog.at_level("WARNING"):
        assert _planner()._sub_batch(
            product_type_id=54321, blueprint_type_id=888, qty_to_build=100, me=10,
            phase1_data={"blueprint_data": blueprint_data},
        ) is None
    assert any("no per-run output" in r.getMessage() for r in caplog.records)


def _sub_decision(**overview):
    return ItemDecision(
        type_id=54321, type_name="Widget", decision="build",
        decision_reason="Sub-manufacture: cheaper than market buy", adjusted_score=0.0,
        absolute_profit_per_batch=0.0, isk_per_hour=0.0, margin_pct=0.0,
        days_of_supply_current=0.0, effective_velocity=1.0, meta_group_id=1,
        pipeline_stage="manufacturing", is_sub_component=True,
        overview_row={"type_id": 54321, "type_name": "Widget", "quantity_needed": 100,
                      "sub_runs": 10, "sub_batch_materials": {34: 18},
                      "sub_manufacture_cost": 18.0, "market_buy_cost": 100.0, **overview},
    )


def _one_slot_pilot():
    from types import SimpleNamespace as _NS
    return _NS(list_characters=lambda: [
        {"character_id": 1, "character_name": "Pilot", "skills": {"skills": []}}])


def test_a_sub_manufacture_action_carries_the_planned_runs_and_batch_materials():
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        CharacterAssigner,
    )
    (action,) = CharacterAssigner().assign(
        ChainPlan(decisions=[_sub_decision()]), [], _one_slot_pilot(), _AdminStub())
    assert (action.action_type, action.quantity, action.runs, action.materials) == (
        "sub_manufacture", 100, 10, {34: 18})


def test_sub_components_are_assigned_only_after_every_top_level_item():
    """With one manufacturing slot, the parent must get it even if a
    sub-component scores higher. Otherwise the component is built for a
    parent that never runs."""
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        CharacterAssigner,
    )
    parent = _decision(_row(), adjusted_score=1.0)
    sub = _sub_decision()
    sub.adjusted_score = 100.0
    actions = CharacterAssigner().assign(
        ChainPlan(decisions=[parent, sub]), [], _one_slot_pilot(), _AdminStub())
    assert [a.action_type for a in actions] == ["manufacture"]


# --- sub-manufacture in the parent's structure (ruling F-Q2) -------------------
#
# A sub-component is built in the same structure as its parent, so its inputs
# get the parent profile's structure material bonus on top of the sub BPO's
# ME. A rig bonus applies only where the rig covers the component's own
# manufacturing group (EVE rigs are category-specific). Need 1000 = 100 runs
# x 2 Tritanium = 200 base units.

# SDE group 334 "Construction Components" (category 17 "Commodity"), which
# the producer files under "Advanced Components".
_COMPONENT_ENTRY = {"type_id": 54321, "type_name": "Widget", "quantity": 1000,
                    "group_id": 334, "group_name": "Construction Components",
                    "category_id": 17, "category_name": "Commodity"}


def _profile(*, rig_group=None, aggregate_rig=None, structure=0.01):
    profile = {"material_efficiency_bonus": structure}
    if rig_group is not None:
        profile["structure_rigs"] = [{"type_id": 1, "effects": [
            {"activity": "manufacturing", "metric": "material", "group": rig_group,
             "value": 0.024},
        ]}]
    if aggregate_rig is not None:
        profile["structure_rig_material_bonus"] = aggregate_rig
    return profile


def _structure_subs(profile, *, entry=_COMPONENT_ENTRY, parents=1):
    rows = []
    for i in range(parents):
        p = profile[i] if isinstance(profile, list) else profile
        rows.append(_row(type_id=12345 + i, runs=10, industry_profile=p,
                         materials={"54321": dict(entry)}))
    plan = _planner().plan_chain([_decision(r) for r in rows], _sub_phase1())
    return [d for d in plan.decisions if d.is_sub_component]


def _producer_batch(profile, entry, *, base=200, runs=100, me=10):
    """What IndustryService would compute for this job in this structure."""
    from eve_online_industry_tracker.application.industry.service import IndustryService as S

    group = S._infer_manufacturing_group_uncached(entry)
    reduction = S._combine_reductions([
        me / 100.0,
        S._profile_base_reduction(profile_payload=profile, activity="manufacturing",
                                  metric="material"),
        S._profile_rig_reduction(profile_payload=profile, activity="manufacturing",
                                 metric="material", manufacturing_group=group),
    ])
    return S._round_material_quantity(float(base) * max(0.0, 1.0 - reduction),
                                      minimum_quantity=runs)


def test_a_sub_job_in_a_rigged_structure_matches_the_producer():
    """ME10 + 1% structure + 2.4% Advanced Components rig:
    200 x 0.9 x 0.99 x 0.976 = 173.9 -> 174 (ME only would be 180)."""
    profile = _profile(rig_group="Advanced Components")
    (sub,) = _structure_subs(profile)
    assert sub.overview_row["sub_runs"] == 100
    assert sub.overview_row["sub_batch_materials"] == {34: 174}
    assert _producer_batch(profile, _COMPONENT_ENTRY) == 174
    assert sub.overview_row["sub_manufacture_cost"] == 174.0
    assert sub.overview_row["rig_applicability"] == "applies"


def test_a_rig_for_another_group_does_not_reduce_the_sub_job():
    """Structure bonus only: 200 x 0.9 x 0.99 = 178.2 -> 179."""
    profile = _profile(rig_group="Basic Small Ships")
    (sub,) = _structure_subs(profile)
    assert sub.overview_row["sub_batch_materials"] == {34: 179}
    assert _producer_batch(profile, _COMPONENT_ENTRY) == 179
    assert sub.overview_row["rig_applicability"] == "not_covered"


def test_an_all_group_rig_reduces_the_sub_job():
    profile = _profile(rig_group="All")
    (sub,) = _structure_subs(profile)
    assert sub.overview_row["sub_batch_materials"] == {34: _producer_batch(profile, _COMPONENT_ENTRY)}
    assert sub.overview_row["sub_batch_materials"] == {34: 174}


def test_an_unknown_component_group_gets_the_structure_bonus_only(caplog):
    """No group/category on the parent's material entry: whether the rig covers
    the component is unknown, so no rig bonus (an over-buy, never an under-buy)."""
    entry = {"type_id": 54321, "type_name": "Widget", "quantity": 1000}
    with caplog.at_level("WARNING"):
        (sub,) = _structure_subs(_profile(rig_group="Advanced Components"), entry=entry)
    assert sub.overview_row["sub_batch_materials"] == {34: 179}
    assert sub.overview_row["rig_applicability"] == "unknown"
    warnings = [r for r in caplog.records if "rig applicability" in r.getMessage()]
    assert len(warnings) == 1


def test_an_aggregate_only_rig_bonus_is_not_applied_to_the_sub_job(caplog):
    """The profile's aggregate structure_rig_material_bonus names no group, so
    it cannot say whether the rig covers the component."""
    with caplog.at_level("WARNING"):
        (sub,) = _structure_subs(_profile(aggregate_rig=0.024))
    assert sub.overview_row["sub_batch_materials"] == {34: 179}
    assert sub.overview_row["rig_applicability"] == "unknown"
    assert any("rig applicability" in r.getMessage() for r in caplog.records)


def test_no_industry_profile_means_me_only():
    (sub,) = _structure_subs(None)
    assert sub.overview_row["sub_batch_materials"] == {34: 180}


def test_parents_in_different_structures_use_the_smallest_reduction():
    """Merged across parents, the sub job gets the least favourable facility
    bonus of its requesters, so no parent's share is under-bought."""
    rigged = _profile(rig_group="Advanced Components")
    subs = _structure_subs([rigged, None], parents=2, entry={**_COMPONENT_ENTRY, "quantity": 500})
    (sub,) = subs
    assert sub.overview_row["quantity_needed"] == 1000
    assert sub.overview_row["sub_batch_materials"] == {34: 180}


def test_reduced_batch_quantity_matches_the_producers_rounding_with_facility_bonuses():
    from eve_online_industry_tracker.application.industry.service import IndustryService as S
    from eve_online_industry_tracker.application.daily_planner import facility_bonus
    from eve_online_industry_tracker.infrastructure.sde.blueprints import reduced_batch_quantity

    for base in (1, 2, 5, 33, 1000):
        for runs in (1, 3, 10, 100, 1000):
            for me in (0, 5, 10):
                for structure, rig in ((0.0, 0.0), (0.01, 0.0), (0.01, 0.024), (0.0, 0.042)):
                    factors = [me / 100.0, structure, rig]
                    reduction = S._combine_reductions(factors)
                    expected = S._round_material_quantity(
                        float(base * runs) * max(0.0, 1.0 - reduction), minimum_quantity=runs)
                    ours = reduced_batch_quantity(base, runs, facility_bonus.combine_reductions(factors))
                    assert ours == expected, (
                        base, runs, me, structure, rig)


def test_an_all_group_rig_still_applies_when_the_component_group_is_unknown():
    """An "All" rig covers every group, so it is certain; a group-specific one
    next to it is not applied."""
    profile = _profile(rig_group="All")
    profile["structure_rigs"].append({"type_id": 2, "effects": [
        {"activity": "manufacturing", "metric": "material", "group": "Advanced Components",
         "value": 0.024}]})
    entry = {"type_id": 54321, "type_name": "Widget", "quantity": 1000}
    (sub,) = _structure_subs(profile, entry=entry)
    # 200 x 0.9 x 0.99 x 0.976 = 173.9 -> 174: the All rig only, not both rigs (169.8 -> 170).
    assert sub.overview_row["sub_batch_materials"] == {34: 174}
    assert sub.overview_row["rig_applicability"] == "unknown"


# --- fix round 1 ---------------------------------------------------------------

# SDE group 1136 "Fuel Block" (category 4 "Material"): the producer's inference
# files any unclassified group under "Modules", which an Equipment rig covers.
_FUEL_BLOCK_ENTRY = {"type_id": 54321, "type_name": "Widget", "quantity": 1000,
                     "group_id": 1136, "group_name": "Fuel Block",
                     "category_id": 4, "category_name": "Material"}


def test_an_equipment_rig_does_not_reduce_an_unclassified_component(caplog):
    """A Fuel Block is not a module: an Equipment ("Modules") rig is not
    applied, only the 1% structure bonus. 200 x 0.9 x 0.99 = 178.2 -> 179."""
    with caplog.at_level("WARNING"):
        (sub,) = _structure_subs(_profile(rig_group="Modules"), entry=_FUEL_BLOCK_ENTRY)
    assert sub.overview_row["sub_batch_materials"] == {34: 179}
    assert sub.overview_row["rig_applicability"] == "unknown"


def test_an_equipment_rig_reduces_a_real_module():
    """SDE group 46 "Propulsion Module" (category 7 "Module") is a module."""
    entry = {**_FUEL_BLOCK_ENTRY, "group_id": 46, "group_name": "Propulsion Module",
             "category_id": 7, "category_name": "Module"}
    (sub,) = _structure_subs(_profile(rig_group="Modules"), entry=entry)
    assert sub.overview_row["sub_batch_materials"] == {34: 174}
    assert sub.overview_row["rig_applicability"] == "applies"


def test_a_ship_token_in_a_non_ship_category_is_not_a_ship_group():
    """The producer matches ship groups by token ("industrial"); outside the
    Ship category that is a guess, so a ship rig is not applied."""
    from eve_online_industry_tracker.application.daily_planner import facility_bonus

    assert facility_bonus.component_manufacturing_group(
        {"group_name": "Industrial Reconfiguration", "category_name": "Module"}) is None
    assert facility_bonus.component_manufacturing_group(
        {"group_name": "Frigate", "category_name": "Ship"}) == "Basic Small Ships"


def test_several_owned_bpos_use_the_lowest_known_me():
    phase1 = _sub_phase1()
    phase1["bpo_assets_by_type_id"] = {888: [_bpo(888, me=10, te=20), _bpo(888, me=None, te=0),
                                             _bpo(888, me=0, te=0)]}
    (sub,) = _subs(phase1)
    assert sub.overview_row["sub_batch_materials"] == {34: 20}   # ME0, not ME10's 18


def test_the_unknown_rig_warning_is_not_logged_for_a_component_covered_by_stock(caplog):
    entry = {"type_id": 54321, "type_name": "Widget", "quantity": 1000}
    row = _row(runs=10, industry_profile=_profile(rig_group="Advanced Components"),
               materials={"54321": entry})
    phase1 = _sub_phase1(stock={54321: 1000})
    with caplog.at_level("WARNING"):
        plan = _planner().plan_chain([_decision(row)], phase1)
    assert [d for d in plan.decisions if d.is_sub_component] == []
    assert not any("rig applicability" in r.getMessage() for r in caplog.records)


def test_the_adapter_matches_the_producer_statics():
    from eve_online_industry_tracker.application.daily_planner import facility_bonus
    from eve_online_industry_tracker.application.industry.service import IndustryService as S

    for factors in ([0.1], [0.1, 0.01], [0.1, 0.01, 0.024], [10, 1, 2.4], [0.0]):
        assert facility_bonus.combine_reductions(factors) == S._combine_reductions(factors)
    for profile in (None, _profile(), _profile(rig_group="Advanced Components")):
        assert facility_bonus.structure_material_reduction(profile) == (
            S._profile_base_reduction(profile_payload=profile, activity="manufacturing",
                                      metric="material"))
    assert facility_bonus.component_manufacturing_group(_COMPONENT_ENTRY) == (
        S._infer_manufacturing_group_uncached(_COMPONENT_ENTRY))



# --- fix round 2 ---------------------------------------------------------------

def _entry(group_id, group_name, category_id, category_name):
    return {"type_id": 54321, "type_name": "Widget", "quantity": 1000, "group_id": group_id,
            "group_name": group_name, "category_id": category_id, "category_name": category_name}


def test_an_ammo_rig_does_not_reduce_a_missile_launcher_module():
    """SDE group 508 "Missile Launcher Heavy" is a Module (category 7); the
    producer's "missile" token files it under Ammo & Charges."""
    (sub,) = _structure_subs(_profile(rig_group="Ammo & Charges"),
                             entry=_entry(508, "Missile Launcher Heavy", 7, "Module"))
    assert sub.overview_row["sub_batch_materials"] == {34: 179}
    assert sub.overview_row["rig_applicability"] == "unknown"


def test_a_drone_rig_reduces_a_real_drone():
    """SDE group 100 "Combat Drone" (category 18 "Drone")."""
    (sub,) = _structure_subs(_profile(rig_group="Drones"),
                             entry=_entry(100, "Combat Drone", 18, "Drone"))
    assert sub.overview_row["sub_batch_materials"] == {34: 174}
    assert sub.overview_row["rig_applicability"] == "applies"


def test_a_drone_rig_does_not_reduce_a_drone_upgrade_module():
    """SDE group 645 "Drone Damage Modules" is a Module (category 7)."""
    (sub,) = _structure_subs(_profile(rig_group="Drones"),
                             entry=_entry(645, "Drone Damage Modules", 7, "Module"))
    assert sub.overview_row["sub_batch_materials"] == {34: 179}


def test_name_token_groups_need_their_category():
    from eve_online_industry_tracker.application.daily_planner import facility_bonus as fb

    group = fb.component_manufacturing_group
    assert group({"group_name": "Smart Bomb", "category_name": "Module"}) is None
    assert group({"group_name": "Infrastructure Upgrades", "category_name": "Infrastructure Upgrades"}) is None
    assert group({"group_name": "Citadel", "category_name": "Structure"}) == "Structures"
    assert group({"group_name": "Missile Guidance Enhancer", "category_name": "Module"}) is None
    assert group({"group_name": "Advanced Hybrid Charge", "category_name": "Charge"}) == "Ammo & Charges"
    assert group({"group_name": "Construction Components", "category_name": "Commodity"}) == (
        "Advanced Components")
    # A hypothetical Module-category group whose name contains "component".
    assert group({"group_name": "Component Analyzer", "category_name": "Module"}) is None
    assert group({"group_name": "Capital Construction Components", "category_name": "Commodity"}) == (
        "Capital Components")


def test_an_unknown_me_bpo_beside_a_researched_one_means_buy(caplog):
    """ME10 + unknown: the pilot may use the unknown one, which could be ME0."""
    phase1 = _sub_phase1()
    phase1["bpo_assets_by_type_id"] = {888: [_bpo(888, me=10, te=20), _bpo(888, me=None, te=0)]}
    with caplog.at_level("WARNING"):
        assert _subs(phase1) == []
    assert any("unknown ME" in r.getMessage() for r in caplog.records)
