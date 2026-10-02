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
    # 10 runs x 2 Tritanium x 1 ISK = 20 ISK, versus 100 x 1 ISK on the market.
    assert sub.overview_row["sub_manufacture_cost"] == 20.0
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
    # 180 runs x 2 Tritanium x 1 ISK, versus 1800 x 1 ISK on the market.
    assert sub.overview_row["sub_manufacture_cost"] == 360.0
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
