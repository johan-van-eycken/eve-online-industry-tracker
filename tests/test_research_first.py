# tests/test_research_first.py
"""Task 8c fix round 2: research first, end to end.

EVE runs one job per blueprint, so a BPO in ME/TE research neither builds
nor is copied. ChainPlanner decides once, before sizing the sub-components,
how the item builds instead (an owned-BPC variant row, the owned-BPC runs
only, or nothing); the sub-components and CharacterAssigner's manufacture
action follow that decision. Research durations use the ASSIGNED pilot's
skills, and ME and TE on one BPO run one after the other. Fake numbers.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner, _sizing_bonus_wording
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from planner_skill_fakes import NO_SKILL_REQUIREMENTS
from fakes import _Admin, _AdminStub, _decision

BLUEPRINT_DATA = {
    # Parent 999: 1 run of 12345 needs 100 x the component 54321.
    999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                            "materials": [{"type_id": 54321, "quantity": 100, "type_name": "Widget"}]}},
    # Owned component blueprint 888: 1 run makes 10 x 54321 from 2 x Tritanium.
    888: {"max_production_limit": 300,
          "manufacturing": {"products": [{"type_id": 54321, "quantity": 10}],
                            "materials": [{"type_id": 34, "quantity": 2}]}},
}
MARKET = {34: {"vwap_5d": 1.0}, 54321: {"vwap_5d": 1.0}, 12345: {"vwap_5d": 100.0}}
SECONDS_TO_LEVEL = [0, 60, 180, 420, 900, 1860, 3780, 7620, 15300, 30660, 61380]


def _pilot(*skills, loaded=True):
    base = [("Laboratory Operation", 3406, 5), ("Mass Production", 3387, 5)]
    entries = [{"skill_id": sid, "skill_name": name, "trained_skill_level": lvl}
               for name, sid, lvl in [*base, *skills]]
    pilot = {"character_id": 1, "character_name": "Pilot"}
    if loaded:
        pilot["skills"] = {"skills": entries}
    return pilot


class _Chars:
    def __init__(self, pilot):
        self.pilot = pilot

    def list_characters(self):
        return [self.pilot]


def _segment(runs, source, widgets):
    return {"runs": runs, "source": source, "materials": {"54321": widgets}}


def _row(*, segments, kind="owned_blueprint_original", material_cost=1000.0, profit_amount=10.0,
         overview_row_id="bpo-variant", bpo_item_id=None):
    widgets = sum(s["materials"]["54321"] for s in segments)
    return {
        "type_id": 12345, "type_name": "Thing", "quantity": 10, "overview_row_id": overview_row_id,
        "isk_per_hour": 1.0, "profit_amount": profit_amount, "profit_margin_fraction": 0.1,
        "manufacturing_job": {
            "runs": 10, "manufacturing_job_cost": 50.0, "material_cost": material_cost,
            "total_cost": material_cost + 50.0,
            # A published install fee: an unknown one buys the component (Task 15 fix round 3).
            "materials": {"54321": {"type_id": 54321, "quantity": widgets, "type_name": "Widget",
                                    "component_job_fee_per_run": 0.0}},
            "blueprint_sde": {"blueprint_type_id": 999},
            "blueprint_source_kind": kind,
            "blueprint_original": {"item_id": bpo_item_id} if bpo_item_id is not None else {},
            "job_segments": segments,
            "research_job_cost_by_level": {"research_material": [0.0] * 11, "research_time": [0.0] * 11},
            "research_duration_seconds_by_level": {
                "research_material": SECONDS_TO_LEVEL, "research_time": SECONDS_TO_LEVEL,
            },
        },
    }


BPO_ROW = dict(segments=[_segment(10, "owned_blueprint_original", 1000)])
MIXED_ROW = dict(segments=[_segment(4, "owned_blueprint_copy", 400),
                           _segment(6, "copied_from_owned_blueprint_original", 600)],
                 kind="owned_blueprint_copy")


def _plan(row, *, te=0, me=10, variants=None, bpo_count=1, bpos=None):
    planner = ChainPlanner(industry_service=None, session_provider=None, admin_settings=_AdminStub())
    planner._compute_optimal_me = lambda bp_data, runs=1: 10
    bpo = SimpleNamespace(type_id=999, is_blueprint_copy=False,
                          blueprint_material_efficiency=me, blueprint_time_efficiency=te)
    component_bpo = SimpleNamespace(type_id=888, is_blueprint_copy=False,
                                    blueprint_material_efficiency=10, blueprint_time_efficiency=20)
    return planner.plan_chain([_decision(row)], {
        "bpo_assets_by_type_id": {999: bpos if bpos is not None else [bpo] * bpo_count, 888: [component_bpo]},
        "blueprint_data": BLUEPRINT_DATA,
        "market_depth_cache": MARKET,
        "variant_rows_by_type_id": {12345: variants or []},
    })


def _assign(plan, pilot=None, *, busy_research_slots=0):
    jobs = [{"character_id": 1, "activity_id": 3, "status": "active"}] * busy_research_slots
    actions = CharacterAssigner().assign(plan, jobs, _Chars(pilot or _pilot()), _Admin(), blueprint_data=NO_SKILL_REQUIREMENTS)
    return {a.action_type: a for a in actions}


def _subs(plan):
    return [d for d in plan.decisions if d.is_sub_component]


# --- sub-components follow the parent's actual manufacture -----------------------

def test_without_research_the_component_is_sized_from_the_full_batch():
    plan = _plan(_row(**BPO_ROW), te=20)
    (sub,) = _subs(plan)
    assert sub.overview_row["quantity_needed"] == 1000


def test_a_parent_not_built_while_its_bpo_researches_builds_no_components(caplog):
    caplog.set_level(logging.INFO)
    plan = _plan(_row(**BPO_ROW))
    assert _subs(plan) == []
    actions = _assign(plan)
    assert set(actions) == {"te_research"}
    assert "BPO in research: not built from it in this plan" in actions["te_research"].notes


def test_a_parent_cut_to_its_owned_bpc_runs_sizes_the_component_to_those_runs():
    plan = _plan(_row(**MIXED_ROW))
    (sub,) = _subs(plan)
    assert sub.overview_row["quantity_needed"] == 400  # the BPC job's 4 runs, not 10
    actions = _assign(plan)
    build = actions["manufacture"]
    assert (build.runs, build.quantity, build.materials) == (4, 4, {54321: 400})
    assert actions["sub_manufacture"].quantity == 400


def test_the_bpc_only_manufacture_says_why_its_cost_is_unknown():
    build = _assign(_plan(_row(**MIXED_ROW)))["manufacture"]
    assert build.estimated_cost_isk is None and build.estimated_profit_isk is None
    assert "material cost and profit unknown" in build.notes
    assert build.estimated_job_fee_isk == pytest.approx(50.0 / 10 * 4)


# --- the dropped owned-BPC variant ------------------------------------------------

def _bpc_variant():
    return _row(segments=[_segment(10, "owned_blueprint_copy", 990)], kind="owned_blueprint_copy",
                material_cost=500.0, profit_amount=7.0, overview_row_id="bpc-variant")


def test_a_bpo_in_research_builds_from_the_dropped_bpc_variant_with_known_cost():
    plan = _plan(_row(**BPO_ROW), variants=[_bpc_variant()])
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert top.overview_row["overview_row_id"] == "bpc-variant"
    assert top.overview_row["needs_te_research"] is True
    actions = _assign(plan)
    build = actions["manufacture"]
    assert build.runs == 10
    assert build.materials == {54321: 990}
    assert build.estimated_cost_isk == 500.0
    # Scored like the kept row: (sell 100 - cost 500 / 10 units) x 10 units.
    assert build.estimated_profit_isk == pytest.approx(500.0)
    assert "owned-BPC variant" in build.notes
    assert "te_research" in actions


def test_the_component_follows_the_bpc_variant():
    (sub,) = _subs(_plan(_row(**BPO_ROW), variants=[_bpc_variant()]))
    assert sub.overview_row["quantity_needed"] == 990


def test_a_variant_that_also_copies_the_bpo_is_not_used_whole():
    variant = _row(**MIXED_ROW, overview_row_id="mixed-variant")
    plan = _plan(_row(**BPO_ROW), variants=[variant])
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert top.overview_row["research_first"]["mode"] == "part"
    assert _assign(plan)["manufacture"].runs == 4


# --- research duration at the assigned pilot's skills ------------------------------

def _with_unskilled(row, factor):
    """The producer character's table is `factor` x the unskilled one."""
    unskilled = [int(s / factor) for s in SECONDS_TO_LEVEL]
    row["manufacturing_job"]["research_duration_seconds_by_level_unskilled"] = {
        "research_material": unskilled, "research_time": unskilled,
    }
    return unskilled


def test_an_l0_pilot_researches_slower_than_the_l5_producer_character():
    row = _row(**BPO_ROW)
    # Producer character: Research 5 and Advanced Industry 5 -> 0.75 x 0.85.
    unskilled = _with_unskilled(row, 0.75 * 0.85)
    before = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    te = _assign(_plan(row), pilot=_pilot())["te_research"]
    assert te.estimated_completion >= before + timedelta(seconds=unskilled[10])
    assert te.estimated_completion > before + timedelta(seconds=SECONDS_TO_LEVEL[10] + 60)


def test_the_pilots_research_skills_shorten_the_duration():
    row = _row(**BPO_ROW)
    unskilled = _with_unskilled(row, 0.75 * 0.85)
    pilot = _pilot(("Research", 3403, 4), ("Advanced Industry", 3388, 2))
    before = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    te = _assign(_plan(row), pilot=pilot)["te_research"]
    expected = timedelta(seconds=-(-unskilled[10] * 0.80 * 0.94 // 1))
    assert abs((te.estimated_completion - before) - expected) < timedelta(seconds=5)


def test_a_pilot_whose_skills_are_unknown_gets_no_research_and_says_so():
    """Never plan what cannot be executed: a pilot whose skills are not
    loaded does not qualify for a job (it used to research at the producer
    character's figure)."""
    row = _row(**BPO_ROW)
    _with_unskilled(row, 0.5)
    pilot = _pilot(loaded=False)
    pilot["skills"] = None
    plan = _plan(row)
    actions = _assign(plan, pilot=pilot)
    assert "te_research" not in actions and "me_research" not in actions
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert "no TE research today: no pilot qualifies: skills not loaded for Pilot" in top.decision_reason


def _with_base(row, implants=None):
    """Task 12 fix round 1: the skill- and implant-free table, and the
    pilots' own implant multipliers."""
    row["manufacturing_job"]["research_duration_seconds_by_level_base"] = {
        "research_material": list(SECONDS_TO_LEVEL), "research_time": list(SECONDS_TO_LEVEL),
    }
    if implants is not None:
        row["manufacturing_job"]["research_implant_time_multiplier_by_character_id"] = implants


def test_the_assigned_pilots_own_implants_time_the_research():
    row = _row(**BPO_ROW)
    _with_unskilled(row, 0.5)  # the producer character's implants are in this one: not used
    _with_base(row, {"1": {"research_material": 0.9, "research_time": 0.9}})
    pilot = _pilot(("Research", 3403, 4))
    before = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    te = _assign(_plan(row), pilot=pilot)["te_research"]
    expected = timedelta(seconds=-(-SECONDS_TO_LEVEL[10] * 0.80 * 0.9 // 1))
    assert abs((te.estimated_completion - before) - expected) < timedelta(seconds=5)
    assert "(structure: the producer's; implants: the pilot's)" in te.notes


def test_a_pilot_without_published_implants_gets_none_applied():
    row = _row(**BPO_ROW)
    _with_base(row, {"7": {"research_material": 0.5, "research_time": 0.5}})  # another pilot's
    before = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    te = _assign(_plan(row), pilot=_pilot())["te_research"]
    assert te.estimated_completion >= before + timedelta(seconds=SECONDS_TO_LEVEL[10])
    assert "the pilot's implants are unknown, none applied" in te.notes


# --- ME and TE on one BPO run one after the other ----------------------------------

def test_te_research_starts_when_the_me_research_on_the_same_bpo_ends():
    actions = _assign(_plan(_row(**BPO_ROW), me=4))
    me, te = actions["me_research"], actions["te_research"]
    assert te.estimated_completion - me.estimated_completion == timedelta(seconds=SECONDS_TO_LEVEL[10])
    assert "starts after the ME research" in te.notes


def test_te_completion_is_unknown_when_the_me_duration_is():
    row = _row(**BPO_ROW)
    row["manufacturing_job"]["research_duration_seconds_by_level"]["research_material"] = None
    actions = _assign(_plan(row, me=4))
    assert actions["me_research"].estimated_completion is None
    assert actions["te_research"].estimated_completion is None
    assert "whose duration is unknown" in actions["te_research"].notes


def test_the_service_hands_chain_planning_the_dropped_variants():
    from eve_online_industry_tracker.application.daily_planner.service import _dropped_variants

    kept, bpc, other_kept, orphan = {"type_id": 1}, {"type_id": 1}, {"type_id": 2}, {"type_id": 3}
    assert _dropped_variants([kept, bpc, other_kept, orphan, "junk"], [kept, other_kept]) == {1: [bpc]}


# --- fix round 3: research first only when the research is actually assigned -------

ALL_SIX_SLOTS = 6  # Laboratory Operation 5: 1 + 5 research slots


def test_a_te_0_bpo_without_a_free_research_slot_is_built_from_in_full(caplog):
    caplog.set_level(logging.INFO)
    actions = _assign(_plan(_row(**BPO_ROW)), busy_research_slots=ALL_SIX_SLOTS)
    assert "te_research" not in actions
    build = actions["manufacture"]
    assert (build.runs, build.materials, build.estimated_cost_isk) == (10, {54321: 1000}, 1000.0)
    assert "no free research slot" in build.notes
    assert any("no free research slot" in r.getMessage() and r.levelno == logging.WARNING
               for r in caplog.records)
    # The plan sized no components for the research-first batch (none):
    # they are re-sized to the full BPO batch (fix round 5).
    assert "components re-sized for the BPO's batch" in build.notes
    assert actions["sub_manufacture"].quantity == 1000


def test_a_partial_case_without_a_free_research_slot_is_built_in_full():
    actions = _assign(_plan(_row(**MIXED_ROW)), busy_research_slots=ALL_SIX_SLOTS)
    build = actions["manufacture"]
    assert build.runs == 10 and build.materials == {54321: 1000}
    assert build.estimated_cost_isk == 1000.0
    assert "components re-sized for the BPO's batch" in build.notes
    assert actions["sub_manufacture"].quantity == 1000


def test_a_variant_case_without_a_free_research_slot_builds_from_the_kept_bpo_row():
    actions = _assign(_plan(_row(**BPO_ROW), variants=[_bpc_variant()]), busy_research_slots=ALL_SIX_SLOTS)
    build = actions["manufacture"]
    assert build.materials == {54321: 1000}
    assert build.estimated_cost_isk == 1000.0
    assert "no free research slot" in build.notes


def test_the_chain_planner_log_does_not_claim_the_bpo_is_in_research(caplog):
    caplog.set_level(logging.INFO)
    _plan(_row(**BPO_ROW))
    messages = [r.getMessage() for r in caplog.records if "ChainPlanner" in r.getMessage()]
    assert messages and all("if its research gets a slot" in m for m in messages if "BPO" in m)


# --- several BPOs of the type ----------------------------------------------------

def test_with_two_bpos_one_researches_and_the_other_builds():
    plan = _plan(_row(**BPO_ROW), bpo_count=2)
    (sub,) = _subs(plan)
    assert sub.overview_row["quantity_needed"] == 1000  # the full batch
    actions = _assign(plan)
    assert "te_research" in actions
    assert actions["manufacture"].runs == 10
    assert "one of 2 BPOs" in actions["te_research"].notes


# --- the TE job queued behind the ME job ------------------------------------------

def test_te_queued_after_me_takes_no_research_slot_of_its_own():
    # Laboratory Operation 0: one research slot, taken by the ME job.
    pilot = {"character_id": 1, "character_name": "Pilot", "skills": {"skills": [
        {"skill_id": 3387, "skill_name": "Mass Production", "trained_skill_level": 5},
    ]}}
    actions = _assign(_plan(_row(**BPO_ROW), me=4), pilot=pilot)
    me, te = actions["me_research"], actions["te_research"]
    assert te.character_id == me.character_id
    assert "no research slot taken in this plan" in te.notes


def test_the_duration_note_names_the_producers_structure_and_implants():
    te = _assign(_plan(_row(**BPO_ROW)))["te_research"]
    assert "(structure and implants: the producer's)" in te.notes


# --- fix round 4: several BPOs at different levels ----------------------------------

def _bpo(item_id, me, te):
    return SimpleNamespace(type_id=999, item_id=item_id, is_blueprint_copy=False,
                           blueprint_material_efficiency=me, blueprint_time_efficiency=te)


A_PRICED = _bpo(1, 10, 0)   # the highest ME: the producer prices the kept row at it
B_BUILDS = _bpo(2, 5, 20)


def _priced_row():
    """The kept row, priced at BPO 1 (ME 10): 900 widgets for 10 runs."""
    return _row(segments=[_segment(10, "owned_blueprint_original", 900)], material_cost=900.0,
                overview_row_id="bpo-1", bpo_item_id=1)


def _variant_of_bpo_2():
    """The producer's variant row for BPO 2 (ME 5): 950 widgets, its own cost."""
    return _row(segments=[_segment(10, "owned_blueprint_original", 950)], material_cost=950.0,
                profit_amount=5.0, overview_row_id="bpo-2", bpo_item_id=2)


def test_the_building_bpo_sizes_the_batch_when_the_priced_bpo_researches():
    """The probe: BPO 1 (ME 10 / TE 0) researches TE, BPO 2 (ME 5 / TE 20)
    builds; its materials are at ME 5 (950), not the kept row's ME 10 (900)."""
    plan = _plan(_priced_row(), bpos=[A_PRICED, B_BUILDS], variants=[_variant_of_bpo_2()])
    (sub,) = _subs(plan)
    assert sub.overview_row["quantity_needed"] == 950
    actions = _assign(plan)
    build = actions["manufacture"]
    assert build.materials == {54321: 950}
    assert build.estimated_cost_isk == 950.0
    assert "ME 5 / TE 20" in build.notes
    te = actions["te_research"]
    assert te.notes.startswith("TE research: 0% → 20%")


def test_a_priced_bpo_that_needs_no_research_builds_and_another_researches():
    priced_done = _bpo(1, 10, 20)
    other = _bpo(2, 10, 0)
    row = _priced_row()
    plan = _plan(row, bpos=[other, priced_done])
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert top.overview_row["overview_row_id"] == "bpo-1"
    assert top.overview_row["te_current"] == 0  # the other BPO's TE
    actions = _assign(plan)
    assert actions["manufacture"].materials == {54321: 900}
    assert "ME 10 / TE 20" in actions["manufacture"].notes
    assert "te_research" in actions


@pytest.mark.parametrize("order", [(A_PRICED, B_BUILDS), (B_BUILDS, A_PRICED)])
def test_the_choice_does_not_depend_on_the_asset_order(order):
    plan = _plan(_priced_row(), bpos=list(order), variants=[_variant_of_bpo_2()])
    actions = _assign(plan)
    assert actions["manufacture"].materials == {54321: 950}
    assert actions["te_research"].notes.startswith("TE research: 0% → 20%")


def test_another_bpo_without_its_variant_row_is_not_built_from_at_the_wrong_me():
    plan = _plan(_priced_row(), bpos=[A_PRICED, B_BUILDS])
    actions = _assign(plan)
    assert "manufacture" not in actions  # 900 widgets would under-buy BPO 2 (ME 5)


# --- the queued TE is flagged -------------------------------------------------------

def test_a_te_queued_after_me_is_flagged():
    actions = _assign(_plan(_row(**BPO_ROW), me=4))
    assert actions["te_research"].queued_after_me is True
    assert actions["me_research"].queued_after_me is False


def test_a_te_without_me_is_not_queued():
    assert _assign(_plan(_row(**BPO_ROW)))["te_research"].queued_after_me is False


def test_the_queued_flag_is_persisted_on_the_action_log_row():
    from eve_online_industry_tracker.infrastructure.models import DailyActionLogModel

    assert "queued_after_me" in DailyActionLogModel.__table__.columns


# --- fix round 5: another BPO without its own producer row (default settings) -----
#
# With build_from_bpc on and owned BPCs, the producer emits only the BPC
# variant: no row per BPO. While BPO 1 researches, BPO 2 builds the BPO runs;
# the planner sizes them from the SDE at BPO 2's ME in the row's structure.

PARENT_SDE = {**BLUEPRINT_DATA, 999: {"max_production_limit": 300,
                                      "manufacturing": BLUEPRINT_DATA[999]["manufacturing"]}}


def _bpc_then_bpo_row(profile=None, *, with_profile=True):
    """The kept row: 4 runs on owned BPCs (400), then 6 on BPO 1 at ME 10 (540)."""
    row = _row(segments=[_segment(4, "owned_blueprint_copy", 400),
                         _segment(6, "owned_blueprint_original", 540)],
               kind="owned_blueprint_copy", material_cost=940.0, overview_row_id="bpc-1", bpo_item_id=1)
    if with_profile:
        row["manufacturing_job"]["industry_profile"] = profile if profile is not None else {}
    return row


def _plan5(row, *, bpos, blueprint_data=PARENT_SDE, variants=None, busy=None):
    planner = ChainPlanner(industry_service=None, session_provider=None, admin_settings=_AdminStub())
    planner._compute_optimal_me = lambda bp_data, runs=1: 10
    component_bpo = SimpleNamespace(type_id=888, item_id=88, is_blueprint_copy=False,
                                    blueprint_material_efficiency=10, blueprint_time_efficiency=20)
    data = {
        "bpo_assets_by_type_id": {999: bpos, 888: [component_bpo]},
        "blueprint_data": blueprint_data,
        "market_depth_cache": MARKET,
        "variant_rows_by_type_id": {12345: variants or []},
    }
    if busy is not None:
        data["busy_blueprint_item_ids"] = set(busy)
    return planner.plan_chain([_decision(row)], data)


def test_another_bpo_without_a_producer_row_builds_the_bpo_runs_at_its_own_me():
    plan = _plan5(_bpc_then_bpo_row(), bpos=[A_PRICED, B_BUILDS])
    (sub,) = _subs(plan)
    # 400 on the BPCs + ceil(6 x 100 x 0.95) = 570 at BPO 2's ME 5.
    assert sub.overview_row["quantity_needed"] == 970
    actions = _assign(plan)
    build = actions["manufacture"]
    assert build.runs == 10
    assert build.materials == {54321: 970}
    assert build.estimated_cost_isk == 970.0
    # The fee is EIV-based (ME 0 inputs): the same 10 runs, the same fee.
    assert build.estimated_job_fee_isk == 50.0
    # Scored like the kept row: (sell 100 - cost 970 / 10 units) x 10 units.
    assert build.estimated_profit_isk == pytest.approx(30.0)
    assert "ME 5 / TE 20" in build.notes
    assert "sized by the planner" in build.notes
    assert actions["te_research"].notes.startswith("TE research: 0% → 20%")


def test_the_other_bpo_sizing_applies_the_rows_structure_bonus():
    plan = _plan5(_bpc_then_bpo_row({"material_efficiency_bonus": 0.01}), bpos=[A_PRICED, B_BUILDS])
    # ceil(600 x (1 - (1 - 0.95 x 0.99))) = ceil(564.3) = 565.
    assert _assign(plan)["manufacture"].materials == {54321: 965}


def test_the_other_bpo_sizing_splits_the_runs_at_the_max_runs_per_job():
    sde = {**PARENT_SDE, 999: {**PARENT_SDE[999], "max_production_limit": 4}}
    plan = _plan5(_bpc_then_bpo_row({"material_efficiency_bonus": 0.01}), bpos=[A_PRICED, B_BUILDS],
                  blueprint_data=sde)
    # Jobs of 4 + 2 runs, each rounded up: 377 + 189 = 566 (one job: 565).
    assert _assign(plan)["manufacture"].materials == {54321: 966}


@pytest.mark.parametrize("case, expected_reason", [
    ("unknown_me", "its ME is unknown"),
    ("no_sde", "no SDE manufacturing data"),
    ("no_profile", "no industry profile"),
])
def test_another_bpo_that_cannot_be_sized_keeps_the_safe_fallback_with_a_reason(case, expected_reason):
    other = _bpo(2, None, 20) if case == "unknown_me" else B_BUILDS
    sde = {**PARENT_SDE, 999: {}} if case == "no_sde" else PARENT_SDE
    row = _bpc_then_bpo_row(with_profile=case != "no_profile")
    plan = _plan5(row, bpos=[A_PRICED, other], blueprint_data=sde)
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    first = top.overview_row["research_first"]
    assert first["mode"] == "part"  # the BPC runs only: never the BPO runs at ME 10
    assert expected_reason in first["note"]
    build = _assign(plan)["manufacture"]
    assert build.runs == 4 and build.materials == {54321: 400}
    assert expected_reason in build.notes


# --- fix round 5: no research slot re-sizes the components to the kept row ---------

def test_no_slot_fallback_resizes_the_components_to_the_kept_bpo_row():
    """The variant planned BPO 2 (950); without a research slot BPO 1 builds
    its kept row (900): the component is re-sized to 900, not left at 950."""
    plan = _plan(_priced_row(), bpos=[A_PRICED, B_BUILDS], variants=[_variant_of_bpo_2()])
    actions = _assign(plan, busy_research_slots=ALL_SIX_SLOTS)
    assert actions["manufacture"].materials == {54321: 900}
    assert actions["sub_manufacture"].quantity == 900
    (sub,) = _subs(plan)
    assert sub.overview_row["quantity_needed"] == 900
    assert "components re-sized for the BPO's batch" in actions["manufacture"].notes
    assert "buys any difference" not in actions["manufacture"].notes


# --- fix round 5: blueprints installed in an active job ------------------------------

def test_a_bpo_in_an_active_job_is_neither_researched_nor_built_from():
    plan = _plan5(_priced_row(), bpos=[A_PRICED], busy={1})
    assert _subs(plan) == []
    actions = _assign(plan)
    assert actions == {}
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert "in an active industry job" in top.overview_row["research_first"]["note"]
    # Not shown as "manufacturing": nothing is built from it (fix round 1, minor 5).
    assert top.pipeline_stage == "blocked"


def test_a_blocked_busy_bpo_row_says_not_built_in_its_decision_reason():
    """Task 13 carry 2: the research-first "none" branch with a busy priced
    BPO sets the stage to "blocked", so its persisted decision_reason must
    carry the same "; not built: <reason>" text every other blocked row has."""
    plan = _plan5(_priced_row(), bpos=[A_PRICED], busy={1})
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert top.pipeline_stage == "blocked"
    assert top.decision_reason == "test; not built: BPO in an active industry job"


def test_a_bpo_in_research_without_a_bpc_has_no_not_built_reason():
    plan = _plan(_row(**BPO_ROW))
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert "not built:" not in top.decision_reason


def test_a_bpo_in_research_without_a_bpc_keeps_its_stage():
    # Conditional on a research slot: it may still build from the BPO.
    plan = _plan(_row(**BPO_ROW))
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert top.overview_row["research_first"]["mode"] == "none"
    assert top.pipeline_stage == "manufacturing"


def test_a_busy_priced_bpo_builds_from_a_free_one_without_a_research_slot():
    free = _bpo(2, 10, 20)  # needs no research
    row = _priced_row()
    row["manufacturing_job"]["industry_profile"] = {}
    plan = _plan5(row, bpos=[A_PRICED, free], busy={1})
    # Busy is not conditional on a research slot.
    actions = _assign(plan, busy_research_slots=ALL_SIX_SLOTS)
    assert "te_research" not in actions
    build = actions["manufacture"]
    assert build.materials == {54321: 900}  # ceil(10 x 100 x 0.90) at BPO 2's ME 10
    assert "in an active industry job" in build.notes


def test_research_skips_a_busy_bpo_and_takes_a_free_one():
    # BPO 1 (TE 2, priced, first in producer order) is busy; BPO 2 (TE 0) researches.
    plan = _plan5(_priced_row(), bpos=[_bpo(1, 10, 2), _bpo(2, 10, 0)], busy={1})
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert top.overview_row.get("needs_te_research") is True
    assert top.overview_row["te_current"] == 0


def test_a_busy_component_bpo_is_not_sub_built_from():
    plan = _plan5(_row(**BPO_ROW), bpos=[_bpo(1, 10, 20)], busy={88})
    assert _subs(plan) == []


def test_the_service_collects_the_blueprints_of_active_jobs():
    from eve_online_industry_tracker.application.daily_planner.service import busy_blueprint_item_ids

    jobs = [SimpleNamespace(blueprint_item_id=11), {"blueprint_item_id": 12}, SimpleNamespace(blueprint_item_id=None)]
    assert busy_blueprint_item_ids(jobs) == {11, 12}


# --- round 5 Task 10 carry 15: the sizing note names only the bonuses applied ------

@pytest.mark.parametrize("profile, wording", [
    ({}, "with no structure or rig bonus"),
    ({"material_efficiency_bonus": 0.01}, "with the row's structure bonus (no rig bonus)"),
])
def test_the_other_bpo_sizing_note_names_only_the_bonuses_applied(profile, wording):
    build = _assign(_plan5(_bpc_then_bpo_row(profile), bpos=[A_PRICED, B_BUILDS]))["manufacture"]
    assert wording in build.notes
    assert "rig bonuses" not in build.notes


@pytest.mark.parametrize("structure, rig, wording", [
    (0.0, 0.0, "with no structure or rig bonus"),
    (0.01, 0.0, "with the row's structure bonus (no rig bonus)"),
    (0.0, 0.02, "with the row's rig bonus (no structure bonus)"),
    (0.01, 0.02, "with the row's structure and rig bonuses"),
])
def test_the_sizing_bonus_wording(structure, rig, wording):
    assert _sizing_bonus_wording(structure, rig) == wording


# --- round 5 Task 10 carries 12, 16: busy blueprints in research first ------------

def test_a_research_first_variant_on_a_busy_bpc_is_not_used():
    variant = _bpc_variant()
    variant["manufacturing_job"]["blueprint_copy"] = {"item_id": 55}
    plan = _plan5(_row(**BPO_ROW), bpos=[_bpo(1, 10, 0)], variants=[variant], busy={55})
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert top.overview_row["overview_row_id"] != "bpc-variant"
    assert top.overview_row["research_first"]["mode"] == "none"


def test_a_free_research_first_variant_is_still_used():
    variant = _bpc_variant()
    variant["manufacturing_job"]["blueprint_copy"] = {"item_id": 56}
    plan = _plan5(_row(**BPO_ROW), bpos=[_bpo(1, 10, 0)], variants=[variant], busy={55})
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert top.overview_row["overview_row_id"] == "bpc-variant"


def _unknown_levels_warnings(caplog):
    return [r for r in caplog.records if r.levelno == logging.WARNING and "unknown ME/TE" in r.getMessage()]


def test_a_busy_priced_bpo_with_unknown_levels_does_not_warn_about_them(caplog):
    caplog.set_level(logging.INFO)
    plan = _plan5(_priced_row(), bpos=[_bpo(1, None, None)], busy={1})
    assert _unknown_levels_warnings(caplog) == []
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert "in an active industry job" in top.overview_row["research_first"]["note"]


def test_a_free_bpo_with_unknown_levels_still_warns(caplog):
    _plan5(_priced_row(), bpos=[_bpo(1, None, None)])
    assert len(_unknown_levels_warnings(caplog)) == 1


# --- Task 10 fix round 1, minor 3: jobs whose copy flag is NULL ------------------

def _job(item_id, activity_id, is_copy=None, bp_type_id=999):
    return {"blueprint_item_id": item_id, "blueprint_type_id": bp_type_id,
            "activity_id": activity_id, "blueprint_is_blueprint_copy": is_copy}


def test_a_null_copy_flag_is_resolved_through_the_item_index():
    from eve_online_industry_tracker.application.daily_planner.research_first import busy_blueprint_type_ids

    jobs = [_job(1, 1)]  # a manufacturing job, flag NULL
    assert busy_blueprint_type_ids(jobs, copies=False, is_copy_by_item_id={1: False}) == {999}
    assert busy_blueprint_type_ids(jobs, copies=True, is_copy_by_item_id={1: False}) == set()
    assert busy_blueprint_type_ids(jobs, copies=True, is_copy_by_item_id={1: True}) == {999}


def test_an_unresolved_null_copy_flag_counts_as_both_so_no_busy_bpo_is_missed():
    from eve_online_industry_tracker.application.daily_planner.research_first import busy_blueprint_type_ids

    jobs = [_job(1, 1)]
    assert busy_blueprint_type_ids(jobs, copies=False) == {999}
    assert busy_blueprint_type_ids(jobs, copies=True) == {999}
    # Research and copying need a BPO; invention needs a BPC.
    assert busy_blueprint_type_ids([_job(2, 5)], copies=True) == set()
    assert busy_blueprint_type_ids([_job(3, 8)], copies=False) == set()
    # A flag the job does give wins.
    assert busy_blueprint_type_ids([_job(4, 1, is_copy=True)], copies=False, is_copy_by_item_id={4: False}) == set()
