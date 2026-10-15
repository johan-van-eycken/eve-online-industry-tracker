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

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ItemDecision

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


class _AdminStub:
    def get(self, section, key, default=None):
        raise KeyError(key)


class _Admin:
    def get(self, section, key, default=None):
        return default


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
         overview_row_id="bpo-variant"):
    widgets = sum(s["materials"]["54321"] for s in segments)
    return {
        "type_id": 12345, "type_name": "Thing", "quantity": 10, "overview_row_id": overview_row_id,
        "isk_per_hour": 1.0, "profit_amount": profit_amount, "profit_margin_fraction": 0.1,
        "manufacturing_job": {
            "runs": 10, "manufacturing_job_cost": 50.0, "material_cost": material_cost,
            "total_cost": material_cost + 50.0,
            "materials": {"54321": {"type_id": 54321, "quantity": widgets, "type_name": "Widget"}},
            "blueprint_sde": {"blueprint_type_id": 999},
            "blueprint_source_kind": kind,
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


def _decision(row) -> ItemDecision:
    return ItemDecision(
        type_id=12345, type_name="Thing", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=1,
        pipeline_stage="manufacturing", overview_row=row,
    )


def _plan(row, *, te=0, me=10, variants=None, bpo_count=1):
    planner = ChainPlanner(industry_service=None, session_provider=None, admin_settings=_AdminStub())
    planner._compute_optimal_me = lambda bp_type_id, runs=1: 10
    bpo = SimpleNamespace(type_id=999, is_blueprint_copy=False,
                          blueprint_material_efficiency=me, blueprint_time_efficiency=te)
    component_bpo = SimpleNamespace(type_id=888, is_blueprint_copy=False,
                                    blueprint_material_efficiency=10, blueprint_time_efficiency=20)
    return planner.plan_chain([_decision(row)], {
        "bpo_assets_by_type_id": {999: [bpo] * bpo_count, 888: [component_bpo]},
        "blueprint_data": BLUEPRINT_DATA,
        "market_depth_cache": MARKET,
        "variant_rows_by_type_id": {12345: variants or []},
    })


def _assign(plan, pilot=None, *, busy_research_slots=0):
    jobs = [{"character_id": 1, "activity_id": 3, "status": "active"}] * busy_research_slots
    actions = CharacterAssigner().assign(plan, jobs, _Chars(pilot or _pilot()), _Admin())
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


def test_unknown_pilot_skills_keep_the_producer_figure_and_say_so():
    row = _row(**BPO_ROW)
    _with_unskilled(row, 0.5)
    pilot = _pilot(loaded=False)
    pilot["skills"] = None
    te = _assign(_plan(row), pilot=pilot)["te_research"]
    assert "(at the producer character's skills: the pilot's are unknown)" in te.notes


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
    # The plan's components were sized for the research-first batch (none):
    # the note says the shopping list buys them.
    assert "the shopping list buys them" in build.notes


def test_a_partial_case_without_a_free_research_slot_is_built_in_full():
    actions = _assign(_plan(_row(**MIXED_ROW)), busy_research_slots=ALL_SIX_SLOTS)
    build = actions["manufacture"]
    assert build.runs == 10 and build.materials == {54321: 1000}
    assert build.estimated_cost_isk == 1000.0
    assert "components were sized for 4 of 10 runs" in build.notes


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
