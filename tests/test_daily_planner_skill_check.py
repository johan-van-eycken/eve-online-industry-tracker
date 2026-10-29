"""Never plan what cannot be executed: a job goes only to a pilot whose loaded
skills meet the SDE skills of that job's activity (manufacturing / reaction,
copying, ME and TE research; invention already checked its own).

A plan built two rows no pilot could install: Capital Algid Energy
Administrations Unit II (blueprint 31429, Energy Weapon Rigging 4) and
'Chivalry' Large Remote Capacitor Transmitter (blueprint 23857, Takmahl
Technology 5): CharacterAssigner picked the pilot with the most free
manufacturing slots and checked no skills. The blueprint and skill type ids
and the required levels are the public SDE's; the pilots, their skill levels,
item ids and everything else are fake.
"""
from __future__ import annotations

from types import SimpleNamespace

from planner_skill_fakes import NO_SKILL_REQUIREMENTS, blueprint, skill
from test_daily_planner_assignment import _AdminStub, _Chars, _char, _decision, _skill

from eve_online_industry_tracker.application.daily_planner.character_assigner import (
    CharacterAssigner,
    _SkillGate,
)
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan
from eve_online_industry_tracker.application.daily_planner.values import utc_now
from eve_online_industry_tracker.application.industry.character_slots import (
    activity_skill_requirements,
    skill_shortfall,
)

INDUSTRY, EWR, TAKMAHL, SCIENCE, METALLURGY, RESEARCH = 3380, 26258, 23123, 3402, 3409, 3403
ALGID_BP, ALGID = 31429, 31428
CHIVALRY_BP, CHIVALRY = 23857, 23856
COMPONENT_BP, COMPONENT = 990011, 990010

BLUEPRINTS = {
    ALGID_BP: blueprint(
        manufacturing=[skill(INDUSTRY, "Industry", 5), skill(EWR, "Energy Weapon Rigging", 4)],
        research_material=[skill(METALLURGY, "Metallurgy", 5), skill(EWR, "Energy Weapon Rigging", 4)],
        research_time=[skill(RESEARCH, "Research", 5), skill(EWR, "Energy Weapon Rigging", 4)],
        copying=[skill(SCIENCE, "Science", 5)],
        invention=[],
    ),
    CHIVALRY_BP: blueprint(
        manufacturing=[skill(INDUSTRY, "Industry", 5), skill(TAKMAHL, "Takmahl Technology", 5)],
        research_material=[], research_time=[], copying=[],
    ),
    COMPONENT_BP: blueprint(manufacturing=[skill(INDUSTRY, "Industry", 5), skill(EWR, "Energy Weapon Rigging", 4)]),
}


def _pilot(char_id, name, *skills, mass_production=0):
    return _char(char_id, name, [
        _skill("Industry", 5, INDUSTRY), _skill("Mass Production", mass_production, 0), *skills,
    ])


def _alpha():
    return _pilot(1, "Pilot Alpha", _skill("Energy Weapon Rigging", 2, EWR), _skill("Takmahl Technology", 3, TAKMAHL),
                  mass_production=5)


def _other():
    return _pilot(2, "Other", _skill("Energy Weapon Rigging", 1, EWR), mass_production=5)


def _row(type_id, bp_type_id, **extra):
    return {"type_id": type_id, "manufacturing_job": {"runs": 1, "blueprint_sde": {"blueprint_type_id": bp_type_id}},
            **extra}


def _assign(decisions, chars, blueprint_data=BLUEPRINTS, plan=None):
    plan = plan if plan is not None else ChainPlan(decisions=decisions)
    return CharacterAssigner().assign(plan, [], _Chars(chars), _AdminStub(), blueprint_data=blueprint_data)


# ── the shared helper ────────────────────────────────────────────────────────

def test_the_shortfall_names_each_missing_skill_with_the_best_pilot_level():
    entries, why = activity_skill_requirements(BLUEPRINTS, ALGID_BP, "manufacturing")
    assert why is None
    pilots = [("Pilot Alpha", {INDUSTRY: 5, EWR: 2}), ("Other", {INDUSTRY: 5, EWR: 1})]
    assert skill_shortfall(entries, pilots) == "no pilot has the skills: Energy Weapon Rigging 4 (best: Pilot Alpha at 2)"


def test_the_shortfall_names_pilots_whose_skills_are_not_loaded():
    entries, _ = activity_skill_requirements(BLUEPRINTS, CHIVALRY_BP, "manufacturing")
    text = skill_shortfall(entries, [("Pilot Alpha", {INDUSTRY: 5, TAKMAHL: 3}), ("Unknown", None)])
    assert text == "no pilot has the skills: Takmahl Technology 5 (best: Pilot Alpha at 3); skills not loaded for Unknown"


def test_the_shortfall_when_no_one_pilot_has_every_skill():
    entries, _ = activity_skill_requirements(BLUEPRINTS, ALGID_BP, "manufacturing")
    text = skill_shortfall(entries, [("A", {INDUSTRY: 5, EWR: 1}), ("B", {INDUSTRY: 1, EWR: 4})])
    assert text == ("no one pilot has all the skills: A lacks Energy Weapon Rigging 4 (at 1); "
                    "B lacks Industry 5 (at 1)")


def test_unknown_requirements_say_why():
    assert activity_skill_requirements(BLUEPRINTS, 123, "manufacturing") == (
        None, "the manufacturing skills are unknown: blueprint 123 is not in the SDE data",
    )
    assert activity_skill_requirements(BLUEPRINTS, COMPONENT_BP, "copying") == (
        None, "the SDE has no copying activity for blueprint 990011",
    )
    assert activity_skill_requirements(BLUEPRINTS, None, "research_material")[1] == (
        "the ME research skills are unknown: the row names no blueprint"
    )


def test_a_reaction_formula_is_checked_on_its_reaction_skills():
    data = {5: blueprint(reaction=[skill(45746, "Reactions", 1)])}
    entries, why = activity_skill_requirements(data, 5, "manufacturing")
    assert why is None and [e["type_id"] for e in entries] == [45746]


# ── manufacture ──────────────────────────────────────────────────────────────

def test_capital_algid_is_not_built_when_no_pilot_has_energy_weapon_rigging_4():
    decision = _decision(type_id=ALGID, type_name="Capital Algid Energy Administrations Unit II",
                         decision_reason="Build: score 11.6M ISK/hr", overview_row=_row(ALGID, ALGID_BP))
    actions = _assign([decision], [_alpha(), _other()])
    assert [a for a in actions if a.action_type == "manufacture"] == []
    assert decision.decision_reason == (
        "Build: score 11.6M ISK/hr; not built: no pilot has the skills: Energy Weapon Rigging 4 (best: Pilot Alpha at 2)"
    )
    assert decision.pipeline_stage == "blocked"


def test_chivalry_is_not_built_when_no_pilot_has_takmahl_technology_5():
    decision = _decision(type_id=CHIVALRY, decision_reason="Build", overview_row=_row(CHIVALRY, CHIVALRY_BP))
    actions = _assign([decision], [_alpha(), _other()])
    assert [a for a in actions if a.action_type == "manufacture"] == []
    assert "not built: no pilot has the skills: Takmahl Technology 5 (best: Pilot Alpha at 3)" in decision.decision_reason


def test_a_qualifying_pilot_is_chosen_over_one_with_more_free_slots():
    skilled = _pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR), mass_production=0)  # 1 slot
    decision = _decision(type_id=ALGID, overview_row=_row(ALGID, ALGID_BP))
    actions = _assign([decision], [_alpha(), skilled])                                          # Alpha: 6 slots
    assert [(a.action_type, a.character_id) for a in actions] == [("manufacture", 3)]


def test_a_pilot_whose_skills_are_not_loaded_does_not_qualify():
    unknown = {"character_id": 4, "character_name": "Unknown"}  # no skill list
    decision = _decision(type_id=CHIVALRY, decision_reason="Build", overview_row=_row(CHIVALRY, CHIVALRY_BP))
    actions = _assign([decision], [unknown])
    assert actions == []
    assert decision.decision_reason == (
        "Build; not built: no pilot has the skills: Industry 5, Takmahl Technology 5; skills not loaded for Unknown"
    )


def test_a_row_whose_blueprint_skills_are_unknown_is_not_built():
    decision = _decision(type_id=ALGID, decision_reason="Build", overview_row=_row(ALGID, 424242))
    assert _assign([decision], [_alpha()]) == []
    assert decision.decision_reason == (
        "Build; not built: the manufacturing skills are unknown: blueprint 424242 is not in the SDE data"
    )


def test_an_owner_without_the_skills_gets_no_manufacture_and_the_row_is_not_built():
    owner_bpc = 7001
    skilled = _pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR))
    decision = _decision(type_id=ALGID, decision_reason="Build", overview_row=_row(
        ALGID, ALGID_BP, blueprint_owner_by_item={owner_bpc: 1}, ledger_claims={"manufacture": [owner_bpc]},
    ))
    actions = _assign([decision], [_alpha(), skilled])
    assert actions == []
    assert decision.decision_reason == "Build; not built: owner Pilot Alpha lacks the skills: Energy Weapon Rigging 4 (at 2)"
    assert decision.pipeline_stage == "blocked"


def test_the_gate_passes_a_corp_blueprint_but_the_ledger_books_the_owners_one():
    """The reproduced disagreement: a corp BPO and the owner's BPC are both
    free, so the gate keeps the row a build (Skilled could install the corp
    one); the batch is priced on the owner's BPC, which the ledger books and
    owner-binds, and that owner lacks the skills. The assigner then marks the
    row not built (stage blocked), so it does not stay a build without a job."""
    skilled = _pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR))
    owned, corp = 990000000031, 990000000032
    decision = _decision(type_id=ALGID, decision_reason="Build", overview_row=_row(ALGID, ALGID_BP))
    _service([_alpha(), skilled])._phase_4_skill_gate([decision], _phase1(
        bpos={ALGID_BP: [{"item_id": corp, "type_id": ALGID_BP}]},
        bpcs={ALGID_BP: [{"item_id": owned, "type_id": ALGID_BP, "character_id": 1, "blueprint_runs": 2}]},
    ))
    assert decision.decision == "build"
    decision.overview_row.update(blueprint_owner_by_item={owned: 1}, ledger_claims={"manufacture": [owned]})
    assert _assign([decision], [_alpha(), skilled]) == []
    assert decision.pipeline_stage == "blocked"
    assert decision.decision_reason.endswith(
        "not built: owner Pilot Alpha lacks the skills: Energy Weapon Rigging 4 (at 2)"
    )


# ── sub-builds ───────────────────────────────────────────────────────────────

def _sub(bp_type_id=COMPONENT_BP):
    return _decision(type_id=COMPONENT, type_name="Component", is_sub_component=True,
                     decision_reason="Sub-manufacture: cheaper than market buy", overview_row={
                         "type_id": COMPONENT, "blueprint_type_id": bp_type_id, "requested_by_type_ids": [ALGID],
                         "sub_job_batches": [{"quantity": 10, "output": 10, "runs": 10, "cost": 1.0, "materials": {}}],
                     })


def test_a_sub_build_no_pilot_can_run_gets_no_job_and_says_why():
    sub = _sub()
    actions = _assign([sub], [_alpha(), _other()])
    assert actions == []
    assert sub.decision_reason == (
        "Sub-manufacture: cheaper than market buy; not built: no pilot has the skills: "
        "Energy Weapon Rigging 4 (best: Pilot Alpha at 2); its units are bought"
    )
    assert sub.pipeline_stage == "blocked"  # as a top-level row not built


def test_a_sub_build_goes_to_a_qualifying_pilot():
    skilled = _pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR))
    actions = _assign([_sub()], [_alpha(), skilled])
    assert [(a.action_type, a.character_id) for a in actions] == [("sub_manufacture", 3)]


def test_the_sub_builds_of_a_parent_no_pilot_can_build_are_dropped():
    parent = _decision(type_id=ALGID, decision_reason="Build", overview_row=_row(ALGID, ALGID_BP))
    sub = _sub(bp_type_id=CHIVALRY_BP)
    calls = []

    def _resize(fell_back, dropped=frozenset()):
        calls.append(set(dropped))
        return []

    plan = SimpleNamespace(decisions=[parent, sub], resize_sub_components=_resize,
                           sub_request_parent_type_ids=frozenset({ALGID}))
    actions = _assign(None, [_alpha(), _other()], plan=plan)
    assert actions == []
    assert calls == [{ALGID}]


# ── research and copy ────────────────────────────────────────────────────────

def test_me_and_te_research_need_their_own_skills():
    lab = _char(5, "Lab", [_skill("Laboratory Operation", 5), _skill("Metallurgy", 5, METALLURGY),
                           _skill("Research", 5, RESEARCH), _skill("Energy Weapon Rigging", 3, EWR)])
    decision = _decision(type_id=ALGID, decision_reason="Build", overview_row=_row(
        ALGID, ALGID_BP, needs_me_research=True, needs_te_research=True,
    ))
    actions = _assign([decision], [lab])
    assert [a for a in actions if a.action_type.endswith("_research")] == []
    assert "no ME research today: no pilot has the skills: Energy Weapon Rigging 4 (best: Lab at 3)" in (
        decision.decision_reason
    )
    assert "no TE research today: no pilot has the skills: Energy Weapon Rigging 4 (best: Lab at 3)" in (
        decision.decision_reason
    )


def test_research_goes_to_the_pilot_with_the_skills():
    weak = _char(5, "Weak", [_skill("Laboratory Operation", 5), _skill("Advanced Laboratory Operation", 5)])
    able = _char(6, "Able", [_skill("Metallurgy", 5, METALLURGY), _skill("Research", 5, RESEARCH),
                             _skill("Energy Weapon Rigging", 4, EWR)])
    decision = _decision(type_id=ALGID, overview_row=_row(
        ALGID, ALGID_BP, needs_me_research=True, needs_te_research=True,
    ))
    actions = _assign([decision], [weak, able])
    assert sorted((a.action_type, a.character_id) for a in actions if a.action_type.endswith("_research")) == [
        ("me_research", 6), ("te_research", 6),
    ]


def test_a_copy_needs_the_copying_skills():
    lab = _char(5, "Lab", [_skill("Laboratory Operation", 5), _skill("Science", 4, SCIENCE)])
    decision = _decision(type_id=12345, decision_reason="Build", pipeline_stage="copying", overview_row={
        "type_id": 12345, "needs_invention": True, "has_t1_bpo": True, "t1_blueprint_type_id": ALGID_BP,
        "manufacturing_job": {"runs": 1, "blueprint_sde": {"blueprint_type_id": CHIVALRY_BP}},
    })
    actions = _assign([decision], [lab])
    assert [a for a in actions if a.action_type == "copy"] == []
    assert f"no copy of its T1 BPO ({ALGID_BP}) today: no pilot has the skills: Science 5 (best: Lab at 4)" in (
        decision.decision_reason
    )


def test_no_skill_requirements_keep_the_old_assignment():
    decision = _decision(type_id=ALGID, overview_row=_row(ALGID, ALGID_BP))
    actions = _assign([decision], [_alpha(), _other()], blueprint_data=NO_SKILL_REQUIREMENTS)
    assert [(a.action_type, a.character_id) for a in actions] == [("manufacture", 1)]


# ── the SDE loader ───────────────────────────────────────────────────────────

def test_the_sde_loader_carries_every_activitys_skills():
    """Against the local SDE (skipped without it): the real requirements of
    the two plan-21 blueprints."""
    import os

    import pytest
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from sde_industry_fakes import LOCAL_SDE, skip_without_local_sde

    from eve_online_industry_tracker.infrastructure.sde.blueprints import get_blueprint_manufacturing_data

    skip_without_local_sde()
    engine = create_engine(f"sqlite:///file:{LOCAL_SDE}?mode=ro&uri=true")
    try:
        with Session(engine) as session:
            data = get_blueprint_manufacturing_data(session, "en", [ALGID_BP, CHIVALRY_BP])
    finally:
        engine.dispose()

    def levels(bp, activity):
        return {(e["type_name"], e["level"]) for e in data[bp]["activity_skills"][activity]}

    assert ("Energy Weapon Rigging", 4) in levels(ALGID_BP, "manufacturing")
    assert ("Energy Weapon Rigging", 4) in levels(ALGID_BP, "research_material")
    assert ("Takmahl Technology", 5) in levels(CHIVALRY_BP, "manufacturing")
    assert levels(CHIVALRY_BP, "copying") == set()          # copyable, no skill needed
    assert "invention" not in data[CHIVALRY_BP]["activity_skills"]  # no such activity


# ── the decision gate ────────────────────────────────────────────────────────

def test_the_decision_gate_names_the_missing_skills():
    def gate(chars):
        return _SkillGate(BLUEPRINTS, CharacterAssigner()._compute_available_slots(chars, [], utc_now()))

    alpha_other = gate([_alpha(), _other(), {**_alpha(), "character_id": 9, "industry_enabled": False}])
    assert alpha_other.for_owners(ALGID_BP, "manufacturing", []) == (
        frozenset(), "no pilot has the skills: Energy Weapon Rigging 4 (best: Pilot Alpha at 2)"
    )
    skilled = gate([_pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR))])
    assert skilled.for_owners(ALGID_BP, "manufacturing", []) == (frozenset({3}), None)


def _phase1(bpos=None, bpcs=None, busy=()):
    """The phase-1 inputs the gate reads: the SDE blueprints and the merged
    corp + pilot blueprint assets (a pilot's carry its character_id)."""
    return {"blueprint_data": BLUEPRINTS, "bpo_assets_by_type_id": bpos or {}, "bpc_assets_by_type_id": bpcs or {},
            "busy_blueprint_item_ids": set(busy)}


def _service(chars):
    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService

    return DailyPlannerService(
        industry_service=SimpleNamespace(), corporations_service=SimpleNamespace(),
        characters_service=_Chars(chars), sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        repo=SimpleNamespace(), admin_settings=SimpleNamespace(get=lambda *a, **k: None),
        session_provider=SimpleNamespace(app_session=lambda: None, sde_session=lambda: None),
    )


def test_the_decision_gate_turns_an_unbuildable_build_into_a_skip():
    """Plan 21: both rows counted as builds. Now they are skips that say why,
    so chain planning neither sub-builds, researches nor buys for them."""
    algid = _decision(type_id=ALGID, decision_reason="Build: score 11.6M ISK/hr, pipeline 0.0d < 3d",
                      overview_row=_row(ALGID, ALGID_BP))
    chivalry = _decision(type_id=CHIVALRY, decision_reason="Build: score 115.5M ISK/hr, pipeline 0.0d < 3d",
                         overview_row=_row(CHIVALRY, CHIVALRY_BP))
    buildable = _decision(type_id=ALGID + 1, overview_row=_row(ALGID + 1, ALGID_BP))
    watched = _decision(type_id=CHIVALRY + 1, decision="watch", decision_reason="Watch",
                        overview_row=_row(CHIVALRY + 1, CHIVALRY_BP))
    skilled = _pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR))

    svc = _service([_alpha(), _other()])
    svc._phase_4_skill_gate([algid, chivalry, watched], _phase1())
    assert (algid.decision, algid.pipeline_stage, algid.decision_reason) == (
        "skip", "blocked",
        "Build: score 11.6M ISK/hr, pipeline 0.0d < 3d; not built: no pilot has the skills: "
        "Energy Weapon Rigging 4 (best: Pilot Alpha at 2)",
    )
    assert (chivalry.decision, chivalry.decision_reason) == (
        "skip",
        "Build: score 115.5M ISK/hr, pipeline 0.0d < 3d; not built: no pilot has the skills: "
        "Takmahl Technology 5 (best: Pilot Alpha at 3)",
    )
    assert (watched.decision, watched.decision_reason) == ("watch", "Watch")

    _service([_alpha(), skilled])._phase_4_skill_gate([buildable], _phase1())
    assert (buildable.decision, buildable.pipeline_stage) == ("build", "manufacturing")


def test_the_decision_gate_with_no_industry_pilot():
    decision = _decision(type_id=ALGID, decision_reason="Build", overview_row=_row(ALGID, ALGID_BP))
    _service([{**_alpha(), "industry_enabled": False}])._phase_4_skill_gate([decision], _phase1())
    assert (decision.decision, decision.decision_reason) == ("skip", "Build; not built: no industry pilot")


# ── the decision gate and blueprint ownership ────────────────────────────────

OWNED_BPC, CORP_BPO = 990000000001, 990000000002


def _owned_bpc(owner):
    return {"item_id": OWNED_BPC, "type_id": ALGID_BP, "character_id": owner, "blueprint_runs": 2}


def _corp_bpo():
    return {"item_id": CORP_BPO, "type_id": ALGID_BP}


def _gate(chars, phase1):
    decision = _decision(type_id=ALGID, decision_reason="Build", overview_row=_row(ALGID, ALGID_BP))
    _service(chars)._phase_4_skill_gate([decision], phase1)
    return decision


def test_a_row_only_a_pilot_owned_blueprint_builds_is_blocked_when_its_owner_lacks_the_skills():
    """Its only blueprint is Pilot Alpha's own BPC, which the ledger binds to
    Pilot Alpha (Task A): Skilled could build the item but cannot install Pilot Alpha's
    blueprint, so the row is not a build."""
    skilled = _pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR))
    decision = _gate([_alpha(), skilled], _phase1(bpcs={ALGID_BP: [_owned_bpc(1)]}))
    assert (decision.decision, decision.pipeline_stage, decision.decision_reason) == (
        "skip", "blocked", "Build; not built: owner Pilot Alpha lacks the skills: Energy Weapon Rigging 4 (at 2)",
    )


def test_a_corp_blueprint_lets_any_pilot_with_the_skills_build():
    skilled = _pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR))
    decision = _gate([_alpha(), skilled], _phase1(bpos={ALGID_BP: [_corp_bpo()]}, bpcs={ALGID_BP: [_owned_bpc(1)]}))
    assert (decision.decision, decision.decision_reason) == ("build", "Build")


def test_a_corp_blueprint_in_an_active_job_does_not_count():
    skilled = _pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR))
    decision = _gate([_alpha(), skilled], _phase1(
        bpos={ALGID_BP: [_corp_bpo()]}, bpcs={ALGID_BP: [_owned_bpc(1)]}, busy={CORP_BPO},
    ))
    assert decision.decision == "skip"
    assert "not built: owner Pilot Alpha lacks the skills" in decision.decision_reason


def test_an_owner_with_the_skills_builds_from_its_own_blueprint():
    skilled = _pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR))
    decision = _gate([_alpha(), skilled], _phase1(bpcs={ALGID_BP: [_owned_bpc(3)]}))
    assert decision.decision == "build"


def test_an_owner_outside_industry_or_unloaded_is_named():
    skilled = _pilot(3, "Skilled", _skill("Energy Weapon Rigging", 4, EWR))
    excluded = _gate([{**_alpha(), "industry_enabled": False}, skilled], _phase1(bpcs={ALGID_BP: [_owned_bpc(1)]}))
    assert excluded.decision_reason == "Build; not built: owner character 1 is not an industry pilot"
    unloaded = _gate([{"character_id": 1, "character_name": "Pilot Alpha"}, skilled],
                     _phase1(bpcs={ALGID_BP: [_owned_bpc(1)]}))
    assert unloaded.decision_reason == "Build; not built: owner Pilot Alpha's skills are not loaded"


# ── invention through the skill gate ─────────────────────────────────────────
# The invention activity lives on the T1 source blueprint (fake id 990021);
# its SDE skills and every pilot's levels are fake.

T1_SOURCE, HEP, ENC = 990021, 11433, 21790
INVENTION_BLUEPRINTS = {
    T1_SOURCE: blueprint(invention=[skill(HEP, "High Energy Physics", 3), skill(ENC, "Amarr Encryption Methods", 1)],
                         copying=[], manufacturing=[]),
    ALGID_BP: blueprint(manufacturing=[]),
}


def _inventor(char_id, name, hep, *, loaded=True):
    if not loaded:
        return {"character_id": char_id, "character_name": name}
    return _char(char_id, name, [_skill("Laboratory Operation", 5), _skill("Science", 5, SCIENCE),
                                 _skill("High Energy Physics", hep, HEP), _skill("Amarr Encryption Methods", 1, ENC)])


def _invention_row(**extra):
    return _decision(type_id=ALGID, decision_reason="Build", pipeline_stage="invention", overview_row={
        "type_id": ALGID, "needs_invention": True, "t1_blueprint_type_id": T1_SOURCE,
        "manufacturing_job": {"runs": 1, "blueprint_sde": {"blueprint_type_id": ALGID_BP}}, **extra,
    })


def _invents(decision, chars):
    actions = _assign([decision], chars, blueprint_data=INVENTION_BLUEPRINTS)
    return [(a.action_type, a.character_id) for a in actions if a.action_type == "invent"]


def test_a_row_without_a_skill_set_invents_only_with_the_sde_invention_skills():
    """No producer skill set: the generic science ranking used to pick any
    pilot with a research slot. Now only one with the SDE skills."""
    decision = _invention_row()
    assert _invents(decision, [_inventor(5, "Lab", hep=1)]) == []
    assert ("no invention today: no pilot has the skills: High Energy Physics 3 (best: Lab at 1)"
            in decision.decision_reason)
    assert _invents(_invention_row(), [_inventor(5, "Lab", hep=1), _inventor(6, "Able", hep=3)]) == [("invent", 6)]


def test_a_pilot_whose_skills_are_not_loaded_never_invents():
    decision = _invention_row()
    assert _invents(decision, [_inventor(5, "Unloaded", 0, loaded=False)]) == []
    assert ("no invention today: no pilot has the skills: High Energy Physics 3, Amarr Encryption Methods 1; "
            "skills not loaded for Unloaded") in decision.decision_reason


def test_an_owner_bound_bpc_whose_owner_lacks_the_invention_skills():
    owned = 990000000021
    decision = _invention_row(t1_bpc_runs_by_item=[[owned, 10]], t1_bpc_runs_on_hand=10,
                              blueprint_owner_by_item={owned: 5}, ledger_claims={"invention": [owned]})
    assert _invents(decision, [_inventor(5, "Owner", hep=1), _inventor(6, "Able", hep=3)]) == []
    assert ("no invention today: owner Owner lacks the skills: High Energy Physics 3 (at 1)"
            in decision.decision_reason)
