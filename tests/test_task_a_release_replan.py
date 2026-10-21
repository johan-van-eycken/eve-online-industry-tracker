"""Task A items 2, 3 and 6: blueprints a row claimed but does not use today
go to the rows that wanted them, within the same compute.

3. A row with fewer invent lanes than claimed T1 BPCs releases the rest to
   the lower rows of the same T1 source.
2. A lower row ChainPlanner marked not built because a higher row held its
   T1 BPCs is re-planned when the assigner releases them.
6. A BPO claimed for a job that gets no slot is released, and the rows that
   wanted it are re-planned by the same pass.

Fake ids, pilots and numbers throughout.
"""
from __future__ import annotations

import copy
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import test_blueprint_ledger as L  # noqa: E402
import test_daily_planner_assigned_inventor as A  # noqa: E402

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan  # noqa: E402
from planner_skill_fakes import NO_SKILL_REQUIREMENTS

OTHER_ENCRYPTION = 99_001


def _pilot(character_id, name, encryption, *, lab=0, mass=0):
    skills = [A._skill("Science A", 3, A.SCIENCE_A), A._skill("Science B", 3, A.SCIENCE_B),
              A._skill("Encryption", 3, encryption)]
    if lab:
        skills.append(A._skill("Laboratory Operation", lab, 3406))
    if mass:
        skills.append(A._skill("Mass Production", mass, 3387))
    return {"character_id": character_id, "character_name": name, "skills": {"skills": skills}}


def _t2_row(type_id, encryption, bpcs):
    row = A._row(inventor=1, best_factor=1.0, decryptor=False, multiplier=1.0)
    row["type_id"] = type_id
    row["manufacturing_job"]["invention_chance"]["encryption_skill_type_id"] = encryption
    row.update({"t1_blueprint_type_id": 998, "t1_bpc_runs_by_item": bpcs,
                "t1_bpc_runs_on_hand": sum(r for _i, r in bpcs)})
    return row


def _assign(decisions, chars, jobs=(), skill_data=NO_SKILL_REQUIREMENTS):
    plan = decisions if isinstance(decisions, ChainPlan) else ChainPlan(decisions=list(decisions))
    return CharacterAssigner().assign(plan, list(jobs), A._Chars(chars), A._Admin(), price_map=A.PRICES,
                                      blueprint_data=skill_data)


# --- item 3: fewer lanes than claimed BPCs -----------------------------------------

def test_bpcs_beyond_a_rows_lanes_go_to_a_lower_row_of_the_same_source():
    """The higher row has two 10-run BPCs but one lane (only Hi has its
    encryption skill, one slot): it invents on 701; 702 goes to the lower
    row, which Lo invents from."""
    high = A._decision(_t2_row(12345, A.ENCRYPTION, [[701, 10], [702, 10]]), type_id=12345, score=2.0)
    low = A._decision(_t2_row(12346, OTHER_ENCRYPTION, []), type_id=12346, score=1.0)
    actions = _assign([high, low], [_pilot(1, "Hi", A.ENCRYPTION), _pilot(2, "Lo", OTHER_ENCRYPTION)])
    assert [(a.action_type, a.type_id, a.character_id, a.blueprint_item_id, a.runs) for a in actions] == [
        ("invent", 12345, 1, 701, 7), ("invent", 12346, 2, 702, 6),
    ]
    assert "; fewer invent lanes than T1 BPCs today: its T1 BPC(s) 702 go to Widget II (12346)" in high.decision_reason


def test_bpcs_beyond_a_rows_lanes_nobody_takes_stay_unused_today():
    high = A._decision(_t2_row(12345, A.ENCRYPTION, [[701, 10], [702, 10]]), type_id=12345, score=2.0)
    actions = _assign([high], [_pilot(1, "Hi", A.ENCRYPTION)])
    assert [(a.action_type, a.blueprint_item_id) for a in actions] == [("invent", 701)]
    assert "; fewer invent lanes than T1 BPCs today: its T1 BPC(s) 702 stay unused today" in high.decision_reason


def test_a_row_that_uses_every_bpc_releases_nothing():
    high = A._decision(_t2_row(12345, A.ENCRYPTION, [[701, 10], [702, 10]]), type_id=12345, score=2.0)
    actions = _assign([high], [_pilot(1, "Hi", A.ENCRYPTION, lab=1)])
    assert sorted(a.blueprint_item_id for a in actions) == [701, 702]
    assert "fewer invent lanes" not in high.decision_reason


# --- item 2: a row not built for BPCs a higher row released is re-planned ---------

def _planned_t2(type_id, name, score, encryption):
    row = L._t2_row(type_id)
    row["manufacturing_job"]["invention_chance"]["encryption_skill_type_id"] = encryption
    return L._dec(row, type_id, name, score, 2, "invention")


def _plan_and_assign(decisions, bpos, bpcs, chars, jobs=(), blueprints=None, skill_data=NO_SKILL_REQUIREMENTS):
    plan = L._planner(0).plan_chain(decisions, {
        "bpo_assets_by_type_id": bpos, "bpc_assets_by_type_id": bpcs,
        "blueprint_data": blueprints or L.BP_B, "market_depth_cache": A.PRICES, "variant_rows_by_type_id": {},
    })
    return plan, _assign(plan, chars, jobs, skill_data)


def test_a_row_not_built_for_released_bpcs_is_re_planned_and_invents():
    """One T1 BPC (701). ChainPlanner gives it to the higher row, so the lower
    row is not built. Nobody free can run the higher row's invention, so it
    releases 701; the same compute re-plans the lower row, which invents."""
    high = _planned_t2(12345, "Widget II", 2.0, A.ENCRYPTION)
    low = _planned_t2(12346, "Widget II b", 1.0, OTHER_ENCRYPTION)
    plan, actions = _plan_and_assign([high, low], {}, {998: [L._bpc(701, 998, 10)]},
                                     [_pilot(2, "Lo", OTHER_ENCRYPTION)])
    assert [(a.action_type, a.type_id, a.character_id, a.blueprint_item_id) for a in actions] == [
        ("invent", 12346, 2, 701),
    ]
    assert low.pipeline_stage == "invention"
    assert "not built:" not in low.decision_reason
    assert ("; re-planned: T1 BPC(s) 701 released by invention for Widget II (12345), "
            "which does not use them today") in low.decision_reason
    assert "its T1 BPC(s) 701 go to Widget II b (12346)" in high.decision_reason
    assert low.overview_row["t1_bpc_runs_by_item"] == [[701, 10]]


def test_only_one_re_planned_row_gets_a_released_bpc():
    """Two lower rows want the one released BPC: the higher of them gets it;
    the other stays not built and names the row that holds it now."""
    high = _planned_t2(12345, "Widget II", 3.0, A.ENCRYPTION)
    mid = _planned_t2(12346, "Widget II b", 2.0, OTHER_ENCRYPTION)
    low = _planned_t2(12347, "Widget II c", 1.0, OTHER_ENCRYPTION)
    _plan, actions = _plan_and_assign([high, mid, low], {}, {998: [L._bpc(701, 998, 10)]},
                                      [_pilot(2, "Lo", OTHER_ENCRYPTION, lab=1)])
    assert [(a.action_type, a.type_id, a.blueprint_item_id) for a in actions] == [("invent", 12346, 701)]
    assert low.pipeline_stage == "blocked"
    assert ("not built: T1 BPCs (998) used by invention for Widget II b (12346) today and no T1 BPO "
            "to copy: no invention in this plan") in low.decision_reason
    assert "Widget II (12345)" not in low.decision_reason


def test_a_hand_built_plan_without_the_planner_is_not_re_planned():
    """Control: a ChainPlan with no re-plan callback keeps the planner's
    not-built rows as they are (the Task 15 wording)."""
    high = A._decision(_t2_row(12345, A.ENCRYPTION, [[701, 10]]), type_id=12345, score=2.0)
    low = A._decision(A._row(), type_id=12346, score=1.0, stage="invention")
    low.overview_row = {"type_id": 12346, "manufacturing_job": {"runs": 20}}
    ChainPlanner._mark_not_built(low, "T1 BPCs (998) used by invention for Widget II (12345) today")
    actions = _assign([high, low], [_pilot(2, "Lo", OTHER_ENCRYPTION)])
    assert actions == []
    assert "(which ended up not inventing)" in low.decision_reason


# --- item 6: a BPO claimed for a job without a slot goes to the row that wanted it --

def _busy(character_id, activity_id):
    from datetime import datetime, timedelta, timezone

    end = datetime.now(tz=timezone.utc).replace(tzinfo=None) + timedelta(days=1)
    return {"character_id": character_id, "activity_id": activity_id, "status": "active", "end_date": end}


def test_a_t1_bpo_whose_manufacture_gets_no_slot_is_copied_by_the_t2_row():
    """Widget I (higher) claims the one T1 BPO 77 to build; the T2 row is
    not built for it. No manufacturing slot is free (the pilot's one slot
    is in a job), so the BPO is idle: released, and the T2 row copies it."""
    t1 = L._dec(L._t1_row(20), 12344, "Widget I", 2.0, 1)
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 1.0, 2, "invention")
    _plan, actions = _plan_and_assign([t1, t2], {998: [L._bpo(77, 998)]}, {},
                                      [A._best_inventor(), A._copier()], jobs=[_busy(A.BEST_ID, 1), _busy(A.COPIER_ID, 1)])
    assert [(a.action_type, a.type_id) for a in actions] == [("copy", 998)]
    assert t2.pipeline_stage == "copying"
    assert t2.overview_row["t1_bpo_item_ids"] == [77]
    assert "not built:" not in t2.decision_reason
    assert ("; re-planned: T1 BPO(s) 77 released by manufacture of Widget I (12344), "
            "which does not use them today") in t2.decision_reason


def test_a_t1_bpo_whose_copy_gets_no_slot_builds_the_t1_row():
    """Probe b reversed: the T2 row (higher) claims BPO 77 to copy, the T1
    row is not built. No research slot is free, so the copy is not made:
    the BPO is released and the T1 row builds from it, buying its inputs."""
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    t1 = L._dec(L._t1_row(20), 12344, "Widget I", 1.0, 1)
    _plan, actions = _plan_and_assign([t2, t1], {998: [L._bpo(77, 998)]}, {},
                                      [A._best_inventor()], jobs=[_busy(A.BEST_ID, 8)])
    assert [(a.action_type, a.type_id, a.runs, a.materials) for a in actions] == [
        ("manufacture", 12344, 10, {34: 50}),
    ]
    assert t1.pipeline_stage == "manufacturing"
    assert "not built:" not in t1.decision_reason
    assert ("; re-planned: its BPO (item 77) released by copy for Widget II (12345), "
            "which does not use it today") in t1.decision_reason
    assert L._shop(actions, A.PRICES, L.BP_B) == {34: 50}


def test_a_used_bpo_is_not_released():
    """Control (probe b): the copy gets a slot, so the T1 row stays not built."""
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    t1 = L._dec(L._t1_row(20), 12344, "Widget I", 1.0, 1)
    _plan, actions = _plan_and_assign([t2, t1], {998: [L._bpo(77, 998)]}, {}, L._chars())
    assert [a.action_type for a in actions] == ["copy"]
    assert t1.pipeline_stage == "blocked"


def test_a_re_planned_invention_is_bought_like_the_same_row_planned_alone():
    """The re-planned row's invent action and its purchase, datacores
    included (blueprint 1999 also makes 12346 here), equal those of the
    same row planned with the BPC free from the start."""
    bp = copy.deepcopy(L.BP_B)
    bp[1999]["manufacturing"]["products"] = [{"type_id": 12345, "quantity": 1}, {"type_id": 12346, "quantity": 1}]
    high = _planned_t2(12345, "Widget II", 2.0, A.ENCRYPTION)
    low = _planned_t2(12346, "Widget II b", 1.0, OTHER_ENCRYPTION)
    _plan, actions = _plan_and_assign([high, low], {}, {998: [L._bpc(701, 998, 10)]},
                                      [_pilot(2, "Lo", OTHER_ENCRYPTION)], blueprints=bp)
    alone = _planned_t2(12346, "Widget II b", 1.0, OTHER_ENCRYPTION)
    _plan2, alone_actions = _plan_and_assign([alone], {}, {998: [L._bpc(701, 998, 10)]},
                                             [_pilot(2, "Lo", OTHER_ENCRYPTION)], blueprints=bp)
    (invent,), (alone_invent,) = actions, alone_actions
    assert (invent.runs, invent.blueprint_item_id, invent.estimated_job_fee_isk) == (5, 701, 20.0)
    assert (invent.runs, invent.estimated_job_fee_isk) == (alone_invent.runs, alone_invent.estimated_job_fee_isk)
    shop = L._shop(actions, A.PRICES, bp)
    assert shop == L._shop(alone_actions, A.PRICES, bp)
    # 2 + 2 datacores per attempt, 5 attempts.
    assert (shop[A.DATACORE_A], shop[A.DATACORE_B]) == (10, 10)


def test_a_re_plan_that_still_gets_no_slot_keeps_the_not_built_reason():
    """The released BPC is free, but nobody can run the lower row's
    invention either: the re-plan is undone, the reason is the planner's."""
    high = _planned_t2(12345, "Widget II", 2.0, A.ENCRYPTION)
    low = _planned_t2(12346, "Widget II b", 1.0, OTHER_ENCRYPTION)
    _plan, actions = _plan_and_assign([high, low], {}, {998: [L._bpc(701, 998, 10)]},
                                      [_pilot(3, "Nobody", 99_002)])
    assert actions == []
    assert low.pipeline_stage == "blocked"
    assert "re-planned" not in low.decision_reason
    # Fix round 1 (I3): worded from the final ledger, with the assigner's cause.
    assert low.decision_reason == (
        "test; not built: T1 BPCs (998) free (its holder does not use it today), "
        "but no free pilot with the invention skills"
    )
    assert "its T1 BPC(s) 701 stay unused today" in high.decision_reason


# --- an undone re-plan keeps the assigner's skill reason ----------------------------

def _skill_data(**required):
    """The SDE skills of the T1 (998) and T2 (1999) blueprints: none, but
    the given activities of 998 (fake levels)."""
    from planner_skill_fakes import blueprint, skill

    activities = ("manufacturing", "copying", "invention", "research_material", "research_time")
    return {
        998: blueprint(**{a: [skill(*required[a])] if a in required else [] for a in activities}),
        1999: blueprint(**{a: [] for a in activities}),
    }


def test_a_re_planned_manufacture_no_pilot_has_the_skills_for_says_so():
    """The T1 row gets the released BPO 77 (as above), but no pilot has its
    manufacturing skills: the re-plan is undone with that reason, not "no
    free manufacturing slot"."""
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    t1 = L._dec(L._t1_row(20), 12344, "Widget I", 1.0, 1)
    _plan, actions = _plan_and_assign([t2, t1], {998: [L._bpo(77, 998)]}, {},
                                      [A._best_inventor()], jobs=[_busy(A.BEST_ID, 8)],
                                      skill_data=_skill_data(manufacturing=(3380, "Industry", 5)))
    assert actions == []
    assert t1.pipeline_stage == "blocked"
    assert "no free manufacturing slot" not in t1.decision_reason
    assert ("BPO free (its holder does not use it today), but no pilot has the skills: "
            "Industry 5 (best: Best at 0)") in t1.decision_reason


def test_a_re_planned_copy_no_pilot_has_the_copying_skills_for_says_so():
    """The T2 row gets the released BPO 77 to copy (as above), but no pilot
    has the copying skills: the re-plan is undone with that reason."""
    t1 = L._dec(L._t1_row(20), 12344, "Widget I", 2.0, 1)
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 1.0, 2, "invention")
    _plan, actions = _plan_and_assign([t1, t2], {998: [L._bpo(77, 998)]}, {},
                                      [A._best_inventor(), A._copier()],
                                      jobs=[_busy(A.BEST_ID, 1), _busy(A.COPIER_ID, 1)],
                                      skill_data=_skill_data(copying=(3402, "Science", 5)))
    assert actions == []
    assert t2.pipeline_stage == "blocked"
    assert "no free research slot for the copy" not in t2.decision_reason
    assert ("T1 BPO (998) free (its holder does not use it today), but no copy of its T1 BPO (998) today: "
            "no pilot has the skills: Science 5 (best: Best at 0)") in t2.decision_reason
