"""Task A fix round 1: the review's probes, pinned. Fake ids and numbers.

I1 one blueprint item in both the corp and a pilot's assets is booked once,
   as the pilot's (owner-bound), with a WARNING.
I2 a parent refused by owner binding gets no sub-builds either.
I3 a re-plan that is undone says why from the current ledger.
M1 owner-only invention falls back to the best (unloaded) owner.
M3 a releasing row says where its blueprint went.
M4 a later pass names a release of an earlier pass.
M6 the ledger indexes its keys.
"""
from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import test_blueprint_ledger as L  # noqa: E402
import test_daily_planner_assigned_inventor as A  # noqa: E402
import test_task_a_character_blueprints as C  # noqa: E402
import test_task_a_release_replan as T  # noqa: E402

from eve_online_industry_tracker.application.daily_planner.blueprint_ledger import BlueprintLedger  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.service import merge_blueprint_indexes  # noqa: E402
from planner_skill_fakes import NO_SKILL_REQUIREMENTS


# --- I1 ------------------------------------------------------------------------------

def test_an_item_in_both_indexes_is_kept_once_as_the_pilots(caplog):
    corp_bpo, corp_bpc = L._bpo(77, 998), L._bpc(701, 998, 10)
    char_bpo, char_bpc = C._owned_bpo(77, 998), C._owned_bpc(701, 998, 10)
    with caplog.at_level(logging.WARNING):
        bpos, bpcs = merge_blueprint_indexes(({998: [corp_bpo, L._bpo(78, 998)]}, {998: [corp_bpc]}),
                                             ({998: [char_bpo]}, {998: [char_bpc]}))
    assert [b.item_id for b in bpos[998]] == [78, 77]
    assert bpos[998][1] is char_bpo and bpcs[998] == [char_bpc]
    assert any("77, 701" in r.getMessage() and r.levelno == logging.WARNING for r in caplog.records)


def test_the_ledger_books_a_repeated_item_once_and_keeps_its_owner():
    ledger = BlueprintLedger({998: [L._bpo(77, 998), C._owned_bpo(77, 998)]},
                             {998: [L._bpc(701, 998, 10), C._owned_bpc(701, 998, 10)]})
    assert [k for k, _a in ledger.free_bpos(998)] == [77]
    assert ledger.free_bpcs(998) == [(701, 701, 10)]
    assert (ledger.owner_of(77), ledger.owner_of(701)) == (C.OWNER_ID, C.OWNER_ID)
    assert ledger.type_of(701) == ("bpc", 998) and ledger.type_of(5) is None


def test_a_stale_duplicate_bpc_gives_one_invent_job_by_its_owner():
    row = L._t2_row()
    row["manufacturing_job"]["activity_breakdown"]["invention"]["whole_attempts"] = 20
    t2 = L._dec(row, 12345, "Widget II", 2.0, 2, "invention")
    bpos, bpcs = merge_blueprint_indexes(({}, {998: [L._bpc(701, 998, 10)]}),
                                         ({}, {998: [C._owned_bpc(701, 998, 10, owner=A.BEST_ID)]}))
    _plan, actions = L._run([t2], bpos, bpcs)
    assert [(a.action_type, a.character_id, a.blueprint_item_id) for a in actions] == [("invent", A.BEST_ID, 701)]


# --- I2 ------------------------------------------------------------------------------

def _thing_plan(bpos, bpcs, parent_row):
    parent = L._dec(parent_row, 12345, "Thing", 2.0, 1)
    plan = L._planner().plan_chain([parent], {
        "bpo_assets_by_type_id": bpos, "bpc_assets_by_type_id": bpcs,
        "blueprint_data": L.R.BLUEPRINT_DATA, "market_depth_cache": L.R.MARKET, "variant_rows_by_type_id": {},
    })
    return parent, plan


def test_no_sub_build_for_a_parent_whose_owner_is_busy():
    parent, plan = _thing_plan({999: [C._owned_bpo(1, 999)], 888: [L._bpo(2, 888)]}, {}, L.R._row(**L.R.BPO_ROW))
    assert [d.type_id for d in plan.decisions if d.is_sub_component] == [54321]
    actions = CharacterAssigner().assign(plan, [C._busy(C.OWNER_ID, 1)], A._Chars([C._other(), C._owner()]),
                                         A._Admin(), price_map=L.R.MARKET, blueprint_data=NO_SKILL_REQUIREMENTS)
    assert actions == []
    assert [d.type_id for d in plan.decisions if d.is_sub_component] == []
    assert "no manufacture today: owner Owner has no free slot" in parent.decision_reason
    assert L._shop(actions, L.R.MARKET, L.R.BLUEPRINT_DATA) == {}


def test_no_sub_build_for_a_parent_whose_blueprints_have_several_owners():
    segments = [
        {"runs": 5, "source": "owned_blueprint_copy", "blueprint_item_id": 78, "materials": {"54321": 500}},
        {"runs": 5, "source": "owned_blueprint_copy", "blueprint_item_id": 79, "materials": {"54321": 500}},
    ]
    row = L.R._row(segments=segments, kind="owned_blueprint_copy")
    parent, plan = _thing_plan({888: [L._bpo(2, 888)]},
                               {999: [C._owned_bpc(78, 999, 5), C._owned_bpc(79, 999, 5, owner=C.OTHER_ID)]}, row)
    assert [d.type_id for d in plan.decisions if d.is_sub_component] == [54321]
    actions = CharacterAssigner().assign(plan, [], A._Chars([C._other(), C._owner()]), A._Admin(),
                                         price_map=L.R.MARKET, blueprint_data=NO_SKILL_REQUIREMENTS)
    assert actions == []
    assert [d.type_id for d in plan.decisions if d.is_sub_component] == []
    assert "its blueprints belong to several pilots (Owner, Other)" in parent.decision_reason


def test_a_parent_that_builds_keeps_its_sub_build():
    parent, plan = _thing_plan({999: [C._owned_bpo(1, 999)], 888: [L._bpo(2, 888)]}, {}, L.R._row(**L.R.BPO_ROW))
    actions = CharacterAssigner().assign(plan, [], A._Chars([C._other(), C._owner()]), A._Admin(),
                                         price_map=L.R.MARKET, blueprint_data=NO_SKILL_REQUIREMENTS)
    assert [(a.action_type, a.type_id, a.character_id, a.runs) for a in actions] == [
        ("manufacture", 12345, C.OWNER_ID, 10), ("sub_manufacture", 54321, C.OTHER_ID, 100),
    ]


# --- I3, M3, M4: the cascade probe --------------------------------------------------

def _cascade(lab):
    """Widget I holds T1 BPO 77 (no manufacturing slot), Widget II holds
    BPC 701 (nobody can run its invention); Widget II b and c are not built
    for them. Lo can invent b and c; `lab` extra research slots."""
    rows = [
        L._dec(L._t1_row(20), 12344, "Widget I", 5.0, 1),
        T._planned_t2(12345, "Widget II", 4.0, A.ENCRYPTION),
        T._planned_t2(12346, "Widget II b", 3.0, T.OTHER_ENCRYPTION),
        T._planned_t2(12347, "Widget II c", 2.0, T.OTHER_ENCRYPTION),
    ]
    plan, actions = T._plan_and_assign(rows, {998: [L._bpo(77, 998)]}, {998: [L._bpc(701, 998, 10)]},
                                       [T._pilot(2, "Lo", T.OTHER_ENCRYPTION, lab=lab)], jobs=[T._busy(2, 1)])
    return rows, actions


def test_an_undone_re_plan_says_why_from_the_current_ledger():
    """One research slot: b invents on 701; c is re-planned to copy 77 but
    no research slot is left, so it is undone. Its reason must not still
    say Widget I uses the BPO."""
    rows, actions = _cascade(lab=0)
    assert [(a.action_type, a.type_id, a.blueprint_item_id) for a in actions] == [("invent", 12346, 701)]
    c = rows[3]
    assert c.pipeline_stage == "blocked"
    assert "used by manufacture of Widget I" not in c.decision_reason
    assert ("; not built: T1 BPO (998) free (its holder does not use it today), "
            "but no free research slot for the copy") in c.decision_reason
    assert "goes to" not in rows[0].decision_reason


def test_a_releasing_row_says_where_its_bpo_went():
    rows, actions = _cascade(lab=1)
    assert [(a.action_type, a.type_id) for a in actions] == [("invent", 12346), ("copy", 998)]
    assert "its BPO 77 goes to copy for Widget II c (12347)" in rows[0].decision_reason


def test_release_history_is_kept_across_passes():
    """Widget II holds T1 BPO 77 to copy, but no research slot is free:
    pass 1 releases 77, Widget II b is re-planned to copy it, gets no
    slot either and is undone. Pass 2 re-plans Widget I, which builds from
    77 (a manufacturing slot is free); its wording still names the pass-1
    release. Widget II b then names Widget I as the holder, Widget II says
    where its BPO went."""
    x = L._dec(L._t2_row(12345), 12345, "Widget II", 3.0, 2, "invention")
    z = L._dec(L._t2_row(12346), 12346, "Widget II b", 2.0, 2, "invention")
    y = L._dec(L._t1_row(20), 12344, "Widget I", 1.0, 1)
    _plan, actions = T._plan_and_assign([x, z, y], {998: [L._bpo(77, 998)]}, {},
                                        [A._best_inventor()], jobs=[T._busy(A.BEST_ID, 8)])
    assert [(a.action_type, a.type_id, a.runs) for a in actions] == [("manufacture", 12344, 10)]
    assert ("re-planned: its BPO (item 77) released by copy for Widget II (12345), "
            "which does not use it today") in y.decision_reason
    assert ("not built: T1 BPO (998) used by manufacture of Widget I (12344) today: "
            "no copy in this plan") in z.decision_reason
    assert "copy for Widget II (12345)" not in z.decision_reason
    assert "its BPO 77 goes to manufacture of Widget I (12344)" in x.decision_reason


# --- M1 ------------------------------------------------------------------------------

def test_an_owner_without_loaded_skills_does_not_invent():
    """Never plan what cannot be executed: the owner whose skills are not
    loaded is no inventor (it used to invent at the SDE base chance), and
    only the owner may invent from its own BPC."""
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    owner = C._owner()
    owner["skills"] = None
    _plan, actions = C._run([t2], {}, {998: [C._owned_bpc(701, 998, 10)]}, [owner, C._other()])
    assert [a for a in actions if a.action_type == "invent"] == []
