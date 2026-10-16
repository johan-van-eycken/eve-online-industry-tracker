"""Task 15 fix round 4: order-independent merged install fees, the merged
decision keeps its upper-bound flag, and T1 BPCs a row cannot invent from
today go to a lower row of the same source. Fake rows and pilots only."""
from __future__ import annotations

import pytest

import test_chain_planner_keying as ck
import test_daily_planner_assigned_inventor as a4
from eve_online_industry_tracker.application.daily_planner import chain_planner as cp
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision


# --- 1. a merged component fee: any requester unknown -> unknown, in any order ------

def _two_parent_plan(fee_a, fee_b, *, b_first):
    ra = ck._row(type_id=12345, materials=ck._batch_materials(fee=fee_a, **{"54321": 100}))
    rb = ck._row(type_id=12346, blueprint_type_id=1000, materials=ck._batch_materials(fee=fee_b, **{"54321": 100}))
    da = ck._decision(ra, type_name="Alpha")
    db = ck._decision(rb, type_name="Beta")
    decisions = [db, da] if b_first else [da, db]
    plan = ck._planner().plan_chain(decisions, ck._limited_sub_phase1(7))
    subs = [d for d in plan.decisions if d.is_sub_component]
    reasons = {
        d.type_name: (d.overview_row.get("component_buy_reasons") or {}).get(54321)
        for d in plan.decisions if not d.is_sub_component
    }
    return subs, reasons


@pytest.mark.parametrize("b_first", [False, True])
def test_one_unknown_requester_fee_buys_the_component_in_either_order(b_first):
    """Alpha publishes no fee for the component, Beta publishes 0.0. One sub
    job serves both, so its fee is unknown: bought, whatever the parent
    order. Only Alpha is told its fee is unknown; Beta is told why too."""
    subs, reasons = _two_parent_plan(None, 0.0, b_first=b_first)
    assert subs == []
    assert reasons == {
        "Alpha": "component Widget bought: its install fee is unknown for Alpha's structure "
                 "(no fee published; refresh the product overview)",
        "Beta": "component Widget bought: one sub job serves every requester, and its install fee "
                "is unknown for Alpha's structure",
    }


def test_both_orders_give_the_same_outcome():
    assert _two_parent_plan(None, 0.0, b_first=False) == _two_parent_plan(None, 0.0, b_first=True)


@pytest.mark.parametrize("b_first", [False, True])
def test_known_but_different_requester_fees_take_the_higher_in_either_order(b_first):
    """1.0 and 2.0 ISK per run: the merged job is priced at 2.0 x 20 runs,
    so no parent's share is under-priced."""
    subs, _reasons = _two_parent_plan(1.0, 2.0, b_first=b_first)
    (sub,) = subs
    assert sub.overview_row["sub_install_fee"] == 40.0


def test_merge_names_every_parent_whose_fee_is_unknown():
    merged = cp.ChainPlanner._merge_sub_requests([
        (1, [{"type_id": 5, "quantity": 10, "parent_type_name": "One",
              "parent_fee_per_run": None, "parent_fee_unknown_reason": "no index"}]),
        (2, [{"type_id": 5, "quantity": 10, "parent_type_name": "Two",
              "parent_fee_per_run": 3.0, "parent_fee_unknown_reason": None}]),
        (3, [{"type_id": 5, "quantity": 10, "parent_type_name": "Three",
              "parent_fee_per_run": None, "parent_fee_unknown_reason": None}]),
    ])[5]
    assert merged["quantity"] == 30
    assert merged["parent_fee_per_run"] is None
    assert merged["parent_fee_unknown_by_parent"] == {1: ("One", "no index"), 3: ("Three", None)}


# --- 2. the merged decision keeps the product's upper-bound flag --------------------

def _d(type_id, decision, *, sub=False, ub=False, iskh=0.0):
    return ItemDecision(
        type_id=type_id, type_name="X", decision=decision, decision_reason="r",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=iskh, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=1,
        is_sub_component=sub, profit_is_upper_bound=ub, pipeline_stage="manufacturing",
    )


def test_the_merged_decision_keeps_profit_is_upper_bound():
    (merged,) = cp._one_decision_per_type([_d(54321, "watch", ub=True, iskh=9e6)], [_d(54321, "build", sub=True)])
    assert merged.isk_per_hour == 9e6
    assert merged.profit_is_upper_bound is True


# --- 3. BPCs of a row that gets no invent action go to a lower row ------------------

_OTHER_ENCRYPTION = 99_001


def _t2_row(type_id, encryption_skill, bpcs):
    row = a4._row(inventor=1, best_factor=1.0, decryptor=False, multiplier=1.0)
    row["type_id"] = type_id
    row["manufacturing_job"]["invention_chance"]["encryption_skill_type_id"] = encryption_skill
    row.update({"t1_blueprint_type_id": 998, "t1_bpc_runs_by_item": bpcs,
                "t1_bpc_runs_on_hand": sum(r for _i, r in bpcs)})
    return row


def _other_pilot(*, lab_slots=0):
    skills = [a4._skill("Science A", 3, a4.SCIENCE_A), a4._skill("Science B", 3, a4.SCIENCE_B),
              a4._skill("Other Encryption", 3, _OTHER_ENCRYPTION)]
    if lab_slots:
        skills.append(a4._skill("Laboratory Operation", lab_slots, 3406))
    return {"character_id": 1, "character_name": "Other", "skills": {"skills": skills}}


def _copying_low_row():
    """A lower row of the same T1 source as ChainPlanner leaves it when the
    higher row claimed every T1 BPC: it copies its own T1 BPO, no BPC."""
    row = _t2_row(12346, _OTHER_ENCRYPTION, [])
    row.update({"has_t1_bpo": True, "t1_bpo_count": 1, "t1_bpo_item_ids": [801], "t1_max_runs_per_copy": 10})
    return a4._decision(row, stage="copying", type_id=12346, score=1.0)


def test_bpcs_of_a_row_without_a_qualifying_pilot_go_to_the_next_row_of_the_source():
    """The free pilot lacks the higher row's encryption skill, so it invents
    nothing there; the lower row invents from that BPC (10 runs cover its
    attempts) and its copy is not needed."""
    high = a4._decision(_t2_row(12345, a4.ENCRYPTION, [[701, 10]]), type_id=12345, score=2.0)
    low = _copying_low_row()
    actions = CharacterAssigner().assign(
        ChainPlan(decisions=[high, low]), [], a4._Chars([_other_pilot(lab_slots=1)]), a4._Admin(),
        price_map=a4.PRICES,
    )
    assert [(a.action_type, a.type_id, a.character_id, a.blueprint_item_id) for a in actions] == [
        ("invent", 12346, 1, 701),
    ]
    assert (
        "; no invention today: no free pilot with the invention skills; "
        "its T1 BPC(s) 701 go to Widget II (12346);"
    ) in high.decision_reason


def test_a_row_that_invents_keeps_its_bpcs():
    """Control: when the higher row invents, the lower row only copies."""
    high = a4._decision(_t2_row(12345, _OTHER_ENCRYPTION, [[701, 10]]), type_id=12345, score=2.0)
    low = _copying_low_row()
    actions = CharacterAssigner().assign(
        ChainPlan(decisions=[high, low]), [], a4._Chars([_other_pilot(lab_slots=1)]), a4._Admin(),
        price_map=a4.PRICES,
    )
    assert [(a.action_type, a.type_id, a.blueprint_item_id) for a in actions] == [
        ("invent", 12345, 701), ("copy", 998, None),
    ]


# --- Task 15 fix round 5: the release note says what happened to each BPC ----------

def _assign_rows(*decisions, lab_slots=1):
    return CharacterAssigner().assign(
        ChainPlan(decisions=list(decisions)), [], a4._Chars([_other_pilot(lab_slots=lab_slots)]), a4._Admin(),
        price_map=a4.PRICES,
    )


def test_released_bpcs_name_the_row_that_takes_them():
    high = a4._decision(_t2_row(12345, a4.ENCRYPTION, [[701, 10]]), type_id=12345, score=2.0)
    _assign_rows(high, _copying_low_row())
    assert ("; no invention today: no free pilot with the invention skills; "
            "its T1 BPC(s) 701 go to Widget II (12346);") in high.decision_reason
    assert "lower row of the same source" not in high.decision_reason


def test_released_bpcs_nobody_takes_stay_unused_today():
    high = a4._decision(_t2_row(12345, a4.ENCRYPTION, [[701, 10]]), type_id=12345, score=2.0)
    _assign_rows(high)
    assert ("; no invention today: no free pilot with the invention skills; "
            "its T1 BPC(s) 701 stay unused today;") in high.decision_reason
    assert "go to" not in high.decision_reason


def test_released_bpcs_partly_taken_say_both():
    """One research slot: the lower row runs one lane, on 701 (the tie goes
    to the lower key); 702 stays unused."""
    high = a4._decision(_t2_row(12345, a4.ENCRYPTION, [[701, 10], [702, 10]]), type_id=12345, score=2.0)
    actions = _assign_rows(high, _copying_low_row(), lab_slots=0)
    assert [(a.action_type, a.type_id, a.blueprint_item_id) for a in actions] == [("invent", 12346, 701)]
    assert ("; no invention today: no free pilot with the invention skills; "
            "its T1 BPC(s) 701 go to Widget II (12346); its T1 BPC(s) 702 stay unused today;") in high.decision_reason


def test_a_row_not_built_for_the_releasing_rows_bpcs_says_that_row_does_not_invent():
    high = a4._decision(_t2_row(12345, a4.ENCRYPTION, [[701, 10]]), type_id=12345, score=2.0)
    low = a4._decision(a4._row(), type_id=12346, score=1.0, stage="invention")
    low.overview_row = {"type_id": 12346, "manufacturing_job": {"runs": 20}}
    cp.ChainPlanner._mark_not_built(
        low, "T1 BPCs (998) used by invention for Widget II (12345) today and no T1 BPO to copy: "
             "no invention in this plan",
    )
    _assign_rows(high, low)
    assert low.decision_reason == (
        "test; not built: T1 BPCs (998) used by invention for Widget II (12345) "
        "(that row does not invent today) today and no T1 BPO to copy: no invention in this plan"
    )
