# tests/test_daily_planner_blueprint_stock.py
"""Phase 6's blueprint stock pass: leftover research slots copy owned BPOs
below the minimum BPC stock and invent T2 blueprints below the minimum
invented stock. Fake data."""
from __future__ import annotations

import copy

import test_blueprint_ledger as L
import test_daily_planner_assigned_inventor as A
from eve_online_industry_tracker.application.daily_planner.blueprint_stock import (
    StockGap,
    count_in_progress,
    rank_gaps,
    stock_gaps,
)
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision
from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
from eve_online_industry_tracker.infrastructure.sde.blueprints import SDE_ACTIVITIES
from fakes import _Chars

# T1 BPO 998 makes 12344 and invents into T2 blueprint 1999 (makes 12345);
# no activity needs a skill.
BLUEPRINTS = copy.deepcopy(L.BP_B)
for _entry in BLUEPRINTS.values():
    _entry["activity_skills"] = {a: [] for a in SDE_ACTIVITIES}


def _decision(type_id, *, decision="build", crpd=None, margin=10.0, row=None):
    return ItemDecision(
        type_id=type_id, type_name=f"Product {type_id}", decision=decision, decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=margin,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=2, pipeline_stage="invention",
        capital_return_per_day=crpd, overview_row=row if row is not None else {"type_id": type_id},
    )


def _t2_skip_row():
    """The T2 product's overview row, a skip decision: it carries the T1
    source's copy fee (0.5 per run) and the invention plan."""
    return _decision(12345, decision="skip", margin=40.0, row=L._t2_row())


def _t1_row(type_id=12344, *, margin=10.0, copy_fee=0.25):
    """A pure T1 product's row, a skip decision, with its own BPO's copy fee."""
    return _decision(type_id, decision="skip", margin=margin, row={
        "type_id": type_id, "manufacturing_job": {"copy_job_cost_per_run": copy_fee},
    })


def _one_slot_pilot():
    return {"character_id": 9, "character_name": "Solo", "skills": {"skills": []}}


def _stock_pass(decisions, chars, bpcs, *, budget=None, min_bpc=3, min_invented=3, jobs=()):
    assigner = CharacterAssigner()
    actions = assigner.assign(
        ChainPlan(decisions=decisions), list(jobs), _Chars(chars), A._Admin(), dict(A.PRICES),
        blueprint_data=BLUEPRINTS, budget=budget,
        blueprint_stock={
            "bpo_assets_by_type_id": {998: [L._bpo(77, 998)]}, "bpc_assets_by_type_id": bpcs,
            "busy_blueprint_item_ids": set(), "min_bpc": min_bpc, "min_invented": min_invented,
        },
    )
    return assigner, actions


def test_count_in_progress():
    jobs = [
        {"activity_id": 5, "blueprint_type_id": 10, "product_type_id": 10, "runs": 2, "status": "active"},
        {"activity_id": 8, "blueprint_type_id": 10, "product_type_id": 20, "runs": 4, "status": "active"},
        {"activity_id": 8, "blueprint_type_id": 11, "product_type_id": 21, "runs": 4, "status": "active"},
        {"activity_id": 1, "blueprint_type_id": 10, "product_type_id": 30, "runs": 9, "status": "active"},
    ]
    assert count_in_progress(jobs, {20: 0.5}.get) == {10: 2.0, 20: 2.0, 21: 4.0}


def _gaps(bpc_counts, in_progress=None, sources=None):
    return stock_gaps(
        bpo_type_ids=[10], bpc_counts=bpc_counts, in_progress=in_progress or {},
        invention_source_index=sources or {}, min_bpc=3, min_invented=3, product_of={10: 100, 30: 300}.get,
    )


def test_copy_gap_below_minimum_for_an_invention_source():
    gaps = _gaps({10: 1}, {10: 1.0}, sources={30: 10})
    assert StockGap("copy", 10, None, 2.0, 3, 100) in gaps


def test_no_copy_gap_for_a_bpo_that_is_no_invention_source():
    """Its product builds from the BPO itself (ChainPlanner._prefer_free_bpo): no BPCs needed."""
    assert _gaps({10: 1}) == []


def test_no_gap_at_minimum():
    assert _gaps({10: 3}) == []


def test_invent_gap_from_owned_t1():
    gaps = _gaps({10: 3}, sources={30: 10, 31: 99})
    assert gaps == [StockGap("invent", 30, 10, 0.0, 3, 300)]


def test_rank_build_rows_first():
    none, skip, build = (StockGap("copy", bp, None, 0, 3, p) for bp, p in ((1, 1), (2, 2), (3, 3)))
    decisions = {2: _decision(2, decision="skip", margin=40.0), 3: _decision(3, crpd=0.01)}
    assert rank_gaps([none, skip, build], decisions) == [build, skip, none]


def test_stock_pass_fills_free_slots():
    _assigner, actions = _stock_pass(
        [_t2_skip_row()], [A._copier()], {998: [L._bpc(701, 998, 10)]}, min_invented=0,
    )
    (action,) = actions
    assert (action.action_type, action.type_id, action.character_id) == ("copy", 998, A.COPIER_ID)
    # Two copies of the SDE max 30 runs, at the T1 source's 0.5 per run.
    assert (action.quantity, action.runs, action.estimated_job_fee_isk) == (2, 60, 30.0)
    assert "stock: 1 of 3 BPCs" in action.notes


def test_stock_pass_respects_wallet():
    budget = WalletBudget(balance=10.0, reserve_pct=0)
    assigner, actions = _stock_pass([_t2_skip_row()], [A._copier()], {}, budget=budget, min_invented=0)
    assert actions == []
    assert budget.spent == 0
    assert assigner.free_slots_after[A.COPIER_ID]["science"] == 2
    assert assigner.free_slots_after[A.COPIER_ID]["science_reason"] == "wallet"


def test_stock_pass_skips_unknown_fee():
    row = _t2_skip_row()
    del row.overview_row["manufacturing_job"]["activity_breakdown"]["invention"]["source_copy_job_cost_per_run"]
    assigner, actions = _stock_pass([row], [A._copier()], {}, min_invented=0)
    assert actions == []
    assert assigner.free_slots_after[A.COPIER_ID]["science"] == 2


def test_stock_pass_skips_a_bpo_without_a_fee_row():
    assigner, actions = _stock_pass([], [A._copier()], {}, min_invented=0)
    assert actions == []


def test_invent_uses_t1_below_minimum():
    # One T1 BPC: T1 stock is short too, but the T2 gap ranks first and invents from it (option B).
    t1 = _decision(12344, decision="skip", margin=1.0)
    _assigner, actions = _stock_pass([_t2_skip_row(), t1], [A._best_inventor()], {998: [L._bpc(701, 998, 10)]})
    (action,) = actions
    assert (action.action_type, action.type_id, action.blueprint_item_id) == ("invent", 12345, 701)
    assert "stock: 0 of 3 BPCs" in action.notes
    assert action.estimated_job_fee_isk is not None and action.estimated_cost_isk is not None


def test_invent_counts_running_inventions():
    # A running invention of 10 attempts at the overview's chance covers the 3 BPCs wanted.
    jobs = [{"activity_id": 8, "blueprint_type_id": 998, "product_type_id": 1999, "runs": 10, "status": "active",
             "installer_id": 0, "end_date": None}]
    _assigner, actions = _stock_pass(
        [_t2_skip_row()], [A._best_inventor()], {998: [L._bpc(i, 998, 10) for i in (701, 702, 703)]}, jobs=jobs,
    )
    assert not [a for a in actions if a.action_type == "invent"]


def test_no_stock_pass_without_inputs():
    assigner = CharacterAssigner()
    actions = assigner.assign(
        ChainPlan(decisions=[_t2_skip_row()]), [], _Chars([A._copier()]), A._Admin(), dict(A.PRICES),
        blueprint_data=BLUEPRINTS,
    )
    assert actions == []


def test_pure_t1_bpo_copies_priced_from_its_own_row():
    # No T2 row invents from BPO 998: the copy fee is its own row's (0.25 per copied run).
    _assigner, actions = _stock_pass([_t1_row()], [A._copier()], {998: [L._bpc(701, 998, 10)]}, min_invented=0)
    (action,) = actions
    assert (action.action_type, action.type_id, action.quantity, action.runs) == ("copy", 998, 2, 60)
    assert action.estimated_job_fee_isk == 15.0
    assert action.notes.startswith("stock: 1 of 3 BPCs; make 2 copies")
    assert "Copy T1 BPO to feed invention pipeline" not in action.notes


def test_copy_gap_without_own_row_ranks_by_its_t2_dependant():
    # BPO 998 has no T1 row, but its T2 dependant (margin 40) outranks BPO 500's own row (margin 10).
    blueprints = dict(BLUEPRINTS)
    blueprints[500] = {**copy.deepcopy(BLUEPRINTS[998]), "invention": {"materials": [], "products": []}}
    blueprints[500]["manufacturing"]["products"] = [{"type_id": 5000, "quantity": 1}]
    assigner = CharacterAssigner()
    actions = assigner.assign(
        ChainPlan(decisions=[_t1_row(5000, margin=10.0), _t2_skip_row()]), [], _Chars([_one_slot_pilot()]),
        A._Admin(), dict(A.PRICES), blueprint_data=blueprints,
        blueprint_stock={
            "bpo_assets_by_type_id": {998: [L._bpo(77, 998)], 500: [L._bpo(55, 500)]}, "bpc_assets_by_type_id": {},
            "busy_blueprint_item_ids": set(), "min_bpc": 3, "min_invented": 0,
        },
    )
    assert [(a.action_type, a.type_id) for a in actions] == [("copy", 998)]


def test_bpcs_this_plan_uses_up_are_not_stock():
    # T2 blueprint 1999: 3 BPCs, 2 of them claimed by a manufacture in this plan -> 1 in stock, an invent gap.
    t2 = _t2_skip_row()
    t2.overview_row["ledger_claims"] = {"manufacture": [801, 802], "copy": [], "invention": [], "research": []}
    bpcs = {998: [L._bpc(701, 998, 10)], 1999: [L._bpc(i, 1999, 10) for i in (801, 802, 803)]}
    _assigner, actions = _stock_pass([t2], [A._best_inventor()], bpcs, min_bpc=0)
    (action,) = actions
    assert (action.action_type, action.blueprint_item_id) == ("invent", 701)
    assert "stock: 1 of 3 BPCs" in action.notes


def test_service_passes_the_stock_inputs():
    from types import SimpleNamespace

    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService

    seen = {}
    svc = DailyPlannerService.__new__(DailyPlannerService)
    svc._admin, svc._characters = None, None
    svc._character_assigner = SimpleNamespace(assign=lambda **kw: seen.update(kw) or [])
    phase1 = {
        "industry_jobs": [], "price_map": {}, "market_depth_cache": {}, "blueprint_data": {},
        "wallet_budget": None, "corp_material_stock": {}, "bpo_assets_by_type_id": {998: ["bpo"]},
        "bpc_assets_by_type_id": {998: ["bpc"]}, "busy_blueprint_item_ids": {7},
    }
    svc._phase_6_assign(ChainPlan(decisions=[]), phase1)
    assert seen["blueprint_stock"] == {
        "bpo_assets_by_type_id": {998: ["bpo"]}, "bpc_assets_by_type_id": {998: ["bpc"]},
        "busy_blueprint_item_ids": {7}, "min_bpc": 3, "min_invented": 3,
    }


def test_stock_note_rounds_expected_bpcs_to_two_decimals(monkeypatch):
    """Jobs in flight count as chance-weighted BPCs (1.85867); the note shows 1.86."""
    import eve_online_industry_tracker.application.daily_planner.blueprint_stock as stock
    monkeypatch.setattr(stock, "count_in_progress", lambda jobs, probability: {998: 0.85867})
    _assigner, actions = _stock_pass(
        [_t2_skip_row()], [A._copier()], {998: [L._bpc(701, 998, 10)]}, min_invented=0,
    )
    (action,) = actions
    assert action.notes.startswith("stock: 1.86 of 3 BPCs")
