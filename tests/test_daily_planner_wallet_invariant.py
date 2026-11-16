"""The service refreshes the corp wallet first and plans within (100 - reserve)% of it."""
from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from eve_online_industry_tracker.application.corporations.corporation_manager import CorporationRefreshError
from eve_online_industry_tracker.application.daily_planner.action_plan_builder import ActionPlanBuilder
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan
from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService
from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder
from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager
from fakes import _Chars, _NoBlueprints
from planner_skill_fakes import NO_SKILL_REQUIREMENTS
from streamlit_ui.components.daily_planner.status_bar import compute_capital_reserved
from test_daily_planner_budget_assignment import MAT, PRICES, M, _AdminStub, _char, _parent
from test_daily_planner_end_to_end import GOOD_ROW
import test_blueprint_ledger as L
import test_chain_planner_keying as K
import test_daily_planner_assigned_inventor as INV
import test_daily_planner_release_replan as R

PAID = {"manufacture", "sub_manufacture", "me_research", "te_research", "copy", "invent"}


def _wallet_corp(balance=100 * M, age_hours=0.0):
    when = (datetime.now(timezone.utc) - timedelta(hours=age_hours)).isoformat()
    return {"wallets": [{"division": 1, "balance": str(balance)}], "wallets_refreshed_at": when}


def _compute(session_provider, planner_repo, tmp_path, corp, refresh):
    calls = []
    corporations = SimpleNamespace(
        refresh_wallet_balances=lambda: calls.append("refresh") or refresh(),
        list_corporations=lambda: calls.append("list") or ([corp] if corp else []),
    )
    svc = DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: [GOOD_ROW]),
        corporations_service=corporations,
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(), market_pricing_service=SimpleNamespace(),
        repo=planner_repo, admin_settings=AdminSettingsManager(file_path=str(tmp_path / "admin.json")),
        session_provider=session_provider,
    )
    svc._run_compute()
    assert svc._status == "done", svc._error
    summary = json.loads(planner_repo.get_active_plan().plan_summary_json)
    return calls, summary


def _boom():
    raise CorporationRefreshError("refresh_wallet_balances", {"Corp": "esi down"})


def _plan_actions(svc, budget, decisions):
    """Phases 6-8 of the service (real assigner, shopping list, action builder) as capital-reserved dicts."""
    phase1 = {
        "wallet_budget": budget, "corp_material_stock": {},
        "industry_jobs": [], "price_map": PRICES, "market_depth_cache": PRICES,
        "blueprint_data": NO_SKILL_REQUIREMENTS, "corp_assets": [],
    }
    chain = ChainPlan(decisions=decisions)
    assigned = svc._phase_6_assign(chain, phase1)
    phase1["blueprint_data"] = {}  # the shopping list and action builder index real SDE data
    rows = svc._phase_8_actions(assigned, svc._phase_7_shopping(assigned, phase1), phase1, chain)
    return [{"action_type": r.action_type, "status": "pending", "estimated_cost_isk": r.estimated_cost_isk,
             "estimated_job_fee_isk": r.estimated_job_fee_isk} for r in rows]


def _pilot_service(session_provider=None, planner_repo=None, tmp_path=None, corp=None, pilots=2):
    corporations = SimpleNamespace(
        refresh_wallet_balances=lambda: _boom(),
        list_corporations=lambda: [corp] if corp else [],
    )
    return DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: [GOOD_ROW]),
        corporations_service=corporations,
        characters_service=_Chars([_char(i + 1) for i in range(pilots)]),
        sales_history_service=SimpleNamespace(), market_pricing_service=SimpleNamespace(),
        repo=planner_repo, admin_settings=AdminSettingsManager(file_path=str(tmp_path / "admin.json")),
        session_provider=session_provider,
    )


def test_plan_never_exceeds_budget():
    # Two builds (500M and 200M of materials) and two pilots compete for 90M of a 100M wallet.
    svc = DailyPlannerService(
        industry_service=SimpleNamespace(), corporations_service=SimpleNamespace(),
        characters_service=_Chars([_char(1), _char(2)]), sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(), repo=SimpleNamespace(), admin_settings=_AdminStub(),
        session_provider=SimpleNamespace(),
    )
    decisions = [_parent(type_id=100, runs=50), _parent(type_id=200, runs=20)]
    actions = _plan_actions(svc, WalletBudget(balance=100 * M, reserve_pct=10), decisions)
    assert any(a["action_type"] == "manufacture" for a in actions)
    reserved = compute_capital_reserved(actions)
    assert 0 < reserved.total <= 90 * M
    assert reserved.unpriced_purchases == 0 and reserved.unknown_fee_jobs == 0


def _phase1_budget_plan(session_provider, planner_repo, tmp_path, corp):
    """The budget the service's own phase 1 builds, then phases 6-8 with a pilot and an affordable build."""
    svc = _pilot_service(session_provider, planner_repo, tmp_path, corp)
    budget = svc._phase_1_collect()["wallet_budget"]
    return budget, _plan_actions(svc, budget, [_parent(runs=5)])  # 50M of materials


def test_healthy_wallet_plans_the_same_build(session_provider, planner_repo, tmp_path):
    # Control: the build of the two tests below is affordable, so they do test the budget.
    _, actions = _phase1_budget_plan(session_provider, planner_repo, tmp_path, _wallet_corp(age_hours=1))
    assert any(a["action_type"] == "manufacture" for a in actions)


def test_service_passes_stale_wallet_as_blocked(session_provider, planner_repo, tmp_path):
    budget, actions = _phase1_budget_plan(session_provider, planner_repo, tmp_path, _wallet_corp(age_hours=7))
    assert budget.blocked_reason.startswith("corp wallet last read")
    assert not [a for a in actions if a["action_type"] in PAID]
    assert {a["action_type"] for a in actions} <= {"deliver"}


def test_unknown_wallet_plans_no_paid_jobs(session_provider, planner_repo, tmp_path):
    budget, actions = _phase1_budget_plan(session_provider, planner_repo, tmp_path, None)
    assert budget.blocked_reason == "corp wallet unknown"
    assert not [a for a in actions if a["action_type"] in PAID]
    assert {a["action_type"] for a in actions} <= {"deliver"}


def test_compute_refreshes_wallet_first(session_provider, planner_repo, tmp_path):
    calls, summary = _compute(session_provider, planner_repo, tmp_path, _wallet_corp(), lambda: None)
    assert calls[:2] == ["refresh", "list"]
    assert summary["wallet"]["balance"] == 100 * M and summary["wallet"]["reserve_pct"] == 10.0
    assert summary["wallet"]["blocked_reason"] is None and "free_slots" in summary
    assert summary["wallet"]["budget"] == 90 * M  # 100M after the 10% reserve


def test_wallet_refresh_failure_uses_stored_balance(session_provider, planner_repo, tmp_path):
    _, summary = _compute(session_provider, planner_repo, tmp_path, _wallet_corp(age_hours=2), _boom)
    assert summary["wallet"]["balance"] == 100 * M
    assert 1.9 < summary["wallet"]["age_hours"] < 2.5 and summary["wallet"]["blocked_reason"] is None


def test_stale_wallet_is_recorded_in_the_summary(session_provider, planner_repo, tmp_path):
    _, summary = _compute(session_provider, planner_repo, tmp_path, _wallet_corp(age_hours=7), _boom)
    assert summary["wallet"]["blocked_reason"].startswith("corp wallet last read")
    assert summary["wallet"]["budget"] == 0


def test_unknown_wallet_is_recorded_in_the_summary(session_provider, planner_repo, tmp_path):
    _, summary = _compute(session_provider, planner_repo, tmp_path, None, lambda: None)
    assert summary["wallet"]["balance"] is None
    assert summary["wallet"]["blocked_reason"] == "corp wallet unknown"



def test_plan_never_exceeds_budget_with_subs_stock_and_science():
    """Phases 5-8 with the real ChainPlanner and its re-size: parents A and B
    share component 54321 (corp stock 300, sub-built from owned BPO 888), B
    is cut by the wallet, a T2 row invents from T1 BPC 701 and a T1 row
    researches ME on its ME0 BPO. Science is set aside first (~4.1k ISK)."""
    # Sub blueprint 888 (54321 from Tritanium) and the invention pair 998 -> 1999 (makes 12345).
    blueprints = {888: copy.deepcopy(K.SUB_BLUEPRINT_DATA[888]), **copy.deepcopy(INV.BLUEPRINTS)}
    for bp, product in ((30002, 30001), (30012, 30011), (30022, 30021)):
        blueprints[bp] = {"manufacturing": {"products": [{"type_id": product, "quantity": 1}],
                                            "materials": [{"type_id": 54321, "quantity": 100}]}}
    prices = {**INV.PRICES, 34: {"vwap_5d": 1.0}, 54321: {"vwap_5d": 10.0}}

    def parent(type_id, bp, runs, score, **job):
        row = K._row(type_id=type_id, blueprint_type_id=bp, runs=runs, manufacturing_job_cost=100.0,
                     material_cost=1000.0 * runs, total_cost=1000.0 * runs + 100.0,
                     materials=K._batch_materials(fee=1.0, **{"54321": 100 * runs}), **job)
        row["quantity"] = runs
        return K._decision(row, capital_return_per_day=score, adjusted_score=score)

    researched = parent(30021, 30022, 1, 1.0, research_job_cost_by_level={
        "research_material": [0.0] + [50.0] * 10, "research_time": [0.0] * 11})
    t2_row = INV._row()
    t2_row["manufacturing_job"]["blueprint_sde"] = {"blueprint_type_id": 1999}
    t2_row.pop("needs_invention")
    t2 = K._decision(t2_row, meta_group_id=2, pipeline_stage="invention", adjusted_score=0.5)
    chain = K._planner().plan_chain([parent(30001, 30002, 20, 3.0), parent(30011, 30012, 2, 2.0), researched, t2], {
        "bpo_assets_by_type_id": {888: [K._bpo(888, me=10, te=20)], 30022: [L._bpo(31, 30022, me=0, te=20)]},
        "bpc_assets_by_type_id": {998: [L._bpc(701, 998, 10)]},
        "blueprint_data": blueprints, "market_depth_cache": prices, "variant_rows_by_type_id": {},
        "corp_material_stock": {54321: 300},
    })
    (sub,) = [d for d in chain.decisions if d.is_sub_component]
    assert sub.overview_row["quantity_needed"] == 1900  # 2000 + 200 requested - 300 stock

    svc = DailyPlannerService(
        industry_service=SimpleNamespace(), corporations_service=SimpleNamespace(),
        characters_service=_Chars([R._pilot(1, "P1", INV.ENCRYPTION, lab=2, mass=4)]),
        sales_history_service=SimpleNamespace(), market_pricing_service=SimpleNamespace(),
        repo=SimpleNamespace(), admin_settings=_AdminStub(), session_provider=SimpleNamespace(),
    )
    svc._meta_resolver = _NoBlueprints()
    balance = 4_790.0
    phase1 = {
        "wallet_budget": WalletBudget(balance=balance, reserve_pct=0), "corp_material_stock": {54321: 300},
        "industry_jobs": [], "price_map": prices, "market_depth_cache": prices,
        "blueprint_data": NO_SKILL_REQUIREMENTS, "corp_assets": [{"type_id": 54321, "quantity": 300}],
    }
    assigned = svc._phase_6_assign(chain, phase1)
    phase1["blueprint_data"] = blueprints
    rows = svc._phase_8_actions(assigned, svc._phase_7_shopping(assigned, phase1), phase1, chain)

    (cut,) = [a for a in assigned if a.action_type == "manufacture" and a.type_id == 30011]
    assert cut.runs == 1 and "cut from 2 to 1 run" in cut.notes
    (sub_job,) = [a for a in assigned if a.action_type == "sub_manufacture"]
    assert sub_job.quantity == 1800  # the real re-size for B's 1 run
    assert {"sub_manufacture", "invent", "me_research"} <= {r.action_type for r in rows}
    reserved = compute_capital_reserved([
        {"action_type": r.action_type, "status": "pending", "estimated_cost_isk": r.estimated_cost_isk,
         "estimated_job_fee_isk": r.estimated_job_fee_isk} for r in rows
    ])
    assert reserved.unpriced_purchases == 0 and reserved.unknown_fee_jobs == 0
    assert 0 < reserved.total <= balance
