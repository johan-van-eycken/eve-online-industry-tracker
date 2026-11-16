"""The service refreshes the corp wallet first and plans within (100 - reserve)% of it."""
from __future__ import annotations

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
from fakes import _Chars
from planner_skill_fakes import NO_SKILL_REQUIREMENTS
from streamlit_ui.components.daily_planner.status_bar import compute_capital_reserved
from test_daily_planner_budget_assignment import MAT, PRICES, M, _AdminStub, _char, _parent
from test_daily_planner_end_to_end import GOOD_ROW

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


def test_plan_never_exceeds_budget():
    # The real assigner, shopping list and action builder, a real WalletBudget: 500M of builds, 100M wallet.
    # (A full _run_compute needs a real overview + SDE; phases 6-8 are where the invariant lives.)
    svc = DailyPlannerService(
        industry_service=SimpleNamespace(), corporations_service=SimpleNamespace(),
        characters_service=_Chars([_char(1)]), sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(), repo=SimpleNamespace(), admin_settings=_AdminStub(),
        session_provider=SimpleNamespace(),
    )
    decision = _parent(runs=50)  # 500M of materials
    phase1 = {
        "wallet_budget": WalletBudget(balance=100 * M, reserve_pct=10), "corp_material_stock": {},
        "industry_jobs": [], "price_map": PRICES, "market_depth_cache": PRICES,
        "blueprint_data": NO_SKILL_REQUIREMENTS, "corp_assets": [],
    }
    chain = ChainPlan(decisions=[decision])
    assigned = svc._phase_6_assign(chain, phase1)
    phase1["blueprint_data"] = {}  # the shopping list and action builder index real SDE data
    rows = svc._phase_8_actions(assigned, svc._phase_7_shopping(assigned, phase1), phase1, chain)
    actions = [{"action_type": r.action_type, "status": "pending", "estimated_cost_isk": r.estimated_cost_isk,
                "estimated_job_fee_isk": r.estimated_job_fee_isk} for r in rows]
    assert any(a["action_type"] == "manufacture" for a in actions)
    reserved = compute_capital_reserved(actions)
    assert 0 < reserved.total <= 90 * M
    assert reserved.unpriced_purchases == 0 and reserved.unknown_fee_jobs == 0


def test_compute_refreshes_wallet_first(session_provider, planner_repo, tmp_path):
    calls, summary = _compute(session_provider, planner_repo, tmp_path, _wallet_corp(), lambda: None)
    assert calls[:2] == ["refresh", "list"]
    assert summary["wallet"]["balance"] == 100 * M and summary["wallet"]["reserve_pct"] == 10.0
    assert summary["wallet"]["blocked_reason"] is None and "free_slots" in summary


def test_wallet_refresh_failure_uses_stored_balance(session_provider, planner_repo, tmp_path):
    _, summary = _compute(session_provider, planner_repo, tmp_path, _wallet_corp(age_hours=2), _boom)
    assert summary["wallet"]["balance"] == 100 * M
    assert 1.9 < summary["wallet"]["age_hours"] < 2.5 and summary["wallet"]["blocked_reason"] is None


def test_service_passes_stale_wallet_as_blocked(session_provider, planner_repo, tmp_path):
    _, summary = _compute(session_provider, planner_repo, tmp_path, _wallet_corp(age_hours=7), _boom)
    assert summary["wallet"]["blocked_reason"].startswith("corp wallet last read")
    assert summary["wallet"]["budget"] == 0


def test_unknown_wallet_plans_no_paid_jobs(session_provider, planner_repo, tmp_path):
    _, summary = _compute(session_provider, planner_repo, tmp_path, None, lambda: None)
    assert summary["wallet"]["balance"] is None
    assert summary["wallet"]["blocked_reason"] == "corp wallet unknown"
    assert WalletBudget(balance=None, reserve_pct=10).charge(1.0) is False
