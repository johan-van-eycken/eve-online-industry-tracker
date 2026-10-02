"""End-to-end smoke test for DailyPlannerService._run_compute().

Fix round 1 (task 11b), finding 1: the wiring tests in
test_daily_planner_wiring.py prove the phase call sites pass the kwargs the
sub-components now require, but none of them actually runs the nine-phase
pipeline. Task 19's integration test is fixture-gated on a real-data capture
that has not been performed and stays skipped, so before this test nothing
committed proved plan computation completes -- a later change could break the
chain end to end and the suite would stay green.

This test does not need that fixture: one synthetic overview row is enough to
drive every phase. It uses real collaborators wherever a stub would hide the
exact bug this branch exists to fix:
  - a real AdminSettingsManager (not a SimpleNamespace stub) -- `_adm()`
    passes admin_settings.get(...)'s result straight into float() for
    settings like planner_ema_alpha, and a stub `get` returning None
    unconditionally makes _phase_1_collect's feedback processing crash before
    Phase 2 is ever reached.
  - a real DailyPlannerRepository backed by an in-memory SQLite engine with
    the real BaseApp schema, so Phase 9's persistence calls (archive_active_plan,
    insert_plan, insert_plan_items, ...) exercise real SQL, not a mock.
  - a real second in-memory engine with the BaseSde schema (via the shared
    session_provider fixture) for the TypeMetadataResolver Phase 1 builds.

Only industry_service, corporations_service, characters_service,
sales_history_service, pricing_suggestion_service, market_pricing_service and
realized_profit_service are stubs -- none of them are exercised by
_run_compute's core phase sequence for this scenario (no characters means
CharacterAssigner assigns nothing, which is fine: the assertions below check
Phase 9's persisted plan, not the shopping/action lists).
"""
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService
from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager

GOOD_ROW = {
    "type_id": 12345,
    "type_name": "Hobgoblin II",
    "quantity": 10,
    "blueprint_type_id": 999,
    "isk_per_hour": 4_000_000.0,
    "profit_amount": 1_000_000.0,
    "profit_margin_fraction": 0.2,
    "days_of_supply": 6.5,
    "pipeline_units_in_jobs": 40,
    "pipeline_units_on_market": 60,
    "pipeline_days_supply": 12.5,
    "price_trend_7d_pct": 3.5,
    "price_trend_30d_pct": -2.0,
    "manufacturing_job": {"runs": 20, "material_cost": 50_000_000.0},
}


def test_run_compute_completes_end_to_end_and_persists_a_plan_with_items(
    session_provider, planner_repo, tmp_path
):
    admin = AdminSettingsManager(file_path=str(tmp_path / "admin.json"))

    svc = DailyPlannerService(
        industry_service=SimpleNamespace(
            get_cached_overview_rows=lambda: [GOOD_ROW],
        ),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        pricing_suggestion_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        realized_profit_service=SimpleNamespace(),
        repo=planner_repo,
        admin_settings=admin,
        session_provider=session_provider,
    )

    svc._run_compute()

    assert svc._status == "done", f"plan computation failed: {svc._error}"
    assert svc._error is None

    plan = planner_repo.get_active_plan()
    assert plan is not None, "Phase 9 must persist an active BuildPlanModel"

    items = planner_repo.get_plan_items(int(plan.id))
    assert len(items) >= 1, (
        "a status of 'done' on an empty plan is a hollow guard -- the one "
        "input row must have produced at least one persisted plan item"
    )
    assert items[0].type_id == GOOD_ROW["type_id"]


def test_a_malformed_wallet_balance_persists_as_unknown_not_a_real_zero(
    session_provider, planner_repo, tmp_path
):
    """FIX #1 of the minors backlog: `_parse_isk` returning `0.0` for a
    malformed balance was indistinguishable from a genuine zero balance --
    the same silent-swallow shape as the original corporate-wallet review
    finding. corp_wallet_snapshot is nullable precisely so "unknown" (a
    present-but-unparseable division-1 balance) can reach the persisted
    plan as `None`/NULL rather than a misleading `0.0`, without aborting
    plan computation or changing any other planner output.
    """
    admin = AdminSettingsManager(file_path=str(tmp_path / "admin.json"))

    svc = DailyPlannerService(
        industry_service=SimpleNamespace(
            get_cached_overview_rows=lambda: [GOOD_ROW],
        ),
        corporations_service=SimpleNamespace(
            list_corporations=lambda: [
                {"wallets": [{"division": 1, "balance": "not-a-number"}]}
            ]
        ),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        pricing_suggestion_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        realized_profit_service=SimpleNamespace(),
        repo=planner_repo,
        admin_settings=admin,
        session_provider=session_provider,
    )

    svc._run_compute()

    assert svc._status == "done", f"plan computation failed: {svc._error}"
    plan = planner_repo.get_active_plan()
    assert plan is not None
    assert plan.corp_wallet_snapshot is None, (
        "an unparseable division-1 balance must persist as unknown (None/NULL), "
        "not as a 0.0 that looks like a real zero balance"
    )


def test_two_variants_of_one_product_yield_exactly_one_plan_item(
    session_provider, planner_repo, tmp_path
):
    """F2: one overview row per blueprint variant used to produce one
    decision per ROW, each mixing that row's pipeline state with the last
    row's profitability. Phase 1 now keeps one row per type_id."""
    admin = AdminSettingsManager(file_path=str(tmp_path / "admin.json"))
    low = dict(GOOD_ROW, overview_row_id="row-a", isk_per_hour=9_800_000.0)
    high = dict(GOOD_ROW, overview_row_id="row-b", isk_per_hour=20_500_000.0)

    svc = DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: [high, low]),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        pricing_suggestion_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        realized_profit_service=SimpleNamespace(),
        repo=planner_repo,
        admin_settings=admin,
        session_provider=session_provider,
    )

    svc._run_compute()

    assert svc._status == "done", f"plan computation failed: {svc._error}"
    items = planner_repo.get_plan_items(int(planner_repo.get_active_plan().id))
    assert len(items) == 1
    assert items[0].isk_per_hour == 20_500_000.0
