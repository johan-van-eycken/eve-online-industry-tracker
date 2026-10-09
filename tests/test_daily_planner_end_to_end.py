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
sales_history_service, market_pricing_service and
realized_profit_service are stubs -- none of them are exercised by
_run_compute's core phase sequence for this scenario (no characters means
CharacterAssigner assigns nothing, which is fine: the assertions below check
Phase 9's persisted plan, not the shopping/action lists).
"""
from __future__ import annotations

import logging

import pytest
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


def test_a_compute_logs_no_pricing_suggestion_or_relist_warning(
    session_provider, planner_repo, tmp_path, caplog
):
    """The RELIST phase was removed: it asked a service for a method that never
    existed and logged a WARNING on every compute. No such warning may return."""
    admin = AdminSettingsManager(file_path=str(tmp_path / "admin.json"))
    svc = DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: [GOOD_ROW]),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        realized_profit_service=SimpleNamespace(),
        repo=planner_repo,
        admin_settings=admin,
        session_provider=session_provider,
    )

    with caplog.at_level(logging.DEBUG):
        svc._run_compute()

    assert svc._status == "done", f"plan computation failed: {svc._error}"
    offending = [
        r.getMessage() for r in caplog.records
        if "relist" in r.getMessage().lower()
        or "get_suggestions" in r.getMessage()
        or "pricing suggestion" in r.getMessage().lower()
    ]
    assert offending == []


# --- round 5 Task 3 fix round 1, I3: invention inputs are priced in phase 1 ---
# FAKE prices. The market-depth table only holds the plan's product types, so
# datacores and decryptors need their own price lookup or the shopping list
# skips them as unpriced.


def _invention_planner(session_provider, planner_repo, tmp_path, *, prices):
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction

    admin = AdminSettingsManager(file_path=str(tmp_path / "admin.json"))
    t2_row = {
        **GOOD_ROW,
        "manufacturing_job": {
            **GOOD_ROW["manufacturing_job"],
            "blueprint_sde": {"blueprint_type_id": 9002},
            "invention_decryptor": {"type_id": 34201, "type_name": "Accelerant Decryptor"},
        },
    }
    asked: list[list[int]] = []

    def get_type_price_map(*, type_ids, hub="jita", side="sell", progress_callback=None):
        asked.append(sorted(type_ids))
        return {int(t): {"unit_price": prices[int(t)]} for t in type_ids if int(t) in prices}

    svc = DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: [t2_row]),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(get_type_price_map=get_type_price_map),
        realized_profit_service=SimpleNamespace(),
        repo=planner_repo,
        admin_settings=admin,
        session_provider=session_provider,
    )
    # The SDE read (the in-memory SDE has no blueprints); the prices are not injected.
    svc._get_blueprint_data = lambda rows, extra_blueprint_type_ids=(): {
        1001: {"manufacturing": {"materials": [], "products": [{"type_id": 6001, "quantity": 1}]},
               "invention": {"materials": [{"type_id": 204, "type_name": "Datacore", "quantity": 2}],
                             "products": [{"type_id": 9002, "quantity": 10}]}},
        9002: {"manufacturing": {"materials": [], "products": [{"type_id": GOOD_ROW["type_id"], "quantity": 1}]}},
    }
    invent = AssignedAction(
        type_id=GOOD_ROW["type_id"], type_name="Hobgoblin II", action_type="invent", character_id=1,
        character_name="Pilot", quantity=None, runs=None, estimated_cost_isk=None, estimated_profit_isk=None,
        estimated_completion=None, notes=None, decryptor_type_id=34201, decryptor_type_name="Accelerant Decryptor",
    )
    return svc, invent, asked


def test_phase_1_prices_datacores_and_the_chosen_decryptor(session_provider, planner_repo, tmp_path):
    svc, invent, asked = _invention_planner(
        session_provider, planner_repo, tmp_path, prices={204: 100.0, 34201: 50.0},
    )
    phase1 = svc._phase_1_collect()

    assert asked == [[204, 34201]]
    items = {i.type_id: i for i in svc._phase_7_shopping([invent], phase1)}
    assert (items[204].quantity, items[204].estimated_unit_price) == (2, 100.0)
    assert (items[34201].quantity, items[34201].estimated_unit_price) == (1, 50.0)
    assert items[34201].shopping_category == "invention_input"


def test_an_unpriced_invention_input_still_warns(session_provider, planner_repo, tmp_path, caplog):
    svc, invent, _ = _invention_planner(session_provider, planner_repo, tmp_path, prices={204: 100.0})
    phase1 = svc._phase_1_collect()
    with caplog.at_level(logging.WARNING):
        items = {i.type_id: i for i in svc._phase_7_shopping([invent], phase1)}

    assert 204 in items and 34201 not in items
    assert any(
        "skipping invention input type_id=34201" in r.getMessage() for r in caplog.records
    )


# --- round 5 Task 3 fix round 2: every buyable material is priced in phase 1 --
# FAKE prices. A mineral (Tritanium) from the producer's batch materials and a
# component (Intact Armor Plates 25624) from an owned BPO's SDE materials are
# both outside the market-depth table; a depth row without any price must not
# block the spot fill either.


def _material_planner(session_provider, planner_repo, tmp_path, *, prices):
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction

    admin = AdminSettingsManager(file_path=str(tmp_path / "admin.json"))
    row = {
        **GOOD_ROW,
        "manufacturing_job": {
            **GOOD_ROW["manufacturing_job"],
            "blueprint_sde": {"blueprint_type_id": 999},
            "materials": {"34": {"type_id": 34, "type_name": "Tritanium", "quantity": 500}},
        },
    }
    asked: list[list[int]] = []

    def get_type_price_map(*, type_ids, hub="jita", side="sell", progress_callback=None):
        asked.append(sorted(type_ids))
        return {int(t): {"unit_price": prices[int(t)]} for t in type_ids if int(t) in prices}

    svc = DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: [row]),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(get_type_price_map=get_type_price_map),
        realized_profit_service=SimpleNamespace(),
        repo=planner_repo,
        admin_settings=admin,
        session_provider=session_provider,
    )
    # The SDE read only: blueprint 999 builds the row's product; owned BPO
    # 7777 builds a component from Intact Armor Plates.
    svc._get_blueprint_data = lambda rows, extra_blueprint_type_ids=(): {
        999: {"manufacturing": {"materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 50}],
                                "products": [{"type_id": GOOD_ROW["type_id"], "quantity": 1}]}},
        7777: {"manufacturing": {"materials": [{"type_id": 25624, "type_name": "Intact Armor Plates", "quantity": 120}],
                                 "products": [{"type_id": 7778, "quantity": 1}]}},
    }
    # A depth row with neither vwap_5d nor spot_sell_price for Tritanium.
    real_depth = svc._repo.get_market_depth
    svc._repo = SimpleNamespace(**{
        name: getattr(planner_repo, name) for name in dir(planner_repo) if not name.startswith("__")
    })
    svc._repo.get_market_depth = lambda type_ids, hub: {
        **real_depth(type_ids, hub), 34: SimpleNamespace(vwap_5d=None, spot_sell_price=None),
    }

    def action(action_type, type_id, materials):
        return AssignedAction(
            type_id=type_id, type_name=str(type_id), action_type=action_type, character_id=1,
            character_name="Pilot", quantity=None, runs=1, estimated_cost_isk=None, estimated_profit_isk=None,
            estimated_completion=None, notes=None, materials=materials,
        )

    actions = [
        action("manufacture", GOOD_ROW["type_id"], {34: 500}),
        action("sub_manufacture", 7778, None),
    ]
    return svc, actions, asked


def test_phase_1_prices_minerals_and_components_so_they_are_bought(session_provider, planner_repo, tmp_path):
    svc, actions, asked = _material_planner(
        session_provider, planner_repo, tmp_path, prices={34: 4.0, 25624: 2_000.0},
    )
    phase1 = svc._phase_1_collect()

    assert asked == [[34, 25624]]
    items = {i.type_id: i for i in svc._phase_7_shopping(actions, phase1)}
    assert (items[34].quantity, items[34].estimated_unit_price) == (500, 4.0)
    assert (items[25624].quantity, items[25624].estimated_unit_price) == (120, 2_000.0)


def test_a_material_with_no_hub_price_still_warns(session_provider, planner_repo, tmp_path, caplog):
    svc, actions, _ = _material_planner(session_provider, planner_repo, tmp_path, prices={34: 4.0})
    phase1 = svc._phase_1_collect()
    with caplog.at_level(logging.WARNING):
        items = {i.type_id: i for i in svc._phase_7_shopping(actions, phase1)}

    assert 34 in items and 25624 not in items
    assert any("skipping material type_id=25624" in r.getMessage() for r in caplog.records)


# --- round 5 Task 3 fix round 3: one merged price map for phases 5 and 7 -----
# FAKE prices. A T1 parent (12345) needs 10 x component 7778 per batch; the
# corp owns BPO 7777 (ME 10) for it, 100 Tritanium per run. Depth holds only
# the parent, so the component and the mineral are priced in phase 1. Build:
# 10 runs x ceil(100 x 0.9) = 900 Tritanium x 4 = 3,600 < 10 x 1,000 = 10,000.


def _sub_manufacture_planner(session_provider, planner_repo, tmp_path, *, depth_extra=None):
    from eve_online_industry_tracker.application.daily_planner.models import ItemDecision

    admin = AdminSettingsManager(file_path=str(tmp_path / "admin.json"))
    row = {
        **GOOD_ROW,
        "manufacturing_job": {
            **GOOD_ROW["manufacturing_job"],
            "blueprint_sde": {"blueprint_type_id": 999},
            "materials": {"7778": {"type_id": 7778, "type_name": "Component", "quantity": 10}},
        },
    }
    prices = {34: 4.0, 7778: 1_000.0}

    def get_type_price_map(*, type_ids, hub="jita", side="sell", progress_callback=None):
        return {int(t): {"unit_price": prices[int(t)]} for t in type_ids if int(t) in prices}

    svc = DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: [row]),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(get_type_price_map=get_type_price_map),
        realized_profit_service=SimpleNamespace(),
        repo=planner_repo,
        admin_settings=admin,
        session_provider=session_provider,
    )
    # SDE reads only (the in-memory SDE holds no blueprints or categories).
    svc._get_blueprint_data = lambda rows, extra_blueprint_type_ids=(): {
        999: {"max_production_limit": 100,
              "manufacturing": {"materials": [{"type_id": 7778, "type_name": "Component", "quantity": 1}],
                                "products": [{"type_id": GOOD_ROW["type_id"], "quantity": 1}]}},
        7777: {"max_production_limit": 100,
               "manufacturing": {"materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 100}],
                                 "products": [{"type_id": 7778, "quantity": 1}]}},
    }
    svc._index_blueprint_assets = lambda assets: (
        {7777: [SimpleNamespace(type_id=7777, blueprint_material_efficiency=10, blueprint_time_efficiency=20)]}, {},
    )
    if depth_extra:
        real_depth = planner_repo.get_market_depth
        svc._repo = SimpleNamespace(**{
            name: getattr(planner_repo, name) for name in dir(planner_repo) if not name.startswith("__")
        })
        svc._repo.get_market_depth = lambda type_ids, hub: {**real_depth(type_ids, hub), **depth_extra}
    decision = ItemDecision(
        type_id=GOOD_ROW["type_id"], type_name="Parent", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=1,
        pipeline_stage="manufacturing", overview_row=row,
    )
    return svc, decision


def _sub_action(sub):
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction

    (job,) = sub.overview_row["sub_job_batches"]
    # What CharacterAssigner makes of the sub decision's one job.
    return AssignedAction(
        type_id=sub.type_id, type_name=sub.type_name, action_type="sub_manufacture", character_id=1,
        character_name="Pilot", quantity=job["quantity"], runs=job["runs"], estimated_cost_isk=job["cost"],
        estimated_profit_isk=None, estimated_completion=None, notes=None, materials=job["materials"],
    )


def test_a_component_whose_minerals_are_not_in_depth_is_sub_manufactured(session_provider, planner_repo, tmp_path):
    svc, decision = _sub_manufacture_planner(session_provider, planner_repo, tmp_path)
    phase1 = svc._phase_1_collect()
    plan = svc._phase_5_chain([decision], phase1)

    subs = [d for d in plan.decisions if d.is_sub_component]
    assert [s.type_id for s in subs] == [7778]
    assert subs[0].overview_row["sub_job_batches"][0]["cost"] == pytest.approx(900 * 4.0)

    items = {i.type_id: i for i in svc._phase_7_shopping([_sub_action(subs[0])], phase1)}
    assert (items[34].quantity, items[34].estimated_unit_price) == (900, 4.0)
    # Phases 2-4 and 9 still see the raw depth map, unchanged.
    assert 34 not in phase1["market_depth_cache"] and 34 in phase1["price_map"]


def test_a_zero_depth_price_does_not_block_the_hub_price(session_provider, planner_repo, tmp_path):
    # Tritanium has a depth row whose vwap_5d is 0.0 and spot is unknown.
    svc, decision = _sub_manufacture_planner(
        session_provider, planner_repo, tmp_path,
        depth_extra={34: SimpleNamespace(vwap_5d=0.0, spot_sell_price=None)},
    )
    phase1 = svc._phase_1_collect()
    assert phase1["price_map"][34] == {"spot_sell_price": 4.0}
    plan = svc._phase_5_chain([decision], phase1)
    (sub,) = [d for d in plan.decisions if d.is_sub_component]
    items = {i.type_id: i for i in svc._phase_7_shopping([_sub_action(sub)], phase1)}
    assert items[34].estimated_unit_price == 4.0


def test_a_zero_vwap_falls_through_to_the_spot_price():
    from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder

    assert ShoppingListBuilder()._get_price(34, {34: SimpleNamespace(vwap_5d=0.0, spot_sell_price=5.0)}) == (5.0, False)
