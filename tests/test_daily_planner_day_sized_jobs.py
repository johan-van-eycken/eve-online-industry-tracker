"""BPO batches sized to whole days of job time, and the top-level order by
profit per slot-day while the wallet pays every batch. Fake data."""
from __future__ import annotations

from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.item_decision_engine import day_sized_runs
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision
from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
from fakes import _Chars
from planner_skill_fakes import NO_SKILL_REQUIREMENTS

MAT = 34
M = 1_000_000
HOUR = 3600


class _AdminStub:
    def get(self, section, key, default=None):
        return {"planner_max_job_days": 3}.get(key, default)


def _char(char_id, mfg_slots):
    skills = [{"skill_id": 0, "skill_name": "Mass Production", "group_name": "Production",
               "trained_skill_level": mfg_slots - 1}] if mfg_slots > 1 else []
    return {"character_id": char_id, "character_name": f"Pilot {char_id}", "skills": {"skills": skills, "total_sp": 0}}


def _row_decision(*, type_id=100, runs=10, hours_per_run=1.0, room_runs=1000, profit=5 * M, crpd=0.1,
                  source="owned_blueprint_original"):
    """A batch of `runs` on one owned blueprint, 1M ISK a run."""
    return ItemDecision(
        type_id=type_id, type_name=f"Product {type_id}", decision="build", decision_reason="margin 20.0%",
        adjusted_score=1.0, absolute_profit_per_batch=profit, isk_per_hour=1.0, margin_pct=20.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=1, pipeline_stage="manufacturing",
        capital_return_per_day=crpd, market_cap_runs=min(runs, room_runs), market_room_runs=room_runs,
        overview_row={
            "type_id": type_id, "quantity": runs * 100, "blueprint_type_id": type_id + 1,
            "manufacturing_job": {
                "runs": runs, "time_seconds": hours_per_run * HOUR * runs,
                "material_cost": 1.0 * M * runs, "total_cost": 1.0 * M * runs, "manufacturing_job_cost": 0.0,
                "materials": {str(MAT): {"type_id": MAT, "quantity": runs}},
                "job_segments": [{"runs": runs, "source": source, "item_id": 1,
                                  "blueprint_material_efficiency": 10, "blueprint_time_efficiency": 20}],
            },
        },
    )


def _assign(decisions, chars, budget=None, sub_parents=frozenset()):
    plan = ChainPlan(decisions=decisions, sub_request_parent_type_ids=frozenset(sub_parents))
    return CharacterAssigner().assign(
        plan, [], _Chars(chars), _AdminStub(), {MAT: {"vwap_5d": 1.0 * M}},
        blueprint_data=NO_SKILL_REQUIREMENTS, budget=budget,
    )


def _jobs(actions, type_id=100):
    return [a for a in actions if a.action_type == "manufacture" and a.type_id == type_id]


# --- day_sized_runs -----------------------------------------------------------

def test_runs_fill_whole_days_up_to_the_max():
    # 1h a run: 24 runs a day; room for 100 runs is 4 whole days, capped at 3.
    assert day_sized_runs(seconds_per_run=HOUR, room_runs=100, max_days=3) == 72
    assert day_sized_runs(seconds_per_run=HOUR, room_runs=50, max_days=3) == 48


def test_less_room_than_a_day_gives_the_room():
    assert day_sized_runs(seconds_per_run=HOUR, room_runs=10, max_days=3) == 10


def test_a_run_longer_than_a_day_or_no_room_is_left_alone():
    assert day_sized_runs(seconds_per_run=30 * HOUR, room_runs=10, max_days=3) is None
    assert day_sized_runs(seconds_per_run=HOUR, room_runs=0, max_days=3) is None


# --- the assigner -------------------------------------------------------------

def test_a_bpo_batch_grows_to_whole_days_with_its_materials_and_figures():
    job = _jobs(_assign([_row_decision(runs=10, room_runs=1000)], [_char(1, 2)]))[0]
    assert job.runs == 72                      # 3 days of 1h runs
    assert job.quantity == 7200
    assert job.materials == {MAT: 72}
    assert job.estimated_profit_isk == 5 * M * 7.2
    assert "sized to 72 runs, 72h of job time" in job.notes


def test_a_bpo_batch_is_cut_to_a_whole_day_when_the_market_has_room_for_less():
    # 300 runs of 1h (12.5 days) but room for 30: one day, not 30 runs ending mid-day 2.
    job = _jobs(_assign([_row_decision(runs=300, room_runs=30)], [_char(1, 2)]))[0]
    assert job.runs == 24


def test_a_bpc_batch_keeps_its_runs():
    job = _jobs(_assign([_row_decision(runs=10, source="owned_blueprint_copy")], [_char(1, 2)]))[0]
    assert job.runs == 10


def test_a_bpo_batch_with_components_keeps_its_runs():
    job = _jobs(_assign([_row_decision(runs=10)], [_char(1, 2)], sub_parents={100}))[0]
    assert job.runs == 10


def test_the_wallet_still_cuts_a_grown_batch():
    budget = WalletBudget(balance=30 * M, reserve_pct=0)
    job = _jobs(_assign([_row_decision(runs=10)], [_char(1, 2)], budget=budget))[0]
    assert job.runs == 30


# --- order: profit per slot-day while the wallet pays every batch ------------

def _two_products():
    # A: best return on capital, but 1M profit for a 1-day job.
    # B: lower return on capital, 20M profit for a 1-day job.
    a = _row_decision(type_id=100, runs=10, hours_per_run=1.0, profit=1 * M, crpd=0.5,
                      source="owned_blueprint_copy")
    b = _row_decision(type_id=200, runs=10, hours_per_run=1.0, profit=20 * M, crpd=0.1,
                      source="owned_blueprint_copy")
    return a, b


def test_with_money_for_every_batch_the_one_slot_goes_to_the_most_profit_per_slot_day():
    actions = _assign(list(_two_products()), [_char(1, 1)], budget=WalletBudget(balance=100 * M, reserve_pct=0))
    assert len(_jobs(actions, 200)) == 1 and not _jobs(actions, 100)


def test_with_too_little_money_the_order_stays_return_on_capital():
    actions = _assign(list(_two_products()), [_char(1, 1)], budget=WalletBudget(balance=15 * M, reserve_pct=0))
    assert len(_jobs(actions, 100)) == 1 and not _jobs(actions, 200)
