"""BPO batches sized to end before a daily check, and the top-level order by
profit per slot-day while the wallet pays every batch. Fake data."""
from __future__ import annotations

import pytest
from datetime import datetime

from eve_online_industry_tracker.application.daily_planner import character_assigner
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.item_decision_engine import check_deadlines, day_sized_runs
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision
from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
from fakes import _Chars
from planner_skill_fakes import NO_SKILL_REQUIREMENTS

MAT = 34
M = 1_000_000
HOUR = 3600


class _AdminStub:
    def get(self, section, key, default=None):
        return {"planner_max_job_days": 3, "planner_daily_check_hour": 11}.get(key, default)


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


@pytest.fixture(autouse=True)
def _plan_at_13_30(monkeypatch):
    """Planned at 13:30 local: the next three checks at 11:00 are 21.5h, 45.5h and 69.5h away."""
    monkeypatch.setattr(character_assigner, "_now", lambda: datetime(2026, 11, 18, 13, 30).astimezone())


def _jobs(actions, type_id=100):
    return [a for a in actions if a.action_type == "manufacture" and a.type_id == type_id]


# --- check_deadlines / day_sized_runs ---------------------------------------

def test_deadlines_are_the_next_daily_checks_at_least_12h_away():
    local = datetime(2026, 11, 18, 13, 30).astimezone()        # after today's 11:00
    gaps = check_deadlines(local, 11, 3)
    assert [round(g / HOUR, 1) for g in gaps] == [21.5, 45.5, 69.5]
    early = datetime(2026, 11, 18, 10, 0).astimezone()          # 1h before today's check
    assert round(check_deadlines(early, 11, 1)[0] / HOUR) == 25


def test_runs_end_before_the_latest_check_the_room_allows():
    gaps = [21.5 * HOUR, 45.5 * HOUR, 69.5 * HOUR]
    assert day_sized_runs(seconds_per_run=HOUR, room_runs=100, deadlines=gaps) == 69
    assert day_sized_runs(seconds_per_run=HOUR, room_runs=50, deadlines=gaps) == 45


def test_less_room_than_the_first_check_gives_the_room():
    assert day_sized_runs(seconds_per_run=HOUR, room_runs=10, deadlines=[21.5 * HOUR]) == 10


def test_no_run_before_the_last_check_or_no_room_is_left_alone():
    assert day_sized_runs(seconds_per_run=80 * HOUR, room_runs=10, deadlines=[21.5 * HOUR, 45.5 * HOUR]) is None
    assert day_sized_runs(seconds_per_run=HOUR, room_runs=0, deadlines=[21.5 * HOUR]) is None


def test_a_run_longer_than_a_day_ends_before_a_later_check():
    assert day_sized_runs(seconds_per_run=30 * HOUR, room_runs=10, deadlines=[21.5 * HOUR, 45.5 * HOUR]) == 1


# --- the assigner -------------------------------------------------------------

def test_a_bpo_batch_grows_to_the_latest_check_with_its_materials_and_figures():
    job = _jobs(_assign([_row_decision(runs=10, room_runs=1000)], [_char(1, 2)]))[0]
    assert job.runs == 69                      # 1h runs, ends before the third 11:00 check
    assert job.quantity == 6900
    assert job.materials == {MAT: 69}
    assert job.estimated_profit_isk == pytest.approx(5 * M * 6.9)
    assert "sized to 69 runs, 69h of job time" in job.notes


def test_a_bpo_batch_is_cut_to_the_first_check_when_the_market_has_room_for_less():
    # 300 runs of 1h but room for 30: 21 runs, done before tomorrow's check, not 30 that miss it.
    job = _jobs(_assign([_row_decision(runs=300, room_runs=30)], [_char(1, 2)]))[0]
    assert job.runs == 21


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
