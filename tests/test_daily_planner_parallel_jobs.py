"""Parallel jobs of one top-level product: while the market has room for
another whole batch, a free blueprint of the same ME, a manufacturing slot and
the wallet allow it, the product gets another job. Fake data."""
from __future__ import annotations

from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision
from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
from fakes import _Chars
from planner_skill_fakes import NO_SKILL_REQUIREMENTS

MAT = 34
M = 1_000_000


class _AdminStub:
    def get(self, section, key, default=None):
        return default


def _char(char_id, mfg_slots):
    skills = [{"skill_id": 0, "skill_name": "Mass Production", "group_name": "Production",
               "trained_skill_level": mfg_slots - 1}] if mfg_slots > 1 else []
    return {"character_id": char_id, "character_name": f"Pilot {char_id}", "skills": {"skills": skills, "total_sp": 0}}


def _missiles(room_runs, runs=10, type_id=100):
    """A 10-run batch of a product without components, on one owned blueprint at ME 10."""
    return ItemDecision(
        type_id=type_id, type_name=f"Product {type_id}", decision="build", decision_reason="Build: test",
        adjusted_score=1.0, absolute_profit_per_batch=5 * M, isk_per_hour=1.0, margin_pct=20.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=1, pipeline_stage="manufacturing",
        capital_return_per_day=0.1, market_cap_runs=min(runs, room_runs), market_room_runs=room_runs,
        overview_row={
            "type_id": type_id, "quantity": runs * 100, "blueprint_type_id": type_id + 1,
            "manufacturing_job": {
                "runs": runs, "material_cost": 1.0 * M * runs, "total_cost": 1.0 * M * runs,
                "manufacturing_job_cost": 0.0,
                "materials": {str(MAT): {"type_id": MAT, "quantity": runs}},
                "job_segments": [{"runs": runs, "source": "owned_blueprint_copy", "item_id": 1,
                                  "blueprint_material_efficiency": 10, "blueprint_time_efficiency": 20}],
            },
        },
    )


class _Blueprints:
    """Stands in for ChainPlanner's ledger hook: hands out the given blueprints once each."""

    def __init__(self, *blueprints):
        self.free = list(blueprints)
        self.calls = []

    def __call__(self, decision, runs, material_efficiency):
        self.calls.append((decision.type_id, runs, material_efficiency))
        return self.free.pop(0) if self.free else None


def _assign(decisions, chars, blueprints, budget=None, sub_parents=frozenset()):
    plan = ChainPlan(decisions=decisions, claim_parallel_blueprint=blueprints,
                     sub_request_parent_type_ids=frozenset(sub_parents))
    return CharacterAssigner().assign(
        plan, [], _Chars(chars), _AdminStub(), {MAT: {"vwap_5d": 1.0 * M}},
        blueprint_data=NO_SKILL_REQUIREMENTS, budget=budget,
    )


def _jobs(actions, type_id=100):
    return [a for a in actions if a.action_type == "manufacture" and a.type_id == type_id]


def test_room_for_three_more_batches_and_two_free_blueprints_gives_three_jobs():
    blueprints = _Blueprints({"item_id": 2, "owner_character_id": None}, {"item_id": 3, "owner_character_id": None})
    jobs = _jobs(_assign([_missiles(room_runs=40)], [_char(1, 5)], blueprints))
    assert len(jobs) == 3
    assert [j.runs for j in jobs] == [10, 10, 10]
    assert [j.blueprint_item_id for j in jobs[1:]] == [2, 3]
    assert all(j.materials == {MAT: 10} for j in jobs)
    assert "parallel job 2 of" in jobs[1].notes
    # Each extra job asks for a blueprint of the batch's runs at the batch's ME.
    assert blueprints.calls[0] == (100, 10, 10)


def test_no_extra_job_without_room_for_a_whole_batch():
    blueprints = _Blueprints({"item_id": 2, "owner_character_id": None})
    jobs = _jobs(_assign([_missiles(room_runs=19)], [_char(1, 5)], blueprints))
    assert len(jobs) == 1
    assert blueprints.calls == []


def test_no_extra_job_without_a_free_blueprint():
    jobs = _jobs(_assign([_missiles(room_runs=100)], [_char(1, 5)], _Blueprints()))
    assert len(jobs) == 1


def test_no_extra_job_without_a_free_slot():
    blueprints = _Blueprints({"item_id": 2, "owner_character_id": None})
    jobs = _jobs(_assign([_missiles(room_runs=100)], [_char(1, 1)], blueprints))
    assert len(jobs) == 1


def test_a_product_with_components_gets_extra_jobs_that_buy_them():
    """Its components are sized for the first job; an extra job buys them
    (the producer prices them at the market, so the batch's profit stands)."""
    blueprints = _Blueprints({"item_id": 2, "owner_character_id": None})
    jobs = _jobs(_assign([_missiles(room_runs=100)], [_char(1, 5)], blueprints, sub_parents={100}))
    assert len(jobs) == 2
    assert "its components are bought" in jobs[1].notes
    assert "components are bought" not in jobs[0].notes


def test_an_extra_job_beats_a_weaker_products_first_job():
    """Slots go by profit per slot-day: a second job of a 5M product before the first of a 1M one."""
    high = _missiles(room_runs=100, type_id=100)
    low = _missiles(room_runs=10, type_id=200)
    low.absolute_profit_per_batch = 1 * M
    for d in (high, low):
        d.overview_row["manufacturing_job"]["time_seconds"] = 3600.0
    blueprints = _Blueprints({"item_id": 2, "owner_character_id": None})
    actions = _assign([high, low], [_char(1, 2)], blueprints)
    assert len(_jobs(actions, 100)) == 2 and not _jobs(actions, 200)


def test_an_extra_job_cut_by_the_wallet_does_not_resize_the_first_jobs_components():
    from eve_online_industry_tracker.application.daily_planner.character_assigner import _BuildCharger
    seen = {}
    real = _BuildCharger.charge

    def spy(self, decision, action, batch_runs, *, extra=False):
        ok = real(self, decision, action, batch_runs, extra=extra)
        seen[extra] = dict(self.scale)
        return ok
    _BuildCharger.charge = spy
    try:
        blueprints = _Blueprints({"item_id": 2, "owner_character_id": None})
        budget = WalletBudget(balance=15 * M, reserve_pct=0)   # 10M a batch: the extra job is cut to 5 runs
        jobs = _jobs(_assign([_missiles(room_runs=100)], [_char(1, 5)], blueprints, budget=budget,
                             sub_parents={100}))
    finally:
        _BuildCharger.charge = real
    assert [j.runs for j in jobs] == [10, 5]
    assert seen[True] == {}


def test_extra_jobs_are_charged_to_the_wallet():
    blueprints = _Blueprints(*({"item_id": i, "owner_character_id": None} for i in range(2, 9)))
    budget = WalletBudget(balance=25 * M, reserve_pct=0)   # 10M a batch
    jobs = _jobs(_assign([_missiles(room_runs=100)], [_char(1, 9)], blueprints, budget=budget))
    # The last one cut to what is left, like any batch.
    assert [j.runs for j in jobs] == [10, 10, 5]
    assert budget.spent == 25 * M


def test_an_owner_bound_blueprint_is_installed_by_its_owner():
    blueprints = _Blueprints({"item_id": 2, "owner_character_id": 2})
    jobs = _jobs(_assign([_missiles(room_runs=20)], [_char(1, 5), _char(2, 5)], blueprints))
    assert len(jobs) == 2
    assert jobs[1].character_id == 2


def test_first_jobs_of_every_product_come_before_extra_jobs():
    """Breadth first: a lower product's first job is not pushed out by a higher product's extras."""
    high = _missiles(room_runs=100, type_id=100)
    low = _missiles(room_runs=10, type_id=200)
    low.capital_return_per_day = 0.05
    blueprints = _Blueprints({"item_id": 2, "owner_character_id": None})
    actions = _assign([high, low], [_char(1, 2)], blueprints)
    assert len(_jobs(actions, 100)) == 1 and len(_jobs(actions, 200)) == 1


# --- ChainPlanner's hook: a real free blueprint from the plan's ledger -----------

def _bpc(item_id, *, runs, me, owner=None):
    from types import SimpleNamespace
    return SimpleNamespace(type_id=999, item_id=item_id, is_blueprint_copy=True, blueprint_runs=runs,
                           blueprint_material_efficiency=me, blueprint_time_efficiency=20,
                           owner_character_id=owner)


def test_the_chain_planner_books_free_bpcs_of_the_batchs_me_and_runs_once_each():
    from test_chain_planner_keying import _SUB_PHASE1, _decision, _planner, _row
    row = _row(runs=10, blueprint_source_kind="owned_blueprint_copy",
               blueprint_copy={"item_id": 501},
               job_segments=[{"runs": 10, "source": "owned_blueprint_copy", "blueprint_item_id": 501,
                              "blueprint_material_efficiency": 10, "blueprint_time_efficiency": 20}],
               materials={"34": {"type_id": 34, "quantity": 10}})
    decision = _decision(row)
    phase1 = {**_SUB_PHASE1, "bpc_assets_by_type_id": {999: [
        _bpc(501, runs=10, me=10),   # the batch's own BPC
        _bpc(502, runs=20, me=10),   # free, enough runs, same ME
        _bpc(503, runs=10, me=8),    # other ME: other materials
        _bpc(504, runs=5, me=10),    # too few runs for the batch
        _bpc(505, runs=10, me=10),   # in an active job
    ]}, "busy_blueprint_item_ids": [505]}
    plan = _planner(own_parent_blueprints=False).plan_chain([decision], phase1)
    first = plan.claim_parallel_blueprint(decision, 10, 10)
    assert first == {"item_id": 502, "owner_character_id": None, "kind": "bpc"}
    assert plan.claim_parallel_blueprint(decision, 10, 10) is None
