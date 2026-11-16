# tests/test_daily_planner_science_fill.py
"""Phase 6 sets the science budget aside before builds, fills the free
science slots, reports what is left per pilot, and plans no job for a T2
product whose T1 BPO nobody owns. Fake data."""
from __future__ import annotations

from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan
from eve_online_industry_tracker.application.daily_planner.wallet_budget import WalletBudget
from fakes import _Chars
from planner_skill_fakes import NO_SKILL_REQUIREMENTS, blueprint, skill
from test_daily_planner_budget_assignment import MAT, M, _AdminStub, _parent

PRICES = {MAT: {"vwap_5d": 1.0 * M}}


def _pilot(char_id=1, research_slots=2, mfg_slots=1):
    skills = [
        {"skill_id": 3406, "skill_name": "Laboratory Operation", "trained_skill_level": research_slots - 1},
        {"skill_id": 3387, "skill_name": "Mass Production", "trained_skill_level": mfg_slots - 1},
    ]
    return {"character_id": char_id, "character_name": f"Pilot {char_id}", "skills": {"skills": skills, "total_sp": 0}}


def _researching(decision, fee=1 * M):
    """The row's BPO needs ME 0 -> 1, at `fee`."""
    row = decision.overview_row
    row.update(needs_me_research=True, me_current=0, me_research_target=1)
    row["manufacturing_job"]["research_job_cost_by_level"] = {
        "research_material": [0.0] + [fee] * 10, "research_time": [0.0] * 11,
    }
    return decision


class _RecordingBudget(WalletBudget):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.asides: list[float] = []

    def set_aside(self, amount):
        self.asides.append(amount)
        return super().set_aside(amount)


def _assign(decisions, chars, budget, blueprint_data=NO_SKILL_REQUIREMENTS, assigner=None):
    assigner = assigner or CharacterAssigner()
    actions = assigner.assign(
        ChainPlan(decisions=decisions), [], _Chars(chars), _AdminStub(), dict(PRICES),
        blueprint_data=blueprint_data, budget=budget,
    )
    return assigner, actions


def _of(actions, kind):
    return [a for a in actions if a.action_type == kind]


def test_science_set_aside_before_builds():
    # A builds 100 runs at 1M each and ranks first; B's research comes after A's build.
    a = _researching(_parent(type_id=100, runs=100, crpd=2.0))
    b = _researching(_parent(type_id=200, runs=1, crpd=1.0))
    budget = WalletBudget(balance=100 * M, reserve_pct=0)
    _assigner, actions = _assign([a, b], [_pilot(research_slots=2, mfg_slots=1)], budget)
    assert len(_of(actions, "me_research")) == 2
    (mfg,) = _of(actions, "manufacture")
    assert mfg.runs <= 98
    assert budget.spent <= 100 * M


def test_fill_pass_uses_free_science_slots():
    rows = [_researching(_parent(type_id=t, runs=1, crpd=c)) for t, c in ((100, 3.0), (200, 2.0), (300, 1.0))]
    _assigner, actions = _assign(rows, [_pilot(research_slots=3, mfg_slots=1)], WalletBudget(balance=1e12, reserve_pct=0))
    assert sorted(a.type_id for a in _of(actions, "me_research")) == [101, 201, 301]
    assert len(_of(actions, "manufacture")) == 1


class _BlueprintData:
    """Blueprint 201's ME research needs Rigging 4, which nobody has."""

    def get(self, bp_type_id, default=None):
        if bp_type_id == 201:
            return blueprint(manufacturing=[], research_material=[skill(26252, "Rigging", 4)])
        return NO_SKILL_REQUIREMENTS.get(bp_type_id)


def test_skill_blocked_research_reserves_nothing():
    runnable = _researching(_parent(type_id=100, runs=1, crpd=2.0), fee=1 * M)
    blocked = _researching(_parent(type_id=200, runs=1, crpd=1.0), fee=5 * M)
    budget = _RecordingBudget(balance=1e12, reserve_pct=0)
    _assigner, actions = _assign([runnable, blocked], [_pilot(research_slots=2)], budget, _BlueprintData())
    assert budget.asides == [1 * M]
    assert [a.type_id for a in _of(actions, "me_research")] == [101]


def test_free_slots_reason_no_eligible_job():
    assigner, _actions = _assign([_parent(runs=1)], [_pilot(research_slots=2)], WalletBudget(balance=1e12, reserve_pct=0))
    left = assigner.free_slots_after[1]
    assert (left["science"], left["reason"], left["manufacturing"]) == (2, "no eligible job", 0)
    assert left["character_name"] == "Pilot 1"


def test_free_slots_reason_wallet_when_the_budget_stopped_science():
    row = _researching(_parent(runs=1), fee=5 * M)
    assigner, actions = _assign([row], [_pilot(research_slots=2)], WalletBudget(balance=1 * M, reserve_pct=0))
    assert not _of(actions, "me_research")
    assert "no science today: wallet" in row.decision_reason
    assert assigner.free_slots_after[1]["reason"] == "wallet"


def test_unowned_t1_bpo_gets_no_jobs():
    from test_chain_planner_keying import INVENTION_BLUEPRINT_DATA, _decision, _planner, _t2_row

    decision = _decision(_t2_row(), meta_group_id=2)
    planner = _planner()
    advised = []
    planner._analyze_bpo_investment = lambda **kwargs: advised.append(kwargs["decision"])
    plan = planner.plan_chain([decision], {"bpo_assets_by_type_id": {}, "blueprint_data": INVENTION_BLUEPRINT_DATA})
    assert advised == [decision]  # the BPO advice still runs
    actions = CharacterAssigner().assign(
        plan, [], _Chars([_pilot(research_slots=3, mfg_slots=3)]), _AdminStub(),
        blueprint_data=NO_SKILL_REQUIREMENTS, budget=WalletBudget(balance=1e12, reserve_pct=0),
    )
    assert not [a for a in actions if a.action_type in ("invent", "copy", "manufacture")]
    assert "T1 BPO 999 not owned: buy it first (see BPO advice)" in decision.decision_reason
