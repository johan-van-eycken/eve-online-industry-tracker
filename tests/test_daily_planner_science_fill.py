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


def _pilot(char_id=1, research_slots=2, mfg_slots=1, extra=()):
    skills = [
        {"skill_id": 3406, "skill_name": "Laboratory Operation", "trained_skill_level": research_slots - 1},
        {"skill_id": 3387, "skill_name": "Mass Production", "trained_skill_level": mfg_slots - 1},
        *extra,
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
    assert (left["science"], left["science_reason"], left["manufacturing"]) == (2, "no eligible job", 0)
    assert left["character_name"] == "Pilot 1"


def test_free_slots_reason_wallet_when_the_budget_stopped_science():
    row = _researching(_parent(runs=1), fee=5 * M)
    assigner, actions = _assign([row], [_pilot(research_slots=2)], WalletBudget(balance=1 * M, reserve_pct=0))
    assert not _of(actions, "me_research")
    assert "no science today: wallet" in row.decision_reason
    assert assigner.free_slots_after[1]["science_reason"] == "wallet"


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


def test_science_the_wallet_cannot_pay_reserves_nothing():
    # ME research at 12M never fits a 10M wallet; the 5M build must not starve for it.
    row = _researching(_parent(runs=5, materials={MAT: 5}), fee=12 * M)
    budget = WalletBudget(balance=10 * M, reserve_pct=0)
    _assigner, actions = _assign([row], [_pilot(research_slots=1, mfg_slots=1)], budget)
    (mfg,) = _of(actions, "manufacture")
    assert mfg.runs == 5 and budget.spent == 5 * M
    assert not _of(actions, "me_research")
    assert "no science today: wallet" in row.decision_reason


def test_fill_pass_copies_the_t1_bpo_the_build_pass_sized_to_nothing():
    """Seed 52's shape: W0 holds corp T1 BPC 700 but nobody free can run its
    invention, so 700 goes to the pool; W1's copy of pilot 1's T1 BPO 70 is
    then sized to 0 jobs (the pooled BPC covers the attempts) and W1 cannot
    invent either. The fill pass copies BPO 70 on pilot 1's free slot."""
    import test_blueprint_ledger as L
    import test_daily_planner_assigned_inventor as A
    import test_daily_planner_character_blueprints as C
    import test_daily_planner_release_replan as T

    rows = [T._planned_t2(12345, "W0", 8.8, T.OTHER_ENCRYPTION), T._planned_t2(12346, "W1", 8.5, T.OTHER_ENCRYPTION)]
    pilots = [T._pilot(1, "P1", A.ENCRYPTION, lab=1, mass=1), T._pilot(2, "P2", A.ENCRYPTION, mass=1),
              T._pilot(3, "P3", T.OTHER_ENCRYPTION)]
    jobs = [T._busy(1, 1), T._busy(2, 8), T._busy(3, 8)]
    _plan, actions = T._plan_and_assign(
        rows, {998: [C._owned_bpo(70, 998, owner=1)]}, {998: [L._bpc(700, 998, 10)]}, pilots, jobs,
    )
    assert [(a.action_type, a.type_id, a.character_id) for a in actions] == [("copy", 998, 1)]


def _research_fill(manufactured):
    """_fill_science on a research row, one research slot free, with or
    without a manufacture action of its own from the build pass."""
    from datetime import datetime

    from eve_online_industry_tracker.application.daily_planner.character_assigner import _SkillGate
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction

    row = _researching(_parent(runs=1))
    assigner = CharacterAssigner()
    slots = assigner._compute_available_slots([_pilot(research_slots=1)], [], datetime.now())
    emitted = {id(row): [AssignedAction(
        type_id=100, type_name="Product 100", action_type="manufacture", character_id=1, character_name="Pilot 1",
        quantity=1, runs=1, estimated_cost_isk=None, estimated_profit_isk=None, estimated_completion=None, notes="",
    )] if manufactured else []}
    return assigner._fill_science(
        row, "me_research", [row], emitted, slots, [_pilot()], _SkillGate(NO_SKILL_REQUIREMENTS, slots), {},
        now=datetime.now(), probability_floor=0.0, price_map={}, charger=None,
    )


def test_fill_pass_never_researches_a_bpo_built_from_today():
    assert [a.action_type for a in _research_fill(manufactured=False)] == ["me_research"]
    assert _research_fill(manufactured=True) == []


def test_free_slots_reason_names_an_unknown_science_cost():
    row = _researching(_parent(runs=1))
    del row.overview_row["manufacturing_job"]["research_job_cost_by_level"]
    assigner, actions = _assign([row], [_pilot(research_slots=1)], WalletBudget(balance=1e12, reserve_pct=0))
    assert not _of(actions, "me_research")
    reason = assigner.free_slots_after[1]["science_reason"]
    assert reason.startswith("no science today: me_research cost unknown (") and reason in row.decision_reason


class _OnlyPilotOneResearches:
    """Blueprint 101's ME research needs Science 1, which only pilot 1 has."""

    def get(self, bp_type_id, default=None):
        if bp_type_id == 101:
            return blueprint(manufacturing=[], research_material=[skill(3402, "Science", 1)])
        return NO_SKILL_REQUIREMENTS.get(bp_type_id)


def test_wallet_reason_only_for_the_pilot_who_could_run_the_job():
    row = _researching(_parent(runs=1), fee=5 * M)
    science = {"skill_id": 3402, "skill_name": "Science", "trained_skill_level": 1}
    assigner, _actions = _assign(
        [row], [_pilot(1, research_slots=1, extra=[science]), _pilot(2, research_slots=1)],
        WalletBudget(balance=1 * M, reserve_pct=0), _OnlyPilotOneResearches(),
    )
    assert assigner.free_slots_after[1]["science_reason"] == "wallet"
    assert assigner.free_slots_after[2]["science_reason"] == "no eligible job"


def test_free_slots_reasons_when_the_budget_is_blocked():
    budget = WalletBudget(balance=1e12, reserve_pct=0, blocked_reason="corp wallet unknown")
    assigner, _actions = _assign([_parent(runs=1)], [_pilot(research_slots=2, mfg_slots=1)], budget)
    left = assigner.free_slots_after[1]
    assert (left["manufacturing"], left["manufacturing_reason"]) == (1, "corp wallet unknown")
    assert (left["science"], left["science_reason"]) == (2, "corp wallet unknown")


def test_free_mfg_reason_wallet_when_a_build_is_cut_for_money():
    # Two pilots, one 10-run build cut to what 5M pays: pilot 2's mfg slot is left for lack of ISK.
    budget = WalletBudget(balance=5 * M, reserve_pct=0)
    assigner, actions = _assign([_parent(runs=10, materials={MAT: 10})], [_pilot(1), _pilot(2)], budget)
    assert _of(actions, "manufacture")[0].runs < 10
    assert {s["manufacturing_reason"] for s in assigner.free_slots_after.values() if s["manufacturing"]} == {"wallet"}


def test_free_mfg_reason_no_eligible_job():
    assigner, _actions = _assign([], [_pilot(mfg_slots=2)], WalletBudget(balance=1e12, reserve_pct=0))
    left = assigner.free_slots_after[1]
    assert (left["manufacturing"], left["manufacturing_reason"]) == (2, "no eligible job")
