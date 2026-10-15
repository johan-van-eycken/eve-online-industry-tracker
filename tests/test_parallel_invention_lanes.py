"""Task 12 (FU-56): invention and source-copy elapsed time over parallel lanes.

Fake characters, fake skill ids and fake durations throughout.
"""
from __future__ import annotations

import math
from datetime import datetime
from types import MethodType, SimpleNamespace
from typing import Any

import pytest

from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.industry import invention_model as im
from eve_online_industry_tracker.application.industry.invention_model import (
    NO_DECRYPTOR,
    CharacterSlotCapacity,
    InventionContext,
    SourceOriginals,
)
from eve_online_industry_tracker.application.industry.service import IndustryService

_SCI_A, _SCI_B, _ENC = 900001, 900002, 900003
_SKILL_ENTRIES = [
    {"type_id": _SCI_A, "type_name": "Fake Science A", "group_id": 270, "level": 1},
    {"type_id": _SCI_B, "type_name": "Fake Science B", "group_id": 270, "level": 1},
    {"type_id": _ENC, "type_name": "Fake Encryption", "group_id": 270, "level": 1},
]
_TRAINED = {_SCI_A: 3, _SCI_B: 3, _ENC: 3}


def _service() -> IndustryService:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
    # Each pilot's own industry payload: no skills, no implants.
    service._get_character_industry_modifier_payload = MethodType(  # type: ignore[attr-defined]
        lambda self, *, character_id: {"modifier_skills": [], "implants": []}, service,
    )
    return service


def _fake_compute(**kw: Any) -> dict[str, Any]:
    """base x runs x (1 - 3% per Advanced Industry level) x (1 - the pilot's
    implant time reduction for the activity). The selected character (no
    pilot payload) has Advanced Industry 0 and no implants."""
    payload = kw.get("pilot_industry_payload") or {"modifier_skills": [], "implants": []}
    levels = {e["type_name"]: e["trained_skill_level"] for e in payload["modifier_skills"]}
    implant = sum(
        m["value"] for i in payload["implants"] for m in i["modifiers"]
        if m["activity"] == kw["activity"] and m["metric"] == "time"
    )
    factor = (1.0 - 0.03 * int(levels.get("Advanced Industry", 0))) * (1.0 - implant)
    return {"duration_seconds": round(kw["base_time_seconds"] * kw["runs"] * factor), "total_job_cost": None}


def _capacity(
    slots: int, advanced_industry: int = 0, *, implant: float = 0.0, payload: bool = True,
) -> CharacterSlotCapacity:
    levels = {"Laboratory Operation": slots - 1, "Advanced Industry": advanced_industry}
    industry_payload = {
        "modifier_skills": [{"type_name": n, "trained_skill_level": lvl} for n, lvl in levels.items()],
        "implants": [{"modifiers": [{"activity": "invention", "metric": "time", "value": implant}]}] if implant else [],
    }
    return CharacterSlotCapacity(slots, levels, industry_payload if payload else None)


def _invention(
    *, levels, slots, base_pct=100.0, invented_runs=120, runs_per_bpc=10, time_seconds=100,
    selected_character_id=None,
) -> dict[str, Any]:
    context = InventionContext(
        inventor_skill_levels_by_character_id=levels,
        encryption_skill_type_ids=frozenset({_ENC}),
        slot_capacity_by_character_id=slots,
        selected_character_id=selected_character_id,
    )
    invention_job = {"materials": [], "skill_entries": [dict(e) for e in _SKILL_ENTRIES], "time_seconds": time_seconds}
    target = {"probability_pct": base_pct, "quantity": runs_per_bpc}
    costs, _ = _service()._expected_invention_costs(
        compute_activity=_fake_compute, invention_job=invention_job, target_entry=target,
        blueprint_type_id=9002, invented_runs=invented_runs, max_production_limit=10,
        adjusted_price_map={}, decryptor=NO_DECRYPTOR, invention_context=context,
        invented_blueprint_eiv=None,
    )
    return costs


def test_three_inventors_with_two_slots_each_run_twelve_attempts_in_two_rounds():
    # p = 1 (capped): 120 runs / 10 per BPC = 12 whole attempts.
    costs = _invention(
        levels={1: _TRAINED, 2: _TRAINED, 3: _TRAINED},
        slots={1: _capacity(2), 2: _capacity(2), 3: _capacity(2)},
    )
    assert costs["whole_attempts"] == 12
    assert costs["elapsed_duration_seconds"] == math.ceil(12 / 6) * 100
    parallel = costs["elapsed_parallel"]
    assert (parallel["lanes"], parallel["rounds"], parallel["sequential_reason"]) == (6, 2, None)
    assert (parallel["lanes_used"], parallel["pilots_used"], parallel["lane_whole_jobs"]) == (6, 3, 12)
    assert parallel["note"] == im.PARALLEL_CAPACITY_NOTE


def test_weaker_lanes_count_at_their_own_chance():
    """Fix round 1: the best pilot 5/5/5 with 1 slot plus four 1/1/1 alts
    with 5 slots each, 300 runs / 10 per BPC = 30 BPCs, base 34 %.
    p_best = 0.34 x (1 + 10/30 + 5/40) = 0.495833, p_alt = 0.34 x
    (1 + 2/30 + 1/40) = 0.371167; 21 lanes, sum 7.919167, so
    ceil(30 / 7.919167) = 4 rounds. The best inventor's 61 attempts over
    21 lanes would have said 3. Lanes run 4 each until the expected
    successes reach 30: the best lane and 18 alt lanes (28.72), then a 19th
    alt lane ceil(1.2927 / 0.371167) = 4: 80 attempts."""
    best = {_SCI_A: 5, _SCI_B: 5, _ENC: 5}
    alt = {_SCI_A: 1, _SCI_B: 1, _ENC: 1}
    costs = _invention(
        levels={1: best, 2: alt, 3: alt, 4: alt, 5: alt},
        slots={1: _capacity(1), 2: _capacity(5), 3: _capacity(5), 4: _capacity(5), 5: _capacity(5)},
        base_pct=34.0, invented_runs=300,
    )
    assert costs["success_probability"] == pytest.approx(0.34 * (1 + 10 / 30 + 5 / 40))
    parallel = costs["elapsed_parallel"]
    assert (parallel["lanes"], parallel["lanes_used"], parallel["pilots_used"]) == (21, 20, 5)
    assert parallel["rounds"] == 4
    assert costs["elapsed_duration_seconds"] == 4 * 100
    # Fix round 2: the row's attempts ARE the lanes' (cost, procurement, budget).
    # The best inventor alone: 61 whole, 30 / 0.495833 = 60.50 expected.
    p_best, p_alt = 0.34 * (1 + 10 / 30 + 5 / 40), 0.34 * (1 + 2 / 30 + 1 / 40)
    assert costs["best_inventor_whole_attempts"] == 61
    assert costs["best_inventor_expected_attempts"] == pytest.approx(30 / p_best)  # 60.50
    assert costs["whole_attempts"] == costs["lane_whole_attempts"] == parallel["lane_whole_jobs"] == 80
    # Expected: 30 BPCs x 80 / (4 x p_best + 76 x p_alt) = 79.49.
    assert costs["runs"] == pytest.approx(30 * 80 / (4 * p_best + 76 * p_alt))
    assert costs["runs"] == pytest.approx(79.49, abs=0.01)


def test_split_invention_attempts_is_the_single_lane_count_for_one_lane():
    assert im.split_invention_attempts(3, [0.4]) == (8,)  # ceil(3 / 0.4)
    assert im.split_invention_attempts(1, [0.5, 0.5, 0.5]) == (1, 1, 0)  # one round over two lanes
    assert im.split_invention_attempts(0, [0.5]) == (0,)
    assert im.split_invention_attempts(2, []) == ()


def test_a_character_missing_a_required_skill_is_not_counted():
    no_encryption = {_SCI_A: 5, _SCI_B: 5}
    costs = _invention(
        levels={1: _TRAINED, 2: _TRAINED, 3: no_encryption},
        slots={1: _capacity(2), 2: _capacity(2), 3: _capacity(2)},
    )
    assert costs["elapsed_parallel"]["lanes"] == 4
    assert costs["elapsed_duration_seconds"] == math.ceil(12 / 4) * 100


def test_a_skill_below_the_sde_required_level_is_not_trained_enough():
    skill_set = im.InventionSkillSet((_SCI_A, _SCI_B), _ENC)
    entries = [{**e, "level": 2} if e["type_id"] == _ENC else e for e in _SKILL_ENTRIES]
    required = im.required_invention_skill_levels(skill_set, entries)
    assert required == {_SCI_A: 1, _SCI_B: 1, _ENC: 2}
    assert im.can_run_invention(required, {_SCI_A: 1, _SCI_B: 1, _ENC: 2})
    assert not im.can_run_invention(required, {_SCI_A: 1, _SCI_B: 1, _ENC: 1})


def test_an_excluded_character_is_not_counted():
    """industry_enabled=False keeps a character out of both the inventor
    skills and the slot capacity the producer builds."""
    service = _service()
    trained = {"skills": [
        {"skill_id": _SCI_A, "skill_name": "Fake Science A", "trained_skill_level": 3},
        {"skill_id": _SCI_B, "skill_name": "Fake Science B", "trained_skill_level": 3},
        {"skill_id": _ENC, "skill_name": "Fake Encryption", "trained_skill_level": 3},
        {"skill_id": 990001, "skill_name": "Laboratory Operation", "trained_skill_level": 1},
    ]}
    service._state = SimpleNamespace(char_manager=SimpleNamespace(get_characters=lambda: [  # type: ignore[attr-defined]
        {"character_id": 1, "skills": trained},
        {"character_id": 2, "industry_enabled": False, "skills": trained},
    ]))
    slots = service._get_corp_slot_capacity()
    assert set(slots) == {1} and slots[1].research_slots == 2
    costs = _invention(levels=service._get_inventor_skill_levels(), slots=slots)
    assert costs["elapsed_parallel"]["lanes"] == 2
    assert costs["elapsed_duration_seconds"] == math.ceil(12 / 2) * 100


def test_unknown_slot_data_keeps_the_sequential_elapsed_time_with_a_reason():
    costs = _invention(levels={1: _TRAINED, 2: _TRAINED}, slots=None)
    assert costs["elapsed_duration_seconds"] == 12 * 100
    assert costs["elapsed_parallel"]["sequential_reason"] == im.NO_SLOT_DATA_REASON
    assert costs["elapsed_parallel"]["rounds"] == 12
    assert costs["elapsed_parallel"]["note"] is None


@pytest.mark.parametrize(("levels", "slots", "reason"), [
    ({1: {_SCI_A: 5}}, {1: _capacity(3)}, im.NO_QUALIFYING_INVENTOR_REASON),
    ({1: _TRAINED}, {}, im.NO_SLOTS_FOR_INVENTORS_REASON),
    ({}, {1: _capacity(3)}, f"invention skills unknown ({im.NO_CHARACTER_SKILLS})"),
])
def test_no_countable_inventor_keeps_the_sequential_elapsed_time(levels, slots, reason):
    costs = _invention(levels=levels, slots=slots)
    assert costs["elapsed_duration_seconds"] == costs["whole_attempts"] * 100
    assert costs["elapsed_parallel"]["sequential_reason"] == reason


def test_a_dc_ii_style_example_with_a_known_whole_attempt_count():
    """Fake DC II-like numbers: base 34 %, 10 runs per BPC, 30 runs invented
    (3 BPCs). Both pilots at 4/4/4: factor 1 + 8/30 + 4/40, p = 0.4647, so
    ceil(3 / 0.4647) = 7 whole attempts. Pilot 1 has 3 slots and Advanced
    Industry 3 (910 s), pilot 2 has 2 slots and Advanced Industry 5 (850 s):
    5 lanes, ceil(3 / (5 x 0.4647)) = 2 rounds (lanes 2, 2, 2, 1, 0) of the
    slowest used pilot's 910 s."""
    four = {_SCI_A: 4, _SCI_B: 4, _ENC: 4}
    costs = _invention(
        levels={1: four, 2: four}, slots={1: _capacity(3, 3), 2: _capacity(2, 5)},
        base_pct=34.0, invented_runs=30, time_seconds=1000,
    )
    assert costs["success_probability"] == pytest.approx(0.34 * (1 + 8 / 30 + 4 / 40))
    assert costs["whole_attempts"] == 7
    assert costs["elapsed_parallel"]["lanes"] == 5
    assert costs["lane_whole_attempts"] == 7
    # Every lane at the same chance: the lane expected attempts equal the
    # best inventor's, 3 / 0.4647 = 6.456 (fix round 2: unchanged).
    assert costs["runs"] == pytest.approx(costs["best_inventor_expected_attempts"])
    assert costs["runs"] == pytest.approx(3 / (0.34 * (1 + 8 / 30 + 4 / 40)))
    assert costs["elapsed_parallel"]["lane_duration_seconds"] == 910
    assert costs["elapsed_duration_seconds"] == 2 * 910  # sequential was 7 x 1000


def test_the_selected_character_is_timed_with_the_producers_own_calculator():
    """The selected pilot's lane gets no override (its own skills and
    implants); the slowest lane decides."""
    costs = _invention(
        levels={1: _TRAINED, 2: _TRAINED}, slots={1: _capacity(3, 5), 2: _capacity(3, 2)},
        selected_character_id=1,
    )
    # Pilot 1 (selected): the fake's default Advanced Industry 0 -> 100 s;
    # pilot 2: 94 s. ceil(12 / 6) = 2 rounds of 100 s.
    assert costs["elapsed_parallel"]["lane_duration_seconds"] == 100
    assert costs["elapsed_duration_seconds"] == 200


def test_another_pilot_is_timed_with_its_own_implants():
    """Fix round 1: one convention for another pilot, its own payload."""
    costs = _invention(levels={2: _TRAINED}, slots={2: _capacity(2, implant=0.1)})
    assert costs["elapsed_parallel"]["lane_duration_seconds"] == 90
    no_payload = _invention(levels={2: _TRAINED}, slots={2: _capacity(2, implant=0.1, payload=False)})
    assert no_payload["elapsed_parallel"]["lane_duration_seconds"] == 100  # implants unknown: none


def test_the_lane_note_names_slots_and_inventors():
    costs = _invention(
        levels={1: _TRAINED, 2: _TRAINED, 3: _TRAINED},
        slots={1: _capacity(2), 2: _capacity(2), 3: _capacity(2)},
    )
    assert im.elapsed_lane_note(costs["elapsed_parallel"]) == (
        "invention assumes 6 research slots over 3 inventors (capacity, not today's free slots)"
    )
    nobody = _invention(levels={1: {_SCI_A: 5}}, slots={1: _capacity(3)})
    assert im.elapsed_lane_note(nobody["elapsed_parallel"]) == (
        f"invention timed in one slot: {im.NO_QUALIFYING_INVENTOR_REASON}"
    )
    one = _invention(levels={1: _TRAINED}, slots={1: _capacity(1)})
    assert im.elapsed_lane_note(one["elapsed_parallel"]) is None


# --- source copies -----------------------------------------------------------

def _source_copy(context: InventionContext | None, whole_attempts: int) -> dict[str, Any]:
    source_row = {"blueprint_type_id": 1001, "copying_job": {"time_seconds": 20},
                  "manufacturing_job": {"materials": []}}
    costs, _ = _service()._invention_source_copy_costs(
        compute_activity=_fake_compute, invention_source_row=source_row,
        invention_costs={"runs": float(whole_attempts), "whole_attempts": whole_attempts},
        available_blueprint_copy_runs_by_type_id={}, adjusted_price_map={}, enabled=True,
        invention_context=context,
    )
    assert costs is not None
    return costs


def test_source_copies_spread_per_owner_and_leave_out_excluded_owners():
    """Pilot 1 owns 2 BPOs (2 lanes of its 3 slots), excluded pilot 9 owns 4
    (not counted: it is not in the slot data), the corp owns 1 (a slot left
    over: pilot 1's third). 3 lanes for 5 copy runs: 2 rounds of 20 s."""
    context = InventionContext(
        slot_capacity_by_character_id={1: _capacity(3), 2: _capacity(3)},
        source_originals_by_type_id={1001: SourceOriginals({1: 2, 9: 4}, corporation=1)},
    )
    assert im.source_copy_lanes(context, 1001).character_ids == (1, 1, 1)
    costs = _source_copy(context, 5)
    assert costs["elapsed_parallel"]["lanes"] == 3
    assert costs["elapsed_duration_seconds"] == 2 * 20


def test_a_character_bpo_is_capped_by_its_owners_slots():
    context = InventionContext(
        slot_capacity_by_character_id={1: _capacity(1), 2: _capacity(5)},
        source_originals_by_type_id={1001: SourceOriginals({1: 3})},
    )
    costs = _source_copy(context, 5)
    assert costs["elapsed_duration_seconds"] == 5 * 20  # one lane: pilot 2 cannot install pilot 1's BPOs
    assert costs["elapsed_parallel"]["sequential_reason"] == im.ONE_SOURCE_BPO_REASON


@pytest.mark.parametrize(("context", "reason"), [
    (InventionContext(slot_capacity_by_character_id={1: _capacity(3)},
                      source_originals_by_type_id={1001: SourceOriginals(corporation=1)}),
     im.ONE_SOURCE_BPO_REASON),
    (InventionContext(slot_capacity_by_character_id={1: _capacity(3)},
                      source_originals_by_type_id={1001: SourceOriginals({9: 4})}),
     im.ONE_SOURCE_BPO_REASON),
    (InventionContext(slot_capacity_by_character_id={1: _capacity(3)}), im.NO_BPO_COUNT_REASON),
    (InventionContext(source_originals_by_type_id={1001: SourceOriginals(corporation=4)}), im.NO_SLOT_DATA_REASON),
])
def test_source_copies_stay_sequential_without_a_second_lane_or_slot_data(context, reason):
    costs = _source_copy(context, 5)
    assert costs["elapsed_duration_seconds"] == 5 * 20
    assert costs["elapsed_parallel"]["sequential_reason"] == reason


def test_the_whole_run_budget_and_the_copies_use_the_lanes_attempts():
    """Fix round 2: whole_attempts is the lanes' total (7 here), and both the
    whole-run budget of owned source runs and the copy elapsed time spend it."""
    context = InventionContext(
        slot_capacity_by_character_id={1: _capacity(2)},
        source_originals_by_type_id={1001: SourceOriginals(corporation=2)},
    )
    source_row = {"blueprint_type_id": 1001, "copying_job": {"time_seconds": 20},
                  "manufacturing_job": {"materials": []}}
    runs = {1001: 3}  # 3 owned source runs
    costs, _ = _service()._invention_source_copy_costs(
        compute_activity=_fake_compute, invention_source_row=source_row,
        invention_costs={"runs": 6.5, "whole_attempts": 7, "best_inventor_whole_attempts": 5},
        available_blueprint_copy_runs_by_type_id=runs, adjusted_price_map={}, enabled=True,
        invention_context=context,
    )
    assert costs is not None
    assert runs[IndustryService._whole_source_runs_key(1001)] == 0  # 3 owned whole runs, 7 spent
    assert costs["elapsed_parallel"]["lane_whole_jobs"] == 4  # 7 - 3 owned
    assert costs["elapsed_duration_seconds"] == 2 * 20  # 4 runs over 2 corp BPOs


def test_the_producer_reads_bpo_owners_from_the_assets():
    originals = IndustryService._source_originals_by_owner({1001: [
        SimpleNamespace(character_id=1), SimpleNamespace(character_id=1), SimpleNamespace(corporation_id=77),
    ]})
    assert originals[1001] == SourceOriginals({1: 2}, corporation=1)


# --- producer and planner count the same research slots ----------------------

def test_producer_and_planner_count_the_same_research_slots():
    characters = [
        {"character_id": 1, "character_name": "Pilot A", "skills": {"skills": [
            {"skill_id": 990001, "skill_name": "Laboratory Operation", "trained_skill_level": 4},
            {"skill_id": 990002, "skill_name": "Advanced Laboratory Operation", "trained_skill_level": 2},
        ]}},
        {"character_id": 2, "character_name": "Pilot B", "skills": {"skills": []}},
        {"character_id": 3, "character_name": "Pilot C"},
        {"character_id": 4, "character_name": "Pilot D", "industry_enabled": False, "skills": {"skills": [
            {"skill_id": 990001, "skill_name": "Laboratory Operation", "trained_skill_level": 5},
        ]}},
    ]
    service = _service()
    service._state = SimpleNamespace(char_manager=SimpleNamespace(get_characters=lambda: characters))  # type: ignore[attr-defined]
    producer = {cid: cap.research_slots for cid, cap in service._get_corp_slot_capacity().items()}
    planner = {
        cid: slot["free_research"]
        for cid, slot in CharacterAssigner()._compute_available_slots(characters, [], datetime(2026, 1, 1)).items()
    }
    assert producer == planner == {1: 7, 2: 1, 3: 1}


def test_later_batches_per_bpc_attempts_never_under_buy_the_lanes():
    """Fix round 2: one BPC over lanes (0.9, 0.1, 0.1, 0.1) takes 2 attempts
    (split_invention_attempts), but 10 BPCs take 28, more than 10 x 2: the
    per-BPC figure the shopping list multiplies is ceil(28 / 10) = 3."""
    lanes = [0.9, 0.1, 0.1, 0.1]
    assert sum(im.split_invention_attempts(1, lanes)) == 2
    assert sum(im.split_invention_attempts(10, lanes)) == 28
    service = _service()
    service._full_batch_invention_whole_attempts = MethodType(  # type: ignore[attr-defined]
        lambda self, *, runs, **kw: (sum(im.split_invention_attempts(runs // 10, lanes)), None, None), service,
    )
    per_bpc, why = service._whole_attempts_per_blueprint(
        invention_job={}, blueprint_type_id=9002, runs_per_bpc=10, batch_runs=100, max_production_limit=100,
        adjusted_price_map={}, decryptor=NO_DECRYPTOR, invention_context=InventionContext(),
    )
    assert (per_bpc, why) == (3, None)
