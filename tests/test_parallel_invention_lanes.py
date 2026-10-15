"""Task 12 (FU-56): invention and source-copy elapsed time over parallel lanes.

Fake characters, fake skill ids and fake durations throughout.
"""
from __future__ import annotations

import math
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.industry import invention_model as im
from eve_online_industry_tracker.application.industry.invention_model import (
    NO_DECRYPTOR,
    CharacterSlotCapacity,
    InventionContext,
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
    return service


def _fake_compute(**kw: Any) -> dict[str, Any]:
    """base x runs x (1 - 3% per Advanced Industry level). The selected
    character (no override) has Advanced Industry 0."""
    levels = kw.get("other_pilot_skill_levels_by_name") or {}
    factor = 1.0 - 0.03 * int(levels.get("Advanced Industry", 0))
    return {"duration_seconds": round(kw["base_time_seconds"] * kw["runs"] * factor), "total_job_cost": None}


def _capacity(slots: int, advanced_industry: int = 0) -> CharacterSlotCapacity:
    return CharacterSlotCapacity(slots, {"Laboratory Operation": slots - 1, "Advanced Industry": advanced_industry})


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
    assert parallel["note"] == im.PARALLEL_CAPACITY_NOTE


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
    5 lanes, ceil(7 / 5) = 2 rounds of the slowest pilot's 910 s."""
    four = {_SCI_A: 4, _SCI_B: 4, _ENC: 4}
    costs = _invention(
        levels={1: four, 2: four}, slots={1: _capacity(3, 3), 2: _capacity(2, 5)},
        base_pct=34.0, invented_runs=30, time_seconds=1000,
    )
    assert costs["success_probability"] == pytest.approx(0.34 * (1 + 8 / 30 + 4 / 40))
    assert costs["whole_attempts"] == 7
    assert costs["elapsed_parallel"]["lanes"] == 5
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


def test_source_copies_spread_over_the_owned_source_bpos():
    context = InventionContext(
        slot_capacity_by_character_id={1: _capacity(3), 2: _capacity(3)},
        blueprint_original_count_by_type_id={1001: 2},
    )
    costs = _source_copy(context, 5)
    # min(2 BPOs, 6 slots) = 2 lanes: ceil(5 / 2) = 3 rounds of 20 s.
    assert costs["elapsed_parallel"]["lanes"] == 2
    assert costs["elapsed_duration_seconds"] == 3 * 20


@pytest.mark.parametrize(("context", "reason"), [
    (InventionContext(slot_capacity_by_character_id={1: _capacity(3)}, blueprint_original_count_by_type_id={1001: 1}),
     im.ONE_SOURCE_BPO_REASON),
    (InventionContext(slot_capacity_by_character_id={1: _capacity(3)}), im.NO_BPO_COUNT_REASON),
    (InventionContext(blueprint_original_count_by_type_id={1001: 4}), im.NO_SLOT_DATA_REASON),
])
def test_source_copies_stay_sequential_without_a_second_bpo_or_slot_data(context, reason):
    costs = _source_copy(context, 5)
    assert costs["elapsed_duration_seconds"] == 5 * 20
    assert costs["elapsed_parallel"]["sequential_reason"] == reason


# --- producer and planner count the same slots -------------------------------

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
