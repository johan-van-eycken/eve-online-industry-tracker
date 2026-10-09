# tests/test_daily_planner_assigned_inventor.py
"""Amendment A4: an invent action uses the ASSIGNED pilot's chance.

The producer sizes a row's invention for the corp's best inventor. When the
CharacterAssigner gives the invent action to a weaker pilot (here: the best
inventor's only research slot is busy), the chance, the whole attempts, the
datacore and decryptor buy and the invention cost follow that pilot.

Fake numbers throughout. Base chance 30%, 20 invented runs, 10 runs per BPC
(2 BPCs). Best inventor: science 5 + 5, encryption 5 -> factor 1 + 10/30 +
5/40 = 1.458333. Weaker pilot: science 2 + 2, no encryption -> 1 + 4/30 =
1.133333.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone

import pytest

from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision
from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder

SCIENCE_A, SCIENCE_B, ENCRYPTION = 11433, 11454, 21790
DATACORE_A, DATACORE_B, DECRYPTOR = 20416, 20417, 34201
BEST_ID, WEAK_ID = 1, 2

BLUEPRINTS = {
    # T1 source blueprint 998 invents into T2 blueprint 1999 (makes product 12345).
    998: {
        "manufacturing": {"materials": [], "products": [{"type_id": 12344, "quantity": 1}]},
        "invention": {
            "materials": [
                {"type_id": DATACORE_A, "type_name": "Datacore A", "quantity": 2},
                {"type_id": DATACORE_B, "type_name": "Datacore B", "quantity": 2},
            ],
            "products": [{"type_id": 1999, "quantity": 10}],
        },
    },
    1999: {
        "manufacturing": {"materials": [], "products": [{"type_id": 12345, "quantity": 1}]},
        "invention": {"materials": [], "products": []},
    },
}
PRICES = {DATACORE_A: {"vwap_5d": 100.0}, DATACORE_B: {"vwap_5d": 100.0}, DECRYPTOR: {"vwap_5d": 500.0}}


class _Admin:
    def __init__(self, floor=None):
        self._floor = floor

    def get(self, section, key, default=None):
        if (section, key) == ("industry", "invention_probability_floor") and self._floor is not None:
            return self._floor
        return default


class _Chars:
    def __init__(self, chars):
        self._chars = chars

    def list_characters(self):
        return self._chars


class _NoBlueprints:
    def is_blueprint(self, type_id):
        return False

    def prefetch(self, type_ids):
        return None


def _skill(name, level, skill_id):
    return {"skill_id": skill_id, "skill_name": name, "group_name": "Science", "trained_skill_level": level}


def _best_inventor():
    return {"character_id": BEST_ID, "character_name": "Best", "skills": {"skills": [
        _skill("Science A", 5, SCIENCE_A), _skill("Science B", 5, SCIENCE_B), _skill("Encryption", 5, ENCRYPTION),
    ]}}


def _weaker_pilot(skills=None):
    entries = [_skill("Science A", 2, SCIENCE_A), _skill("Science B", 2, SCIENCE_B)] if skills is None else skills
    return {"character_id": WEAK_ID, "character_name": "Weaker", "skills": {"skills": entries}}


def _busy(character_id):
    """An active invention job filling the pilot's only research slot."""
    end = datetime.now(tz=timezone.utc).replace(tzinfo=None) + timedelta(days=1)
    return {"character_id": character_id, "activity_id": 8, "status": "active", "end_date": end}


def _row(*, multiplier, decryptor=True, skills_unknown_reason=None, invention_costs=True):
    """An overview row as the producer writes it, sized for the best inventor."""
    best_factor = 1 + 10 / 30 + 5 / 40
    best_p = min(1.0, 0.30 * best_factor * multiplier)
    expected = 2 / best_p
    mj = {
        "runs": 20,
        "invention_chance": {
            "base_probability": 0.30, "skill_factor": best_factor, "decryptor_multiplier": multiplier,
            "probability": best_p, "effective_probability": best_p,
            "inventor_character_id": BEST_ID,
            "science_skill_type_ids": [SCIENCE_A, SCIENCE_B], "encryption_skill_type_id": ENCRYPTION,
            "skills_unknown_reason": skills_unknown_reason, "note": None,
        },
        # Amortized inputs at the best inventor's expected attempts: 9000 ISK, fee 1000 ISK.
        "expected_invention_materials": {
            str(DATACORE_A): {"type_id": DATACORE_A, "quantity": 2 * expected, "line_total": 4500.0},
            str(DATACORE_B): {"type_id": DATACORE_B, "quantity": 2 * expected, "line_total": 4500.0},
            # A nested sub-build's input: not this action's invention.
            f"{DATACORE_A}@5555": {"type_id": DATACORE_A, "line_total": 99999.0, "invented_blueprint_type_id": 5555},
        },
    }
    if invention_costs:
        mj["activity_breakdown"] = {"invention": {
            "runs": expected, "whole_attempts": math.ceil(2 / best_p - 1e-9),
            "invented_runs": 20, "runs_per_invented_blueprint": 10,
            "success_probability": best_p, "total_job_cost": 1000.0,
        }}
    if decryptor:
        mj["invention_decryptor"] = {"type_id": DECRYPTOR, "type_name": "Fake Decryptor"}
    return {"type_id": 12345, "needs_invention": True, "manufacturing_job": mj}


def _plan(row):
    return ChainPlan(decisions=[ItemDecision(
        type_id=12345, type_name="Widget II", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=2,
        pipeline_stage="invention", overview_row=row,
    )])


def _invent(row, chars, jobs=(), admin=None):
    actions = CharacterAssigner().assign(_plan(row), list(jobs), _Chars(chars), admin or _Admin())
    (invent,) = [a for a in actions if a.action_type == "invent"]
    return invent


def _buy(action):
    items = ShoppingListBuilder().build(
        assigned_actions=[action], corp_assets=[], market_depth_cache=PRICES, admin_settings=_Admin(),
        blueprint_data=BLUEPRINTS, meta_resolver=_NoBlueprints(),
    )
    return {i.type_id: i.quantity for i in items}


def test_a_weaker_assigned_pilot_gets_its_own_lower_chance_and_buys_more():
    row = _row(multiplier=1.2)
    invent = _invent(row, [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])

    assert invent.character_id == WEAK_ID
    # 0.30 x (1 + 4/30) x 1.2 = 0.408; best 0.30 x 1.458333 x 1.2 = 0.525.
    assert invent.invention_probability == pytest.approx(0.408)
    assert invent.invention_best_probability == pytest.approx(0.525)
    # 2 BPCs / 0.408 = 4.902 expected, ceil -> 5 whole (best inventor: 4).
    assert invent.invention_expected_attempts == pytest.approx(2 / 0.408)
    assert invent.runs == 5
    # (1000 fee + 9000 inputs) x 0.525 / 0.408; the nested sub-build line is not counted.
    assert invent.estimated_cost_isk == pytest.approx(10_000.0 * 0.525 / 0.408)
    assert invent.estimated_cost_isk == pytest.approx(12_867.647, abs=0.001)
    assert invent.invention_chance_reason is None
    assert "chance 40.8% (assigned pilot), best inventor 52.5%" in invent.notes
    # 2 per attempt x 5 attempts; one decryptor per attempt.
    assert _buy(invent) == {DATACORE_A: 10, DATACORE_B: 10, DECRYPTOR: 5}


def test_the_best_inventor_keeps_the_producers_numbers():
    row = _row(multiplier=1.2)
    invent = _invent(row, [_best_inventor(), _weaker_pilot()])

    assert invent.character_id == BEST_ID
    best = row["manufacturing_job"]["activity_breakdown"]["invention"]
    assert invent.invention_probability == best["success_probability"]
    assert invent.invention_expected_attempts == best["runs"]
    assert invent.runs == best["whole_attempts"] == 4
    assert invent.estimated_cost_isk == 10_000.0
    assert invent.invention_chance_reason is None
    assert "chance 52.5% (best inventor)" in invent.notes
    assert _buy(invent) == {DATACORE_A: 8, DATACORE_B: 8, DECRYPTOR: 4}


def test_the_best_inventor_recomputed_without_its_id_gives_the_same_numbers():
    """The fixture sanitiser drops inventor_character_id: recomputing the best
    inventor's own chance must land on the producer's figures."""
    row = _row(multiplier=1.2)
    del row["manufacturing_job"]["invention_chance"]["inventor_character_id"]
    invent = _invent(row, [_best_inventor()])
    assert invent.invention_probability == pytest.approx(0.525)
    assert invent.runs == 4
    assert invent.estimated_cost_isk == pytest.approx(10_000.0)


def test_unknown_pilot_skills_keep_the_best_inventor_figure_with_a_warning(caplog):
    row = _row(multiplier=1.2)
    no_skills = {"character_id": WEAK_ID, "character_name": "Weaker"}
    with caplog.at_level(logging.WARNING):
        invent = _invent(row, [_best_inventor(), no_skills], jobs=[_busy(BEST_ID)])

    assert invent.character_id == WEAK_ID
    assert invent.invention_probability == pytest.approx(0.525)
    assert invent.runs == 4
    assert invent.estimated_cost_isk == 10_000.0
    assert invent.invention_chance_reason == "the assigned pilot's skills are not loaded"
    assert "assigned pilot unknown: the assigned pilot's skills are not loaded" in invent.notes
    assert any(
        r.levelno == logging.WARNING and "12345" in r.getMessage() and "not loaded" in r.getMessage()
        for r in caplog.records
    )
    assert _buy(invent) == {DATACORE_A: 8, DATACORE_B: 8, DECRYPTOR: 4}


def test_unknown_item_skills_keep_the_best_inventor_figure_with_a_warning(caplog):
    row = _row(multiplier=1.2, skills_unknown_reason="no corp character has its skills loaded")
    row["manufacturing_job"]["invention_chance"]["science_skill_type_ids"] = None
    with caplog.at_level(logging.WARNING):
        invent = _invent(row, [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])

    assert invent.runs == 4
    assert invent.invention_chance_reason == (
        "the item's invention skills are unknown (no corp character has its skills loaded)"
    )
    assert any(r.levelno == logging.WARNING and "invention skills are unknown" in r.getMessage()
               for r in caplog.records)


def test_without_a_decryptor_only_the_datacores_scale():
    row = _row(multiplier=1.0, decryptor=False)
    best = _invent(row, [_best_inventor(), _weaker_pilot()])
    weak = _invent(_row(multiplier=1.0, decryptor=False), [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])

    # Best 0.4375 -> ceil(2 / 0.4375) = 5; weaker 0.30 x 1.133333 = 0.34 -> ceil(5.88) = 6.
    assert (best.runs, weak.runs) == (5, 6)
    assert weak.invention_probability == pytest.approx(0.34)
    assert weak.decryptor_type_id is None
    assert _buy(best) == {DATACORE_A: 10, DATACORE_B: 10}
    assert _buy(weak) == {DATACORE_A: 12, DATACORE_B: 12}


def test_the_admin_floor_applies_to_the_assigned_pilot():
    row = _row(multiplier=1.2)
    invent = _invent(row, [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)], admin=_Admin(floor=0.45))
    # 0.408 is below the floor 0.45: ceil(2 / 0.45) = 5.
    assert invent.invention_probability == pytest.approx(0.45)
    assert invent.runs == 5


def test_a_row_without_a_sized_invention_buys_one_attempt_and_warns(caplog):
    row = _row(multiplier=1.2, invention_costs=False)
    with caplog.at_level(logging.WARNING):
        invent = _invent(row, [_best_inventor()])
    assert invent.runs is None and invent.estimated_cost_isk is None
    assert invent.invention_chance_reason.startswith("the row carries no sized invention plan")
    assert any(r.levelno == logging.WARNING and "no sized invention plan" in r.getMessage() for r in caplog.records)
    assert _buy(invent) == {DATACORE_A: 2, DATACORE_B: 2, DECRYPTOR: 1}


def test_invention_attempts_never_divides_by_zero():
    from eve_online_industry_tracker.application.industry.invention_model import invention_attempts

    assert invention_attempts(0, 10, 0.5) == (0.0, 0)
    assert invention_attempts(20, 10, 0.0) == (0.0, 0)
    assert invention_attempts(20, 10, 0.5) == (4.0, 4)
