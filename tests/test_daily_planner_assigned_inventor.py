# tests/test_daily_planner_assigned_inventor.py
"""Amendment A4: an invent action uses the ASSIGNED pilot's chance.

The producer sizes a row's invention for the corp's best inventor. When the
CharacterAssigner gives the invent action to a weaker pilot (here: the best
inventor's only research slot is busy), the chance, the whole attempts, the
datacore and decryptor buy and the invention cost follow that pilot.

Fake numbers throughout. Base chance 30%, 20 invented runs, 10 runs per BPC
(2 BPCs). Best inventor: science 5 + 5, encryption 5 -> factor 1 + 10/30 +
5/40 = 1.458333. Weaker pilot: science 2 + 2, no encryption -> 1 + 4/30 =
1.133333. Per attempt: 2 x Datacore A, 2 x Datacore B, 1 decryptor. Market
prices: datacores 100, decryptor 500. Datacore A is held at a FIFO cost of 50.
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
BEST_FACTOR = 1 + 10 / 30 + 5 / 40

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
    # Laboratory Operation 1: two research slots (the invent and the copy action).
    entries = [_skill("Science A", 2, SCIENCE_A), _skill("Science B", 2, SCIENCE_B)] if skills is None else skills
    entries = [*entries, _skill("Laboratory Operation", 1, 3406)]
    return {"character_id": WEAK_ID, "character_name": "Weaker", "skills": {"skills": entries}}


def _busy(character_id):
    """An active invention job filling the pilot's only research slot."""
    end = datetime.now(tz=timezone.utc).replace(tzinfo=None) + timedelta(days=1)
    return {"character_id": character_id, "activity_id": 8, "status": "active", "end_date": end}


def _row(*, multiplier=1.2, base=0.30, decryptor=True, skills_unknown_reason=None, plan="breakdown",
         inventor=BEST_ID, best_factor=BEST_FACTOR, unpriced_b=False, t1_bpo=False):
    """An overview row as the producer writes it, sized for the best inventor.

    plan="breakdown": batch 1 invents (activity_breakdown.invention).
    plan="bpc_plan": an owned T2 BPC covers batch 1, so only the later-batch
    invention_bpc_plan is written. plan="unknown": the odds are unknown."""
    best_p = min(1.0, base * (best_factor or 1.0) * multiplier)
    expected = 2 / best_p
    mj = {
        "runs": 20,
        # The install fees (D4): a priced row warns about no unknown fee.
        "manufacturing_job_cost": 2_000.0,
        "invention_chance": {
            "base_probability": base, "skill_factor": best_factor, "decryptor_multiplier": multiplier,
            "probability": best_p, "effective_probability": best_p,
            "inventor_character_id": inventor,
            "science_skill_type_ids": [SCIENCE_A, SCIENCE_B], "encryption_skill_type_id": ENCRYPTION,
            "skills_unknown_reason": skills_unknown_reason, "note": None,
        },
    }
    if plan == "breakdown":
        mj["activity_breakdown"] = {"invention": {
            "runs": expected, "whole_attempts": math.ceil(2 / best_p - 1e-9),
            "invented_runs": 20, "runs_per_invented_blueprint": 10,
            "success_probability": best_p, "total_job_cost": 1000.0,
            "job_cost_per_attempt": 4.0, "source_copy_job_cost_per_run": 0.5,
        }}
        lines = {
            # Held in stock: priced at its FIFO cost 50, below the market 100.
            str(DATACORE_A): {"type_id": DATACORE_A, "quantity": 2 * expected, "quantity_per_attempt": 2,
                              "unit_price": 50.0, "line_total": 100.0 * expected},
            str(DATACORE_B): {"type_id": DATACORE_B, "quantity": 2 * expected, "quantity_per_attempt": 2,
                              "unit_price": None if unpriced_b else 100.0,
                              "line_total": None if unpriced_b else 200.0 * expected},
            # A nested sub-build's input: not this action's invention.
            f"{DATACORE_A}@5555": {"type_id": DATACORE_A, "quantity": 9.0, "unit_price": 100.0,
                                    "line_total": 900.0, "invented_blueprint_type_id": 5555},
        }
        if decryptor:
            lines[str(DECRYPTOR)] = {"type_id": DECRYPTOR, "quantity": expected, "quantity_per_attempt": 1,
                                     "unit_price": 500.0, "line_total": 500.0 * expected}
        mj["expected_invention_materials"] = lines
    elif plan == "bpc_plan":
        mj["invention_bpc_plan"] = {
            "batch_runs": 20, "runs_per_invented_blueprint": 10,
            "whole_attempts_per_blueprint": math.ceil(1 / best_p - 1e-9),
            "owned_blueprint_runs": 20, "batch_one_invented_blueprints": 0, "per_attempt_materials": {},
            "job_cost_per_attempt": 4.0, "source_copy_job_cost_per_run": 0.5,
        }
    elif plan == "unknown":
        mj["activity_breakdown"] = {"invention": {
            "runs": 0.0, "whole_attempts": 0, "invented_runs": 20, "runs_per_invented_blueprint": 10,
            "success_probability": 0.0, "total_job_cost": None,
            "cost_unknown_reason": "no SDE success probability and an admin floor of 0",
        }}
        mj["invention_datacore_need_unknown"] = {
            "reason": "no SDE success probability and an admin floor of 0", "from_batch": 1,
        }
    if decryptor:
        mj["invention_decryptor"] = {"type_id": DECRYPTOR, "type_name": "Fake Decryptor"}
    row = {"type_id": 12345, "needs_invention": True, "manufacturing_job": mj}
    if t1_bpo:
        row.update({"has_t1_bpo": True, "t1_blueprint_type_id": 998})
    return row


def _plan(row):
    return ChainPlan(decisions=[ItemDecision(
        type_id=12345, type_name="Widget II", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=2,
        pipeline_stage="invention", overview_row=row,
    )])


def _assign(row, chars, jobs=(), admin=None):
    return CharacterAssigner().assign(
        _plan(row), list(jobs), _Chars(chars), admin or _Admin(), price_map=PRICES,
    )


def _invent(row, chars, jobs=(), admin=None):
    (invent,) = [a for a in _assign(row, chars, jobs, admin) if a.action_type == "invent"]
    return invent


def _buy(action):
    items = ShoppingListBuilder().build(
        assigned_actions=[action], corp_assets=[], market_depth_cache=PRICES, admin_settings=_Admin(),
        blueprint_data=BLUEPRINTS, meta_resolver=_NoBlueprints(),
    )
    return {i.type_id: i.quantity for i in items}


E_BEST = 2 / 0.525
E_WEAK = 2 / 0.408


def _cost(expected):
    """Fee scaled by the expected attempts; the best inventor's inputs at their
    own line totals (FIFO 50 for Datacore A), every unit more at the market."""
    return (
        1000.0 * expected / E_BEST
        + 800.0 * E_BEST
        + (expected - E_BEST) * (2 * 100.0 + 2 * 100.0 + 500.0)
    )


# --- the weaker pilot (A4) ---------------------------------------------------------


def test_a_weaker_assigned_pilot_gets_its_own_lower_chance_and_buys_more():
    row = _row()
    invent = _invent(row, [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])

    assert invent.character_id == WEAK_ID
    # 0.30 x (1 + 4/30) x 1.2 = 0.408; best 0.30 x 1.458333 x 1.2 = 0.525.
    assert invent.invention_probability == pytest.approx(0.408)
    assert invent.invention_best_probability == pytest.approx(0.525)
    # 2 BPCs / 0.408 = 4.902 expected, ceil -> 5 whole (best inventor: 4).
    assert invent.invention_expected_attempts == pytest.approx(E_WEAK)
    assert invent.runs == 5
    assert invent.invention_chance_reason is None
    assert "chance 40.8% (assigned pilot), best inventor 52.5%" in invent.notes
    # 2 per attempt x 5 attempts; one decryptor per attempt.
    assert _buy(invent) == {DATACORE_A: 10, DATACORE_B: 10, DECRYPTOR: 5}


def test_the_extra_expected_units_are_priced_at_the_market_not_the_fifo_cost():
    """Minor 2: the best inventor's inputs keep their line totals (Datacore A
    at FIFO 50); the weaker pilot's extra 1.0924 expected attempts buy at the
    market (900 ISK per attempt), the fee scales with the attempts."""
    invent = _invent(_row(), [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])
    assert invent.estimated_cost_isk == pytest.approx(_cost(E_WEAK))
    # 1286.765 fee + 3047.619 best inputs + 1.092437 x 900 (983.193) = 5317.577
    assert invent.estimated_cost_isk == pytest.approx(5317.577, abs=0.001)


def test_the_assigned_pilots_chance_caps_at_one():
    row = _row(base=0.90)
    invent = _invent(row, [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])
    # 0.90 x 1.133333 x 1.2 = 1.224 -> 1.0: one attempt per BPC.
    assert invent.invention_probability == 1.0
    assert invent.runs == 2


def test_the_admin_floor_applies_to_the_assigned_pilot():
    invent = _invent(_row(), [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)], admin=_Admin(floor=0.45))
    # 0.408 is below the floor 0.45: ceil(2 / 0.45) = 5.
    assert invent.invention_probability == pytest.approx(0.45)
    assert invent.runs == 5


def test_a_bad_floor_value_warns_and_uses_the_default(caplog):
    with caplog.at_level(logging.WARNING):
        invent = _invent(_row(), [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)], admin=_Admin(floor="x"))
    assert invent.invention_probability == pytest.approx(0.408)
    assert any(r.levelno == logging.WARNING and "invention_probability_floor" in r.getMessage()
               for r in caplog.records)


def test_without_a_decryptor_only_the_datacores_scale():
    best = _invent(_row(multiplier=1.0, decryptor=False), [_best_inventor(), _weaker_pilot()])
    weak = _invent(_row(multiplier=1.0, decryptor=False), [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])

    # Best 0.4375 -> ceil(2 / 0.4375) = 5; weaker 0.30 x 1.133333 = 0.34 -> ceil(5.88) = 6.
    assert (best.runs, weak.runs) == (5, 6)
    assert weak.invention_probability == pytest.approx(0.34)
    assert weak.decryptor_type_id is None
    assert _buy(best) == {DATACORE_A: 10, DATACORE_B: 10}
    assert _buy(weak) == {DATACORE_A: 12, DATACORE_B: 12}


def test_the_copy_action_makes_one_t1_bpc_run_per_invention_attempt():
    """Minor 6: the copy feeding the invention covers its whole attempts."""
    actions = _assign(_row(t1_bpo=True), [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])
    (invent,) = [a for a in actions if a.action_type == "invent"]
    (copy,) = [a for a in actions if a.action_type == "copy"]
    assert invent.runs == 5
    assert copy.runs == 5


# --- the best inventor -------------------------------------------------------------


def test_the_best_inventor_keeps_the_producers_numbers():
    row = _row()
    invent = _invent(row, [_best_inventor(), _weaker_pilot()])

    assert invent.character_id == BEST_ID
    best = row["manufacturing_job"]["activity_breakdown"]["invention"]
    assert invent.invention_probability == best["success_probability"]
    assert invent.invention_expected_attempts == best["runs"]
    assert invent.runs == best["whole_attempts"] == 4
    # Fee + the row's own line totals; the nested sub-build line is not counted.
    assert invent.estimated_cost_isk == pytest.approx(1000.0 + 800.0 * E_BEST)
    assert invent.invention_chance_reason is None
    assert "chance 52.5% (best inventor)" in invent.notes
    assert _buy(invent) == {DATACORE_A: 8, DATACORE_B: 8, DECRYPTOR: 4}


def test_the_best_inventor_recomputed_without_its_id_gives_the_same_numbers():
    """The fixture sanitiser drops inventor_character_id: recomputing the best
    inventor's own chance must land on the producer's figures."""
    row = _row()
    del row["manufacturing_job"]["invention_chance"]["inventor_character_id"]
    invent = _invent(row, [_best_inventor()])
    assert invent.invention_probability == pytest.approx(0.525)
    assert invent.runs == 4
    assert invent.estimated_cost_isk == pytest.approx(1000.0 + 800.0 * E_BEST)


def test_an_unpriced_input_line_falls_back_to_the_hub_price():
    """Minor 3: Datacore B has no producer price; the planner's price map (100) prices it."""
    invent = _invent(_row(unpriced_b=True), [_best_inventor()])
    assert invent.estimated_cost_isk == pytest.approx(1000.0 + 800.0 * E_BEST)


# --- unknown skills (I2, minor 1, minor 7) -----------------------------------------


def test_unloaded_pilot_skills_buy_at_the_sde_base_chance_with_a_warning(caplog):
    """I2: never the best inventor's chance (that under-buys); the SDE base
    chance x the decryptor is a lower bound."""
    no_skills = {"character_id": WEAK_ID, "character_name": "Weaker"}
    with caplog.at_level(logging.WARNING):
        invent = _invent(_row(), [_best_inventor(), no_skills], jobs=[_busy(BEST_ID)])

    assert invent.character_id == WEAK_ID
    # 0.30 x 1.2 = 0.36 -> ceil(2 / 0.36) = 6.
    assert invent.invention_probability == pytest.approx(0.36)
    assert invent.invention_best_probability == pytest.approx(0.525)
    assert invent.runs == 6
    assert invent.invention_chance_reason == "the assigned pilot's skills are not loaded"
    assert "chance 36% (SDE base: the assigned pilot's skills are not loaded), best inventor 52.5%" in invent.notes
    assert any(
        r.levelno == logging.WARNING and "12345" in r.getMessage() and "not loaded" in r.getMessage()
        for r in caplog.records
    )
    assert _buy(invent) == {DATACORE_A: 12, DATACORE_B: 12, DECRYPTOR: 6}


@pytest.mark.parametrize("skills", [
    [],  # loaded, nothing trained
    [_skill("Mass Production", 5, 3387)],  # loaded, none of the item's skills
])
def test_loaded_skills_without_the_items_skills_are_level_zero(skills, caplog):
    """Minor 7 / I2: a loaded list without the item's skills is level 0, computed, no warning."""
    with caplog.at_level(logging.WARNING):
        invent = _invent(_row(), [_best_inventor(), _weaker_pilot(skills=skills)], jobs=[_busy(BEST_ID)])
    assert invent.invention_probability == pytest.approx(0.36)
    assert invent.runs == 6
    assert invent.invention_chance_reason is None
    assert "chance 36% (assigned pilot), best inventor 52.5%" in invent.notes
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_the_pilots_chance_is_computed_when_the_producer_had_no_skills_loaded():
    """Minor 1: the producer used the base chance (no corp character with
    skills), but the item's skill set is known and the planner has this
    pilot's skills: compute them."""
    row = _row(skills_unknown_reason="no corp character has its skills loaded", inventor=None, best_factor=None)
    invent = _invent(row, [_weaker_pilot()])
    assert invent.invention_probability == pytest.approx(0.408)
    assert invent.invention_best_probability == pytest.approx(0.36)
    assert invent.runs == 5
    assert invent.invention_chance_reason is None


def test_an_unknown_item_skill_set_buys_at_the_sde_base_chance_with_a_warning(caplog):
    row = _row(skills_unknown_reason="the SDE lists no invention skills for this blueprint",
               inventor=None, best_factor=None)
    row["manufacturing_job"]["invention_chance"]["science_skill_type_ids"] = None
    with caplog.at_level(logging.WARNING):
        invent = _invent(row, [_weaker_pilot()])

    assert invent.invention_probability == pytest.approx(0.36)
    assert invent.runs == 6
    assert invent.invention_chance_reason == (
        "the row carries no invention skill set for this item "
        "(the SDE lists no invention skills for this blueprint)"
    )
    assert any(r.levelno == logging.WARNING and "no invention skill set" in r.getMessage()
               for r in caplog.records)


# --- unsized plans (I1) ------------------------------------------------------------


def test_an_owned_bpc_covering_batch_one_sizes_from_the_later_batch_plan():
    """I1 case A: no activity_breakdown.invention, but the producer's
    invention_bpc_plan and chance: one batch of 20 runs at the pilot's chance."""
    row = _row(plan="bpc_plan")
    weak = _invent(row, [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])
    best = _invent(_row(plan="bpc_plan"), [_best_inventor(), _weaker_pilot()])

    assert (best.runs, weak.runs) == (4, 5)
    assert weak.invention_expected_attempts == pytest.approx(E_WEAK)
    assert _buy(weak) == {DATACORE_A: 10, DATACORE_B: 10, DECRYPTOR: 5}


def test_the_later_batch_plans_unknown_cost_has_a_reason_a_warning_and_an_over_buy_flag(caplog):
    """Fix round 2, item 1: no top-level invention fee, so the cost is
    unknown, not guessed, and the action says so; one full batch is sized
    although an owned BPC (the planner may not see) can cover part of it."""
    with caplog.at_level(logging.WARNING):
        weak = _invent(_row(plan="bpc_plan"), [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])
    assert weak.estimated_cost_isk is None
    assert weak.invention_chance_reason == "invention cost unknown: the later-batch plan carries no fee"
    assert "sized for a full batch; an owned BPC may cover part of it" in weak.notes
    assert "invention cost unknown: the later-batch plan carries no fee" in weak.notes
    assert any(r.levelno == logging.WARNING and "the later-batch plan carries no fee" in r.getMessage()
               for r in caplog.records)


def test_unknown_odds_buy_no_inputs_and_say_datacore_need_unknown(caplog):
    """I1 case B: never a constant one attempt."""
    with caplog.at_level(logging.WARNING):
        invent = _invent(_row(plan="unknown"), [_best_inventor()])
        bought = _buy(invent)

    assert invent.runs is None and invent.estimated_cost_isk is None
    assert invent.invention_chance_reason.startswith("datacore need unknown")
    assert "datacore need unknown" in invent.notes
    assert bought == {}
    assert any(
        r.levelno == logging.WARNING and "ShoppingListBuilder" in r.getMessage()
        and "datacore need unknown" in r.getMessage()
        for r in caplog.records
    )


def test_a_row_without_any_invention_figures_is_datacore_need_unknown():
    row = _row(plan="none")
    invent = _invent(row, [_best_inventor()])
    assert invent.runs is None
    assert invent.invention_chance_reason.startswith("datacore need unknown")
    assert _buy(invent) == {}


def test_an_sde_base_chance_of_zero_keeps_the_producers_floored_chance(caplog):
    """Fix round 2, item 2: base 0 x any skill factor is 0, so every pilot
    invents at the admin floor: the producer's chance is exact."""
    row = _row()
    mj = row["manufacturing_job"]
    mj["invention_chance"].update({"base_probability": 0.0, "probability": 0.0, "effective_probability": 0.01})
    mj["activity_breakdown"]["invention"].update({"runs": 200.0, "whole_attempts": 200, "success_probability": 0.01})
    with caplog.at_level(logging.WARNING):
        invent = _invent(row, [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])
    assert invent.character_id == WEAK_ID
    assert invent.invention_probability == pytest.approx(0.01)
    assert invent.runs == 200
    assert invent.invention_chance_reason is None
    assert "SDE base chance 0: sized at the admin floor" in invent.notes
    assert "no invention chance breakdown" not in invent.notes
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_an_input_line_without_a_quantity_makes_the_cost_unknown_with_a_reason(caplog):
    """Fix round 2, item 3: never a silently dropped line."""
    row = _row()
    row["manufacturing_job"]["expected_invention_materials"][str(DATACORE_B)]["quantity"] = None
    with caplog.at_level(logging.WARNING):
        invent = _invent(row, [_best_inventor()])
    assert invent.estimated_cost_isk is None
    assert invent.invention_chance_reason == f"invention cost unknown: invention input {DATACORE_B} has no quantity"
    assert invent.runs == 4  # the buy is unaffected
    assert any(r.levelno == logging.WARNING and "has no quantity" in r.getMessage() for r in caplog.records)


def test_invention_attempts_never_divides_by_zero():
    from eve_online_industry_tracker.application.industry.invention_model import invention_attempts

    assert invention_attempts(0, 10, 0.5) == (0.0, 0)
    assert invention_attempts(20, 10, 0.0) == (0.0, 0)
    assert invention_attempts(20, 10, 0.5) == (4.0, 4)


def test_the_invent_fee_is_paid_per_attempt_the_assigned_pilot_launches():
    """D4: a weaker pilot launches 6 whole attempts (best inventor: 4), so
    the invent action's install fee is 6 x 4.0, not the best inventor's 4 x 4.0."""
    best = _invent(_row(), [_best_inventor(), _weaker_pilot(skills=[])])
    weak = _invent(_row(), [_best_inventor(), _weaker_pilot(skills=[])], jobs=[_busy(BEST_ID)])
    assert (best.runs, best.estimated_job_fee_isk) == (4, pytest.approx(16.0))
    assert (weak.runs, weak.estimated_job_fee_isk) == (6, pytest.approx(24.0))
