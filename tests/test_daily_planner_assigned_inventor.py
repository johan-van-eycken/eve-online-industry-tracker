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


def _decision(row, *, stage="invention", type_id=12345, score=1.0):
    return ItemDecision(
        type_id=type_id, type_name="Widget II", decision="build", decision_reason="test",
        adjusted_score=score, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=2,
        pipeline_stage=stage, overview_row=row,
    )


def _plan(row, *, stage="invention"):
    return ChainPlan(decisions=[_decision(row, stage=stage)])


def _assign(row, chars, jobs=(), admin=None, *, stage="invention"):
    return CharacterAssigner().assign(
        _plan(row, stage=stage), list(jobs), _Chars(chars), admin or _Admin(), price_map=PRICES,
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
    """Minor 6, Task 12 fix rounds 2-3: the copy covers the attempts the T1
    BPC runs on hand do not, for the pilot free to invent today (the best
    inventor is busy: the weaker pilot's 5); with none on hand it is the only
    action, and with them on hand today's attempts are capped by them."""
    actions = _assign(_row(t1_bpo=True), [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])
    assert [a for a in actions if a.action_type == "invent"] == []
    (copy,) = [a for a in actions if a.action_type == "copy"]
    assert copy.runs == 5
    assert "no T1 BPC on hand: no invention today" in copy.notes
    row = _row(t1_bpo=True)
    row["t1_bpc_runs_on_hand"] = 4
    actions = _assign(row, [_best_inventor(), _weaker_pilot()], jobs=[_busy(BEST_ID)])
    (invent,) = [a for a in actions if a.action_type == "invent"]
    (copy,) = [a for a in actions if a.action_type == "copy"]
    assert copy.runs == 1  # 5 needed, 4 on hand
    assert invent.runs == 4  # the weaker pilot's 5, capped at the 4 runs on hand
    assert "capped at the 4 T1 BPC run(s) on hand" in invent.notes


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


# --- Task 12 fix round 1: invention split over the free qualifying inventors -----
# A second qualifying inventor at 3/3/3: 0.30 x (1 + 6/30 + 3/40) x 1.2 = 0.459.
# 2 BPCs over the lanes (0.525, 0.459), sum 0.984: ceil(2 / 0.984) = 3 rounds.
# The best lane runs 3 (1.575 expected BPCs), the second ceil(0.425 / 0.459) = 1.
# One pilot alone would run 4 in sequence: the same 4 attempts are bought.
SECOND_ID, COPIER_ID = 3, 4
P_SECOND = 0.30 * (1 + 6 / 30 + 3 / 40) * 1.2
COVERED = 3 * 0.525 + 1 * P_SECOND


def _second_inventor():
    return {"character_id": SECOND_ID, "character_name": "Second", "skills": {"skills": [
        _skill("Science A", 3, SCIENCE_A), _skill("Science B", 3, SCIENCE_B), _skill("Encryption", 3, ENCRYPTION),
    ]}}


def _copier():
    """No invention skills, two research slots: the copy actions' pilot."""
    return {"character_id": COPIER_ID, "character_name": "Copier", "skills": {"skills": [
        _skill("Laboratory Operation", 1, 3406),
    ]}}


def _invents(row, chars, jobs=()):
    return [a for a in _assign(row, chars, jobs) if a.action_type == "invent"]


def test_two_free_inventors_get_one_invent_action_each_at_their_own_chance():
    best, second = _invents(_row(), [_best_inventor(), _second_inventor()])
    assert (best.character_id, second.character_id) == (BEST_ID, SECOND_ID)
    assert best.invention_probability == pytest.approx(0.525)
    assert second.invention_probability == pytest.approx(P_SECOND)
    assert (best.runs, second.runs) == (3, 1)
    assert "lane 1 of 2" in best.notes and "lane 2 of 2" in second.notes
    # Each lane's share of the 2 expected BPCs over its own chance.
    assert best.invention_expected_attempts == pytest.approx(2 * 3 / COVERED)
    assert second.invention_expected_attempts == pytest.approx(2 * 1 / COVERED)
    # Fix round 2: the lanes' 2 x 4 / COVERED expected attempts are priced
    # once (Datacore A's FIFO discount counts once); each lane takes its share.
    total = 2 * 4 / COVERED
    assert best.estimated_cost_isk == pytest.approx(_cost(total) * 3 / 4)
    assert second.estimated_cost_isk == pytest.approx(_cost(total) * 1 / 4)
    # Capital Reserved: the fee of the attempts each lane launches.
    assert (best.estimated_job_fee_isk, second.estimated_job_fee_isk) == (pytest.approx(12.0), pytest.approx(4.0))


def test_the_shopping_list_buys_the_lanes_attempts_summed():
    invents = _invents(_row(), [_best_inventor(), _second_inventor()])
    items = ShoppingListBuilder().build(
        assigned_actions=invents, corp_assets=[], market_depth_cache=PRICES, admin_settings=_Admin(),
        blueprint_data=BLUEPRINTS, meta_resolver=_NoBlueprints(),
    )
    bought = {i.type_id: i.quantity for i in items}
    # (3 + 1) attempts x 2 datacores, one decryptor each: never fewer than the
    # best inventor alone (4 attempts).
    assert bought == {DATACORE_A: 8, DATACORE_B: 8, DECRYPTOR: 4}


def test_a_pilot_without_the_encryption_skill_gets_no_lane():
    (invent,) = _invents(_row(), [_best_inventor(), _weaker_pilot()])
    assert invent.character_id == BEST_ID and invent.runs == 4


def test_copies_go_one_per_free_corp_bpo_with_the_runs_spread():
    row = _row(t1_bpo=True)
    row["t1_bpo_count"] = 2
    actions = _assign(row, [_best_inventor(), _second_inventor(), _copier()])
    copies = [a for a in actions if a.action_type == "copy"]
    assert [a for a in actions if a.action_type == "invent"] == []  # no T1 BPC on hand yet
    # Task 14: one copy per invent lane (3 + 1 attempts), one copy job per BPO:
    # each lane later gets its own BPC of enough runs.
    assert [(c.quantity, c.runs) for c in copies] == [(1, 3), (1, 1)]
    assert copies[0].character_id == COPIER_ID  # the most free research slots
    assert [c.estimated_job_fee_isk for c in copies] == [pytest.approx(1.5), pytest.approx(0.5)]
    assert "BPO 1 of 2" in copies[0].notes
    assert "make 1 copy × 3 runs" in copies[0].notes


def test_one_corp_bpo_keeps_one_copy_action():
    actions = _assign(_row(t1_bpo=True), [_best_inventor(), _second_inventor(), _copier()])
    (copy,) = [a for a in actions if a.action_type == "copy"]
    # Task 14: lanes of 3 + 1 on one BPO: one job of 2 copies x 3 runs (one
    # job's copies share their runs, so sized at the larger lane).
    assert (copy.quantity, copy.runs) == (2, 6)


def _alt(character_id):
    """A 3/3/3 inventor with Laboratory Operation 4: five research slots."""
    alt = _second_inventor()
    alt["character_id"], alt["character_name"] = character_id, f"Alt {character_id}"
    alt["skills"]["skills"].append(_skill("Laboratory Operation", 4, 3406))
    return alt


def test_the_lanes_costs_add_up_to_one_pricing_of_their_expected_attempts():
    """Fix round 2 (review probe): five lanes priced one by one applied the
    held stock's discount five times (summing to 3019 ISK where 4543 is
    right, and a lane could go negative). Now the lanes' total expected
    attempts are priced once and split by share."""
    invents = _invents(_row(), [_best_inventor(), *(_alt(cid) for cid in (3, 5, 6, 7))])
    assert len(invents) > 2
    total_expected = sum(a.invention_expected_attempts for a in invents)
    assert sum(a.estimated_cost_isk for a in invents) == pytest.approx(_cost(total_expected))
    assert all(a.estimated_cost_isk > 0 for a in invents)
    for a in invents:
        assert a.estimated_cost_isk == pytest.approx(_cost(total_expected) * a.invention_expected_attempts / total_expected)


# --- fix round 2: copy first in the copying stage --------------------------------


def test_a_small_corp_of_inventors_copies_first_and_does_not_livelock():
    """Review probe: two inventors with one research slot each and an owned
    T1 BPO but no T1 BPC. Invention used to take both slots and the copy
    none, so no BPC was ever made. Now only the copy is planned today."""
    actions = _assign(_row(t1_bpo=True), [_best_inventor(), _second_inventor()])
    kinds = [a.action_type for a in actions]
    assert "invent" not in kinds
    (copy,) = [a for a in actions if a.action_type == "copy"]
    assert copy.runs == 4  # the row's whole attempts
    assert "no T1 BPC on hand: no invention today" in copy.notes


def test_bpc_runs_on_hand_cap_todays_invention_after_the_copy():
    """1 T1 BPC run on hand of 4 needed: the copy takes one slot for the
    other 3, the second slot invents the 1 attempt the BPC run allows."""
    row = _row(t1_bpo=True)
    row["t1_bpc_runs_on_hand"] = 1
    actions = _assign(row, [_best_inventor(), _second_inventor()])
    (copy,) = [a for a in actions if a.action_type == "copy"]
    (invent,) = [a for a in actions if a.action_type == "invent"]
    assert copy.runs == 3
    assert invent.runs == 1
    assert copy.character_id != invent.character_id
    assert "capped at the 1 T1 BPC run(s) on hand" in invent.notes
    # Bought for the attempt launched today only.
    assert _buy(invent) == {DATACORE_A: 2, DATACORE_B: 2, DECRYPTOR: 1}


def test_fewer_free_lanes_than_the_build_time_assumes_are_said_on_the_invent_action():
    row = _row()
    row["manufacturing_job"]["activity_breakdown"]["invention"]["elapsed_parallel"] = {
        "lanes": 3, "lanes_used": 3, "pilots_used": 3, "sequential_reason": None,
    }
    best, second = _invents(row, [_best_inventor(), _second_inventor()])
    note = "2 of 3 lanes free today; completion later than the overview's Build time"
    assert note in best.notes and note in second.notes
    (alone,) = _invents(row, [_best_inventor(), _second_inventor()], jobs=[_busy(SECOND_ID)])
    assert "1 of 3 lanes free today" in alone.notes
    full = _invents(_row(), [_best_inventor(), _second_inventor()])
    assert all("lanes free today" not in a.notes for a in full)


# --- fix round 3 ------------------------------------------------------------------

NO_MANUFACTURE_NOTE = "no manufacture until a T2 BPC exists"


def _buy_all(actions):
    items = ShoppingListBuilder().build(
        assigned_actions=actions, corp_assets=[], market_depth_cache=PRICES, admin_settings=_Admin(),
        blueprint_data=BLUEPRINTS, meta_resolver=_NoBlueprints(),
    )
    return {i.type_id: i.quantity for i in items}


@pytest.mark.parametrize("stage", ["copying", "invention"])
def test_no_manufacture_before_a_t2_bpc_exists(stage):
    """Review probe: a copying / invention-stage row got a full manufacture
    action, its batch materials bought and its fee reserved, though it can
    only start once invented BPCs are delivered."""
    row = _row(t1_bpo=stage == "copying")
    row["t1_bpc_runs_on_hand"] = 0
    row["manufacturing_job"]["materials"] = {"34": {"type_id": 34, "type_name": "Tritanium", "quantity": 5000}}
    actions = _assign(row, [_best_inventor(), _second_inventor(), _copier()], stage=stage)
    assert "manufacture" not in [a.action_type for a in actions]
    assert 34 not in _buy_all(actions)
    feeders = [a for a in actions if a.action_type in ("copy", "invent")]
    assert feeders and all(NO_MANUFACTURE_NOTE in a.notes for a in feeders)


def test_the_manufacturing_stage_still_manufactures():
    row = _row()
    row["needs_invention"] = False
    actions = _assign(row, [_best_inventor()], stage="manufacturing")
    (manufacture,) = [a for a in actions if a.action_type == "manufacture"]
    assert NO_MANUFACTURE_NOTE not in manufacture.notes


def test_copies_cover_the_weaker_free_pilot_when_the_best_inventor_is_busy():
    """The best inventor's slot is busy: the free 3/3/3 pilot alone needs
    ceil(2 / 0.459) = 5 attempts, more than the row's 4, so 5 are copied."""
    actions = _assign(_row(t1_bpo=True), [_best_inventor(), _second_inventor(), _copier()], jobs=[_busy(BEST_ID)])
    (copy,) = [a for a in actions if a.action_type == "copy"]
    assert copy.runs == 5
    assert copy.character_id == COPIER_ID


def test_the_copy_goes_to_a_non_inventor_when_one_can_copy():
    many_slots = _second_inventor()
    many_slots["skills"]["skills"].append(_skill("Laboratory Operation", 4, 3406))  # 5 slots
    single = _copier()
    single["skills"]["skills"] = []  # 1 slot, no invention skills
    actions = _assign(_row(t1_bpo=True), [_best_inventor(), many_slots, single])
    (copy,) = [a for a in actions if a.action_type == "copy"]
    assert copy.character_id == COPIER_ID


def test_without_a_free_inventor_after_the_copy_it_covers_the_weakest_qualifying_pilot():
    """The best inventor's only slot goes to the copy (nobody else can copy)
    and the 3/3/3 pilot is busy: nothing is free to size today's lanes, so
    the copy covers the weakest qualifying pilot alone: 5."""
    actions = _assign(_row(t1_bpo=True), [_best_inventor(), _second_inventor()], jobs=[_busy(SECOND_ID)])
    (copy,) = [a for a in actions if a.action_type == "copy"]
    assert copy.runs == 5


def test_a_dropped_lane_renumbers_the_lane_wording():
    row = _row(t1_bpo=True)
    row["t1_bpc_runs_on_hand"] = 1
    actions = _assign(row, [_best_inventor(), _second_inventor(), _copier()])
    (invent,) = [a for a in actions if a.action_type == "invent"]
    assert invent.runs == 1
    # Task 14: one BPC, so one lane from the start: no lane wording left over.
    assert "of 2" not in invent.notes


def test_an_unknown_copy_run_count_says_so():
    row = _row(t1_bpo=True, plan="unknown")
    row["t1_bpc_runs_on_hand"] = 2
    actions = _assign(row, [_best_inventor(), _second_inventor(), _copier()])
    (copy,) = [a for a in actions if a.action_type == "copy"]
    assert copy.runs is None
    assert "cover part" not in copy.notes
    assert "the copy runs are unknown" in copy.notes


def test_two_rows_share_the_t1_bpc_runs_on_hand_once():
    """Both rows invent from T1 source 998 and each carries the full free
    count (4). The first row (higher score) spends them; the second sees none
    left, so it copies first and does not invent."""
    first, second = _row(t1_bpo=True), _row(t1_bpo=True)
    second["type_id"] = 12346
    first["t1_bpc_runs_on_hand"] = second["t1_bpc_runs_on_hand"] = 4
    plan = ChainPlan(decisions=[_decision(first, stage="copying", score=2.0),
                                _decision(second, stage="copying", type_id=12346, score=1.0)])
    chars = [_best_inventor(), _second_inventor(), _copier(), {**_copier(), "character_id": 9, "character_name": "C2"}]
    actions = CharacterAssigner().assign(plan, [], _Chars(chars), _Admin(), price_map=PRICES)
    invents_first = [a for a in actions if a.action_type == "invent" and a.type_id == 12345]
    invents_second = [a for a in actions if a.action_type == "invent" and a.type_id == 12346]
    assert sum(a.runs for a in invents_first) == 4
    assert invents_second == []
    assert [a for a in actions if a.action_type == "copy" and a.type_id == 998]


# --- fix round 4 ------------------------------------------------------------------


def test_the_copy_is_re_sized_after_a_released_slot():
    """Two corp BPOs reserve both inventors' slots for copies. With nobody
    free the weakest qualifying pilot alone needs 5: 1 copy run beyond the 4
    on hand, so one reserved slot (the best inventor's) is given back.
    Re-sized with that lane free, the best inventor's 4 attempts are covered
    by the 4 runs on hand: no copy is needed and the other slot returns too.

    Corrected in Task 14 (I2): the 4 runs are ONE BPC, and an invention job
    uses one BPC, so one lane invents: the best inventor's 4 attempts, not
    3 + 1 from the same BPC."""
    row = _row(t1_bpo=True)
    row["t1_bpo_count"], row["t1_bpc_runs_on_hand"] = 2, 4
    actions = _assign(row, [_best_inventor(), _second_inventor()])
    assert [a for a in actions if a.action_type == "copy"] == []
    assert [(a.character_id, a.runs) for a in actions if a.action_type == "invent"] == [(BEST_ID, 4)]


def test_the_fewer_lanes_note_counts_the_lanes_kept_after_the_cap():
    row = _row(t1_bpo=True)
    row["t1_bpc_runs_on_hand"] = 1
    row["manufacturing_job"]["activity_breakdown"]["invention"]["elapsed_parallel"] = {
        "lanes": 2, "lanes_used": 2, "pilots_used": 2, "sequential_reason": None,
    }
    actions = _assign(row, [_best_inventor(), _second_inventor(), _copier()])
    (invent,) = [a for a in actions if a.action_type == "invent"]
    assert "1 of 2 lanes free today" in invent.notes
