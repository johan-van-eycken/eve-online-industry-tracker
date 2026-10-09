"""The invent action's chance, attempts and cost for the pilot it is assigned to.

Amendment A4. The producer sizes a row's invention for the corp's best
inventor of that blueprint (manufacturing_job.invention_chance, and
activity_breakdown.invention or, when an owned T2 BPC covers batch 1, the
later-batch invention_bpc_plan). CharacterAssigner can give the invent action
to another pilot, for example when the best inventor has no free research
slot. That pilot's chance is recomputed here with the same invention_model
functions as the producer (no second copy of the formula):

    chance = min(1, max(floor, min(1, base x skill factor x decryptor multiplier)))
    expected attempts = invented runs / runs per BPC / chance
    whole attempts    = ceil(ceil(invented runs / runs per BPC) / chance)

The decryptor stays the producer's choice (made for the best inventor); only
its multiplier is applied.

When the pilot's factor cannot be computed (the pilot's skills are not
loaded, or the row has no skill set for the item) the BUY uses the SDE base
chance (factor 1): a lower bound, so it can over-buy but never under-buy. A
WARNING is logged, the reason recorded, and the best inventor's chance is
shown in the note only. A loaded skill list without the item's skills is
level 0 and is computed. A skill level is never guessed.

When the odds are unknown nothing is sized: whole_attempts None and the
reason "datacore need unknown: ...", and the shopping list buys no inputs.

Cost: the producer's invention fee scaled by the expected attempts (it is
linear in them), plus the row's own expected inputs at the producer's line
totals, with every expected unit more (or fewer) than the best inventor's at
the planner's market price (phase-1 price map); an unpriced producer line
falls back to that price map. None when anything is unpriced or there is no
fee (the later-batch plan carries none).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from eve_online_industry_tracker.application.daily_planner.admin_setting import admin_setting
from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import price_map_unit_price
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.industry.invention_model import (
    DEFAULT_INVENTION_PROBABILITY_FLOOR,
    InventionSkillSet,
    effective_invention_probability,
    invention_attempts,
    invention_probability,
    invention_skill_factor,
)

logger = logging.getLogger(__name__)

DATACORE_NEED_UNKNOWN = "datacore need unknown"
NO_INVENTION_PLAN_REASON = "the row carries no sized invention plan"
NO_ITEM_SKILL_SET_REASON = "the row carries no invention skill set for this item"
PILOT_SKILLS_UNKNOWN_REASON = "the assigned pilot's skills are not loaded"


@dataclass(frozen=True)
class AssignedInvention:
    """The invent action's figures. probability / best_probability are the
    effective chances (floor and cap applied). whole_attempts is what the
    shopping list buys inputs for; None when the need is unknown. reason is
    set when the pilot's own chance could not be computed, or nothing could
    be sized."""

    probability: float | None
    best_probability: float | None
    expected_attempts: float | None
    whole_attempts: int | None
    cost_isk: float | None
    note: str | None
    reason: str | None = None


def invention_probability_floor(admin_settings: Any) -> float:
    """industry.invention_probability_floor, the producer's floor. The
    default when there is no settings store; a WARNING and the default for a
    value that is not a chance in (0, 1]."""
    value = admin_setting(admin_settings, "industry", "invention_probability_floor", None)
    if value is None:
        return DEFAULT_INVENTION_PROBABILITY_FLOOR
    if isinstance(value, (int, float)) and not isinstance(value, bool) and 0 < float(value) <= 1:
        return float(value)
    logger.warning(
        "Daily planner: industry.invention_probability_floor=%r is not a chance in (0, 1]; using %s",
        value, DEFAULT_INVENTION_PROBABILITY_FLOOR,
    )
    return DEFAULT_INVENTION_PROBABILITY_FLOOR


def _pct(probability: float) -> str:
    text = f"{probability * 100:.1f}".rstrip("0").rstrip(".")
    return f"{text}%"


def _positive_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if value > 0 else None


def _positive_int(value: Any) -> int | None:
    number = _positive_float(value)
    return int(number) if number is not None else None


@dataclass(frozen=True)
class _Sizing:
    invented_runs: int
    runs_per_bpc: int
    best_expected: float
    best_whole: int
    fee: float | None


def _sizing(row: dict[str, Any], best_probability: float | None) -> tuple[_Sizing | None, str]:
    """The best inventor's attempts, or (None, why the need is unknown)."""
    costs = orow.get_invention_costs(row)
    if costs is not None:
        if costs.get("cost_unknown_reason"):
            return None, str(costs["cost_unknown_reason"])
        invented_runs = _positive_int(costs.get("invented_runs"))
        runs_per_bpc = _positive_int(costs.get("runs_per_invented_blueprint"))
        best_expected = _positive_float(costs.get("runs"))
        best_whole = _positive_int(costs.get("whole_attempts"))
        if None not in (invented_runs, runs_per_bpc, best_expected, best_whole):
            raw_fee = costs.get("total_job_cost")
            fee = (
                float(raw_fee) if isinstance(raw_fee, (int, float)) and not isinstance(raw_fee, bool) and raw_fee >= 0
                else None
            )
            return _Sizing(int(invented_runs), int(runs_per_bpc), float(best_expected), int(best_whole), fee), ""
    # I1 case A: an owned T2 BPC (one the planner may not see) covers batch 1,
    # so the producer wrote only the later-batch plan: one batch of runs.
    plan = orow.get_invention_bpc_plan(row)
    if plan is not None and best_probability is not None:
        batch_runs = _positive_int(plan.get("batch_runs"))
        runs_per_bpc = _positive_int(plan.get("runs_per_invented_blueprint"))
        if batch_runs is not None and runs_per_bpc is not None:
            expected, whole = invention_attempts(batch_runs, runs_per_bpc, best_probability)
            if whole > 0:
                return _Sizing(batch_runs, runs_per_bpc, expected, whole, None), ""
    return None, orow.get_invention_datacore_need_unknown(row) or NO_INVENTION_PLAN_REASON


def _input_cost(
    row: dict[str, Any], ratio: float, price_map: Mapping[int, Any],
) -> float | None:
    """The row's own expected inputs for `ratio` x the best inventor's
    expected attempts; None when a line has no price at all."""
    lines = orow.get_own_expected_invention_inputs(row)
    if not lines:
        return None
    total = 0.0
    for line in lines:
        quantity = _positive_float(line.get("quantity"))
        if quantity is None:
            continue
        market, _ = price_map_unit_price(price_map.get(int(line.get("type_id") or 0)))
        own_price = _positive_float(line.get("unit_price"))
        line_total = _positive_float(line.get("line_total"))
        if line_total is None:
            price = own_price if own_price is not None else market
            if price is None:
                return None
            line_total = price * quantity
        extra_units = quantity * (ratio - 1.0)
        if extra_units:
            extra_price = market if market is not None else own_price
            if extra_price is None:
                return None
            line_total += extra_units * extra_price
        total += line_total
    return total


def assigned_invention(
    row: dict[str, Any],
    *,
    character_id: int,
    skill_levels_by_id: Mapping[int, int] | None,
    probability_floor: float,
    price_map: Mapping[int, Any],
) -> AssignedInvention:
    """Chance, attempts and cost of `row`'s invention for pilot `character_id`.
    `skill_levels_by_id` None: the pilot's skills are not loaded."""
    type_id = row.get("type_id")
    chance = orow.get_invention_chance(row) or {}
    costs = orow.get_invention_costs(row) or {}
    best_probability = _positive_float(chance.get("effective_probability")) or _positive_float(
        costs.get("success_probability")
    )
    sizing, unknown_why = _sizing(row, best_probability)
    if sizing is None or best_probability is None:
        reason = f"{DATACORE_NEED_UNKNOWN}: {unknown_why}"
        logger.warning(
            "Invent action for type_id=%s (pilot %s): %s; no invention inputs are bought for it",
            type_id, character_id, reason,
        )
        return AssignedInvention(None, best_probability, None, None, None, reason, reason)

    def cost(expected: float) -> float | None:
        if sizing.fee is None:
            return None
        ratio = expected / sizing.best_expected
        inputs = _input_cost(row, ratio, price_map)
        return None if inputs is None else sizing.fee * ratio + inputs

    inventor = chance.get("inventor_character_id")
    if inventor is not None and int(inventor) == int(character_id):
        return AssignedInvention(
            best_probability, best_probability, sizing.best_expected, sizing.best_whole,
            cost(sizing.best_expected), f"chance {_pct(best_probability)} (best inventor)",
        )

    base = _positive_float(chance.get("base_probability"))
    multiplier = _positive_float(chance.get("decryptor_multiplier"))
    if base is None or multiplier is None:
        # No chance breakdown at all: nothing to recompute from.
        reason = "the row carries no invention chance breakdown"
        logger.warning(
            "Invent action for type_id=%s (pilot %s): %s; keeping the producer's chance %s",
            type_id, character_id, reason, _pct(best_probability),
        )
        return AssignedInvention(
            best_probability, best_probability, sizing.best_expected, sizing.best_whole,
            cost(sizing.best_expected), f"chance {_pct(best_probability)} (producer; {reason})", reason,
        )

    skills = orow.get_invention_skill_set(row)
    reason: str | None = None
    factor: float | None = None
    if skills is None:
        why = chance.get("skills_unknown_reason") or "no science skill ids on the row"
        reason = f"{NO_ITEM_SKILL_SET_REASON} ({why})"
    elif skill_levels_by_id is None:
        reason = PILOT_SKILLS_UNKNOWN_REASON
    else:
        # A loaded list without the item's skills is level 0, not unknown.
        factor = invention_skill_factor(InventionSkillSet(skills[0], skills[1]), skill_levels_by_id)

    probability = effective_invention_probability(
        invention_probability(base, factor, multiplier), probability_floor,
    )
    expected, whole = invention_attempts(sizing.invented_runs, sizing.runs_per_bpc, probability)
    if reason is not None:
        logger.warning(
            "Invent action for type_id=%s (pilot %s): %s; buying at the SDE base chance %s "
            "(best inventor %s), which may over-buy",
            type_id, character_id, reason, _pct(probability), _pct(best_probability),
        )
        note = f"chance {_pct(probability)} (SDE base: {reason}), best inventor {_pct(best_probability)}"
    else:
        note = f"chance {_pct(probability)} (assigned pilot), best inventor {_pct(best_probability)}"
    return AssignedInvention(probability, best_probability, expected, whole, cost(expected), note, reason)
