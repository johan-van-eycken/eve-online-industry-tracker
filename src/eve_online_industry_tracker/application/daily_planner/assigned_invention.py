"""The invent action's chance, attempts and cost for the pilot it is assigned to.

Amendment A4. The producer sizes a row's invention for the corp's best
inventor of that blueprint (manufacturing_job.invention_chance and
activity_breakdown.invention). CharacterAssigner can give the invent action
to another pilot, for example when the best inventor has no free research
slot. That pilot's chance is recomputed here with the same invention_model
functions as the producer (no second copy of the formula):

    chance = min(1, max(floor, min(1, base x skill factor x decryptor multiplier)))
    expected attempts = invented runs / runs per BPC / chance
    whole attempts    = ceil(ceil(invented runs / runs per BPC) / chance)

The decryptor stays the producer's choice (made for the best inventor); only
its multiplier is applied. The action's invention cost (fee + amortized
inputs) scales with the expected attempts, both being linear in them.

When the assigned pilot's chance cannot be computed (the item's invention
skills or the pilot's skills are unknown) the best-inventor figures are kept,
a WARNING is logged and the reason is returned. A skill level is never guessed.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

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

NO_INVENTION_PLAN_REASON = (
    "the row carries no sized invention plan (manufacturing_job.activity_breakdown.invention), "
    "so the invention attempts are unknown"
)
ITEM_SKILLS_UNKNOWN_REASON = "the item's invention skills are unknown"
PILOT_SKILLS_UNKNOWN_REASON = "the assigned pilot's skills are not loaded"


@dataclass(frozen=True)
class AssignedInvention:
    """The invent action's figures. probability / best_probability are the
    effective chances (floor and cap applied). whole_attempts is what the
    shopping list buys inputs for; None when unknown. reason is set when the
    assigned pilot's own chance could not be used."""

    probability: float | None
    best_probability: float | None
    expected_attempts: float | None
    whole_attempts: int | None
    cost_isk: float | None
    note: str | None
    reason: str | None = None


def invention_probability_floor(admin_settings: Any) -> float:
    """industry.invention_probability_floor, the producer's floor; the
    producer's default when there is no settings store (test stubs)."""
    try:
        value = admin_settings.get("industry", "invention_probability_floor")
    except (AttributeError, KeyError):
        value = None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
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


def assigned_invention(
    row: dict[str, Any],
    *,
    character_id: int,
    skill_levels_by_id: Mapping[int, int],
    probability_floor: float,
) -> AssignedInvention:
    """Chance, attempts and cost of `row`'s invention for pilot `character_id`."""
    type_id = row.get("type_id")
    costs = orow.get_invention_costs(row) or {}
    chance = orow.get_invention_chance(row) or {}

    invented_runs = _positive_int(costs.get("invented_runs"))
    runs_per_bpc = _positive_int(costs.get("runs_per_invented_blueprint"))
    best_expected = _positive_float(costs.get("runs"))
    best_whole = _positive_int(costs.get("whole_attempts"))
    best_probability = _positive_float(chance.get("effective_probability")) or _positive_float(
        costs.get("success_probability")
    )
    if (
        invented_runs is None or runs_per_bpc is None or best_expected is None
        or best_whole is None or best_probability is None or costs.get("cost_unknown_reason")
    ):
        reason = NO_INVENTION_PLAN_REASON
        if costs.get("cost_unknown_reason"):
            reason = f"{reason} ({costs['cost_unknown_reason']})"
        logger.warning(
            "Invent action for type_id=%s (pilot %s): %s; buying the inputs of one attempt",
            type_id, character_id, reason,
        )
        return AssignedInvention(None, None, None, None, None, f"invention attempts unknown: {reason}", reason)

    raw_fee = costs.get("total_job_cost")
    fee = float(raw_fee) if isinstance(raw_fee, (int, float)) and not isinstance(raw_fee, bool) and raw_fee >= 0 else None
    inputs = orow.get_own_invention_input_cost(row)
    best_cost = fee + inputs if fee is not None and inputs is not None else None
    best = AssignedInvention(
        best_probability, best_probability, best_expected, best_whole, best_cost,
        f"chance {_pct(best_probability)} (best inventor)",
    )

    skills = orow.get_invention_skill_set(row)
    base = _positive_float(chance.get("base_probability"))
    multiplier = _positive_float(chance.get("decryptor_multiplier"))
    if skills is None or base is None or multiplier is None or chance.get("skills_unknown_reason"):
        why = chance.get("skills_unknown_reason") or "the row carries no invention skill set"
        return _keep_best(best, type_id, character_id, f"{ITEM_SKILLS_UNKNOWN_REASON} ({why})")

    inventor = chance.get("inventor_character_id")
    if inventor is not None and int(inventor) == int(character_id):
        return best
    if not skill_levels_by_id:
        return _keep_best(best, type_id, character_id, PILOT_SKILLS_UNKNOWN_REASON)

    factor = invention_skill_factor(InventionSkillSet(skills[0], skills[1]), skill_levels_by_id)
    probability = effective_invention_probability(
        invention_probability(base, factor, multiplier), probability_floor,
    )
    expected, whole = invention_attempts(invented_runs, runs_per_bpc, probability)
    cost = best_cost * expected / best_expected if best_cost is not None else None
    return AssignedInvention(
        probability, best_probability, expected, whole, cost,
        f"chance {_pct(probability)} (assigned pilot), best inventor {_pct(best_probability)}",
    )


def _keep_best(best: AssignedInvention, type_id: Any, character_id: int, reason: str) -> AssignedInvention:
    logger.warning(
        "Invent action for type_id=%s (pilot %s): %s; keeping the best inventor's chance %s, "
        "which may under-buy the invention inputs",
        type_id, character_id, reason, _pct(best.probability or 0.0),
    )
    return AssignedInvention(
        best.probability, best.best_probability, best.expected_attempts, best.whole_attempts, best.cost_isk,
        f"chance {_pct(best.probability or 0.0)} (best inventor; assigned pilot unknown: {reason})",
        reason,
    )
