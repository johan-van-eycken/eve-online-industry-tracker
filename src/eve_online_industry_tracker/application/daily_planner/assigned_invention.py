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
falls back to that price map. None, with an "invention cost unknown: ..."
reason, a WARNING and the note, when a line has no quantity or price or there
is no fee (the later-batch plan carries none; its action is also flagged as
sized for a full batch, which an owned BPC may partly cover).

An SDE base chance of 0 makes every pilot's chance the admin floor, so the
producer's floored chance is kept (exact) and named in the note.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

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
    split_invention_attempts,
)

logger = logging.getLogger(__name__)

DATACORE_NEED_UNKNOWN = "datacore need unknown"
NO_INVENTION_PLAN_REASON = "the row carries no sized invention plan"
NO_ITEM_SKILL_SET_REASON = "the row carries no invention skill set for this item"
PILOT_SKILLS_UNKNOWN_REASON = "the assigned pilot's skills are not loaded"
INVENTION_COST_UNKNOWN = "invention cost unknown"
LATER_BATCH_NO_FEE = "the later-batch plan carries no fee"
LATER_BATCH_OVER_BUY_NOTE = "sized for a full batch; an owned BPC may cover part of it"
ZERO_BASE_CHANCE_NOTE = "SDE base chance 0: sized at the admin floor"


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
    #: Sized from invention_bpc_plan (I1 case A): one full batch, no fee.
    later_batch: bool = False
    #: The expected attempts the row's fee and expected input lines are
    #: priced at: the producer's lanes' when they run in parallel,
    #: else the best inventor's. None: best_expected.
    basis_expected: float | None = None

    @property
    def basis(self) -> float:
        return self.basis_expected if self.basis_expected is not None else self.best_expected


def _sizing(row: dict[str, Any], best_probability: float | None) -> tuple[_Sizing | None, str]:
    """The best inventor's attempts, or (None, why the need is unknown)."""
    costs = orow.get_invention_costs(row)
    if costs is not None:
        if costs.get("cost_unknown_reason"):
            return None, str(costs["cost_unknown_reason"])
        invented_runs = _positive_int(costs.get("invented_runs"))
        runs_per_bpc = _positive_int(costs.get("runs_per_invented_blueprint"))
        basis_expected = _positive_float(costs.get("runs"))
        # The best inventor alone; a row from before it
        # carries only the row's attempts, which were the best inventor's.
        best_expected = _positive_float(costs.get("best_inventor_expected_attempts")) or basis_expected
        best_whole = _positive_int(costs.get("best_inventor_whole_attempts")) or _positive_int(costs.get("whole_attempts"))
        if None not in (invented_runs, runs_per_bpc, best_expected, best_whole, basis_expected):
            raw_fee = costs.get("total_job_cost")
            fee = (
                float(raw_fee) if isinstance(raw_fee, (int, float)) and not isinstance(raw_fee, bool) and raw_fee >= 0
                else None
            )
            return _Sizing(
                int(invented_runs), int(runs_per_bpc), float(best_expected), int(best_whole), fee,
                basis_expected=float(basis_expected),
            ), ""
    # I1 case A: an owned T2 BPC (one the planner may not see) covers batch 1,
    # so the producer wrote only the later-batch plan: one batch of runs.
    plan = orow.get_invention_bpc_plan(row)
    if plan is not None and best_probability is not None:
        batch_runs = _positive_int(plan.get("batch_runs"))
        runs_per_bpc = _positive_int(plan.get("runs_per_invented_blueprint"))
        if batch_runs is not None and runs_per_bpc is not None:
            expected, whole = invention_attempts(batch_runs, runs_per_bpc, best_probability)
            if whole > 0:
                return _Sizing(batch_runs, runs_per_bpc, expected, whole, None, later_batch=True), ""
    return None, orow.get_invention_datacore_need_unknown(row) or NO_INVENTION_PLAN_REASON


def _input_cost(
    row: dict[str, Any], ratio: float, price_map: Mapping[int, Any],
) -> tuple[float | None, str | None]:
    """(ISK of the row's own expected inputs for `ratio` x the best
    inventor's expected attempts, None) or (None, why it is unknown): no
    lines, a line without a quantity, or a line with no price at all."""
    lines = orow.get_own_expected_invention_inputs(row)
    if not lines:
        return None, "the row carries no expected invention inputs"
    total = 0.0
    for line in lines:
        type_id = int(line.get("type_id") or 0)
        quantity = _positive_float(line.get("quantity"))
        if quantity is None:
            return None, f"invention input {type_id} has no quantity"
        market, _ = price_map_unit_price(price_map.get(type_id))
        own_price = _positive_float(line.get("unit_price"))
        line_total = _positive_float(line.get("line_total"))
        if line_total is None:
            price = own_price if own_price is not None else market
            if price is None:
                return None, f"invention input {type_id} has no price"
            line_total = price * quantity
        extra_units = quantity * (ratio - 1.0)
        if extra_units:
            extra_price = market if market is not None else own_price
            if extra_price is None:
                return None, f"invention input {type_id} has no market price"
            line_total += extra_units * extra_price
        total += line_total
    return total, None


@dataclass(frozen=True)
class _PilotChance:
    """One pilot's chance for the row. keep_producer: the producer's
    figures are exact for this pilot (the best inventor, or an SDE base
    chance of 0)."""

    probability: float
    note: str
    reason: str | None = None
    keep_producer: bool = False


def _pilot_chance(
    row: dict[str, Any], chance: Mapping[str, Any], best_probability: float, *,
    character_id: int, skill_levels_by_id: Mapping[int, int] | None, probability_floor: float,
) -> _PilotChance:
    """The A4 rule for one pilot (the single copy: the single invent action
    and every parallel lane use it)."""
    type_id = row.get("type_id")
    inventor = chance.get("inventor_character_id")
    if inventor is not None and int(inventor) == int(character_id):
        return _PilotChance(best_probability, f"chance {_pct(best_probability)} (best inventor)", keep_producer=True)
    raw_base = chance.get("base_probability")
    multiplier = _positive_float(chance.get("decryptor_multiplier"))
    if isinstance(raw_base, (int, float)) and not isinstance(raw_base, bool) and raw_base <= 0 and multiplier:
        # 0 x any skill factor is 0: every pilot invents at the admin floor,
        # so the producer's (floored) chance is exact for this pilot too.
        logger.info(
            "Invent action for type_id=%s (pilot %s): %s (%s)",
            type_id, character_id, ZERO_BASE_CHANCE_NOTE, _pct(best_probability),
        )
        return _PilotChance(
            best_probability, f"chance {_pct(best_probability)} ({ZERO_BASE_CHANCE_NOTE})", keep_producer=True,
        )
    base = _positive_float(raw_base)
    if base is None or multiplier is None:
        # No chance breakdown at all: nothing to recompute from.
        reason = "the row carries no invention chance breakdown"
        logger.warning(
            "Invent action for type_id=%s (pilot %s): %s; keeping the producer's chance %s",
            type_id, character_id, reason, _pct(best_probability),
        )
        return _PilotChance(
            best_probability, f"chance {_pct(best_probability)} (producer; {reason})", reason, keep_producer=True,
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
    if reason is not None:
        logger.warning(
            "Invent action for type_id=%s (pilot %s): %s; buying at the SDE base chance %s "
            "(best inventor %s), which may over-buy",
            type_id, character_id, reason, _pct(probability), _pct(best_probability),
        )
        note = f"chance {_pct(probability)} (SDE base: {reason}), best inventor {_pct(best_probability)}"
    else:
        note = f"chance {_pct(probability)} (assigned pilot), best inventor {_pct(best_probability)}"
    return _PilotChance(probability, note, reason)


class _Sized:
    """The row's sizing and the per-pilot figures shared by the single
    invent action and the parallel lanes."""

    def __init__(self, row: dict[str, Any], price_map: Mapping[int, Any]):
        self.row = row
        self.type_id = row.get("type_id")
        self.chance = orow.get_invention_chance(row) or {}
        costs = orow.get_invention_costs(row) or {}
        self.best_probability = _positive_float(self.chance.get("effective_probability")) or _positive_float(
            costs.get("success_probability")
        )
        self.sizing, self.unknown_why = _sizing(row, self.best_probability)
        self.price_map = price_map

    def unknown(self, character_id: int) -> AssignedInvention | None:
        if self.sizing is not None and self.best_probability is not None:
            return None
        reason = f"{DATACORE_NEED_UNKNOWN}: {self.unknown_why}"
        logger.warning(
            "Invent action for type_id=%s (pilot %s): %s; no invention inputs are bought for it",
            self.type_id, character_id, reason,
        )
        return AssignedInvention(None, self.best_probability, None, None, None, reason, reason)

    def cost(self, expected: float) -> tuple[float | None, str | None]:
        sizing = self.sizing
        assert sizing is not None
        if sizing.fee is None:
            return None, LATER_BATCH_NO_FEE
        ratio = expected / sizing.basis
        inputs, why = _input_cost(self.row, ratio, self.price_map)
        return (None, why) if inputs is None else (sizing.fee * ratio + inputs, None)

    def finish(
        self, character_id: int, probability: float, expected: float, whole: int, note: str,
        chance_reason: str | None, priced: tuple[float | None, str | None] | None = None,
    ) -> AssignedInvention:
        """The action's figures; an unknown cost and the later-batch sizing
        are named in the note, and an unknown cost in the reason too.
        `priced`: the (cost, why unknown) already worked out for this action
        (a lane's share of the lanes' one cost), else self.cost(expected)."""
        sizing = self.sizing
        assert sizing is not None
        value, cost_why = priced if priced is not None else self.cost(expected)
        notes, reasons = [note], ([chance_reason] if chance_reason else [])
        if sizing.later_batch:
            notes.append(LATER_BATCH_OVER_BUY_NOTE)
        if cost_why is not None:
            cost_reason = f"{INVENTION_COST_UNKNOWN}: {cost_why}"
            logger.warning(
                "Invent action for type_id=%s (pilot %s): %s; its estimated cost is left unknown",
                self.type_id, character_id, cost_reason,
            )
            notes.append(cost_reason)
            reasons.append(cost_reason)
        return AssignedInvention(
            probability, self.best_probability, expected, whole, value, "; ".join(notes),
            "; ".join(reasons) or None,
        )


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
    sized = _Sized(row, price_map)
    unknown = sized.unknown(character_id)
    if unknown is not None:
        return unknown
    sizing, best_probability = sized.sizing, sized.best_probability
    assert sizing is not None and best_probability is not None
    pilot = _pilot_chance(
        row, sized.chance, best_probability, character_id=character_id,
        skill_levels_by_id=skill_levels_by_id, probability_floor=probability_floor,
    )
    if pilot.keep_producer:
        return sized.finish(character_id, best_probability, sizing.best_expected, sizing.best_whole, pilot.note, pilot.reason)
    expected, whole = invention_attempts(sizing.invented_runs, sizing.runs_per_bpc, pilot.probability)
    return sized.finish(character_id, pilot.probability, expected, whole, pilot.note, pilot.reason)


def assigned_invention_lanes(
    row: dict[str, Any],
    *,
    lanes: Sequence[tuple[int, Mapping[int, int] | None]],
    probability_floor: float,
    price_map: Mapping[int, Any],
) -> list[tuple[int, AssignedInvention]]:
    """The row's invention split over parallel lanes:
    `lanes` holds one (character_id, skill levels by id) per free research
    slot of a qualifying pilot. Each lane runs at its OWN pilot's chance
    (_pilot_chance, the A4 rule), best chance first, and the attempts per lane
    come from invention_model.split_invention_attempts, the formula the
    producer's elapsed time uses. Returns (character_id, figures) for every
    lane that runs at least one attempt.

    Each lane's expected attempts are its share of the expected BPCs over its
    chance: E x a_i / sum(a_j x p_j). The lanes' total expected attempts are
    priced once (the held stock's discount counts once) and each
    lane carries its share of that cost. The whole attempts (what is
    bought) add up to at least what one pilot would buy. One lane: exactly assigned_invention. Unknown odds: one
    action, as assigned_invention."""
    if not lanes:
        return []
    if len(lanes) == 1:
        character_id, levels = lanes[0]
        return [(character_id, assigned_invention(
            row, character_id=character_id, skill_levels_by_id=levels,
            probability_floor=probability_floor, price_map=price_map,
        ))]
    sized = _Sized(row, price_map)
    first_id = lanes[0][0]
    unknown = sized.unknown(first_id)
    if unknown is not None:
        return [(first_id, unknown)]
    sizing, best_probability = sized.sizing, sized.best_probability
    assert sizing is not None and best_probability is not None
    chances: dict[int, _PilotChance] = {}
    for character_id, levels in lanes:
        if character_id not in chances:
            chances[character_id] = _pilot_chance(
                row, sized.chance, best_probability, character_id=character_id,
                skill_levels_by_id=levels, probability_floor=probability_floor,
            )
    ordered = sorted((cid for cid, _ in lanes), key=lambda cid: (-chances[cid].probability, cid))
    probabilities = [chances[cid].probability for cid in ordered]
    whole_blueprints = int(math.ceil(sizing.invented_runs / sizing.runs_per_bpc))
    attempts = split_invention_attempts(whole_blueprints, probabilities)
    covered = sum(a * p for a, p in zip(attempts, probabilities))
    expected_blueprints = float(sizing.invented_runs) / float(sizing.runs_per_bpc)
    used = [(cid, a) for cid, a in zip(ordered, attempts) if a > 0]
    # The lanes' expected attempts are priced ONCE (the row's
    # held stock discount counts once), then each lane takes its share.
    total_expected = expected_blueprints * sum(a for _, a in used) / covered
    total_cost, cost_why = sized.cost(total_expected)
    out: list[tuple[int, AssignedInvention]] = []
    for index, (cid, whole) in enumerate(used, start=1):
        pilot = chances[cid]
        expected = expected_blueprints * whole / covered
        share = expected / total_expected
        note = f"{pilot.note}; lane {index} of {len(used)}"
        out.append((cid, sized.finish(
            cid, pilot.probability, expected, whole, note, pilot.reason,
            priced=(None if total_cost is None else total_cost * share, cost_why),
        )))
    return out
