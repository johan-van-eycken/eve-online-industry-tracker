"""EVE invention rules shared by the overview producer and the daily planner.

Game rules, not settings. An invented BPC without a decryptor is ME 2 /
TE 4, with the SDE's runs per invented BPC. A decryptor adds its ME, TE and
max-run modifiers and multiplies the success chance. Decryptor values come
from the SDE: dogma attributes 1112 (inventionPropabilityMultiplier), 1113,
1114 and 1124 on group 1304 "Generic Decryptor", read by
infrastructure/sde/decryptors.py:get_t2_invention_decryptors.
"""
from __future__ import annotations

import logging
import math
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Iterator, Mapping

logger = logging.getLogger(__name__)

INVENTED_BASE_ME = 2
INVENTED_BASE_TE = 4
#: EVE's research ceilings; a blueprint's ME/TE never leaves 0..10 / 0..20.
MAX_BLUEPRINT_ME = 10
MAX_BLUEPRINT_TE = 20


@dataclass(frozen=True)
class DecryptorOption:
    """One way to invent: no decryptor (type_id None) or one SDE decryptor."""

    type_id: int | None
    type_name: str
    probability_multiplier: float = 1.0
    me_modifier: int = 0
    te_modifier: int = 0
    max_run_modifier: int = 0


NO_DECRYPTOR = DecryptorOption(type_id=None, type_name="No decryptor")


def invented_blueprint_efficiency(decryptor: DecryptorOption) -> tuple[int, int]:
    """(ME, TE) of a BPC invented with `decryptor`: 2 / 4 plus its modifiers."""
    me = min(MAX_BLUEPRINT_ME, max(0, INVENTED_BASE_ME + int(decryptor.me_modifier)))
    te = min(MAX_BLUEPRINT_TE, max(0, INVENTED_BASE_TE + int(decryptor.te_modifier)))
    return me, te


def invented_runs_per_blueprint(base_runs: int, decryptor: DecryptorOption) -> int:
    """Runs on one invented BPC: the SDE's runs plus the decryptor's, at least 1."""
    return max(1, int(base_runs) + int(decryptor.max_run_modifier))


def decryptor_options_from_rows(rows: Iterable[Mapping[str, Any]]) -> tuple[DecryptorOption, ...]:
    """NO_DECRYPTOR first, then every usable SDE decryptor by type_id.

    `rows` have the get_t2_invention_decryptors shape. A row without the four
    modifiers, or with a non-positive id or success multiplier, is skipped
    with a WARNING: it cannot be costed, and a guessed modifier would change
    the ME, the runs or the odds.
    """
    options: list[DecryptorOption] = []
    for row in rows:
        try:
            option = DecryptorOption(
                type_id=int(row["type_id"]),
                type_name=str(row.get("type_name") or row["type_id"]),
                probability_multiplier=float(row["invention_probability_multiplier"]),
                me_modifier=int(row["invention_me_modifier"]),
                te_modifier=int(row["invention_te_modifier"]),
                max_run_modifier=int(row["invention_max_run_modifier"]),
            )
        except (KeyError, TypeError, ValueError):
            logger.warning("Invention: skipping malformed SDE decryptor row %r", dict(row))
            continue
        if int(option.type_id or 0) <= 0 or option.probability_multiplier <= 0:
            logger.warning(
                "Invention: skipping decryptor %s with a non-positive id or success multiplier",
                option.type_name,
            )
            continue
        options.append(option)
    return (NO_DECRYPTOR, *sorted(options, key=lambda o: int(o.type_id or 0)))


_quiet_state = threading.local()


@contextmanager
def quiet_warnings() -> Iterator[None]:
    """Drop WARNING records on this thread from loggers carrying
    QuietWarningsFilter. This is used while the producer evaluates plans it
    throws away (decryptor candidates, the extra-batch plan), so a missing
    input is logged once, by the plan that is kept. ERROR and above pass."""
    _quiet_state.depth = getattr(_quiet_state, "depth", 0) + 1
    try:
        yield
    finally:
        _quiet_state.depth -= 1


def warnings_are_quiet() -> bool:
    return getattr(_quiet_state, "depth", 0) > 0


class QuietWarningsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not (record.levelno == logging.WARNING and warnings_are_quiet())


logger.addFilter(QuietWarningsFilter())


#: SDE groups.id of the "Science" skill group: the invention science and
#: encryption skills (verified against the 2026 SDE). Production prerequisites
#: such as Capital Ship Construction (group 268) are not science skills.
SCIENCE_SKILL_GROUP_ID = 270

NO_CHARACTER_SKILLS = "no corp character has its skills loaded"
#: Skills are loaded, but only for characters with industry_enabled=False.
ALL_INVENTORS_EXCLUDED = "every corp character with loaded skills is excluded from industry"
_NO_SKILL_ENTRIES = "the SDE lists no invention skills for this blueprint"
_NO_ENCRYPTION_SKILLS = "no Encryption Methods skills were found in the SDE"
#: Amendment A3: 6 SDE blueprints (the Exotic Plasma family) list two science
#: skills and no encryption skill. Their chance has no encryption term.
NO_ENCRYPTION_SKILL_NOTE = (
    "the SDE lists no encryption skill for this blueprint: the chance uses the science skills only"
)


@dataclass(frozen=True)
class InventionSkillSet:
    """The skills EVE's invention chance reads for one blueprint: two science
    skills and an encryption skill (None when the SDE lists none, A3)."""

    science_skill_type_ids: tuple[int, int]
    encryption_skill_type_id: int | None


def invention_skill_set(
    skill_entries: Iterable[Mapping[str, Any]], encryption_skill_type_ids: frozenset[int],
) -> tuple[InventionSkillSet | None, str | None]:
    """(skill set, None), or (None, why) when the SDE entries are not two
    science skills plus at most one encryption skill. Science = a group-270
    entry that is not an encryption skill (verified against the SDE). Two
    science skills and no encryption skill is a valid set (A3)."""
    if not encryption_skill_type_ids:
        return None, _NO_ENCRYPTION_SKILLS
    pairs: list[tuple[int, int]] = []
    for entry in skill_entries or []:
        if not isinstance(entry, Mapping):
            continue
        try:
            pairs.append((int(entry.get("type_id") or 0), int(entry.get("group_id") or 0)))
        except (TypeError, ValueError):
            continue
    pairs = [(type_id, group_id) for type_id, group_id in pairs if type_id > 0]
    if not pairs:
        return None, _NO_SKILL_ENTRIES
    encryption = sorted({type_id for type_id, _ in pairs if type_id in encryption_skill_type_ids})
    science = sorted({
        type_id for type_id, group_id in pairs
        if group_id == SCIENCE_SKILL_GROUP_ID and type_id not in encryption_skill_type_ids
    })
    if len(encryption) > 1 or len(science) != 2:
        return None, (
            "the SDE invention skills are not 2 science skills + 1 encryption skill "
            f"(found {len(science)} + {len(encryption)})"
        )
    return InventionSkillSet((science[0], science[1]), encryption[0] if encryption else None), None


def invention_skill_factor(skill_set: InventionSkillSet, levels_by_skill_type_id: Mapping[int, int]) -> float:
    """1 + (science1 + science2) / 30 + encryption / 40 (EVE, since Crius).

    The single copy of the formula: the producer's best inventor and the
    daily planner's pilot ranking both call it. No encryption skill in the
    set (A3): no encryption term. `levels_by_skill_type_id` is one
    character's loaded skill list, so an absent skill is level 0, not
    unknown."""
    science = sum(int(levels_by_skill_type_id.get(type_id, 0) or 0) for type_id in skill_set.science_skill_type_ids)
    encryption = (
        int(levels_by_skill_type_id.get(skill_set.encryption_skill_type_id, 0) or 0)
        if skill_set.encryption_skill_type_id is not None else 0
    )
    return 1.0 + science / 30.0 + encryption / 40.0


def invention_probability(base_probability: float, skill_factor: float | None, decryptor_multiplier: float) -> float:
    """EVE's success chance: base x skill factor x decryptor multiplier,
    capped at 1, before the admin floor. skill_factor None (skills unknown)
    counts as 1: the SDE base chance. The single copy, used by the producer's
    best inventor and the daily planner's assigned pilot."""
    factor = 1.0 if skill_factor is None else float(skill_factor)
    return min(1.0, float(base_probability) * factor * float(decryptor_multiplier))


#: The schema default of industry.invention_probability_floor.
DEFAULT_INVENTION_PROBABILITY_FLOOR = 0.01


def effective_invention_probability(probability: float, floor: float) -> float:
    """The chance the attempt counts divide by: the admin floor, then the cap at 1."""
    return min(1.0, max(float(probability), float(floor)))


def invention_attempts(
    invented_runs: int, runs_per_invented_blueprint: int, effective_probability: float,
) -> tuple[float, int]:
    """(expected attempts, whole attempts) to invent `invented_runs` runs.

    Expected = runs / runs per BPC / chance (fractional, for cost). Whole =
    ceil(whole BPCs needed / chance), at least 1 (what to buy and run).
    (0.0, 0) when nothing is invented or the chance is not positive: never a
    division by zero."""
    if invented_runs <= 0 or effective_probability <= 0:
        return 0.0, 0
    runs_per_bpc = max(1, int(runs_per_invented_blueprint))
    expected = float(invented_runs) / float(runs_per_bpc) / float(effective_probability)
    whole_blueprints = int(math.ceil(float(invented_runs) / float(runs_per_bpc)))
    whole = max(1, int(math.ceil(float(whole_blueprints) / float(effective_probability) - 1e-9)))
    return expected, whole


@dataclass(frozen=True)
class InventorChoice:
    character_id: int | None
    skill_factor: float | None
    unknown_reason: str | None


def best_inventor(
    skill_set: InventionSkillSet, levels_by_character_id: Mapping[int, Mapping[int, int]],
    *, empty_reason: str = NO_CHARACTER_SKILLS,
) -> InventorChoice:
    """The corp character with the highest skill factor for `skill_set`
    (ties: lowest character id), or `empty_reason` when there is none."""
    if not levels_by_character_id:
        return InventorChoice(None, None, empty_reason)
    factor, negative_id = max(
        (invention_skill_factor(skill_set, levels), -int(character_id))
        for character_id, levels in levels_by_character_id.items()
    )
    return InventorChoice(-negative_id, factor, None)


@dataclass(frozen=True)
class InventionChance:
    base_probability: float
    #: None: skills unknown, so the base chance is used (see skills_unknown_reason).
    skill_factor: float | None
    decryptor_multiplier: float
    #: min(1, base x (skill_factor or 1) x decryptor_multiplier), before the admin floor.
    probability: float
    inventor_character_id: int | None
    skill_set: InventionSkillSet | None
    skills_unknown_reason: str | None
    #: A row note that does not lower confidence (A3: no encryption skill).
    note: str | None = None


def _no_decryptor_for(_blueprint_type_id: int) -> DecryptorOption:
    return NO_DECRYPTOR


@dataclass
class InventionContext:
    """Per-refresh invention inputs. One object is shared by the top-level row
    and every nested sub-build of one overview refresh, across the producer's
    worker threads: the mappings are read-only after construction and
    warn_once is guarded by a lock."""

    decryptor_for_blueprint: Callable[[int], DecryptorOption] = _no_decryptor_for
    #: {character_id: {skill type_id: trained level}}, only characters whose skills are loaded.
    inventor_skill_levels_by_character_id: Mapping[int, Mapping[int, int]] = field(default_factory=dict)
    encryption_skill_type_ids: frozenset[int] = frozenset()
    #: Why inventor_skill_levels_by_character_id is empty, when it is.
    no_inventor_reason: str = NO_CHARACTER_SKILLS
    #: {character_id: total research slots and skill levels by name} for every
    #: industry-enabled corp character (Task 12, the daily planner's slot
    #: model). None: no slot data was supplied, so elapsed times stay sequential.
    slot_capacity_by_character_id: Mapping[int, "CharacterSlotCapacity"] | None = None
    #: The producer's selected character: its lane uses the producer's own
    #: implants; every other pilot's lane is timed without implants.
    selected_character_id: int | None = None
    #: {blueprint type_id: owned originals}. A BPO is in one job at a time, so
    #: it caps the parallel source copies. None: unknown (copies sequential).
    blueprint_original_count_by_type_id: Mapping[int, int] | None = None
    _warned: set[str] = field(default_factory=set, init=False, repr=False, compare=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False, compare=False)

    def warn_once(self, key: str, message: str, *args: Any) -> None:
        """One WARNING per key per refresh. While warnings are quiet nothing
        is registered, so the plan that is kept still logs it."""
        if warnings_are_quiet():
            return
        with self._lock:
            if key in self._warned:
                return
            self._warned.add(key)
        logger.warning(message, *args)


def resolve_invention_chance(
    context: InventionContext,
    *,
    blueprint_type_id: int,
    skill_entries: Iterable[Mapping[str, Any]],
    base_probability: float,
    decryptor: DecryptorOption,
) -> InventionChance:
    """EVE's invention chance for the corp's best inventor of this blueprint:
    base x (1 + (sci1 + sci2)/30 + enc/40) x decryptor multiplier, capped at 1.

    Skills unknown: the base chance (times the decryptor), one WARNING per
    reason per refresh, and the reason on the result. Never a guessed level."""
    multiplier = float(decryptor.probability_multiplier)
    skill_set, reason = invention_skill_set(skill_entries, context.encryption_skill_type_ids)
    choice: InventorChoice | None = None
    if skill_set is not None:
        choice = best_inventor(
            skill_set, context.inventor_skill_levels_by_character_id, empty_reason=context.no_inventor_reason,
        )
        reason = choice.unknown_reason
    if reason is not None or choice is None or choice.skill_factor is None:
        why = reason or context.no_inventor_reason
        context.warn_once(
            why,
            "Invention chance (first seen for blueprint %s): %s; using the SDE base chance, "
            "which overstates the invention cost",
            blueprint_type_id, why,
        )
        return InventionChance(
            base_probability, None, multiplier, invention_probability(base_probability, None, multiplier),
            None, skill_set, why,
        )
    return InventionChance(
        base_probability, choice.skill_factor, multiplier,
        invention_probability(base_probability, choice.skill_factor, multiplier),
        choice.character_id, skill_set, None,
        note=NO_ENCRYPTION_SKILL_NOTE if skill_set.encryption_skill_type_id is None else None,
    )


def invention_chance_payload(chance: InventionChance, effective_probability: float) -> dict[str, Any]:
    """The row's manufacturing_job.invention_chance (read by the UI and the
    daily planner's inventor ranking)."""
    return {
        "base_probability": chance.base_probability,
        "skill_factor": chance.skill_factor,
        "decryptor_multiplier": chance.decryptor_multiplier,
        "probability": chance.probability,
        "effective_probability": effective_probability,
        # Dropped by the fixture sanitiser ("character" key substring).
        "inventor_character_id": chance.inventor_character_id,
        "science_skill_type_ids": list(chance.skill_set.science_skill_type_ids) if chance.skill_set else None,
        "encryption_skill_type_id": chance.skill_set.encryption_skill_type_id if chance.skill_set else None,
        "skills_unknown_reason": chance.skills_unknown_reason,
        "note": chance.note,
    }


# --- parallel lanes (Task 12) ----------------------------------------------

#: Shown with every parallel elapsed time.
PARALLEL_CAPACITY_NOTE = (
    "parallel lanes count each qualifying character's total research slots: "
    "steady-state capacity, not today's free slots"
)
NO_SLOT_DATA_REASON = "no corp slot data was supplied"
NO_QUALIFYING_INVENTOR_REASON = (
    "no industry-enabled corp character has every required invention skill trained"
)
NO_SLOTS_FOR_INVENTORS_REASON = "no qualifying inventor has slot data"
NO_BPO_COUNT_REASON = "the owned source BPOs are not counted"
ONE_SOURCE_BPO_REASON = "one owned source BPO at most: a BPO is in one copy job at a time"
NO_COPY_SLOTS_REASON = "no industry-enabled corp character has slot data"


@dataclass(frozen=True)
class CharacterSlotCapacity:
    """One corp character's research slots (character_slots.research_slot_capacity,
    the planner's count) and skill levels by name (for its lane's duration)."""

    research_slots: int
    skill_levels_by_name: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ParallelLanes:
    """How many jobs of one kind run side by side. sequential_reason set: the
    slot data or skills are unknown, so the caller keeps the sequential
    (one-slot) elapsed time and records the reason."""

    lanes: int
    character_ids: tuple[int, ...] = ()
    sequential_reason: str | None = None

    def rounds(self, whole_jobs: int) -> int:
        """ceil(whole_jobs / lanes): jobs one lane runs back to back."""
        if whole_jobs <= 0:
            return 0
        return int(math.ceil(int(whole_jobs) / max(1, int(self.lanes))))

    def payload(self, *, whole_jobs: int, lane_duration_seconds: float | None) -> dict[str, Any]:
        return {
            "lanes": int(self.lanes),
            "lane_character_count": len(self.character_ids),
            "whole_jobs": int(whole_jobs),
            "rounds": self.rounds(whole_jobs) if self.sequential_reason is None else int(whole_jobs),
            "lane_duration_seconds": lane_duration_seconds,
            "sequential_reason": self.sequential_reason,
            "note": PARALLEL_CAPACITY_NOTE if self.sequential_reason is None else None,
        }


def _sequential(reason: str) -> ParallelLanes:
    return ParallelLanes(1, (), reason)


def required_invention_skill_levels(
    skill_set: InventionSkillSet, skill_entries: Iterable[Mapping[str, Any]],
) -> dict[int, int]:
    """{skill type_id: required level} for the set's science and encryption
    skills: the SDE entry's level, at least 1 (a skill must be trained to
    run the invention at all)."""
    wanted = [*skill_set.science_skill_type_ids]
    if skill_set.encryption_skill_type_id is not None:
        wanted.append(skill_set.encryption_skill_type_id)
    levels = {int(type_id): 1 for type_id in wanted}
    for entry in skill_entries or []:
        if not isinstance(entry, Mapping):
            continue
        try:
            type_id = int(entry.get("type_id") or 0)
            level = int(entry.get("level") or 1)
        except (TypeError, ValueError):
            continue
        if type_id in levels:
            levels[type_id] = max(levels[type_id], level, 1)
    return levels


def can_run_invention(required_levels: Mapping[int, int], levels_by_skill_type_id: Mapping[int, int]) -> bool:
    """Whether one character has every required invention skill trained."""
    return all(
        int(levels_by_skill_type_id.get(type_id, 0) or 0) >= int(level)
        for type_id, level in required_levels.items()
    )


def invention_lanes(
    context: InventionContext, chance: InventionChance | None, skill_entries: Iterable[Mapping[str, Any]],
) -> ParallelLanes:
    """Parallel invention lanes: the total research slots of every corp
    character that can run this invention (every required science and
    encryption skill trained; industry_enabled=False characters are never in
    the context). Total slots, not free ones: steady-state capacity. Unknown
    slot data or skills: one sequential lane with the reason."""
    if context.slot_capacity_by_character_id is None:
        return _sequential(NO_SLOT_DATA_REASON)
    if chance is None or chance.skill_set is None or chance.skills_unknown_reason is not None:
        why = (chance.skills_unknown_reason if chance is not None else None) or "the invention skills are unknown"
        return _sequential(f"invention skills unknown ({why})")
    required = required_invention_skill_levels(chance.skill_set, skill_entries)
    qualifying = sorted(
        int(character_id) for character_id, levels in context.inventor_skill_levels_by_character_id.items()
        if can_run_invention(required, levels)
    )
    if not qualifying:
        return _sequential(NO_QUALIFYING_INVENTOR_REASON)
    slots = context.slot_capacity_by_character_id
    with_slots = [cid for cid in qualifying if cid in slots and int(slots[cid].research_slots) > 0]
    if not with_slots:
        return _sequential(NO_SLOTS_FOR_INVENTORS_REASON)
    return ParallelLanes(sum(int(slots[cid].research_slots) for cid in with_slots), tuple(with_slots))


def source_copy_lanes(context: InventionContext, source_blueprint_type_id: int) -> ParallelLanes:
    """Parallel copy lanes for an invention's T1 source: min(owned source
    BPOs, total research slots of the industry-enabled corp characters). A
    BPO runs one copy job at a time, so one BPO (or an unknown count) keeps
    the copies sequential, with the reason. Copying needs no skill."""
    slots = context.slot_capacity_by_character_id
    if slots is None:
        return _sequential(NO_SLOT_DATA_REASON)
    counts = context.blueprint_original_count_by_type_id
    if counts is None:
        return _sequential(NO_BPO_COUNT_REASON)
    originals = int(counts.get(int(source_blueprint_type_id), 0) or 0)
    if originals <= 1:
        return _sequential(ONE_SOURCE_BPO_REASON)
    copiers = sorted(int(cid) for cid, cap in slots.items() if int(cap.research_slots) > 0)
    if not copiers:
        return _sequential(NO_COPY_SLOTS_REASON)
    lanes = min(originals, sum(int(slots[cid].research_slots) for cid in copiers))
    return ParallelLanes(lanes, tuple(copiers))


# --- decryptor selection (amendment A1) -----------------------------------

#: Differences below this (ISK per cycle, or ISK per unit in the fallback)
#: are ties, and ties go to no decryptor.
DECRYPTOR_TIE_TOLERANCE_ISK = 0.01
PROFIT_PER_CYCLE_CRITERION = "profit_per_cycle"
COST_PER_UNIT_CRITERION = "cost_per_unit"
SELL_PRICE_UNKNOWN_FALLBACK_REASON = "sell price unknown: chose lowest cost per unit"
DECRYPTOR_SELECTION_OFF_REASON = "no decryptor: decryptor selection is off (admin setting)"
NO_DECRYPTOR_DATA_REASON = "no decryptor: the SDE gave no decryptor data"


@dataclass(frozen=True)
class DecryptorEvaluation:
    """One candidate over one invention cycle: the expected attempts for one
    successful BPC plus the manufacturing runs that BPC yields.

    cost_per_unit: materials + invention inputs (decryptor included) + every
    job fee of the cycle, per manufactured unit; None when unknown.
    profit_per_cycle: the cycle's net proceeds (units x sell price, less sales
    tax and broker fee, as the row's profit_amount) minus that cost; None
    when the sell price or the market fees are unknown.
    skip_reason: why the candidate was not built or has no cost (it is then
    never chosen).
    profit_unknown_reason: why profit_per_cycle is None, as the fallback
    reason (default SELL_PRICE_UNKNOWN_FALLBACK_REASON)."""

    option: DecryptorOption
    cost_per_unit: float | None
    skip_reason: str | None = None
    profit_per_cycle: float | None = None
    units_per_cycle: int | None = None
    profit_unknown_reason: str | None = None


@dataclass(frozen=True)
class DecryptorChoice:
    option: DecryptorOption
    reason: str
    cost_per_unit: float | None = None
    baseline_cost_per_unit: float | None = None
    evaluations: tuple[DecryptorEvaluation, ...] = ()
    profit_per_cycle: float | None = None
    baseline_profit_per_cycle: float | None = None
    #: PROFIT_PER_CYCLE_CRITERION or COST_PER_UNIT_CRITERION; None when nothing was compared.
    criterion: str | None = None
    #: Set when the cost-per-unit fallback was used (A1).
    fallback_reason: str | None = None


def _pick(
    evaluations: tuple[DecryptorEvaluation, ...], metric: Callable[[DecryptorEvaluation], float | None],
    *, higher_is_better: bool,
) -> tuple[DecryptorEvaluation | None, float, float]:
    """(the chosen decryptor (never NO_DECRYPTOR), its own metric value, the
    best metric value). Decryptors within DECRYPTOR_TIE_TOLERANCE_ISK of the
    best value are a tie, which goes to the lowest type id; the comparison
    against no decryptor uses the best value. Candidates with a skip reason or
    an unknown metric are left out."""
    priced = [
        e for e in evaluations
        if e.option.type_id is not None and e.skip_reason is None and metric(e) is not None
    ]
    if not priced:
        return None, 0.0, 0.0
    sign = -1.0 if higher_is_better else 1.0
    top = min(sign * float(metric(e) or 0.0) for e in priced)
    tied = [e for e in priced if sign * float(metric(e) or 0.0) <= top + DECRYPTOR_TIE_TOLERANCE_ISK]
    best = min(tied, key=lambda e: int(e.option.type_id or 0))
    return best, float(metric(best) or 0.0), sign * top


def select_best_decryptor(evaluations: Iterable[DecryptorEvaluation]) -> DecryptorChoice:
    """THE decryptor selection criterion, kept in this one function so it can
    be switched later (round-5 amendment A1).

    Highest profit per invention cycle. One cycle is the expected attempts
    for one successful BPC plus the manufacturing runs that BPC yields;
    profit is those units at the row's sell price minus materials, invention
    inputs (decryptor included) and every job fee of the cycle. When the sell
    price is unknown or not positive (no profit on the no-decryptor
    candidate), the lowest total cost per manufactured unit decides instead,
    and the choice records SELL_PRICE_UNKNOWN_FALLBACK_REASON.

    Ties within DECRYPTOR_TIE_TOLERANCE_ISK go to no decryptor first, then
    (between decryptors) to the lowest type id. The profit is net of sales
    tax and broker fee; unknown fees fall back to cost per unit with the
    evaluation's profit_unknown_reason. A candidate with a skip reason or an
    unknown figure is never chosen. When the no-decryptor cost itself is
    unknown nothing can be compared, so no decryptor is used.
    """
    evaluations = tuple(evaluations)
    baseline = next((e for e in evaluations if e.option.type_id is None), None)
    if baseline is None or baseline.cost_per_unit is None:
        return DecryptorChoice(
            NO_DECRYPTOR,
            "no decryptor: the cost without a decryptor is unknown, so nothing could be compared",
            evaluations=evaluations,
        )
    base_cost = float(baseline.cost_per_unit)
    if baseline.profit_per_cycle is not None:
        base_profit = float(baseline.profit_per_cycle)
        best, best_profit, top_profit = _pick(evaluations, lambda e: e.profit_per_cycle, higher_is_better=True)
        if best is None or top_profit <= base_profit + DECRYPTOR_TIE_TOLERANCE_ISK:
            detail = (
                f"; best decryptor {best.option.type_name} {best_profit:,.2f} ISK"
                if best is not None else "; no decryptor could be priced"
            )
            return DecryptorChoice(
                NO_DECRYPTOR,
                f"no decryptor: highest profit per invention cycle ({base_profit:,.2f} ISK{detail})",
                base_cost, base_cost, evaluations, base_profit, base_profit, PROFIT_PER_CYCLE_CRITERION,
            )
        return DecryptorChoice(
            best.option,
            f"{best.option.type_name}: highest profit per invention cycle "
            f"({best_profit:,.2f} ISK vs {base_profit:,.2f} ISK without a decryptor)",
            best.cost_per_unit, base_cost, evaluations, best_profit, base_profit, PROFIT_PER_CYCLE_CRITERION,
        )
    fallback = baseline.profit_unknown_reason or SELL_PRICE_UNKNOWN_FALLBACK_REASON
    best, best_cost, top_cost = _pick(evaluations, lambda e: e.cost_per_unit, higher_is_better=False)
    if best is None or top_cost >= base_cost - DECRYPTOR_TIE_TOLERANCE_ISK:
        detail = (
            f"; best decryptor {best.option.type_name} {best_cost:,.2f} ISK"
            if best is not None else "; no decryptor could be priced"
        )
        return DecryptorChoice(
            NO_DECRYPTOR,
            f"no decryptor: lowest cost per unit ({base_cost:,.2f} ISK{detail}); {fallback}",
            base_cost, base_cost, evaluations, None, None, COST_PER_UNIT_CRITERION, fallback,
        )
    return DecryptorChoice(
        best.option,
        f"{best.option.type_name}: lowest cost per unit ({best_cost:,.2f} ISK vs "
        f"{base_cost:,.2f} ISK without a decryptor); {fallback}",
        best_cost, base_cost, evaluations, None, None, COST_PER_UNIT_CRITERION, fallback,
    )


def decryptor_choice_payload(choice: DecryptorChoice) -> dict[str, Any]:
    """The row's manufacturing_job.invention_decryptor (shown in the UI, read
    by the daily planner's invent action)."""
    return {
        "type_id": choice.option.type_id,
        "type_name": choice.option.type_name,
        "reason": choice.reason,
        "criterion": choice.criterion,
        "fallback_reason": choice.fallback_reason,
        "profit_per_cycle": choice.profit_per_cycle,
        "baseline_profit_per_cycle": choice.baseline_profit_per_cycle,
        "cost_per_unit": choice.cost_per_unit,
        "baseline_cost_per_unit": choice.baseline_cost_per_unit,
        "probability_multiplier": choice.option.probability_multiplier,
        "me_modifier": choice.option.me_modifier,
        "te_modifier": choice.option.te_modifier,
        "max_run_modifier": choice.option.max_run_modifier,
        "candidates": [
            {"type_id": e.option.type_id, "type_name": e.option.type_name,
             "cost_per_unit": e.cost_per_unit, "profit_per_cycle": e.profit_per_cycle,
             "units_per_cycle": e.units_per_cycle, "skip_reason": e.skip_reason}
            for e in choice.evaluations
        ],
    }
