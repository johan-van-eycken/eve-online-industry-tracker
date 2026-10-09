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
            base_probability, None, multiplier, min(1.0, base_probability * multiplier),
            None, skill_set, why,
        )
    return InventionChance(
        base_probability, choice.skill_factor, multiplier,
        min(1.0, base_probability * float(choice.skill_factor) * multiplier),
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
