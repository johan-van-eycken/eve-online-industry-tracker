"""CharacterAssigner — Phase 6: assign planned jobs to characters.

Priority rules:
  1. Manufacturing / sub-manufacture → character with most free manufacturing slots
  2. Invention → free research slot + highest invention skill factor for the
     item's own skills (the producer's invention_chance, D2); a row without
     them falls back to the generic science skill sum
  3. Copy → character with most free research slots
  4. ME/TE research → character with most free research slots
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.application.daily_planner.assigned_invention import (
    assigned_invention,
    invention_probability_floor,
)
from eve_online_industry_tracker.application.daily_planner.input_row import require_batch_runs
from eve_online_industry_tracker.application.daily_planner.models import AssignedAction, ItemDecision
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.industry.invention_model import (
    InventionSkillSet,
    invention_skill_factor,
)

logger = logging.getLogger(__name__)

# EVE activity IDs
ACTIVITY_MANUFACTURING = 1
ACTIVITY_RESEARCHING_TE = 3
ACTIVITY_RESEARCHING_ME = 4
ACTIVITY_COPYING = 5
ACTIVITY_INVENTION = 8

# Slot capacity skills. Base one slot, plus one per level of each.
MFG_SLOT_SKILLS = ("Mass Production", "Advanced Mass Production")
RESEARCH_SLOT_SKILLS = ("Laboratory Operation", "Advanced Laboratory Operation")
# Fallback inventor ranking for a row without the producer's invention skills.
SCIENCE_SKILLS = ("Science", "Advanced Industry", "Metallurgy", "Research")


def _now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


def skill_levels_by_name(char: dict[str, Any]) -> dict[str, int]:
    """{skill_name: trained_skill_level} for one character.

    Reads the enriched skill list built in characters/character.py, which joins
    ESI's skill list against every published SDE skill and carries skill_name
    alongside trained_skill_level. Note the stored payload has no
    `active_skill_level` key at all — reading one yields 0 for every skill.

    A character with no skills data at all (missing/`None`/empty) is a
    legitimate, quiet `{}` — nothing trained yet, or not fetched. A `skills`
    value that *is* present but not the expected `{"skills": [...]}` wrapper
    (e.g. a bare list) is a malformed payload, not an absence: it is logged
    at warning and still returns `{}` rather than raising, since one pilot's
    bad payload must not abort the whole plan -- it just falls back to base
    slot capacity for that pilot.
    """
    raw = char.get("skills")
    if not raw:
        return {}

    entries = raw.get("skills") if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        logger.warning(
            "CharacterAssigner: character %s has a 'skills' payload that is not the "
            "expected {'skills': [...]} wrapper (got %s) -- treating as no trained "
            "skills for this pilot rather than aborting the plan",
            char.get("character_id") or char.get("id") or char.get("character_name") or char.get("name") or "?",
            type(raw).__name__,
        )
        return {}

    levels: dict[str, int] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("skill_name") or "").strip()
        if not name:
            continue
        try:
            level = int(entry.get("trained_skill_level") or 0)
        except (TypeError, ValueError):
            continue
        levels[name] = max(level, levels.get(name, 0))
    return levels


def skill_levels_by_id(char: dict[str, Any]) -> dict[int, int]:
    """{skill type_id: trained_skill_level} for one character, from the same
    enriched list as skill_levels_by_name. Ids, not names: the producer's
    invention chance names its skills by SDE type id."""
    raw = char.get("skills")
    entries = raw.get("skills") if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        return {}
    levels: dict[int, int] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            skill_id = int(entry.get("skill_id") or 0)
            level = int(entry.get("trained_skill_level") or 0)
        except (TypeError, ValueError):
            continue
        if skill_id > 0:
            levels[skill_id] = max(level, levels.get(skill_id, 0))
    return levels


def _skills_loaded(char: dict[str, Any]) -> bool:
    """Whether the character carries a loaded skill list ({"skills": [...]},
    possibly empty). Without one its levels are unknown, not 0."""
    raw = char.get("skills")
    return isinstance(raw, dict) and isinstance(raw.get("skills"), list)


def _slot_capacity(levels: dict[str, int], skill_names: tuple[str, ...]) -> int:
    """One base slot plus one per trained level of each contributing skill."""
    return 1 + sum(int(levels.get(name, 0)) for name in skill_names)


class CharacterAssigner:
    """Phase 6: skill-aware job-to-character assignment."""

    _MFG_ACTIONS = ("manufacture", "sub_manufacture")
    _RESEARCH_ACTIONS = ("invent", "copy", "me_research", "te_research")

    def assign(
        self,
        chain_plan: Any,                     # ChainPlan
        industry_jobs: list[Any],            # active corp + character industry jobs
        characters_service: Any,             # CharactersService
        admin_settings: Any,
        price_map: dict[int, Any] | None = None,
    ) -> list[AssignedAction]:
        """Return list of AssignedAction for all planned jobs.

        price_map: phase 1's merged price map, the market price of the extra
        invention inputs a weaker assigned inventor needs (A4)."""
        now = _now()

        # Build character capability map
        characters = self._get_characters(characters_service)
        if not characters:
            logger.warning("CharacterAssigner: no characters available")
            return []

        char_slots = self._compute_available_slots(characters, industry_jobs, now)
        # The producer's floor, so an assigned pilot's chance is floored the same way.
        probability_floor = invention_probability_floor(admin_settings)
        assigned_actions: list[AssignedAction] = []

        # Two passes: every top-level item first (highest score first), then
        # sub-components. A sub-component's quantity is its parents' need, so
        # it must never take a slot ahead of a parent. Slots only decrease, so
        # a parent left without a slot leaves none for its components either.
        build_decisions = [d for d in chain_plan.decisions if d.decision in ("build", "pause")]
        top_level = sorted(
            (d for d in build_decisions if not d.is_sub_component), key=lambda d: -d.adjusted_score
        )
        sub_components = [d for d in build_decisions if d.is_sub_component]

        for decision in [*top_level, *sub_components]:
            actions = self._assign_decision(
                decision=decision,
                row=decision.overview_row,
                char_slots=char_slots,
                characters=characters,
                now=now,
                probability_floor=probability_floor,
                price_map=price_map or {},
            )
            # Slots are consumed inside _assign_decision, as each action is
            # created -- two research actions on one item must not share a slot.
            assigned_actions.extend(actions)

        return assigned_actions

    def _take_slot(self, char_slots: dict[int, dict], char_id: int, action_type: str) -> None:
        """Consume one slot immediately, so sibling actions cannot reuse it."""
        slot = char_slots.get(char_id)
        if slot is None:
            return
        if action_type in self._MFG_ACTIONS:
            slot["free_mfg"] = max(0, slot.get("free_mfg", 0) - 1)
        elif action_type in self._RESEARCH_ACTIONS:
            slot["free_research"] = max(0, slot.get("free_research", 0) - 1)

    def _assign_decision(
        self,
        *,
        decision: ItemDecision,
        row: dict[str, Any],
        char_slots: dict[int, dict[str, Any]],
        characters: list[dict[str, Any]],
        now: datetime,
        probability_floor: float,
        price_map: dict[int, Any],
    ) -> list[AssignedAction]:
        """Return AssignedAction(s) for a single ItemDecision."""
        actions: list[AssignedAction] = []
        type_id = decision.type_id
        type_name = decision.type_name

        # DELIVER actions: in-flight jobs completing soon (end_date < now)
        # These are built separately in ActionPlanBuilder from industry_jobs directly.

        if decision.decision == "pause":
            # Paused items: only deliver actions (built from jobs, not here)
            return actions

        if decision.decision != "build":
            return actions

        # ME/TE research
        if row.get("needs_me_research"):
            bp_type_id = self._research_blueprint_type_id(row, type_id, "ME")
            char_id, char_name = (
                (None, None) if bp_type_id is None
                else self._best_research_char(char_slots, characters)
            )
            if char_id is not None:
                actions.append(AssignedAction(
                    type_id=bp_type_id,
                    type_name=type_name + " BPO",
                    action_type="me_research",
                    character_id=char_id,
                    character_name=char_name,
                    quantity=None,
                    runs=None,
                    estimated_cost_isk=None,
                    estimated_profit_isk=None,
                    estimated_completion=None,
                    notes=f"ME research: {row.get('me_current', '?')} → {row.get('me_research_target', '?')}",
                    estimated_job_fee_isk=_known_fee(
                        _research_fee(row, "research_material", "me_current", "me_research_target"),
                        "me_research", type_id,
                    ),
                ))
                self._take_slot(char_slots, char_id, "me_research")
            elif bp_type_id is not None:
                logger.debug("CharacterAssigner: no free research slot for ME research on type_id=%s", type_id)

        if row.get("needs_te_research"):
            bp_type_id = self._research_blueprint_type_id(row, type_id, "TE")
            char_id, char_name = (
                (None, None) if bp_type_id is None
                else self._best_research_char(char_slots, characters)
            )
            if char_id is not None:
                actions.append(AssignedAction(
                    type_id=bp_type_id,
                    type_name=type_name + " BPO",
                    action_type="te_research",
                    character_id=char_id,
                    character_name=char_name,
                    quantity=None,
                    runs=None,
                    estimated_cost_isk=None,
                    estimated_profit_isk=None,
                    estimated_completion=None,
                    notes=f"TE research: {row.get('te_current', '?')} → {row.get('te_research_target', '?')}",
                    estimated_job_fee_isk=_known_fee(
                        _research_fee(row, "research_time", "te_current", "te_research_target"),
                        "te_research", type_id,
                    ),
                ))
                self._take_slot(char_slots, char_id, "te_research")

        # Invention
        invent_whole_attempts: int | None = None
        if row.get("needs_invention"):
            char_id, char_name = self._best_invention_char(char_slots, characters, row)
            if char_id is not None:
                # The producer's choice: bought one per attempt by the shopping list.
                decryptor = orow.get_invention_decryptor(row)
                # A4: the assigned pilot's chance, whole attempts and cost (the
                # best inventor's when this pilot is the best inventor or its
                # skills are unknown). runs = whole attempts, read by the
                # shopping list; None (unknown) buys one attempt.
                invention = assigned_invention(
                    row, character_id=char_id,
                    # None: this pilot's skills are not loaded (not level 0).
                    skill_levels_by_id=char_slots[char_id].get("skills_by_id"),
                    probability_floor=probability_floor, price_map=price_map,
                )
                invent_whole_attempts = invention.whole_attempts
                actions.append(AssignedAction(
                    type_id=type_id,
                    type_name=type_name,
                    action_type="invent",
                    character_id=char_id,
                    character_name=char_name,
                    quantity=None,
                    runs=invention.whole_attempts,
                    estimated_cost_isk=invention.cost_isk,
                    estimated_profit_isk=None,
                    estimated_completion=None,
                    notes="; ".join([
                        "Invention: BPC stock below pipeline threshold",
                        *([f"use {decryptor[1]}"] if decryptor else []),
                        *([invention.note] if invention.note else []),
                    ]),
                    decryptor_type_id=decryptor[0] if decryptor else None,
                    decryptor_type_name=decryptor[1] if decryptor else None,
                    invention_probability=invention.probability,
                    invention_best_probability=invention.best_probability,
                    invention_expected_attempts=invention.expected_attempts,
                    invention_chance_reason=invention.reason,
                    # The cash for the jobs really launched: the fee of one
                    # attempt x the whole attempts (runs), not the expected.
                    estimated_job_fee_isk=_known_fee(
                        _times_runs(orow.get_invention_fee_per_attempt(row), invention.whole_attempts),
                        "invent", type_id,
                    ),
                ))
                self._take_slot(char_slots, char_id, "invent")

        # Copy (T1 BPO copy for invention feed). What is copied is the T1
        # source blueprint, never the T2 product or the T2 blueprint.
        t1_blueprint_type_id = int(row.get("t1_blueprint_type_id") or 0)
        wants_copy = bool(row.get("needs_invention") and row.get("has_t1_bpo"))
        if wants_copy and t1_blueprint_type_id <= 0:
            logger.warning(
                "CharacterAssigner: has_t1_bpo set without a t1_blueprint_type_id for "
                "type_id=%s; not creating a copy action",
                type_id,
            )
        elif wants_copy:
            char_id, char_name = self._best_research_char(char_slots, characters)
            if char_id is not None:
                actions.append(AssignedAction(
                    type_id=t1_blueprint_type_id,
                    type_name=type_name + " (copy)",
                    action_type="copy",
                    character_id=char_id,
                    character_name=char_name,
                    quantity=None,
                    # One T1 BPC run per invention attempt; None when the
                    # invent action's attempts are unknown (or it has no slot).
                    runs=invent_whole_attempts,
                    estimated_cost_isk=None,
                    estimated_profit_isk=None,
                    estimated_completion=None,
                    notes="Copy T1 BPO to feed invention pipeline",
                    estimated_job_fee_isk=_known_fee(
                        _times_runs(orow.get_source_copy_fee_per_run(row), invent_whole_attempts),
                        "copy", type_id,
                    ),
                ))
                self._take_slot(char_slots, char_id, "copy")

        # Sub-manufacture: one action, and one manufacturing slot, per job.
        # A job left without a slot gets no action, so the shopping list
        # neither buys its materials nor subtracts its units from the
        # parents' purchase: those units are bought instead.
        if decision.is_sub_component:
            jobs = _sub_jobs(type_id, row)
            for index, job in enumerate(jobs):
                char_id, char_name = self._best_mfg_char(char_slots, characters)
                if char_id is None:
                    logger.info(
                        "CharacterAssigner: no free mfg slot for %d of %d sub-manufacture "
                        "jobs of type_id=%s; their units are bought instead",
                        len(jobs) - index, len(jobs), type_id,
                    )
                    break
                actions.append(AssignedAction(
                    type_id=type_id,
                    type_name=type_name,
                    action_type="sub_manufacture",
                    character_id=char_id,
                    character_name=char_name,
                    quantity=job["quantity"],
                    runs=job["runs"],
                    estimated_cost_isk=job["cost"],
                    estimated_profit_isk=None,
                    estimated_completion=None,
                    notes=_sub_manufacture_notes(row, job, index, len(jobs)),
                    materials=job["materials"],
                    # The component's fee per run x this job's runs (ChainPlanner).
                    estimated_job_fee_isk=_known_fee(job.get("fee"), "sub_manufacture", type_id),
                ))
                self._take_slot(char_slots, char_id, "sub_manufacture")
            return actions  # sub-components don't get a manufacture action

        # Manufacturing
        char_id, char_name = self._best_mfg_char(char_slots, characters)
        if char_id is not None:
            # Both live under manufacturing_job. A top-level runs_per_batch /
            # runs / estimated_material_cost is written by no producer, so
            # every manufacture action used to get 1 run and a 0 ISK cost --
            # and the shopping list bought materials for one run of the batch.
            runs = _batch_runs(type_id, row)
            # The batch's material cost, or None when the producer could not
            # price it -- unknown, not free.
            estimated_cost = orow.get_material_cost_total(row)
            estimated_profit = decision.absolute_profit_per_batch
            # The batch's unit total, so feedback can turn the batch cost into
            # a per-unit cost (FeedbackProcessor compares per unit). None when
            # the row carries none -- feedback then skips the cost update.
            batch_units = orow.get_product_quantity(row)
            actions.append(AssignedAction(
                type_id=type_id,
                type_name=type_name,
                action_type="manufacture",
                character_id=char_id,
                character_name=char_name,
                quantity=batch_units if batch_units > 0 else None,
                runs=runs,
                estimated_cost_isk=estimated_cost,
                # Materials + job fees for the batch: the closest counterpart
                # of a realized industry-build unit cost, which includes the
                # manufacturing install fee (FeedbackProcessor compares these;
                # the remaining differences are documented there).
                estimated_build_cost_isk=orow.get_total_cost(row),
                estimated_profit_isk=estimated_profit,
                estimated_completion=None,
                notes=f"Build: {decision.decision_reason}",
                materials=orow.get_batch_materials(row),
                estimated_job_fee_isk=_known_fee(orow.get_manufacturing_fee(row), "manufacture", type_id),
            ))
            self._take_slot(char_slots, char_id, "manufacture")
        else:
            logger.debug("CharacterAssigner: no free mfg slot for type_id=%s", type_id)

        return actions

    def get_character_name_map(self, characters_service: Any) -> dict[int, str]:
        """{character_id: character_name}, resolved via list_characters().

        ActionPlanBuilder needs the pilot name for each in-flight job's
        deliver row (installer_id -> name) but must not reach for
        characters_service itself -- that is a service it does not own.
        This reuses the same characters_service.list_characters() call
        _compute_available_slots already resolves names from, so Phase 8
        does not have to invent its own name-resolution path.
        """
        characters = self._get_characters(characters_service)
        result: dict[int, str] = {}
        for char in characters:
            char_id = int(char.get("character_id") or char.get("id") or 0)
            if char_id <= 0:
                continue
            result[char_id] = str(char.get("character_name") or char.get("name") or "")
        return result

    def _get_characters(self, characters_service: Any) -> list[dict[str, Any]]:
        """Fetch characters list from service.

        CharacterManager.get_characters() raises ValueError when it has no
        cached characters yet — that is the only condition meaning "no
        characters to assign". Any other exception is a real failure deeper
        in the stack and must propagate, not be logged away as an empty
        roster (that used to hide genuine bugs behind a "no characters"
        warning).
        """
        try:
            chars = characters_service.list_characters()
        except ValueError:
            return []
        if isinstance(chars, list):
            return [c if isinstance(c, dict) else c.__dict__ for c in chars]
        return []

    def _compute_available_slots(
        self,
        characters: list[dict[str, Any]],
        industry_jobs: list[Any],
        now: datetime,
    ) -> dict[int, dict[str, Any]]:
        """Compute free slot counts per character, treating end_date < now as already delivered.

        The one place the user's industry exclusion applies: a character with
        industry_enabled=False (the flag the UI filters on) gets no slot, so
        no assignment of any type (manufacture, sub_manufacture, copy, ME/TE
        research, invent) can pick it. Its in-flight jobs are still delivered
        (ActionPlanBuilder works from the jobs, not from these slots)."""
        slot_map: dict[int, dict[str, Any]] = {}
        excluded: list[int] = []
        for char in characters:
            char_id = int(char.get("character_id") or char.get("id") or 0)
            if char_id <= 0:
                continue
            if not char.get("industry_enabled", True):
                excluded.append(char_id)
                continue

            levels = skill_levels_by_name(char)
            max_mfg = _slot_capacity(levels, MFG_SLOT_SKILLS)
            max_research = _slot_capacity(levels, RESEARCH_SLOT_SKILLS)
            slot_map[char_id] = {
                "character_id": char_id,
                "character_name": str(char.get("character_name") or char.get("name") or ""),
                "free_mfg": max_mfg,
                "free_research": max_research,
                "total_active_jobs": 0,
                "skills": levels,
                # None: no skill list loaded for this pilot (unknown, not level 0).
                "skills_by_id": skill_levels_by_id(char) if _skills_loaded(char) else None,
            }
        if excluded:
            logger.info(
                "CharacterAssigner: characters %s are excluded from industry (industry_enabled=False); "
                "no job is assigned to them", sorted(excluded),
            )

        # Deduct active jobs (treat end_date < now as already delivered — slots free)
        for job in industry_jobs:
            char_id = _job_attr(job, "character_id") or _job_attr(job, "installer_id")
            if char_id is None:
                continue
            char_id = int(char_id)
            if char_id not in slot_map:
                continue

            status = str(_job_attr(job, "status") or "").lower()
            if status == "delivered":
                continue  # already done

            end_date_raw = _job_attr(job, "end_date")
            if end_date_raw is not None:
                try:
                    if isinstance(end_date_raw, str):
                        end_date = datetime.fromisoformat(end_date_raw.replace("Z", "+00:00")).replace(tzinfo=None)
                    elif isinstance(end_date_raw, datetime):
                        end_date = end_date_raw.replace(tzinfo=None)
                    else:
                        end_date = None
                    if end_date is not None and end_date <= now:
                        # Job complete but not delivered — slots already freed in game
                        continue
                except ValueError:
                    # Unparseable end_date: count the job as active (the
                    # conservative choice -- it keeps its slot).
                    pass

            # activity_id is now a real column on both job ORM models (Task 13),
            # backfilled from `raw` for pre-existing rows and set directly on new
            # syncs. The `raw` fallback below stays anyway: a database that has
            # not yet run that migration still has activity_id NULL/0 on every
            # row, and without this fallback slot deduction would silently stop
            # working there. Do not remove it until every deployed database is
            # confirmed migrated.
            activity_id = int(_job_attr(job, "activity_id") or 0)
            if activity_id == 0:
                raw = _job_attr(job, "raw")
                if isinstance(raw, dict):
                    activity_id = int(raw.get("activity_id") or 0)

            if activity_id == ACTIVITY_MANUFACTURING:
                slot_map[char_id]["free_mfg"] = max(0, slot_map[char_id]["free_mfg"] - 1)
            elif activity_id in (ACTIVITY_RESEARCHING_TE, ACTIVITY_RESEARCHING_ME, ACTIVITY_COPYING, ACTIVITY_INVENTION):
                slot_map[char_id]["free_research"] = max(0, slot_map[char_id]["free_research"] - 1)

            slot_map[char_id]["total_active_jobs"] = slot_map[char_id]["total_active_jobs"] + 1

        return slot_map

    def _best_mfg_char(
        self, char_slots: dict[int, dict], characters: list[dict]
    ) -> tuple[int | None, str | None]:
        """Character with most free manufacturing slots; tie-break by fewest total active jobs."""
        candidates = [(s["free_mfg"], -s["total_active_jobs"], s["character_id"], s["character_name"])
                      for s in char_slots.values() if s["free_mfg"] > 0]
        if not candidates:
            return None, None
        candidates.sort(key=lambda x: (-x[0], x[1]))
        best = candidates[0]
        return best[2], best[3]

    @staticmethod
    def _research_blueprint_type_id(row: dict[str, Any], type_id: int, kind: str) -> int | None:
        """The blueprint to research, or None (WARNING): a product id is not a blueprint."""
        bp_type_id = orow.get_blueprint_type_id(row)
        if bp_type_id is None:
            logger.warning(
                "CharacterAssigner: %s research flagged for type_id=%s but its row has no "
                "blueprint type id; not creating a research action",
                kind, type_id,
            )
        return bp_type_id

    def _best_research_char(
        self, char_slots: dict[int, dict], characters: list[dict]
    ) -> tuple[int | None, str | None]:
        """Character with most free research slots; tie-break by fewest active jobs."""
        candidates = [(s["free_research"], -s["total_active_jobs"], s["character_id"], s["character_name"])
                      for s in char_slots.values() if s["free_research"] > 0]
        if not candidates:
            return None, None
        candidates.sort(key=lambda x: (-x[0], x[1]))
        best = candidates[0]
        return best[2], best[3]

    def _best_invention_char(
        self, char_slots: dict[int, dict], characters: list[dict], row: dict[str, Any] | None = None,
    ) -> tuple[int | None, str | None]:
        """The free pilot with the highest invention skill factor for this
        item: the producer's invention_chance skills (D2, the same rule as the
        chance it assumed). A row without them ranks by the generic
        SCIENCE_SKILLS sum (logged at INFO). Pilots excluded from industry
        have no slot (see _compute_available_slots). Ties break toward the
        fewest active jobs."""
        skills = orow.get_invention_skill_set(row or {})
        skill_set = InventionSkillSet(skills[0], skills[1]) if skills is not None else None
        if skill_set is None:
            logger.info(
                "CharacterAssigner: type_id=%s carries no invention skills from the producer; "
                "ranking inventors by the generic %s sum",
                (row or {}).get("type_id"), "/".join(SCIENCE_SKILLS),
            )
        candidates = []
        for slot in char_slots.values():
            if slot.get("free_research", 0) <= 0:
                continue
            if skill_set is not None:
                score = invention_skill_factor(skill_set, slot.get("skills_by_id") or {})
            else:
                levels = slot.get("skills") or {}
                score = float(sum(int(levels.get(name, 0) or 0) for name in SCIENCE_SKILLS))
            candidates.append(
                (-score, slot.get("total_active_jobs", 0),
                 slot["character_id"], slot["character_name"])
            )
        if not candidates:
            return None, None
        candidates.sort()
        return candidates[0][2], candidates[0][3]


def _known_fee(fee: float | None, action_type: str, type_id: int) -> float | None:
    """The action's install fee, logging a WARNING when it is unknown
    (Capital Reserved then reads "unknown", never a partial sum)."""
    if fee is None:
        logger.warning(
            "CharacterAssigner: install fee unknown for the %s action of type_id=%s "
            "(the producer published no priced fee for it); Capital Reserved shows unknown",
            action_type, type_id,
        )
    return fee


def _times_runs(fee_per_run: float | None, runs: int | None) -> float | None:
    """A per-run (per-attempt) fee x the runs, or None when either is unknown."""
    if fee_per_run is None or runs is None or runs <= 0:
        return None
    return fee_per_run * runs


def _research_fee(row: dict[str, Any], activity: str, current_key: str, target_key: str) -> float | None:
    """The research job's fee from the row's current level to its target
    (A2), via the producer's cumulative fee table. ME is stored as the level
    (0..10). TE is stored as EVE's percentage (2 % per level, 0..20): the
    current level is the one held (te // 2), the target the lowest level that
    reaches the target percentage (ceil(te / 2))."""
    try:
        current, target = int(row[current_key]), int(row[target_key])
    except (KeyError, TypeError, ValueError):
        return None
    if activity == "research_time":
        current, target = current // 2, -(-target // 2)
    return orow.get_research_fee(row, activity, current, target)


def _job_attr(job: Any, attr: str) -> Any:
    if isinstance(job, dict):
        return job.get(attr)
    return getattr(job, attr, None)


def _batch_runs(type_id: int, row: dict[str, Any]) -> int:
    """Blueprint runs for one batch; see input_row.require_batch_runs.

    PlannerInputRow already required a positive `manufacturing_job.runs` on
    every top-level overview row, so a missing or non-positive value here means
    a row bypassed that contract. Raise the same error rather than default to
    1: one run on a 20-run job under-buys materials by 20x. Deliberately no
    fallback to `quantity`, which is a units total, not a run count.
    """
    return require_batch_runs(type_id, orow.get_manufacturing_job(row))


def _sub_jobs(type_id: int, row: dict[str, Any]) -> list[dict[str, Any]]:
    """The sub-build's jobs, ChainPlanner's `sub_job_batches`, or [] with a
    WARNING when the row has none.

    Without the job list the split over slots is unknown; one action on one
    slot for the whole batch was the slot overcommit. No action means the
    shopping list subtracts nothing from the parents' purchase, so the
    component is bought instead (the safe direction).
    """
    jobs = row.get("sub_job_batches")
    if not isinstance(jobs, list) or not jobs:
        logger.warning(
            "CharacterAssigner: sub-component type_id=%s has no sub_job_batches "
            "(got %r), so its job split is unknown; not assigning it, the "
            "component is bought instead",
            type_id, jobs,
        )
        return []
    return jobs


def _sub_manufacture_notes(
    row: dict[str, Any], job: dict[str, Any], index: int, job_count: int
) -> str:
    """The sub-manufacture action's note, naming the job when the runs were
    split over several jobs at the blueprint's max runs per job
    (ChainPlanner._sub_batch), and the spare units when the job makes more
    than it counts toward the need."""
    notes = (
        f"Sub-manufacture: cheaper to build than buy "
        f"(market: {(row.get('market_buy_cost') or 0)/1e6:.1f}M ISK)"
    )
    if job_count > 1:
        notes += (
            f"; job {index + 1} of {job_count} "
            f"(at most {row['sub_max_runs_per_job']} runs per job)"
        )
    if job["output"] > job["quantity"]:
        notes += f"; outputs {job['output']}, {job['quantity']} needed"
    return notes
