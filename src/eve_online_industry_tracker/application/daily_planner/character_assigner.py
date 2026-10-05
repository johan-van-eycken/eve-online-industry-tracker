"""CharacterAssigner — Phase 6: assign planned jobs to characters.

Priority rules:
  1. Manufacturing / sub-manufacture → character with most free manufacturing slots
  2. Invention → character with highest science skill sum + free research slot
  3. Copy → character with most free research slots
  4. ME/TE research → character with most free research slots
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.application.daily_planner.input_row import PlannerInputError
from eve_online_industry_tracker.application.daily_planner.models import AssignedAction, ItemDecision
from eve_online_industry_tracker.application.industry import overview_row as orow

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
# Skills that make a pilot a better inventor, used only to rank candidates.
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
    ) -> list[AssignedAction]:
        """Return list of AssignedAction for all planned jobs."""
        now = _now()

        # Build character capability map
        characters = self._get_characters(characters_service)
        if not characters:
            logger.warning("CharacterAssigner: no characters available")
            return []

        char_slots = self._compute_available_slots(characters, industry_jobs, now)
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
            char_id, char_name = self._best_research_char(char_slots, characters)
            if char_id is not None:
                actions.append(AssignedAction(
                    type_id=orow.get_blueprint_type_id(row) or type_id,
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
                ))
                self._take_slot(char_slots, char_id, "me_research")
            else:
                logger.debug("CharacterAssigner: no free research slot for ME research on type_id=%s", type_id)

        if row.get("needs_te_research"):
            char_id, char_name = self._best_research_char(char_slots, characters)
            if char_id is not None:
                actions.append(AssignedAction(
                    type_id=orow.get_blueprint_type_id(row) or type_id,
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
                ))
                self._take_slot(char_slots, char_id, "te_research")

        # Invention
        if row.get("needs_invention"):
            char_id, char_name = self._best_invention_char(char_slots, characters)
            if char_id is not None:
                actions.append(AssignedAction(
                    type_id=type_id,
                    type_name=type_name,
                    action_type="invent",
                    character_id=char_id,
                    character_name=char_name,
                    quantity=None,
                    runs=None,
                    estimated_cost_isk=None,
                    estimated_profit_isk=None,
                    estimated_completion=None,
                    notes="Invention: BPC stock below pipeline threshold",
                ))
                self._take_slot(char_slots, char_id, "invent")

        # Copy (T1 BPO copy for invention feed). What is copied is the T1
        # source blueprint, never the T2 product or the T2 blueprint.
        t1_blueprint_type_id = int(row.get("t1_blueprint_type_id") or 0)
        if row.get("needs_invention") and row.get("has_t1_bpo") and t1_blueprint_type_id <= 0:
            logger.warning(
                "CharacterAssigner: has_t1_bpo set without a t1_blueprint_type_id for "
                "type_id=%s; not creating a copy action",
                type_id,
            )
        elif row.get("needs_invention") and row.get("has_t1_bpo"):
            char_id, char_name = self._best_research_char(char_slots, characters)
            if char_id is not None:
                actions.append(AssignedAction(
                    type_id=t1_blueprint_type_id,
                    type_name=type_name + " (copy)",
                    action_type="copy",
                    character_id=char_id,
                    character_name=char_name,
                    quantity=None,
                    runs=None,
                    estimated_cost_isk=None,
                    estimated_profit_isk=None,
                    estimated_completion=None,
                    notes="Copy T1 BPO to feed invention pipeline",
                ))
                self._take_slot(char_slots, char_id, "copy")

        # Sub-manufacture
        if decision.is_sub_component:
            char_id, char_name = self._best_mfg_char(char_slots, characters)
            if char_id is not None:
                qty_needed = int(row.get("quantity_needed") or 0)
                actions.append(AssignedAction(
                    type_id=type_id,
                    type_name=type_name,
                    action_type="sub_manufacture",
                    character_id=char_id,
                    character_name=char_name,
                    quantity=qty_needed,
                    runs=row.get("sub_runs"),
                    estimated_cost_isk=float(row.get("sub_manufacture_cost") or 0.0),
                    estimated_profit_isk=None,
                    estimated_completion=None,
                    notes=(
                        f"Sub-manufacture: cheaper to build than buy "
                        f"(market: {(row.get('market_buy_cost') or 0)/1e6:.1f}M ISK)"
                    ),
                    materials=row.get("sub_batch_materials"),
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
                estimated_profit_isk=estimated_profit,
                estimated_completion=None,
                notes=f"Build: {decision.decision_reason}",
                materials=orow.get_batch_materials(row),
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
        """Compute free slot counts per character, treating end_date < now as already delivered."""
        slot_map: dict[int, dict[str, Any]] = {}
        for char in characters:
            char_id = int(char.get("character_id") or char.get("id") or 0)
            if char_id <= 0:
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
            }

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
        self, char_slots: dict[int, dict], characters: list[dict]
    ) -> tuple[int | None, str | None]:
        """Highest science skill sum among characters with a free research slot.

        Ties break toward the character with the fewest active jobs.
        """
        candidates = []
        for slot in char_slots.values():
            if slot.get("free_research", 0) <= 0:
                continue
            levels = slot.get("skills") or {}
            science_sum = sum(int(levels.get(name, 0) or 0) for name in SCIENCE_SKILLS)
            candidates.append(
                (-science_sum, slot.get("total_active_jobs", 0),
                 slot["character_id"], slot["character_name"])
            )
        if not candidates:
            return None, None
        candidates.sort()
        return candidates[0][2], candidates[0][3]


def _job_attr(job: Any, attr: str) -> Any:
    if isinstance(job, dict):
        return job.get(attr)
    return getattr(job, attr, None)


def _batch_runs(type_id: int, row: dict[str, Any]) -> int:
    """Blueprint runs for one batch, from manufacturing_job.runs.

    PlannerInputRow already required a positive `manufacturing_job.runs` on
    every top-level overview row, so a missing or non-positive value here means
    a row bypassed that contract. Raise the same error rather than default to
    1: one run on a 20-run job under-buys materials by 20x. Deliberately no
    fallback to `quantity`, which is a units total, not a run count.
    """
    raw = orow.get_manufacturing_job(row).get("runs")
    try:
        runs = int(raw)
    except (TypeError, ValueError):
        raise PlannerInputError(
            type_id=type_id, field="manufacturing_job.runs",
            detail="is missing" if raw is None else f"is not an integer: {raw!r}",
        ) from None
    if runs <= 0:
        raise PlannerInputError(
            type_id=type_id, field="manufacturing_job.runs", detail=f"must be > 0, got {runs}"
        )
    return runs
