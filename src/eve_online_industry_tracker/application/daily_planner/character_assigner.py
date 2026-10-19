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

import dataclasses
import logging
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from eve_online_industry_tracker.application.daily_planner.assigned_invention import (
    assigned_invention,
    assigned_invention_lanes,
    invention_probability_floor,
)
from eve_online_industry_tracker.application.daily_planner.input_row import require_batch_runs
from eve_online_industry_tracker.application.daily_planner.models import AssignedAction, ItemDecision
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.industry.research_time import research_skill_time_multiplier
# The slot model and skill readers are shared with the producer's parallel
# invention lanes (Task 12), so both count the same slots.
from eve_online_industry_tracker.application.industry.character_slots import (
    MFG_SLOT_SKILLS,
    research_slot_capacity,
    skill_levels_by_id,
    skill_levels_by_name,
    skills_loaded as _skills_loaded,
    slot_capacity as _slot_capacity,
)
from eve_online_industry_tracker.application.industry.invention_model import (
    InventionSkillSet,
    can_run_invention,
    invention_skill_factor,
    required_invention_skill_levels,
)

logger = logging.getLogger(__name__)

#: On the copy / invent actions of a row that cannot manufacture yet.
NO_MANUFACTURE_NOTE = "no manufacture until a T2 BPC exists"
#: Task 15 fix rounds 1-2 (I1, B): why a row gets no invent action today.
NO_FREE_RESEARCH_SLOT_REASON = "no free research slot today"
NO_FREE_SKILLED_INVENTOR_REASON = "no free pilot with the invention skills"

# EVE activity IDs (domain.industry_activities)
from eve_online_industry_tracker.domain.industry_activities import (  # noqa: E402
    ACTIVITY_COPYING,
    ACTIVITY_INVENTION,
    ACTIVITY_MANUFACTURING,
    ACTIVITY_ME_RESEARCH as ACTIVITY_RESEARCHING_ME,
    ACTIVITY_TE_RESEARCH as ACTIVITY_RESEARCHING_TE,
)

# Fallback inventor ranking for a row without the producer's invention skills.
SCIENCE_SKILLS = ("Science", "Advanced Industry", "Metallurgy", "Research")


def _now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


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

        resize = getattr(chain_plan, "resize_sub_components", None)
        # Research-first parents that got no research slot and build from
        # their kept BPO row instead (filled by _assign_decision).
        fell_back: set[int] = set()
        # The T1 BPCs ("bpcs") and T1 BPOs ("bpos") already given to an
        # invent / copy job, across rows (fix round 3, Task 14): one job per
        # blueprint, so each is used once, in assignment order. "released":
        # {T1 blueprint type_id: BPCs} a row could not invent from today,
        # offered to the lower rows of that source (Task 15 fix round 4).
        bpc_pool: dict[str, Any] = {}
        # The actions of each top-level row (Task A): which claims they use.
        emitted: dict[int, list[AssignedAction]] = {}

        def _assign_one(decision: ItemDecision) -> list[AssignedAction]:
            return self._assign_decision(
                decision=decision,
                row=decision.overview_row,
                char_slots=char_slots,
                characters=characters,
                now=now,
                probability_floor=probability_floor,
                price_map=price_map or {},
                fell_back=fell_back,
                can_resize=callable(resize),
                bpc_pool=bpc_pool,
            )

        for pass_decisions in (top_level, None):
            if pass_decisions is None:
                # Task A items 2, 3 and 6: blueprints claimed for a job that
                # got no action go back; rows not built for them are planned
                # again, before any sub-component takes a slot.
                replan = getattr(chain_plan, "replan_released", None)
                undo = getattr(chain_plan, "undo_replan", None)
                tried: set[int] = set()
                for _pass in range(MAX_REPLAN_PASSES if callable(replan) else 0):
                    release, used_by = _unused_claims(top_level, emitted, bpc_pool)
                    eligible = {int(d.type_id) for d in top_level if not emitted.get(id(d)) and id(d) not in tried}
                    revived = replan(release, used_by, eligible) if (release or used_by) else []
                    if not revived:
                        break
                    for decision in revived:
                        tried.add(id(decision))
                        actions = _assign_one(decision)
                        if not actions and callable(undo):
                            # Still nothing to do today (no free slot): keep
                            # its not-built plan and reason as they were.
                            undo(decision)
                            bpc_pool["releases"] = [e for e in bpc_pool.get("releases") or [] if e[0] is not decision]
                            continue
                        logger.info("CharacterAssigner: type_id=%s re-planned after a blueprint release", decision.type_id)
                        emitted[id(decision)] = actions
                        assigned_actions.extend(actions)
                if fell_back and callable(resize):
                    # Their components were sized for the research-first
                    # batch: re-plan them for the batch actually built (fix
                    # round 5), before any sub-component takes a slot.
                    resized = resize(set(fell_back))
                    sub_components = [d for d in resized if d.decision in ("build", "pause")]
                    rebuild = getattr(chain_plan, "rebuild_decisions", None)
                    # One ItemDecision per type_id (Task 14): the planner's own merge.
                    chain_plan.decisions[:] = rebuild(resized) if callable(rebuild) else [
                        *(d for d in chain_plan.decisions if not d.is_sub_component), *sub_components,
                    ]
                    logger.info(
                        "CharacterAssigner: re-sized the sub-components for type_id(s) %s, "
                        "built from their kept BPO row", sorted(fell_back),
                    )
                pass_decisions = sub_components
            for decision in pass_decisions:
                actions = _assign_one(decision)
                if pass_decisions is top_level:
                    emitted[id(decision)] = actions
                # Slots are consumed inside _assign_decision, as each action is
                # created -- two research actions on one item must not share a slot.
                assigned_actions.extend(actions)

        if fell_back:
            _reword_unused_research_claims(chain_plan.decisions, assigned_actions, fell_back)
        _reword_released_bpcs(chain_plan.decisions, bpc_pool)
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
        fell_back: set[int] | None = None,
        can_resize: bool = False,
        bpc_pool: dict[str, Any] | None = None,
    ) -> list[AssignedAction]:
        """Return AssignedAction(s) for a single ItemDecision. A research-first
        parent that builds from its kept BPO row is added to `fell_back`;
        `can_resize` says its components will be re-sized for that row."""
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

        # Task A item 1: a pilot's own blueprint takes jobs from that pilot
        # only. Pooled across rows: a BPC a higher row released keeps its owner.
        owner_by_item = _owner_by_item(decision)
        if bpc_pool is not None:
            bpc_pool.setdefault("owners", {}).update(owner_by_item)
            owner_by_item = dict(bpc_pool["owners"])
        research_owners = _claim_owners(decision, owner_by_item, "research")

        # ME/TE research
        me_scheduled, lab_free_at = False, now
        me_pilot: tuple[int | None, str | None] = (None, None)
        if row.get("needs_me_research"):
            bp_type_id = self._research_blueprint_type_id(row, type_id, "ME")
            char_id, char_name = (
                (None, None) if bp_type_id is None
                else self._best_research_char(char_slots, characters, only=research_owners)
            )
            if char_id is not None:
                me_timing = _research_timing(
                    row, "research_material", "me_current", "me_research_target", now,
                    f"ME research: {row.get('me_current', '?')} → {row.get('me_research_target', '?')}",
                    _pilot_skills(char_slots, char_id), pilot_id=char_id,
                )
                # EVE runs one job per blueprint: a TE job on the same BPO starts after this one.
                lab_free_at: datetime | None = me_timing["estimated_completion"]
                me_scheduled = True
                me_pilot = (char_id, char_name)
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
                    **me_timing,
                    **_job_fee(
                        *_research_fee(row, "research_material", "me_current", "me_research_target"),
                        "me_research", type_id,
                    ),
                ))
                self._take_slot(char_slots, char_id, "me_research")
            elif bp_type_id is not None:
                logger.debug("CharacterAssigner: no free research slot for ME research on type_id=%s", type_id)

        if row.get("needs_te_research"):
            bp_type_id = self._research_blueprint_type_id(row, type_id, "TE")
            if me_scheduled:
                # Queued behind the ME job on the same BPO (one job per
                # blueprint): it cannot start in this plan, so it goes to the
                # ME pilot and takes no research slot now.
                char_id, char_name = me_pilot
            else:
                char_id, char_name = (
                    (None, None) if bp_type_id is None
                    else self._best_research_char(char_slots, characters, only=research_owners)
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
                    **_research_timing(
                        row, "research_time", "te_current", "te_research_target", lab_free_at,
                        _te_research_note(row), _pilot_skills(char_slots, char_id),
                        after_me=me_scheduled, pilot_id=char_id,
                    ),
                    **_job_fee(
                        *_research_fee(row, "research_time", "te_current", "te_research_target"),
                        "te_research", type_id,
                    ),
                    queued_after_me=me_scheduled,
                ))
                if not me_scheduled:
                    self._take_slot(char_slots, char_id, "te_research")

        # Invention and the T1 copies feeding it (Task 12). Invention runs over
        # parallel lanes: one invent action per free research slot of every
        # qualifying pilot that runs at least one attempt, each at its own
        # chance, sized by the formula the producer's elapsed time uses; one
        # lane or none qualifying: the single best free pilot (A4).
        #
        # Fix round 2: in the copying stage (an owned T1 BPO) the copy comes
        # FIRST, for the attempts the T1 BPC runs on hand do not cover. With
        # none on hand only the copy is planned, so the invention can never
        # take every research slot while no BPC exists to invent from.
        #
        # Task 14: one invention job per T1 BPC, of at most its runs, so the
        # lanes come from the row's list of BPCs (t1_bpc_runs_by_item, each
        # BPC used once across rows), and one copy job per T1 BPO, each BPO
        # used once across rows. A copy states copies x runs per copy.
        # What is copied is the T1 source blueprint, never the T2 product.
        t1_blueprint_type_id = int(row.get("t1_blueprint_type_id") or 0)
        wants_copy = bool(row.get("needs_invention") and row.get("has_t1_bpo"))
        if wants_copy and t1_blueprint_type_id <= 0:
            logger.warning(
                "CharacterAssigner: has_t1_bpo set without a t1_blueprint_type_id for "
                "type_id=%s; not creating a copy action",
                type_id,
            )
            wants_copy = False
        stage_reason: str | None = None
        if row.get("needs_invention"):
            pools = bpc_pool if bpc_pool is not None else {}
            used_bpcs: set[Any] = pools.setdefault("bpcs", set())
            used_bpos: set[Any] = pools.setdefault("bpos", set())
            # None: a row without BPC data (not from ChainPlanner): uncapped.
            bpcs = _row_bpcs(row, t1_blueprint_type_id, used_bpcs, carries_bpcs=wants_copy)
            own_bpcs = list(bpcs or [])
            # Task 15 fix round 4: T1 BPCs a higher row of the same source
            # could not invent from today (no qualifying pilot free) are
            # this row's too; used_bpcs keeps each in one job.
            released: dict[int, list[Any]] = pools.setdefault("released", {})
            if bpcs is not None and released.get(t1_blueprint_type_id):
                own = {e[0] for e in bpcs}
                bpcs = sorted(
                    [*bpcs, *(e for e in released[t1_blueprint_type_id] if e[0] not in used_bpcs and e[0] not in own)],
                    key=lambda e: (-e[2], str(e[0])),
                )
            on_hand = sum(runs for _key, _item, runs in bpcs) if bpcs is not None else 0
            if wants_copy:
                copy_bpos = _row_copy_bpos(row, t1_blueprint_type_id, used_bpos)
                if not copy_bpos:
                    stage_reason = (
                        f"no free T1 BPO ({t1_blueprint_type_id}) to copy: another row copies it today, no copy"
                    )
                    logger.info("CharacterAssigner: type_id=%s: %s", type_id, stage_reason)
                # 1. The copy's slots first, steered away from the inventors
                #    (non-inventors, then the weakest inventor first).
                reserved, reserved_bpos, blocked_owners = self._reserve_owned_copy_pilots(
                    char_slots, row, copy_bpos, owner_by_item,
                )
                if not reserved and blocked_owners:
                    stage_reason = (
                        f"owner {_pilot_names(characters, blocked_owners)} has no free slot: "
                        f"no copy of its T1 BPO ({t1_blueprint_type_id}) today"
                    )
                # 2. Sized for the invent lanes actually free after the copy
                #    took its slots (fix round 3); none free: the weakest
                #    qualifying pilot alone (the most attempts).
                max_runs_per_copy = _max_runs_per_copy(row, type_id) if copy_bpos else None
                jobs, needed, lane_sizes = self._copy_jobs_for(
                    char_slots, characters, row, on_hand, len(reserved), max_runs_per_copy,
                    probability_floor=probability_floor, price_map=price_map,
                )
                # 3. Give back the slots no copy job needs; a returned slot can
                #    free an invent lane, so the lanes AND the copies are re-sized
                #    (fix round 4) until no further slot comes back.
                while jobs is not None and len(jobs) < len(reserved):
                    for char_id, _name in reserved[len(jobs):]:
                        self._release_slot(char_slots, char_id, "copy")
                    reserved = reserved[:len(jobs)]
                    reserved_bpos = reserved_bpos[:len(jobs)]
                    jobs, needed, lane_sizes = self._copy_jobs_for(
                        char_slots, characters, row, on_hand, len(reserved), max_runs_per_copy,
                        probability_floor=probability_floor, price_map=price_map,
                    )
                if copy_bpos and not reserved and (jobs is None or jobs):
                    logger.info("CharacterAssigner: type_id=%s: no free research slot for its copy", type_id)
                if needed is None:
                    note = f"the copy runs are unknown (the invention attempts are unknown); {on_hand} T1 BPC run(s) on hand"
                elif on_hand == 0:
                    note = "no T1 BPC on hand: no invention today; it starts once these copies are delivered"
                else:
                    note = f"{on_hand} T1 BPC run(s) on hand cover part of the attempts; these copies cover the rest"
                if reserved and (jobs is None or jobs):
                    actions.extend(self._copy_actions(
                        row, type_id, type_name, t1_blueprint_type_id, reserved, jobs, note,
                        max_runs_per_copy=max_runs_per_copy, lane_sizes=lane_sizes,
                    ))
                    used_bpos.update(reserved_bpos)
                    # Which T1 BPOs this row's copies use (Task A item 6).
                    pools.setdefault("copy_used", {}).setdefault(int(type_id), []).extend(reserved_bpos)
                if on_hand <= 0:
                    logger.info(
                        "CharacterAssigner: type_id=%s: no T1 BPC on hand; copy first, no invent action today",
                        type_id,
                    )
            if bpcs is None or bpcs:
                # Today's lanes: at most one per T1 BPC, the best pilots first.
                bpc_owners = [owner_by_item.get(e[1]) if e[1] is not None else None for e in bpcs or []]
                results, free_lanes = self._invention_results(
                    char_slots, characters, row, probability_floor=probability_floor, price_map=price_map,
                    max_lanes=len(bpcs) if bpcs is not None else None, bpc_owners=bpc_owners,
                )
                lanes: list[tuple[int, Any, tuple[Any, int | None, int] | None]] = (
                    [(cid, inv, None) for cid, inv in results] if bpcs is None
                    else _lanes_on_bpcs(results, bpcs, owner_by_item)
                )
                if bpcs is not None:
                    # The lanes kept after the cap (fix round 4), not the free ones.
                    free_lanes = min(free_lanes, len(lanes))
                    used_bpcs.update(bpc[0] for _cid, _inv, bpc in lanes if bpc is not None)
                    # Who invents from each BPC, for a releasing row's reason.
                    takers: dict[Any, ItemDecision] = pools.setdefault("taken_by", {})
                    for _cid, _inv, bpc in lanes:
                        if bpc is not None:
                            takers.setdefault(bpc[0], decision)
                notes = _fewer_lanes_note(row, free_lanes)
                if results and bpcs is not None:
                    # Task A item 3: fewer lanes than the BPCs this row
                    # claimed: the rest go to the lower rows of the source,
                    # as when the row invents nothing (worded after the loop).
                    in_lanes = {bpc[0] for _cid, _inv, bpc in lanes if bpc is not None}
                    leftover = [e for e in own_bpcs if e[0] not in in_lanes]
                    lane_pilots = {cid for cid, _inv, _bpc in lanes}
                    stuck = sorted({
                        owner_by_item[e[1]] for e in leftover
                        if e[1] in owner_by_item and owner_by_item[e[1]] not in lane_pilots
                    })
                    if stuck:
                        decision.decision_reason = (
                            f"{decision.decision_reason}; owner {_pilot_names(characters, stuck)} "
                            "has no free slot for its own T1 BPC(s)"
                        )
                    if leftover:
                        released.setdefault(t1_blueprint_type_id, []).extend(leftover)
                        placeholder = _released_placeholder(type_id)
                        pools.setdefault("releases", []).append((decision, list(leftover), placeholder))
                        decision.decision_reason = (
                            f"{decision.decision_reason}; fewer invent lanes than T1 BPCs today: {placeholder}"
                        )
                if not results:
                    # I1 (Task 15 fix round 1): never an unqualified pilot,
                    # whose chance and attempts would be another pilot's.
                    # Fix round 2 (B): no research slot free at all, or free
                    # slots only on pilots without the skills.
                    why = (
                        NO_FREE_SKILLED_INVENTOR_REASON
                        if any(int(s.get("free_research", 0)) > 0 for s in char_slots.values())
                        else NO_FREE_RESEARCH_SLOT_REASON
                    )
                    if bpc_owners and all(o is not None for o in bpc_owners):
                        # Every BPC is a pilot's own: only its owner may invent.
                        owners = sorted(set(bpc_owners))
                        free = any(int((char_slots.get(o) or {}).get("free_research", 0)) > 0 for o in owners)
                        why = (
                            f"owner {_pilot_names(characters, owners)} "
                            f"{'lacks the invention skills' if free else 'has no free slot'}"
                        )
                    logger.info("CharacterAssigner: type_id=%s: no invention today: %s", type_id, why)
                    if bpcs:
                        # Task 15 fix round 4: the BPCs ChainPlanner claimed
                        # for it would sit idle all day; a lower row of the
                        # same T1 source may invent from them instead.
                        # Fix round 5: what became of each BPC is worded after
                        # every row is assigned (_reword_released_bpcs).
                        released.setdefault(t1_blueprint_type_id, []).extend(bpcs)
                        placeholder = _released_placeholder(type_id)
                        pools.setdefault("releases", []).append((decision, list(bpcs), placeholder))
                        why = f"{why}; {placeholder}"
                    if "no invention today:" not in (decision.decision_reason or ""):
                        decision.decision_reason = f"{decision.decision_reason}; no invention today: {why}"
                # The producer's choice: bought one per attempt by the shopping list.
                decryptor = orow.get_invention_decryptor(row)
                for char_id, invention, bpc in lanes:
                    action = self._invent_action(
                        row, type_id, type_name, char_id, str(char_slots[char_id].get("character_name") or ""),
                        invention, decryptor,
                    )
                    if bpc is not None and bpc[1] is not None:
                        action.blueprint_item_id = bpc[1]
                        action.notes = f"{action.notes}; from T1 BPC item {bpc[1]} ({_count(bpc[2], 'run', 'runs')})"
                    if notes:
                        action.notes = f"{action.notes}; {notes}"
                    actions.append(action)
                    self._take_slot(char_slots, char_id, "invent")

        # Sub-manufacture: one action, and one manufacturing slot, per job.
        # A job left without a slot gets no action, so the shopping list
        # neither buys its materials nor subtracts its units from the
        # parents' purchase: those units are bought instead.
        if decision.is_sub_component:
            jobs = _sub_jobs(type_id, row)
            pilots: list[tuple[int | None, str | None]] = []
            for index, job in enumerate(jobs):
                after = job.get("after_job")
                queued = isinstance(after, int) and 1 <= after <= len(pilots)
                # Minor 4: a job beyond the free BPOs waits for an earlier job
                # on the same BPO; it goes to that job's pilot and takes no
                # slot of its own (the earlier job's slot frees first).
                char_id, char_name = pilots[after - 1] if queued else self._best_mfg_char(char_slots, characters)
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
                    notes=_sub_manufacture_notes(row, job, index, len(jobs)) + (
                        f"; sequential on the same BPO (later): starts after job {after}" if queued else ""
                    ),
                    materials=job["materials"],
                    # The component's fee per run x this job's runs (ChainPlanner).
                    **_job_fee(job.get("fee"), job.get("fee_unknown_reason"), "sub_manufacture", type_id),
                ))
                pilots.append((char_id, char_name))
                if not queued:
                    self._take_slot(char_slots, char_id, "sub_manufacture")
            return actions  # sub-components don't get a manufacture action

        # Research first (research_first.py): ChainPlanner planned, before
        # sizing the sub-components, how this item builds while its BPO is
        # in the lab: from an owned-BPC variant (the row is that variant),
        # from the owned-BPC runs only, or not at all. It applies only when a
        # research action was emitted; without a free research slot the BPO
        # is not in the lab and builds from the kept row as usual. A priced
        # BPO in an active industry job (`unconditional`) applies always.
        first = row.get("research_first") if isinstance(row.get("research_first"), dict) else None
        build_note: str | None = None
        if first is not None:
            research = [a for a in actions if a.action_type in ("me_research", "te_research")]
            if not research and first["mode"] != "other_bpo" and not first.get("unconditional"):
                build_note = "BPO research planned but no free research slot: built from the BPO"
                sized = (
                    "its components re-sized for the BPO's batch" if can_resize
                    else first.get("sized_note")
                )
                if sized:
                    build_note += f"; {sized}"
                logger.warning(
                    "CharacterAssigner: type_id=%s: no free research slot for the planned research "
                    "on its BPO; building from the BPO instead (%s)",
                    type_id, sized or "no sub-component change",
                )
                row = first.get("kept_row") or row
                first = None
                if fell_back is not None:
                    fell_back.add(int(type_id))
            else:
                for action in research:
                    action.notes = f"{action.notes}; {first['note']}"
                if first["mode"] == "none":
                    logger.info("CharacterAssigner: type_id=%s: no manufacture: %s", type_id, first["note"])
                    _no_manufacture_today(decision, str(first["note"]).split(": not built")[0])
                    return actions

        # No T2 BPC yet (fix round 3): a row still copying or inventing gets
        # no manufacture, so its batch materials are not bought and its fee
        # not reserved before invented BPCs exist. Gated on the stage: an
        # invention-stage row carries no needs_invention.
        if decision.pipeline_stage in ("copying", "invention"):
            for action in actions:
                if action.action_type in ("copy", "invent"):
                    action.notes = f"{action.notes}; {NO_MANUFACTURE_NOTE}"
            logger.info(
                "CharacterAssigner: type_id=%s: stage %s, no manufacture until a T2 BPC exists",
                type_id, decision.pipeline_stage,
            )
            _no_manufacture_today(decision, stage_reason or (
                "copying the T1 BPO for invention; no T2 BPC yet" if decision.pipeline_stage == "copying"
                else "inventing from T1 BPCs; no T2 BPC yet"
            ))
            return actions

        # Manufacturing. Task A item 1: the batch's own blueprints decide
        # who may install it; the research BPO counts unless it is in the lab.
        researching = any(a.action_type in ("me_research", "te_research") for a in actions)
        mfg_owners = _claim_owners(
            decision, owner_by_item, "manufacture", *(() if researching else ("research",)),
        )
        if len(mfg_owners) > 1:
            _no_manufacture_today(decision, (
                f"its blueprints belong to several pilots ({_pilot_names(characters, mfg_owners)}): "
                "no one pilot can install the batch"
            ))
            return actions
        char_id, char_name = self._best_mfg_char(char_slots, characters, only=mfg_owners)
        if char_id is None and mfg_owners:
            logger.info("CharacterAssigner: type_id=%s: its blueprint's owner has no free mfg slot", type_id)
            _no_manufacture_today(decision, f"owner {_pilot_names(characters, mfg_owners)} has no free slot")
        if char_id is not None and first is not None and first["mode"] == "part":
            actions.append(AssignedAction(
                type_id=type_id,
                type_name=type_name,
                action_type="manufacture",
                character_id=char_id,
                character_name=char_name,
                quantity=first["quantity"],
                runs=first["runs"],
                # The producer priced the whole batch, not this part of it:
                # unknown, with the reason in the note, not a wrong figure.
                estimated_cost_isk=None,
                estimated_build_cost_isk=None,
                estimated_profit_isk=None,
                estimated_completion=None,
                notes=f"Build: {decision.decision_reason}; {first['note']}; {first['cost_unknown_reason']}",
                materials=dict(first["materials"]),
                **_job_fee(
                    _times_runs(orow.get_manufacturing_fee_per_run(row), first["runs"]),
                    orow.get_manufacturing_fee_unknown_reason(row),
                    "manufacture", type_id,
                ),
            ))
            _component_buy_reasons(decision, actions[-1])
            self._take_slot(char_slots, char_id, "manufacture")
        elif char_id is not None:
            # Both live under manufacturing_job. A top-level runs_per_batch /
            # runs / estimated_material_cost is written by no producer, so
            # every manufacture action used to get 1 run and a 0 ISK cost --
            # and the shopping list bought materials for one run of the batch.
            runs = _batch_runs(type_id, row)
            # The batch's material cost, or None when the producer could not
            # price it -- unknown, not free.
            estimated_cost = orow.get_material_cost_total(row)
            estimated_profit = decision.absolute_profit_per_batch
            notes = f"Build: {decision.decision_reason}"
            # Task 15 fix round 1 (I2): a lower-bound total cost is no build
            # cost to learn cost_multiplier from (FeedbackProcessor skips None).
            upper_bound = orow.get_profit_upper_bound_reason(row)
            build_cost = None if upper_bound is not None else orow.get_total_cost(row)
            if upper_bound is not None and upper_bound not in notes:
                notes += f"; {upper_bound}; build cost left unknown for feedback"
            if build_note:
                notes += f"; {build_note}"
            if first is not None and first.get("build_note"):
                notes += f"; {first['build_note']}"
            if first is not None and first["mode"] == "variant":
                # The owned-BPC variant's own profit, scored like the kept row's.
                estimated_profit = first.get("profit")
                notes += f"; {first['note']}"
                if first.get("profit_unknown_reason"):
                    notes += f"; profit unknown: {first['profit_unknown_reason']}"
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
                estimated_build_cost_isk=build_cost,
                estimated_profit_isk=estimated_profit,
                estimated_completion=None,
                notes=notes,
                materials=orow.get_batch_materials(row),
                **_job_fee(
                    orow.get_manufacturing_fee(row), orow.get_manufacturing_fee_unknown_reason(row),
                    "manufacture", type_id,
                ),
            ))
            _component_buy_reasons(decision, actions[-1])
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
            max_research = research_slot_capacity(levels)
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
        self, char_slots: dict[int, dict], characters: list[dict], only: list[int] | None = None,
    ) -> tuple[int | None, str | None]:
        """Character with most free manufacturing slots; tie-break by fewest
        total active jobs. `only`: the blueprint owner(s) allowed (Task A)."""
        candidates = [(s["free_mfg"], -s["total_active_jobs"], s["character_id"], s["character_name"])
                      for s in char_slots.values() if s["free_mfg"] > 0
                      and (not only or s["character_id"] in only)]
        if not candidates:
            return None, None
        candidates.sort(key=lambda x: (-x[0], x[1]))
        best = candidates[0]
        return best[2], best[3]

    @staticmethod
    def _research_blueprint_type_id(row: dict[str, Any], type_id: int, kind: str) -> int | None:
        """The blueprint to research, or None (WARNING): a product id is not a blueprint."""
        bp_type_id = orow.get_blueprint_type_id(row)
        # Unreachable today: needs_me/te_research is set only by
        # ChainPlanner._plan_t1_chain for an owned BPO found through this same
        # accessor (and copied onto a research-first variant of the same
        # blueprint), so a flagged row always has a blueprint id. Kept so a
        # future flag source cannot schedule research on a product id.
        if bp_type_id is None:
            logger.warning(
                "CharacterAssigner: %s research flagged for type_id=%s but its row has no "
                "blueprint type id; not creating a research action",
                kind, type_id,
            )
        return bp_type_id

    def _best_research_char(
        self, char_slots: dict[int, dict], characters: list[dict], only: list[int] | None = None,
    ) -> tuple[int | None, str | None]:
        """Character with most free research slots; tie-break by fewest
        active jobs. `only`: the blueprint owner(s) allowed (Task A)."""
        candidates = [(s["free_research"], -s["total_active_jobs"], s["character_id"], s["character_name"])
                      for s in char_slots.values() if s["free_research"] > 0
                      and (not only or s["character_id"] in only)]
        if not candidates:
            return None, None
        candidates.sort(key=lambda x: (-x[0], x[1]))
        best = candidates[0]
        return best[2], best[3]

    def _invention_results(
        self, char_slots: dict[int, dict], characters: list[dict], row: dict[str, Any], *,
        probability_floor: float, price_map: dict[int, Any], max_lanes: int | None = None,
        bpc_owners: list[int | None] | None = None,
    ) -> tuple[list[tuple[int, Any]], int]:
        """(the invent actions' (pilot, figures), the free lanes today): over
        the free qualifying lanes when there is more than one, else the single
        best free pilot (A4). `max_lanes` (Task 14: one lane per T1 BPC)
        keeps the best pilots' lanes only. `bpc_owners` (Task A item 1): the
        owner of each BPC (None: corp); every BPC a pilot's own limits the
        lanes to their owners, and an owner keeps a lane per own BPC. No
        slot is taken here."""
        if max_lanes is not None and max_lanes <= 0:
            return [], 0
        lanes = self._invention_lanes(char_slots, row)
        owners = [o for o in bpc_owners or [] if o is not None]
        only = sorted(set(owners)) if bpc_owners and len(owners) == len(bpc_owners) else None
        if only is not None:
            lanes = [cid for cid in lanes if cid in only]
        if max_lanes is not None and len(lanes) > max_lanes:
            lanes = self._best_lanes(char_slots, row, lanes, max_lanes, owner_need=owners)
        if len(lanes) > 1:
            return assigned_invention_lanes(
                row, lanes=[(cid, char_slots[cid].get("skills_by_id")) for cid in lanes],
                probability_floor=probability_floor, price_map=price_map,
            ), len(lanes)
        if only is not None and not lanes:
            return [], 0
        char_id, _name = (
            (lanes[0], None) if only is not None and len(lanes) == 1
            else self._best_invention_char(char_slots, characters, row)
        )
        if char_id is None:
            return [], 0
        return [(char_id, assigned_invention(
            row, character_id=char_id,
            # None: this pilot's skills are not loaded (not level 0).
            skill_levels_by_id=char_slots[char_id].get("skills_by_id"),
            probability_floor=probability_floor, price_map=price_map,
        ))], 1

    @staticmethod
    def _best_lanes(
        char_slots: dict[int, dict], row: dict[str, Any], lanes: list[int], count: int,
        owner_need: list[int] | None = None,
    ) -> list[int]:
        """The `count` lanes of the pilots with the highest invention skill
        factor for this item (ties: the lower id). `owner_need` (Task A):
        one pilot id per owner-bound BPC; those owners' lanes come first,
        one per own BPC, as only they can invent from them."""
        skills = orow.get_invention_skill_set(row)
        skill_set = InventionSkillSet(skills[0], skills[1]) if skills is not None else None

        def _factor(cid: int) -> float:
            if skill_set is None:
                return 0.0
            return invention_skill_factor(skill_set, char_slots[cid].get("skills_by_id") or {})

        ranked = sorted(lanes, key=lambda cid: (-_factor(cid), cid))
        chosen: list[int] = []
        for owner in owner_need or []:
            if owner in ranked and len(chosen) < count:
                ranked.remove(owner)
                chosen.append(owner)
        chosen.extend(ranked[:max(0, count - len(chosen))])
        return sorted(chosen, key=lambda cid: (-_factor(cid), cid))

    def _copy_jobs_for(
        self, char_slots: dict[int, dict], characters: list[dict], row: dict[str, Any], on_hand: int,
        job_count: int, max_runs_per_copy: int | None, *, probability_floor: float, price_map: dict[int, Any],
    ) -> tuple[list[dict[str, int]] | None, int | None, list[int] | None]:
        """(copy jobs, the attempts needed, the lane sizes the copies match)
        for the invent lanes free now: the T1 BPC runs the invention needs
        (_copy_need) less the runs on hand, shaped as copies x runs per copy
        over at most `job_count` BPOs (_shape_copy_jobs). (None, None, None)
        when the attempts are unknown."""
        results, _free = self._invention_results(
            char_slots, characters, row, probability_floor=probability_floor, price_map=price_map,
        )
        needed = self._copy_need(results, char_slots, row, probability_floor=probability_floor, price_map=price_map)
        if needed is None:
            return None, None, None
        copy_runs = max(0, needed - on_hand)
        wholes = [inv.whole_attempts for _cid, inv in results]
        lane_sizes = [int(w) for w in wholes] if wholes and all(w is not None for w in wholes) else [copy_runs]
        return _shape_copy_jobs(
            copy_runs, lane_sizes, max(1, job_count), max_runs_per_copy, _copy_seconds_per_run(row),
        ), needed, lane_sizes

    def _release_slot(self, char_slots: dict[int, dict], char_id: int, action_type: str) -> None:
        """Give back one slot taken by _take_slot (a reserved copy not needed)."""
        slot = char_slots.get(char_id)
        if slot is None:
            return
        if action_type in self._MFG_ACTIONS:
            slot["free_mfg"] = slot.get("free_mfg", 0) + 1
        elif action_type in self._RESEARCH_ACTIONS:
            slot["free_research"] = slot.get("free_research", 0) + 1

    def _reserve_owned_copy_pilots(
        self, char_slots: dict[int, dict], row: dict[str, Any], copy_bpos: list[Any], owner_by_item: dict[int, int],
    ) -> tuple[list[tuple[int, str | None]], list[Any], list[int]]:
        """(pilots, the BPO each copies, owners with no free slot): a pilot's
        own T1 BPO is copied by that pilot only (Task A item 1), first; the
        corp BPOs then go to the pilots _reserve_copy_pilots steers to."""
        reserved: list[tuple[int, str | None]] = []
        reserved_bpos: list[Any] = []
        blocked: list[int] = []
        corp: list[Any] = []
        for bpo in copy_bpos:
            owner = owner_by_item.get(bpo) if isinstance(bpo, int) else None
            if owner is None:
                corp.append(bpo)
            elif int((char_slots.get(owner) or {}).get("free_research", 0)) > 0:
                reserved.append((owner, char_slots[owner].get("character_name")))
                reserved_bpos.append(bpo)
                self._take_slot(char_slots, owner, "copy")
            elif owner not in blocked:
                blocked.append(owner)
        for pilot, bpo in zip(self._reserve_copy_pilots(char_slots, row, len(corp)), corp):
            reserved.append(pilot)
            reserved_bpos.append(bpo)
        return reserved, reserved_bpos, blocked

    def _reserve_copy_pilots(
        self, char_slots: dict[int, dict], row: dict[str, Any], count: int,
    ) -> list[tuple[int, str | None]]:
        """Take up to `count` research slots for copy jobs, steered away from
        the inventors (fix round 3): pilots who cannot run this invention
        first (most free slots first), then the inventors weakest first, so
        the best inventor copies only when nobody else can."""
        skills = orow.get_invention_skill_set(row)
        skill_set = InventionSkillSet(skills[0], skills[1]) if skills is not None else None
        required = self._required_levels(row)
        reserved: list[tuple[int, str | None]] = []
        for _ in range(max(0, int(count))):
            candidates = []
            for cid, slot in char_slots.items():
                if int(slot.get("free_research", 0)) <= 0:
                    continue
                levels = slot.get("skills_by_id")
                inventor = (
                    skill_set is not None and required is not None and levels is not None
                    and can_run_invention(required, levels)
                )
                factor = invention_skill_factor(skill_set, levels) if inventor and skill_set is not None else 0.0
                candidates.append((
                    1 if inventor else 0, factor, -int(slot.get("free_research", 0)),
                    int(slot.get("total_active_jobs", 0)), cid,
                ))
            if not candidates:
                break
            cid = min(candidates)[-1]
            reserved.append((cid, char_slots[cid].get("character_name")))
            self._take_slot(char_slots, cid, "copy")
        return reserved

    def _copy_need(
        self, results: list[tuple[int, Any]], char_slots: dict[int, dict], row: dict[str, Any], *,
        probability_floor: float, price_map: dict[int, Any],
    ) -> int | None:
        """The T1 BPC runs the invention needs (one per attempt): today's
        free invent lanes' total; with none free, the weakest qualifying
        pilot alone (the most attempts: never short); nobody qualifying, the
        producer's whole attempts. None when unknown."""
        if results:
            wholes = [inv.whole_attempts for _cid, inv in results]
            return None if any(w is None for w in wholes) else sum(int(w) for w in wholes)
        required = self._required_levels(row)
        qualifying = [
            cid for cid, slot in sorted(char_slots.items())
            if required is not None and slot.get("skills_by_id") is not None
            and can_run_invention(required, slot["skills_by_id"])
        ]
        if qualifying:
            wholes = [
                assigned_invention(
                    row, character_id=cid, skill_levels_by_id=char_slots[cid]["skills_by_id"],
                    probability_floor=probability_floor, price_map=price_map,
                ).whole_attempts
                for cid in qualifying
            ]
            return None if any(w is None for w in wholes) else max(int(w) for w in wholes)
        needed = (orow.get_invention_costs(row) or {}).get("whole_attempts")
        if isinstance(needed, (int, float)) and not isinstance(needed, bool) and needed > 0:
            return int(needed)
        return None

    @staticmethod
    def _required_levels(row: dict[str, Any]) -> dict[int, int] | None:
        """The row's required invention skill levels (the producer's, else
        level 1 of each skill in its set); None without a skill set."""
        skills = orow.get_invention_skill_set(row)
        if skills is None:
            return None
        return orow.get_invention_required_skill_levels(row) or required_invention_skill_levels(
            InventionSkillSet(skills[0], skills[1]), [],
        )

    def _copy_actions(
        self, row: dict[str, Any], type_id: int, type_name: str, t1_blueprint_type_id: int,
        pilots: list[tuple[int, str | None]], jobs: list[dict[str, int]] | None, note: str, *,
        max_runs_per_copy: int | None = None, lane_sizes: list[int] | None = None,
    ) -> list[AssignedAction]:
        """One copy action per reserved pilot, each one copy job on its own
        free T1 BPO (a BPO is in one job at a time, a corp BPO is installed by
        any pilot). In EVE a copy job's runs are its number of copies, each
        with its own licensed runs; here `quantity` is the copies and `runs`
        the copied BPC runs in all (copies x runs per copy), which the copy
        fee is charged on (job_cost.py) and one invention attempt uses one
        of. `jobs` None: the runs are unknown. The slots are already taken."""
        actions: list[AssignedAction] = []
        for index, (char_id, char_name) in enumerate(pilots):
            job = jobs[index] if jobs is not None and index < len(jobs) else None
            runs = None if job is None else job["copies"] * job["runs_per_copy"]
            shape: list[str] = []
            if job is not None:
                copies = job["copies"]
                shape.append(f"make {_count(copies, 'copy', 'copies')} × {_count(job['runs_per_copy'], 'run', 'runs')}")
                if lane_sizes and len(lane_sizes) > 1:
                    shape.append(f"for invent lanes of {' + '.join(str(n) for n in lane_sizes)} attempts")
                if job.get("short"):
                    shape.append(
                        f"more copies are needed than one job makes: {job['short']} more in a later plan"
                    )
                if max_runs_per_copy is None:
                    shape.append("runs per copy not capped: the SDE max runs per copy is unknown")
                if _copy_seconds_per_run(row) is None:
                    shape.append("the SDE copy time is unknown: copies per job not checked against the 30-day job limit")
            actions.append(AssignedAction(
                type_id=t1_blueprint_type_id,
                type_name=type_name + " (copy)",
                action_type="copy",
                character_id=char_id,
                character_name=char_name,
                quantity=None if job is None else job["copies"],
                # The copied BPC runs (one per invention attempt); None when
                # the attempts are unknown.
                runs=runs,
                estimated_cost_isk=None,
                estimated_profit_isk=None,
                estimated_completion=None,
                notes="; ".join([
                    "Copy T1 BPO to feed invention pipeline",
                    *([f"BPO {index + 1} of {len(pilots)}"] if len(pilots) > 1 else []),
                    *shape,
                    note,
                ]),
                **_job_fee(
                    _times_runs(orow.get_source_copy_fee_per_run(row), runs),
                    _rate_fee_reason(
                        row, "source_copy_job_cost_per_run", runs,
                        "the copy runs are unknown (the invention attempts are unknown)",
                    ),
                    "copy", type_id,
                ),
            ))
        return actions

    @staticmethod
    def _invention_lanes(char_slots: dict[int, dict], row: dict[str, Any]) -> list[int]:
        """One entry (the pilot's id) per FREE research slot of every pilot
        that can run this invention: its loaded skills reach every required
        level (the producer's required_skill_levels, else level 1 of each
        skill in the set). The producer counts the same pilots' total slots
        (invention_model.invention_lanes). A row without a skill set: none."""
        required = CharacterAssigner._required_levels(row)
        if required is None:
            return []
        lanes: list[int] = []
        for cid in sorted(char_slots):
            slot = char_slots[cid]
            levels = slot.get("skills_by_id")
            if levels is None or int(slot.get("free_research", 0)) <= 0:
                continue
            if can_run_invention(required, levels):
                lanes.extend([cid] * int(slot["free_research"]))
        return lanes

    @staticmethod
    def _invent_action(
        row: dict[str, Any], type_id: int, type_name: str, char_id: int, char_name: str | None,
        invention: Any, decryptor: tuple[int, str] | None,
    ) -> AssignedAction:
        """One invent action: its pilot's chance, whole attempts (runs, read
        by the shopping list; None buys nothing) and cost (A4), and the fee of
        the attempts it launches (Capital Reserved)."""
        return AssignedAction(
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
            **_job_fee(
                _times_runs(orow.get_invention_fee_per_attempt(row), invention.whole_attempts),
                _rate_fee_reason(
                    row, "job_cost_per_attempt", invention.whole_attempts,
                    "the invention attempts are unknown",
                ),
                "invent", type_id,
            ),
        )

    def _best_invention_char(
        self, char_slots: dict[int, dict], characters: list[dict], row: dict[str, Any] | None = None,
    ) -> tuple[int | None, str | None]:
        """The free pilot with the highest invention skill factor for this
        item: the producer's invention_chance skills (D2, the same rule as the
        chance it assumed). A row without them ranks by the generic
        SCIENCE_SKILLS sum (logged at INFO). Pilots excluded from industry
        have no slot (see _compute_available_slots). Ties break toward the
        fewest active jobs.

        Only a pilot who can run the invention (every required skill level,
        _required_levels) is ranked (Task 15 fix round 1, I1): an unqualified
        pilot's chance would size another pilot's attempts (an under-buy). A
        pilot whose skills are not loaded stays a candidate: A4 then buys at
        the SDE base chance with a reason (an over-buy). None: nobody free
        qualifies."""
        skills = orow.get_invention_skill_set(row or {})
        skill_set = InventionSkillSet(skills[0], skills[1]) if skills is not None else None
        required = self._required_levels(row or {}) if skill_set is not None else None
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
            loaded = slot.get("skills_by_id")
            if required is not None and loaded is not None and not can_run_invention(required, loaded):
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


_LANE_WORDING = re.compile(r"lane \d+ of \d+")

#: Task A: release-and-re-plan passes after the top-level rows (bounded:
#: each pass only re-plans rows that had no action yet).
MAX_REPLAN_PASSES = 3


def _unused_claims(
    top_level: list[ItemDecision], emitted: dict[int, list[AssignedAction]], bpc_pool: dict[str, Any],
) -> tuple[dict[int, list[int]], dict[int, int]]:
    """({row type_id: claimed blueprint item_ids no action of it uses},
    {item_id: row type_id whose action uses it}), from ChainPlanner's
    ledger_claims. Conservative: a manufacture keeps every BPO / BPC the
    row claimed to build (and its research BPO); research keeps its BPO; a
    copy or invention claim is used only by the copy / invent action that
    names that blueprint."""
    used_by: dict[int, int] = {}
    copy_used: dict[int, list[Any]] = bpc_pool.get("copy_used") or {}
    for decision in top_level:
        for action in emitted.get(id(decision)) or []:
            if action.action_type == "invent" and action.blueprint_item_id is not None:
                used_by[int(action.blueprint_item_id)] = int(decision.type_id)
        for key in copy_used.get(int(decision.type_id)) or []:
            if isinstance(key, int) and not isinstance(key, bool):
                used_by[key] = int(decision.type_id)
    release: dict[int, list[int]] = {}
    for decision in top_level:
        claims = decision.overview_row.get("ledger_claims")
        if not isinstance(claims, dict):
            continue
        kinds = {a.action_type for a in emitted.get(id(decision)) or []}
        idle: list[int] = []
        if "manufacture" not in kinds:
            idle.extend(claims.get("manufacture") or [])
            if not kinds & {"me_research", "te_research"}:
                idle.extend(claims.get("research") or [])
        idle.extend(k for k in claims.get("copy") or [] if used_by.get(k) != int(decision.type_id))
        idle.extend(k for k in claims.get("invention") or [] if used_by.get(k) != int(decision.type_id))
        if idle:
            release[int(decision.type_id)] = idle
    return release, used_by


def _owner_by_item(decision: ItemDecision) -> dict[int, int]:
    """{blueprint item_id: owning pilot} of the row's owner-bound claims
    (ChainPlanner's blueprint_owner_by_item; Task A item 1)."""
    value = decision.overview_row.get("blueprint_owner_by_item")
    return {int(k): int(v) for k, v in value.items()} if isinstance(value, dict) else {}


def _claim_owners(decision: ItemDecision, owner_by_item: dict[int, int], *categories: str) -> list[int]:
    """The pilots owning the row's claims of these job kinds (sorted)."""
    claims = decision.overview_row.get("ledger_claims")
    if not isinstance(claims, dict) or not owner_by_item:
        return []
    return sorted({
        owner_by_item[item] for category in categories for item in claims.get(category) or []
        if item in owner_by_item
    })


def _pilot_names(characters: list[dict[str, Any]], ids: list[int]) -> str:
    """The pilots' names (their id when not listed), for a reason."""
    names = {
        int(c.get("character_id") or c.get("id") or 0): str(c.get("character_name") or c.get("name") or "")
        for c in characters
    }
    return ", ".join(names.get(int(i)) or f"character {i}" for i in ids)


def _component_buy_reasons(decision: ItemDecision, action: AssignedAction) -> None:
    """M8: ChainPlanner's "component X bought: ..." reasons, added to the
    persisted reason and the action's note only for a component this
    manufacture actually buys (it is in the action's materials)."""
    reasons = decision.overview_row.get("component_buy_reasons")
    if not isinstance(reasons, dict):
        return
    for type_id, reason in reasons.items():
        if not (action.materials or {}).get(int(type_id)):
            continue
        if reason not in (decision.decision_reason or ""):
            decision.decision_reason = f"{decision.decision_reason}; {reason}"
        if reason not in (action.notes or ""):
            action.notes = f"{action.notes}; {reason}"


def _reword_unused_research_claims(decisions: list[ItemDecision], actions: list[AssignedAction], fell_back: set[int]) -> None:
    """M10: a research claim whose research got no slot did not happen.
    When its row's fallback manufacture was emitted, reasons and notes that
    name the research claim (e.g. "used by TE research for Thing (12345)
    today") name that manufacture instead; when nothing was emitted (no
    manufacturing slot either, fix round 2), they say the BPO was kept for
    a research that got no slot."""
    for decision in decisions:
        if int(decision.type_id) not in fell_back or decision.is_sub_component:
            continue
        label = decision.overview_row.get("research_claim_label")
        if not isinstance(label, str) or not label:
            continue
        built = any(a.action_type == "manufacture" and int(a.type_id) == int(decision.type_id) for a in actions)
        # Fix round 3: reworded per holder, so a reason naming several jobs
        # ("used by copy for X (1), TE research for Thing (2) today") too.
        if built:
            rewords = [(label, f"manufacture of {decision.type_name} ({decision.type_id})")]
        else:
            rewords = [
                (f"used by {label} today", f"kept for {label}, which got no free slot today"),
                (label, f"{label} (kept for it, but it got no free slot)"),
            ]

        def _reword(text: str | None) -> str | None:
            if not text:
                return text
            for old, new in rewords:
                if old in text:
                    return text.replace(old, new)
            return text

        for other in decisions:
            if other is not decision:
                other.decision_reason = _reword(other.decision_reason)
        for action in actions:
            action.notes = _reword(action.notes)


def _count(n: int, one: str, many: str) -> str:
    """'1 copy', '2 copies'."""
    return f"{n} {one if n == 1 else many}"


def _no_manufacture_today(decision: ItemDecision, reason: str) -> None:
    """Minor 2 (Task 14): a build row that gets no manufacture today says why
    in its persisted decision_reason, once, and not on top of a "not built:"."""
    current = decision.decision_reason or ""
    if "not built:" in current or "no manufacture today:" in current:
        return
    decision.decision_reason = f"{current}; no manufacture today: {reason}"


def _released_placeholder(type_id: int) -> str:
    """A unique marker in a releasing row's reason, replaced once every row
    is assigned (Task 15 fix round 5)."""
    return f"\x00released-bpcs-{int(type_id)}\x00"


def _reword_released_bpcs(decisions: list[ItemDecision], bpc_pool: dict[str, Any]) -> None:
    """Task 15 fix rounds 4-5: a row that invents nothing today released its
    T1 BPCs. Its reason names the row that invents from each BPC ("go to
    <row>") or says it stays unused today. A row ChainPlanner marked not
    built because that row's invention held the BPCs is told that row does
    not invent today."""
    taken_by: dict[Any, ItemDecision] = bpc_pool.get("taken_by") or {}
    for releaser, bpcs, placeholder in bpc_pool.get("releases") or []:
        groups: dict[int, tuple[ItemDecision, list[str]]] = {}
        unused: list[str] = []
        for key, item, _runs in bpcs:
            name = str(item) if item is not None else "unnamed"
            taker = taken_by.get(key)
            if taker is None or taker is releaser:
                unused.append(name)
            else:
                groups.setdefault(id(taker), (taker, []))[1].append(name)
        parts = [
            f"its T1 BPC(s) {', '.join(names)} go to {taker.type_name} ({taker.type_id})"
            for taker, names in groups.values()
        ]
        if unused:
            parts.append(f"its T1 BPC(s) {', '.join(unused)} stay unused today")
        releaser.decision_reason = (releaser.decision_reason or "").replace(placeholder, "; ".join(parts))
        label = f"invention for {releaser.type_name} ({releaser.type_id})"
        marked = f"{label} (which ended up not inventing)"
        for decision in decisions:
            reason = decision.decision_reason or ""
            if decision is releaser or "not built:" not in reason or f"used by {label}" not in reason:
                continue
            if marked not in reason:
                decision.decision_reason = reason.replace(label, marked)
            first = decision.overview_row.get("research_first")
            if isinstance(first, dict) and isinstance(first.get("note"), str) and marked not in first["note"]:
                first["note"] = first["note"].replace(label, marked)


def _row_bpcs(
    row: dict[str, Any], t1_blueprint_type_id: int, used: set[Any], *, carries_bpcs: bool,
) -> list[tuple[Any, int | None, int]] | None:
    """The row's free T1 BPCs not yet given to a job, as (key, item_id,
    runs), most runs first. From ChainPlanner's t1_bpc_runs_by_item; a row
    with only the older total (t1_bpc_runs_on_hand) counts as ONE BPC of
    that many runs, shared by every row of the source (conservative: never
    more lanes than BPCs). None: the row carries no BPC data (not copying,
    not from ChainPlanner), so its invention is not capped."""
    entries: list[tuple[Any, int | None, int]] = []
    listed = row.get("t1_bpc_runs_by_item")
    if isinstance(listed, list):
        for index, entry in enumerate(listed):
            try:
                item, runs = entry
                runs = int(runs or 0)
                item = int(item) if item is not None else None
            except (TypeError, ValueError):
                continue
            key = item if item is not None else ("bpc", t1_blueprint_type_id, int(row.get("type_id") or 0), index)
            entries.append((key, item, runs))
    elif carries_bpcs:
        total = max(0, int(row.get("t1_bpc_runs_on_hand") or 0))
        entries.append((("bpc-total", t1_blueprint_type_id), None, total))
    else:
        return None
    return sorted(
        (e for e in entries if e[2] > 0 and e[0] not in used), key=lambda e: (-e[2], str(e[0])),
    )


def _row_copy_bpos(row: dict[str, Any], t1_blueprint_type_id: int, used: set[Any]) -> list[Any]:
    """The T1 BPOs the row may copy that no other row's copy took: the
    items ChainPlanner claimed for it (t1_bpo_item_ids), else t1_bpo_count
    BPOs of the source shared by every row of that source."""
    items = row.get("t1_bpo_item_ids")
    if isinstance(items, list) and items and all(isinstance(i, int) and not isinstance(i, bool) for i in items):
        keys: list[Any] = list(items)
    else:
        keys = [("bpo", t1_blueprint_type_id, n) for n in range(max(1, int(row.get("t1_bpo_count") or 1)))]
    return [k for k in keys if k not in used]


def _max_runs_per_copy(row: dict[str, Any], type_id: int) -> int | None:
    """The SDE maxProductionLimit of the T1 source (ChainPlanner's
    t1_max_runs_per_copy), or None with a WARNING: the copies are then not
    capped, which may exceed what EVE allows per copy."""
    value = row.get("t1_max_runs_per_copy")
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    logger.warning(
        "CharacterAssigner: type_id=%s: no SDE max runs per copy for its T1 source (%r); "
        "runs per copy are not capped",
        type_id, value,
    )
    return None


#: EVE refuses a job of more than one run that would take longer than 30
#: days (community-verified, see max_copies_per_job).
MAX_JOB_SECONDS = 30 * 24 * 3600


def max_copies_per_job(runs_per_copy: int, copy_seconds_per_run: int | None, max_runs_per_copy: int | None) -> int | None:
    """The most copies one copy job may make (Task A item 5).

    EVE's rule, verified as far as the sources go:
    - runs per copy: at most the "maximum production runs" of the
      blueprint original (CCP support, "Copying": "This is limited by the
      maximum number of production runs listed in the Bill of Materials");
      the SDE's blueprint maxProductionLimit. Enforced by the caller.
    - copies per job: no SDE field limits them (the SDE copying activity
      holds only its time). The job's duration is "the total number of
      licensed productions over all copies" x the copy time per run (CCP
      support), and EVE refuses a job of more than one run longer than 30
      days (EVE forums, "BPO Maximum runs", several players with a worked
      example; not an official CCP statement). So copies x runs per copy x
      copy time <= 30 days, at least one copy. The SDE base copy time is
      used: skills and structures only shorten it, so this never exceeds
      what EVE allows.
    - conservative: the earlier cap (copies <= maxProductionLimit) is kept
      on top, as it is unverified but never plans more than before.
    None: no limit known (both inputs unknown)."""
    caps: list[int] = []
    if max_runs_per_copy is not None:
        caps.append(max_runs_per_copy)
    if copy_seconds_per_run is not None and copy_seconds_per_run > 0 and runs_per_copy > 0:
        caps.append(max(1, MAX_JOB_SECONDS // (runs_per_copy * copy_seconds_per_run)))
    return min(caps) if caps else None


def _copy_seconds_per_run(row: dict[str, Any]) -> int | None:
    """The T1 source's SDE copy time per run (ChainPlanner's
    t1_copy_seconds_per_run), or None when unknown."""
    value = row.get("t1_copy_seconds_per_run")
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def _shape_copy_jobs(
    copy_runs: int, lane_sizes: list[int], job_count: int, max_runs_per_copy: int | None,
    copy_seconds_per_run: int | None = None,
) -> list[dict[str, int]]:
    """The copy jobs for `copy_runs` copied runs: one copy per invent lane
    (largest lanes first, the last cut to what is left), a lane above the
    max runs per copy split into copies of that size, and the copies spread
    round-robin over at most `job_count` BPOs. A job's copies all carry the
    same runs (EVE), so a job with several lanes is sized at its largest:
    conservative (a few spare BPC runs), never short of a lane. A job never
    makes more copies than max_copies_per_job either; what it cannot make
    is `short`."""
    if copy_runs <= 0:
        return []
    lanes: list[int] = []
    left = copy_runs
    for size in sorted((max(1, n) for n in lane_sizes), reverse=True):
        if left <= 0:
            break
        lanes.append(min(size, left))
        left -= lanes[-1]
    if left > 0:
        lanes.append(left)
    if max_runs_per_copy is not None:
        split: list[int] = []
        for size in lanes:
            full, rest = divmod(size, max_runs_per_copy)
            split.extend([max_runs_per_copy] * full + ([rest] if rest else []))
        lanes = sorted(split, reverse=True)
    groups = [lanes[i::job_count] for i in range(min(job_count, len(lanes)))]
    jobs: list[dict[str, int]] = []
    for group in groups:
        copies, short = len(group), 0
        cap = max_copies_per_job(max(group), copy_seconds_per_run, max_runs_per_copy)
        if cap is not None and copies > cap:
            copies, short = cap, copies - cap
        jobs.append({"copies": copies, "runs_per_copy": max(group), "short": short})
    return jobs


def _cap_lane(invention: Any, cap: int, wording: str) -> Any:
    """One invent lane limited to `cap` attempts (its BPC's runs), keeping
    its share of the expected attempts and cost. A lane of unknown size is
    said to run at most `cap`."""
    whole = invention.whole_attempts
    if whole is None:
        return dataclasses.replace(invention, note="; ".join(n for n in (
            invention.note, f"at most {cap} attempt(s): {wording}",
        ) if n))
    if whole <= cap:
        return invention
    share = cap / whole
    return dataclasses.replace(
        invention, whole_attempts=cap,
        expected_attempts=None if invention.expected_attempts is None else invention.expected_attempts * share,
        cost_isk=None if invention.cost_isk is None else invention.cost_isk * share,
        note="; ".join(n for n in (invention.note, f"capped at {wording}") if n),
    )


def _lanes_on_bpcs(
    results: list[tuple[int, Any]], bpcs: list[tuple[Any, int | None, int]],
    owner_by_item: dict[int, int] | None = None,
) -> list[tuple[int, Any, tuple[Any, int | None, int]]]:
    """Today's invent lanes, each on its own T1 BPC (I2): one invention job
    per BPC, of at most its runs. Largest lane first, each takes the
    smallest free BPC with enough runs (best fit, M7), else the largest
    left (and is capped at its runs); a lane of unknown size takes the
    largest. Lanes beyond the BPCs are dropped. Kept lanes are renumbered
    "lane k of n" when some were dropped."""
    ordered = sorted(results, key=lambda r: (r[1].whole_attempts is None, -(r[1].whole_attempts or 0)))
    left = sorted(bpcs, key=lambda e: (-e[2], str(e[0])))
    out: list[tuple[int, Any, tuple[Any, int | None, int]]] = []
    owners = owner_by_item or {}

    def _owner(entry: tuple[Any, int | None, int]) -> int | None:
        return owners.get(entry[1]) if entry[1] is not None else None

    for char_id, invention in ordered:
        if not left:
            break
        # Task A item 1: a pilot's own BPC only for its owner, who uses
        # its own BPCs before the corp's.
        allowed = [e for e in left if _owner(e) in (None, char_id)]
        if not allowed:
            continue
        pool = [e for e in allowed if _owner(e) == char_id] or allowed
        whole = invention.whole_attempts
        fits = [e for e in pool if whole is not None and e[2] >= whole]
        bpc = min(fits, key=lambda e: (e[2], str(e[0]))) if fits else pool[0]
        left.remove(bpc)
        _key, item, runs = bpc
        wording = f"the {runs} run(s) of T1 BPC item {item}" if item is not None else f"the {runs} T1 BPC run(s) on hand"
        out.append((char_id, _cap_lane(invention, runs, wording), bpc))
    if len(out) < len(results):
        out = [
            (cid, dataclasses.replace(inv, note=_LANE_WORDING.sub(f"lane {index} of {len(out)}", inv.note)) if inv.note else inv, bpc)
            for index, (cid, inv, bpc) in enumerate(out, start=1)
        ]
    return out


def _fewer_lanes_note(row: dict[str, Any], free_lanes: int) -> str | None:
    """Said on the invent actions when today has fewer free lanes than the
    producer's Build time assumes (Task 12 fix round 2)."""
    costs = orow.get_invention_costs(row) or {}
    parallel = costs.get("elapsed_parallel")
    used = int(parallel.get("lanes_used") or 0) if isinstance(parallel, dict) and parallel.get("sequential_reason") is None else 0
    if used > 1 and free_lanes < used:
        return f"{free_lanes} of {used} lanes free today; completion later than the overview's Build time"
    return None


def _job_fee(fee: float | None, reason: str | None, action_type: str, type_id: int) -> dict[str, Any]:
    """The action's install fee fields. An unknown fee logs a WARNING
    (Capital Reserved then reads "unknown", never a partial sum) and keeps
    its cause; no cause (None) means the producer recorded none: an old
    product overview, which a refresh fixes."""
    if fee is not None:
        return {"estimated_job_fee_isk": fee, "estimated_job_fee_unknown_reason": None}
    logger.warning(
        "CharacterAssigner: install fee unknown for the %s action of type_id=%s (%s); "
        "Capital Reserved shows unknown",
        action_type, type_id,
        reason or "the producer published no priced fee for it; refresh the product overview",
    )
    return {"estimated_job_fee_isk": None, "estimated_job_fee_unknown_reason": reason}


def _rate_fee_reason(row: dict[str, Any], key: str, runs: int | None, runs_unknown: str) -> str | None:
    """Why a per-attempt / per-run fee x runs is unknown: the runs, else the
    producer's reason for the rate (None = none recorded)."""
    if runs is None or runs <= 0:
        return runs_unknown
    return orow.get_invention_fee_unknown_reason(row, key)


def _times_runs(fee_per_run: float | None, runs: int | None) -> float | None:
    """A per-run (per-attempt) fee x the runs, or None when either is unknown."""
    if fee_per_run is None or runs is None or runs <= 0:
        return None
    return fee_per_run * runs


def _research_levels(row: dict[str, Any], activity: str, current_key: str, target_key: str) -> tuple[int, int] | None:
    """(from, to) research level of the row's research job, or None when a
    level is unknown. ME is stored as the level (0..10), TE as EVE's
    percentage (see _te_research_levels)."""
    try:
        current, target = int(row[current_key]), int(row[target_key])
    except (KeyError, TypeError, ValueError):
        return None
    if activity == "research_time":
        return _te_research_levels(current, target)
    return current, target


def _format_duration(seconds: int) -> str:
    """'2d 1h 36m', '16h 48m', '5m' (whole minutes, rounded down)."""
    minutes = int(seconds) // 60
    days, minutes = divmod(minutes, 24 * 60)
    hours, minutes = divmod(minutes, 60)
    parts = [f"{days}d"] if days else []
    if days or hours:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return " ".join(parts)


def _pilot_skills(char_slots: dict[int, dict[str, Any]], char_id: int) -> dict[str, int] | None:
    """The assigned pilot's skill levels by name, or None when its skill
    list is not loaded (unknown, not level 0)."""
    slot = char_slots.get(char_id) or {}
    return slot.get("skills") if slot.get("skills_by_id") is not None else None


#: Which structure and implants a research duration assumes (the note).
_PRODUCER_STRUCTURE_AND_IMPLANTS = "structure and implants: the producer's"
_PILOT_IMPLANTS = "structure: the producer's; implants: the pilot's"
_NO_PILOT_IMPLANTS = "structure: the producer's; the pilot's implants are unknown, none applied"


def _research_seconds(
    row: dict[str, Any], activity: str, levels: tuple[int, int], pilot_skills: dict[str, int] | None,
    pilot_id: int | None = None,
) -> tuple[int | None, str | None, str]:
    """(seconds, basis note, structure/implants note) of the research job
    over `levels`.

    The one convention for another pilot (Task 12 fix round 1): the
    producer's skill- and implant-free duration x the assigned pilot's
    research-skill multiplier x its own implant multiplier (none applied
    when the producer published none for it: longer, never understated).
    A row without that table: the producer's unskilled duration (its own
    character's implants) x the pilot's skills; else the producer's figure
    (its own character's skills), said in the note; None when neither is
    published."""
    if pilot_skills is not None and pilot_id is not None:
        base = orow.get_research_duration_seconds(row, activity, *levels, base=True)
        if base is not None:
            implants = orow.get_research_implant_time_multiplier(row, pilot_id, activity)
            seconds = base * research_skill_time_multiplier(activity, pilot_skills) * (implants or 1.0)
            return math.ceil(seconds), None, (_PILOT_IMPLANTS if implants is not None else _NO_PILOT_IMPLANTS)
    unskilled = orow.get_research_duration_seconds(row, activity, *levels, unskilled=True)
    if pilot_skills is not None and unskilled is not None:
        return math.ceil(unskilled * research_skill_time_multiplier(activity, pilot_skills)), None, _PRODUCER_STRUCTURE_AND_IMPLANTS
    producer = orow.get_research_duration_seconds(row, activity, *levels)
    if producer is None:
        return None, None, _PRODUCER_STRUCTURE_AND_IMPLANTS
    why = "the pilot's are unknown" if pilot_skills is None else "no unskilled duration published"
    return producer, f"at the producer character's skills: {why}", _PRODUCER_STRUCTURE_AND_IMPLANTS


def _research_timing(
    row: dict[str, Any], activity: str, current_key: str, target_key: str, start: datetime | None,
    note: str, pilot_skills: dict[str, int] | None, *, after_me: bool = False, pilot_id: int | None = None,
) -> dict[str, Any]:
    """The research action's estimated_completion and notes: the duration of
    exactly the levels from the current to the target, at the assigned
    pilot's skills and implants (_research_seconds), from `start` (now, or
    the end of the ME job on the same BPO: one job per blueprint). Unknown
    is said in the note, with no completion."""
    levels = _research_levels(row, activity, current_key, target_key)
    seconds, basis, convention = (
        _research_seconds(row, activity, levels, pilot_skills, pilot_id) if levels is not None
        else (None, None, _PRODUCER_STRUCTURE_AND_IMPLANTS)
    )
    queued = "; starts after the ME research on this BPO" if after_me else ""
    no_slot = "; no research slot taken in this plan" if after_me else ""
    if seconds is None:
        return {"estimated_completion": None, "notes": f"{note}; duration unknown{queued}{no_slot}"}
    note = f"{note}; duration {_format_duration(seconds)}"
    if basis:
        note += f" ({basis})"
    note += f" ({convention}){queued}"
    if start is None:
        return {"estimated_completion": None, "notes": f"{note}, whose duration is unknown{no_slot}"}
    return {"estimated_completion": start + timedelta(seconds=seconds), "notes": note + no_slot}


def _te_research_levels(current_pct: int, target_pct: int) -> tuple[int, int]:
    """(from, to) research level of a TE job between two TE percentages
    (EVE: 2 % per level, 0..20): the level held (te // 2) and the lowest
    level that reaches the target percentage (ceil(te / 2))."""
    return current_pct // 2, -(-target_pct // 2)


def _te_research_note(row: dict[str, Any]) -> str:
    """'TE research: 10% → 20% (level 5 → 10)': TE is a percentage, the
    research job spans levels."""
    try:
        current, target = int(row["te_current"]), int(row["te_research_target"])
    except (KeyError, TypeError, ValueError):
        return f"TE research: {row.get('te_current', '?')} → {row.get('te_research_target', '?')}"
    from_level, to_level = _te_research_levels(current, target)
    return f"TE research: {current}% → {target}% (level {from_level} → {to_level})"


def _research_fee(row: dict[str, Any], activity: str, current_key: str, target_key: str) -> tuple[float | None, str | None]:
    """(fee, None), or (None, why), for the research job from the row's
    current level to its target (A2), via the producer's cumulative fee
    table. ME is stored as the level (0..10). TE is stored as EVE's
    percentage (2 % per level, 0..20): the current level is the one held
    (te // 2), the target the lowest level that reaches the target
    percentage (ceil(te / 2))."""
    try:
        current, target = int(row[current_key]), int(row[target_key])
    except (KeyError, TypeError, ValueError):
        return None, f"the blueprint's current or target {current_key.split('_')[0].upper()} level is unknown"
    if activity == "research_time":
        current, target = _te_research_levels(current, target)
    fee = orow.get_research_fee(row, activity, current, target)
    if fee is not None:
        return fee, None
    if not (0 <= current <= target <= 10):
        return None, f"the research levels {current} -> {target} are outside 0..10"
    return None, orow.get_research_fee_unknown_reason(row, activity)


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
