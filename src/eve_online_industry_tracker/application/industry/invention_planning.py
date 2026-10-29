"""IndustryService: copying, invention and research planning: lanes, nested T2, decryptors.

A mixin of IndustryService (service.py); it holds no state of its own."""

from __future__ import annotations

import logging
import math
import time
import dataclasses
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, Callable, cast

from eve_online_industry_tracker.application.industry.job_cost import (
    JobSurchargeRates,
    research_level_multiplier,
)
from eve_online_industry_tracker.application.industry.invention_model import (
    ALL_INVENTORS_EXCLUDED,
    DECRYPTOR_SELECTION_OFF_REASON,
    NO_CHARACTER_SKILLS,
    NO_DECRYPTOR,
    NO_DECRYPTOR_DATA_REASON,
    DecryptorChoice,
    DecryptorEvaluation,
    DecryptorOption,
    InventionContext,
    PARALLEL_CAPACITY_NOTE,
    ParallelLanes,
    SourceOriginals,
    elapsed_lane_note,
    required_invention_skill_levels,
    split_invention_attempts,
    decryptor_choice_payload,
    decryptor_options_from_rows,
    effective_invention_probability,
    invented_runs_per_blueprint,
    invention_chance_payload,
    invention_lanes,
    invention_probability,
    quiet_warnings,
    resolve_invention_chance,
    # Aliased: _expected_invention_costs has a local named invention_attempts.
    invention_attempts as expected_and_whole_invention_attempts,
    select_best_decryptor,
    source_copy_lanes,
)
from eve_online_industry_tracker.application.industry.character_slots import required_skill_levels
from eve_online_industry_tracker.infrastructure.sde.decryptors import get_t2_invention_decryptors

if TYPE_CHECKING:
    from eve_online_industry_tracker.application.industry.service import _BatchRunLayout, _ProductPlanningContext


# The service module's logger: it carries the QuietWarningsFilter (see service.py).
logger = logging.getLogger("eve_online_industry_tracker.application.industry.service")


def _segments_key(segments: list[dict[str, Any]]) -> tuple:
    return tuple(
        (int(s["runs"]), int(s["blueprint_material_efficiency"]), int(s["blueprint_time_efficiency"]), str(s["source"]))
        for s in segments
    )


def _payload_from_skill_levels(levels: Mapping[str, int]) -> dict[str, Any]:
    """An industry payload from a character's skill levels with no implants
    (its implants are unknown: the conservative, longer duration)."""
    return {
        "modifier_skills": [{"type_name": str(name), "trained_skill_level": int(level or 0)} for name, level in levels.items()],
        "implants": [],
    }


class InventionPlanningMixin:
    def _load_decryptor_options(self) -> tuple[DecryptorOption, ...]:
        """(NO_DECRYPTOR, every T2 invention decryptor in the SDE).

        Without SDE access, or with no decryptor rows: (NO_DECRYPTOR,) and a
        WARNING. Inventions are then planned without a decryptor, never with
        a guessed one.
        """
        if self._sessions is None:
            logger.warning(
                "Invention: no SDE session, so no decryptor data; inventions are planned without a decryptor"
            )
            return (NO_DECRYPTOR,)
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        session = self._sessions.sde_session()
        try:
            rows = get_t2_invention_decryptors(session, language=language)
        finally:
            session.close()
        options = decryptor_options_from_rows(rows)
        if len(options) == 1:
            logger.warning(
                "Invention: the SDE lists no T2 invention decryptors; inventions are planned without a decryptor"
            )
        return options

    @staticmethod
    def _elapsed_lane_notes(manufacturing_job: Mapping[str, Any]) -> list[str]:
        """The row's parallel-lane notes (invention_model.elapsed_lane_note),
        for its own invention and source copy and every nested one, deduped
        in order."""
        notes: list[str] = []

        def walk(node: Any, prefix: str) -> None:
            if not isinstance(node, Mapping):
                return
            activity = node.get("activity")
            if activity in ("invention", "copying") and "elapsed_parallel" in node:
                note = elapsed_lane_note(
                    node.get("elapsed_parallel"), label="invention" if activity == "invention" else "source copying",
                )
                if note:
                    notes.append(f"{prefix}{note}")
            for child in node.values():
                if isinstance(child, Mapping):
                    walk(child, prefix)

        breakdown = manufacturing_job.get("activity_breakdown")
        if isinstance(breakdown, Mapping):
            for key in ("invention", "invention_source_copying"):
                walk(breakdown.get(key), "")
        walk(manufacturing_job.get("recursive_activity_breakdown"), "sub-build ")
        return list(dict.fromkeys(notes))

    @staticmethod
    def _source_originals_by_owner(
        blueprint_original_assets_by_type_id: Mapping[int, list[Any]],
    ) -> dict[int, SourceOriginals]:
        """{blueprint type_id: owned originals per owning character, and the
        corp's}: only a BPO's owner installs it (source_copy_lanes). A
        character asset carries its character_id; a corp asset does not."""
        out: dict[int, SourceOriginals] = {}
        for bp_type_id, assets in blueprint_original_assets_by_type_id.items():
            by_character: dict[int, int] = {}
            corporation = 0
            for asset in assets or []:
                owner = getattr(asset, "character_id", None)
                try:
                    owner_id = int(owner) if owner is not None else 0
                except (TypeError, ValueError):
                    owner_id = 0
                if owner_id > 0:
                    by_character[owner_id] = by_character.get(owner_id, 0) + 1
                else:
                    corporation += 1
            out[int(bp_type_id)] = SourceOriginals(by_character, corporation)
        return out

    def _no_inventor_reason(self) -> str:
        """Why there is no inventor: skills are loaded only for characters
        excluded from industry, or for nobody."""
        excluded_with_skills = any(
            not char.get("industry_enabled", True) and self._loaded_skill_levels(char) is not None
            for char in self._corp_characters()
        )
        return ALL_INVENTORS_EXCLUDED if excluded_with_skills else NO_CHARACTER_SKILLS

    @staticmethod
    def _runs_per_invented_blueprint(
        target_entry: dict[str, Any] | None, *, max_production_limit: int, decryptor: DecryptorOption,
    ) -> int:
        """Runs on one invented BPC: the SDE's runs of the invention product
        `target_entry` (else max_production_limit; _expected_invention_costs
        warns about that once per blueprint) plus the decryptor's, at least 1.
        The one source for the run layout's BPC boundaries, the invention
        costs and the shared-BPC plan, and the decryptor cycle."""
        base_runs = int((target_entry or {}).get("quantity") or 0)
        if base_runs <= 0:
            base_runs = max(1, int(max_production_limit or 0) or 1)
        return invented_runs_per_blueprint(base_runs, decryptor)

    def _iter_product_variants(
        self, ctx: _ProductPlanningContext,
    ) -> list[tuple[int, int, dict[str, Any], dict[str, Any], list, Any, int]]:
        """Flatten the triple loop into a list of (row_idx, prod_idx, row, product, copy_assets, original_asset, effective_runs) (Step 6)."""
        results: list[tuple[int, int, dict[str, Any], dict[str, Any], list, Any, int]] = []
        for row_index, row in enumerate(ctx.blueprint_rows, start=1):
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue
            blueprint_type_id = int(row.get("blueprint_type_id") or 0)
            matched_copies = ctx.blueprint_copy_assets_by_type_id.get(blueprint_type_id) or []
            matched_originals = ctx.blueprint_original_assets_by_type_id.get(blueprint_type_id) or []
            raw_products = manufacturing_job.get("products") or []
            if not isinstance(raw_products, list) or not raw_products:
                continue
            for product_index, raw_product in enumerate(raw_products, start=1):
                if not isinstance(raw_product, dict):
                    continue
                if int(raw_product.get("type_id") or 0) <= 0:
                    continue
                max_production_limit = int(manufacturing_job.get("max_production_limit") or 0)
                # Variant selection
                if bool(ctx.build_from_bpc) and matched_copies:
                    if bool(ctx.group_identical_bpcs):
                        variants = [{"blueprint_copy_assets": list(matched_copies), "blueprint_original_asset": None}]
                    else:
                        variants = [{"blueprint_copy_assets": [c], "blueprint_original_asset": None} for c in matched_copies]
                elif matched_originals:
                    variants = [{"blueprint_copy_assets": [], "blueprint_original_asset": o} for o in matched_originals]
                else:
                    variants = [{"blueprint_copy_assets": [], "blueprint_original_asset": None}]
                if bool(ctx.have_blueprint_source_only):
                    variants = [v for v in variants if (v.get("blueprint_copy_assets") or []) or v.get("blueprint_original_asset") is not None]
                    if not variants:
                        continue
                for variant in variants:
                    copy_assets = cast(list, list(variant.get("blueprint_copy_assets") or []))
                    original_asset = variant.get("blueprint_original_asset")
                    copy_runs = sum(max(0, int(getattr(a, "blueprint_runs", 0) or 0)) for a in copy_assets)
                    effective_runs = 1
                    if bool(ctx.maximize_bp_runs):
                        if copy_runs > 0:
                            effective_runs = copy_runs
                        elif max_production_limit > 0:
                            effective_runs = max_production_limit
                    results.append((row_index, product_index, row, raw_product, copy_assets, original_asset, effective_runs))
        return results

    def _plan_copying_for_variant(
        self,
        ctx: _ProductPlanningContext,
        *,
        row: dict[str, Any],
        blueprint_type_id: int,
        per_run_material_eiv: float | None,
        missing_copy_runs: int,
        requires_copying_job: bool,
        owned_target_copy_runs_used: int,
        compact_product: dict[str, Any],
        has_invention_path: bool,
        matched_blueprint_originals: list,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """Plan the copying activity for a variant (Step 4)."""
        copying_job_data = row.get("copying_job") or {}
        base_copy_time = int((copying_job_data.get("time_seconds") or 0))
        if base_copy_time <= 0 or (has_invention_path and not matched_blueprint_originals):
            return None, []

        costs = self._plan_activity_costs(ctx, activity="copying", base_time_seconds=base_copy_time, runs=max(0, missing_copy_runs), process_value=self._copy_process_value(per_run_material_eiv, missing_copy_runs))
        bp_name = str(((row.get("blueprint") or {}) if isinstance(row.get("blueprint"), dict) else {}).get("type_name") or ((row.get("blueprint") or {}) if isinstance(row.get("blueprint"), dict) else {}).get("name") or blueprint_type_id or "Blueprint")
        costs["runs"] = max(0, missing_copy_runs)
        costs["estimated_item_value"] = per_run_material_eiv

        tree_nodes: list[dict[str, Any]] = []
        if requires_copying_job or owned_target_copy_runs_used <= 0:
            tree_nodes.append(self._job_tree_node(
                label=self._ACTIVITY_LABELS["copying"], node_type="activity", activity="copying",
                blueprint_name=bp_name, quantity=1, runs=max(0, missing_copy_runs),
                duration_seconds=costs["duration_seconds"], direct_duration_seconds=costs["duration_seconds"],
                job_cost=costs.get("total_job_cost"), total_job_cost=costs.get("total_job_cost"),
                category_name=str(compact_product.get("category_name") or "") or None,
                meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                recommendation_action="copy", children=[],
            ))
        return costs, tree_nodes

    _NO_INVENTION_PROBABILITY_REASON = "no invention success probability (SDE and admin floor are both 0)"

    @staticmethod
    def _invention_target_entry(invention_job: dict[str, Any], blueprint_type_id: int) -> dict[str, Any] | None:
        """The invention job's product entry for the invented blueprint, or None."""
        for product in invention_job.get("products") or []:
            if not isinstance(product, dict):
                continue
            inv_product = product.get("product") or {}
            if not isinstance(inv_product, dict):
                inv_product = {}
            if int(inv_product.get("type_id") or product.get("type_id") or 0) == int(blueprint_type_id):
                return product
        return None

    def _expected_invention_costs(
        self,
        *,
        compute_activity: Callable[..., dict[str, Any]],
        invention_job: dict[str, Any],
        target_entry: dict[str, Any] | None,
        blueprint_type_id: int,
        invented_runs: int,
        max_production_limit: int,
        adjusted_price_map: dict[int, dict[str, Any]],
        decryptor: DecryptorOption,
        invention_context: InventionContext,
        invented_blueprint_eiv: float | None,
        log_unknown_cost: bool = True,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Invention fees, time and inputs for `invented_runs` runs of an invented
        BPC, on the expected-value model. Shared by the top-level T2 row and the
        nested (sub-build) invention paths.

        BPCs needed = runs / runs_per_BPC, attempts = BPCs / probability. Inputs,
        fees and slot time scale with these fractional attempts, so a 1-run row
        carries 1/(p x runs_per_BPC) of an attempt. A separate elapsed time uses
        whole attempts: ceil(whole BPCs needed / probability) jobs in sequence.

        With no SDE probability and an admin floor of 0 the attempt count is
        unknown: never divides, logs a WARNING, and returns zero attempts with
        `cost_unknown_reason` set, total_job_cost and elapsed time None.

        `log_unknown_cost=False` logs that unknown-cost line at DEBUG: a caller
        that logs its own row-level WARNING for it (see
        _full_batch_invention_whole_attempts).

        `compute_activity(activity=, base_time_seconds=, runs=, process_value=)`
        prices one activity (the cached ctx calculator or the context-free one).

        `invented_blueprint_eiv` is the per-run EIV of the INVENTED blueprint's
        manufacturing materials (base quantities): EVE prices an attempt at
        invention_cost_eiv_multiplier (0.02) x that EIV (job_cost.py, Task 8b
        fit), not at the datacores' EIV. None = the fee is unknown.

        The success chance is EVE's: the SDE base chance x the corp's best
        inventor's skill factor x the decryptor multiplier, capped at 1, then
        the admin floor (resolve_invention_chance). Its breakdown is
        costs["invention_chance"] whenever something is invented.
        Returns (costs, invention_materials sized by the expected attempts).
        """
        base_probability = 0.0
        if isinstance(target_entry, dict):
            try:
                base_probability = float(target_entry.get("probability_pct") or 0.0) / 100.0
            except (TypeError, ValueError):
                base_probability = 0.0
        # EVE: base x (1 + (sci1 + sci2)/30 + enc/40) x decryptor, for the
        # corp's best inventor of this blueprint. Resolved only when
        # something is invented, so a row that invents nothing warns nothing.
        chance = (
            resolve_invention_chance(
                invention_context, blueprint_type_id=int(blueprint_type_id),
                skill_entries=[e for e in (invention_job.get("skill_entries") or []) if isinstance(e, dict)],
                base_probability=base_probability, decryptor=decryptor,
            )
            if invented_runs > 0 else None
        )
        probability = (
            chance.probability if chance is not None
            else invention_probability(base_probability, None, decryptor.probability_multiplier)
        )
        probability_floor = float(self._adm("industry", "invention_probability_floor"))
        runs_per_invented_blueprint = int((target_entry or {}).get("quantity") or 0)
        if invented_runs > 0:
            # Once per blueprint per refresh: batch 1, the later batches and
            # the D3 pre-check all evaluate the same blueprint's odds.
            if probability <= 0:
                invention_context.warn_once(
                    f"invention-no-sde-probability:{int(blueprint_type_id)}",
                    "Invention plan for blueprint %s: no SDE success probability; using the admin floor %s",
                    blueprint_type_id, probability_floor,
                )
            if runs_per_invented_blueprint <= 0:
                invention_context.warn_once(
                    f"invention-no-sde-runs:{int(blueprint_type_id)}",
                    "Invention plan for blueprint %s: no SDE runs per invented BPC; falling back to max_production_limit %s",
                    blueprint_type_id, max(1, max_production_limit or 1),
                )
        # A decryptor adds runs to every invented BPC (EVE).
        runs_per_invented_blueprint = self._runs_per_invented_blueprint(
            target_entry, max_production_limit=max_production_limit, decryptor=decryptor,
        )
        effective_probability = effective_invention_probability(probability, probability_floor)
        cost_unknown_reason: str | None = None
        if invented_runs > 0 and effective_probability <= 0:
            cost_unknown_reason = self._NO_INVENTION_PROBABILITY_REASON
            (logger.warning if log_unknown_cost else logger.debug)(
                "Invention plan for blueprint %s: %s; invention cost left unknown",
                blueprint_type_id, cost_unknown_reason,
            )
        if invented_runs > 0 and cost_unknown_reason is None:
            successful_jobs = float(invented_runs) / float(runs_per_invented_blueprint)
            # Shared with the daily planner's assigned-pilot attempts.
            invention_attempts, whole_attempts = expected_and_whole_invention_attempts(
                invented_runs, runs_per_invented_blueprint, effective_probability,
            )
        else:
            successful_jobs = 0.0
            invention_attempts = 0.0
            whole_attempts = 0
        # The corp's best inventor alone (the daily planner's single
        # invent action keeps these when its pilot is that inventor).
        best_inventor_expected, best_inventor_whole = invention_attempts, whole_attempts
        # Parallel lanes: every qualifying corp inventor's research
        # slots, each at its own chance, sized by split_invention_attempts.
        # When they run in parallel the row's whole and expected
        # attempts ARE the lanes', so the cost, the procurement lines, the
        # whole-run copy budget and the elapsed time all assume one plan.
        # Unknown slots or skills: the best inventor in sequence, as before.
        lanes = (
            invention_lanes(
                invention_context, chance, invention_job.get("skill_entries") or [],
                probability_floor=probability_floor,
            )
            if whole_attempts > 0 else None
        )
        lane_attempts = (
            split_invention_attempts(
                int(math.ceil(float(invented_runs) / float(runs_per_invented_blueprint))), lanes.probabilities,
            )
            if lanes is not None and lanes.sequential_reason is None else None
        )
        if lane_attempts and sum(lane_attempts) > 0:
            covered = sum(a * p for a, p in zip(lane_attempts, lanes.probabilities))  # type: ignore[union-attr]
            whole_attempts = int(sum(lane_attempts))
            # Each lane's share of the expected BPCs over its own chance.
            invention_attempts = successful_jobs * float(whole_attempts) / covered

        per_attempt_materials = [dict(e) for e in (invention_job.get("materials") or []) if isinstance(e, dict)]
        # The decryptor is consumed like a datacore, one per attempt, and priced
        # like one (take-or-buy against the hub price). It is not part of the
        # fee's job value.
        per_attempt_inputs = [*per_attempt_materials, *self._decryptor_input_entries(decryptor)]
        invention_materials = [{**e, "quantity_per_attempt": int(e.get("quantity") or 0), "quantity": float(int(e.get("quantity") or 0)) * invention_attempts} for e in per_attempt_inputs]
        # Job value per attempt: 0.02 x the invented blueprint's per-run
        # manufacturing EIV (EVE), times the attempts.
        per_attempt_process_value = (
            float(invented_blueprint_eiv) * float(self._adm("industry", "invention_cost_eiv_multiplier"))
            if invented_blueprint_eiv is not None else None
        )
        invention_process_value = float(per_attempt_process_value) * invention_attempts if per_attempt_process_value is not None else None

        invention_base_time = int(invention_job.get("time_seconds") or 0)
        costs = compute_activity(activity="invention", base_time_seconds=invention_base_time, runs=cast(int, invention_attempts), process_value=invention_process_value)
        costs["runs"] = invention_attempts
        costs["estimated_item_value"] = invention_process_value
        costs["success_probability"] = effective_probability
        if chance is not None:
            costs["invention_chance"] = invention_chance_payload(chance, effective_probability)
            if chance.skill_set is not None:
                # The levels a lane pilot needs: the
                # daily planner counts the same qualifying inventors.
                costs["invention_chance"]["required_skill_levels"] = {
                    str(type_id): level for type_id, level in required_invention_skill_levels(
                        chance.skill_set, invention_job.get("skill_entries") or [],
                    ).items()
                }
        costs["runs_per_invented_blueprint"] = runs_per_invented_blueprint
        costs["invented_runs"] = invented_runs
        costs["expected_successful_blueprints"] = successful_jobs
        # One attempt's duration (per-job), and the whole-job elapsed time.
        costs["job_duration_seconds"] = compute_activity(activity="invention", base_time_seconds=invention_base_time, runs=1, process_value=None)["duration_seconds"]
        # The install fee of ONE attempt (the same EIV x index x reductions +
        # surcharge as above, at one attempt): the daily planner pays it per
        # whole attempt it launches (Capital Reserved). None = unpriced.
        per_attempt_fee = compute_activity(
            activity="invention", base_time_seconds=invention_base_time, runs=1,
            process_value=per_attempt_process_value,
        )
        costs["job_cost_per_attempt"] = per_attempt_fee.get("total_job_cost")
        # Why an unknown fee is unknown: the planner names it.
        costs["job_cost_per_attempt_unknown_reason"] = (
            (per_attempt_fee.get("fee_unknown_reason") or "the invention fee could not be priced")
            if costs["job_cost_per_attempt"] is None else None
        )
        costs["whole_attempts"] = whole_attempts
        costs["best_inventor_expected_attempts"] = best_inventor_expected
        costs["best_inventor_whole_attempts"] = best_inventor_whole
        costs["elapsed_duration_seconds"], costs["elapsed_parallel"] = self._lane_elapsed_seconds(
            compute_activity=compute_activity, invention_context=invention_context, lanes=lanes,
            jobs_per_lane=lane_attempts, activity="invention", base_time_seconds=invention_base_time,
            whole_jobs=whole_attempts,
        )
        costs["duration_is_amortized"] = invention_attempts > 0
        if cost_unknown_reason is not None:
            costs["cost_unknown_reason"] = cost_unknown_reason
            costs["total_job_cost"] = None
            costs["elapsed_duration_seconds"] = None
            costs["elapsed_parallel"] = None
        if invention_attempts <= 0:
            costs["duration_seconds"] = 0
        return costs, invention_materials

    def _invention_source_copy_costs(
        self,
        *,
        compute_activity: Callable[..., dict[str, Any]],
        invention_source_row: dict[str, Any],
        invention_costs: dict[str, Any],
        available_blueprint_copy_runs_by_type_id: dict[int, Any],
        adjusted_price_map: dict[int, dict[str, Any]],
        enabled: bool,
        invention_context: InventionContext | None = None,
    ) -> tuple[dict[str, Any] | None, float]:
        """Copying the T1 source blueprint for the invention attempts in
        `invention_costs` (from _expected_invention_costs): owned source-copy
        runs first, then fractional copy runs for the remaining expected
        attempts, plus a whole-run elapsed time. Consumes the owned runs from
        `available_blueprint_copy_runs_by_type_id`. Returns (costs or None when
        not `enabled` or the source has no copy time, missing source runs).

        The whole copy runs (one per lane attempt) spread over the parallel
        copy lanes of `invention_context` (source_copy_lanes: per
        owner, min(its BPOs, its slots)); without it, or with one lane, they
        run in sequence."""
        attempts = float(invention_costs.get("runs") or 0.0)
        whole_attempts = int(invention_costs.get("whole_attempts") or 0)
        cost_unknown = invention_costs.get("cost_unknown_reason") is not None
        source_copying_job = invention_source_row.get("copying_job") or {}
        source_bp_type_id = int(invention_source_row.get("blueprint_type_id") or 0)
        # Expected-value model: the runs are fractional, read and kept as such
        # (int() here read a 0.7-run remainder as 0, FU-50).
        available_source_runs = max(0.0, float(available_blueprint_copy_runs_by_type_id.get(source_bp_type_id, 0) or 0))
        source_runs_used = min(available_source_runs, attempts)
        available_blueprint_copy_runs_by_type_id[source_bp_type_id] = available_source_runs - source_runs_used
        missing_source_runs = max(0.0, attempts - source_runs_used)
        # The whole-run elapsed time has its own budget of whole owned runs,
        # spent by whole attempts: the floor of the expected-value remainder
        # (2 - 0.5 - 0.5 = 1) undercounts after a second invention. It lives
        # in the same map under a negative key, so trial copies and commits
        # carry it, and it never exceeds the whole runs physically left.
        whole_source_runs_left = int(math.floor(available_source_runs + 1e-9))
        if source_bp_type_id > 0:
            whole_key = self._whole_source_runs_key(source_bp_type_id)
            whole_source_runs_left = min(
                int(available_blueprint_copy_runs_by_type_id.get(whole_key, whole_source_runs_left)), whole_source_runs_left,
            )
            available_blueprint_copy_runs_by_type_id[whole_key] = max(0, whole_source_runs_left - whole_attempts)
        whole_missing_source_runs = max(0, whole_attempts - whole_source_runs_left)
        if not (enabled and isinstance(source_copying_job, dict) and int(source_copying_job.get("time_seconds") or 0) > 0):
            return None, missing_source_runs

        source_mfg_materials = [dict(e) for e in ((invention_source_row.get("manufacturing_job") or {}).get("materials") or []) if isinstance(e, dict)]
        source_pv, _ = self._sum_estimated_item_value(source_mfg_materials, quantity_key="quantity", adjusted_price_map=adjusted_price_map)
        source_base_time = int(source_copying_job.get("time_seconds") or 0)
        source_copy_costs = compute_activity(
            activity="copying", base_time_seconds=source_base_time,
            runs=max(0, missing_source_runs),
            process_value=self._copy_process_value(source_pv, missing_source_runs),
        )
        source_copy_costs["job_duration_seconds"] = compute_activity(activity="copying", base_time_seconds=source_base_time, runs=1, process_value=None)["duration_seconds"]
        # The install fee of ONE source-copy run: the planner's copy
        # action copies one run per whole invention attempt.
        source_copy_costs["job_cost_per_run"], source_copy_costs["job_cost_per_run_unknown_reason"] = self._source_copy_fee_per_run(
            compute_activity=compute_activity, invention_source_row=invention_source_row,
            adjusted_price_map=adjusted_price_map,
        )
        # One source run per whole attempt: the lanes' total when they run
        # in parallel (whole_attempts is then the lanes'), so
        # the whole-run budget and the copy elapsed time agree.
        elapsed_missing_runs = whole_missing_source_runs
        copy_lanes = (
            source_copy_lanes(invention_context, source_bp_type_id)
            if invention_context is not None else ParallelLanes((), (), "no invention context")
        ) if elapsed_missing_runs > 0 else None
        copy_jobs = (
            tuple(
                len(range(index, elapsed_missing_runs, copy_lanes.lanes)) for index in range(copy_lanes.lanes)
            )
            if copy_lanes is not None and copy_lanes.sequential_reason is None else None
        )
        copy_elapsed, copy_parallel = self._lane_elapsed_seconds(
            compute_activity=compute_activity, invention_context=invention_context, lanes=copy_lanes,
            jobs_per_lane=copy_jobs, activity="copying", base_time_seconds=source_base_time,
            whole_jobs=elapsed_missing_runs,
        )
        source_copy_costs["elapsed_duration_seconds"] = copy_elapsed if not cost_unknown else None
        source_copy_costs["elapsed_parallel"] = copy_parallel if not cost_unknown else None
        source_copy_costs["duration_is_amortized"] = missing_source_runs > 0
        return source_copy_costs, missing_source_runs

    def _source_copy_fee_per_run(
        self,
        *,
        compute_activity: Callable[..., dict[str, Any]],
        invention_source_row: dict[str, Any],
        adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[float | None, str | None]:
        """(fee, None) for one copy run of the T1 source blueprint: the same
        process value as _invention_source_copy_costs (source EIV x
        copy_cost_eiv_multiplier) at one run. (None, why) when the source has
        no copy time, no priced EIV or another unknown input (unknown, not
        free)."""
        source_copying_job = invention_source_row.get("copying_job") or {}
        source_base_time = int((source_copying_job if isinstance(source_copying_job, dict) else {}).get("time_seconds") or 0)
        if source_base_time <= 0:
            return None, "the T1 source blueprint has no copy time in the SDE"
        source_mfg_materials = [dict(e) for e in ((invention_source_row.get("manufacturing_job") or {}).get("materials") or []) if isinstance(e, dict)]
        eiv_reasons: list[str] = []
        source_pv, _ = self._sum_estimated_item_value(source_mfg_materials, quantity_key="quantity", adjusted_price_map=adjusted_price_map, unknown_reasons=eiv_reasons)
        if source_pv is None:
            return None, f"no priced EIV for the source copy ({'; '.join(eiv_reasons) or 'a material has no adjusted price'})"
        fee = compute_activity(
            activity="copying", base_time_seconds=source_base_time, runs=1,
            process_value=self._copy_process_value(source_pv, 1),
        )
        total = fee.get("total_job_cost")
        return total, (None if total is not None else (fee.get("fee_unknown_reason") or "the copy fee could not be priced"))

    @staticmethod
    def _whole_source_runs_key(source_bp_type_id: int) -> int:
        """The BPC-run map key of a T1 source's whole-run budget (see
        _invention_source_copy_costs): the negated type id, never a real one."""
        return -int(source_bp_type_id)

    @staticmethod
    def _lane_elapsed_seconds(
        *,
        compute_activity: Callable[..., dict[str, Any]],
        invention_context: InventionContext | None,
        lanes: ParallelLanes | None,
        jobs_per_lane: tuple[int, ...] | None,
        activity: str,
        base_time_seconds: int,
        whole_jobs: int,
    ) -> tuple[int, dict[str, Any] | None]:
        """(elapsed seconds, lanes payload) for `activity` over `lanes`,
        `jobs_per_lane` aligned with lanes.character_ids.

        Parallel: the most jobs any lane runs x one job's duration of the
        SLOWEST pilot of a used lane (never understated, whichever pilot gets
        the last round). The selected character is timed with the producer's
        own calculator; every other pilot with its own industry payload
        (skills and implants). Sequential (no lanes, or a sequential_reason):
        `whole_jobs` in one slot, as before."""
        if whole_jobs <= 0 and not any(jobs_per_lane or ()):
            return 0, None
        sequential = lanes is None or lanes.sequential_reason is not None or invention_context is None or not jobs_per_lane
        if sequential:
            elapsed = int(compute_activity(
                activity=activity, base_time_seconds=base_time_seconds, runs=whole_jobs, process_value=None,
            )["duration_seconds"])
            return elapsed, {
                "lanes": 1, "lanes_used": 1, "pilots_used": 0, "whole_jobs": int(whole_jobs),
                "lane_whole_jobs": int(whole_jobs), "rounds": int(whole_jobs), "lane_duration_seconds": None,
                "sequential_reason": (lanes.sequential_reason if lanes is not None else None) or "sequential",
                "note": None,
            }
        assert lanes is not None and invention_context is not None and jobs_per_lane is not None
        slots = invention_context.slot_capacity_by_character_id or {}
        used = sorted({cid for cid, jobs in zip(lanes.character_ids, jobs_per_lane) if jobs > 0})
        durations: list[float] = []
        for character_id in used:
            kw: dict[str, Any] = {}
            if character_id != invention_context.selected_character_id:
                capacity = slots.get(character_id)
                kw["pilot_industry_payload"] = dict(
                    (capacity.industry_payload if capacity is not None else None)
                    or _payload_from_skill_levels(capacity.skill_levels_by_name if capacity is not None else {})
                )
            durations.append(float(compute_activity(
                activity=activity, base_time_seconds=base_time_seconds, runs=1, process_value=None, **kw,
            )["duration_seconds"]))
        lane_duration = max(durations) if durations else 0.0
        rounds = max(jobs_per_lane) if jobs_per_lane else 0
        return int(round(rounds * lane_duration)), {
            "lanes": lanes.lanes,
            "lanes_used": sum(1 for jobs in jobs_per_lane if jobs > 0),
            "pilots_used": len(used),
            "whole_jobs": int(whole_jobs),
            "lane_whole_jobs": int(sum(jobs_per_lane)),
            "rounds": int(rounds),
            "lane_duration_seconds": lane_duration,
            "sequential_reason": None,
            "note": PARALLEL_CAPACITY_NOTE,
        }

    @staticmethod
    def _invention_elapsed_adjustment(
        invention_costs: dict[str, Any], source_copy_costs: dict[str, Any] | None,
    ) -> float | None:
        """What to add to the amortized slot time to get the whole-job elapsed
        time for one invention chain: (elapsed - amortized) for the invention
        and for its source copy. None when the elapsed time is unknown."""
        if invention_costs.get("elapsed_duration_seconds") is None:
            return None
        adjustment = float(invention_costs["elapsed_duration_seconds"]) - float(invention_costs["duration_seconds"])
        if source_copy_costs is not None:
            if source_copy_costs.get("elapsed_duration_seconds") is None:
                return None
            source_copy_counted = source_copy_costs.get("duration_seconds", 0) > 0
            adjustment += float(source_copy_costs["elapsed_duration_seconds"]) - (
                float(source_copy_costs["duration_seconds"]) if source_copy_counted else 0.0
            )
        return adjustment

    def _plan_nested_invention(
        self,
        *,
        invention_source_row: dict[str, Any],
        invention_job: dict[str, Any],
        target_entry: dict[str, Any] | None,
        blueprint_type_id: int,
        blueprint_display_name: str,
        invented_runs: int,
        max_production_limit: int,
        selected_industry_profile: dict[str, Any] | None,
        selected_character_modifiers: dict[str, Any] | None,
        character_skill_levels_by_name: dict[str, int],
        surcharge_rates: JobSurchargeRates | None,
        adjusted_market_price_map: dict[int, dict[str, Any]],
        sell_price_map: dict[int, dict[str, Any]] | None,
        available_blueprint_copy_runs_by_type_id: dict[int, Any],
        available_owned_item_quantity_by_type_id: dict[int, Any],
        invention_valuation_stock: dict[int, Any],
        owned_item_unit_cost_by_type_id: dict[int, float] | None,
        source_copy_enabled: bool,
        decryptor: DecryptorOption,
        invention_context: InventionContext,
        invented_blueprint_eiv: float | None,
    ) -> dict[str, Any]:
        """Invention of a T2 blueprint inside the recursive (sub-build) plan, on
        the same expected-value model as the top-level T2 row: fractional
        expected attempts for fees, slot time and the (amortized) inputs, whole
        attempts for the elapsed time, and no division by a zero probability."""
        def compute_activity(**kw: Any) -> dict[str, Any]:
            # A parallel lane of another pilot: its own payload.
            pilot = kw.pop("pilot_industry_payload", None)
            return self._compute_activity_job(
                **kw,
                selected_industry_profile=selected_industry_profile,
                selected_character_modifiers=selected_character_modifiers if pilot is None else dict(pilot),
                character_skill_levels_by_name=(
                    character_skill_levels_by_name if pilot is None else self._skill_levels_by_name(dict(pilot))
                ),
                surcharge_rates=surcharge_rates,
            )

        invention_costs, invention_materials = self._expected_invention_costs(
            compute_activity=compute_activity, invention_job=invention_job,
            target_entry=target_entry, blueprint_type_id=blueprint_type_id,
            invented_runs=invented_runs, max_production_limit=max_production_limit,
            adjusted_price_map=adjusted_market_price_map,
            decryptor=decryptor, invention_context=invention_context,
            invented_blueprint_eiv=invented_blueprint_eiv,
        )
        # Valued against the row's one invention valuation stock (FU-63): the
        # top-level invention spends it first, then each nested invention in
        # turn. The caller evaluates a child on a copy and commits it only when
        # the child is built. The whole attempts are taken from the real stock
        # in invention_procurement_materials (_build_single_product_row).
        planned_materials, material_nodes = self._plan_take_or_buy_material_nodes(
            invention_materials,
            available_owned_item_quantity_by_type_id=invention_valuation_stock,
            owned_item_unit_cost_by_type_id=owned_item_unit_cost_by_type_id,
            sell_price_map=sell_price_map,
            adjusted_price_map=adjusted_market_price_map,
        )
        invention_tags = {"invented_blueprint_type_id": int(blueprint_type_id), "invented_blueprint_name": blueprint_display_name}
        expected_inputs = [
            {**entry, **invention_tags}
            for entry in self._expected_invention_inputs(planned_materials=planned_materials, invention_costs=invention_costs)
        ]
        invention_requirements = [
            {**entry, **invention_tags}
            for entry in self._invention_whole_attempt_requirements(
                invention_job, int(invention_costs["whole_attempts"]), decryptor=decryptor,
            )
        ]
        bpc_plan = self._nested_invention_bpc_plan(
            invention_job=invention_job, invention_costs=invention_costs,
            blueprint_type_id=blueprint_type_id, blueprint_display_name=blueprint_display_name,
            max_production_limit=max_production_limit, adjusted_price_map=adjusted_market_price_map,
            decryptor=decryptor, invention_context=invention_context,
        )
        source_copy_costs, missing_source_runs = self._invention_source_copy_costs(
            compute_activity=compute_activity, invention_source_row=invention_source_row,
            invention_costs=invention_costs,
            available_blueprint_copy_runs_by_type_id=available_blueprint_copy_runs_by_type_id,
            adjusted_price_map=adjusted_market_price_map, enabled=source_copy_enabled,
            invention_context=invention_context,
        )
        cost_unknown_reason = invention_costs.get("cost_unknown_reason")
        breakdown: dict[str, Any] = {
            "activity": "invention",
            "blueprint_type_id": blueprint_type_id,
            "attempts": invention_costs["runs"],
            "whole_attempts": invention_costs["whole_attempts"],
            "successful_jobs": invention_costs["expected_successful_blueprints"],
            "success_probability": invention_costs["success_probability"],
            "runs_per_invented_blueprint": invention_costs["runs_per_invented_blueprint"],
            "invented_runs": invention_costs["invented_runs"],
            "time_seconds": invention_costs["duration_seconds"],
            "elapsed_time_seconds": invention_costs["elapsed_duration_seconds"],
            "elapsed_parallel": invention_costs.get("elapsed_parallel"),
            "duration_is_amortized": invention_costs["duration_is_amortized"],
            "job_cost": invention_costs.get("total_job_cost"),
        }
        if cost_unknown_reason is not None:
            breakdown["cost_unknown_reason"] = cost_unknown_reason
        nested_chance = invention_costs.get("invention_chance")
        invention_chance_notes: list[str] = []
        if isinstance(nested_chance, dict):
            breakdown["invention_chance"] = nested_chance
            if nested_chance.get("skills_unknown_reason"):
                invention_chance_notes.append(
                    f"{blueprint_display_name} sub-build: uses the SDE base chance "
                    f"({nested_chance['skills_unknown_reason']}), so its invention cost is overstated"
                )
            if nested_chance.get("note"):
                invention_chance_notes.append(f"{blueprint_display_name} sub-build: {nested_chance['note']}")
        source_copy_breakdown = None
        if source_copy_costs is not None:
            source_copy_breakdown = {
                "activity": "copying",
                "blueprint_type_id": int(invention_source_row.get("blueprint_type_id") or 0),
                "attempts": missing_source_runs,
                "time_seconds": source_copy_costs["duration_seconds"],
                "elapsed_time_seconds": source_copy_costs["elapsed_duration_seconds"],
                "elapsed_parallel": source_copy_costs.get("elapsed_parallel"),
                "duration_is_amortized": source_copy_costs["duration_is_amortized"],
                "job_cost": source_copy_costs.get("total_job_cost"),
            }
        return {
            "invention_costs": invention_costs,
            "source_copy_costs": source_copy_costs,
            "missing_source_runs": missing_source_runs,
            "expected_invention_materials": expected_inputs,
            "invention_requirements": invention_requirements,
            "bpc_plan": bpc_plan,
            "material_nodes": material_nodes,
            "breakdown": breakdown,
            "source_copy_breakdown": source_copy_breakdown,
            "cost_unknown_reason": (
                f"{blueprint_display_name} sub-build: {cost_unknown_reason}" if cost_unknown_reason is not None else None
            ),
            "elapsed_time_adjustment": self._invention_elapsed_adjustment(invention_costs, source_copy_costs),
            "invention_chance_notes": invention_chance_notes,
        }

    def _nested_invention_bpc_plan(
        self,
        *,
        invention_job: dict[str, Any],
        invention_costs: dict[str, Any],
        blueprint_type_id: int,
        blueprint_display_name: str,
        max_production_limit: int,
        adjusted_price_map: dict[int, dict[str, Any]],
        decryptor: DecryptorOption,
        invention_context: InventionContext,
    ) -> dict[str, Any] | None:
        """One nested invention as the shopping list carries it across
        batches, on the top-level invention_bpc_plan's model: the runs
        invented, runs per invented BPC, whole attempts per BPC and the
        inputs of one attempt. None when nothing is invented or the odds are
        unknown (the row flags that need as unknown)."""
        invented_runs = int(invention_costs.get("invented_runs") or 0)
        if invented_runs <= 0 or int(invention_costs.get("whole_attempts") or 0) <= 0:
            return None
        runs_per_bpc = max(1, int(invention_costs.get("runs_per_invented_blueprint") or 1))
        attempts_per_bpc, unknown_reason = self._whole_attempts_per_blueprint(
            invention_job=invention_job, blueprint_type_id=blueprint_type_id, runs_per_bpc=runs_per_bpc,
            batch_runs=invented_runs, max_production_limit=max_production_limit,
            adjusted_price_map=adjusted_price_map, decryptor=decryptor, invention_context=invention_context,
        )
        if unknown_reason is not None or attempts_per_bpc <= 0:
            return None
        return {
            "invented_blueprint_type_id": int(blueprint_type_id),
            "invented_blueprint_name": blueprint_display_name,
            "invented_runs": invented_runs,
            # The whole attempts batch 1 buys for it.
            "whole_attempts": int(invention_costs.get("whole_attempts") or 0),
            "runs_per_invented_blueprint": runs_per_bpc,
            "whole_attempts_per_blueprint": int(attempts_per_bpc),
            "per_attempt_requirements": self._invention_whole_attempt_requirements(invention_job, 1, decryptor=decryptor),
        }

    @staticmethod
    def _decryptor_input_entries(decryptor: DecryptorOption) -> list[dict[str, Any]]:
        """The decryptor as an invention input, one per attempt; [] for none."""
        if decryptor.type_id is None:
            return []
        return [{
            "type_id": int(decryptor.type_id), "type_name": decryptor.type_name,
            "quantity": 1, "invention_decryptor": True,
        }]

    @classmethod
    def _invention_whole_attempt_requirements(
        cls, invention_job: dict[str, Any], whole_attempts: int, decryptor: DecryptorOption = NO_DECRYPTOR,
    ) -> list[dict[str, Any]]:
        """The invention inputs to actually buy or take: quantity_per_attempt x
        whole attempts, integer units (the amortized quantities are for cost
        only), the decryptor included. [] when the attempts are unknown or zero."""
        if whole_attempts <= 0:
            return []
        out: list[dict[str, Any]] = []
        for entry in [*(invention_job.get("materials") or []), *cls._decryptor_input_entries(decryptor)]:
            if not isinstance(entry, dict):
                continue
            per_attempt = int(entry.get("quantity") or 0)
            if int(entry.get("type_id") or 0) <= 0 or per_attempt <= 0:
                continue
            out.append({
                **dict(entry), "quantity": per_attempt * int(whole_attempts),
                "quantity_per_attempt": per_attempt, "whole_attempts": int(whole_attempts),
                "invention_input": True,
            })
        return out

    @classmethod
    def _build_total_cost_or_none(cls, child_plan: dict[str, Any]) -> float | None:
        """A sub-build's total cost for the build-vs-buy choice, or None (buy,
        _build_cost_unknown_reason says why) when its invention cost or any
        of its job fees is unknown: its cost would leave them out, a partial
        cost that favours building."""
        if cls._build_cost_unknown_reason(child_plan) is not None:
            return None
        return cls._as_float(child_plan.get("estimated_total_cost"))

    @classmethod
    def _build_cost_unknown_fields(cls, child_plan: dict[str, Any] | None, build_total_cost: float | None) -> dict[str, Any]:
        """{"build_cost_unknown_reason": ...} for a bought node whose sub-build
        was not costed, else {}."""
        if not child_plan or build_total_cost is not None:
            return {}
        reason = cls._build_cost_unknown_reason(child_plan)
        return {"build_cost_unknown_reason": reason} if reason is not None else {}

    @staticmethod
    def _build_cost_unknown_reason(child_plan: dict[str, Any]) -> str | None:
        """The bought sub-component's node reason when its build cost is
        unknown (an unknown invention cost or job fee), else None."""
        invention = [str(r) for r in (child_plan.get("invention_cost_unknown_reasons") or [])]
        if invention:
            return f"build cost unknown: invention cost unknown ({'; '.join(dict.fromkeys(invention))}); bought"
        fees = [str(r) for r in (child_plan.get("job_fee_unknown_reasons") or [])]
        if fees:
            return f"build cost unknown: job fees unknown for: {'; '.join(dict.fromkeys(fees))}; bought"
        return None

    @staticmethod
    def _commit_trial(target: dict[int, Any] | None, trial: dict[int, Any]) -> None:
        """Make a built child's evaluation real: the trial map replaces the
        live one in place (the caller's dict object stays shared)."""
        if target is None:
            return
        target.clear()
        target.update(trial)

    @staticmethod
    def _merge_invention_requirements(requirements: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Whole-attempt invention requirements merged per type_id (quantities
        summed), without the per-invention tags."""
        merged: dict[int, dict[str, Any]] = {}
        for entry in requirements:
            type_id = int(entry.get("type_id") or 0)
            if type_id in merged:
                merged[type_id]["quantity"] += int(entry.get("quantity") or 0)
            else:
                merged[type_id] = {
                    key: value for key, value in entry.items()
                    if key not in ("invented_blueprint_type_id", "invented_blueprint_name", "whole_attempts", "quantity_per_attempt")
                }
        return list(merged.values())

    def _whole_attempts_per_blueprint(
        self,
        *,
        invention_job: dict[str, Any],
        blueprint_type_id: int,
        runs_per_bpc: int,
        batch_runs: int,
        max_production_limit: int,
        adjusted_price_map: dict[int, dict[str, Any]],
        decryptor: DecryptorOption,
        invention_context: InventionContext,
    ) -> tuple[int, str | None]:
        """Whole attempts per invented BPC for the shopping list's later
        batches (BPCs x this): at least one BPC's attempts, and at least the
        parallel lanes' attempts for a full batch spread over its BPCs
        (weaker lanes need more per BPC as a batch grows, so one BPC's
        count alone could under-buy). (0, why) when the odds are unknown."""
        one, unknown_reason, _chance = self._full_batch_invention_whole_attempts(
            invention_job=invention_job, blueprint_type_id=blueprint_type_id, runs=runs_per_bpc,
            max_production_limit=max_production_limit, adjusted_price_map=adjusted_price_map,
            decryptor=decryptor, invention_context=invention_context,
        )
        if unknown_reason is not None or one <= 0:
            return one, unknown_reason
        bpcs = int(math.ceil(max(0, int(batch_runs)) / max(1, int(runs_per_bpc))))
        if bpcs <= 1:
            return one, None
        batch, batch_unknown, _chance = self._full_batch_invention_whole_attempts(
            invention_job=invention_job, blueprint_type_id=blueprint_type_id, runs=bpcs * int(runs_per_bpc),
            max_production_limit=max_production_limit, adjusted_price_map=adjusted_price_map,
            decryptor=decryptor, invention_context=invention_context,
        )
        if batch_unknown is not None:
            return one, None
        return max(one, int(math.ceil(batch / bpcs))), None

    def _full_batch_invention_whole_attempts(
        self,
        *,
        invention_job: dict[str, Any],
        blueprint_type_id: int,
        runs: int,
        max_production_limit: int,
        adjusted_price_map: dict[int, dict[str, Any]],
        decryptor: DecryptorOption,
        invention_context: InventionContext,
    ) -> tuple[int, str | None, dict[str, Any] | None]:
        """Whole invention attempts to invent all `runs` of one batch, on the
        same model as the costed invention, why they are unknown (0 attempts
        and a reason when the odds are unknown, never a guess), and the chance
        payload. Only the attempt count is used, so the activity is not
        priced."""
        costs, _ = self._expected_invention_costs(
            compute_activity=lambda **kw: {"duration_seconds": 0, "total_job_cost": None},
            invention_job=invention_job,
            target_entry=self._invention_target_entry(invention_job, int(blueprint_type_id)),
            blueprint_type_id=int(blueprint_type_id), invented_runs=max(0, int(runs)),
            max_production_limit=max_production_limit, adjusted_price_map=adjusted_price_map,
            decryptor=decryptor, invention_context=invention_context,
            # Only the attempt count is used: the fee is not priced.
            invented_blueprint_eiv=None,
            # The row logs one "datacore need unknown" WARNING for this instead.
            log_unknown_cost=False,
        )
        return int(costs.get("whole_attempts") or 0), costs.get("cost_unknown_reason"), costs.get("invention_chance")

    @staticmethod
    def _blueprint_copy_runs_spent(before: dict[int, Any], after: dict[int, Any]) -> bool:
        """Whether planning batch 1 used any owned BPC run (any type)."""
        return any(float(after.get(type_id, 0) or 0) < float(runs or 0) for type_id, runs in before.items())

    @staticmethod
    def _later_batch_schedule(
        layout: _BatchRunLayout,
        *,
        batch_runs: int,
        batch_one_segments: list[dict[str, Any]],
        nested_copy_runs_spent: bool,
        plan_for: Callable[[list[dict[str, Any]]], dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Batches 2..N of a row: {"plans", "lead", "cycle", "unknown_reasons"},
        or None when every later batch repeats batch 1 exactly.

        A later batch's own jobs come from `layout` (it may still run on owned
        BPC runs batch 1 left, or straddle two shared invented BPCs), and its
        sub-builds have no owned BPC run batch 1 spent. `plan_for(segments)`
        plans one such batch (_plan_extra_batch); each distinct job list is
        planned once. lead = [[plan index, batches]] for batches 2, 3, ... in
        order, then cycle = [plan index, ...] repeats for every batch after
        (read by shopping_list._later_batch_plan_counts). unknown_reasons
        collects every plan's, for the row's one WARNING.
        """
        lead_keys, cycle_keys = layout.later_batch_keys(batch_runs)
        batch_one_key = _segments_key(batch_one_segments)
        if (
            not nested_copy_runs_spent
            and all(key == batch_one_key for _batch, _count, key in lead_keys)
            and all(key == batch_one_key for key in cycle_keys)
        ):
            return None
        plans: list[dict[str, Any]] = []
        index_by_key: dict[tuple, int] = {}
        unknown_reasons: list[str] = []

        def plan_index(start_batch: int, key: tuple) -> int:
            if key not in index_by_key:
                plan = plan_for(layout.segments((start_batch - 1) * max(1, int(batch_runs)), batch_runs))
                unknown_reasons.extend(r for r in plan.pop("unknown_reasons") if r not in unknown_reasons)
                index_by_key[key] = len(plans)
                plans.append(plan)
            return index_by_key[key]

        lead = [[plan_index(batch, key), count] for batch, count, key in lead_keys]
        steady_batch = 2 + sum(count for _batch, count, _key in lead_keys)
        cycle = [plan_index(steady_batch + i, key) for i, key in enumerate(cycle_keys)]
        return {"plans": plans, "lead": lead, "cycle": cycle, "unknown_reasons": unknown_reasons}

    def _plan_extra_batch(
        self,
        ctx: _ProductPlanningContext,
        *,
        manufacturing_job: dict[str, Any],
        blueprint_type_id: int,
        job_segments: list[dict[str, Any]],
        manufacturing_group: str | Collection[str] | None,
        required_skill_entries: list[dict[str, Any]],
        own_invention_source: str | None,
        invention_job: dict[str, Any],
        decryptor: DecryptorOption,
        max_production_limit: int,
        sub_build_plan_cache: dict[tuple, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """One batch after batch 1, as the shopping list needs it.

        The row's own jobs are `job_segments` (from the row's _BatchRunLayout),
        each sized at its own ME/TE with one ceiling per job. Every sub-build
        is planned afresh without owned BPC runs, so it invents, copies from an
        owned BPO or is bought; a sub-build with unknown invention odds is
        bought, as in batch 1 (FU-60). Planned against no stock: pure
        requirements (take 0), because the shopping list nets owned stock once
        over all batches.

        `own_invention_source`: when set, the runs of the jobs with that source
        are invented for this batch alone (whole attempts), for a row whose
        own invention has no shared-BPC plan.

        Returns {"job_segments", "procurement_materials",
        "invention_procurement_materials" (own + nested),
        "invention_procurement_materials_nested", "unknown_reasons"}, the lists
        keyed by type_id. Called inside quiet_warnings(): the row logs one
        WARNING for any unknown later-batch need.

        `sub_build_plan_cache` (one per row): later batches whose jobs differ
        but whose material quantities match share one sub-build plan.
        """
        segments = [dict(segment) for segment in job_segments]
        sized = self._size_manufacturing_job_segments(
            ctx, manufacturing_job=manufacturing_job, job_segments=segments,
            manufacturing_group=manufacturing_group, required_skill_entries=required_skill_entries,
        )
        materials_key = tuple(
            (int(m.get("type_id") or 0), int(m.get("quantity") or 0)) for m in sized["adjusted_material_entries"]
        )
        cached_plan = sub_build_plan_cache.get(materials_key) if sub_build_plan_cache is not None else None
        plan = cached_plan if cached_plan is not None else self._build_recursive_prerequisite_plan(
            adjusted_material_entries=cast(list[dict[str, Any]], sized["adjusted_material_entries"]),
            blueprint_type_id=blueprint_type_id,
            build_from_bpc=ctx.build_from_bpc, include_reactions=ctx.include_reactions,
            selected_industry_profile=ctx.selected_industry_profile,
            selected_character_modifiers=ctx.selected_character_modifiers,
            character_skill_levels_by_name=ctx.character_skill_levels_by_name,
            adjusted_market_price_map=ctx.adjusted_market_price_map,
            sell_price_map=ctx.material_price_map,
            blueprint_copy_assets_by_type_id=ctx.blueprint_copy_assets_by_type_id,
            available_blueprint_copy_runs_by_type_id={},
            available_owned_item_quantity_by_type_id={},
            invention_valuation_stock={},
            owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
            blueprint_original_assets_by_type_id=ctx.blueprint_original_assets_by_type_id,
            character_name_by_id=ctx.character_name_by_id, corporation_name_by_id=ctx.corporation_name_by_id,
            manufacturing_row_by_product_type_id=ctx.manufacturing_row_by_product_type_id,
            reaction_row_by_product_type_id=ctx.reaction_row_by_product_type_id,
            invention_row_by_blueprint_type_id=ctx.invention_row_by_blueprint_type_id,
            invention_context=ctx.invention_context,
        )
        if sub_build_plan_cache is not None:
            sub_build_plan_cache[materials_key] = plan
        nested_requirements = [dict(e) for e in (plan.get("invention_requirements") or []) if isinstance(e, dict)]
        unknown_reasons = [
            f"batches 2+: {reason}" for reason in dict.fromkeys(str(r) for r in (plan.get("invention_cost_unknown_reasons") or []))
        ]
        own_requirements: list[dict[str, Any]] = []
        invented_runs = sum(int(s["runs"]) for s in segments if own_invention_source and s["source"] == own_invention_source)
        if invented_runs > 0:
            attempts, _unknown_reason, _chance = self._full_batch_invention_whole_attempts(
                invention_job=invention_job, blueprint_type_id=blueprint_type_id, runs=invented_runs,
                max_production_limit=max_production_limit, adjusted_price_map=ctx.adjusted_market_price_map,
                decryptor=decryptor, invention_context=ctx.invention_context,
            )
            # Unknown odds give 0 attempts: the row already flags its own
            # later-batch datacore need as unknown, never a guessed count.
            own_requirements = self._invention_whole_attempt_requirements(invention_job, attempts, decryptor=decryptor)

        def keyed_requirements(requirements: list[dict[str, Any]]) -> dict[str, Any]:
            lines, _ = self._plan_take_or_buy_material_nodes(
                self._merge_invention_requirements(requirements),
                available_owned_item_quantity_by_type_id={},
                owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
                sell_price_map=ctx.material_price_map,
                adjusted_price_map=ctx.adjusted_market_price_map,
            )
            return self._overview_keyed_entries(lines, compactor=lambda e: e)

        return {
            "job_segments": segments,
            "procurement_materials": self._overview_keyed_entries(
                list(plan.get("procurement_materials") or []), compactor=lambda e: e,
            ),
            "invention_procurement_materials": keyed_requirements([*own_requirements, *nested_requirements]),
            "invention_procurement_materials_nested": keyed_requirements(nested_requirements),
            # The nested inventions, carried across batches by the shopping list.
            "nested_invention_bpc_plans": self._compact_nested_invention_bpc_plans(
                ctx, plan.get("nested_invention_bpc_plans"),
            ),
            "unknown_reasons": unknown_reasons,
        }

    def _compact_nested_invention_bpc_plans(
        self, ctx: _ProductPlanningContext, plans: Any,
    ) -> list[dict[str, Any]]:
        """The nested inventions' BPC plans as published on the row:
        each one's inputs of one attempt priced like the top-level
        invention_bpc_plan's per_attempt_materials (take nothing)."""
        out: list[dict[str, Any]] = []
        for plan in plans or []:
            if not isinstance(plan, dict):
                continue
            lines, _ = self._plan_take_or_buy_material_nodes(
                [dict(e) for e in plan.get("per_attempt_requirements") or [] if isinstance(e, dict)],
                available_owned_item_quantity_by_type_id={},
                owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
                sell_price_map=ctx.material_price_map,
                adjusted_price_map=ctx.adjusted_market_price_map,
            )
            out.append({
                **{k: v for k, v in plan.items() if k != "per_attempt_requirements"},
                "per_attempt_materials": self._overview_keyed_entries(lines, compactor=lambda e: e),
            })
        return out

    @staticmethod
    def _absorb_nested_invention(
        child_plan: dict[str, Any],
        *,
        expected_invention_materials: list[dict[str, Any]],
        invention_requirements: list[dict[str, Any]],
        invention_cost_unknown_reasons: list[str],
        invention_chance_notes: list[str],
        elapsed_time_adjustment: float | None,
        nested_invention_bpc_plans: list[dict[str, Any]] | None = None,
    ) -> float | None:
        """Carry a built child plan's nested-invention results into its parent:
        extends the lists in place and returns the combined elapsed-time
        adjustment (None once any part is unknown). Only built children are
        absorbed, so a sub-build bought in batch 1 adds nothing to any list."""
        expected_invention_materials.extend(
            dict(e) for e in (child_plan.get("expected_invention_materials") or []) if isinstance(e, dict)
        )
        invention_requirements.extend(
            dict(e) for e in (child_plan.get("invention_requirements") or []) if isinstance(e, dict)
        )
        if nested_invention_bpc_plans is not None:
            nested_invention_bpc_plans.extend(
                dict(e) for e in (child_plan.get("nested_invention_bpc_plans") or []) if isinstance(e, dict)
            )
        invention_cost_unknown_reasons.extend(str(r) for r in (child_plan.get("invention_cost_unknown_reasons") or []))
        invention_chance_notes.extend(str(r) for r in (child_plan.get("invention_chance_notes") or []))
        child_adjustment = child_plan.get("elapsed_time_adjustment", 0.0)
        if elapsed_time_adjustment is None or child_adjustment is None:
            return None
        return float(elapsed_time_adjustment) + float(child_adjustment)

    def _plan_invention_chain_for_variant(
        self,
        ctx: _ProductPlanningContext,
        *,
        row: dict[str, Any],
        blueprint_type_id: int,
        missing_top_level_copy_runs: int,
        requires_invention_chain: bool,
        owned_target_copy_runs_used: int,
        max_production_limit: int,
        compact_product: dict[str, Any],
        available_blueprint_copy_runs_by_type_id: dict[int, int],
        available_owned_item_quantity_by_type_id: dict[int, int],
        invention_valuation_stock: dict[int, Any],
        decryptor: DecryptorOption,
        per_run_material_eiv: float | None,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]], dict[str, Any] | None]:
        """Plan the invention chain for a variant (Step 4). Returns (invention_costs, tree_nodes, procurement_materials, source_copy_costs).
        `per_run_material_eiv` is this (invented) blueprint's per-run
        manufacturing EIV: the invention fee's basis."""
        invention_source_row = ctx.invention_row_by_blueprint_type_id.get(int(blueprint_type_id))
        invention_job = (invention_source_row or {}).get("invention_job")
        # The shared invention predicate: the source row really has an
        # invention job dict (a None job is no invention path, not {}).
        if not (invention_source_row and isinstance(invention_job, dict)):
            return None, [], [], None

        invention_bp_payload = invention_source_row.get("blueprint") or {}
        if not isinstance(invention_bp_payload, dict):
            invention_bp_payload = {}
        invention_bp_name = str(invention_bp_payload.get("type_name") or invention_bp_payload.get("name") or invention_source_row.get("blueprint_type_id") or "Blueprint")

        target_entry = self._invention_target_entry(invention_job, blueprint_type_id)
        target_bp_name = str(((target_entry or {}).get("product") or {}).get("type_name") if isinstance((target_entry or {}).get("product"), dict) else "").strip() or str((row.get("blueprint") or {}).get("type_name") or blueprint_type_id)

        invented_runs = max(0, missing_top_level_copy_runs) if requires_invention_chain else 0
        costs, invention_materials = self._expected_invention_costs(
            compute_activity=lambda **kw: self._plan_activity_costs(ctx, **kw),
            invention_job=invention_job, target_entry=target_entry,
            blueprint_type_id=blueprint_type_id, invented_runs=invented_runs,
            max_production_limit=max_production_limit,
            adjusted_price_map=ctx.adjusted_market_price_map,
            decryptor=decryptor, invention_context=ctx.invention_context,
            invented_blueprint_eiv=per_run_material_eiv,
        )
        invention_attempts = float(costs["runs"])

        planned_inv_materials, inv_material_nodes = self._plan_take_or_buy_material_nodes(
            invention_materials,
            # Values the amortized inputs only, against the row's invention
            # valuation stock (spent here first, then by the nested inventions,
            # FU-63); the whole attempts are taken from the real stock in
            # invention_procurement_materials (integer units).
            available_owned_item_quantity_by_type_id=invention_valuation_stock,
            owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
            sell_price_map=ctx.material_price_map,
            adjusted_price_map=ctx.adjusted_market_price_map,
        )

        tree_nodes: list[dict[str, Any]] = []
        if requires_invention_chain or owned_target_copy_runs_used <= 0:
            tree_nodes.append(self._job_tree_node(
                label=self._ACTIVITY_LABELS["invention"], node_type="activity", activity="invention",
                blueprint_name=target_bp_name, blueprint_type_id=blueprint_type_id,
                runs=invention_attempts, duration_seconds=costs["duration_seconds"],
                direct_duration_seconds=costs["duration_seconds"],
                job_duration_seconds=costs["job_duration_seconds"],
                elapsed_duration_seconds=costs["elapsed_duration_seconds"],
                duration_is_amortized=costs["duration_is_amortized"],
                job_cost=costs.get("total_job_cost"), total_job_cost=costs.get("total_job_cost"),
                category_name=str(compact_product.get("category_name") or "") or None,
                meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                recommendation_action=(
                    "invent" if invention_attempts > 0
                    else ("unknown" if costs.get("cost_unknown_reason") else "take")
                ),
                **({"cost_unknown_reason": costs["cost_unknown_reason"]} if costs.get("cost_unknown_reason") else {}),
                children=([self._job_tree_node(label=self._ACTIVITY_LABELS["materials"], node_type="materials", activity="materials", children=inv_material_nodes)] if invention_attempts > 0 and inv_material_nodes else []),
            ))

        # Source blueprint copying for invention
        source_copy_costs, missing_source_runs = self._invention_source_copy_costs(
            compute_activity=lambda **kw: self._plan_activity_costs(ctx, **kw),
            invention_source_row=invention_source_row, invention_costs=costs,
            available_blueprint_copy_runs_by_type_id=available_blueprint_copy_runs_by_type_id,
            adjusted_price_map=ctx.adjusted_market_price_map,
            enabled=requires_invention_chain,
            invention_context=ctx.invention_context,
        )
        # The planner's copy action copies one source run per attempt.
        # Computed even when batch 1 copies nothing (an owned T2 BPC covers it):
        # the later batches' plan carries it too.
        costs["source_copy_job_cost_per_run"], costs["source_copy_job_cost_per_run_unknown_reason"] = (
            (source_copy_costs.get("job_cost_per_run"), source_copy_costs.get("job_cost_per_run_unknown_reason"))
            if source_copy_costs is not None
            else self._source_copy_fee_per_run(
                compute_activity=lambda **kw: self._plan_activity_costs(ctx, **kw),
                invention_source_row=invention_source_row, adjusted_price_map=ctx.adjusted_market_price_map,
            )
        )
        if source_copy_costs is not None:
            tree_nodes.append(self._job_tree_node(
                label=self._ACTIVITY_LABELS["copying"], node_type="activity", activity="copying",
                blueprint_name=invention_bp_name, runs=max(0, missing_source_runs),
                duration_seconds=source_copy_costs["duration_seconds"],
                direct_duration_seconds=source_copy_costs["duration_seconds"],
                job_duration_seconds=source_copy_costs["job_duration_seconds"],
                elapsed_duration_seconds=source_copy_costs["elapsed_duration_seconds"],
                duration_is_amortized=source_copy_costs["duration_is_amortized"],
                job_cost=source_copy_costs.get("total_job_cost"), total_job_cost=source_copy_costs.get("total_job_cost"),
                category_name=str(compact_product.get("category_name") or "") or None,
                meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                recommendation_action="copy", children=[],
            ))

        return costs, tree_nodes, planned_inv_materials if requires_invention_chain else [], source_copy_costs

    def _plan_research_chain_for_variant(
        self,
        ctx: _ProductPlanningContext,
        *,
        row: dict[str, Any],
        per_run_material_eiv: float | None,
    ) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
        """Plan ME/TE research activities (Step 4). Returns list of (activity_name, costs, tree_node)."""
        results: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        for activity_name, source_field in [("research_material", "research_material_job"), ("research_time", "research_time_job")]:
            base_level_one = int(((row.get(source_field) or {}).get("time_seconds") or 0))
            target_duration = self._research_target_duration_seconds(level_one_duration_seconds=base_level_one, target_level=self._MAX_RESEARCH_LEVEL)
            if target_duration <= 0:
                continue
            process_value = self._research_process_value(per_run_material_eiv, self._MAX_RESEARCH_LEVEL)
            costs = self._plan_activity_costs(ctx, activity=activity_name, base_time_seconds=target_duration, runs=1, process_value=process_value)
            costs["estimated_item_value"] = per_run_material_eiv
            tree_node = self._job_tree_node(
                label=self._ACTIVITY_LABELS.get(activity_name, activity_name),
                node_type="activity", activity=activity_name, runs=None,
                duration_seconds=costs["duration_seconds"], direct_duration_seconds=costs["duration_seconds"],
                job_cost=costs.get("total_job_cost"), total_job_cost=costs.get("total_job_cost"), children=[],
            )
            results.append((activity_name, costs, tree_node))
        return results

    def _with_component_job_fee(self, ctx: _ProductPlanningContext, entry: dict[str, Any]) -> dict[str, Any]:
        """A parent's material entry, plus `component_job_fee_per_run` when the
        material is itself manufactured: the daily
        planner's sub_manufacture fee when the component has no overview row
        of its own (e.g. owned_blueprints_scope excludes corp assets)."""
        fee, reason = self._component_job_fee_per_run(ctx, int(entry.get("type_id") or 0))
        if fee is not None:
            return {**entry, "component_job_fee_per_run": fee}
        if reason is not None:
            return {**entry, "component_job_fee_per_run_unknown_reason": reason}
        return entry

    def _component_job_fee_per_run(self, ctx: _ProductPlanningContext, component_type_id: int) -> tuple[float | None, str | None]:
        """The install fee of ONE manufacturing run of a component in this
        refresh's industry profile (the parent's structure, F-Q2): the SDE
        base (ME 0) materials x adjusted prices as the EIV, priced by
        _plan_activity_costs -- the same formula as a row's own
        manufacturing_job_cost. (None, None) for a material with no
        manufacturing blueprint (bought, no fee); (None, why) when the
        component's time, EIV or another input is unknown (unknown, not free)."""
        component_row = ctx.manufacturing_row_by_product_type_id.get(int(component_type_id))
        job = (component_row or {}).get("manufacturing_job")
        if not isinstance(job, dict):
            return None, None
        base_time = int(job.get("time_seconds") or 0)
        if base_time <= 0:
            return None, "the component's blueprint has no manufacturing time in the SDE"
        materials = [dict(e) for e in (job.get("materials") or []) if isinstance(e, dict)]
        eiv_reasons: list[str] = []
        process_value, _ = self._sum_estimated_item_value(
            materials, quantity_key="quantity", adjusted_price_map=ctx.adjusted_market_price_map,
            unknown_reasons=eiv_reasons,
        )
        if process_value is None:
            return None, f"no priced EIV for the component ({'; '.join(eiv_reasons) or 'a material has no adjusted price'})"
        product = next(
            (p for p in (job.get("products") or []) if isinstance(p, dict) and int(p.get("type_id") or 0) == int(component_type_id)),
            {},
        )
        fee = self._plan_activity_costs(
            ctx, activity="manufacturing", base_time_seconds=base_time, runs=1, process_value=process_value,
            manufacturing_group=self._manufacturing_rig_groups(product),
        )
        total = fee.get("total_job_cost")
        return total, (None if total is not None else (fee.get("fee_unknown_reason") or "the component fee could not be priced"))

    def _copy_process_value(self, per_run_material_eiv: float | None, copied_runs: float) -> float | None:
        """The job value of a copy job (job_cost.py): the blueprint's per-run
        manufacturing EIV x the total copied runs x copy_cost_eiv_multiplier
        (0.02). None when the EIV is unknown; 0 runs (or fewer) is 0."""
        if per_run_material_eiv is None:
            return None
        return (
            float(per_run_material_eiv) * max(0.0, float(copied_runs))
            * float(self._adm("industry", "copy_cost_eiv_multiplier"))
        )

    def _research_process_value(self, per_run_material_eiv: float | None, target_level: int) -> float | None:
        """The job value of a research job from level 0 to `target_level`:
        per-run material EIV x research_cost_eiv_multiplier (0.02) x EVE's
        level multiplier (job_cost.research_level_multiplier: 1 at level 1 ...
        2438.1 at level 10). Rank-independent: real ESI jobs of rank 1 to 40
        cost the same per ISK of EIV x level multiplier (Task 8b fit). The
        one formula for every research fee (the SDE research chains and the
        daily planner's ME/TE research actions). None when the EIV is unknown."""
        if per_run_material_eiv is None:
            return None
        level_multiplier = research_level_multiplier(0, int(target_level))
        if level_multiplier <= 0:
            return None
        return float(per_run_material_eiv) * float(self._adm("industry", "research_cost_eiv_multiplier")) * level_multiplier

    def _research_job_cost_by_level(
        self,
        ctx: _ProductPlanningContext,
        *,
        row: dict[str, Any],
        per_run_material_eiv: float | None,
    ) -> tuple[dict[str, list[float] | None], dict[str, str]]:
        """(tables, reasons). The install fee of researching this blueprint from level 0 to each
        level 0..max, per research activity: a list indexed by level,
        [0] = 0.0. Priced exactly like _plan_research_chain_for_variant (the
        same process value, cost index, structure/rig/implant reductions and
        surcharge). The fee is linear in the process value, so a job from
        level a to b costs [b] - [a]. None for an activity whose fee is
        unknown at any level (no research time, no EIV, no cost index or no
        facility tax); `reasons[activity]` then says why."""
        tables: dict[str, list[float] | None] = {}
        reasons: dict[str, str] = {}
        for activity_name, source_field in [("research_material", "research_material_job"), ("research_time", "research_time_job")]:
            base_level_one = int(((row.get(source_field) or {}).get("time_seconds") or 0))
            fees: list[float] | None = [0.0]
            for level in range(1, self._MAX_RESEARCH_LEVEL + 1):
                duration = self._research_target_duration_seconds(level_one_duration_seconds=base_level_one, target_level=level)
                if duration <= 0:
                    fees = None
                    reasons[activity_name] = "the blueprint has no research time in the SDE"
                    break
                priced = self._plan_activity_costs(
                    ctx, activity=activity_name, base_time_seconds=duration, runs=1,
                    process_value=self._research_process_value(per_run_material_eiv, level),
                )
                fee = priced.get("total_job_cost")
                if fee is None:
                    fees = None
                    reasons[activity_name] = priced.get("fee_unknown_reason") or "the research fee could not be priced"
                    break
                fees.append(float(fee))
            tables[activity_name] = fees
        return tables, reasons

    def _research_implant_multipliers(self, ctx: _ProductPlanningContext) -> dict[str, dict[str, float]]:
        """{character_id: {research activity: the share of the duration its
        implants leave}} for the selected character and every corp pilot
        whose industry payload is known (an unknown one is left out: the
        planner then applies no implants, the longer duration)."""
        payloads: dict[int, Any] = {}
        for cid, capacity in (ctx.invention_context.slot_capacity_by_character_id or {}).items():
            if capacity.industry_payload is not None:
                payloads[int(cid)] = capacity.industry_payload
        if ctx.character_id and ctx.selected_character_modifiers is not None:
            payloads[int(ctx.character_id)] = ctx.selected_character_modifiers
        return {
            str(cid): {
                activity: 1.0 - self._implant_reduction(
                    character_modifier_payload=dict(payload), activity=activity, metric="time",
                )
                for activity in ("research_material", "research_time")
            }
            for cid, payload in sorted(payloads.items())
        }

    def _research_duration_by_level(
        self, ctx: _ProductPlanningContext, *, row: dict[str, Any], skilled: bool = True, implants: bool = True,
    ) -> dict[str, list[int] | None]:
        """The duration in seconds of researching this blueprint from level 0
        to each level 0..max, per research activity: a list indexed by level,
        [0] = 0. The model the research fee uses: the SDE rank
        (level-1 time / 105 s) x EVE's rank-1 level table, then the skill,
        structure, rig and implant time reductions of _plan_activity_costs.
        A job from level a to b takes [b] - [a] (EVE's table is cumulative).
        None for an activity without a research time in the SDE.

        skilled=False leaves the research skills out (structure, rig and
        implant reductions stay): the daily planner applies the ASSIGNED
        pilot's skills to it (research_time.research_skill_time_multiplier).
        implants=False also leaves the producer character's implants out."""
        tables: dict[str, list[int] | None] = {}
        for activity_name, source_field in [("research_material", "research_material_job"), ("research_time", "research_time_job")]:
            base_level_one = int(((row.get(source_field) or {}).get("time_seconds") or 0))
            if base_level_one <= 0:
                tables[activity_name] = None
                continue
            durations = [0]
            for level in range(1, self._MAX_RESEARCH_LEVEL + 1):
                target = self._research_target_duration_seconds(level_one_duration_seconds=base_level_one, target_level=level)
                if skilled:
                    priced = self._plan_activity_costs(ctx, activity=activity_name, base_time_seconds=target, runs=1, process_value=None)
                else:
                    priced = self._compute_activity_job(
                        activity=activity_name, base_time_seconds=target, runs=1, process_value=None,
                        selected_industry_profile=ctx.selected_industry_profile,
                        selected_character_modifiers=ctx.selected_character_modifiers if implants else None,
                        character_skill_levels_by_name={}, surcharge_rates=ctx.surcharge_rates,
                    )
                durations.append(int(priced["duration_seconds"]))
            tables[activity_name] = durations
        return tables

    @staticmethod
    def _row_invents(ctx: _ProductPlanningContext, blueprint_type_id: int) -> bool:
        """Whether rows of this blueprint invent (in batch 1 or the later
        batches): the predicate _build_single_product_row resolves its
        decryptor under."""
        source = ctx.invention_row_by_blueprint_type_id.get(int(blueprint_type_id))
        return (
            bool(ctx.build_from_bpc)
            and isinstance((source or {}).get("invention_job"), dict)
            and not ctx.blueprint_original_assets_by_type_id.get(int(blueprint_type_id))
        )

    def _build_product_row(self, ctx: _ProductPlanningContext, **variant: Any) -> dict[str, Any] | None:
        """One overview row, invented with its blueprint's best decryptor; the
        choice is on manufacturing_job.invention_decryptor."""
        blueprint_type_id = int(variant["row"].get("blueprint_type_id") or 0)
        if not self._row_invents(ctx, blueprint_type_id):
            return self._build_single_product_row(ctx, **variant)
        choice = self._decryptor_choice_for(ctx, blueprint_type_id)
        row_out = self._build_single_product_row(ctx, decryptor=choice.option, **variant)
        if row_out is not None:
            row_out["manufacturing_job"]["invention_decryptor"] = decryptor_choice_payload(choice)
        return row_out

    def _decryptor_choice_for(self, ctx: _ProductPlanningContext, blueprint_type_id: int) -> DecryptorChoice:
        """The decryptor to invent `blueprint_type_id` with: chosen once per
        refresh by invention_model.select_best_decryptor and memoized on ctx.

        Thread-safe without a lock: two workers may evaluate the same
        blueprint at once (same inputs, same result), and setdefault keeps the
        first, so every caller uses one choice. No lock is held while
        evaluating, so nested evaluations on different threads cannot
        deadlock."""
        blueprint_type_id = int(blueprint_type_id)
        cached = ctx.decryptor_choice_by_blueprint_type_id.get(blueprint_type_id)
        if cached is not None:
            return cached
        if not ctx.decryptor_selection_enabled:
            choice = DecryptorChoice(NO_DECRYPTOR, DECRYPTOR_SELECTION_OFF_REASON)
        elif len(ctx.decryptor_options) <= 1:
            choice = DecryptorChoice(NO_DECRYPTOR, NO_DECRYPTOR_DATA_REASON)
        else:
            evaluating = getattr(ctx.decryptor_evaluation_guard, "blueprint_type_ids", None)
            if evaluating is None:
                evaluating = set()
                ctx.decryptor_evaluation_guard.blueprint_type_ids = evaluating
            if blueprint_type_id in evaluating:
                # Met again inside its own evaluation (a material cycle): this
                # inner use gets no decryptor, and nothing is cached for it.
                return DecryptorChoice(NO_DECRYPTOR, "no decryptor: met inside its own decryptor evaluation")
            evaluating.add(blueprint_type_id)
            try:
                choice = self._evaluate_decryptors(ctx, blueprint_type_id)
            finally:
                evaluating.discard(blueprint_type_id)
        return ctx.decryptor_choice_by_blueprint_type_id.setdefault(blueprint_type_id, choice)

    def _evaluate_decryptors(self, ctx: _ProductPlanningContext, blueprint_type_id: int) -> DecryptorChoice:
        """Build one invention cycle of the blueprint's own row per decryptor
        option and let select_best_decryptor pick.

        One cycle = the runs one invented BPC yields with that option (SDE
        runs + its run modifier), every run invented: the expected attempts
        are then exactly 1 / chance, so the row's materials, amortized
        invention inputs (decryptor included) and job fees are the cycle's.
        The builds are thrown away, so their WARNINGs are dropped
        (quiet_warnings); the row that is kept logs its own."""
        row = ctx.blueprint_row_by_type_id.get(blueprint_type_id) or {}
        manufacturing_job = row.get("manufacturing_job") or {}
        products = [
            p for p in (manufacturing_job.get("products") or [])
            if isinstance(p, dict) and int(p.get("type_id") or 0) > 0
        ] if isinstance(manufacturing_job, dict) else []
        if not products:
            return DecryptorChoice(NO_DECRYPTOR, "no decryptor: the blueprint makes no product to compare on")
        source = ctx.invention_row_by_blueprint_type_id.get(blueprint_type_id) or {}
        invention_job = source.get("invention_job") if isinstance(source.get("invention_job"), dict) else {}
        target_entry = self._invention_target_entry(invention_job, blueprint_type_id)
        max_production_limit = int(manufacturing_job.get("max_production_limit") or 0)
        candidates: list[tuple[DecryptorOption, dict[str, Any] | None, str | None]] = []
        with quiet_warnings():
            for option in ctx.decryptor_options:
                skip_reason = self._decryptor_price_skip_reason(ctx, option)
                if skip_reason is not None:
                    candidates.append((option, None, skip_reason))
                    continue
                candidate = self._build_single_product_row(
                    ctx, row=row, raw_product=products[0], row_index=0, product_index=0,
                    blueprint_copy_assets=[], blueprint_original_asset=None,
                    effective_runs=self._runs_per_invented_blueprint(
                        target_entry, max_production_limit=max_production_limit, decryptor=option,
                    ),
                    decryptor=option, fully_invented=True,
                )
                candidates.append((option, candidate, None))
        include_known_fees, fee_note = self._decryptor_fee_basis([c for _, c, skip in candidates if skip is None])
        evaluations = [
            DecryptorEvaluation(option, None, skip) if skip is not None
            else self._cycle_evaluation(ctx, option, candidate, fees=("known" if include_known_fees else "none"))
            for option, candidate, skip in candidates
        ]
        choice = select_best_decryptor(evaluations)
        if fee_note is not None:
            choice = dataclasses.replace(choice, reason=f"{choice.reason}; {fee_note}")
        return choice

    @staticmethod
    def _decryptor_fee_basis(candidate_rows: list[dict[str, Any] | None]) -> tuple[bool, str | None]:
        """How the decryptor candidates' job fees enter the comparison:
        (True, None) every fee is known; (True, note) the same fees are unknown
        in every candidate (a profile-wide gap: the decryptor changes runs and
        attempts, not which activity has an index), so the candidates compare
        on their priced fees and the note says which were left out;
        (False, note) the candidates lack different fees, so no fee is
        compared at all (a like-for-like comparison of the rest)."""
        sets = [
            tuple((row.get("manufacturing_job") or {}).get("job_fee_unknown_reasons") or [])
            for row in candidate_rows if isinstance(row, dict)
        ]
        if not any(sets):
            return True, None
        if len(set(sets)) == 1:
            return True, f"job fees unknown for: {'; '.join(sets[0])} — left out of every candidate's cost"
        union = list(dict.fromkeys(r for reasons in sets for r in reasons))
        return False, (
            f"job fees unknown for some candidates ({'; '.join(union)}) — "
            f"every job fee left out of the comparison"
        )

    @classmethod
    def _decryptor_price_skip_reason(cls, ctx: _ProductPlanningContext, option: DecryptorOption) -> str | None:
        """Why a decryptor cannot be a candidate: it has no hub market price.
        An adjusted-price fallback would make an unbuyable decryptor look cheap."""
        if option.type_id is None:
            return None
        unit_price = cls._as_float((ctx.material_price_map.get(int(option.type_id)) or {}).get("unit_price"))
        if unit_price is None or unit_price <= 0:
            return f"no market price for {option.type_name}"
        return None
