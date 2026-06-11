from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import math


@dataclass(slots=True)
class PortfolioCandidateScope:
    categories: list[str] = field(default_factory=list)
    meta_groups: list[str] = field(default_factory=list)
    pricing_confidences: list[str] = field(default_factory=list)
    blueprint_sources: list[str] = field(default_factory=list)
    positive_profit_only: bool = False
    min_margin_pct: float = 0.0
    min_isk_per_hour: float = 0.0
    min_region_daily_volume: int = 0
    min_owned_input_coverage_pct: float = 0.0


@dataclass(slots=True)
class PortfolioCandidateDirective:
    overview_row_id: str = ""
    force_include: bool = False
    exclude: bool = False
    lock_required: bool = False
    max_batches_override: int | None = None
    target_batches_override: int | None = None
    target_units_override: int | None = None


@dataclass(slots=True)
class PortfolioPlanRequest:
    candidate_snapshot_id: str = ""
    capital_limit_isk: float = 0.0
    manufacturing_slots_available: int = 0
    research_slots_available: int = 0
    reaction_slots_available: int = 0
    planning_horizon_hours: float = 24.0
    objective: str = "balanced"
    minimum_pricing_confidence: str = "low"
    candidate_scope: PortfolioCandidateScope = field(default_factory=PortfolioCandidateScope)
    candidate_directives: list[PortfolioCandidateDirective] = field(default_factory=list)


class IndustryPortfolioService:
    _CONFIDENCE_RANK = {
        "low": 1,
        "medium": 2,
        "high": 3,
    }

    @classmethod
    def _confidence_rank_value(cls, value: Any) -> int:
        normalized = str(value or "").strip().lower()
        return int(cls._CONFIDENCE_RANK.get(normalized, 0))

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        try:
            parsed = float(value)
        except Exception:
            return None
        if math.isnan(parsed) or math.isinf(parsed):
            return None
        return parsed

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(value or 0)
        except Exception:
            return 0

    @staticmethod
    def _directive_map(plan_request: PortfolioPlanRequest) -> dict[str, PortfolioCandidateDirective]:
        directives: dict[str, PortfolioCandidateDirective] = {}
        for directive in plan_request.candidate_directives or []:
            overview_row_id = str(getattr(directive, "overview_row_id", "") or "").strip()
            if not overview_row_id:
                continue
            directives[overview_row_id] = directive
        return directives

    @staticmethod
    def _override_or_none(value: Any) -> int | None:
        try:
            parsed = int(value)
        except Exception:
            return None
        return parsed if parsed > 0 else None

    @classmethod
    def _candidate_requested_batches(
        cls,
        *,
        directive: PortfolioCandidateDirective | None,
        units_per_batch: int,
    ) -> int | None:
        if directive is None:
            return None
        target_batches = cls._override_or_none(directive.target_batches_override)
        target_units = cls._override_or_none(directive.target_units_override)
        requested_batches = target_batches
        if target_units is not None:
            requested_from_units = max(1, int(math.ceil(float(target_units) / float(max(1, units_per_batch)))))
            requested_batches = max(requested_batches or 0, requested_from_units)
        return requested_batches

    @classmethod
    def _effective_max_batches(
        cls,
        *,
        candidate: dict[str, Any],
        directive: PortfolioCandidateDirective | None,
    ) -> int:
        max_batches_total = cls._safe_int(candidate.get("max_batches_total"))
        override_batches = cls._override_or_none(getattr(directive, "max_batches_override", None) if directive else None)
        if override_batches is None:
            return max_batches_total
        return min(max_batches_total, override_batches)

    @staticmethod
    def _decision_label(candidate: dict[str, Any]) -> str:
        return str(candidate.get("type_name") or candidate.get("type_id") or candidate.get("overview_row_id") or "Candidate")

    @classmethod
    def _limiting_reason(
        cls,
        *,
        effective_max_batches: int,
        max_by_capital: int,
        max_by_slots: int,
    ) -> str:
        if effective_max_batches <= 0:
            return "No batches allowed after operator override or market limit"
        if max_by_capital <= 0:
            return "Capital exhausted"
        if max_by_slots <= 0:
            return "Manufacturing slot budget exhausted"
        return "Insufficient remaining capital or slot budget"

    @classmethod
    def _append_decision_entry(
        cls,
        items: list[dict[str, Any]],
        *,
        candidate: dict[str, Any],
        directive: PortfolioCandidateDirective,
        reason: str | None = None,
    ) -> None:
        items.append(
            {
                "overview_row_id": candidate.get("overview_row_id"),
                "type_id": candidate.get("type_id"),
                "type_name": candidate.get("type_name"),
                "force_include": bool(directive.force_include),
                "exclude": bool(directive.exclude),
                "lock_required": bool(directive.lock_required),
                "max_batches_override": cls._override_or_none(directive.max_batches_override),
                "target_batches_override": cls._override_or_none(directive.target_batches_override),
                "target_units_override": cls._override_or_none(directive.target_units_override),
                "reason": reason,
            }
        )

    @classmethod
    def _objective_sort_key(cls, candidate: dict[str, Any], objective: str) -> tuple[float, float, float, float]:
        effective_profit = float(candidate.get("effective_profit_per_batch") or 0.0)
        effective_isk_per_hour = float(candidate.get("effective_isk_per_hour") or 0.0)
        margin_fraction = float(candidate.get("profit_margin_fraction") or 0.0)
        confidence_rank = float(cls._confidence_rank_value(candidate.get("pricing_confidence")))
        normalized_objective = str(objective or "balanced").strip().lower()

        if normalized_objective == "max_isk_per_hour":
            return (effective_isk_per_hour, effective_profit, margin_fraction, confidence_rank)
        if normalized_objective == "balanced":
            return (effective_profit, effective_isk_per_hour, margin_fraction, confidence_rank)
        return (effective_profit, effective_isk_per_hour, margin_fraction, confidence_rank)

    @classmethod
    def optimize_manufacturing_portfolio(
        cls,
        *,
        candidates: list[dict[str, Any]],
        plan_request: PortfolioPlanRequest,
    ) -> dict[str, Any]:
        normalized_objective = str(plan_request.objective or "balanced").strip().lower() or "balanced"
        minimum_confidence_rank = cls._confidence_rank_value(plan_request.minimum_pricing_confidence)
        scope = plan_request.candidate_scope
        normalized_categories = {
            str(value or "").strip().casefold() for value in (scope.categories or []) if str(value or "").strip()
        }
        normalized_meta_groups = {
            str(value or "").strip().casefold() for value in (scope.meta_groups or []) if str(value or "").strip()
        }
        normalized_confidences = {
            str(value or "").strip().lower() for value in (scope.pricing_confidences or []) if str(value or "").strip()
        }
        normalized_blueprint_sources = {
            str(value or "").strip().lower() for value in (scope.blueprint_sources or []) if str(value or "").strip()
        }
        directive_by_row_id = cls._directive_map(plan_request)
        total_slot_hours_budget = max(
            0.0,
            float(plan_request.manufacturing_slots_available or 0) * float(plan_request.planning_horizon_hours or 0.0),
        )
        total_research_slot_hours_budget = max(
            0.0,
            float(plan_request.research_slots_available or 0) * float(plan_request.planning_horizon_hours or 0.0),
        )
        remaining_capital = max(0.0, float(plan_request.capital_limit_isk or 0.0))
        remaining_slot_hours = total_slot_hours_budget
        remaining_research_slot_hours = total_research_slot_hours_budget

        selected_items: list[dict[str, Any]] = []
        selected_item_by_row_id: dict[str, dict[str, Any]] = {}
        skipped_items: list[dict[str, Any]] = []
        forced_items: list[dict[str, Any]] = []
        excluded_items: list[dict[str, Any]] = []
        locked_items: list[dict[str, Any]] = []
        override_items: list[dict[str, Any]] = []
        unfulfilled_locked_items: list[dict[str, Any]] = []

        filtered_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            overview_row_id = str(candidate.get("overview_row_id") or "").strip()
            directive = directive_by_row_id.get(overview_row_id)
            reasons: list[str] = []
            candidate_category = str(candidate.get("category_name") or "").strip().casefold()
            candidate_meta_group = str(candidate.get("meta_group_name") or "").strip().casefold()
            candidate_confidence = str(candidate.get("pricing_confidence") or "").strip().lower()
            candidate_blueprint_source = str(candidate.get("blueprint_source_kind") or "").strip().lower()

            if directive is not None:
                if bool(directive.exclude):
                    reason = "Operator excluded item"
                    skipped_items.append(
                        {
                            "overview_row_id": overview_row_id,
                            "type_id": candidate.get("type_id"),
                            "type_name": candidate.get("type_name"),
                            "reason": reason,
                        }
                    )
                    cls._append_decision_entry(excluded_items, candidate=candidate, directive=directive, reason=reason)
                    continue
                if bool(directive.force_include):
                    cls._append_decision_entry(forced_items, candidate=candidate, directive=directive)
                if bool(directive.lock_required):
                    cls._append_decision_entry(locked_items, candidate=candidate, directive=directive)
                if any(
                    cls._override_or_none(value) is not None
                    for value in [directive.max_batches_override, directive.target_batches_override, directive.target_units_override]
                ):
                    cls._append_decision_entry(override_items, candidate=candidate, directive=directive)

            bypass_scope = bool(directive and (directive.force_include or directive.lock_required))

            if not bypass_scope and normalized_categories and candidate_category not in normalized_categories:
                reasons.append("Outside selected categories")
            if not bypass_scope and normalized_meta_groups and candidate_meta_group not in normalized_meta_groups:
                reasons.append("Outside selected meta groups")
            if not bypass_scope and normalized_confidences and candidate_confidence not in normalized_confidences:
                reasons.append("Outside selected pricing confidence")
            if not bypass_scope and normalized_blueprint_sources and candidate_blueprint_source not in normalized_blueprint_sources:
                reasons.append("Outside selected blueprint sources")
            if not bypass_scope and bool(scope.positive_profit_only) and float(candidate.get("profit_amount") or 0.0) <= 0.0:
                reasons.append("Non-positive profit")
            if not bypass_scope and float(candidate.get("profit_margin_fraction") or 0.0) < (float(scope.min_margin_pct or 0.0) / 100.0):
                reasons.append("Below minimum margin")
            if not bypass_scope and float(candidate.get("isk_per_hour") or 0.0) < float(scope.min_isk_per_hour or 0.0):
                reasons.append("Below minimum ISK/hour")
            if not bypass_scope and int(candidate.get("region_daily_volume") or 0) < int(scope.min_region_daily_volume or 0):
                reasons.append("Below minimum region daily volume")
            if not bypass_scope and float(candidate.get("owned_input_coverage_fraction") or 0.0) < (
                float(scope.min_owned_input_coverage_pct or 0.0) / 100.0
            ):
                reasons.append("Below minimum owned input coverage")
            if not bypass_scope and cls._confidence_rank_value(candidate.get("pricing_confidence")) < minimum_confidence_rank:
                reasons.append("Below minimum pricing confidence")
            if not bypass_scope and candidate.get("skill_requirements_met") is False:
                reasons.append("Missing required manufacturing skills")
            if not bool(candidate.get("is_portfolio_candidate", False)):
                reasons.append("Not portfolio-eligible")

            if reasons:
                skipped_items.append(
                    {
                        "type_id": candidate.get("type_id"),
                        "type_name": candidate.get("type_name"),
                        "overview_row_id": overview_row_id,
                        "reason": "; ".join(reasons),
                    }
                )
                continue
            filtered_candidates.append(candidate)

        sorted_candidates = sorted(
            filtered_candidates,
            key=lambda candidate: cls._objective_sort_key(candidate, normalized_objective),
            reverse=True,
        )

        total_expected_profit = 0.0
        total_required_capital = 0.0
        total_allocated_units = 0
        total_allocated_batches = 0
        slot_hours_committed = 0.0

        allocated_batches_by_row_id: dict[str, int] = {}

        def record_selected_item(
            *,
            candidate: dict[str, Any],
            directive: PortfolioCandidateDirective | None,
            allocated_batches: int,
            pass_name: str,
        ) -> None:
            nonlocal total_expected_profit, total_required_capital, total_allocated_units, total_allocated_batches, slot_hours_committed

            overview_row_id = str(candidate.get("overview_row_id") or "")
            cash_outlay_per_batch = float(candidate.get("cash_outlay_per_batch") or 0.0)
            slot_hours_per_batch = float(candidate.get("slot_hours_per_batch") or 0.0)
            units_per_batch = max(1, cls._safe_int(candidate.get("quantity_per_batch") or 1))
            committed_capital = float(cash_outlay_per_batch) * float(allocated_batches)
            committed_slot_hours = float(slot_hours_per_batch) * float(allocated_batches)
            expected_profit = float(candidate.get("profit_amount") or 0.0) * float(allocated_batches)
            allocated_units = int(units_per_batch * allocated_batches)

            existing = selected_item_by_row_id.get(overview_row_id)
            if existing is None:
                existing = {
                    "overview_row_id": overview_row_id,
                    "type_id": candidate.get("type_id"),
                    "type_name": candidate.get("type_name"),
                    "batches": 0,
                    "units": 0,
                    "capital_committed": 0.0,
                    "slot_hours_committed": 0.0,
                    "expected_profit": 0.0,
                    "profit_margin_fraction": candidate.get("profit_margin_fraction"),
                    "isk_per_hour": candidate.get("isk_per_hour"),
                    "pricing_confidence": candidate.get("pricing_confidence"),
                    "force_include": bool(directive.force_include) if directive else False,
                    "lock_required": bool(directive.lock_required) if directive else False,
                    "max_batches_override": cls._override_or_none(directive.max_batches_override) if directive else None,
                    "target_batches_override": cls._override_or_none(directive.target_batches_override) if directive else None,
                    "target_units_override": cls._override_or_none(directive.target_units_override) if directive else None,
                    "allocation_passes": [],
                }
                selected_item_by_row_id[overview_row_id] = existing
                selected_items.append(existing)

            existing["batches"] = int(existing.get("batches") or 0) + int(allocated_batches)
            existing["units"] = int(existing.get("units") or 0) + int(allocated_units)
            existing["capital_committed"] = float(existing.get("capital_committed") or 0.0) + committed_capital
            existing["slot_hours_committed"] = float(existing.get("slot_hours_committed") or 0.0) + committed_slot_hours
            existing["expected_profit"] = float(existing.get("expected_profit") or 0.0) + expected_profit
            allocation_passes = existing.get("allocation_passes")
            if not isinstance(allocation_passes, list):
                allocation_passes = []
                existing["allocation_passes"] = allocation_passes
            allocation_passes.append(pass_name)

            total_expected_profit += expected_profit
            total_required_capital += committed_capital
            total_allocated_units += allocated_units
            total_allocated_batches += allocated_batches
            slot_hours_committed += committed_slot_hours

        def allocate_candidate(
            *,
            candidate: dict[str, Any],
            directive: PortfolioCandidateDirective | None,
            requested_batches: int | None = None,
            minimum_batches: int = 0,
            pass_name: str,
        ) -> bool:
            nonlocal remaining_capital, remaining_slot_hours, remaining_research_slot_hours

            overview_row_id = str(candidate.get("overview_row_id") or "")
            cash_outlay_per_batch = cls._safe_float(candidate.get("cash_outlay_per_batch"))
            slot_hours_per_batch = cls._safe_float(candidate.get("manufacturing_slot_hours_per_batch")) or cls._safe_float(candidate.get("slot_hours_per_batch"))
            preparation_slot_hours_per_batch = cls._safe_float(candidate.get("preparation_slot_hours_per_batch")) or 0.0
            units_per_batch = max(1, cls._safe_int(candidate.get("quantity_per_batch") or 1))
            already_allocated = allocated_batches_by_row_id.get(overview_row_id, 0)
            effective_max_batches = cls._effective_max_batches(candidate=candidate, directive=directive)
            remaining_candidate_batches = max(0, effective_max_batches - int(already_allocated))

            if cash_outlay_per_batch is None or cash_outlay_per_batch <= 0:
                skipped_items.append(
                    {
                        "overview_row_id": overview_row_id,
                        "type_id": candidate.get("type_id"),
                        "type_name": candidate.get("type_name"),
                        "reason": "Missing cash outlay per batch",
                    }
                )
                return False
            if slot_hours_per_batch is None or slot_hours_per_batch <= 0:
                skipped_items.append(
                    {
                        "overview_row_id": overview_row_id,
                        "type_id": candidate.get("type_id"),
                        "type_name": candidate.get("type_name"),
                        "reason": "Missing slot-hours per batch",
                    }
                )
                return False
            if remaining_candidate_batches <= 0:
                skipped_items.append(
                    {
                        "overview_row_id": overview_row_id,
                        "type_id": candidate.get("type_id"),
                        "type_name": candidate.get("type_name"),
                        "reason": "No batches allowed after operator override or market limit",
                    }
                )
                return False

            max_by_capital = int(math.floor(remaining_capital / cash_outlay_per_batch)) if cash_outlay_per_batch > 0 else 0
            max_by_slots = int(math.floor(remaining_slot_hours / slot_hours_per_batch)) if slot_hours_per_batch > 0 else 0
            max_by_research_slots = (
                int(math.floor(remaining_research_slot_hours / preparation_slot_hours_per_batch))
                if preparation_slot_hours_per_batch > 0 and total_research_slot_hours_budget > 0
                else max_by_slots  # no research constraint if no prep time or no research budget configured
            )
            feasible_batches = min(remaining_candidate_batches, max_by_capital, max_by_slots, max_by_research_slots)

            if feasible_batches < int(minimum_batches):
                return False

            if requested_batches is not None and requested_batches > 0:
                allocatable_batches = min(int(requested_batches), feasible_batches)
                if allocatable_batches <= 0:
                    return False
            else:
                allocatable_batches = feasible_batches
                if minimum_batches > 0:
                    allocatable_batches = min(feasible_batches, int(minimum_batches))

            if allocatable_batches <= 0:
                return False

            remaining_capital = max(0.0, remaining_capital - (float(cash_outlay_per_batch) * float(allocatable_batches)))
            remaining_slot_hours = max(0.0, remaining_slot_hours - (float(slot_hours_per_batch) * float(allocatable_batches)))
            if preparation_slot_hours_per_batch > 0:
                remaining_research_slot_hours = max(0.0, remaining_research_slot_hours - (float(preparation_slot_hours_per_batch) * float(allocatable_batches)))
            allocated_batches_by_row_id[overview_row_id] = already_allocated + int(allocatable_batches)
            record_selected_item(
                candidate=candidate,
                directive=directive,
                allocated_batches=int(allocatable_batches),
                pass_name=pass_name,
            )
            return True

        operator_priority_candidates: list[tuple[dict[str, Any], PortfolioCandidateDirective, int | None, int]] = []
        normal_candidates: list[tuple[dict[str, Any], PortfolioCandidateDirective | None]] = []
        for candidate in sorted_candidates:
            overview_row_id = str(candidate.get("overview_row_id") or "")
            directive = directive_by_row_id.get(overview_row_id)
            requested_batches = cls._candidate_requested_batches(
                directive=directive,
                units_per_batch=max(1, cls._safe_int(candidate.get("quantity_per_batch") or 1)),
            )
            minimum_batches = 1 if directive and (directive.force_include or directive.lock_required) else 0
            if directive and (directive.force_include or directive.lock_required or requested_batches is not None):
                operator_priority_candidates.append((candidate, directive, requested_batches, minimum_batches))
            normal_candidates.append((candidate, directive))

        for candidate, directive, requested_batches, minimum_batches in operator_priority_candidates:
            overview_row_id = str(candidate.get("overview_row_id") or "")
            if allocate_candidate(
                candidate=candidate,
                directive=directive,
                requested_batches=requested_batches,
                minimum_batches=max(minimum_batches, 1 if directive and directive.lock_required else 0),
                pass_name="operator",
            ):
                continue

            limiting_reason = cls._limiting_reason(
                effective_max_batches=cls._effective_max_batches(candidate=candidate, directive=directive),
                max_by_capital=(
                    int(math.floor(remaining_capital / float(candidate.get("cash_outlay_per_batch") or 0.0)))
                    if float(candidate.get("cash_outlay_per_batch") or 0.0) > 0
                    else 0
                ),
                max_by_slots=(
                    int(math.floor(remaining_slot_hours / float(candidate.get("slot_hours_per_batch") or 0.0)))
                    if float(candidate.get("slot_hours_per_batch") or 0.0) > 0
                    else 0
                ),
            )
            operator_reason = (
                f"Locked item could not fit: {limiting_reason}"
                if directive and directive.lock_required
                else f"Forced item could not fit: {limiting_reason}"
            )
            skipped_items.append(
                {
                    "overview_row_id": overview_row_id,
                    "type_id": candidate.get("type_id"),
                    "type_name": candidate.get("type_name"),
                    "reason": operator_reason,
                }
            )
            if directive and directive.lock_required:
                cls._append_decision_entry(
                    unfulfilled_locked_items,
                    candidate=candidate,
                    directive=directive,
                    reason=operator_reason,
                )

        for candidate, directive in normal_candidates:
            overview_row_id = str(candidate.get("overview_row_id") or "")
            if allocated_batches_by_row_id.get(overview_row_id, 0) >= cls._effective_max_batches(candidate=candidate, directive=directive):
                continue

            requested_batches = None
            minimum_batches = 0
            if not allocate_candidate(
                candidate=candidate,
                directive=directive,
                requested_batches=requested_batches,
                minimum_batches=minimum_batches,
                pass_name="ranked",
            ):
                limiting_reason = cls._limiting_reason(
                    effective_max_batches=cls._effective_max_batches(candidate=candidate, directive=directive),
                    max_by_capital=(
                        int(math.floor(remaining_capital / float(candidate.get("cash_outlay_per_batch") or 0.0)))
                        if float(candidate.get("cash_outlay_per_batch") or 0.0) > 0
                        else 0
                    ),
                    max_by_slots=(
                        int(math.floor(remaining_slot_hours / float(candidate.get("slot_hours_per_batch") or 0.0)))
                        if float(candidate.get("slot_hours_per_batch") or 0.0) > 0
                        else 0
                    ),
                )
                skipped_items.append(
                    {
                        "overview_row_id": overview_row_id,
                        "type_id": candidate.get("type_id"),
                        "type_name": candidate.get("type_name"),
                        "reason": limiting_reason,
                    }
                )

        return {
            "objective": normalized_objective,
            "candidate_snapshot_id": str(plan_request.candidate_snapshot_id or ""),
            "planning_horizon_hours": float(plan_request.planning_horizon_hours or 0.0),
            "manufacturing_slots_available": int(plan_request.manufacturing_slots_available or 0),
            "research_slots_available": int(plan_request.research_slots_available or 0),
            "capital_limit_isk": float(plan_request.capital_limit_isk or 0.0),
            "slot_hours_budget": total_slot_hours_budget,
            "slot_hours_committed": slot_hours_committed,
            "slot_hours_remaining": remaining_slot_hours,
            "research_slot_hours_budget": total_research_slot_hours_budget,
            "research_slot_hours_remaining": remaining_research_slot_hours,
            "capital_committed": total_required_capital,
            "capital_remaining": remaining_capital,
            "total_expected_profit": total_expected_profit,
            "total_allocated_units": total_allocated_units,
            "total_allocated_batches": total_allocated_batches,
            "selected_items": selected_items,
            "skipped_items": skipped_items,
            "selected_count": len(selected_items),
            "skipped_count": len(skipped_items),
            "candidate_scope_count": len(filtered_candidates),
            "minimum_pricing_confidence": str(plan_request.minimum_pricing_confidence or "low"),
            "operator_decisions": {
                "forced_includes": forced_items,
                "exclusions": excluded_items,
                "locked_items": locked_items,
                "override_items": override_items,
                "unfulfilled_locked_items": unfulfilled_locked_items,
            },
        }
