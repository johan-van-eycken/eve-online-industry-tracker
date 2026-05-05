from __future__ import annotations

from typing import Any
import math


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
        capital_limit_isk: float,
        manufacturing_slots_available: int,
        planning_horizon_hours: float,
        objective: str = "balanced",
        positive_profit_only: bool = True,
        min_margin_pct: float = 0.0,
        min_isk_per_hour: float = 0.0,
        min_region_daily_volume: int = 0,
        minimum_pricing_confidence: str = "low",
    ) -> dict[str, Any]:
        normalized_objective = str(objective or "balanced").strip().lower() or "balanced"
        minimum_confidence_rank = cls._confidence_rank_value(minimum_pricing_confidence)
        total_slot_hours_budget = max(0.0, float(manufacturing_slots_available or 0) * float(planning_horizon_hours or 0.0))
        remaining_capital = max(0.0, float(capital_limit_isk or 0.0))
        remaining_slot_hours = total_slot_hours_budget

        selected_items: list[dict[str, Any]] = []
        skipped_items: list[dict[str, Any]] = []

        filtered_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            reasons: list[str] = []
            if positive_profit_only and float(candidate.get("profit_amount") or 0.0) <= 0.0:
                reasons.append("Non-positive profit")
            if float(candidate.get("profit_margin_fraction") or 0.0) < (float(min_margin_pct or 0.0) / 100.0):
                reasons.append("Below minimum margin")
            if float(candidate.get("isk_per_hour") or 0.0) < float(min_isk_per_hour or 0.0):
                reasons.append("Below minimum ISK/hour")
            if int(candidate.get("region_daily_volume") or 0) < int(min_region_daily_volume or 0):
                reasons.append("Below minimum region daily volume")
            if cls._confidence_rank_value(candidate.get("pricing_confidence")) < minimum_confidence_rank:
                reasons.append("Below minimum pricing confidence")
            if not bool(candidate.get("is_portfolio_candidate", False)):
                reasons.append("Not portfolio-eligible")

            if reasons:
                skipped_items.append(
                    {
                        "type_id": candidate.get("type_id"),
                        "type_name": candidate.get("type_name"),
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

        for candidate in sorted_candidates:
            cash_outlay_per_batch = cls._safe_float(candidate.get("cash_outlay_per_batch"))
            slot_hours_per_batch = cls._safe_float(candidate.get("slot_hours_per_batch"))
            max_batches_total = cls._safe_int(candidate.get("max_batches_total"))
            units_per_batch = max(1, cls._safe_int(candidate.get("quantity_per_batch") or 1))

            if cash_outlay_per_batch is None or cash_outlay_per_batch <= 0:
                skipped_items.append(
                    {
                        "type_id": candidate.get("type_id"),
                        "type_name": candidate.get("type_name"),
                        "reason": "Missing cash outlay per batch",
                    }
                )
                continue
            if slot_hours_per_batch is None or slot_hours_per_batch <= 0:
                skipped_items.append(
                    {
                        "type_id": candidate.get("type_id"),
                        "type_name": candidate.get("type_name"),
                        "reason": "Missing slot-hours per batch",
                    }
                )
                continue
            if max_batches_total <= 0:
                skipped_items.append(
                    {
                        "type_id": candidate.get("type_id"),
                        "type_name": candidate.get("type_name"),
                        "reason": "No market-supported batches available in planning horizon",
                    }
                )
                continue

            max_by_capital = int(math.floor(remaining_capital / cash_outlay_per_batch)) if cash_outlay_per_batch > 0 else 0
            max_by_slots = int(math.floor(remaining_slot_hours / slot_hours_per_batch)) if slot_hours_per_batch > 0 else 0
            allocatable_batches = min(max_batches_total, max_by_capital, max_by_slots)

            if allocatable_batches <= 0:
                limiting_reason = "Capital exhausted" if max_by_capital <= 0 else "Manufacturing slot budget exhausted"
                skipped_items.append(
                    {
                        "type_id": candidate.get("type_id"),
                        "type_name": candidate.get("type_name"),
                        "reason": limiting_reason,
                    }
                )
                continue

            committed_capital = float(cash_outlay_per_batch) * float(allocatable_batches)
            committed_slot_hours = float(slot_hours_per_batch) * float(allocatable_batches)
            expected_profit = float(candidate.get("profit_amount") or 0.0) * float(allocatable_batches)
            allocated_units = int(units_per_batch * allocatable_batches)

            selected_items.append(
                {
                    "type_id": candidate.get("type_id"),
                    "type_name": candidate.get("type_name"),
                    "batches": allocatable_batches,
                    "units": allocated_units,
                    "capital_committed": committed_capital,
                    "slot_hours_committed": committed_slot_hours,
                    "expected_profit": expected_profit,
                    "profit_margin_fraction": candidate.get("profit_margin_fraction"),
                    "isk_per_hour": candidate.get("isk_per_hour"),
                    "pricing_confidence": candidate.get("pricing_confidence"),
                }
            )

            remaining_capital = max(0.0, remaining_capital - committed_capital)
            remaining_slot_hours = max(0.0, remaining_slot_hours - committed_slot_hours)
            total_expected_profit += expected_profit
            total_required_capital += committed_capital
            total_allocated_units += allocated_units
            total_allocated_batches += allocatable_batches
            slot_hours_committed += committed_slot_hours

        return {
            "objective": normalized_objective,
            "planning_horizon_hours": float(planning_horizon_hours or 0.0),
            "manufacturing_slots_available": int(manufacturing_slots_available or 0),
            "capital_limit_isk": float(capital_limit_isk or 0.0),
            "slot_hours_budget": total_slot_hours_budget,
            "slot_hours_committed": slot_hours_committed,
            "slot_hours_remaining": remaining_slot_hours,
            "capital_committed": total_required_capital,
            "capital_remaining": remaining_capital,
            "total_expected_profit": total_expected_profit,
            "total_allocated_units": total_allocated_units,
            "total_allocated_batches": total_allocated_batches,
            "selected_items": selected_items,
            "skipped_items": skipped_items,
            "selected_count": len(selected_items),
            "skipped_count": len(skipped_items),
        }
