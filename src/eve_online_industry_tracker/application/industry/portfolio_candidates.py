"""IndustryService: portfolio candidates: which overview rows are worth building, and in what order.

A mixin of IndustryService (service.py); it holds no state of its own."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, cast

from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.industry.facilities import FacilitiesMixin

if TYPE_CHECKING:
    from eve_online_industry_tracker.application.industry.service import ProgressCallback


class PortfolioCandidatesMixin:
    @classmethod
    def _filter_overview_rows_for_portfolio_candidates(
        cls,
        overview_rows: list[dict[str, Any]],
        *,
        enabled_meta_groups: list[str] | tuple[str, ...] | None = None,
        have_skills_only: bool = False,
        positive_profit_only: bool = False,
        min_margin_pct: float = 0.0,
        min_isk_per_hour: float = 0.0,
        min_region_daily_volume: int = 0,
        industry_profile_payload: dict[str, Any] | None = None,
        facility_gate_hidden: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """`facility_gate_hidden`, when given, receives {type_id, type_name,
        reason} for every row the facility gate hides."""
        enabled_meta_group_set = set(enabled_meta_groups) if enabled_meta_groups is not None else None
        filtered_rows: list[dict[str, Any]] = []
        for row in overview_rows:
            if enabled_meta_group_set is not None and cls._normalized_overview_meta_group_name(row) not in enabled_meta_group_set:
                continue
            if have_skills_only and not cls._overview_row_skill_requirements_met(row):
                continue
            if industry_profile_payload is not None:
                gate = cls._facility_gate(
                    manufacturing_group=row.get("manufacturing_group"),
                    profile_payload=industry_profile_payload, group_id=row.get("group_id"),
                )
                cls._warn_gate_once(FacilitiesMixin._facility_gate_warned, "Facility gate", gate)
                if not gate.allowed:
                    if facility_gate_hidden is not None:
                        facility_gate_hidden.append(
                            {"type_id": row.get("type_id"), "type_name": row.get("type_name"), "reason": gate.reason})
                    continue
            if positive_profit_only and float(row.get("profit_amount") or 0.0) <= 0.0:
                continue
            if float(row.get("profit_margin_fraction") or 0.0) < (float(min_margin_pct or 0.0) / 100.0):
                continue
            if float(row.get("isk_per_hour") or 0.0) < float(min_isk_per_hour or 0.0):
                continue
            if int(row.get("region_daily_volume") or 0) < int(min_region_daily_volume or 0):
                continue
            filtered_rows.append(row)
        return filtered_rows

    def _score_inventory_allocation_priority(self, product_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Score inventory allocation priority based on ISK/hr ranking (#3).

        Products that consume owned inventory are ranked by profitability.
        Products using owned materials but with lower ISK/hr than other products
        that need the same materials get flagged with an inventory_priority_rank.
        """
        if not product_rows:
            return product_rows

        material_consumers: dict[int, list[tuple[int, float]]] = {}
        for row_index, row in enumerate(product_rows):
            if not isinstance(row, dict):
                continue
            isk_per_hour = self._as_float(row.get("isk_per_hour")) or 0.0
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue
            procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
            if not isinstance(procurement_materials, dict):
                continue
            for material in procurement_materials.values():
                if not isinstance(material, dict):
                    continue
                if str(material.get("sourcing_strategy") or "").lower() == "take":
                    type_id = int(material.get("type_id") or 0)
                    if type_id > 0:
                        material_consumers.setdefault(type_id, []).append((row_index, isk_per_hour))

        contended_materials: set[int] = set()
        for type_id, consumers in material_consumers.items():
            if len(consumers) > 1:
                contended_materials.add(type_id)

        if not contended_materials:
            for row in product_rows:
                if isinstance(row, dict):
                    row.setdefault("inventory_allocation_optimal", True)
                    manufacturing_job = row.get("manufacturing_job")
                    if isinstance(manufacturing_job, dict):
                        manufacturing_job.setdefault("inventory_allocation_optimal", True)
            return product_rows

        ranked_rows = sorted(
            enumerate(product_rows),
            key=lambda item: -(self._as_float(item[1].get("isk_per_hour")) or 0.0),
        )
        rank_by_row_index: dict[int, int] = {row_index: rank + 1 for rank, (row_index, _) in enumerate(ranked_rows)}

        for row_index, row in enumerate(product_rows):
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue
            procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
            if not isinstance(procurement_materials, dict):
                continue

            uses_contended = False
            for material in procurement_materials.values():
                if not isinstance(material, dict):
                    continue
                if str(material.get("sourcing_strategy") or "").lower() == "take":
                    type_id = int(material.get("type_id") or 0)
                    if type_id in contended_materials:
                        consumers = material_consumers[type_id]
                        best_isk_per_hour = max(c[1] for c in consumers)
                        current_isk_per_hour = self._as_float(row.get("isk_per_hour")) or 0.0
                        if current_isk_per_hour < best_isk_per_hour:
                            uses_contended = True
                            break

            is_optimal = not uses_contended
            row["inventory_allocation_optimal"] = is_optimal
            row["inventory_priority_rank"] = rank_by_row_index.get(row_index)
            # Task 15 fix round 1 (I2): the rank orders ISK/h, an upper bound
            # on a row whose job fees are partly unknown; flagged, not changed.
            uses_upper_bound = bool(row.get("profit_is_upper_bound"))
            row["inventory_priority_uses_upper_bound"] = uses_upper_bound
            manufacturing_job["inventory_priority_uses_upper_bound"] = uses_upper_bound
            manufacturing_job["inventory_allocation_optimal"] = is_optimal
            manufacturing_job["inventory_priority_rank"] = rank_by_row_index.get(row_index)

        return product_rows

    @staticmethod
    def _portfolio_confidence_penalty(confidence: Any) -> float:
        normalized = str(confidence or "").strip().lower()
        if normalized == "high":
            return 1.0
        if normalized == "medium":
            return 0.85
        if normalized == "low":
            return 0.65
        return 0.75

    @classmethod
    def _owned_input_coverage_fraction(cls, procurement_materials: Any) -> float:
        if not isinstance(procurement_materials, dict) or not procurement_materials:
            return 0.0
        total_quantity = 0
        owned_quantity = 0
        for material in procurement_materials.values():
            if not isinstance(material, dict):
                continue
            quantity = max(0, int(material.get("quantity") or 0))
            if quantity <= 0:
                continue
            total_quantity += quantity
            price_source = str(material.get("price_source") or "").strip().lower()
            if price_source.startswith("owned_asset"):
                owned_quantity += quantity
        if total_quantity <= 0:
            return 0.0
        return float(owned_quantity) / float(total_quantity)

    @classmethod
    def _build_portfolio_candidate(
        cls,
        row: dict[str, Any],
        *,
        planning_horizon_hours: float,
    ) -> dict[str, Any]:
        manufacturing_job = row.get("manufacturing_job") or {}
        if not isinstance(manufacturing_job, dict):
            manufacturing_job = {}
        procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
        if not isinstance(procurement_materials, dict):
            procurement_materials = {}

        quantity_per_batch = max(0, int(row.get("quantity") or 0))
        total_cost = cls._as_float(manufacturing_job.get("total_cost"))
        profit_amount = cls._as_float(row.get("profit_amount"))
        profit_margin_fraction = cls._as_float(row.get("profit_margin_fraction"))
        isk_per_hour = cls._as_float(row.get("isk_per_hour"))
        total_time_seconds = cls._as_float(manufacturing_job.get("time_seconds"))
        manufacturing_time_seconds = cls._as_float(manufacturing_job.get("manufacturing_time_seconds"))
        preparation_time_seconds = cls._as_float(manufacturing_job.get("preparation_time_seconds"))
        region_daily_volume = max(0, int(row.get("region_daily_volume") or 0))
        region_daily_volume_7d_avg = cls._as_float(row.get("region_daily_volume_7d_avg"))
        confidence_penalty_factor = cls._portfolio_confidence_penalty(row.get("pricing_confidence"))
        slot_hours_per_batch = (
            float(total_time_seconds) / 3600.0
            if total_time_seconds is not None and total_time_seconds > 0
            else None
        )
        manufacturing_slot_hours_per_batch = (
            float(manufacturing_time_seconds) / 3600.0
            if manufacturing_time_seconds is not None and manufacturing_time_seconds > 0
            else slot_hours_per_batch
        )
        preparation_slot_hours_per_batch = (
            float(preparation_time_seconds) / 3600.0
            if preparation_time_seconds is not None and preparation_time_seconds > 0
            else None
        )

        horizon_days = max(0.0, float(planning_horizon_hours or 0.0)) / 24.0
        effective_daily_volume = (
            float(region_daily_volume_7d_avg)
            if region_daily_volume_7d_avg is not None and region_daily_volume_7d_avg > 0
            else float(region_daily_volume)
        )
        estimated_market_absorption_units = int(math.floor(max(0.0, effective_daily_volume) * horizon_days))
        max_batches_total = (
            int(math.floor(float(estimated_market_absorption_units) / float(quantity_per_batch)))
            if quantity_per_batch > 0 and estimated_market_absorption_units > 0
            else 0
        )
        effective_profit_per_batch = (
            float(profit_amount) * confidence_penalty_factor
            if profit_amount is not None
            else None
        )
        effective_isk_per_hour = (
            float(isk_per_hour) * confidence_penalty_factor
            if isk_per_hour is not None
            else None
        )
        owned_input_coverage_fraction = cls._owned_input_coverage_fraction(procurement_materials)

        meta_group = str(row.get("meta_group_name") or "").strip().lower()
        is_t2 = meta_group == "tech ii"
        threshold = 5 if is_t2 else 3
        bpc_count = int(row.get("bpc_count") or 0)
        has_bpo = bool(row.get("has_bpo"))

        if has_bpo and bpc_count == 0:
            bpc_status = "needed"          # Have BPO, need to run copy jobs
        elif has_bpo and bpc_count < threshold:
            bpc_status = "low_with_bpo"    # Have BPO + some copies, but below threshold
        elif has_bpo:
            bpc_status = "bpo"             # BPO owned; copies above threshold → treated as well-stocked
        elif bpc_count == 0:
            bpc_status = "invent"          # No BPO, no BPCs → need to invent (T2) or source externally
        elif bpc_count < threshold:
            bpc_status = "low"             # Some BPCs but below threshold
        else:
            bpc_status = "stocked"         # Enough BPCs

        is_portfolio_candidate = bool(
            profit_amount is not None
            and profit_amount > 0
            and total_cost is not None
            and total_cost > 0
            and quantity_per_batch > 0
            and slot_hours_per_batch is not None
            and slot_hours_per_batch > 0
            and max_batches_total > 0
        )

        # Capital Cycle ISK/day — total profit over planning horizon ÷ one capital cycle
        # Elapsed (whole-job) build time, not the amortized slot time.
        _elapsed_time_seconds = orow.get_elapsed_time_seconds(row)
        _build_days = float(_elapsed_time_seconds or 0) / 86400.0
        _effective_daily_volume = (
            float(region_daily_volume_7d_avg)
            if region_daily_volume_7d_avg is not None and float(region_daily_volume_7d_avg) > 0
            else float(region_daily_volume)
        )
        _sell_days = (
            (float(max_batches_total) * float(quantity_per_batch)) / _effective_daily_volume
            if _effective_daily_volume > 0 and quantity_per_batch > 0 and max_batches_total > 0
            else None
        )
        _cycle_days = (_build_days + _sell_days) if _sell_days is not None and _build_days > 0 else None
        isk_per_cycle_day: float | None = (
            (float(profit_amount) * float(max_batches_total)) / _cycle_days
            if _cycle_days is not None and _cycle_days > 0 and profit_amount is not None and max_batches_total > 0
            else None
        )

        return {
            "overview_row_id": row.get("overview_row_id"),
            "type_id": int(row.get("type_id") or 0),
            "type_name": row.get("type_name"),
            "category_name": row.get("category_name") or row.get("type_category_name"),
            "meta_group_name": row.get("meta_group_name"),
            "quantity_per_batch": quantity_per_batch,
            "profit_amount": profit_amount,
            "profit_margin_fraction": profit_margin_fraction,
            "isk_per_hour": isk_per_hour,
            "net_proceeds": cls._as_float(row.get("net_proceeds")),
            "cash_outlay_per_batch": total_cost,
            "material_cost": cls._as_float(manufacturing_job.get("material_cost")),
            "total_job_cost": cls._as_float(manufacturing_job.get("total_job_cost")),
            "slot_hours_per_batch": slot_hours_per_batch,
            "manufacturing_slot_hours_per_batch": manufacturing_slot_hours_per_batch,
            "preparation_slot_hours_per_batch": preparation_slot_hours_per_batch,
            "time_seconds": total_time_seconds,
            "elapsed_time_seconds": _elapsed_time_seconds,
            "manufacturing_time_seconds": manufacturing_time_seconds,
            "preparation_time_seconds": preparation_time_seconds,
            # Unknown (no region history) stays None; the absorption above then assumes nothing sellable.
            "region_daily_volume": None if row.get("region_daily_volume") is None else region_daily_volume,
            "region_daily_volume_7d_avg": region_daily_volume_7d_avg,
            "region_daily_volume_unavailable_reason": row.get("region_daily_volume_unavailable_reason"),
            # Unknown hub liquidity (a failed order-book fetch) stays None, with its reason.
            "hub_buy_liquidity": cls._optional_int(row, "hub_buy_liquidity"),
            "hub_sell_liquidity": cls._optional_int(row, "hub_sell_liquidity"),
            "liquidity_unavailable_reason": row.get("liquidity_unavailable_reason"),
            "price_unavailable_reason": row.get("price_unavailable_reason"),
            "days_of_supply": cls._calculate_days_of_supply(
                hub_sell_liquidity=cls._optional_int(row, "hub_sell_liquidity"),
                region_daily_volume_7d_avg=cls._as_float(region_daily_volume_7d_avg),
            ),
            "sell_through_rate": cls._calculate_sell_through_rate(
                region_daily_volume_7d_avg=cls._as_float(region_daily_volume_7d_avg),
                hub_sell_liquidity=cls._optional_int(row, "hub_sell_liquidity"),
            ),
            "liquidity_indicator": cls._calculate_liquidity_indicator(
                hub_sell_liquidity=cls._optional_int(row, "hub_sell_liquidity"),
                region_daily_volume_7d_avg=cls._as_float(region_daily_volume_7d_avg),
                hub_sell_orders=cls._optional_int(row, "hub_sell_order_count"),
            ),
            "liquidity_score": cls._calculate_liquidity_score(
                hub_sell_liquidity=cls._optional_int(row, "hub_sell_liquidity"),
                region_daily_volume_7d_avg=cls._as_float(region_daily_volume_7d_avg),
                hub_sell_orders=cls._optional_int(row, "hub_sell_order_count"),
            ),
            "pricing_confidence": row.get("pricing_confidence"),
            "pricing_confidence_reasons": row.get("pricing_confidence_reasons") or [],
            # Task 15 fix round 1: its profit is an upper bound (job fees unknown).
            "profit_is_upper_bound": bool(row.get("profit_is_upper_bound")),
            "market_price_age_minutes": cls._as_float(row.get("market_price_age_minutes")),
            "estimated_market_absorption_units": estimated_market_absorption_units,
            "max_batches_total": max_batches_total,
            "blueprint_source_kind": manufacturing_job.get("blueprint_source_kind"),
            "manufacturing_group": row.get("manufacturing_group"),
            "skill_requirements_met": cls._overview_row_skill_requirements_met(row),
            "owned_input_coverage_fraction": owned_input_coverage_fraction,
            "confidence_penalty_factor": confidence_penalty_factor,
            "effective_profit_per_batch": effective_profit_per_batch,
            "effective_isk_per_hour": effective_isk_per_hour,
            "is_portfolio_candidate": is_portfolio_candidate,
            "bpc_count": bpc_count,
            "has_bpo": has_bpo,
            "bpc_status": bpc_status,
            "bpc_threshold": threshold,
            "isk_per_cycle_day": isk_per_cycle_day,
        }

    @classmethod
    def _build_portfolio_candidates(
        cls,
        rows: list[dict[str, Any]],
        *,
        planning_horizon_hours: float,
    ) -> list[dict[str, Any]]:
        candidates = [
            cls._build_portfolio_candidate(row, planning_horizon_hours=planning_horizon_hours)
            for row in rows
            if isinstance(row, dict)
        ]
        return sorted(
            candidates,
            key=lambda candidate: (
                float(candidate.get("effective_profit_per_batch") or 0.0),
                float(candidate.get("effective_isk_per_hour") or 0.0),
            ),
            reverse=True,
        )

    def industry_manufacturing_portfolio_candidates_payload(
        self,
        *,
        force_refresh: bool = False,
        maximize_bp_runs: bool = False,
        group_identical_bpcs: bool = True,
        build_from_bpc: bool = True,
        have_blueprint_source_only: bool = True,
        include_reactions: bool = False,
        market_hub: str = "jita",
        material_price_side: str = "sell",
        product_price_side: str = "sell",
        industry_profile_id: int | None = None,
        owned_blueprints_scope: str = "all_characters",
        character_id: int | None = None,
        planning_horizon_hours: float = 24.0,
        enabled_meta_groups: list[str] | tuple[str, ...] | None = None,
        have_skills_only: bool = False,
        positive_profit_only: bool = False,
        min_margin_pct: float = 0.0,
        min_isk_per_hour: float = 0.0,
        min_region_daily_volume: int = 0,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        if progress_callback is not None:
            progress_callback(
                0.03,
                "Step 2/11: Preparing portfolio candidate request",
                {"step": 2, "step_count": 11, "stage": "startup"},
            )
        overview_payload = self.industry_manufacturing_product_overview_payload(
            force_refresh=force_refresh,
            maximize_bp_runs=maximize_bp_runs,
            group_identical_bpcs=group_identical_bpcs,
            build_from_bpc=build_from_bpc,
            have_blueprint_source_only=have_blueprint_source_only,
            include_reactions=include_reactions,
            market_hub=market_hub,
            material_price_side=material_price_side,
            product_price_side=product_price_side,
            industry_profile_id=industry_profile_id,
            owned_blueprints_scope=owned_blueprints_scope,
            character_id=character_id,
            progress_callback=(
                None
                if progress_callback is None
                else lambda progress_fraction, progress_label, progress_meta=None: progress_callback(
                    0.05 + (0.82 * float(progress_fraction)),
                    str(progress_label),
                    {
                        **(dict(progress_meta) if isinstance(progress_meta, dict) else {}),
                        "step_count": 11,
                    },
                )
            ),
        )
        facility_gate_hidden: list[dict[str, Any]] = []
        industry_profile_payload = self._resolve_industry_profile_context(
            character_id=character_id,
            industry_profile_id=industry_profile_id,
        )
        rows = self._filter_overview_rows_for_portfolio_candidates(
            cast(list[dict[str, Any]], overview_payload.get("rows") or []),
            facility_gate_hidden=facility_gate_hidden,
            enabled_meta_groups=enabled_meta_groups,
            have_skills_only=have_skills_only,
            positive_profit_only=positive_profit_only,
            min_margin_pct=min_margin_pct,
            min_isk_per_hour=min_isk_per_hour,
            min_region_daily_volume=min_region_daily_volume,
            industry_profile_payload=industry_profile_payload,
        )
        if progress_callback is not None:
            progress_callback(
                0.90,
                "Step 10/11: Building portfolio candidates",
                {"step": 10, "step_count": 11, "stage": "candidates", "rows": len(rows)},
            )
        candidates = self._build_portfolio_candidates(rows, planning_horizon_hours=planning_horizon_hours)
        if progress_callback is not None:
            progress_callback(
                1.0,
                "Step 11/11: Portfolio candidates ready",
                {
                    "step": 11,
                    "step_count": 11,
                    "stage": "completed",
                    "rows": len(rows),
                    "candidate_count": len(candidates),
                },
            )
        return {
            "rows": rows,
            "candidates": candidates,
            "pricing_batch": overview_payload.get("pricing_batch") or {},
            "summary": {
                "row_count": len(rows),
                "candidate_count": len(candidates),
                "portfolio_candidate_count": sum(1 for candidate in candidates if bool(candidate.get("is_portfolio_candidate"))),
                "planning_horizon_hours": float(planning_horizon_hours or 0.0),
                # Rows the facility gate hid, each with why (e.g. "needs
                # Standup Capital Shipyard I: ...").
                "facility_gate_hidden_count": len(facility_gate_hidden),
                **({"facility_gate_applied": True} if industry_profile_payload is not None else {
                    # No profile: the overview is facility-agnostic and
                    # nothing is hidden, capitals included.
                    "facility_gate_applied": False,
                    "facility_gate_not_applied_reason": "no industry profile",
                }),
            },
            "facility_gate_hidden": facility_gate_hidden,
        }
