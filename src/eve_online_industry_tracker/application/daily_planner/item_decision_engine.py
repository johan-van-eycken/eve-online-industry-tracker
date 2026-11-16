"""ItemDecisionEngine — Phase 4: decide build / watch / pause / skip for each item.

Two-pass ordering:
  Pass 1: Run Phases 2, 3, 4 for ALL top-level items → get decisions.
  Pass 2: ChainPlanner resolves sub-components (assigns decision='build' directly, bypasses this engine).
"""
from __future__ import annotations

import logging
import math
from typing import Any

from eve_online_industry_tracker.application.daily_planner.admin_setting import planner_setting as _adm
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.daily_planner.models import (
    ItemDecision,
    PipelineState,
    ScoredItem,
)

logger = logging.getLogger(__name__)


def market_cap_runs(
    *, batch_runs: int, units_per_run: float, velocity: float, pipeline_units: float, max_days: float,
) -> int:
    """Runs of a batch the market can absorb within max_days, given what is already in the pipeline."""
    room = math.floor((velocity * max_days - pipeline_units) / units_per_run)
    return max(0, min(batch_runs, room))


class ItemDecisionEngine:
    """Phase 4: assign a decision to each scored item."""

    def decide(
        self,
        scored: ScoredItem,
        pipeline: PipelineState,
        overview_row: dict[str, Any],
        admin_settings: Any,
        meta_group_id: int | None = None,
        margin_known: bool = True,
    ) -> ItemDecision:
        """_decide's decision; when the row's profit is an upper bound (job
        fees unknown) its reason says so. The scores
        stay on that bound, labelled."""
        decision = self._decide(scored, pipeline, overview_row, admin_settings, meta_group_id, margin_known)
        upper_bound = orow.get_profit_upper_bound_reason(overview_row)
        if upper_bound is not None:
            decision.profit_is_upper_bound = True
            if upper_bound not in (decision.decision_reason or ""):
                decision.decision_reason = f"{decision.decision_reason}; {upper_bound}"
        return decision

    def _decide(
        self,
        scored: ScoredItem,
        pipeline: PipelineState,
        overview_row: dict[str, Any],
        admin_settings: Any,
        meta_group_id: int | None = None,
        margin_known: bool = True,
    ) -> ItemDecision:
        """Apply Phase 4 decision rules and return an ItemDecision.

        `meta_group_id` is the numeric id resolved from the SDE (see
        `PlannerInputRow.meta_group_id` / `TypeMetadataResolver.meta_group_id`),
        passed in explicitly by the caller. The overview row itself never
        carries a numeric meta group id -- only `meta_group_name` -- so this
        engine must not try to read one off it.
        """
        type_id = scored.type_id
        type_name = str(overview_row.get("type_name") or "")

        # ── UNSCOREABLE check (must precede every threshold comparison and
        # every /1e6 arithmetic below) ─────────────────────────────────────────
        # ProfitabilityScorer sets this when it had no cost basis and/or no
        # isk/hour to work with. Its numeric fields are then placeholder
        # 0.0s, not genuine measurements -- comparing them against the
        # thresholds below (or formatting them) would read as "confirmed
        # unprofitable", a false and more specific claim than "we could not
        # price this item at all". Skip immediately instead, carrying the
        # scorer's reason through unchanged.
        if scored.unscoreable_reason is not None:
            return ItemDecision(
                type_id=type_id,
                type_name=type_name,
                decision="skip",
                decision_reason=scored.unscoreable_reason,
                adjusted_score=scored.adjusted_score,
                absolute_profit_per_batch=scored.absolute_profit_per_batch,
                isk_per_hour=scored.isk_per_hour,
                margin_pct=scored.margin_pct,
                days_of_supply_current=pipeline.days_of_supply_current,
                effective_velocity=pipeline.effective_velocity,
                velocity_unknown_reason=pipeline.velocity_unknown_reason,
                meta_group_id=meta_group_id,
                pipeline_stage="unscoreable",
                overview_row=overview_row,
            )

        competition_gate = float(_adm(admin_settings, "planner_competition_index_gate"))
        momentum_pause_threshold = float(_adm(admin_settings, "planner_momentum_pause_threshold"))

        price_trend_7d = pipeline.price_trend_7d_pct
        momentum_signal = pipeline.momentum_signal
        pipeline_days = pipeline.total_pipeline_days
        competition_index = pipeline.competition_index
        has_active_jobs = pipeline.has_active_manufacturing_jobs

        # ── PAUSE check (hard override regardless of score) ───────────────────
        # Requires active in-flight jobs AND (declining trend OR bad momentum)
        if has_active_jobs and (price_trend_7d < -8.0 or momentum_signal < momentum_pause_threshold):
            reason_parts = []
            if price_trend_7d < -8.0:
                reason_parts.append(f"price trend {price_trend_7d:.1f}% < -8%")
            if momentum_signal < momentum_pause_threshold:
                reason_parts.append(f"momentum signal {momentum_signal:.1f} < {momentum_pause_threshold:.1f}")
            reason = "Pause: active jobs + declining market (" + ", ".join(reason_parts) + ")"
            return ItemDecision(
                type_id=type_id,
                type_name=type_name,
                decision="pause",
                decision_reason=reason,
                adjusted_score=scored.adjusted_score,
                absolute_profit_per_batch=scored.absolute_profit_per_batch,
                isk_per_hour=scored.isk_per_hour,
                margin_pct=scored.margin_pct,
                days_of_supply_current=pipeline.days_of_supply_current,
                effective_velocity=pipeline.effective_velocity,
                velocity_unknown_reason=pipeline.velocity_unknown_reason,
                meta_group_id=meta_group_id,
                pipeline_stage="manufacturing",
                overview_row=overview_row,
            )

        # ── Build rules: margin, sell time, market cap, capital return ────────
        min_margin = float(_adm(admin_settings, "planner_min_margin_pct"))
        max_days = float(_adm(admin_settings, "planner_max_days_to_sell"))
        velocity = pipeline.effective_velocity
        pipeline_units = pipeline_days * velocity
        declining = price_trend_7d < -8.0 or momentum_signal < momentum_pause_threshold
        computed: dict[str, Any] = {}

        def _result(decision: str, reason: str) -> ItemDecision:
            if decision == "skip" and declining:
                reason += "; declining market (no active jobs)"
            return ItemDecision(
                type_id=type_id,
                type_name=type_name,
                decision=decision,
                decision_reason=reason,
                adjusted_score=scored.adjusted_score,
                absolute_profit_per_batch=scored.absolute_profit_per_batch,
                isk_per_hour=scored.isk_per_hour,
                margin_pct=scored.margin_pct,
                days_of_supply_current=pipeline.days_of_supply_current,
                effective_velocity=velocity,
                velocity_unknown_reason=pipeline.velocity_unknown_reason,
                meta_group_id=meta_group_id,
                # The overview row carries no pipeline stage or in-flight flags
                # (no producer writes them), so a non-build is always "watching".
                pipeline_stage="manufacturing" if decision == "build" else "watching",
                overview_row=overview_row,
                **computed,
            )

        if pipeline.velocity_unknown_reason is not None:
            return _result("skip", f"not built: sell velocity unknown ({pipeline.velocity_unknown_reason})")
        if not margin_known:
            return _result("skip", "not built: margin unknown")
        if scored.margin_pct < min_margin:
            return _result("skip", f"margin {scored.margin_pct:.1f}% below {min_margin:g}%")

        batch_runs = orow.get_manufacturing_job(overview_row).get("runs")
        if not isinstance(batch_runs, int) or isinstance(batch_runs, bool) or batch_runs <= 0:
            return _result("skip", "not built: batch runs unknown")
        quantity = orow.get_product_quantity(overview_row)
        if quantity <= 0:
            return _result("skip", "not built: product quantity unknown")
        units_per_run = quantity / batch_runs
        capital = orow.get_total_cost(overview_row)
        if capital is None or capital <= 0:
            return _result("skip", "not built: batch cost unknown")

        # ── WATCH check (market flooded, or no room left to sell a batch) ─────
        # competition_index gate: only applied if competition_index is not None (Phase A: skip gate)
        if competition_index is not None and competition_index >= competition_gate:
            return _result("watch", f"Watch: competition index {competition_index:.1f} ≥ {competition_gate:.1f}")

        cap = market_cap_runs(
            batch_runs=batch_runs, units_per_run=units_per_run, velocity=velocity,
            pipeline_units=pipeline_units, max_days=max_days,
        )
        computed["market_cap_runs"] = cap
        if cap == 0:
            return _result("watch", f"market full: {pipeline_days:.0f} days of supply already, max {max_days:g}")

        # ── BUILD ─────────────────────────────────────────────────────────────
        expected_days = (pipeline_units + cap * units_per_run) / velocity
        # Runs cancel out: the full batch's profit and cost give the capped batch's ratio.
        crpd = (scored.absolute_profit_per_batch / capital) / max(expected_days, 1.0)
        computed["expected_days_to_sell"] = expected_days
        computed["capital_return_per_day"] = crpd
        return _result(
            "build",
            f"Build: margin {scored.margin_pct:.1f}%, {expected_days:.0f} days to sell, {100 * crpd:.2f}%/day on capital",
        )
