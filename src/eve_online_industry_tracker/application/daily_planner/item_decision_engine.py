"""ItemDecisionEngine — Phase 4: decide build / watch / pause / skip for each item.

Two-pass ordering:
  Pass 1: Run Phases 2, 3, 4 for ALL top-level items → get decisions.
  Pass 2: ChainPlanner resolves sub-components (assigns decision='build' directly, bypasses this engine).
"""
from __future__ import annotations

import logging
from typing import Any

from eve_online_industry_tracker.application.daily_planner.models import (
    ItemDecision,
    PipelineState,
    ScoredItem,
)

logger = logging.getLogger(__name__)


class ItemDecisionEngine:
    """Phase 4: assign a decision to each scored item."""

    def decide(
        self,
        scored: ScoredItem,
        pipeline: PipelineState,
        overview_row: dict[str, Any],
        admin_settings: Any,
        meta_group_id: int | None = None,
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
                meta_group_id=meta_group_id,
                pipeline_stage="unscoreable",
                overview_row=overview_row,
            )

        min_isk_per_hour = float(_adm(admin_settings, "planner_min_isk_per_hour", 5_000_000))
        min_profit_per_batch = float(_adm(admin_settings, "planner_min_profit_per_batch", 20_000_000))
        competition_gate = float(_adm(admin_settings, "planner_competition_index_gate", 4.0))
        momentum_pause_threshold = float(_adm(admin_settings, "planner_momentum_pause_threshold", -6.0))

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
                meta_group_id=meta_group_id,
                pipeline_stage="manufacturing",
                overview_row=overview_row,
            )

        # ── SKIP check (score or profit too low) ──────────────────────────────
        if scored.adjusted_score <= min_isk_per_hour or scored.absolute_profit_per_batch <= min_profit_per_batch:
            if scored.adjusted_score <= min_isk_per_hour:
                reason = f"Score {scored.adjusted_score/1e6:.1f}M ISK/hr below {min_isk_per_hour/1e6:.1f}M threshold"
            else:
                reason = f"Batch profit {scored.absolute_profit_per_batch/1e6:.1f}M ISK below {min_profit_per_batch/1e6:.1f}M threshold"
            # Special case: declining trend with no active jobs → skip not pause
            if price_trend_7d < -8.0 or momentum_signal < momentum_pause_threshold:
                reason += "; declining market (no active jobs)"
            return ItemDecision(
                type_id=type_id,
                type_name=type_name,
                decision="skip",
                decision_reason=reason,
                adjusted_score=scored.adjusted_score,
                absolute_profit_per_batch=scored.absolute_profit_per_batch,
                isk_per_hour=scored.isk_per_hour,
                margin_pct=scored.margin_pct,
                days_of_supply_current=pipeline.days_of_supply_current,
                effective_velocity=pipeline.effective_velocity,
                meta_group_id=meta_group_id,
                pipeline_stage="watching",
                overview_row=overview_row,
            )

        # ── WATCH check (pipeline saturated or market flooded) ────────────────
        # competition_index gate: only applied if competition_index is not None (Phase A: skip gate)
        competition_flooded = (
            competition_index is not None and competition_index >= competition_gate
        )
        pipeline_saturated = pipeline_days >= 3.0

        if pipeline_saturated or competition_flooded:
            reasons = []
            if pipeline_saturated:
                reasons.append(f"{pipeline_days:.1f}d of supply (≥ 3d)")
            if competition_flooded:
                reasons.append(f"competition index {competition_index:.1f} ≥ {competition_gate:.1f}")
            reason = "Watch: " + ", ".join(reasons)
            # The overview row carries no pipeline stage or in-flight flags
            # (pipeline_stage / invention_in_flight / bpc_in_flight /
            # copy_in_flight are written by no producer), so the old
            # inference always fell through to "watching". Say so directly.
            pipeline_stage = "watching"
            return ItemDecision(
                type_id=type_id,
                type_name=type_name,
                decision="watch",
                decision_reason=reason,
                adjusted_score=scored.adjusted_score,
                absolute_profit_per_batch=scored.absolute_profit_per_batch,
                isk_per_hour=scored.isk_per_hour,
                margin_pct=scored.margin_pct,
                days_of_supply_current=pipeline.days_of_supply_current,
                effective_velocity=pipeline.effective_velocity,
                meta_group_id=meta_group_id,
                pipeline_stage=pipeline_stage,
                overview_row=overview_row,
            )

        # ── BUILD ─────────────────────────────────────────────────────────────
        reason = (
            f"Build: score {scored.adjusted_score/1e6:.1f}M ISK/hr, "
            f"pipeline {pipeline_days:.1f}d < 3d"
        )
        return ItemDecision(
            type_id=type_id,
            type_name=type_name,
            decision="build",
            decision_reason=reason,
            adjusted_score=scored.adjusted_score,
            absolute_profit_per_batch=scored.absolute_profit_per_batch,
            isk_per_hour=scored.isk_per_hour,
            margin_pct=scored.margin_pct,
            days_of_supply_current=pipeline.days_of_supply_current,
            effective_velocity=pipeline.effective_velocity,
            meta_group_id=meta_group_id,
            pipeline_stage="manufacturing",
            overview_row=overview_row,
        )


def _adm(admin_settings: Any, key: str, fallback: Any) -> Any:
    """A daily_planner setting, or `fallback` when there is no settings store.

    AttributeError covers stub/None admin objects; KeyError is what
    AdminSettingsManager.get raises for an unknown key (every key read here is
    pinned to the schema by tests/test_daily_planner_fail_loud.py).
    """
    try:
        return admin_settings.get("daily_planner", key)
    except (AttributeError, KeyError):
        return fallback
