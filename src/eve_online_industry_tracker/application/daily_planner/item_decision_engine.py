"""ItemDecisionEngine — Phase 4: decide build / watch / pause / skip for each item.

Two-pass ordering:
  Pass 1: Run Phases 2, 3, 4 for ALL top-level items → get decisions.
  Pass 2: ChainPlanner resolves sub-components (assigns decision='build' directly, bypasses this engine).
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any

from eve_online_industry_tracker.application.daily_planner.admin_setting import planner_setting as _adm
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.daily_planner.models import (
    ItemDecision,
    PipelineState,
    ScoredItem,
)

logger = logging.getLogger(__name__)


def market_room_runs(*, units_per_run: float, velocity: float, pipeline_units: float, max_days: float) -> int:
    """Runs the market can absorb within max_days, given what is already in the pipeline."""
    return max(0, math.floor((velocity * max_days - pipeline_units) / units_per_run))


def market_cap_runs(
    *, batch_runs: int, units_per_run: float, velocity: float, pipeline_units: float, max_days: float,
) -> int:
    """Runs of one batch the market can absorb within max_days (market_room_runs, at most the batch)."""
    return min(batch_runs, market_room_runs(
        units_per_run=units_per_run, velocity=velocity, pipeline_units=pipeline_units, max_days=max_days))


DAY_SECONDS = 86_400
#: A check closer than this to the plan is today's own session: the job aims for the next one.
_MIN_HOURS_TO_FIRST_CHECK = 12


def check_deadlines(now: datetime, check_hour: int, max_days: int) -> list[float]:
    """Seconds from `now` to each of the next `max_days` daily checks at
    `check_hour` local time (this machine's time zone, DST included); the
    first at least _MIN_HOURS_TO_FIRST_CHECK away. A naive `now` is UTC
    (the planner's utc_now)."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    day = now.astimezone().date()
    out: list[float] = []
    while len(out) < max_days:
        check = datetime(day.year, day.month, day.day, check_hour).astimezone()
        gap = (check - now).total_seconds()
        if gap >= _MIN_HOURS_TO_FIRST_CHECK * 3600:
            out.append(gap)
        day += timedelta(days=1)
    return out


def day_sized_runs(*, seconds_per_run: float, room_runs: int, deadlines: list[float]) -> int | None:
    """Runs of a BPO job that ends just before one of the next daily checks
    (`deadlines`, check_deadlines), the latest the market room allows; the
    room when even the first check fits more. None when no run fits before
    the last check or the market has no room."""
    if seconds_per_run <= 0 or room_runs < 1:
        return None
    for gap in reversed(deadlines):
        runs = math.floor(gap / seconds_per_run)
        if 1 <= runs <= room_runs:
            return runs
    if deadlines and math.floor(deadlines[0] / seconds_per_run) > room_runs:
        return room_runs
    return None


def batch_fraction(decision: ItemDecision, runs: int | None) -> float | None:
    batch = orow.get_manufacturing_job(decision.overview_row).get("runs")
    if runs is None or not isinstance(batch, int) or isinstance(batch, bool) or batch <= 0:
        return None
    return runs / batch


def slot_day_profit(decision: ItemDecision, runs: int | None) -> float:
    """The job's profit per day it holds a slot: jobs are checked once a day,
    so a job holds its slot for whole days, at least one. 0 when unknown."""
    fraction = batch_fraction(decision, runs)
    seconds = orow.get_elapsed_time_seconds(decision.overview_row)
    if fraction is None or not seconds or seconds <= 0:
        return 0.0
    days = max(1, math.ceil(seconds * fraction / DAY_SECONDS))
    return (getattr(decision, "absolute_profit_per_batch", None) or 0.0) * fraction / days


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
        decision.accuracy_ema = float(getattr(scored, "accuracy_ema", 1.0) or 1.0)
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
                momentum_signal=pipeline.momentum_signal,
                meta_group_id=meta_group_id,
                pipeline_stage="unscoreable",
                overview_row=overview_row,
            )

        momentum_pause_threshold = float(_adm(admin_settings, "planner_momentum_pause_threshold"))

        price_trend_7d = pipeline.price_trend_7d_pct
        momentum_signal = pipeline.momentum_signal
        pipeline_days = pipeline.total_pipeline_days
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
                momentum_signal=pipeline.momentum_signal,
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
                momentum_signal=pipeline.momentum_signal,
                meta_group_id=meta_group_id,
                # The overview row carries no pipeline stage or in-flight flags
                # (no producer writes them), so a non-build is always "watching".
                pipeline_stage="manufacturing" if decision == "build" else "watching",
                overview_row=overview_row,
                **computed,
            )

        if declining:
            # No active jobs (those pause above): no first batch into a falling market either.
            parts = ([f"price trend {price_trend_7d:.1f}% < -8%"] if price_trend_7d < -8.0 else []) + (
                [f"momentum signal {momentum_signal:.1f} < {momentum_pause_threshold:.1f}"]
                if momentum_signal < momentum_pause_threshold else [])
            return _result("watch", "Watch: declining market (" + ", ".join(parts) + ")")
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

        # ── WATCH check (no room left to sell a batch) ────────────────────────
        # No competition-index gate: it divides the hub's whole sell stock by our
        # own sales, so it flagged ~300 of 330 items; the sell-time cap below
        # already uses our real sell rate, which competition is part of.
        cap = market_cap_runs(
            batch_runs=batch_runs, units_per_run=units_per_run, velocity=velocity,
            pipeline_units=pipeline_units, max_days=max_days,
        )
        computed["market_cap_runs"] = cap
        computed["market_room_runs"] = market_room_runs(
            units_per_run=units_per_run, velocity=velocity, pipeline_units=pipeline_units, max_days=max_days,
        )
        if cap == 0:
            return _result("watch", f"market full: {pipeline_days:.0f} days of supply already, max {max_days:g}")

        # ── BUILD ─────────────────────────────────────────────────────────────
        expected_days = (pipeline_units + cap * units_per_run) / velocity
        # Runs cancel out: the full batch's profit and cost give the capped batch's ratio.
        crpd = (scored.absolute_profit_per_batch / capital) / max(expected_days, 1.0)
        computed["expected_days_to_sell"] = expected_days
        computed["capital_return_per_day"] = crpd
        if crpd <= 0:
            return _result("skip", f"not built: no return on capital at the recent sale price ({100 * crpd:.2f}%/day)")
        return _result(
            "build",
            f"margin {scored.margin_pct:.1f}%, {expected_days:.0f} days to sell, {100 * crpd:.2f}%/day on capital",
        )
