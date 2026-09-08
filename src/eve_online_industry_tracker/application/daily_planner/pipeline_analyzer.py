"""PipelineAnalyzer — Phase 2: compute pipeline state per item.

Takes IndustryService overview rows, active industry jobs, corp assets,
market depth cache, and learning weights; returns one PipelineState per item.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.application.daily_planner.models import PipelineState

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


class PipelineAnalyzer:
    """Phase 2: compute pipeline state per manufacturable item."""

    def analyze(
        self,
        overview_rows: list[dict[str, Any]],
        industry_jobs: list[Any],
        corp_assets: list[Any],
        market_depth_cache: dict[int, Any],  # keyed by type_id; values are MarketDepthCacheModel or dict
        weights: dict[int, Any],              # keyed by type_id; values are PlanLearningWeightsModel
        sell_velocities: dict[int, float],    # type_id → sell_velocity_per_day (from SalesHistoryService)
    ) -> list[PipelineState]:
        """Return one PipelineState for each overview row."""
        now = _now()
        result: list[PipelineState] = []

        # Index active manufacturing jobs by product type_id for quick lookup
        mfg_jobs_by_type: dict[int, list[Any]] = {}
        for job in industry_jobs:
            activity_id = _job_attr(job, "activity_id") or _job_attr(job, "activityID")
            # activity_id 1 = manufacturing
            if int(activity_id or 0) != 1:
                continue
            product_type_id = _job_attr(job, "product_type_id") or _job_attr(job, "productTypeID")
            if product_type_id is None:
                continue
            tid = int(product_type_id)
            mfg_jobs_by_type.setdefault(tid, []).append(job)

        # Index BPC assets by product type_id
        bpc_runs_by_type: dict[int, int] = {}
        for asset in corp_assets:
            # BPCs have is_blueprint=True and runs > 0
            if not _asset_attr_bool(asset, "is_blueprint"):
                continue
            runs = int(_asset_attr(asset, "runs") or 0)
            if runs <= 0:
                continue
            type_id = int(_asset_attr(asset, "type_id") or 0)
            if type_id <= 0:
                continue
            bpc_runs_by_type[type_id] = bpc_runs_by_type.get(type_id, 0) + runs

        for row in overview_rows:
            type_id = int(row.get("type_id") or 0)
            if type_id <= 0:
                continue

            try:
                state = self._analyze_single(
                    row=row,
                    type_id=type_id,
                    mfg_jobs_by_type=mfg_jobs_by_type,
                    bpc_runs_by_type=bpc_runs_by_type,
                    market_depth_cache=market_depth_cache,
                    weights=weights,
                    sell_velocities=sell_velocities,
                    now=now,
                )
                result.append(state)
            except Exception:
                logger.exception("PipelineAnalyzer: error analyzing type_id=%s", type_id)

        return result

    def _analyze_single(
        self,
        *,
        row: dict[str, Any],
        type_id: int,
        mfg_jobs_by_type: dict[int, list[Any]],
        bpc_runs_by_type: dict[int, int],
        market_depth_cache: dict[int, Any],
        weights: dict[int, Any],
        sell_velocities: dict[int, float],
        now: datetime,
    ) -> PipelineState:
        # ── Velocity ──────────────────────────────────────────────────────────
        sell_velocity_per_day = float(sell_velocities.get(type_id, 0.0))
        velocity_multiplier = 1.0
        w = weights.get(type_id)
        if w is not None:
            velocity_multiplier = float(getattr(w, "velocity_multiplier", 1.0))

        days_of_supply = float(row.get("days_of_supply") or 0.0)

        if sell_velocity_per_day > 0.0:
            effective_velocity = max(0.01, sell_velocity_per_day * velocity_multiplier)
        else:
            # Fallback: 1/days_of_supply, floored at 0.01
            fallback = (1.0 / days_of_supply) if days_of_supply > 0.0 else 0.01
            effective_velocity = max(0.01, fallback)

        # ── Pipeline days ─────────────────────────────────────────────────────
        corp_stock_units = float(row.get("corp_stock_qty") or row.get("corp_stock_units") or 0.0)
        units_in_mfg = _units_in_manufacturing(mfg_jobs_by_type.get(type_id, []))
        units_on_market = float(row.get("units_on_market") or row.get("market_units") or 0.0)

        total_pipeline_days = (corp_stock_units + units_in_mfg + units_on_market) / effective_velocity

        # ── Competition (from pre-computed cache, Phase A = None) ─────────────
        cache_entry = market_depth_cache.get(type_id)
        competition_index: float | None = None
        competitor_units: int = 0
        if cache_entry is not None:
            competition_index = _get_attr(cache_entry, "competition_index")
            competitor_units = int(_get_attr(cache_entry, "competitor_units") or 0)

        # ── BPC runs available ────────────────────────────────────────────────
        # The overview row's blueprint type_id vs the product — BPC stock is keyed
        # by blueprint type_id in assets; use row["blueprint_type_id"] or fall back
        # to looking up via product type_id.
        bp_type_id = int(row.get("blueprint_type_id") or 0)
        bpc_runs_available = bpc_runs_by_type.get(bp_type_id, 0)

        # ── Momentum signal ───────────────────────────────────────────────────
        price_trend_7d = float(
            row.get("price_trend_7d_pct") or row.get("price_trend_pct") or 0.0
        )
        price_trend_30d_raw = row.get("price_trend_30d_pct")
        price_trend_30d: float | None = (
            float(price_trend_30d_raw) if price_trend_30d_raw is not None else None
        )
        trend_30d_for_signal = price_trend_30d if price_trend_30d is not None else 0.0
        momentum_signal = price_trend_7d - (trend_30d_for_signal / 4.0)

        # ── Active manufacturing jobs flag ────────────────────────────────────
        active_jobs = mfg_jobs_by_type.get(type_id, [])
        has_active_manufacturing_jobs = len(active_jobs) > 0

        return PipelineState(
            type_id=type_id,
            effective_velocity=effective_velocity,
            total_pipeline_days=total_pipeline_days,
            competition_index=competition_index,
            competitor_units=competitor_units,
            bpc_runs_available=bpc_runs_available,
            momentum_signal=momentum_signal,
            days_of_supply_current=days_of_supply,
            price_trend_7d_pct=price_trend_7d,
            price_trend_30d_pct=price_trend_30d,
            has_active_manufacturing_jobs=has_active_manufacturing_jobs,
        )


def _job_attr(job: Any, attr: str) -> Any:
    """Get attribute from job object (supports both dict and ORM object)."""
    if isinstance(job, dict):
        return job.get(attr)
    return getattr(job, attr, None)


def _asset_attr(asset: Any, attr: str) -> Any:
    if isinstance(asset, dict):
        return asset.get(attr)
    return getattr(asset, attr, None)


def _asset_attr_bool(asset: Any, attr: str) -> bool:
    v = _asset_attr(asset, attr)
    if v is None:
        return False
    return bool(v)


def _get_attr(obj: Any, attr: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(attr)
    return getattr(obj, attr, None)


def _units_in_manufacturing(jobs: list[Any]) -> float:
    """Sum up output quantities from in-progress manufacturing jobs."""
    total = 0.0
    now = _now()
    for job in jobs:
        # Count only active (non-delivered) jobs
        status = str(_job_attr(job, "status") or "").lower()
        if status == "delivered":
            continue
        runs = int(_job_attr(job, "runs") or 1)
        output_qty = int(_job_attr(job, "output_quantity") or _job_attr(job, "product_quantity") or runs)
        total += output_qty
    return total
