"""PipelineAnalyzer — Phase 2: compute pipeline state per item.

Takes PlannerInputRow contract rows, active industry jobs, corp assets,
market depth cache, and learning weights; returns one PipelineState per item.

`row.days_of_supply` (from the producer, industry/service.py:_calculate_days_of_supply)
is `None` when the region's 7-day sell volume is itself unknown/zero, i.e. "we have no
reliable read on how fast this sells" -- not "zero units already in the pipeline". Two
places in this module read it, and None must not silently become "sells out instantly"
in either:

  * The effective-velocity fallback (`1.0 / days_of_supply`) treats an unknown value as
    0.0, which is safe only because of the `> 0.0` guard immediately below it: that
    routes 0.0 to the same 0.01 floor as any other non-positive days_of_supply, i.e.
    "assume a slow, unknown-quality signal", not "assume it sells out instantly" (which
    `1.0 / small_number` would imply if the guard were absent).
  * `days_of_supply_current` is exposed on PipelineState and consumed by
    ProfitabilityScorer.pipeline_saturation (profitability_scorer.py:59-63:
    `max(0.0, 1.0 - (days_supply - 3.0) / 11.0)`). There, 0.0 means "nothing already in
    the pipeline" and *raises* the multiplier above 1.0 -- encouraging more production on
    a total unknown, the opposite of conservative. This module instead reports the
    formula's own neutral point (3.0, where pipeline_saturation == 1.0 -- no adjustment)
    when the producer could not say, mirroring this same scorer's existing convention for
    other missing signals (`competition_index=None -> competition_factor=1.0`).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Protocol

from eve_online_industry_tracker.application.daily_planner.character_assigner import (
    ACTIVITY_MANUFACTURING,
)
from eve_online_industry_tracker.application.daily_planner.input_row import PlannerInputRow
from eve_online_industry_tracker.application.daily_planner.learning_weights import read_weight
from eve_online_industry_tracker.application.daily_planner.models import PipelineState

logger = logging.getLogger(__name__)

# Neutral fallback for PipelineState.days_of_supply_current when the producer could not
# compute a days_of_supply for this item (see module docstring). Matches
# ProfitabilityScorer.pipeline_saturation's own neutral point (profitability_scorer.py:61).
_NEUTRAL_DAYS_OF_SUPPLY = 3.0


def _now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


class BlueprintCategorySource(Protocol):
    """The TypeMetadataResolver methods this module needs — see type_metadata.py."""

    def is_blueprint(self, type_id: int) -> bool: ...

    def prefetch(self, type_ids: Any) -> None: ...


class PipelineAnalyzer:
    """Phase 2: compute pipeline state per manufacturable item."""

    def analyze(
        self,
        input_rows: list[PlannerInputRow],
        industry_jobs: list[Any],
        corp_assets: list[Any],
        market_depth_cache: dict[int, Any],  # keyed by type_id; values are MarketDepthCacheModel or dict
        weights: dict[int, Any],              # keyed by type_id; values are PlanLearningWeightsModel
        sell_velocities: dict[int, float],    # type_id → sell_velocity_per_day (from SalesHistoryService)
        meta_resolver: BlueprintCategorySource,
    ) -> list[PipelineState]:
        """Return one PipelineState for each input row."""
        now = _now()
        result: list[PipelineState] = []

        # Index active manufacturing jobs by product type_id for quick lookup.
        # activity_id is now a real column (backfilled from the raw ESI payload by
        # schema_migrations.backfill_job_activity_ids for pre-existing rows, and set
        # directly on new syncs by corporation.py) -- no more raw-JSON fallback needed
        # here.
        mfg_jobs_by_type: dict[int, list[Any]] = {}
        for job in industry_jobs:
            if int(_job_attr(job, "activity_id") or 0) != ACTIVITY_MANUFACTURING:
                continue
            product_type_id = _job_attr(job, "product_type_id")
            if product_type_id is None:
                continue
            mfg_jobs_by_type.setdefault(int(product_type_id), []).append(job)

        # Index BPC runs by blueprint type_id.
        # A blueprint *copy* is is_blueprint_copy=True with blueprint_runs > 0;
        # the category check keeps non-blueprint assets out even if a future
        # schema reuses those column names.
        #
        # Pre-warm the resolver's cache with every distinct asset type_id in one
        # batched SDE query before the per-asset is_blueprint() loop below --
        # mirrors the same fix already applied to index_blueprint_assets (service.py)
        # and _build_corp_stock_map. Without this, TypeMetadataResolver._entry()
        # self-heals a cache miss by calling prefetch() for a single id, so
        # is_blueprint() here would otherwise open one SDE session (with its
        # metaGroups table reflection) per distinct type_id -- measured as hundreds of
        # reflected open/query/close cycles for this app's live corp_assets table
        # of thousands of rows. prefetch() is idempotent (skips ids already cached or
        # already marked missing), so this is safe even if a caller already
        # warmed it.
        asset_type_ids = {int(_asset_attr(a, "type_id") or 0) for a in corp_assets}
        meta_resolver.prefetch({t for t in asset_type_ids if t > 0})

        bpc_runs_by_type: dict[int, int] = {}
        for asset in corp_assets:
            type_id = int(_asset_attr(asset, "type_id") or 0)
            if type_id <= 0:
                continue
            if not meta_resolver.is_blueprint(type_id):
                continue
            if not _asset_attr_bool(asset, "is_blueprint_copy"):
                continue  # BPO — unlimited runs, not BPC stock
            runs = int(_asset_attr(asset, "blueprint_runs") or 0)
            if runs <= 0:
                continue
            bpc_runs_by_type[type_id] = bpc_runs_by_type.get(type_id, 0) + runs

        for row in input_rows:
            type_id = row.type_id

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
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                logger.exception("PipelineAnalyzer: error analyzing type_id=%s", type_id)

        return result

    def _analyze_single(
        self,
        *,
        row: PlannerInputRow,
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
        velocity_multiplier = read_weight(weights.get(type_id), "velocity_multiplier")

        # See module docstring: 0.0 here is safe only because of the `> 0.0` guard
        # below, which floors it at 0.01 same as any other non-positive value.
        days_of_supply_for_velocity = row.days_of_supply if row.days_of_supply is not None else 0.0

        if sell_velocity_per_day > 0.0:
            effective_velocity = max(0.01, sell_velocity_per_day * velocity_multiplier)
        else:
            # Fallback: 1/days_of_supply, floored at 0.01
            fallback = (
                (1.0 / days_of_supply_for_velocity) if days_of_supply_for_velocity > 0.0 else 0.01
            )
            effective_velocity = max(0.01, fallback)

        # ── Pipeline days ─────────────────────────────────────────────────────
        # The producer already computes this (industry/service.py:2004-2012) as
        # (pipeline_units_in_jobs + pipeline_units_on_market) / vol_7d, returning
        # None only when vol_7d == 0. pipeline_units_in_jobs is itself
        # SUM(output_quantity) over jobs with status in ("active", "ready")
        # (industry/service.py:1872-1889) -- i.e. it already IS the units
        # sitting in manufacturing. The fallback below must reuse that same
        # numerator verbatim and differ only in the denominator (effective_velocity
        # standing in for the unavailable vol_7d); adding a second, independently
        # computed "units in manufacturing" term here would double-count them.
        if row.pipeline_days_supply is not None:
            total_pipeline_days = float(row.pipeline_days_supply)
        else:
            units_in_pipeline = float(row.pipeline_units_in_jobs) + float(row.pipeline_units_on_market)
            total_pipeline_days = units_in_pipeline / effective_velocity

        # ── Competition (from pre-computed cache, Phase A = None) ─────────────
        cache_entry = market_depth_cache.get(type_id)
        competition_index: float | None = None
        competitor_units: int = 0
        if cache_entry is not None:
            competition_index = _get_attr(cache_entry, "competition_index")
            competitor_units = int(_get_attr(cache_entry, "competitor_units") or 0)

        # ── BPC runs available ────────────────────────────────────────────────
        bp_type_id = row.blueprint_type_id or 0
        bpc_runs_available = bpc_runs_by_type.get(bp_type_id, 0)

        # ── Momentum signal ───────────────────────────────────────────────────
        price_trend_7d = row.price_trend_7d_pct
        price_trend_30d = row.price_trend_30d_pct
        trend_30d_for_signal = price_trend_30d if price_trend_30d is not None else 0.0
        momentum_signal = price_trend_7d - (trend_30d_for_signal / 4.0)

        # ── Active manufacturing jobs flag ────────────────────────────────────
        active_jobs = mfg_jobs_by_type.get(type_id, [])
        has_active_manufacturing_jobs = len(active_jobs) > 0

        # See module docstring: unlike the velocity fallback above, 0.0 here would
        # *raise* ProfitabilityScorer.pipeline_saturation above neutral, so an
        # unknown days_of_supply is reported as the formula's own neutral point
        # instead of 0.0.
        days_of_supply_current = (
            row.days_of_supply if row.days_of_supply is not None else _NEUTRAL_DAYS_OF_SUPPLY
        )

        return PipelineState(
            type_id=type_id,
            effective_velocity=effective_velocity,
            total_pipeline_days=total_pipeline_days,
            competition_index=competition_index,
            competitor_units=competitor_units,
            bpc_runs_available=bpc_runs_available,
            momentum_signal=momentum_signal,
            days_of_supply_current=days_of_supply_current,
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
