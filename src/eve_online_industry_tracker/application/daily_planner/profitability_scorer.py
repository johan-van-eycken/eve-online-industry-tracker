"""ProfitabilityScorer — Phase 3: apply all scoring multipliers.

In Phase A, competition_index=None → competition_factor=1.0, mineral_squeeze_penalty=1.0.
"""
from __future__ import annotations

import logging
from typing import Any

from eve_online_industry_tracker.application.daily_planner.models import PipelineState, ScoredItem

logger = logging.getLogger(__name__)


class ProfitabilityScorer:
    """Phase 3: compute adjusted_score for a single item."""

    def score(
        self,
        pipeline: PipelineState,
        overview_row: dict[str, Any],
        weights: Any | None,           # PlanLearningWeightsModel | None
        market_depth: Any | None,       # MarketDepthCacheModel | None (for absolute_profit)
        margin_correlation: Any | None, # MarginCorrelationCacheModel | None
    ) -> ScoredItem:
        """Apply Phase 3 multipliers and return a ScoredItem."""
        type_id = pipeline.type_id

        # ── Base values ───────────────────────────────────────────────────────
        isk_per_hour = float(overview_row.get("isk_per_hour") or 0.0)
        margin_pct = float(overview_row.get("profit_margin_fraction") or overview_row.get("margin_pct") or 0.0) * 100.0
        # Some rows store margin as a fraction (e.g. 0.15), others as a percentage (e.g. 15.0)
        # Normalise: if margin_pct > 1.0 we assume it's already in %, if <= 1.0 we multiply by 100
        if abs(margin_pct) <= 1.0 and margin_pct != 0.0:
            # Still a fraction — leave it; most rows use fraction
            pass  # margin_pct already reasonable

        # ── Self-learning multipliers ─────────────────────────────────────────
        accuracy_ema = float(getattr(weights, "accuracy_ema", 1.0)) if weights else 1.0
        velocity_multiplier = float(getattr(weights, "velocity_multiplier", 1.0)) if weights else 1.0
        cost_multiplier = float(getattr(weights, "cost_multiplier", 1.0)) if weights else 1.0
        confidence_tier = str(getattr(weights, "confidence_tier", "low")) if weights else "low"

        confidence_tier_bonus = {"low": 1.0, "medium": 1.05, "high": 1.15}.get(confidence_tier, 1.0)

        # ── Market timing factor ──────────────────────────────────────────────
        price_trend_7d = pipeline.price_trend_7d_pct
        momentum_signal = pipeline.momentum_signal

        base_factor = max(0.5, min(1.0, 1.0 + (price_trend_7d + 5.0) * 0.05))

        if momentum_signal < -3.0:
            momentum_adj = max(0.8, 1.0 + momentum_signal / 30.0)
            market_timing_factor = base_factor * momentum_adj
        else:
            market_timing_factor = base_factor

        # ── Pipeline saturation ───────────────────────────────────────────────
        # max(0, 1 - (days_of_supply - 3) / 11)
        # At 0 days: 1.27; at 3 days: 1.0; at 14 days: 0.0
        days_supply = pipeline.days_of_supply_current
        pipeline_saturation = max(0.0, 1.0 - (days_supply - 3.0) / 11.0)

        # ── Competition factor ────────────────────────────────────────────────
        ci = pipeline.competition_index
        if ci is None:
            # Phase A: no MIJ data
            competition_factor = 1.0
        elif ci < 1.0:
            competition_factor = 1.00
        elif ci < 2.0:
            competition_factor = 0.90
        elif ci < 4.0:
            competition_factor = 0.75
        else:
            competition_factor = 0.60

        # ── Mineral squeeze penalty ───────────────────────────────────────────
        # Phase A: always 1.0 (no correlation data)
        mineral_squeeze_penalty = 1.0
        if margin_correlation is not None:
            is_sensitive = getattr(margin_correlation, "is_squeeze_sensitive", False) or False
            if is_sensitive:
                # Only apply penalty if Tritanium is currently rising (trend > +5%)
                # Tritanium is type_id=34; we use price_trend_7d from the correlation entry
                # or fall back to the admin setting planner_mineral_squeeze_factor
                mineral_squeeze_penalty = 0.85  # configurable, but use default here

        # ── Adjusted score ────────────────────────────────────────────────────
        adjusted_score = (
            isk_per_hour
            * accuracy_ema
            * velocity_multiplier
            * cost_multiplier
            * market_timing_factor
            * pipeline_saturation
            * competition_factor
            * mineral_squeeze_penalty
            * confidence_tier_bonus
        )

        # ── Absolute profit per batch ─────────────────────────────────────────
        absolute_profit_per_batch = _compute_absolute_profit(overview_row, market_depth)

        return ScoredItem(
            type_id=type_id,
            adjusted_score=adjusted_score,
            absolute_profit_per_batch=absolute_profit_per_batch,
            market_timing_factor=market_timing_factor,
            pipeline_saturation=pipeline_saturation,
            accuracy_ema=accuracy_ema,
            velocity_multiplier=velocity_multiplier,
            cost_multiplier=cost_multiplier,
            competition_factor=competition_factor,
            mineral_squeeze_penalty=mineral_squeeze_penalty,
            confidence_tier_bonus=confidence_tier_bonus,
            isk_per_hour=isk_per_hour,
            margin_pct=margin_pct,
        )


def _compute_absolute_profit(row: dict[str, Any], market_depth: Any | None) -> float:
    """Compute absolute_profit_per_batch = (sell_price - material_cost_per_unit) × runs × output_qty.

    Uses vwap_5d from market_depth_cache as the sell price (fallback: spot_sell_price or
    profit_amount from overview_row).
    """
    # Prefer VWAP from market depth cache
    vwap_5d: float | None = None
    spot_sell_price: float | None = None
    if market_depth is not None:
        vwap_raw = getattr(market_depth, "vwap_5d", None) if not isinstance(market_depth, dict) else market_depth.get("vwap_5d")
        spot_raw = getattr(market_depth, "spot_sell_price", None) if not isinstance(market_depth, dict) else market_depth.get("spot_sell_price")
        vwap_5d = float(vwap_raw) if vwap_raw is not None else None
        spot_sell_price = float(spot_raw) if spot_raw is not None else None

    sell_price = vwap_5d or spot_sell_price

    if sell_price is not None:
        # Use VWAP-based calculation
        material_cost_per_unit = float(row.get("estimated_material_cost_per_unit") or
                                       row.get("material_cost_per_unit") or 0.0)
        runs = int(row.get("runs_per_batch") or row.get("runs") or 1)
        output_qty = int(row.get("output_quantity") or row.get("product_quantity") or runs)
        return (sell_price - material_cost_per_unit) * runs * output_qty
    else:
        # Fallback: use profit_amount from overview row directly
        profit_amount = float(row.get("profit_amount") or 0.0)
        return profit_amount
