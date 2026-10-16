"""ProfitabilityScorer — Phase 3: apply all scoring multipliers.

In Phase A, competition_index=None → competition_factor=1.0, mineral_squeeze_penalty=1.0.
"""
from __future__ import annotations

import logging
from typing import Any

from eve_online_industry_tracker.application.daily_planner.input_row import PlannerInputRow
from eve_online_industry_tracker.application.daily_planner.learning_weights import read_weight
from eve_online_industry_tracker.application.daily_planner.models import PipelineState, ScoredItem

logger = logging.getLogger(__name__)


class ProfitabilityScorer:
    """Phase 3: compute adjusted_score for a single item."""

    def score(
        self,
        pipeline: PipelineState,
        row: PlannerInputRow,
        weights: Any | None,            # PlanLearningWeightsModel | None
        market_depth: Any | None,       # MarketDepthCacheModel | None (for absolute_profit)
        margin_correlation: Any | None, # MarginCorrelationCacheModel | None
        trit_trend_7d: float | None = None,  # Tritanium 7d price trend %
    ) -> ScoredItem:
        """Apply Phase 3 multipliers and return a ScoredItem."""
        type_id = pipeline.type_id

        # ── Base values ───────────────────────────────────────────────────────
        # isk_per_hour and profit_margin_fraction can legitimately be None
        # (PlannerInputRow docs: missing net_proceeds/total_cost/time_seconds
        # upstream). margin_pct is informational only, so a missing fraction
        # becomes 0.0 without affecting scoreability. isk_per_hour instead
        # feeds directly into adjusted_score below, so its absence is one of
        # the two conditions that make an item unscoreable -- see reasons.
        unscoreable_reasons: list[str] = []

        if row.isk_per_hour is None:
            isk_per_hour = 0.0
            unscoreable_reasons.append("no isk/hour (missing cost basis or job time)")
        else:
            isk_per_hour = row.isk_per_hour

        margin_fraction = row.profit_margin_fraction if row.profit_margin_fraction is not None else 0.0
        margin_pct = margin_fraction * 100.0

        # ── Self-learning multipliers ─────────────────────────────────────────
        accuracy_ema = read_weight(weights, "accuracy_ema")
        velocity_multiplier = read_weight(weights, "velocity_multiplier")
        cost_multiplier = read_weight(weights, "cost_multiplier")
        confidence_tier = str(getattr(weights, "confidence_tier", "low")) if weights else "low"

        confidence_tier_bonus = {"low": 1.0, "medium": 1.05, "high": 1.15}.get(confidence_tier, 1.0)

        # ── Market timing factor ──────────────────────────────────────────────
        # price_trend_7d_pct is 0.0 both when the 7d trend is genuinely flat
        # and when it was absent/None on the source row (overview_row.py's
        # get_price_trend_7d_pct treats both as 0.0 by design). That
        # conflates "no history" with "flat", but it is harmless here: at
        # price_trend_7d == 0.0, base_factor below evaluates to 1.0 -- neutral,
        # neither a bonus nor a penalty -- so an unknown trend scores exactly
        # like a flat one instead of skewing the plan either way.
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
        # Only applies when: item is squeeze-sensitive AND Tritanium's 7d trend > +5%.
        # Without confirmed rising Tritanium prices the penalty must not fire.
        mineral_squeeze_penalty = 1.0
        if margin_correlation is not None:
            is_sensitive = getattr(margin_correlation, "is_squeeze_sensitive", False) or False
            if is_sensitive and trit_trend_7d is not None and trit_trend_7d > 5.0:
                mineral_squeeze_penalty = 0.85

        # ── Adjusted score ────────────────────────────────────────────────────
        # isk_per_hour is already 0.0 above when unscoreable, which naturally
        # zeroes adjusted_score without any special-casing here.
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
        absolute_profit_per_batch, cost_basis_reason = _compute_absolute_profit(row, market_depth)
        if cost_basis_reason is not None:
            unscoreable_reasons.append(cost_basis_reason)

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
            unscoreable_reason="; ".join(unscoreable_reasons) or None,
        )


def _compute_absolute_profit(
    row: PlannerInputRow, market_depth: Any | None
) -> tuple[float, str | None]:
    """(sell price − material cost per unit) × units per batch.

    `row.quantity` is already the batch's unit total (units per run × runs,
    see PlannerInputRow), and material_cost_per_unit is the batch material
    cost divided by that same total. Multiplying by `row.runs` as well would
    count the runs twice.

    Falls back to the producer's own profit_amount when no market depth is
    available, or when material_cost_per_unit is itself unknown (it is
    legitimately None whenever IndustryService could not price materials
    yet -- see PlannerInputRow's docstring). Only when profit_amount is ALSO
    None is there truly no cost basis to score from; that is reported as an
    unscoreable reason rather than silently returning 0.0 as if the item
    were unprofitable.
    """
    sell_price = _sell_price(market_depth)
    if sell_price is not None and row.material_cost_per_unit is not None:
        return (sell_price - row.material_cost_per_unit) * row.quantity, None
    if row.profit_amount is not None:
        return row.profit_amount, None
    return 0.0, "no cost basis (material cost and profit unavailable)"


def _sell_price(market_depth: Any | None) -> float | None:
    if market_depth is None:
        return None
    get = market_depth.get if isinstance(market_depth, dict) else (
        lambda key, default=None: getattr(market_depth, key, default)
    )
    for key in ("vwap_5d", "spot_sell_price"):
        raw = get(key)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None
