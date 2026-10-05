"""Typed intermediate structures shared across Daily Planner pipeline phases."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class PipelineState:
    """Phase 2 output — pipeline state for one item."""
    type_id: int
    effective_velocity: float          # units/day (floored at 0.01)
    total_pipeline_days: float         # (stock + in-mfg + on-market) / velocity
    competition_index: float | None    # None in Phase A (no MIJ data yet)
    competitor_units: int              # raw competitor units from market_depth_cache
    bpc_runs_available: int            # runs available in corp BPC stock
    momentum_signal: float             # price_trend_7d - price_trend_30d/4
    days_of_supply_current: float      # from IndustryService overview row
    price_trend_7d_pct: float          # from overview row (may be renamed from price_trend_pct)
    price_trend_30d_pct: float | None  # None if not enough history
    has_active_manufacturing_jobs: bool  # any in-flight mfg jobs for this type_id
    # None when effective_velocity is a real signal (own sales, or the
    # producer's days of supply). Otherwise names why there was none, and
    # effective_velocity is only the analyzer's 0.01 floor.
    velocity_unknown_reason: str | None = None


@dataclass
class ScoredItem:
    """Phase 3 output — scored item with all multipliers applied."""
    type_id: int
    adjusted_score: float
    absolute_profit_per_batch: float
    market_timing_factor: float
    pipeline_saturation: float
    accuracy_ema: float
    velocity_multiplier: float
    cost_multiplier: float
    competition_factor: float
    mineral_squeeze_penalty: float
    confidence_tier_bonus: float
    isk_per_hour: float                # base isk/hour before multipliers
    margin_pct: float
    # None when scoreable. Otherwise names the missing input(s) that forced
    # adjusted_score / absolute_profit_per_batch to a placeholder 0.0 -- e.g.
    # "no cost basis (material cost and profit unavailable)" or "no isk/hour
    # (missing cost basis or job time)". Additive field with a safe default
    # so existing construction sites keep working.
    unscoreable_reason: str | None = None


@dataclass
class ItemDecision:
    """Phase 4 output — decision for one item."""
    type_id: int
    type_name: str
    decision: str            # 'build' | 'watch' | 'pause' | 'skip'
    decision_reason: str
    adjusted_score: float
    absolute_profit_per_batch: float
    isk_per_hour: float
    margin_pct: float
    days_of_supply_current: float
    effective_velocity: float
    meta_group_id: int | None
    pipeline_stage: str      # 'manufacturing' | 'invention' | 'copying' | 'researching' | 'watching' | 'unscoreable'
    # BPO analysis fields (populated in Phase 5)
    bpo_investment_recommended: bool | None = None
    bpo_market_price: float | None = None
    break_even_days: float | None = None
    projected_annual_savings: float | None = None
    # None when the BPO analysis ran (or was never attempted). Otherwise names
    # the missing input that made it skip -- e.g. "no unit price for material
    # type_id=34" -- and the BPO fields above stay None (unknown), same
    # convention as ScoredItem.unscoreable_reason.
    bpo_analysis_skip_reason: str | None = None
    # Copied from PipelineState.velocity_unknown_reason (see there).
    velocity_unknown_reason: str | None = None
    # Chain context flag — True for sub-components added in Phase 5 Pass 2
    is_sub_component: bool = False
    # overview row snapshot for downstream phases
    overview_row: dict[str, Any] = field(default_factory=dict)


@dataclass
class AssignedAction:
    """Phase 6 output — a single action assigned to a character (or corp-level)."""
    type_id: int
    type_name: str
    action_type: str         # matches DailyActionLogModel.action_type values
    character_id: int | None
    character_name: str | None
    quantity: int | None
    runs: int | None
    estimated_cost_isk: float | None
    estimated_profit_isk: float | None
    estimated_completion: datetime | None
    notes: str | None
    shopping_category: str | None = None  # for buy_materials rows
    # manufacture: {material type_id: units for the whole batch} from the
    # producer (runs and ME/structure already applied). sub_manufacture: the
    # same shape from ChainPlanner, at the owned BPO's ME in the parent's
    # structure (structure bonus, plus a rig bonus only where the rig covers
    # the component's group). None = unknown, and the shopping list falls back
    # to SDE per-run quantities x runs.
    materials: dict[int, int] | None = None
    # manufacture only: materials + job costs for the whole batch (producer's
    # manufacturing_job.total_cost), the basis FeedbackProcessor compares
    # against a sale's realized industry-build unit cost. None = unknown.
    estimated_build_cost_isk: float | None = None


@dataclass
class ShoppingItem:
    """Phase 7 output — one shopping list line item."""
    type_id: int
    type_name: str
    quantity: int
    shopping_category: str   # 'current_job' | 'future_stock' | 'invention_input'
    estimated_unit_price: float
    estimated_total: float
    notes: str | None = None


@dataclass
class ChainPlan:
    """Phase 5 output — chain plan including all sub-components."""
    decisions: list[ItemDecision]   # top-level + sub-component decisions
    bpo_opportunities: list[dict[str, Any]] = field(default_factory=list)
    sub_manufacture_actions: list[AssignedAction] = field(default_factory=list)
