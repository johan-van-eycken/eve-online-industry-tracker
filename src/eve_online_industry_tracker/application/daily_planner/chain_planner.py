"""ChainPlanner — Phase 5: resolve full production chain per build item.

For each item with decision='build':
- Determine BPO/BPC path (T1 vs T2)
- Schedule ME/TE research jobs if below optimal
- Sub-manufacture decision: build vs buy for each required material
- BPO investment analysis

Sub-components get decision='build' directly (bypass Phase 4 gate).
They are added as extra ItemDecision rows.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.daily_planner.models import (
    AssignedAction,
    ChainPlan,
    ItemDecision,
)

logger = logging.getLogger(__name__)

# EVE Online meta_group_id for T1 items (eligible for BPO)
META_GROUP_T1 = 1


def _now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


def _adm(admin_settings: Any, key: str, fallback: Any) -> Any:
    try:
        return admin_settings.get("daily_planner", key)
    except Exception:
        return fallback


def build_product_to_blueprint_index(
    blueprint_data: dict[int, dict[str, Any]]
) -> dict[int, int]:
    """{product type_id: blueprint type_id} from SDE blueprint manufacturing data.

    blueprint_data is keyed by *blueprint* type_id. Anything that starts from a
    product — a material requirement, an overview row — has to cross over
    through this index. When two blueprints make the same product the first
    one seen wins (same tie-break as a linear scan over blueprint_data).
    """
    index: dict[int, int] = {}
    for blueprint_type_id, entry in (blueprint_data or {}).items():
        if not isinstance(entry, dict):
            continue
        manufacturing = entry.get("manufacturing")
        if not isinstance(manufacturing, dict):
            continue
        products = manufacturing.get("products")
        if not isinstance(products, list):
            continue
        for product in products:
            if not isinstance(product, dict):
                continue
            try:
                product_type_id = int(product.get("type_id") or 0)
            except (TypeError, ValueError):
                continue
            if product_type_id > 0:
                index.setdefault(product_type_id, int(blueprint_type_id))
    return index


def build_invention_source_index(
    blueprint_data: dict[int, dict[str, Any]]
) -> dict[int, int]:
    """{invented blueprint type_id: source (T1) blueprint type_id}.

    In the SDE the invention activity lives on the *source* blueprint, and its
    products are the blueprints it invents into. A T2 overview row only knows
    its own (T2) blueprint, so finding the T1 BPO to copy means walking this
    index backwards. First source seen wins.
    """
    index: dict[int, int] = {}
    for source_type_id, entry in (blueprint_data or {}).items():
        if not isinstance(entry, dict):
            continue
        invention = entry.get("invention")
        if not isinstance(invention, dict):
            continue
        products = invention.get("products")
        if not isinstance(products, list):
            continue
        for product in products:
            if not isinstance(product, dict):
                continue
            try:
                invented_type_id = int(product.get("type_id") or 0)
            except (TypeError, ValueError):
                continue
            if invented_type_id > 0:
                index.setdefault(invented_type_id, int(source_type_id))
    return index


class ChainPlanner:
    """Phase 5: resolve sub-component chain for each build decision.

    This is Pass 2 of the two-pass ordering:
      Pass 1 (Phases 2–4): all top-level items evaluated → ItemDecision list.
      Pass 2 (Phase 5, here): for each 'build' item, resolve sub-components.
    """

    def __init__(
        self,
        industry_service: Any,   # IndustryService (for _build_recursive_prerequisite_plan)
        session_provider: Any,   # SessionProvider (for SDE lookups)
        admin_settings: Any,
    ) -> None:
        self._industry_service = industry_service
        self._session_provider = session_provider
        self._admin = admin_settings

    def plan_chain(
        self,
        top_level_decisions: list[ItemDecision],
        phase1_data: dict[str, Any],
    ) -> ChainPlan:
        """Resolve sub-components for all 'build' items.

        phase1_data must contain:
          - corp_assets: list of corp asset objects
          - market_depth_cache: dict[int, MarketDepthCacheModel]
          - industry_jobs: list of active industry jobs
          - bpo_assets_by_type_id: dict[int, list] — owned BPO assets keyed by blueprint type_id
          - bpc_assets_by_type_id: dict[int, list] — owned BPC assets keyed by blueprint type_id
          - blueprint_data: dict[int, dict] — from get_blueprint_manufacturing_data()
          - invention_success_rates: dict[int, float] — from repo
        """
        all_decisions: list[ItemDecision] = list(top_level_decisions)
        bpo_opportunities: list[dict[str, Any]] = []
        sub_manufacture_actions: list[AssignedAction] = []

        bpo_strong_buy_days = float(_adm(self._admin, "planner_bpo_strong_buy_days", 30))
        bpo_consider_days = float(_adm(self._admin, "planner_bpo_consider_days", 90))
        optimal_te_threshold = float(_adm(self._admin, "planner_optimal_te_threshold_pct", 1.0))
        invention_min_attempts = int(_adm(self._admin, "planner_invention_min_attempts", 10))

        bpo_assets_by_type_id: dict[int, list] = phase1_data.get("bpo_assets_by_type_id", {})
        bpc_assets_by_type_id: dict[int, list] = phase1_data.get("bpc_assets_by_type_id", {})
        blueprint_data: dict[int, dict] = phase1_data.get("blueprint_data", {})
        market_depth_cache: dict[int, Any] = phase1_data.get("market_depth_cache", {})
        invention_success_rates: dict[int, float] = phase1_data.get("invention_success_rates", {})
        # Built once per plan: material requirements are product type_ids, but
        # BPO ownership and blueprint_data are keyed by blueprint type_id.
        product_to_blueprint = build_product_to_blueprint_index(blueprint_data)
        invention_source = build_invention_source_index(blueprint_data)

        for decision in top_level_decisions:
            if decision.decision != "build":
                continue

            row = decision.overview_row
            meta_group_id = decision.meta_group_id
            # Nested under manufacturing_job.blueprint_sde on a real row; there
            # is no top-level blueprint_type_id key.
            bp_type_id = orow.get_blueprint_type_id(row) or 0
            bp_data = blueprint_data.get(bp_type_id, {})

            try:
                if meta_group_id == META_GROUP_T1:
                    self._plan_t1_chain(
                        decision=decision,
                        bp_type_id=bp_type_id,
                        bp_data=bp_data,
                        bpo_assets_by_type_id=bpo_assets_by_type_id,
                        bpc_assets_by_type_id=bpc_assets_by_type_id,
                        market_depth_cache=market_depth_cache,
                        bpo_opportunities=bpo_opportunities,
                        optimal_te_threshold=optimal_te_threshold,
                        bpo_strong_buy_days=bpo_strong_buy_days,
                        bpo_consider_days=bpo_consider_days,
                    )
                else:
                    # T2+ — invention chain only
                    self._plan_t2_chain(
                        decision=decision,
                        bp_type_id=bp_type_id,
                        bp_data=bp_data,
                        blueprint_data=blueprint_data,
                        invention_source=invention_source,
                        bpo_assets_by_type_id=bpo_assets_by_type_id,
                        bpc_assets_by_type_id=bpc_assets_by_type_id,
                        market_depth_cache=market_depth_cache,
                        invention_success_rates=invention_success_rates,
                        bpo_opportunities=bpo_opportunities,
                        bpo_strong_buy_days=bpo_strong_buy_days,
                        bpo_consider_days=bpo_consider_days,
                    )

                # Sub-manufacture decision for each required material
                sub_decisions = self._resolve_sub_manufacture(
                    decision=decision,
                    bp_data=bp_data,
                    bpo_assets_by_type_id=bpo_assets_by_type_id,
                    product_to_blueprint=product_to_blueprint,
                    market_depth_cache=market_depth_cache,
                    phase1_data=phase1_data,
                )
                all_decisions.extend(sub_decisions)

            except Exception:
                logger.exception("ChainPlanner: error planning chain for type_id=%s", decision.type_id)

        return ChainPlan(
            decisions=all_decisions,
            bpo_opportunities=bpo_opportunities,
            sub_manufacture_actions=sub_manufacture_actions,
        )

    def _plan_t1_chain(
        self,
        *,
        decision: ItemDecision,
        bp_type_id: int,
        bp_data: dict[str, Any],
        bpo_assets_by_type_id: dict[int, list],
        bpc_assets_by_type_id: dict[int, list],
        market_depth_cache: dict[int, Any],
        bpo_opportunities: list[dict[str, Any]],
        optimal_te_threshold: float,
        bpo_strong_buy_days: float,
        bpo_consider_days: float,
    ) -> None:
        """Plan T1 chain: BPO path, ME/TE research, or BPC/shopping fallback."""
        bpo_owned = bp_type_id in bpo_assets_by_type_id and bool(bpo_assets_by_type_id[bp_type_id])

        if bpo_owned:
            # Compute optimal ME/TE via SDE lookup
            optimal_me = self._compute_optimal_me(bp_type_id)
            optimal_te = self._compute_optimal_te(bp_type_id, optimal_te_threshold)

            bpo_assets = bpo_assets_by_type_id[bp_type_id]
            # Corp asset columns are blueprint_*-prefixed. None means unknown,
            # not ME0: an unknown level schedules no research rather than a
            # job on a blueprint that may already be fully researched.
            current_me = _blueprint_efficiency(bpo_assets[0], "material")
            current_te = _blueprint_efficiency(bpo_assets[0], "time")
            if current_me is None or current_te is None:
                logger.warning(
                    "ChainPlanner: owned BPO %s has unknown ME/TE (me=%s, te=%s); "
                    "not scheduling research for the unknown level(s)",
                    bp_type_id, current_me, current_te,
                )

            # Schedule ME/TE research in the action plan (signals to ActionPlanBuilder)
            if current_me is not None and current_me < optimal_me:
                decision.overview_row["needs_me_research"] = True
                decision.overview_row["me_research_target"] = optimal_me
                decision.overview_row["me_current"] = current_me

            if current_te is not None and current_te < optimal_te:
                decision.overview_row["needs_te_research"] = True
                decision.overview_row["te_research_target"] = optimal_te
                decision.overview_row["te_current"] = current_te

            decision.pipeline_stage = "manufacturing"

        elif bp_type_id in bpc_assets_by_type_id and bpc_assets_by_type_id[bp_type_id]:
            # BPC available — plan manufacturing from BPC + run BPO investment analysis
            decision.pipeline_stage = "manufacturing"
            self._analyze_bpo_investment(
                decision=decision,
                bp_type_id=bp_type_id,
                market_depth_cache=market_depth_cache,
                bpo_opportunities=bpo_opportunities,
                bpo_strong_buy_days=bpo_strong_buy_days,
                bpo_consider_days=bpo_consider_days,
            )
        else:
            # No BPO or BPC — BPO investment analysis + shopping list
            self._analyze_bpo_investment(
                decision=decision,
                bp_type_id=bp_type_id,
                market_depth_cache=market_depth_cache,
                bpo_opportunities=bpo_opportunities,
                bpo_strong_buy_days=bpo_strong_buy_days,
                bpo_consider_days=bpo_consider_days,
            )
            decision.pipeline_stage = "watching"

    def _plan_t2_chain(
        self,
        *,
        decision: ItemDecision,
        bp_type_id: int,
        bp_data: dict[str, Any],
        blueprint_data: dict[int, dict[str, Any]],
        invention_source: dict[int, int],
        bpo_assets_by_type_id: dict[int, list],
        bpc_assets_by_type_id: dict[int, list],
        market_depth_cache: dict[int, Any],
        invention_success_rates: dict[int, float],
        bpo_opportunities: list[dict[str, Any]],
        bpo_strong_buy_days: float,
        bpo_consider_days: float,
    ) -> None:
        """Plan T2+ chain: invention pipeline or BPC fallback."""
        if bp_type_id in bpc_assets_by_type_id and bpc_assets_by_type_id[bp_type_id]:
            decision.pipeline_stage = "manufacturing"
            return

        # No BPC — need invention chain. bp_type_id is the T2 blueprint; the
        # blueprint that is copied and invented from is its T1 source, and the
        # invention activity (with its datacores) lives on that T1 blueprint.
        t1_bp_type_id = invention_source.get(bp_type_id)
        t1_bpo_owned = t1_bp_type_id is not None and bool(bpo_assets_by_type_id.get(t1_bp_type_id))
        if t1_bp_type_id is not None:
            decision.overview_row["t1_blueprint_type_id"] = t1_bp_type_id
        else:
            logger.info(
                "ChainPlanner: no known T1 source blueprint invents into bp_type_id=%s "
                "(type_id=%s); treating the T1 BPO as not owned",
                bp_type_id, decision.type_id,
            )

        if t1_bpo_owned:
            t1_invention = (blueprint_data.get(t1_bp_type_id) or {}).get("invention") or {}
            decision.pipeline_stage = "copying"
            decision.overview_row["needs_invention"] = True
            # "We own the T1 BPO, so copy it" — the gate CharacterAssigner reads.
            decision.overview_row["has_t1_bpo"] = True
            decision.overview_row["invention_materials"] = list(t1_invention.get("materials") or [])
        else:
            decision.pipeline_stage = "invention"
            # "A T1 BPO must be acquired first." Read by nothing downstream yet.
            decision.overview_row["needs_t1_bpo"] = True
            # Flag T1 BPO for investment analysis as invention enabler
            self._analyze_bpo_investment(
                decision=decision,
                bp_type_id=bp_type_id,
                market_depth_cache=market_depth_cache,
                bpo_opportunities=bpo_opportunities,
                bpo_strong_buy_days=bpo_strong_buy_days,
                bpo_consider_days=bpo_consider_days,
                as_invention_enabler=True,
            )

    def _blueprint_for_product(
        self,
        product_type_id: int,
        product_to_blueprint: dict[int, int],
        bpo_assets_by_blueprint_type_id: dict[int, list],
    ) -> int | None:
        """The blueprint type_id the corp owns for this product, if any."""
        blueprint_type_id = product_to_blueprint.get(int(product_type_id))
        if blueprint_type_id is None:
            return None
        if not bpo_assets_by_blueprint_type_id.get(blueprint_type_id):
            return None
        return blueprint_type_id

    def _resolve_sub_manufacture(
        self,
        *,
        decision: ItemDecision,
        bp_data: dict[str, Any],
        bpo_assets_by_type_id: dict[int, list],
        product_to_blueprint: dict[int, int],
        market_depth_cache: dict[int, Any],
        phase1_data: dict[str, Any],
    ) -> list[ItemDecision]:
        """For each required material, decide: sub-manufacture (build) vs buy.

        Returns list of sub-component ItemDecisions (decision='build').
        These bypass Phase 4 gate per the two-pass spec.
        """
        sub_decisions: list[ItemDecision] = []
        manufacturing = bp_data.get("manufacturing", {})
        materials = manufacturing.get("materials", []) or []

        for mat in materials:
            mat_type_id = int(mat.get("type_id") or 0)
            if mat_type_id <= 0:
                continue

            # mat_type_id is a product; BPO ownership is keyed by blueprint.
            mat_blueprint_type_id = self._blueprint_for_product(
                mat_type_id, product_to_blueprint, bpo_assets_by_type_id
            )
            if mat_blueprint_type_id is None:
                continue  # no BPO for this material → buy from market

            qty_needed = int(mat.get("quantity") or 0)
            if qty_needed <= 0:
                continue

            # Estimate sub-manufacture cost vs market buy cost
            sub_cost = self._estimate_sub_manufacture_cost(mat_blueprint_type_id, qty_needed, phase1_data)
            market_cost = self._estimate_market_cost(mat_type_id, qty_needed, market_depth_cache)

            if sub_cost is None or market_cost is None:
                continue  # Can't compare → default to buy

            if sub_cost < market_cost:
                # Sub-manufacture is cheaper → add as build decision
                mat_name = str(mat.get("type_name") or f"type_{mat_type_id}")
                sub_decision = ItemDecision(
                    type_id=mat_type_id,
                    type_name=mat_name,
                    decision="build",
                    decision_reason="Sub-manufacture: cheaper than market buy",
                    adjusted_score=0.0,
                    absolute_profit_per_batch=0.0,
                    isk_per_hour=0.0,
                    margin_pct=0.0,
                    days_of_supply_current=0.0,
                    effective_velocity=1.0,
                    meta_group_id=META_GROUP_T1,
                    pipeline_stage="manufacturing",
                    is_sub_component=True,
                    overview_row={
                        "type_id": mat_type_id,
                        "type_name": mat_name,
                        "blueprint_type_id": mat_blueprint_type_id,
                        "quantity_needed": qty_needed,
                        "sub_manufacture_cost": sub_cost,
                        "market_buy_cost": market_cost,
                    },
                )
                sub_decisions.append(sub_decision)

        return sub_decisions

    def _estimate_sub_manufacture_cost(
        self,
        blueprint_type_id: int,
        qty_needed: int,
        phase1_data: dict[str, Any],
    ) -> float | None:
        """Estimate the cost of sub-manufacturing qty_needed units."""
        # Delegate to IndustryService if possible, otherwise estimate from blueprint data
        blueprint_data: dict[int, dict] = phase1_data.get("blueprint_data", {})
        bp_data = blueprint_data.get(blueprint_type_id)
        if bp_data is None:
            return None

        manufacturing = bp_data.get("manufacturing", {})
        materials = manufacturing.get("materials", []) or []
        market_depth_cache: dict[int, Any] = phase1_data.get("market_depth_cache", {})

        total_cost = 0.0
        for mat in materials:
            mat_type_id = int(mat.get("type_id") or 0)
            mat_qty = int(mat.get("quantity") or 0)
            if mat_type_id <= 0 or mat_qty <= 0:
                continue
            mat_cost = self._estimate_market_cost(mat_type_id, mat_qty, market_depth_cache)
            if mat_cost is None:
                return None
            total_cost += mat_cost

        # Scale to needed quantity (1 run = products_per_run output quantity)
        products = manufacturing.get("products", []) or []
        output_per_run = int(products[0].get("quantity") or 1) if products else 1
        if output_per_run <= 0:
            output_per_run = 1

        runs_needed = math.ceil(qty_needed / output_per_run)
        return total_cost * runs_needed

    def _estimate_market_cost(
        self,
        type_id: int,
        quantity: int,
        market_depth_cache: dict[int, Any],
    ) -> float | None:
        """Estimate market buy cost using vwap_5d (fallback: spot_sell_price)."""
        entry = market_depth_cache.get(type_id)
        if entry is None:
            return None

        vwap = _get_attr(entry, "vwap_5d")
        spot = _get_attr(entry, "spot_sell_price")
        price = vwap or spot
        if price is None:
            return None
        return float(price) * quantity

    def _analyze_bpo_investment(
        self,
        *,
        decision: ItemDecision,
        bp_type_id: int,
        market_depth_cache: dict[int, Any],
        bpo_opportunities: list[dict[str, Any]],
        bpo_strong_buy_days: float,
        bpo_consider_days: float,
        as_invention_enabler: bool = False,
    ) -> None:
        """Compute BPO break-even and add to bpo_opportunities if worthwhile."""
        # Effective velocity for planned runs per day
        eff_velocity = max(0.033, decision.effective_velocity)  # floor at 1/30

        # BPO market price from market depth cache (spot price on BPO type_id)
        bpo_cache_entry = market_depth_cache.get(bp_type_id)
        bpo_market_price: float | None = None
        if bpo_cache_entry is not None:
            spot = _get_attr(bpo_cache_entry, "spot_sell_price")
            bpo_market_price = float(spot) if spot is not None else None

        if bpo_market_price is None:
            return  # Can't do analysis without BPO price

        # Simplified break-even: total_investment / (savings_per_run × planned_runs_per_day)
        # Use overview_row's material cost as proxy for savings
        row = decision.overview_row
        material_cost_base = float(row.get("estimated_material_cost") or row.get("material_cost") or 0.0)
        # ME10 savings ≈ 10% of base material cost for T1 items (rough approximation)
        me_savings_per_run = material_cost_base * 0.10 if material_cost_base > 0 else 0.0

        runs_per_batch = int(row.get("runs_per_batch") or row.get("runs") or 1)
        planned_runs_per_day = max(0.033, eff_velocity / max(1, runs_per_batch))

        total_investment = bpo_market_price  # simplified (ignores research cost)
        if me_savings_per_run <= 0 or planned_runs_per_day <= 0:
            break_even_days = float("inf")
        else:
            break_even_days = total_investment / (me_savings_per_run * planned_runs_per_day)

        projected_annual_savings = me_savings_per_run * planned_runs_per_day * 365.0

        # Tag decision with BPO analysis results
        if break_even_days <= bpo_strong_buy_days:
            decision.bpo_investment_recommended = True
            recommendation = "strong_buy"
        elif break_even_days <= bpo_consider_days:
            decision.bpo_investment_recommended = True
            recommendation = "consider"
        else:
            decision.bpo_investment_recommended = False
            recommendation = "hold"

        decision.bpo_market_price = bpo_market_price
        decision.break_even_days = break_even_days if break_even_days != float("inf") else None
        decision.projected_annual_savings = projected_annual_savings

        bpo_opportunities.append({
            "type_id": decision.type_id,
            "type_name": decision.type_name,
            "bp_type_id": bp_type_id,
            "bpo_market_price": bpo_market_price,
            "break_even_days": break_even_days if break_even_days != float("inf") else None,
            "projected_annual_savings": projected_annual_savings,
            "recommendation": recommendation,
            "as_invention_enabler": as_invention_enabler,
        })

    def _compute_optimal_me(self, blueprint_type_id: int) -> int:
        try:
            from eve_online_industry_tracker.infrastructure.sde.blueprints import compute_optimal_me
            sde_session = self._session_provider.sde_session()
            try:
                return compute_optimal_me(blueprint_type_id, sde_session)
            finally:
                sde_session.close()
        except Exception:
            logger.debug("ChainPlanner: compute_optimal_me failed for bp_type_id=%s", blueprint_type_id)
            return 0

    def _compute_optimal_te(self, blueprint_type_id: int, threshold_pct: float) -> int:
        try:
            from eve_online_industry_tracker.infrastructure.sde.blueprints import compute_optimal_te
            sde_session = self._session_provider.sde_session()
            try:
                return compute_optimal_te(blueprint_type_id, sde_session, threshold_pct)
            finally:
                sde_session.close()
        except Exception:
            logger.debug("ChainPlanner: compute_optimal_te failed for bp_type_id=%s", blueprint_type_id)
            return 0


def _blueprint_efficiency(asset: Any, kind: str) -> int | None:
    """An owned blueprint's ME (kind='material') or TE (kind='time'), or None.

    Corp asset rows name these blueprint_material_efficiency /
    blueprint_time_efficiency; the unprefixed name is accepted as a fallback
    for ESI-shaped blueprint dicts.
    """
    for attr in (f"blueprint_{kind}_efficiency", f"{kind}_efficiency"):
        value = _get_attr(asset, attr)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def _get_attr(obj: Any, attr: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(attr)
    return getattr(obj, attr, None)
