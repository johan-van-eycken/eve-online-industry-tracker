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

from sqlalchemy.exc import SQLAlchemyError

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
    """A daily_planner setting, or `fallback` when there is no settings store.

    AttributeError covers stub/None admin objects; KeyError is what
    AdminSettingsManager.get raises for an unknown key. Every key the planner
    reads is pinned to the schema by tests/test_daily_planner_fail_loud.py, so
    with the real manager the KeyError branch cannot hide a typo.
    """
    try:
        return admin_settings.get("daily_planner", key)
    except (AttributeError, KeyError):
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
        # {material type_id: merged sub-manufacture request}, in first-seen order.
        sub_requests: dict[int, dict[str, Any]] = {}

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

                # Which of this batch's materials the corp could build itself.
                # Collected across every parent first, so one material needed
                # by several builds becomes ONE sub-manufacture decision.
                for request in self._collect_sub_manufacture_requests(
                    decision=decision,
                    bpo_assets_by_type_id=bpo_assets_by_type_id,
                    product_to_blueprint=product_to_blueprint,
                ):
                    merged = sub_requests.setdefault(request["type_id"], {
                        **request, "quantity": 0, "requested_by_type_ids": [],
                    })
                    merged["quantity"] += request["quantity"]
                    merged["requested_by_type_ids"].append(int(decision.type_id))

            except (TypeError, ValueError):
                # Per-item isolation for malformed SDE / market values (int()
                # and float() conversions); anything else is a bug and aborts.
                logger.exception("ChainPlanner: error planning chain for type_id=%s", decision.type_id)

        for request in sub_requests.values():
            try:
                sub_decision = self._resolve_sub_manufacture(
                    request=request,
                    market_depth_cache=market_depth_cache,
                    phase1_data=phase1_data,
                )
            except (TypeError, ValueError):
                logger.exception(
                    "ChainPlanner: error resolving sub-manufacture for type_id=%s", request["type_id"]
                )
                continue
            if sub_decision is not None:
                all_decisions.append(sub_decision)

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
            optimal_me = self._compute_optimal_me(
                bp_type_id, runs=orow.get_effective_runs(decision.overview_row)
            )
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

            # None means the SDE lookup failed (already logged at WARNING by
            # _compute_optimal_*): schedule no research rather than treating
            # the optimum as 0, which silently reads as "already optimal".
            if optimal_me is None or optimal_te is None:
                logger.warning(
                    "ChainPlanner: optimal ME/TE unknown for owned BPO %s (optimal_me=%s, "
                    "optimal_te=%s); not scheduling research for the unknown level(s)",
                    bp_type_id, optimal_me, optimal_te,
                )

            # Schedule ME/TE research in the action plan (signals to ActionPlanBuilder)
            if current_me is not None and optimal_me is not None and current_me < optimal_me:
                decision.overview_row["needs_me_research"] = True
                decision.overview_row["me_research_target"] = optimal_me
                decision.overview_row["me_current"] = current_me

            if current_te is not None and optimal_te is not None and current_te < optimal_te:
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
                bp_data=bp_data,
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
                bp_data=bp_data,
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

    def _collect_sub_manufacture_requests(
        self,
        *,
        decision: ItemDecision,
        bpo_assets_by_type_id: dict[int, list],
        product_to_blueprint: dict[int, int],
    ) -> list[dict[str, Any]]:
        """This batch's materials that the corp owns a BPO for, with the
        quantity the batch actually consumes.

        The quantity is the producer's manufacturing_job.materials entry:
        already per-run x runs and after ME/structure reduction. The SDE's
        per-run quantity (what this used to read) is one run at ME0, which
        under-built the component by the run count. Without the producer's
        figures there is no correct amount, so nothing is requested and the
        parent buys the component (logged).
        """
        batch_materials = orow.get_batch_materials(decision.overview_row)
        if batch_materials is None:
            logger.warning(
                "ChainPlanner: type_id=%s has no producer batch materials "
                "(manufacturing_job.materials); skipping sub-manufacture analysis, "
                "its components are bought",
                decision.type_id,
            )
            return []

        names = {
            int(e.get("type_id") or 0): e.get("type_name")
            for e in (orow.get_manufacturing_job(decision.overview_row).get("materials") or {}).values()
            if isinstance(e, dict)
        }
        requests: list[dict[str, Any]] = []
        for mat_type_id, quantity in batch_materials.items():
            # mat_type_id is a product; BPO ownership is keyed by blueprint.
            mat_blueprint_type_id = self._blueprint_for_product(
                mat_type_id, product_to_blueprint, bpo_assets_by_type_id
            )
            if mat_blueprint_type_id is None:
                continue  # no BPO for this material → buy from market
            requests.append({
                "type_id": mat_type_id,
                "type_name": str(names.get(mat_type_id) or f"type_{mat_type_id}"),
                "blueprint_type_id": mat_blueprint_type_id,
                "quantity": quantity,
            })
        return requests

    def _resolve_sub_manufacture(
        self,
        *,
        request: dict[str, Any],
        market_depth_cache: dict[int, Any],
        phase1_data: dict[str, Any],
    ) -> ItemDecision | None:
        """Build vs buy for one merged material request.

        Returns a sub-component ItemDecision (decision='build') when building
        is cheaper, else None (buy). These bypass the Phase 4 gate per the
        two-pass spec. ShoppingListBuilder subtracts the built quantity from
        the parents' purchases.
        """
        mat_type_id = int(request["type_id"])
        mat_blueprint_type_id = int(request["blueprint_type_id"])
        qty_needed = int(request["quantity"])

        sub_cost = self._estimate_sub_manufacture_cost(mat_blueprint_type_id, qty_needed, phase1_data)
        market_cost = self._estimate_market_cost(mat_type_id, qty_needed, market_depth_cache)
        if sub_cost is None or market_cost is None:
            return None  # Can't compare → default to buy
        if sub_cost >= market_cost:
            return None

        mat_name = request["type_name"]
        return ItemDecision(
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
                "requested_by_type_ids": list(request["requested_by_type_ids"]),
                "sub_manufacture_cost": sub_cost,
                "market_buy_cost": market_cost,
            },
        )

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
        bp_data: dict[str, Any] | None = None,
        as_invention_enabler: bool = False,
    ) -> None:
        """Compute BPO break-even and add to bpo_opportunities.

        Break-even = BPO price / (ISK saved per planned batch by researching ME
        from the blueprint's current level to its optimal level × batches per
        day). When any input is unavailable the analysis is skipped and the
        reason is recorded on decision.bpo_analysis_skip_reason; the BPO fields stay None
        (unknown) rather than reporting a confident "hold" built on zeros.
        """
        if as_invention_enabler:
            # ME research on the T1 BPO does not change T2 material quantities;
            # its value is enabling invention, which this model does not price.
            self._skip_bpo_analysis(
                decision, "invention-enabler BPO: value is not an ME saving (not modelled)"
            )
            return

        # BPO market price from market depth cache (spot price on BPO type_id)
        bpo_cache_entry = market_depth_cache.get(bp_type_id)
        bpo_market_price: float | None = None
        if bpo_cache_entry is not None:
            spot = _get_attr(bpo_cache_entry, "spot_sell_price")
            bpo_market_price = float(spot) if spot is not None else None
        if bpo_market_price is None:
            self._skip_bpo_analysis(decision, f"no BPO market price for bp_type_id={bp_type_id}")
            return

        row = decision.overview_row
        if orow.get_material_cost_total(row) is None:
            self._skip_bpo_analysis(decision, "no material cost (no priced materials)")
            return

        saving_per_batch, reason = self._me_saving_isk_per_batch(row=row, bp_data=bp_data or {})
        if saving_per_batch is None:
            self._skip_bpo_analysis(decision, reason or "ME saving unavailable")
            return

        # effective_velocity is units/day; one planned batch makes `quantity` units.
        quantity = orow.get_product_quantity(row)
        if quantity <= 0:
            self._skip_bpo_analysis(decision, f"no batch units (quantity={quantity})")
            return
        if decision.velocity_unknown_reason is not None:
            self._skip_bpo_analysis(
                decision, f"sell velocity unknown ({decision.velocity_unknown_reason})"
            )
            return
        if decision.effective_velocity <= 0:
            self._skip_bpo_analysis(
                decision, f"zero sell velocity ({decision.effective_velocity} units/day)"
            )
            return
        planned_batches_per_day = decision.effective_velocity / quantity

        saving_per_day = saving_per_batch * planned_batches_per_day
        total_investment = bpo_market_price  # simplified (ignores research cost)
        if saving_per_day <= 0:
            break_even_days = float("inf")
        else:
            break_even_days = total_investment / saving_per_day

        projected_annual_savings = saving_per_day * 365.0

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
            # The SDE entry's own name is the blueprint's ("... Blueprint");
            # type_name above is the product's.
            "bp_type_name": str((bp_data or {}).get("type_name") or ""),
            "bpo_market_price": bpo_market_price,
            "break_even_days": break_even_days if break_even_days != float("inf") else None,
            "projected_annual_savings": projected_annual_savings,
            "recommendation": recommendation,
            "as_invention_enabler": as_invention_enabler,
        })

    def _me_saving_isk_per_batch(
        self,
        *,
        row: dict[str, Any],
        bp_data: dict[str, Any],
    ) -> tuple[float | None, str | None]:
        """ISK saved on ONE planned batch by researching ME from its current to its optimal level.

        sum over materials of
            (batch_qty(base, runs, me_current) - batch_qty(base, runs, me_target)) * unit_price
        with batch_qty = blueprints.me_adjusted_batch_quantity: the producer's
        ceiling over the whole batch, not a per-run ceiling times runs.
        me_target is the optimal ME at this batch's run count. Returns
        (None, reason) when an input is missing; never a constant.
        """
        from eve_online_industry_tracker.infrastructure.sde.blueprints import (
            me_adjusted_batch_quantity,
            optimal_me_for_quantities,
        )

        manufacturing = bp_data.get("manufacturing") if isinstance(bp_data, dict) else None
        base_materials = (manufacturing or {}).get("materials") if isinstance(manufacturing, dict) else None
        base: list[tuple[int, int]] = []
        for mat in base_materials or []:
            if not isinstance(mat, dict):
                continue
            try:
                mat_type_id = int(mat.get("type_id") or 0)
                base_qty = int(mat.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
            if mat_type_id > 0 and base_qty > 0:
                base.append((mat_type_id, base_qty))
        if not base:
            return None, "no SDE base material quantities for the blueprint"

        job = orow.get_manufacturing_job(row)
        source_kind = job.get("blueprint_source_kind")
        if source_kind == "blueprint_sde_fallback":
            # No owned blueprint: the producer assumed a max-researched ME.
            return None, "blueprint ME is assumed (blueprint_sde_fallback), not owned"
        try:
            me_current = int(job["blueprint_material_efficiency"])
        except (KeyError, TypeError, ValueError):
            return None, "unknown current blueprint ME"
        try:
            runs = int(job["runs"])
        except (KeyError, TypeError, ValueError):
            return None, "unknown batch run count (manufacturing_job.runs)"
        if runs <= 0:
            return None, f"non-positive batch run count ({runs})"

        me_target = optimal_me_for_quantities((q for _, q in base), runs=runs)

        priced = _material_unit_prices(job)
        saving = 0.0
        for mat_type_id, base_qty in base:
            unit_price = priced.get(mat_type_id)
            if unit_price is None:
                return None, f"no unit price for material type_id={mat_type_id}"
            saved_units = (
                me_adjusted_batch_quantity(base_qty, runs, me_current)
                - me_adjusted_batch_quantity(base_qty, runs, me_target)
            )
            saving += max(0, saved_units) * unit_price
        return saving, None

    @staticmethod
    def _skip_bpo_analysis(decision: ItemDecision, reason: str) -> None:
        decision.bpo_analysis_skip_reason = reason
        logger.info(
            "ChainPlanner: BPO analysis skipped for type_id=%s: %s", decision.type_id, reason
        )

    def _compute_optimal_me(self, blueprint_type_id: int, runs: int = 1) -> int | None:
        """Optimal ME at a `runs`-run batch from the SDE, or None when the SDE query fails.

        Only a database error degrades (logged at WARNING); None must never be
        read as 0 -- that would make every owned BPO look fully researched.
        """
        from eve_online_industry_tracker.infrastructure.sde.blueprints import compute_optimal_me
        try:
            sde_session = self._session_provider.sde_session()
            try:
                return compute_optimal_me(blueprint_type_id, sde_session, runs=runs)
            finally:
                sde_session.close()
        except SQLAlchemyError:
            logger.warning(
                "ChainPlanner: SDE lookup of optimal ME failed for bp_type_id=%s",
                blueprint_type_id, exc_info=True,
            )
            return None

    def _compute_optimal_te(self, blueprint_type_id: int, threshold_pct: float) -> int | None:
        """Optimal TE from the SDE, or None when the SDE query fails (see _compute_optimal_me)."""
        from eve_online_industry_tracker.infrastructure.sde.blueprints import compute_optimal_te
        try:
            sde_session = self._session_provider.sde_session()
            try:
                return compute_optimal_te(blueprint_type_id, sde_session, threshold_pct)
            finally:
                sde_session.close()
        except SQLAlchemyError:
            logger.warning(
                "ChainPlanner: SDE lookup of optimal TE failed for bp_type_id=%s",
                blueprint_type_id, exc_info=True,
            )
            return None


def _material_unit_prices(manufacturing_job: dict[str, Any]) -> dict[int, float]:
    """{material type_id: unit_price} from an overview row's manufacturing job.

    The producer prices `procurement_materials` when present and `materials`
    otherwise (industry/service.py _enrich_product_rows_with_material_prices),
    so both are read; a direct `materials` price wins.
    """
    prices: dict[int, float] = {}
    for key in ("procurement_materials", "materials"):
        entries = manufacturing_job.get(key)
        if not isinstance(entries, dict):
            continue
        for entry in entries.values():
            if not isinstance(entry, dict):
                continue
            try:
                type_id = int(entry.get("type_id") or 0)
                price = entry.get("unit_price")
                if type_id > 0 and price is not None:
                    prices[type_id] = float(price)
            except (TypeError, ValueError):
                continue
    return prices


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
