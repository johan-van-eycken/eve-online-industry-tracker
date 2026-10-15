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

from eve_online_industry_tracker.application.daily_planner import research_first
from eve_online_industry_tracker.application.daily_planner.admin_setting import admin_setting
from eve_online_industry_tracker.application.industry.invention_model import MAX_BLUEPRINT_TE as DEFAULT_MAX_BLUEPRINT_TE
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.daily_planner import facility_bonus
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
    return admin_setting(admin_settings, "daily_planner", key, fallback)


def _lowest_blueprint_by_activity_product(
    blueprint_data: dict[int, dict[str, Any]], activity: str
) -> dict[int, int]:
    """{product type_id of `activity`: lowest blueprint type_id with that product}.

    The lowest id wins whatever the dict order, matching
    blueprints.get_invention_source_blueprint_ids (ORDER BY blueprintTypeID).
    """
    index: dict[int, int] = {}
    for blueprint_type_id, entry in (blueprint_data or {}).items():
        if not isinstance(entry, dict):
            continue
        job = entry.get(activity)
        if not isinstance(job, dict):
            continue
        products = job.get("products")
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
                current = index.get(product_type_id)
                if current is None or int(blueprint_type_id) < current:
                    index[product_type_id] = int(blueprint_type_id)
    return index


def build_product_to_blueprint_index(
    blueprint_data: dict[int, dict[str, Any]]
) -> dict[int, int]:
    """{product type_id: blueprint type_id} from SDE blueprint manufacturing data.

    blueprint_data is keyed by *blueprint* type_id. Anything that starts from a
    product (a material requirement, an overview row) crosses over through
    this index. When two blueprints make the same product the lowest id wins.
    """
    return _lowest_blueprint_by_activity_product(blueprint_data, "manufacturing")


def build_invention_source_index(
    blueprint_data: dict[int, dict[str, Any]]
) -> dict[int, int]:
    """{invented blueprint type_id: source (T1) blueprint type_id}.

    In the SDE the invention activity lives on the *source* blueprint, and its
    products are the blueprints it invents into. A T2 overview row only knows
    its own (T2) blueprint, so finding the T1 BPO to copy means walking this
    index backwards. When several sources invent into one blueprint the lowest
    source id wins.
    """
    return _lowest_blueprint_by_activity_product(blueprint_data, "invention")


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
        # TE is always researched to the max (the user's decision, Task 8c fix
        # round 1): EVE's percentage, 0..20 in steps of 2.
        te_research_target = int(admin_setting(
            self._admin, "industry", "max_blueprint_time_efficiency", DEFAULT_MAX_BLUEPRINT_TE,
        ))
        invention_min_attempts = int(_adm(self._admin, "planner_invention_min_attempts", 10))

        bpo_assets_by_type_id: dict[int, list] = phase1_data.get("bpo_assets_by_type_id", {})
        bpc_assets_by_type_id: dict[int, list] = phase1_data.get("bpc_assets_by_type_id", {})
        blueprint_data: dict[int, dict] = phase1_data.get("blueprint_data", {})
        # Phase 1's merged price map (market depth, plus hub prices for the
        # materials depth does not hold), the same source as the shopping list.
        market_depth_cache: dict[int, Any] = phase1_data.get(
            "price_map", phase1_data.get("market_depth_cache", {}),
        )
        invention_success_rates: dict[int, float] = phase1_data.get("invention_success_rates", {})
        # Research first: the owned-BPC variants dedupe_by_type_id dropped,
        # and the raw market depth their profit is scored on.
        variant_rows_by_type_id: dict[int, list] = phase1_data.get("variant_rows_by_type_id", {})
        raw_market_depth: dict[int, Any] = phase1_data.get("market_depth_cache", {})
        # Built once per plan: material requirements are product type_ids, but
        # BPO ownership and blueprint_data are keyed by blueprint type_id.
        product_to_blueprint = build_product_to_blueprint_index(blueprint_data)
        invention_source = build_invention_source_index(blueprint_data)
        # Blueprints installed in an active industry job (Task 8c fix round
        # 5): EVE runs one job per blueprint, so they neither research nor
        # build. The asset snapshot can predate the job (ESI caches assets
        # longer than jobs), so they are excluded here, not trusted to have
        # dropped out of the assets.
        busy_item_ids: set[int] = {int(i) for i in (phase1_data.get("busy_blueprint_item_ids") or ())}
        free_bpo_assets_by_type_id = {
            type_id: [a for a in assets if research_first.item_id(a) not in busy_item_ids]
            for type_id, assets in bpo_assets_by_type_id.items()
        } if busy_item_ids else bpo_assets_by_type_id
        # The same for BPCs (Task 10 carry 12), and the blueprint types with a
        # BPO / BPC in a job, which covers a blueprint the snapshot dropped.
        free_bpc_assets_by_type_id = {
            type_id: [a for a in assets if research_first.item_id(a) not in busy_item_ids]
            for type_id, assets in bpc_assets_by_type_id.items()
        } if busy_item_ids else bpc_assets_by_type_id
        industry_jobs = phase1_data.get("industry_jobs") or []
        busy_bpo_type_ids = research_first.busy_blueprint_type_ids(industry_jobs, copies=False) | {
            t for t, assets in bpo_assets_by_type_id.items()
            if any(research_first.item_id(a) in busy_item_ids for a in assets)
        }
        busy_bpc_type_ids = research_first.busy_blueprint_type_ids(industry_jobs, copies=True) | {
            t for t, assets in bpc_assets_by_type_id.items()
            if any(research_first.item_id(a) in busy_item_ids for a in assets)
        }

        def _on_busy_bpc(candidate: dict[str, Any]) -> bool:
            return self._row_on_busy_bpc(candidate, busy_item_ids, busy_bpc_type_ids) is not None
        # Each parent's sub-manufacture requests, in order, and (research
        # first) the ones its kept BPO row would make instead.
        requests_by_parent: list[tuple[int, list[dict[str, Any]]]] = []
        fallback_requests: dict[int, list[dict[str, Any]]] = {}

        for decision in top_level_decisions:
            if decision.decision != "build":
                continue

            row = decision.overview_row
            meta_group_id = decision.meta_group_id
            # Nested under manufacturing_job.blueprint_sde on a real row; there
            # is no top-level blueprint_type_id key.
            bp_type_id = orow.get_blueprint_type_id(row) or 0
            bp_data = blueprint_data.get(bp_type_id, {})
            # Research first never builds from a variant on a busy BPC.
            variants = [
                v for v in variant_rows_by_type_id.get(int(decision.type_id)) or []
                if not _on_busy_bpc(v)
            ]
            busy_bpc_reason = self._row_on_busy_bpc(row, busy_item_ids, busy_bpc_type_ids)
            if busy_bpc_reason is not None:
                self._mark_not_built(decision, busy_bpc_reason)

            try:
                if meta_group_id == META_GROUP_T1:
                    bpo_plan = self._plan_t1_chain(
                        decision=decision,
                        bp_type_id=bp_type_id,
                        bp_data=bp_data,
                        bpo_assets_by_type_id=bpo_assets_by_type_id,
                        bpc_assets_by_type_id=free_bpc_assets_by_type_id,
                        market_depth_cache=market_depth_cache,
                        bpo_opportunities=bpo_opportunities,
                        te_research_target=te_research_target,
                        busy_item_ids=busy_item_ids,
                        bpo_strong_buy_days=bpo_strong_buy_days,
                        bpo_consider_days=bpo_consider_days,
                    )
                    self._apply_research_first(
                        decision, bp_type_id, bpo_plan, variants,
                        raw_market_depth.get(int(decision.type_id)),
                        bp_data=bp_data, market_depth_cache=market_depth_cache,
                    )
                elif busy_bpc_reason is not None:
                    # Priced on the busy BPC's runs: inventing instead would
                    # buy no datacores for them (an under-buy). Not built.
                    decision.pipeline_stage = "manufacturing"
                else:
                    # T2+ — invention chain only
                    self._plan_t2_chain(
                        decision=decision,
                        bp_type_id=bp_type_id,
                        bp_data=bp_data,
                        blueprint_data=blueprint_data,
                        invention_source=invention_source,
                        bpo_assets_by_type_id=free_bpo_assets_by_type_id,
                        bpc_assets_by_type_id=free_bpc_assets_by_type_id,
                        busy_bpo_type_ids=busy_bpo_type_ids,
                        market_depth_cache=market_depth_cache,
                        invention_success_rates=invention_success_rates,
                        bpo_opportunities=bpo_opportunities,
                        bpo_strong_buy_days=bpo_strong_buy_days,
                        bpo_consider_days=bpo_consider_days,
                    )

                # Which of this batch's materials the corp could build itself.
                # Collected across every parent first, so one material needed
                # by several builds becomes ONE sub-manufacture decision.
                requests = self._collect_sub_manufacture_requests(
                    decision=decision,
                    bpo_assets_by_type_id=free_bpo_assets_by_type_id,
                    product_to_blueprint=product_to_blueprint,
                )
                requests_by_parent.append((int(decision.type_id), requests))
                kept_row = self._research_first_fallback_row(decision.overview_row)
                if kept_row is not None:
                    fallback_requests[int(decision.type_id)] = self._collect_sub_manufacture_requests(
                        decision=decision, row=kept_row,
                        bpo_assets_by_type_id=free_bpo_assets_by_type_id,
                        product_to_blueprint=product_to_blueprint,
                    )

            except (TypeError, ValueError):
                # Per-item isolation for malformed SDE / market values (int()
                # and float() conversions); anything else is a bug and aborts.
                logger.exception("ChainPlanner: error planning chain for type_id=%s", decision.type_id)

        def _sub_decisions(fallen: set[int]) -> list[ItemDecision]:
            per_parent = [
                (parent, fallback_requests[parent] if parent in fallen and parent in fallback_requests else requests)
                for parent, requests in requests_by_parent
            ]
            return self._resolve_sub_requests(
                self._merge_sub_requests(per_parent), market_depth_cache, phase1_data,
            )

        all_decisions.extend(_sub_decisions(set()))

        # Logged once, and only for components that are actually sub-built.
        unknown_rig = sorted(
            d.type_id for d in all_decisions
            if d.is_sub_component and d.overview_row.get("rig_applicability") == "unknown"
        )
        if unknown_rig:
            logger.warning(
                "ChainPlanner: rig applicability unknown for sub-manufactured type_ids=%s "
                "(component group not known for sure, or only an aggregate rig bonus "
                "on the profile); applying the structure bonus only, so their input "
                "quantities are an upper bound",
                unknown_rig,
            )

        return ChainPlan(
            decisions=all_decisions,
            bpo_opportunities=bpo_opportunities,
            sub_manufacture_actions=sub_manufacture_actions,
            resize_sub_components=_sub_decisions,
        )

    @staticmethod
    def _merge_sub_requests(
        per_parent: list[tuple[int, list[dict[str, Any]]]],
    ) -> dict[int, dict[str, Any]]:
        """{material type_id: one merged request}, in first-seen order."""
        sub_requests: dict[int, dict[str, Any]] = {}
        for parent_type_id, requests in per_parent:
            for request in requests:
                merged = sub_requests.setdefault(request["type_id"], {
                    **request, "quantity": 0, "requested_by_type_ids": [],
                })
                merged["quantity"] += request["quantity"]
                merged["requested_by_type_ids"].append(parent_type_id)
                # One sub job serves every requester. If their structures
                # differ, take the smallest facility bonus so no parent's
                # share is under-bought.
                if facility_bonus.facility_reduction(request) < facility_bonus.facility_reduction(merged):
                    merged.update({k: request[k] for k in facility_bonus.FACILITY_KEYS})
        return sub_requests

    def _resolve_sub_requests(
        self,
        sub_requests: dict[int, dict[str, Any]],
        market_depth_cache: dict[int, Any],
        phase1_data: dict[str, Any],
    ) -> list[ItemDecision]:
        decisions: list[ItemDecision] = []
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
                decisions.append(sub_decision)
        return decisions

    @staticmethod
    def _row_on_busy_bpc(
        row: dict[str, Any], busy_item_ids: set[int], busy_bpc_type_ids: set[int],
    ) -> str | None:
        """Why the row's owned-BPC runs are not available, or None.

        The producer counts the runs of every owned BPC in the asset
        snapshot, which can still list a BPC installed in a job. The row
        names its BPCs (row_bpc_item_ids); a row that names none while a BPC
        of its blueprint is in a job cannot be shown to be free."""
        if not research_first.builds_on_owned_bpcs(row):
            return None
        item_ids = research_first.row_bpc_item_ids(row)
        if item_ids:
            busy = sorted(item_ids & busy_item_ids)
            return f"BPC in an active industry job (item {', '.join(map(str, busy))})" if busy else None
        if (orow.get_blueprint_type_id(row) or 0) in busy_bpc_type_ids:
            return "BPC in an active industry job (the row does not name its BPCs)"
        return None

    @staticmethod
    def _mark_not_built(decision: ItemDecision, reason: str) -> None:
        """Research first's "not built" plan, unconditional like a busy BPO
        (a blueprint it needs is in an active job): no manufacture action and
        no sub-builds, so nothing is bought for it."""
        decision.overview_row["research_first"] = {
            "mode": "none", "note": f"{reason}: not built from it in this plan",
            "sized_note": "its components were not sub-built in this plan",
            "unconditional": True,
        }
        logger.info("ChainPlanner: type_id=%s: %s: not built from it in this plan", decision.type_id, reason)

    @staticmethod
    def _research_first_fallback_row(row: dict[str, Any]) -> dict[str, Any] | None:
        """The row the parent builds from when its planned research gets no
        slot (CharacterAssigner), when that differs from the plan: the kept
        BPO row of a variant, else the row without its research-first plan.
        None when the plan does not depend on a research slot."""
        first = row.get("research_first")
        if not isinstance(first, dict) or first.get("unconditional") or first.get("mode") == "other_bpo":
            return None
        return first.get("kept_row") or {k: v for k, v in row.items() if k != "research_first"}

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
        te_research_target: int,
        bpo_strong_buy_days: float,
        bpo_consider_days: float,
        busy_item_ids: set[int] = frozenset(),
    ) -> dict[str, Any] | None:
        """Plan T1 chain: BPO path, ME/TE research, or BPC/shopping fallback.

        Returns the owned-BPO plan ({assets (producer order), priced, research
        (index or None), busy (indices of BPOs in an active job)}) for
        research first, or None without an owned BPO."""
        bpo_owned = bp_type_id in bpo_assets_by_type_id and bool(bpo_assets_by_type_id[bp_type_id])

        if bpo_owned:
            # Compute optimal ME/TE via SDE lookup. The optimal ME depends on the
            # batch run count; with no validated manufacturing_job.runs it is
            # unknown (never guessed from quantity) and no ME research is scheduled.
            batch_runs, runs_reason = _batch_runs(
                orow.get_manufacturing_job(decision.overview_row)
            )
            if batch_runs is None:
                logger.warning(
                    "ChainPlanner: owned BPO %s has an unusable batch run count (%s); "
                    "optimal ME unknown, not scheduling ME research",
                    bp_type_id, runs_reason,
                )
                optimal_me = None
            else:
                optimal_me = self._compute_optimal_me(bp_type_id, runs=batch_runs)
            optimal_te = te_research_target

            # The producer's BPO order, so the choice is deterministic. The
            # BPO researched (Task 8c fix round 4): the one the row is priced
            # from when it needs research, else the first other BPO that
            # does; the row then builds from the priced BPO as usual.
            bpo_assets = research_first.producer_bpo_order(bpo_assets_by_type_id[bp_type_id])
            priced = research_first.priced_bpo_index(decision.overview_row, bpo_assets)

            busy = {i for i, a in enumerate(bpo_assets) if research_first.item_id(a) in busy_item_ids}

            def _needs_research(asset: Any) -> bool:
                # A BPO installed in an active job cannot start a research job.
                if research_first.item_id(asset) in busy_item_ids:
                    return False
                me = _blueprint_efficiency(asset, "material")
                te = _blueprint_efficiency(asset, "time")
                return (me is not None and optimal_me is not None and me < optimal_me) or (
                    te is not None and te < optimal_te
                )

            research_index = next(
                (i for i in [priced, *(j for j in range(len(bpo_assets)) if j != priced)]
                 if _needs_research(bpo_assets[i])),
                None,
            )
            subject = bpo_assets[research_index if research_index is not None else priced]
            # Corp asset columns are blueprint_*-prefixed. None means unknown,
            # not ME0: an unknown level schedules no research rather than a
            # job on a blueprint that may already be fully researched.
            current_me = _blueprint_efficiency(subject, "material")
            current_te = _blueprint_efficiency(subject, "time")
            # A busy BPO is not researched anyway, and the busy state is the
            # reason (carry 16): no WARNING about its unknown levels.
            subject_busy = research_first.item_id(subject) in busy_item_ids
            if (current_me is None or current_te is None) and not subject_busy:
                logger.warning(
                    "ChainPlanner: owned BPO %s has unknown ME/TE (me=%s, te=%s); "
                    "not scheduling research for the unknown level(s)",
                    bp_type_id, current_me, current_te,
                )

            # None means no optimal ME: schedule no ME research rather than
            # treating the optimum as 0, which silently reads as "already
            # optimal". The TE target is the setting and always known. When
            # the missing batch runs caused it, that WARNING was logged above
            # and this one is skipped; an SDE lookup failure (runs fine)
            # still gets this WARNING on top of _compute_optimal_me's own.
            if optimal_me is None and runs_reason is None:
                logger.warning(
                    "ChainPlanner: optimal ME unknown for owned BPO %s; "
                    "not scheduling ME research",
                    bp_type_id,
                )

            # Schedule ME/TE research in the action plan (signals to
            # ActionPlanBuilder), only on a BPO chosen for research: never on
            # one in an active job.
            if research_index is None:
                current_me = current_te = None
            if current_me is not None and optimal_me is not None and current_me < optimal_me:
                decision.overview_row["needs_me_research"] = True
                decision.overview_row["me_research_target"] = optimal_me
                decision.overview_row["me_current"] = current_me

            # TE is EVE's percentage on both sides (0..20, 2% per research
            # level): the asset's blueprint_time_efficiency and the max TE
            # setting. CharacterAssigner converts to research levels for the
            # fee and the duration.
            if current_te is not None and current_te < optimal_te:
                decision.overview_row["needs_te_research"] = True
                decision.overview_row["te_research_target"] = optimal_te
                decision.overview_row["te_current"] = current_te

            decision.pipeline_stage = "manufacturing"
            if priced in busy:
                logger.info(
                    "ChainPlanner: type_id=%s: the BPO its row is priced from (item %s) is in an "
                    "active industry job; not building from it in this plan",
                    decision.type_id, research_first.item_id(bpo_assets[priced]),
                )
            return {"assets": bpo_assets, "priced": priced, "research": research_index, "busy": busy}

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
        busy_bpo_type_ids: set[int] = frozenset(),
    ) -> None:
        """Plan T2+ chain: invention pipeline or BPC fallback.

        `bpo_assets_by_type_id` / `bpc_assets_by_type_id` hold only the
        blueprints not installed in an active job. A T1 BPO that is in a job
        (`busy_bpo_type_ids`, also when the snapshot dropped it) gets no copy
        and is not flagged for purchase: it is owned, only busy."""
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

        if not t1_bpo_owned and t1_bp_type_id is not None and t1_bp_type_id in busy_bpo_type_ids:
            decision.pipeline_stage = "copying"
            self._skip_bpo_analysis(
                decision,
                f"the T1 BPO ({t1_bp_type_id}) is in an active industry job: no copy in this plan; "
                "it is owned, so not a purchase",
            )
            # No copy, so no invention and no T2 BPC to build from.
            self._mark_not_built(decision, "T1 BPO in an active industry job")
        elif t1_bpo_owned:
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

    def _apply_research_first(
        self,
        decision: ItemDecision,
        bp_type_id: int,
        bpo_plan: dict[str, Any] | None,
        variants: list[dict[str, Any]],
        market_depth: Any | None,
        *,
        bp_data: dict[str, Any] | None = None,
        market_depth_cache: dict[int, Any] | None = None,
    ) -> None:
        """Research first (see research_first.py): when research is scheduled
        on the BPO this batch builds from or copies, build from an owned-BPC
        variant row, else the owned-BPC runs only, else nothing. Recorded on
        the row as `research_first` ({mode: variant | part | none, note,
        ...}); decided here, before the sub-components are sized, so they
        and the manufacture action follow the same decision.

        It is a plan, not a fact: CharacterAssigner applies it only when a
        research action is actually emitted for the BPO, and otherwise builds
        from the kept BPO row (`kept_row` for a variant; the row itself
        otherwise).

        Several BPOs of the type (Task 8c fix round 4): when the researched
        BPO is not the one the row is priced from, the row builds as usual
        (mode other_bpo). When it is, the batch builds from another BPO: at
        the same ME/TE from the row itself, else from that BPO's own variant
        row (its materials, cost and profit), else (fix round 5: the producer
        emits no per-BPO row with build-from-BPC on) sized by the planner
        from the SDE at that BPO's ME (_size_on_other_bpo); never at the
        priced BPO's ME.

        A priced BPO installed in an active industry job (fix round 5) is
        unavailable the same way, but unconditionally (`unconditional`): it
        does not wait for a research slot."""
        row = decision.overview_row
        if not bpo_plan or not research_first.batch_uses_bpo(row):
            return
        if isinstance(row.get("research_first"), dict):
            return  # already not built: its owned BPC is in an active job
        assets, priced, research = bpo_plan["assets"], bpo_plan["priced"], bpo_plan["research"]
        busy: set[int] = set(bpo_plan.get("busy") or ())
        researching = research is not None and bool(
            row.get("needs_me_research") or row.get("needs_te_research")
        )
        priced_busy = priced in busy
        if not priced_busy and not researching:
            return
        if not priced_busy and research != priced:
            build = research_first.describe_bpo(assets[priced])
            row["research_first"] = {
                "mode": "other_bpo",
                "note": f"researching another BPO ({research_first.describe_bpo(assets[research])}); "
                        f"the batch builds from the BPO at {build}",
                "build_note": f"built from the BPO at {build}",
            }
            logger.info("ChainPlanner: type_id=%s: %s", decision.type_id, row["research_first"]["note"])
            return
        cause = "BPO in an active industry job" if priced_busy else "BPO in research"
        # Busy is a fact of this plan, research a plan that needs a slot.
        flags = {"unconditional": True} if priced_busy else {}
        unavailable = busy | ({research} if researching else set())

        def _log(note: str) -> None:
            if priced_busy:
                logger.info("ChainPlanner: type_id=%s: %s", decision.type_id, note)
            else:
                logger.info(
                    "ChainPlanner: type_id=%s: research planned on BPO %s; if its research gets a slot: %s",
                    decision.type_id, bp_type_id, note,
                )

        unsized: list[str] = []
        for other in (i for i in range(len(assets)) if i != priced and i not in unavailable):
            build = research_first.describe_bpo(assets[other])
            if research_first.bpo_levels(assets[other]) == research_first.bpo_levels(assets[priced]):
                note = (
                    f"{cause}: the batch builds from another BPO at {build}" if priced_busy
                    else f"researching one of {len(assets)} BPOs; the batch builds from another at {build}"
                )
                row["research_first"] = {
                    "mode": "other_bpo", "note": note,
                    "build_note": f"built from another BPO at {build}", **flags,
                }
                _log(note)
                return
            other_variant = research_first.variant_for_bpo(variants, bp_type_id, assets[other])
            sized_reason = None
            if other_variant is not None:
                new_row = self._research_first_variant_row(row, other_variant)
                profit, reason = _variant_profit(other_variant, market_depth)
                note = f"{cause}: built from another BPO at {build}"
            else:
                new_row, sized_reason, bonus_wording = self._size_on_other_bpo(
                    row, assets[other], bp_data or {}, market_depth_cache or {},
                )
                if new_row is None:
                    unsized.append(f"the BPO at {build}: {sized_reason}")
                    continue
                profit, reason = (
                    _variant_profit(new_row, market_depth)
                    if orow.get_material_cost_total(new_row) is not None
                    else (None, "a material of the batch has no market price")
                )
                note = (
                    f"{cause}: built from another BPO at {build} (its BPO runs sized by the planner "
                    f"from the SDE at that ME, {bonus_wording})"
                )
            new_row["research_first"] = {
                "mode": "variant", "note": note,
                "profit": profit, "profit_unknown_reason": reason,
                "kept_row": row,
                "sized_note": f"its components were sized for the BPO at {build}; "
                              "the shopping list buys any shortfall, any surplus stays in stock",
                **flags,
            }
            decision.overview_row = new_row
            _log(note)
            return
        unsized_note = f"; another BPO could not size the batch ({'; '.join(unsized)})" if unsized else ""
        variant = research_first.best_bpc_variant(variants, bp_type_id)
        total_runs = orow.get_manufacturing_job(row).get("runs")
        if variant is not None:
            new_row = self._research_first_variant_row(row, variant)
            profit, reason = _variant_profit(variant, market_depth)
            new_row["research_first"] = {
                "mode": "variant",
                "note": f"{cause}: built from the owned-BPC variant{unsized_note}",
                "profit": profit, "profit_unknown_reason": reason,
                "kept_row": row,
                "sized_note": "its components were sized for the owned-BPC variant's batch; "
                              "the shopping list buys any shortfall, any surplus stays in stock",
                **flags,
            }
            decision.overview_row = new_row
            note = new_row["research_first"]["note"]
        else:
            part = research_first.best_part([row, *variants])
            if part is not None:
                row["research_first"] = {
                    "mode": "part", **part,
                    "note": f"{cause}: built from the owned BPC runs only ({part['runs']} of "
                            f"{total_runs} runs){unsized_note}",
                    "cost_unknown_reason": research_first.PART_COST_UNKNOWN_REASON,
                    "sized_note": f"its components were sized for {part['runs']} of {total_runs} runs; "
                                  f"the shopping list buys the rest",
                    **flags,
                }
            else:
                row["research_first"] = {
                    "mode": "none", "note": f"{cause}: not built from it in this plan{unsized_note}",
                    "sized_note": "its components were not sub-built in this plan; the shopping list buys them",
                    **flags,
                }
            note = row["research_first"]["note"]
        _log(note)

    def _size_on_other_bpo(
        self,
        row: dict[str, Any],
        asset: Any,
        bp_data: dict[str, Any],
        market_depth_cache: dict[int, Any],
    ) -> tuple[dict[str, Any] | None, str | None, str | None]:
        """The kept row rebuilt on another owned BPO that has no producer row
        of its own (Task 8c fix round 5): (row, None, which bonuses were
        applied, for the note), or (None, why, None).

        The row's owned-BPC job segments keep their own materials; the runs
        it builds from (or copies from) the BPO are built from this BPO
        instead, sized like a sub job (_sub_batch): SDE base quantities x
        runs, split at the blueprint's max runs per job, each job rounded
        up (blueprints.reduced_batch_quantity) at the producer's combination
        of [this BPO's ME, the structure bonus, the rig bonus] from the
        row's industry profile (facility_bonus). A rig whose coverage of the
        product is unknown adds only an "All" rig, so the result is an upper
        bound, never an under-buy. Priced with _price_materials (unknown
        when a line has no price). The manufacturing fee is EIV-based (ME 0
        inputs, job_cost.py), so the same runs keep the row's fee; the
        row's other job fees stay in total_cost.
        """
        from eve_online_industry_tracker.infrastructure.sde.blueprints import reduced_batch_quantity

        me, te = research_first.bpo_levels(asset)
        if me is None:
            return None, "its ME is unknown", None
        manufacturing = bp_data.get("manufacturing") if isinstance(bp_data, dict) else None
        if not isinstance(manufacturing, dict):
            return None, "no SDE manufacturing data for the blueprint", None
        base_materials: dict[int, int] = {}
        for mat in manufacturing.get("materials") or []:
            if isinstance(mat, dict) and int(mat.get("type_id") or 0) > 0 and int(mat.get("quantity") or 0) > 0:
                base_materials[int(mat["type_id"])] = base_materials.get(int(mat["type_id"]), 0) + int(mat["quantity"])
        if not base_materials:
            return None, "no SDE manufacturing data for the blueprint (no materials)", None
        max_runs = _max_runs_per_job(bp_data)
        if max_runs is None:
            return None, "no SDE manufacturing data for the blueprint (no max runs per job)", None
        job = orow.get_manufacturing_job(row)
        profile = job.get("industry_profile")
        if not isinstance(profile, dict):
            return None, "no industry profile on the row, so the structure bonus is unknown", None

        segments = research_first.job_segments(row)
        if segments is None:
            runs, reason = _batch_runs(job)
            if runs is None:
                return None, reason, None
            segments = [{"runs": runs, "source": "owned_blueprint_original"}]
        kept_segments: list[dict[str, Any]] = []
        bpo_runs = 0
        for segment in segments:
            if str(segment.get("source")) in research_first.BPO_SOURCES:
                bpo_runs += int(segment.get("runs") or 0)
            elif isinstance(segment.get("materials"), dict):
                kept_segments.append(segment)
            else:
                return None, "a job segment's materials are unknown", None
        if bpo_runs <= 0:
            return None, "the batch has no BPO runs", None

        product_type_id = int(row.get("type_id") or 0)
        product_entry = next(
            (p for p in (job.get("products") or [])
             if isinstance(p, dict) and int(p.get("type_id") or 0) == product_type_id),
            row,
        )
        bonuses = facility_bonus.sub_job_facility_reductions(profile, product_entry)
        reduction = facility_bonus.combine_reductions([
            float(me) / 100.0, bonuses["structure_material_reduction"], bonuses["rig_material_reduction"],
        ])
        full_jobs, remainder = divmod(bpo_runs, max_runs)
        new_segments = [dict(s) for s in kept_segments]
        for runs_in_job in [max_runs] * full_jobs + ([remainder] if remainder else []):
            new_segments.append({
                "runs": runs_in_job, "source": "owned_blueprint_original",
                "blueprint_material_efficiency": me, "blueprint_time_efficiency": te,
                "material_reduction": reduction,
                "materials": {
                    str(t): reduced_batch_quantity(q, runs_in_job, reduction) for t, q in base_materials.items()
                },
            })
        batch: dict[int, int] = {}
        for segment in new_segments:
            for type_id, quantity in segment["materials"].items():
                batch[int(type_id)] = batch.get(int(type_id), 0) + int(quantity)
        batch = {t: q for t, q in batch.items() if q > 0}

        entries = {
            int(e.get("type_id") or 0): e
            for e in (job.get("materials") or {}).values() if isinstance(e, dict)
        }
        names = {int(m.get("type_id") or 0): m.get("type_name") for m in manufacturing.get("materials") or []
                 if isinstance(m, dict)}
        materials = {
            str(t): {**(entries.get(t) or {"type_id": t, "type_name": names.get(t) or f"type_{t}"}), "quantity": q}
            for t, q in batch.items()
        }
        cost = self._price_materials(batch, market_depth_cache)
        kept_material = orow.get_material_cost_total(row)
        kept_total = orow.get_total_cost(row)
        other_fees = kept_total - kept_material if kept_total is not None and kept_material is not None else None
        new_job = {
            **job, "materials": materials, "job_segments": new_segments,
            "material_cost": cost,
            "total_cost": cost + other_fees if cost is not None and other_fees is not None else None,
            "blueprint_source_kind": job.get("blueprint_source_kind"),
            "blueprint_original": {"item_id": research_first.item_id(asset)},
            "rig_applicability": bonuses["rig_applicability"],
        }
        return {**row, "manufacturing_job": new_job}, None, _sizing_bonus_wording(
            bonuses["structure_material_reduction"], bonuses["rig_material_reduction"],
        )

    @staticmethod
    def _research_first_variant_row(row: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
        """The owned-BPC variant, carrying the kept row's research scheduling
        and, where the variant lacks them, its research fee/duration tables
        (the same BPO's)."""
        kept_job = orow.get_manufacturing_job(row)
        job = dict(orow.get_manufacturing_job(variant))
        for key in ("research_job_cost_by_level", "research_job_cost_by_level_unknown_reason",
                    "research_duration_seconds_by_level", "research_duration_seconds_by_level_unskilled"):
            if key not in job and key in kept_job:
                job[key] = kept_job[key]
        new_row = {**variant, "manufacturing_job": job}
        new_row.update({k: row[k] for k in research_first.RESEARCH_KEYS if k in row})
        return new_row

    def _collect_sub_manufacture_requests(
        self,
        *,
        decision: ItemDecision,
        bpo_assets_by_type_id: dict[int, list],
        product_to_blueprint: dict[int, int],
        row: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """This batch's materials that the corp owns a BPO for, with the
        quantity the batch actually consumes. `row` is the parent row to
        size from (default: the decision's).

        The quantity is the producer's manufacturing_job.materials entry:
        already per-run x runs and after ME/structure reduction. The SDE's
        per-run quantity (what this used to read) is one run at ME0, which
        under-built the component by the run count. Without the producer's
        figures there is no correct amount, so nothing is requested and the
        parent buys the component (logged).

        Each request also carries the owned BPO's ME and the material bonuses
        the sub job gets in this parent's structure
        (facility_bonus.sub_job_facility_reductions).
        """
        row = decision.overview_row if row is None else row
        first = row.get("research_first")
        mode = first.get("mode") if isinstance(first, dict) else None
        if mode == "none":
            # Research first: the parent is not built, so neither are its components.
            return []
        batch_materials = (
            dict(first["materials"]) if mode == "part"
            else orow.get_batch_materials(row)
        )
        if batch_materials is None:
            logger.warning(
                "ChainPlanner: type_id=%s has no producer batch materials "
                "(manufacturing_job.materials); skipping sub-manufacture analysis, "
                "its components are bought",
                decision.type_id,
            )
            return []

        manufacturing_job = orow.get_manufacturing_job(row)
        entries = {
            int(e.get("type_id") or 0): e
            for e in (manufacturing_job.get("materials") or {}).values()
            if isinstance(e, dict)
        }
        names = {t: e.get("type_name") for t, e in entries.items()}
        # The sub job runs in the parent's structure (ruling F-Q2), so its
        # facility bonuses come from the parent's industry profile.
        profile = manufacturing_job.get("industry_profile")
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
                # Lowest known ME of the owned BPOs; None = none known, so resolution buys.
                "blueprint_me": _lowest_known_me(bpo_assets_by_type_id[mat_blueprint_type_id]),
                "quantity": quantity,
                # The component's fee per run as the parent's producer row
                # priced it (fix round 1): used when the component has no
                # overview row of its own.
                "parent_fee_per_run": _non_negative((entries.get(mat_type_id) or {}).get("component_job_fee_per_run")),
                # Why the parent's producer could not price it (8b fix round 1).
                "parent_fee_unknown_reason": (entries.get(mat_type_id) or {}).get("component_job_fee_per_run_unknown_reason"),
                **facility_bonus.sub_job_facility_reductions(profile, entries.get(mat_type_id)),
            })
        return requests

    def _resolve_sub_manufacture(
        self,
        *,
        request: dict[str, Any],
        market_depth_cache: dict[int, Any],
        phase1_data: dict[str, Any],
    ) -> ItemDecision | None:
        """Build vs buy for one merged material request, net of corp stock.

        Returns a sub-component ItemDecision (decision='build') when building
        the part corp stock does not cover is cheaper than buying it, else
        None (buy). These bypass the Phase 4 gate per the two-pass spec.
        ShoppingListBuilder subtracts the built quantity from the parents'
        purchases and nets the rest against the same corp stock.
        """
        mat_type_id = int(request["type_id"])
        mat_blueprint_type_id = int(request["blueprint_type_id"])
        qty_requested = int(request["quantity"])

        in_stock = max(0, int((phase1_data.get("corp_material_stock") or {}).get(mat_type_id, 0)))
        qty_to_build = qty_requested - in_stock
        if qty_to_build <= 0:
            logger.info(
                "ChainPlanner: type_id=%s needs %d, corp stock holds %d; not sub-manufacturing",
                mat_type_id, qty_requested, in_stock,
            )
            return None

        me = request.get("blueprint_me")
        if me is None:
            logger.warning(
                "ChainPlanner: owned BPO %s for type_id=%s has unknown ME; buying the "
                "component instead of sub-manufacturing it",
                mat_blueprint_type_id, mat_type_id,
            )
            return None

        batch = self._sub_batch(
            product_type_id=mat_type_id, blueprint_type_id=mat_blueprint_type_id,
            qty_to_build=qty_to_build, me=int(me), phase1_data=phase1_data,
            facility_reductions=(
                float(request.get("structure_material_reduction") or 0.0),
                float(request.get("rig_material_reduction") or 0.0),
            ),
        )
        if batch is None:
            return None
        runs, batch_materials, material_reduction, jobs = batch

        sub_cost = self._price_materials(batch_materials, market_depth_cache)
        market_cost = self._estimate_market_cost(mat_type_id, qty_to_build, market_depth_cache)
        if sub_cost is None or market_cost is None:
            return None  # Can't compare -> default to buy
        # The install fee per run: the component's own overview row, else the
        # figure its parent published (D4, fix round 1). Building pays it, so
        # build-vs-buy counts it. Unknown: compared on materials alone, as
        # before, and the fee shows unknown in Capital Reserved.
        fee_per_run = self._component_fee_per_run(mat_type_id, phase1_data)
        if fee_per_run is None:
            fee_per_run = request.get("parent_fee_per_run")
        install_fee = fee_per_run * runs if fee_per_run is not None else None
        # Why it is unknown: the component's own row, else its parent's
        # figure; None = neither recorded one (an old overview: refresh).
        fee_unknown_reason = (
            (self._component_fee_unknown_reason(mat_type_id, phase1_data)
             or _str_or_none(request.get("parent_fee_unknown_reason")))
            if install_fee is None else None
        )
        if install_fee is None:
            logger.warning(
                "ChainPlanner: install fee unknown for sub-manufacturing type_id=%s (%s); "
                "build vs buy compares materials only, so building may look cheaper than it is",
                mat_type_id, fee_unknown_reason or "no fee published; refresh the product overview",
            )
        if sub_cost + (install_fee or 0.0) >= market_cost:
            return None
        # Linear pricing, so the per-job costs sum to sub_cost exactly.
        job_batches = [
            {
                **job,
                "cost": self._price_materials(job["materials"], market_depth_cache),
                "fee": fee_per_run * job["runs"] if fee_per_run is not None else None,
                "fee_unknown_reason": fee_unknown_reason,
            }
            for job in jobs
        ]

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
            # A sub-component is consumed by its parent, not sold, so it has
            # no sell rate. The reason makes phase 9 persist NULL instead of
            # this placeholder (market_depth_collector reads that column as
            # a measured velocity). BPO analysis never runs for subs.
            effective_velocity=1.0,
            velocity_unknown_reason="sub-component: no own sell velocity",
            meta_group_id=META_GROUP_T1,
            pipeline_stage="manufacturing",
            is_sub_component=True,
            overview_row={
                "type_id": mat_type_id,
                "type_name": mat_name,
                "blueprint_type_id": mat_blueprint_type_id,
                "quantity_requested": qty_requested,
                "corp_stock_used": min(in_stock, qty_requested),
                "quantity_needed": qty_to_build,
                "sub_runs": runs,
                # The runs split into jobs of at most the blueprint's max runs.
                "sub_max_runs_per_job": max(j["runs"] for j in job_batches),
                # One entry per job, each taking its own manufacturing slot
                # (CharacterAssigner): runs, units output, materials, cost.
                "sub_job_batches": job_batches,
                "sub_batch_materials": batch_materials,
                # Combined ME + structure + rig fraction the batch was cut by.
                "sub_material_reduction": material_reduction,
                "rig_applicability": request.get("rig_applicability", "no_profile"),
                "requested_by_type_ids": list(request["requested_by_type_ids"]),
                "sub_manufacture_cost": sub_cost,
                # The jobs' install fees (None = unknown), in build vs buy.
                "sub_install_fee": install_fee,
                "sub_install_fee_unknown_reason": fee_unknown_reason,
                "market_buy_cost": market_cost,
            },
        )

    @staticmethod
    def _component_fee_unknown_reason(type_id: int, phase1_data: dict[str, Any]) -> str | None:
        """The producer's reason the component's own row has no fee, or None."""
        for row in phase1_data.get("overview_rows") or []:
            if isinstance(row, dict) and int(row.get("type_id") or 0) == int(type_id):
                reason = orow.get_manufacturing_fee_unknown_reason(row)
                if reason is not None:
                    return reason
        return None

    @staticmethod
    def _component_fee_per_run(type_id: int, phase1_data: dict[str, Any]) -> float | None:
        """The manufacturing install fee of one run of the component, from its
        own overview row: the same refresh and industry profile, which is the
        parent's structure (F-Q2). The fee is EIV-based, so linear in runs and
        independent of ME. None when no kept row of it has a priced fee:
        unknown, not free (CharacterAssigner logs it)."""
        for row in phase1_data.get("overview_rows") or []:
            if isinstance(row, dict) and int(row.get("type_id") or 0) == int(type_id):
                fee = orow.get_manufacturing_fee_per_run(row)
                if fee is not None:
                    return fee
        return None

    def _sub_batch(
        self,
        *,
        product_type_id: int,
        blueprint_type_id: int,
        qty_to_build: int,
        me: int,
        phase1_data: dict[str, Any],
        facility_reductions: tuple[float, ...] = (),
    ) -> tuple[int, dict[int, int], float, list[dict[str, Any]]] | None:
        """(runs, {material type_id: units for all the sub jobs}, combined
        material reduction, jobs) at the owned BPO's ME in the parent's
        structure. Each job is {"runs", "output", "quantity", "materials"}:
        its runs, the units it makes (runs x output per run), the units it
        counts toward the need (output, the last job capped at what is still
        needed so the jobs sum to qty_to_build) and its own batch; the jobs'
        materials sum to the total.

        One job runs at most the blueprint's max_production_limit (SDE
        maxProductionLimit) runs, so the runs are split into full jobs at the
        limit plus a remainder job. Each job rounds its own batch up, so the
        materials are the sum of the per-job batches: one big batch would
        under-buy by up to (jobs - 1) units per material. An unknown limit
        means the split is unknown, so the component is bought (WARNING).

        Base quantities are the SDE's per-run figures. The reduction is the
        producer's own combination (facility_bonus.combine_reductions of
        [me / 100, structure, rig]), applied with its batch ceiling and
        per-run floor (blueprints.reduced_batch_quantity).
        `facility_reductions` are the parent structure's material bonus and
        the rig bonus for the component's own group (see
        facility_bonus.sub_job_facility_reductions). A group-specific rig
        whose coverage is unknown contributes 0, so the result is then an
        upper bound, never an under-buy. Implant material bonuses are not
        applied.
        None (with a WARNING) when the blueprint cannot say how to build the
        component.
        """
        from eve_online_industry_tracker.infrastructure.sde.blueprints import (
            reduced_batch_quantity,
        )

        material_reduction = facility_bonus.combine_reductions(
            [float(me) / 100.0, *facility_reductions]
        )

        bp_data = (phase1_data.get("blueprint_data") or {}).get(blueprint_type_id)
        manufacturing = bp_data.get("manufacturing") if isinstance(bp_data, dict) else None
        if not isinstance(manufacturing, dict):
            logger.warning(
                "ChainPlanner: no SDE manufacturing data for blueprint %s; buying type_id=%s",
                blueprint_type_id, product_type_id,
            )
            return None

        output_per_run = 0
        for product in manufacturing.get("products") or []:
            if isinstance(product, dict) and int(product.get("type_id") or 0) == product_type_id:
                output_per_run = int(product.get("quantity") or 0)
                break
        if output_per_run <= 0:
            logger.warning(
                "ChainPlanner: blueprint %s has no per-run output of type_id=%s; buying it",
                blueprint_type_id, product_type_id,
            )
            return None

        base_materials: dict[int, int] = {}
        for mat in manufacturing.get("materials") or []:
            if not isinstance(mat, dict):
                continue
            mat_type_id = int(mat.get("type_id") or 0)
            base_qty = int(mat.get("quantity") or 0)
            if mat_type_id <= 0 or base_qty <= 0:
                continue
            base_materials[mat_type_id] = base_materials.get(mat_type_id, 0) + base_qty
        if not base_materials:
            # A job with no inputs would price at 0 ISK and always beat the
            # market: that is missing SDE data, not a free build.
            logger.warning(
                "ChainPlanner: blueprint %s has no SDE materials; not sub-manufacturing type_id=%s",
                blueprint_type_id, product_type_id,
            )
            return None

        max_runs = _max_runs_per_job(bp_data)
        if max_runs is None:
            logger.warning(
                "ChainPlanner: blueprint %s has no usable max runs per job "
                "(max_production_limit=%r), so the job split and its rounding are "
                "unknown; buying type_id=%s",
                blueprint_type_id, bp_data.get("max_production_limit"), product_type_id,
            )
            return None

        runs = math.ceil(qty_to_build / output_per_run)
        full_jobs, remainder = divmod(runs, max_runs)
        job_runs = [max_runs] * full_jobs + ([remainder] if remainder else [])
        jobs: list[dict[str, Any]] = []
        units_left = qty_to_build
        for runs_in_job in job_runs:
            output = runs_in_job * output_per_run
            quantity = min(output, units_left)
            units_left -= quantity
            jobs.append({
                "runs": runs_in_job,
                "output": output,
                "quantity": quantity,
                "materials": {
                    mat_type_id: reduced_batch_quantity(base_qty, runs_in_job, material_reduction)
                    for mat_type_id, base_qty in base_materials.items()
                },
            })
        batch = {
            mat_type_id: sum(job["materials"][mat_type_id] for job in jobs)
            for mat_type_id in base_materials
        }
        return runs, batch, material_reduction, jobs

    def _price_materials(
        self, materials: dict[int, int], market_depth_cache: dict[int, Any]
    ) -> float | None:
        """Market cost of a material list, or None when any line has no price."""
        total = 0.0
        for mat_type_id, quantity in materials.items():
            cost = self._estimate_market_cost(mat_type_id, quantity, market_depth_cache)
            if cost is None:
                logger.info(
                    "ChainPlanner: no market price for sub-job input type_id=%s; "
                    "cannot compare build vs buy", mat_type_id,
                )
                return None
            total += cost
        return total

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
        # 0 or negative is no price, never a free material.
        price = next((float(p) for p in (vwap, spot) if p is not None and float(p) > 0), None)
        if price is None:
            return None
        return price * quantity

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
            # 0 or negative is no price (as service._has_depth_price), never a free BPO.
            bpo_market_price = float(spot) if spot is not None and float(spot) > 0 else None
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
        me_target is the optimal ME at the job's run count. A batch of several
        jobs (manufacturing_job.job_segments: one per owned BPC, then one per
        invented BPC) saves only on the runs a researched BPO would build: the
        owned-BPC jobs stay at their ME, and each run of consecutive other jobs
        becomes ONE job on the BPO. Returns
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
        runs, runs_reason = _batch_runs(job)
        if runs is None:
            return None, runs_reason

        # Groups of [(runs, current ME), ...]: each group is the jobs one
        # researched-BPO job would replace. The owned BPC's runs stay at its
        # ME whatever BPO is bought, so they are in no group.
        groups: list[list[tuple[int, int]]] = [[(runs, me_current)]]
        segments = job.get("job_segments")
        if isinstance(segments, list) and len(segments) > 1:
            groups = []
            try:
                previous_owned = True
                for segment in segments:
                    owned = segment.get("source") == "owned_blueprint_copy"
                    if not owned:
                        if previous_owned:
                            groups.append([])
                        groups[-1].append((int(segment["runs"]), int(segment["blueprint_material_efficiency"])))
                    previous_owned = owned
            except (AttributeError, KeyError, TypeError, ValueError):
                return None, "malformed job_segments on a multi-job batch"

        priced = _material_unit_prices(job)
        saving = 0.0
        for group in groups:
            group_runs = sum(job_runs for job_runs, _me in group)
            me_target = optimal_me_for_quantities((q for _, q in base), runs=group_runs)
            for mat_type_id, base_qty in base:
                unit_price = priced.get(mat_type_id)
                if unit_price is None:
                    return None, f"no unit price for material type_id={mat_type_id}"
                saved_units = (
                    sum(me_adjusted_batch_quantity(base_qty, job_runs, job_me) for job_runs, job_me in group)
                    - me_adjusted_batch_quantity(base_qty, group_runs, me_target)
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


class _NoMetaGroups:
    def meta_group_id(self, type_id: int) -> None:
        return None


def _sizing_bonus_wording(structure_reduction: float, rig_reduction: float) -> str:
    """Which facility bonuses a planner-sized batch got, for its note: only
    the ones applied (> 0) are named."""
    structure, rig = structure_reduction > 0.0, rig_reduction > 0.0
    if structure and rig:
        return "with the row's structure and rig bonuses"
    if structure:
        return "with the row's structure bonus (no rig bonus)"
    if rig:
        return "with the row's rig bonus (no structure bonus)"
    return "with no structure or rig bonus"


def _variant_profit(variant: dict[str, Any], market_depth: Any | None) -> tuple[float | None, str | None]:
    """The variant's batch profit, scored exactly like the kept row's
    (ProfitabilityScorer's absolute profit), or (None, why)."""
    from eve_online_industry_tracker.application.daily_planner.input_row import (
        PlannerInputError,
        PlannerInputRow,
    )
    from eve_online_industry_tracker.application.daily_planner.profitability_scorer import _compute_absolute_profit

    try:
        input_row = PlannerInputRow.from_overview(variant, meta_groups=_NoMetaGroups())
    except PlannerInputError as exc:
        return None, f"the owned-BPC variant row is malformed ({exc})"
    profit, reason = _compute_absolute_profit(input_row, market_depth)
    return (None, reason) if reason is not None else (profit, None)


def _batch_runs(manufacturing_job: dict[str, Any]) -> tuple[int | None, str | None]:
    """The batch run count from manufacturing_job.runs, with no fallback to quantity.

    Returns (runs, None), or (None, reason) when runs is missing, not an
    integer, or not positive (see input_row: quantity is not a run count).
    """
    try:
        runs = int(manufacturing_job["runs"])
    except (KeyError, TypeError, ValueError):
        return None, "unknown batch run count (manufacturing_job.runs)"
    if runs <= 0:
        return None, f"non-positive batch run count ({runs})"
    return runs, None


def _max_runs_per_job(bp_data: dict[str, Any]) -> int | None:
    """The blueprint's max runs per job (SDE maxProductionLimit), or None
    when it is missing or not a positive integer."""
    limit = bp_data.get("max_production_limit")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        return None
    return limit


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


def _lowest_known_me(bpo_assets: list[Any]) -> int | None:
    """The worst-case ME across the corp's BPOs of one blueprint, or None.

    The pilot may run the job from any of them, so the job is sized for the
    worst one. An unknown level could be anything down to ME0:
    - lowest known ME is 0: 0, the exact worst case (ME cannot go lower);
    - any level unknown and the lowest known above 0: None (buy);
    - otherwise the lowest known ME; None when none is known.
    """
    levels = [_blueprint_efficiency(a, "material") for a in bpo_assets]
    known = [me for me in levels if me is not None]
    if not known:
        return None
    lowest = min(known)
    if lowest == 0:
        return 0
    if len(known) < len(levels):
        return None
    return lowest


def _non_negative(value: Any) -> float | None:
    """A finite fee >= 0, else None (unknown, never 0)."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


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


def _str_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None
