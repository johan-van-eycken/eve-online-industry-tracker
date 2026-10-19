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
from eve_online_industry_tracker.application.daily_planner.blueprint_ledger import BlueprintLedger
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


#: The pipeline stage of an item this plan does not build: a blueprint it
#: needs is in an active job, or it has no blueprint source to build from.
NOT_BUILT_STAGE = "blocked"


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


def _blueprint_runs(asset: Any) -> int:
    """A BPC asset's runs (ORM row or dict); 0 when absent."""
    value = asset.get("blueprint_runs") if isinstance(asset, dict) else getattr(asset, "blueprint_runs", 0)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


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
        # A job's NULL copy flag is resolved through the assets (minor 3).
        is_copy_by_item_id: dict[int, bool] = {
            **{int(k): bool(v) for k, v in (phase1_data.get("blueprint_copy_flags") or {}).items()},
            **{research_first.item_id(a): False for assets in bpo_assets_by_type_id.values() for a in assets
               if research_first.item_id(a) is not None},
            **{research_first.item_id(a): True for assets in bpc_assets_by_type_id.values() for a in assets
               if research_first.item_id(a) is not None},
        }
        busy_bpo_type_ids = research_first.busy_blueprint_type_ids(
            industry_jobs, copies=False, is_copy_by_item_id=is_copy_by_item_id,
        ) | {
            t for t, assets in bpo_assets_by_type_id.items()
            if any(research_first.item_id(a) in busy_item_ids for a in assets)
        }
        busy_bpc_type_ids = research_first.busy_blueprint_type_ids(
            industry_jobs, copies=True, is_copy_by_item_id=is_copy_by_item_id,
        ) | {
            t for t, assets in bpc_assets_by_type_id.items()
            if any(research_first.item_id(a) in busy_item_ids for a in assets)
        }

        def _on_busy_bpc(candidate: dict[str, Any]) -> bool:
            # A research-first variant is used whole: any busy BPC rules it out.
            return self._busy_bpc_plan(candidate, busy_item_ids, busy_bpc_type_ids) is not None

        # The plan-wide blueprint reservation (Task 14): every BPO and BPC
        # the plan uses is booked once, in plan order (see blueprint_ledger).
        ledger = BlueprintLedger(free_bpo_assets_by_type_id, free_bpc_assets_by_type_id)
        build_decisions = [d for d in top_level_decisions if d.decision == "build"]
        # Plan order is score order (CharacterAssigner's), ties as given.
        ordered = sorted(build_decisions, key=lambda d: -float(d.adjusted_score or 0.0))
        top_level_build_types = {int(d.type_id) for d in build_decisions}
        state: dict[int, dict[str, Any]] = {}

        def _isolated(decision: ItemDecision, step: Any) -> None:
            # Per-item isolation for malformed SDE / market values (int() and
            # float() conversions); anything else is a bug and aborts. A row
            # that fails drops out of the later phases.
            if id(decision) not in state and step is not _phase_a:
                return
            try:
                step(decision)
            except (TypeError, ValueError):
                logger.exception("ChainPlanner: error planning chain for type_id=%s", decision.type_id)
                state.pop(id(decision), None)

        def _phase_a(decision: ItemDecision) -> None:
            """The chains, and research claims its BPO first (research first)."""
            row = decision.overview_row
            # Nested under manufacturing_job.blueprint_sde on a real row; there
            # is no top-level blueprint_type_id key.
            bp_type_id = orow.get_blueprint_type_id(row) or 0
            ctx: dict[str, Any] = {
                "bp_type_id": bp_type_id,
                "bp_data": blueprint_data.get(bp_type_id, {}),
                # Research first never builds from a variant on a busy BPC.
                "variants": [
                    v for v in variant_rows_by_type_id.get(int(decision.type_id)) or []
                    if not _on_busy_bpc(v)
                ],
            }
            state[id(decision)] = ctx
            busy_bpc = self._busy_bpc_plan(row, busy_item_ids, busy_bpc_type_ids)
            ctx["busy_bpc_reason"] = busy_bpc["reason"] if busy_bpc is not None else None
            if busy_bpc is not None:
                self._build_free_bpc_jobs_only(decision, busy_bpc)
            if decision.meta_group_id != META_GROUP_T1:
                return
            bpo_plan = self._plan_t1_chain(
                decision=decision,
                bp_type_id=bp_type_id,
                bp_data=ctx["bp_data"],
                bpo_assets_by_type_id=bpo_assets_by_type_id,
                bpc_assets_by_type_id=free_bpc_assets_by_type_id,
                market_depth_cache=market_depth_cache,
                bpo_opportunities=bpo_opportunities,
                te_research_target=te_research_target,
                busy_item_ids=busy_item_ids,
                bpo_strong_buy_days=bpo_strong_buy_days,
                bpo_consider_days=bpo_consider_days,
                busy_blueprint_type_ids=busy_bpo_type_ids | busy_bpc_type_ids,
            )
            ctx["bpo_plan"] = bpo_plan
            if bpo_plan is None:
                return
            ctx["bpo_keys"] = ledger.bpo_keys(bp_type_id, bpo_plan["assets"])
            label = _research_label(decision.overview_row)
            if bpo_plan["research"] is not None and label is not None:
                claim_label = f"{label} for {decision.type_name} ({decision.type_id})"
                ledger.claim(ctx["bpo_keys"][bpo_plan["research"]], decision.type_id, claim_label)
                # CharacterAssigner rewords it when the research gets no slot (M10).
                decision.overview_row["research_claim_label"] = claim_label

        def _phase_b(decision: ItemDecision) -> None:
            """Top-level manufacture claims its BPO / BPCs, a T2 row one T1 BPO
            to copy, in score order."""
            ctx = state[id(decision)]
            bp_type_id = ctx["bp_type_id"]
            label = f"manufacture of {decision.type_name} ({decision.type_id})"
            if decision.meta_group_id == META_GROUP_T1:
                bpo_plan = ctx.get("bpo_plan")
                if bpo_plan is None:
                    if decision.pipeline_stage != NOT_BUILT_STAGE:
                        self._claim_row_bpcs(ledger, decision.overview_row, decision, label)
                    return
                keys = ctx["bpo_keys"]
                taken = {
                    index: claim[1] for index, claim in ((i, ledger.claim_of(k)) for i, k in enumerate(keys))
                    if claim is not None and claim[0] != int(decision.type_id)
                }
                self._apply_research_first(
                    decision, bp_type_id, bpo_plan, ctx["variants"],
                    raw_market_depth.get(int(decision.type_id)),
                    bp_data=ctx["bp_data"], market_depth_cache=market_depth_cache, taken=taken,
                )
                self._claim_t1_build(ledger, decision, bpo_plan, keys, label)
                return
            if ctx["busy_bpc_reason"] is not None:
                # Priced on the busy BPC's runs: inventing instead would buy
                # no datacores for them (an under-buy). Only the free BPCs'
                # jobs build (or nothing: stage set in phase A).
                if decision.pipeline_stage != NOT_BUILT_STAGE:
                    decision.pipeline_stage = "manufacturing"
                    self._claim_row_bpcs(ledger, decision.overview_row, decision, label)
                return
            if free_bpc_assets_by_type_id.get(bp_type_id) or free_bpo_assets_by_type_id.get(bp_type_id):
                return  # builds from its own T2 blueprint (phase C)
            t1_bp_type_id = invention_source.get(bp_type_id)
            if t1_bp_type_id is None or bp_type_id in busy_bpo_type_ids:
                return
            # Fix rounds 1-2, I2: copy only when the BPCs this row will get in
            # phase C do not cover its attempts. Phase C claims WHOLE BPCs
            # (best fit, _pick_bpcs), so the same picks are simulated here
            # over the free BPCs no T1 row builds on and no higher row picked.
            need = _whole_attempts(decision.overview_row)
            picks = _pick_bpcs([
                e for e in ledger.free_bpcs(t1_bp_type_id)
                if not _bpc_used_by_t1_rows(t1_bpc_use, t1_bp_type_id, e[1]) and e[0] not in promised_bpcs
            ], need)
            promised_bpcs.update(e[0] for e in picks)
            if need is not None and sum(e[2] for e in picks) >= need:
                return
            free = ledger.free_bpos(t1_bp_type_id)
            if free:
                key, asset = free[0]
                ledger.claim(key, decision.type_id, f"copy for {decision.type_name} ({decision.type_id})")
                ctx["copy"] = [(key, asset)]

        def _phase_b2(decision: ItemDecision) -> None:
            """The T2 rows that copy take the T1 BPOs left, for parallel copies:
            up to the producer's invention lanes (one copy job per lane at
            most), none extra when the lane count is unknown."""
            ctx = state[id(decision)]
            if not ctx.get("copy"):
                return
            t1_bp_type_id = invention_source.get(ctx["bp_type_id"])
            for key, asset in ledger.free_bpos(t1_bp_type_id)[:max(0, _invention_lanes(decision.overview_row) - 1)]:
                ledger.claim(key, decision.type_id, f"copy for {decision.type_name} ({decision.type_id})")
                ctx["copy"].append((key, asset))

        def _phase_c(decision: ItemDecision) -> None:
            """T2 rows: own T2 blueprint, else invention on the free BPCs."""
            ctx = state[id(decision)]
            if decision.meta_group_id != META_GROUP_T1 and ctx["busy_bpc_reason"] is None:
                _t2_chain(decision, ctx.get("copy") or [])
            # The chains set the stage from the blueprints owned; a row
            # this plan does not build at all (unconditionally) is
            # "blocked" whatever they set (fix round 2, item 2).
            first = decision.overview_row.get("research_first")
            if isinstance(first, dict) and first.get("unconditional") and first.get("mode") == "none":
                decision.pipeline_stage = NOT_BUILT_STAGE

        def _t2_chain(decision: ItemDecision, copy_claims: list[tuple[Any, Any]]) -> None:
            ctx = state[id(decision)]
            self._plan_t2_chain(
                decision=decision,
                bp_type_id=ctx["bp_type_id"],
                bp_data=ctx["bp_data"],
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
                ledger=ledger,
                copy_claims=copy_claims,
            )

        # The T1 BPCs the T1 rows' own batches (and research-first variants)
        # build on: not counted as free for a T2 row's copy decision.
        t1_bpc_use: dict[int, set[int] | None] = {}
        promised_bpcs: set[Any] = set()
        for step in (_phase_a, _phase_b, _phase_b2, _phase_c):
            if step is _phase_b:
                for decision in ordered:
                    ctx = state.get(id(decision))
                    if ctx is not None and decision.meta_group_id == META_GROUP_T1:
                        for candidate in (decision.overview_row, *ctx["variants"]):
                            _note_bpc_use(t1_bpc_use, candidate)
            for decision in ordered:
                _isolated(decision, step)

        def _annotate(decision: ItemDecision) -> None:
            # The blueprints this row holds, by job kind (named items only):
            # CharacterAssigner releases the ones no action used (Task A).
            claims: dict[str, list[int]] = {"manufacture": [], "copy": [], "invention": [], "research": []}
            for key, label in ledger.claims_of(decision.type_id):
                if isinstance(key, int) and not isinstance(key, bool):
                    claims[_claim_category(label)].append(key)
            decision.overview_row["ledger_claims"] = claims

        for decision in ordered:
            if id(decision) in state:
                _annotate(decision)

        names = {int(d.type_id): f"{d.type_name} ({d.type_id})" for d in build_decisions}

        def _replan(
            release: dict[int, list[int]], used_by: dict[int, int], eligible: set[int],
        ) -> list[ItemDecision]:
            """Task A items 2, 3 and 6: CharacterAssigner's report after a
            pass. `release`: {row type_id: blueprint item_ids it claimed but
            no action of it used}; `used_by`: {item_id: row type_id} of the
            blueprints its actions use (a released BPC another row invents
            from); `eligible`: the rows with no action yet. The claims are
            released, the uses booked, and the eligible rows not built for a
            blueprint now free are planned again, in score order. Returns
            those rows (the same objects, re-planned)."""
            released: dict[Any, str] = {}
            for owner, items in release.items():
                for item in items:
                    held = ledger.claim_of(item)
                    if held is not None and held[0] == int(owner):
                        released[item] = held[1]
                        ledger.release(item, owner)
            for item, user in used_by.items():
                held = ledger.claim_of(item)
                if held is None or held[0] != int(user):
                    kind = (ledger.type_of(item) or ("bpc", 0))[0]
                    job = "invention for" if kind == "bpc" else "copy for"
                    ledger.transfer(item, user, f"{job} {names.get(int(user), f'type {user}')}")
            revived: list[ItemDecision] = []
            for decision in ordered:
                row = decision.overview_row
                replan_on = row.get("replan_on")
                ctx = state.get(id(decision))
                if not isinstance(replan_on, dict) or ctx is None or int(decision.type_id) not in eligible:
                    continue
                before = {k for k, _l in ledger.claims_of(decision.type_id)}
                snapshot = (
                    decision.decision_reason, decision.pipeline_stage, decision.bpo_analysis_skip_reason, dict(row),
                )
                if replan_on.get("kind") == "t2":
                    done = self._replan_t2(decision, ledger, replan_on, lambda d: _t2_chain(d, []))
                else:
                    done = self._replan_t1(decision, ledger, ctx)
                if done:
                    gained = [k for k, _l in ledger.claims_of(decision.type_id) if k not in before]
                    _append_reason(decision, _released_wording(decision, ledger, gained, released))
                    undo_state[id(decision)] = (*snapshot, gained)
                    revived.append(decision)
                    _annotate(decision)
                elif replan_on.get("kind") == "t2":
                    self._reword_t2_blocked(decision, ledger, replan_on)
            return revived

        undo_state: dict[int, tuple[Any, ...]] = {}

        def _undo_replan(decision: ItemDecision) -> None:
            """A re-planned row that still got no action: back to its
            not-built plan and reason, and the blueprints it gained are free."""
            saved = undo_state.pop(id(decision), None)
            if saved is None:
                return
            reason, stage, skip_reason, row, gained = saved
            decision.decision_reason, decision.pipeline_stage, decision.bpo_analysis_skip_reason = reason, stage, skip_reason
            decision.overview_row.clear()
            decision.overview_row.update(row)
            for key in gained:
                ledger.release(key, decision.type_id)

        # Each parent's sub-manufacture requests, in order, and (research
        # first) the ones its kept BPO row would make instead. Sub-builds
        # use only the component BPOs no job above claimed.
        requests_by_parent: list[tuple[int, list[dict[str, Any]]]] = []
        fallback_requests: dict[int, list[dict[str, Any]]] = {}

        def _phase_d(decision: ItemDecision) -> None:
            # Which of this batch's materials the corp could build itself.
            # Collected across every parent first, so one material needed
            # by several builds becomes ONE sub-manufacture decision.
            # M8: the bought need per component, the larger of the batch's
            # and its research-first fallback's (fix round 3: either may run).
            primary_need: dict[int, int] = {}
            fallback_need: dict[int, int] = {}
            requests = self._collect_sub_manufacture_requests(
                decision=decision,
                bpo_assets_by_type_id=free_bpo_assets_by_type_id,
                product_to_blueprint=product_to_blueprint,
                ledger=ledger, top_level_build_types=top_level_build_types,
                stock=phase1_data.get("corp_material_stock") or {},
                need_sink=primary_need,
            )
            requests_by_parent.append((int(decision.type_id), requests))
            kept_row = self._research_first_fallback_row(decision.overview_row)
            if kept_row is not None:
                fallback_requests[int(decision.type_id)] = self._collect_sub_manufacture_requests(
                    decision=decision, row=kept_row,
                    bpo_assets_by_type_id=free_bpo_assets_by_type_id,
                    product_to_blueprint=product_to_blueprint,
                    ledger=ledger, top_level_build_types=top_level_build_types,
                    stock=phase1_data.get("corp_material_stock") or {},
                    need_sink=fallback_need,
                )
            for type_id in {*primary_need, *fallback_need}:
                bought_need[type_id] = bought_need.get(type_id, 0) + max(
                    primary_need.get(type_id, 0), fallback_need.get(type_id, 0),
                )

        bought_need: dict[int, int] = {}
        for decision in build_decisions:
            _isolated(decision, _phase_d)
        stock = phase1_data.get("corp_material_stock") or {}
        for decision in build_decisions:
            reasons = decision.overview_row.get("component_buy_reasons")
            if isinstance(reasons, dict):
                for type_id in [t for t in reasons if bought_need.get(t, 0) <= max(0, int(stock.get(t, 0)))]:
                    del reasons[type_id]

        def _sub_decisions(fallen: set[int]) -> list[ItemDecision]:
            per_parent = [
                (parent, fallback_requests[parent] if parent in fallen and parent in fallback_requests else requests)
                for parent, requests in requests_by_parent
            ]
            buy_reasons: dict[int, dict[int, str]] = {}
            subs = self._resolve_sub_requests(
                self._merge_sub_requests(per_parent), market_depth_cache, phase1_data,
                buy_reasons=buy_reasons,
            )
            # Fix rounds 3-4 (Task 15): a component bought for an unknown fee
            # says why on every parent that requested it, each in its own words.
            parents = {int(d.type_id): d for d in build_decisions}
            for mat_type_id, by_parent in buy_reasons.items():
                for parent_type_id, reason in by_parent.items():
                    parent = parents.get(int(parent_type_id))
                    if parent is not None:
                        parent.overview_row.setdefault("component_buy_reasons", {}).setdefault(
                            int(mat_type_id), reason,
                        )
            return subs

        def _rebuild(subs: list[ItemDecision]) -> list[ItemDecision]:
            return _one_decision_per_type(top_level_decisions, subs)

        all_decisions = _rebuild(_sub_decisions(set()))

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
            rebuild_decisions=_rebuild,
            replan_released=_replan,
            undo_replan=_undo_replan,
        )

    @staticmethod
    def _unmark_not_built(decision: ItemDecision) -> None:
        """Undo _mark_not_built for a row a re-plan builds after all."""
        row = decision.overview_row
        row.pop("research_first", None)
        row.pop("replan_on", None)
        suffix = row.pop("not_built_suffix", None)
        if suffix:
            decision.decision_reason = (decision.decision_reason or "").replace(suffix, "", 1)

    def _replan_t2(
        self, decision: ItemDecision, ledger: BlueprintLedger, replan_on: dict[str, Any], plan: Any,
    ) -> bool:
        """A T2 row not built for its T1 BPO / BPCs (held by other jobs):
        planned again when one of that T1 source is free now."""
        t1 = int(replan_on["t1"])
        if not ledger.free_bpcs(t1) and not ledger.free_bpos(t1):
            return False
        self._unmark_not_built(decision)
        plan(decision)
        return decision.pipeline_stage != NOT_BUILT_STAGE

    def _replan_t1(self, decision: ItemDecision, ledger: BlueprintLedger, ctx: dict[str, Any]) -> bool:
        """A T1 row not built because another row's job held the BPO it is
        priced from: built from it when that BPO is free now, and every
        owned BPC its batch names is free too. Its components were not
        sub-built, so its manufacture buys them."""
        bpo_plan = ctx.get("bpo_plan")
        keys = ctx.get("bpo_keys")
        if not bpo_plan or not keys:
            return False
        priced = bpo_plan["priced"]
        if priced in set(bpo_plan.get("busy") or ()) or ledger.claim_of(keys[priced]) is not None:
            return False
        row = decision.overview_row
        if research_first.builds_on_owned_bpcs(row):
            named = [
                research_first.segment_bpc_item_id(seg) for seg in research_first.job_segments(row) or []
                if str(seg.get("source")) == research_first.OWNED_BPC_SOURCE
            ]
            bp_type_id = orow.get_blueprint_type_id(row) or 0
            if not named or any(i is None for i in named):
                return False  # unnamed BPCs: cannot show they are free
            for item in named:
                key = ledger.bpc_key(bp_type_id, int(item))
                if key is None or ledger.claim_of(key) is not None:
                    return False
        self._unmark_not_built(decision)
        decision.pipeline_stage = "manufacturing"
        self._claim_t1_build(
            ledger, decision, bpo_plan, keys, f"manufacture of {decision.type_name} ({decision.type_id})",
        )
        _append_reason(decision, "its components are bought: they were not sub-built in this plan")
        return True

    @staticmethod
    def _reword_t2_blocked(decision: ItemDecision, ledger: BlueprintLedger, replan_on: dict[str, Any]) -> None:
        """A T2 row still not built after a re-plan names who holds its T1
        blueprints now (a released one may have gone to another row)."""
        t1 = int(replan_on["t1"])
        labels = ledger.bpo_claim_labels(t1) if replan_on.get("blocked") == "bpo" else ledger.bpc_claim_labels(t1)
        if not labels:
            return
        row = decision.overview_row
        old = row.get("not_built_suffix")
        new = f"; not built: {_t2_blocked_reason(str(replan_on.get('blocked')), t1, _holders(labels))}"
        if old and old != new and old in (decision.decision_reason or ""):
            decision.decision_reason = decision.decision_reason.replace(old, new, 1)
            row["not_built_suffix"] = new
            first = row.get("research_first")
            if isinstance(first, dict):
                first["note"] = f"{new[len('; not built: '):]}: not built from it in this plan"

    @staticmethod
    def _merge_sub_requests(
        per_parent: list[tuple[int, list[dict[str, Any]]]],
    ) -> dict[int, dict[str, Any]]:
        """{material type_id: one merged request}, in first-seen order.

        One sub job serves every requester, so the parents' published fee
        per run (the fallback when the component has no row of its own) is
        merged independently of their order (Task 15 fix round 4): unknown
        when ANY requester's is unknown (the job's cost is then a lower
        bound, so the component is bought), else the highest, so no parent's
        share is under-priced. `parent_fee_unknown_by_parent` names the
        parents whose fee is unknown: {parent type_id: (name, reason)}."""
        sub_requests: dict[int, dict[str, Any]] = {}
        for parent_type_id, requests in per_parent:
            for request in requests:
                merged = sub_requests.setdefault(request["type_id"], {
                    **request, "quantity": 0, "requested_by_type_ids": [],
                    "parent_fee_per_run": None, "parent_fee_unknown_reason": None,
                    "parent_fee_unknown_by_parent": {}, "_fees_seen": [],
                })
                merged["quantity"] += request["quantity"]
                merged["requested_by_type_ids"].append(parent_type_id)
                fee = request.get("parent_fee_per_run")
                merged["_fees_seen"].append(fee)
                if fee is None:
                    merged["parent_fee_unknown_by_parent"][int(parent_type_id)] = (
                        str(request.get("parent_type_name") or f"type_{parent_type_id}"),
                        _str_or_none(request.get("parent_fee_unknown_reason")),
                    )
                # One sub job serves every requester. If their structures
                # differ, take the smallest facility bonus so no parent's
                # share is under-bought.
                if facility_bonus.facility_reduction(request) < facility_bonus.facility_reduction(merged):
                    merged.update({k: request[k] for k in facility_bonus.FACILITY_KEYS})
        for merged in sub_requests.values():
            fees = merged.pop("_fees_seen")
            unknown = merged["parent_fee_unknown_by_parent"]
            merged["parent_fee_per_run"] = None if unknown else max(fees)
            # The first unknown parent's reason, for the job and the log.
            merged["parent_fee_unknown_reason"] = next(
                (reason for _name, reason in unknown.values() if reason is not None), None,
            )
        return sub_requests

    def _resolve_sub_requests(
        self,
        sub_requests: dict[int, dict[str, Any]],
        market_depth_cache: dict[int, Any],
        phase1_data: dict[str, Any],
        *,
        buy_reasons: dict[int, dict[int, str]] | None = None,
    ) -> list[ItemDecision]:
        """`buy_reasons`: filled with {type_id: {parent type_id: reason}} for
        each component _resolve_sub_manufacture buys for a stated reason."""
        decisions: list[ItemDecision] = []
        for request in sub_requests.values():
            try:
                sub_decision = self._resolve_sub_manufacture(
                    request=request,
                    market_depth_cache=market_depth_cache,
                    phase1_data=phase1_data,
                    buy_reasons=buy_reasons,
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
    def _busy_bpc_plan(
        row: dict[str, Any], busy_item_ids: set[int], busy_bpc_type_ids: set[int],
    ) -> dict[str, Any] | None:
        """{reason, part} when a BPC the batch builds on is in an active job,
        else None. `part` is the batch's jobs on the free BPCs
        (research_first.owned_bpc_part), None when none is left.

        The producer counts the runs of every owned BPC in the asset snapshot
        (it adds BPCs locked in active jobs back in), so one variant can hold
        a busy BPC. Each owned-BPC job names its BPC (blueprint_item_id), so
        only the jobs on a busy BPC are left out. An older row without those
        ids falls back to the variant's BPC list (row_bpc_item_ids), and a
        row that names no BPC while one of its blueprint is in a job cannot
        be shown to be free: neither builds."""
        if not research_first.builds_on_owned_bpcs(row):
            return None
        bpc_segments = [
            s for s in research_first.job_segments(row) or []
            if str(s.get("source")) == research_first.OWNED_BPC_SOURCE
        ]
        segment_ids = [research_first.segment_bpc_item_id(s) for s in bpc_segments]
        if bpc_segments and all(i is not None for i in segment_ids):
            busy = sorted({i for i in segment_ids if i in busy_item_ids})
            if not busy:
                return None
            return {
                "reason": f"BPC in an active industry job (item {', '.join(map(str, busy))})",
                "part": research_first.owned_bpc_part(row, exclude_item_ids=busy_item_ids),
            }
        item_ids = research_first.row_bpc_item_ids(row)
        if item_ids:
            busy = sorted(item_ids & busy_item_ids)
            if not busy:
                return None
            return {"reason": f"BPC in an active industry job (item {', '.join(map(str, busy))})", "part": None}
        if (orow.get_blueprint_type_id(row) or 0) in busy_bpc_type_ids:
            return {"reason": "BPC in an active industry job (the row does not name its BPCs)", "part": None}
        return None

    def _build_free_bpc_jobs_only(self, decision: ItemDecision, busy_bpc: dict[str, Any]) -> None:
        """Research first's "part" plan over the free BPCs' jobs, unconditional
        like a busy BPO: the manufacture action and the sub-builds cover those
        runs only. Not built when no free job is left."""
        part = busy_bpc["part"]
        if part is None:
            self._mark_not_built(decision, busy_bpc["reason"])
            return
        total_runs = orow.get_manufacturing_job(decision.overview_row).get("runs")
        note = (f"{busy_bpc['reason']}: built from the free owned BPC runs only "
                f"({part['runs']} of {total_runs} runs)")
        decision.overview_row["research_first"] = {
            "mode": "part", **part, "note": note,
            "cost_unknown_reason": research_first.PART_COST_UNKNOWN_REASON,
            "sized_note": f"its components were sized for {part['runs']} of {total_runs} runs",
            "unconditional": True,
        }
        logger.info("ChainPlanner: type_id=%s: %s", decision.type_id, note)

    @staticmethod
    def _claim_row_bpcs(ledger: BlueprintLedger, row: dict[str, Any], decision: ItemDecision, label: str) -> None:
        """Book the owned BPCs the row's batch builds on: the ones its job
        segments name, else the producer's BPC list, else (unnamed) every
        free BPC of the blueprint."""
        if not research_first.builds_on_owned_bpcs(row):
            return
        bp_type_id = orow.get_blueprint_type_id(row) or 0
        named = [
            research_first.segment_bpc_item_id(s) for s in research_first.job_segments(row) or []
            if str(s.get("source")) == research_first.OWNED_BPC_SOURCE
        ]
        if named and all(i is not None for i in named):
            item_ids: set[int] | None = {int(i) for i in named}
        else:
            item_ids = research_first.row_bpc_item_ids(row) or None
        ledger.claim_bpcs_of_row(bp_type_id, item_ids, decision.type_id, label)

    def _claim_t1_build(
        self, ledger: BlueprintLedger, decision: ItemDecision, bpo_plan: dict[str, Any],
        keys: list[Any], label: str,
    ) -> None:
        """Book what a T1 row actually builds from after research first:
        the BPO it builds on (the priced one, or research first's
        build_index) and its owned BPCs; a part only its BPCs; none nothing."""
        row = decision.overview_row
        first = row.get("research_first") if isinstance(row.get("research_first"), dict) else None
        mode = first.get("mode") if first is not None else None
        bp_type_id = orow.get_blueprint_type_id(row) or 0
        if mode == "none":
            pass
        elif mode == "part":
            item_ids = first.get("item_ids")
            ledger.claim_bpcs_of_row(
                bp_type_id, set(item_ids) if item_ids is not None else None, decision.type_id, label,
            )
        else:
            index = first.get("build_index") if first is not None else None
            if index is None and mode is None and research_first.batch_uses_bpo(row):
                index = bpo_plan["priced"]
            if index is not None and index not in set(bpo_plan.get("busy") or ()):
                ledger.claim(keys[index], decision.type_id, label)
            self._claim_row_bpcs(ledger, row, decision, label)
        # Fix round 1, I1: when its research gets no slot the row builds from
        # its kept row instead (CharacterAssigner), so that row's BPO and
        # BPCs are booked too: the union, so nothing can double-book them.
        kept = self._research_first_fallback_row(row)
        if kept is not None:
            if research_first.batch_uses_bpo(kept) and bpo_plan["priced"] not in set(bpo_plan.get("busy") or ()):
                ledger.claim(keys[bpo_plan["priced"]], decision.type_id, label)
            self._claim_row_bpcs(ledger, kept, decision, label)

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
        # Not "manufacturing": nothing is built from this row (minor 5).
        decision.pipeline_stage = NOT_BUILT_STAGE
        # Persisted and shown in the Build Plan's Reason column (Task 12 fix
        # round 4): the row keeps decision="build", so say why it is not.
        decision.decision_reason = f"{decision.decision_reason}; not built: {reason}"
        # Task A item 2: what a re-plan strips when the row is built after all.
        decision.overview_row["not_built_suffix"] = f"; not built: {reason}"
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
        busy_blueprint_type_ids: set[int] | frozenset[int] = frozenset(),
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
            self._not_built_without_a_free_blueprint(
                decision, bp_type_id, bp_data, market_depth_cache, bpo_opportunities,
                bpo_strong_buy_days, bpo_consider_days, busy_blueprint_type_ids,
            )
        return None

    def _not_built_without_a_free_blueprint(
        self, decision: ItemDecision, bp_type_id: int, bp_data: dict[str, Any],
        market_depth_cache: dict[int, Any], bpo_opportunities: list[dict[str, Any]],
        bpo_strong_buy_days: float, bpo_consider_days: float, busy_blueprint_type_ids: Any,
    ) -> None:
        """A T1 row with no free owned BPO or BPC in the corp hangar (I3, fix
        round 1). Nothing it could build from is free, so it is not built
        (no manufacture, nothing bought, no fee). Why, in this order:

        - already not built (a busy BPC, research first "none"): kept as is;
        - a blueprint of its type in an active industry job (the asset
          snapshot dropped it): owned, only busy, so not a purchase;
        - the producer priced it from an owned blueprint the corp hangar does
          not hold (it reads character blueprints too, owned_blueprints_scope;
          the planner reads the corp hangar only): not planned, not a purchase;
        - else (blueprint_sde_fallback, or no source): truly unowned, so the
          BPO analysis says whether to buy it, and "buy the BPO first"."""
        first = decision.overview_row.get("research_first")
        if isinstance(first, dict) and first.get("mode") == "none":
            return
        if bp_type_id in busy_blueprint_type_ids:
            self._skip_bpo_analysis(decision, "owned blueprint in an active industry job: not a purchase")
            self._mark_not_built(decision, "owned blueprint in an active industry job")
            return
        if _priced_from_an_owned_blueprint(decision.overview_row):
            self._skip_bpo_analysis(decision, "priced from an owned blueprint outside the corp hangar: not a purchase")
            self._mark_not_built(decision, "blueprint outside the corp hangar (e.g. character-owned): not planned")
            return
        self._analyze_bpo_investment(
            decision=decision,
            bp_type_id=bp_type_id,
            bp_data=bp_data,
            market_depth_cache=market_depth_cache,
            bpo_opportunities=bpo_opportunities,
            bpo_strong_buy_days=bpo_strong_buy_days,
            bpo_consider_days=bpo_consider_days,
        )
        # Nothing owned to build from (I3, Task 14): the producer priced the
        # row on an assumed blueprint (blueprint_sde_fallback). A manufacture
        # would buy materials and reserve a fee for a job that cannot run.
        self._mark_not_built(decision, "no owned BPO or BPC: buy the BPO first")

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
        ledger: BlueprintLedger | None = None,
        copy_claims: list[tuple[Any, Any]] | None = None,
    ) -> None:
        """Plan T2+ chain: invention pipeline or BPC fallback.

        `bpo_assets_by_type_id` / `bpc_assets_by_type_id` hold only the
        blueprints not installed in an active job. A T1 BPO that is in a job
        (`busy_bpo_type_ids`, also when the snapshot dropped it) gets no copy
        and is not flagged for purchase: it is owned, only busy.

        The plan's ledger (Task 14): `copy_claims` are the T1 BPOs this row
        claimed to copy ((key, asset), score order, never one in research,
        manufacture or another row's copy); the free T1 BPCs it invents from
        are claimed here, after every manufacture took its own. Without a
        copy BPO but with T1 BPCs the row invents from them (I4); a T1 BPO
        that is owned but claimed by another job is not a purchase. A
        direct call without a ledger books on a fresh one."""
        if ledger is None:
            ledger = BlueprintLedger(bpo_assets_by_type_id, bpc_assets_by_type_id)
        copies = list(copy_claims or [])
        if bp_type_id in bpc_assets_by_type_id and bpc_assets_by_type_id[bp_type_id]:
            decision.pipeline_stage = "manufacturing"
            self._claim_row_bpcs(
                ledger, decision.overview_row, decision,
                f"manufacture of {decision.type_name} ({decision.type_id})",
            )
            return
        if bpo_assets_by_type_id.get(bp_type_id):
            # A free owned T2 BPO builds the item: no invention (I2).
            decision.pipeline_stage = "manufacturing"
            return
        if bp_type_id in busy_bpo_type_ids:
            self._mark_not_built(decision, "T2 BPO in an active industry job")
            return

        # No BPC — need invention chain. bp_type_id is the T2 blueprint; the
        # blueprint that is copied and invented from is its T1 source, and the
        # invention activity (with its datacores) lives on that T1 blueprint.
        t1_bp_type_id = invention_source.get(bp_type_id)
        bpcs = self._claim_invention_bpcs(ledger, decision, t1_bp_type_id) if t1_bp_type_id is not None else []
        need = _whole_attempts(decision.overview_row)
        if copies and need is not None and sum(runs for _k, _i, runs in bpcs) >= need:
            # Fix round 3: phase B counted a BPC as used by a T1 row (its
            # variant names it) that in the end built from something else;
            # the BPCs claimed here cover the attempts, so the copy is not
            # needed: release it, and the stage follows what happens.
            for key, _asset in copies:
                ledger.release(key, decision.type_id)
            copies = []
        if not copies and not bpcs and t1_bp_type_id is not None:
            # Fix round 2: phase B planned no copy, yet no BPC was left in the
            # end: a free unclaimed T1 BPO copies instead.
            free = ledger.free_bpos(t1_bp_type_id)
            if free:
                key, asset = free[0]
                ledger.claim(key, decision.type_id, f"copy for {decision.type_name} ({decision.type_id})")
                copies = [(key, asset)]
        if t1_bp_type_id is not None:
            decision.overview_row["t1_blueprint_type_id"] = t1_bp_type_id
        else:
            logger.info(
                "ChainPlanner: no known T1 source blueprint invents into bp_type_id=%s "
                "(type_id=%s); treating the T1 BPO as not owned",
                bp_type_id, decision.type_id,
            )

        if copies or bpcs:
            t1_invention = (blueprint_data.get(t1_bp_type_id) or {}).get("invention") or {}
            row = decision.overview_row
            # Copy first when a T1 BPO is claimed; else invent from the T1
            # BPCs on hand only (I4).
            decision.pipeline_stage = "copying" if copies else "invention"
            row["needs_invention"] = True
            # "We own the T1 BPO, so copy it" — the gate CharacterAssigner reads.
            row["has_t1_bpo"] = bool(copies)
            # The T1 BPOs this row copies: one copy action each (Task 12).
            row["t1_bpo_count"] = len(copies)
            row["t1_bpo_item_ids"] = [research_first.item_id(a) for _k, a in copies]
            # One invention job per BPC, of at most its runs (I2): the free
            # BPCs this row claimed, as [item_id, runs], most runs first.
            row["t1_bpc_runs_by_item"] = [[item, runs] for _k, item, runs in bpcs]
            row["t1_bpc_runs_on_hand"] = sum(runs for _k, _item, runs in bpcs)
            # Runs per copy are capped at the SDE maxProductionLimit (None: unknown).
            row["t1_max_runs_per_copy"] = _max_runs_per_job(blueprint_data.get(t1_bp_type_id) or {})
            # The SDE copy time per run (Task A item 5): with the runs per
            # copy it caps the copies per job (30-day job limit).
            row["t1_copy_seconds_per_run"] = _copy_seconds(blueprint_data.get(t1_bp_type_id) or {})
            row["invention_materials"] = list(t1_invention.get("materials") or [])
            if not copies:
                logger.info(
                    "ChainPlanner: type_id=%s: no free T1 BPO (%s) to copy; inventing from %d T1 BPC(s) on hand",
                    decision.type_id, t1_bp_type_id, len(bpcs),
                )
        elif t1_bp_type_id is not None and ledger.owns_bpos(t1_bp_type_id):
            # Owned and free of active jobs, but every one is claimed today.
            holders = _holders(ledger.bpo_claim_labels(t1_bp_type_id))
            decision.pipeline_stage = "copying"
            self._skip_bpo_analysis(
                decision,
                f"the T1 BPO ({t1_bp_type_id}) is used by {holders} today: no copy in this plan; "
                "it is owned, so not a purchase",
            )
            self._mark_not_built(decision, _t2_blocked_reason("bpo", t1_bp_type_id, holders))
            decision.overview_row["replan_on"] = {"kind": "t2", "t1": t1_bp_type_id, "blocked": "bpo"}
        elif t1_bp_type_id is not None and ledger.bpc_claim_labels(t1_bp_type_id):
            # Owned T1 BPCs, all taken by other jobs today, and no BPO: not
            # a purchase either (fix round 2).
            holders = _holders(ledger.bpc_claim_labels(t1_bp_type_id))
            self._skip_bpo_analysis(decision, f"the T1 BPCs ({t1_bp_type_id}) are used by {holders} today: owned")
            self._mark_not_built(decision, _t2_blocked_reason("bpcs", t1_bp_type_id, holders))
            decision.overview_row["replan_on"] = {"kind": "t2", "t1": t1_bp_type_id, "blocked": "bpcs"}
        elif t1_bp_type_id is not None and t1_bp_type_id in busy_bpo_type_ids:
            decision.pipeline_stage = "copying"
            self._skip_bpo_analysis(
                decision,
                f"the T1 BPO ({t1_bp_type_id}) is in an active industry job: no copy in this plan; "
                "it is owned, so not a purchase",
            )
            # No copy, so no invention and no T2 BPC to build from.
            self._mark_not_built(decision, "T1 BPO in an active industry job")
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
            # Nothing can build it in this plan: a manufacture action would
            # buy its materials for a job that cannot run (I2).
            t1_name = str((blueprint_data.get(t1_bp_type_id) or {}).get("type_name") or "") if t1_bp_type_id else ""
            purchase = (
                f"buy a T1 BPO ({f'{t1_name}, ' if t1_name else ''}{t1_bp_type_id}) first to copy and invent"
                if t1_bp_type_id is not None
                else "buy a T1 BPO first to copy and invent (no known T1 source in the SDE)"
            )
            self._mark_not_built(decision, f"no T2 BPC and no T1 BPO to copy: {purchase}")

    @staticmethod
    def _claim_invention_bpcs(
        ledger: BlueprintLedger, decision: ItemDecision, t1_bp_type_id: int,
    ) -> list[tuple[Any, int | None, int]]:
        """Claim free T1 BPCs for this row's invention, most runs first, until
        their runs reach the producer's whole attempts (all of them when that
        is unknown), so a lower row can still use the rest."""
        need = _whole_attempts(decision.overview_row)
        picked = _pick_bpcs(ledger.free_bpcs(t1_bp_type_id), need)
        label = f"invention for {decision.type_name} ({decision.type_id})"
        claimed = [e for e in picked if ledger.claim(e[0], decision.type_id, label)]
        return sorted(claimed, key=lambda e: (-e[2], str(e[0])))

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
        taken: dict[int, str] | None = None,
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
        does not wait for a research slot.

        `taken` (Task 14): {BPO index: the job} for the BPOs another row's
        job has claimed in the plan's ledger (a copy). Unavailable like a
        busy BPO, with that job in the reason; research_first records the
        BPO index it builds from (`build_index`) for the ledger."""
        row = decision.overview_row
        if not bpo_plan or not research_first.batch_uses_bpo(row):
            return
        if isinstance(row.get("research_first"), dict):
            return  # already not built: its owned BPC is in an active job
        assets, priced, research = bpo_plan["assets"], bpo_plan["priced"], bpo_plan["research"]
        busy: set[int] = set(bpo_plan.get("busy") or ())
        # A busy BPO keeps its own (stronger) cause.
        taken = {i: label for i, label in (taken or {}).items() if i not in busy}
        researching = research is not None and bool(
            row.get("needs_me_research") or row.get("needs_te_research")
        )
        priced_busy = priced in busy or priced in taken
        if not priced_busy and not researching:
            return
        if not priced_busy and research != priced:
            build = research_first.describe_bpo(assets[priced])
            row["research_first"] = {
                "mode": "other_bpo",
                "note": f"researching another BPO ({research_first.describe_bpo(assets[research])}); "
                        f"the batch builds from the BPO at {build}",
                "build_note": f"built from the BPO at {build}",
                "build_index": priced,
            }
            logger.info("ChainPlanner: type_id=%s: %s", decision.type_id, row["research_first"]["note"])
            return
        if priced in busy:
            cause = "BPO in an active industry job"
        elif priced in taken:
            cause = f"BPO used by {taken[priced]} today"
        else:
            cause = "BPO in research"
        # Busy (or claimed) is a fact of this plan, research a plan that needs a slot.
        flags = {"unconditional": True} if priced_busy else {}
        unavailable = busy | set(taken) | ({research} if researching else set())

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
                    "build_note": f"built from another BPO at {build}", "build_index": other, **flags,
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
                "build_index": other,
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
                if priced_busy:
                    # A fact of this plan, not a research slot's outcome.
                    decision.pipeline_stage = NOT_BUILT_STAGE
                    # The same persisted suffix as _mark_not_built (Task 13
                    # carry 2); its note is f"{reason}: not built from it in
                    # this plan". Not routed through _mark_not_built, which
                    # would drop the unsized note and the shopping-list note.
                    decision.decision_reason = (
                        f"{decision.decision_reason}; not built: {cause}{unsized_note}"
                    )
                    row["not_built_suffix"] = f"; not built: {cause}{unsized_note}"
                    if priced in taken:
                        # Task A item 6: another row's job holds the BPO; when
                        # that job gets no slot the row is re-planned.
                        row["replan_on"] = {"kind": "t1_bpo"}
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
                    "research_duration_seconds_by_level", "research_duration_seconds_by_level_unskilled",
                    "research_duration_seconds_by_level_base", "research_implant_time_multiplier_by_character_id"):
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
        ledger: BlueprintLedger | None = None,
        top_level_build_types: set[int] | frozenset[int] = frozenset(),
        stock: dict[int, int] | None = None,
        need_sink: dict[int, int] | None = None,
    ) -> list[dict[str, Any]]:
        """This batch's materials that the corp owns a BPO for, with the
        quantity the batch actually consumes. `row` is the parent row to
        size from (default: the decision's).

        The plan's ledger (Task 14): a component is requested only when a
        BPO of it is unclaimed (not in research, a copy or a manufacture
        today), and never when the component is a top-level build row
        itself (one ItemDecision per type_id). Otherwise the parent buys
        its full need and its persisted reason says why. A request carries
        the unclaimed BPOs: as many of its jobs run in parallel.

        The "component bought" reason (M8) is kept on the row
        (`component_buy_reasons`, {type_id: reason}) and only when corp
        stock does not cover the need; CharacterAssigner adds it to the
        persisted reason only when a manufacture action actually buys it.

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
        if decision.pipeline_stage in ("copying", "invention"):
            # No T2 BPC yet (Task 12 fix round 4): the parent gets no
            # manufacture action, so its components are not sub-built either.
            logger.info(
                "ChainPlanner: type_id=%s: stage %s, no sub-builds until a T2 BPC exists",
                decision.type_id, decision.pipeline_stage,
            )
            return []
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
            name = str(names.get(mat_type_id) or f"type_{mat_type_id}")
            if ledger is not None:
                unclaimed = [a for _k, a in ledger.free_bpos(mat_blueprint_type_id)]
            else:
                unclaimed = list(bpo_assets_by_type_id[mat_blueprint_type_id])
            # Whether the plan buys it is judged on the need pooled across
            # every parent (fix round 2, M8): plan_chain drops the reasons
            # of the components the pooled corp stock covers.
            reasons = decision.overview_row.setdefault("component_buy_reasons", {})
            if not unclaimed or int(mat_type_id) in top_level_build_types:
                if need_sink is not None:
                    need_sink[int(mat_type_id)] = need_sink.get(int(mat_type_id), 0) + int(quantity)
            if not unclaimed:
                holders = _holders(ledger.bpo_claim_labels(mat_blueprint_type_id) if ledger else [])
                reasons.setdefault(int(mat_type_id), f"component {name} bought: its BPO is used by {holders} today")
                continue
            if int(mat_type_id) in top_level_build_types:
                reasons.setdefault(
                    int(mat_type_id),
                    f"component {name} bought: it is a top-level build row of this plan (one decision per type)",
                )
                continue
            requests.append({
                "type_id": mat_type_id,
                "type_name": name,
                "blueprint_type_id": mat_blueprint_type_id,
                # Lowest known ME of the unclaimed BPOs; None = none known, so resolution buys.
                "blueprint_me": _lowest_known_me(unclaimed),
                # Its jobs run in parallel on at most this many BPOs (minor 4).
                "bpo_count": len(unclaimed),
                "quantity": quantity,
                # The component's fee per run as the parent's producer row
                # priced it (fix round 1): used when the component has no
                # overview row of its own.
                "parent_fee_per_run": _non_negative((entries.get(mat_type_id) or {}).get("component_job_fee_per_run")),
                # Why the parent's producer could not price it (8b fix round 1).
                "parent_fee_unknown_reason": (entries.get(mat_type_id) or {}).get("component_job_fee_per_run_unknown_reason"),
                # Named in the buy reason when this parent's fee is unknown (Task 15 fix round 4).
                "parent_type_name": str(decision.type_name),
                **facility_bonus.sub_job_facility_reductions(profile, entries.get(mat_type_id)),
            })
        return requests

    def _resolve_sub_manufacture(
        self,
        *,
        request: dict[str, Any],
        market_depth_cache: dict[int, Any],
        phase1_data: dict[str, Any],
        buy_reasons: dict[int, dict[int, str]] | None = None,
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
            # Task 15 fix round 3: a build cost without its fee is a lower
            # bound, so the component is bought, the producer's rule
            # (IndustryService._build_cost_unknown_reason).
            why = fee_unknown_reason or "no fee published; refresh the product overview"
            logger.warning(
                "ChainPlanner: install fee unknown for sub-manufacturing type_id=%s (%s); buying the component",
                mat_type_id, why,
            )
            if buy_reasons is not None:
                buy_reasons[mat_type_id] = _unknown_fee_buy_reasons(request, self._component_fee_unknown_reason(
                    mat_type_id, phase1_data,
                ))
            return None
        if sub_cost + install_fee >= market_cost:
            return None
        # Linear pricing, so the per-job costs sum to sub_cost exactly.
        # One job per unclaimed BPO runs today; the rest wait on the same
        # BPOs, one after the other (minor 4): job i follows job i - bpos.
        bpos = max(1, int(request.get("bpo_count") or 1))
        job_batches = [
            {
                **job,
                "cost": self._price_materials(job["materials"], market_depth_cache),
                "fee": fee_per_run * job["runs"] if fee_per_run is not None else None,
                "fee_unknown_reason": fee_unknown_reason,
                "after_job": index - bpos + 1 if index >= bpos else None,
            }
            for index, job in enumerate(jobs)
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


def _whole_attempts(row: dict[str, Any]) -> int | None:
    """The producer's whole invention attempts for the row: whole_attempts,
    its parallel lanes' total when they run (Task 12 fix round 2), else the
    best inventor's; None unknown."""
    value = (orow.get_invention_costs(row) or {}).get("whole_attempts")
    if not isinstance(value, bool) and isinstance(value, (int, float)) and value > 0:
        return int(value)
    return None


def _pick_bpcs(free: list[tuple[Any, int | None, int]], need: int | None) -> list[tuple[Any, int | None, int]]:
    """The whole BPCs an invention claims, best fit (M7): while attempts are
    still needed, the smallest BPC that covers the rest, else the largest.
    All of them when the need is unknown. One function for phase B's copy
    decision and phase C's claim (fix round 2), so both pick the same."""
    left_bpcs = sorted((e for e in free if e[2] > 0), key=lambda e: (-e[2], str(e[0])))
    if need is None:
        return left_bpcs
    picked: list[tuple[Any, int | None, int]] = []
    left = need
    while left > 0 and left_bpcs:
        fits = [e for e in left_bpcs if e[2] >= left]
        pick = min(fits, key=lambda e: (e[2], str(e[0]))) if fits else left_bpcs[0]
        left_bpcs.remove(pick)
        picked.append(pick)
        left -= pick[2]
    return picked


def _t2_blocked_reason(blocked: str, t1_bp_type_id: int, holders: str) -> str:
    """Why a T2 row is not built: its T1 BPO ("bpo") or its T1 BPCs
    ("bpcs") are held by other jobs today."""
    if blocked == "bpo":
        return f"T1 BPO ({t1_bp_type_id}) used by {holders} today: no copy in this plan"
    return f"T1 BPCs ({t1_bp_type_id}) used by {holders} today and no T1 BPO to copy: no invention in this plan"


def _released_wording(
    decision: ItemDecision, ledger: BlueprintLedger, gained: list[Any], released: dict[Any, str],
) -> str:
    """ "re-planned: T1 BPC(s) 701 released by invention for X (1), which
    does not use them today", per releasing job, for the released
    blueprints a re-planned row now holds."""
    by_holder: dict[tuple[str, str], list[str]] = {}
    for key in gained:
        if key not in released:
            continue
        kind = (ledger.type_of(key) or ("bpo", 0))[0]
        by_holder.setdefault((kind, released[key]), []).append(str(key))
    parts: list[str] = []
    for (kind, holder), items in by_holder.items():
        if decision.meta_group_id == META_GROUP_T1 and kind == "bpo":
            parts.append(f"its BPO (item {', '.join(items)}) released by {holder}, which does not use it today")
        else:
            what = "T1 BPC(s)" if kind == "bpc" else "T1 BPO(s)"
            parts.append(f"{what} {', '.join(items)} released by {holder}, which does not use them today")
    return "re-planned: " + ("; ".join(parts) if parts else "a blueprint it needs is free again")


def _claim_category(label: str) -> str:
    """The job kind of a ledger claim, from its label."""
    for prefix, category in (("manufacture of", "manufacture"), ("copy for", "copy"), ("invention for", "invention")):
        if label.startswith(prefix):
            return category
    return "research"


def _holders(labels: list[str]) -> str:
    """The jobs holding a blueprint, for a "used by ..." reason; never empty."""
    return ", ".join(labels) if labels else "another job"


def _invention_lanes(row: dict[str, Any]) -> int:
    """The producer's parallel invention lanes (elapsed_parallel.lanes_used),
    1 when it runs them sequentially or does not say."""
    parallel = (orow.get_invention_costs(row) or {}).get("elapsed_parallel")
    if not isinstance(parallel, dict) or parallel.get("sequential_reason") is not None:
        return 1
    try:
        return max(1, int(parallel.get("lanes_used") or 1))
    except (TypeError, ValueError):
        return 1


def _note_bpc_use(use: dict[int, set[int] | None], row: dict[str, Any]) -> None:
    """Record the owned BPCs a row's batch builds on, by blueprint type; an
    unnamed BPC (None) means any of the type's."""
    if not research_first.builds_on_owned_bpcs(row):
        return
    bp_type_id = orow.get_blueprint_type_id(row) or 0
    named = [
        research_first.segment_bpc_item_id(s) for s in research_first.job_segments(row) or []
        if str(s.get("source")) == research_first.OWNED_BPC_SOURCE
    ]
    if not named or any(i is None for i in named):
        use[bp_type_id] = None
    elif bp_type_id not in use or use[bp_type_id] is not None:
        use[bp_type_id] = {*(use.get(bp_type_id) or set()), *(int(i) for i in named)}


def _bpc_used_by_t1_rows(use: dict[int, set[int] | None], bp_type_id: int, item: int | None) -> bool:
    if bp_type_id not in use:
        return False
    named = use[bp_type_id]
    return named is None or item is None or item in named


#: Batch sources that mean the producer priced the row from a blueprint it owns.
_OWNED_SOURCES = research_first.BPO_SOURCES | {research_first.OWNED_BPC_SOURCE}


def _priced_from_an_owned_blueprint(row: dict[str, Any]) -> bool:
    job = orow.get_manufacturing_job(row)
    sources = {str(s.get("source")) for s in research_first.job_segments(row) or []}
    sources.add(str(job.get("blueprint_source_kind")))
    return bool(sources & _OWNED_SOURCES)


def _research_label(row: dict[str, Any]) -> str | None:
    """The research a row schedules on its BPO, for a ledger claim's label."""
    me, te = bool(row.get("needs_me_research")), bool(row.get("needs_te_research"))
    if me and te:
        return "ME and TE research"
    return "ME research" if me else ("TE research" if te else None)


def _append_reason(decision: ItemDecision, reason: str) -> None:
    """Append `; reason` to the persisted decision_reason once."""
    if reason not in (decision.decision_reason or ""):
        decision.decision_reason = f"{decision.decision_reason}; {reason}"


def _one_decision_per_type(
    top_level: list[ItemDecision], subs: list[ItemDecision],
) -> list[ItemDecision]:
    """The plan's decisions with one ItemDecision per type_id (Task 14).

    A component that is a top-level BUILD row is never sub-built (its parent
    buys it: _collect_sub_manufacture_requests), so a collision here is a
    top-level row the plan does not build today (watch / skip / pause). The
    sub-build is what the plan does with the type, so it replaces that row,
    and its persisted reason keeps the row's decision and reason ("as a
    product: ..."). Re-applied on every re-size of the sub-components."""
    sub_by_type = {int(d.type_id): d for d in subs}
    kept: list[ItemDecision] = []
    for decision in top_level:
        sub = sub_by_type.get(int(decision.type_id))
        if sub is not None and decision.decision != "build":
            _append_reason(sub, f"as a product: {decision.decision} ({decision.decision_reason})")
            # M4: the persisted item keeps the product's own figures, and
            # whether its ISK/h is an upper bound (Task 15 fix round 4), so
            # isk_per_hour_is_upper_bound is persisted with that ISK/h.
            for name in ("adjusted_score", "isk_per_hour", "margin_pct", "days_of_supply_current",
                         "effective_velocity", "velocity_unknown_reason", "meta_group_id",
                         "profit_is_upper_bound"):
                setattr(sub, name, getattr(decision, name))
            continue
        kept.append(decision)
    return [*kept, *subs]


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


def _copy_seconds(bp_data: dict[str, Any]) -> int | None:
    """The blueprint's SDE copy time per run in seconds, or None when it is
    missing or not a positive integer."""
    value = bp_data.get("copying")
    if isinstance(value, dict):
        value = value.get("time")
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


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


def _unknown_fee_buy_reasons(request: dict[str, Any], own_row_reason: str | None) -> dict[int, str]:
    """{parent type_id: reason} for a component bought because the merged
    sub job's install fee is unknown (Task 15 fix round 4). Only a parent
    whose own published fee is unknown is told so; the others learn that
    the one sub job they share is unpriced. The component's own row reason
    wins over a parent's, as for the fee itself."""
    name = request["type_name"]
    default_why = "no fee published; refresh the product overview"
    unknown: dict[int, tuple[str, str | None]] = request.get("parent_fee_unknown_by_parent") or {}
    requesters = [int(p) for p in request.get("requested_by_type_ids") or []]
    if not unknown:
        # A request without per-parent fees (not from _merge_sub_requests).
        why = own_row_reason or _str_or_none(request.get("parent_fee_unknown_reason")) or default_why
        return {p: f"component {name} bought: its install fee is unknown ({why})" for p in requesters}
    structures = ", ".join(f"{parent_name}'s" for parent_name, _r in unknown.values())
    reasons: dict[int, str] = {}
    for parent in requesters:
        if parent in unknown:
            parent_name, parent_reason = unknown[parent]
            why = own_row_reason or parent_reason or default_why
            reasons[parent] = f"component {name} bought: its install fee is unknown for {parent_name}'s structure ({why})"
        else:
            reasons[parent] = (
                f"component {name} bought: one sub job serves every requester, and its install fee "
                f"is unknown for {structures} structure"
            )
    return reasons


def _str_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None
