"""DailyPlannerService — orchestrator for all nine computation phases.

Background thread pattern mirrors IndustryService (threading.Thread + daemon=True).
Does NOT use AppState; uses a simple Lock + instance attributes for status tracking.
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from eve_online_industry_tracker.application.daily_planner.action_plan_builder import ActionPlanBuilder
from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.feedback_processor import FeedbackProcessor
from eve_online_industry_tracker.application.daily_planner.input_row import (
    PlannerInputError,
    PlannerInputRow,
)
from eve_online_industry_tracker.application.daily_planner.item_decision_engine import ItemDecisionEngine
from eve_online_industry_tracker.application.daily_planner.pipeline_analyzer import PipelineAnalyzer
from eve_online_industry_tracker.application.daily_planner.profitability_scorer import ProfitabilityScorer
from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder
from eve_online_industry_tracker.application.industry.type_metadata import TypeMetadataResolver
from eve_online_industry_tracker.infrastructure.models import (
    BuildPlanItemModel,
    BuildPlanModel,
)

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


def _spot_sell_price(entry: Any) -> float | None:
    """Extract the spot sell price from a market-depth entry, or None."""
    if entry is None:
        return None
    price = _get_attr(entry, "spot_sell_price")
    if price is None:
        return None
    try:
        return float(price)
    except (TypeError, ValueError):
        return None


def compute_freshness_stats(
    plan_items: list[Any],
    market_depth: dict[int, Any],
    drift_threshold_pct: float,
) -> tuple[float, int, int]:
    """Compute freshness plus the counts behind it: (score, comparable, total).

    `total` is the number of plan items given; `comparable` is how many of
    them had both a snapshot and a current price and so actually
    contributed to `score`. Items with no snapshot or no current price are
    excluded from `comparable` — unknown is not the same as stale.

    `score` is 1.0 whenever `comparable` is 0, so a plan is never marked
    stale purely for lack of data. That means a 1.0 score is ambiguous by
    itself: it means either "checked and nothing drifted" or "nothing was
    checkable at all" (e.g. every item's snapshot write silently broke).
    `comparable` is what tells those two apart — callers that care should
    surface it next to the score rather than trusting 1.0 alone.
    """
    threshold = abs(float(drift_threshold_pct)) / 100.0
    total = len(plan_items)
    comparable = 0
    drifted = 0

    for item in plan_items:
        snapshot = _get_attr(item, "snapshot_sell_price")
        if snapshot is None:
            continue
        try:
            snapshot_price = float(snapshot)
        except (TypeError, ValueError):
            continue
        if snapshot_price <= 0:
            continue

        entry = market_depth.get(int(_get_attr(item, "type_id") or 0))
        if entry is None:
            continue
        current_price = _spot_sell_price(entry)
        if current_price is None:
            continue

        comparable += 1
        if abs(current_price - snapshot_price) / snapshot_price > threshold:
            drifted += 1

    if comparable == 0:
        return 1.0, comparable, total
    return 1.0 - (drifted / comparable), comparable, total


def compute_freshness_score(
    plan_items: list[Any],
    market_depth: dict[int, Any],
    drift_threshold_pct: float,
) -> float:
    """Fraction of plan items whose sell price has not drifted past the threshold.

    Each item stores the sell price it was planned against
    (`snapshot_sell_price`); freshness compares that with the current price.
    Items with no snapshot or no current price are excluded from the
    denominator — unknown is not the same as stale.

    Returns 1.0 when nothing is comparable, so a plan is never marked stale
    purely for lack of data. See `compute_freshness_stats` for a version
    that also exposes the comparable/total counts behind this score.
    """
    score, _comparable, _total = compute_freshness_stats(
        plan_items, market_depth, drift_threshold_pct
    )
    return score


def _adm(admin_settings: Any, key: str, fallback: Any) -> Any:
    try:
        return admin_settings.get("daily_planner", key)
    except Exception:
        return fallback


#: Job statuses ESI reports for a *finished* job -- excluded from "active" job
#: queries so delivered/cancelled/reverted jobs stop being counted as in-progress.
#: A deny-list (rather than mirroring industry/service.py's allow-list of
#: ("active", "ready")) because ESI's job status enum is closed and small
#: (active, cancelled, delivered, paused, ready, reverted); enumerating the three
#: terminal ones and excluding them also keeps "paused" jobs -- which still hold a
#: manufacturing slot -- counted as active, unlike an allow-list of ("active", "ready").
_TERMINAL_JOB_STATUSES = ("delivered", "cancelled", "reverted")


def _no_sde_session() -> None:
    """Fallback sde_session provider for a session_provider that has no sde_session().

    Used only so DailyPlannerService.__init__ never raises AttributeError while
    building its TypeMetadataResolver — stub collaborators in tests may not
    implement sde_session(). It intentionally returns None rather than a lambda,
    matching TypeMetadataResolver's own SessionProvider protocol.
    """
    return None


class DailyPlannerService:
    """Orchestrate plan computation (Phases 1–9) in a background thread."""

    def __init__(
        self,
        industry_service: Any,
        corporations_service: Any,
        characters_service: Any,
        sales_history_service: Any,
        pricing_suggestion_service: Any,
        market_pricing_service: Any,
        realized_profit_service: Any,
        repo: Any,
        admin_settings: Any,
        session_provider: Any,
    ) -> None:
        # `realized_profit_service` is accepted but intentionally not stored
        # or forwarded: FeedbackProcessor no longer takes it (its one use
        # site, `get_realized_profit_for_type`, does not exist anywhere in
        # `src/` and was dead code), and nothing else on this service used
        # it either. The parameter itself stays in the signature because
        # dropping it would ripple into `flask_app/bootstrap.py` and several
        # test constructions that pass it positionally/by keyword.
        self._industry = industry_service
        self._corporations = corporations_service
        self._characters = characters_service
        self._sales_history = sales_history_service
        self._pricing_suggestions = pricing_suggestion_service
        self._market_pricing = market_pricing_service
        self._repo = repo
        self._admin = admin_settings
        self._session_provider = session_provider

        # Thread-safe status tracking (no AppState dependency)
        self._lock = threading.Lock()
        self._status: str = "idle"   # "idle" | "running" | "done" | "failed"
        self._error: str | None = None

        # Sub-components
        self._feedback_processor = FeedbackProcessor(
            repo=repo,
            admin_settings=admin_settings,
            session_provider=session_provider,
        )
        self._pipeline_analyzer = PipelineAnalyzer()
        self._profitability_scorer = ProfitabilityScorer()
        self._decision_engine = ItemDecisionEngine()
        self._chain_planner = ChainPlanner(
            industry_service=industry_service,
            session_provider=session_provider,
            admin_settings=admin_settings,
        )
        self._character_assigner = CharacterAssigner()
        self._shopping_list_builder = ShoppingListBuilder()
        self._action_plan_builder = ActionPlanBuilder()

        # Blueprint-ness comes from the SDE category (see type_metadata.py), so
        # indexing corp assets into BPOs/BPCs needs an SDE-backed resolver. Some
        # tests construct this service with stub collaborators whose session
        # providers don't implement sde_session() — guard against that so
        # construction never raises; a missing sde_session() just means the
        # resolver has no SDE data to look up.
        sde_session_provider = getattr(session_provider, "sde_session", None)
        if not callable(sde_session_provider):
            sde_session_provider = _no_sde_session
        self._meta_resolver = TypeMetadataResolver(
            sde_session_provider=sde_session_provider,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def compute_plan_async(self) -> None:
        """Spawn background thread to compute the plan. No-ops if already running."""
        with self._lock:
            if self._status == "running":
                logger.info("DailyPlannerService: compute already running, ignoring request")
                return
            self._status = "running"
            self._error = None

        thread = threading.Thread(
            target=self._run_compute,
            daemon=True,
            name="daily-planner-compute",
        )
        thread.start()

    def get_compute_status(self) -> dict[str, Any]:
        """Return current computation status."""
        with self._lock:
            return {"status": self._status, "error": self._error}

    def get_active_plan(self) -> dict[str, Any]:
        """Fetch the active plan and compute freshness_score for the response.

        Freshness is computed fresh on every call but not persisted here — a
        GET must not have a write side effect. The persisted freshness_score
        is only ever updated by the recompute pipeline (`_phase_9_persist`).
        """
        plan = self._repo.get_active_plan()
        if plan is None:
            return {"plan": None, "items": [], "actions": []}

        items = self._repo.get_plan_items(int(plan.id))
        actions = self._repo.get_actions(int(plan.id))

        freshness, comparable_items, total_items = self._compute_freshness_score(plan, items)

        return {
            "plan": _model_to_dict(plan),
            "items": [_model_to_dict(i) for i in items],
            "actions": [_model_to_dict(a) for a in actions],
            "freshness_score": freshness,
            # A 1.0 freshness_score alone can't distinguish "checked and
            # nothing drifted" from "nothing was comparable" (e.g. every
            # item's snapshot_sell_price is NULL). These two counts make
            # that visible in the response instead of only in a log line.
            "freshness_comparable_items": comparable_items,
            "freshness_total_items": total_items,
        }

    def mark_action_done(self, action_id: int) -> None:
        self._repo.mark_action_done(action_id)

    def get_action(self, action_id: int) -> dict[str, Any] | None:
        row = self._repo.get_action(action_id)
        return _model_to_dict(row) if row is not None else None

    def set_action_status(self, action_id: int, status: str) -> None:
        self._repo.set_action_status(action_id, status)

    def get_analytics(self) -> dict[str, Any]:
        """Fetch self-learning accuracy stats for all tracked type_ids."""
        plan = self._repo.get_active_plan()
        if plan is None:
            return {"items": [], "plan_history": []}

        items = self._repo.get_plan_items(int(plan.id))
        type_ids = [int(i.type_id) for i in items]
        weights = self._repo.get_weights(type_ids)
        invention_rates = self._repo.get_invention_success_rates(type_ids)

        # Competition data
        try:
            hub = str(_adm(self._admin, "planner_market_hub", "jita"))
            market_depth = self._repo.get_market_depth(type_ids, hub)
        except Exception:
            market_depth = {}

        # Margin correlation data
        try:
            from eve_online_industry_tracker.infrastructure.models import MarginCorrelationCacheModel  # noqa: PLC0415
            mc_session = self._session_provider.app_session()
            try:
                margin_rows = mc_session.query(MarginCorrelationCacheModel).filter(
                    MarginCorrelationCacheModel.type_id.in_(type_ids)
                ).all()
                margin_by_type = {int(r.type_id): r for r in margin_rows}
            finally:
                mc_session.close()
        except Exception:
            margin_by_type: dict[int, Any] = {}

        # Invention details from raw SQL
        try:
            from sqlalchemy import text  # noqa: PLC0415
            inv_session = self._session_provider.app_session()
            try:
                inv_placeholders = ",".join(str(t) for t in type_ids) or "0"
                inv_rows = inv_session.execute(text(
                    f"SELECT type_id, "
                    f"CAST(SUM(CASE WHEN was_success THEN 1 ELSE 0 END) AS REAL) / COUNT(*) AS actual_rate, "
                    f"COUNT(*) AS attempts, "
                    f"AVG(theoretical_success_pct) AS theoretical_rate "
                    f"FROM invention_outcome_log "
                    f"WHERE type_id IN ({inv_placeholders}) GROUP BY type_id"
                )).fetchall()
            finally:
                inv_session.close()
            inv_details = {
                int(r[0]): {
                    "actual_rate": float(r[1]),
                    "attempts": int(r[2]),
                    "theoretical_rate": float(r[3] or 0.0),
                }
                for r in inv_rows
            }
        except Exception:
            inv_details: dict[int, Any] = {}

        result = []
        for item in items:
            tid = int(item.type_id)
            w = weights.get(tid)
            outcomes = self._repo.get_outcomes(tid, limit=20)
            md = market_depth.get(tid)
            mc = margin_by_type.get(tid)
            result.append({
                "type_id": tid,
                "type_name": item.type_name,
                "decision": item.decision,
                "accuracy_ema": float(getattr(w, "accuracy_ema", 1.0)) if w else 1.0,
                "velocity_multiplier": float(getattr(w, "velocity_multiplier", 1.0)) if w else 1.0,
                "cost_multiplier": float(getattr(w, "cost_multiplier", 1.0)) if w else 1.0,
                "sample_count": int(getattr(w, "sample_count", 0)) if w else 0,
                "confidence_tier": str(getattr(w, "confidence_tier", "low")) if w else "low",
                "invention_success_rate": invention_rates.get(tid),
                "recent_outcomes": [_model_to_dict(o) for o in outcomes],
                # Competition data
                "competition_index": float(getattr(md, "competition_index", None) or 0.0) if md else None,
                "competitor_units": int(getattr(md, "competitor_units", 0) or 0) if md else None,
                "market_snapshot_at": str(getattr(md, "snapshot_at", None) or "") if md else None,
                # Margin correlation data
                "pearson_correlation": (
                    float(getattr(mc, "pearson_correlation", None))
                    if (mc and mc.pearson_correlation is not None)
                    else None
                ),
                "is_squeeze_sensitive": bool(getattr(mc, "is_squeeze_sensitive", False)) if mc else False,
                "correlation_data_points": int(getattr(mc, "data_points", 0) or 0) if mc else 0,
                # Invention details
                "invention_details": inv_details.get(tid),
            })

        # Plan history (last N days, default 90) -- cutoff must be a datetime,
        # not an ISO string: BuildPlanModel.created_at is a DateTime column,
        # and SQLite silently rejects (rather than coerces) a string compared
        # against it, so a bare `except` around this used to hide a permanently
        # empty plan_history. Let a genuinely broken query raise instead.
        history_days = int(_adm(self._admin, "planner_history_days", 90))
        cutoff = datetime.utcnow() - timedelta(days=history_days)
        ph_session = self._session_provider.app_session()
        try:
            plan_history_rows = (
                ph_session.query(BuildPlanModel)
                .filter(BuildPlanModel.created_at >= cutoff)
                .order_by(BuildPlanModel.created_at.desc())
                .limit(history_days)
                .all()
            )
        finally:
            ph_session.close()
        plan_history = [
            {
                "id": int(p.id),
                "created_at": p.created_at.isoformat() if p.created_at else None,
                "status": str(p.status),
                "freshness_score": float(p.freshness_score or 1.0),
            }
            for p in plan_history_rows
        ]

        return {"items": result, "plan_history": plan_history}

    # ──────────────────────────────────────────────────────────────────────────
    # Private: computation phases
    # ──────────────────────────────────────────────────────────────────────────

    def _run_compute(self) -> None:
        """Main compute pipeline — runs in background thread."""
        logger.info("DailyPlannerService: starting plan computation")
        try:
            # Phase 1 — Data collection + feedback processing
            phase1_data = self._phase_1_collect()

            if not phase1_data["overview_rows"]:
                logger.error(
                    "DailyPlannerService: aborting — no product overview rows available. "
                    "Open the Industry Builder and refresh the overview first, then recompute."
                )
                with self._lock:
                    self._status = "failed"
                    self._error = (
                        "No product overview available. Open Industry Builder → refresh the "
                        "product overview, then recompute the daily plan."
                    )
                return

            # Phase 2 — Pipeline state per item
            pipeline_states = self._phase_2_pipeline(phase1_data)

            # Phase 3 — Profitability scoring
            scored_items = self._phase_3_score(pipeline_states, phase1_data)

            # Phase 4 — Item decisions (Pass 1 — top-level only)
            top_level_decisions = self._phase_4_decide(scored_items, pipeline_states, phase1_data)

            # Phase 5 — Chain planning (Pass 2 — adds sub-components)
            chain_plan = self._phase_5_chain(top_level_decisions, phase1_data)

            # Phase 6 — Character assignment
            assigned_actions = self._phase_6_assign(chain_plan, phase1_data)

            # Phase 7 — Shopping list
            shopping_items = self._phase_7_shopping(assigned_actions, phase1_data)

            # Phase 8 — Action list
            action_log_rows = self._phase_8_actions(assigned_actions, shopping_items, phase1_data, chain_plan)

            # Phase 9 — Persistence
            self._phase_9_persist(
                phase1_data=phase1_data,
                chain_plan=chain_plan,
                action_log_rows=action_log_rows,
            )

            with self._lock:
                self._status = "done"
            logger.info("DailyPlannerService: plan computation complete")

        except PlannerInputError as exc:
            # A contract violation must abort the whole computation, not be
            # swallowed into a generic failure message: naming the offending
            # field and type_id here is what lets someone fix the producer
            # (or the contract) instead of guessing which row broke.
            logger.exception(
                "DailyPlannerService: plan computation failed — input contract violation "
                "(type_id=%s, field=%s)", exc.type_id, exc.field,
            )
            with self._lock:
                self._status = "failed"
                self._error = (
                    f"Overview row type_id={exc.type_id} violates the planner input "
                    f"contract: {exc.field} {exc.detail}."
                )
        except Exception as exc:
            logger.exception("DailyPlannerService: plan computation failed")
            with self._lock:
                self._status = "failed"
                self._error = str(exc)

    def _phase_1_collect(self) -> dict[str, Any]:
        """Phase 1: collect all data needed for plan computation."""
        logger.info("DailyPlannerService: Phase 1 — data collection")

        # Feedback processing first (before new scoring begins)
        feedback_count = self._feedback_processor.process_pending_feedback()
        if feedback_count > 0:
            logger.info("DailyPlannerService: processed %d feedback actions", feedback_count)

        # Corp wallet
        corp_wallet = self._get_corp_wallet()

        # IndustryService overview rows
        overview_rows = self._get_overview_rows()

        # Active industry jobs (corp + character)
        industry_jobs = self._get_industry_jobs()

        # Corp assets
        corp_assets = self._get_corp_assets()

        # Corp market orders
        corp_orders = self._get_corp_orders()

        # Learning weights
        type_ids = [int(row.get("type_id") or 0) for row in overview_rows if row.get("type_id")]
        weights = self._repo.get_weights(type_ids) if type_ids else {}

        # Sell velocity per type_id
        sell_velocities = self._get_sell_velocities(type_ids)

        # Market depth cache (from pre-populated tables written by MarketIntelligenceJob)
        hub = str(_adm(self._admin, "planner_market_hub", "jita"))
        market_depth = self._repo.get_market_depth(type_ids, hub) if type_ids else {}

        # Margin correlation cache
        margin_correlations = self._get_margin_correlations(type_ids)

        # Invention success rates
        min_attempts = int(_adm(self._admin, "planner_invention_min_attempts", 10))
        invention_rates = self._repo.get_invention_success_rates(type_ids, min_attempts) if type_ids else {}

        # Tritanium 7d price trend (for squeeze penalty gate)
        trit_trend_7d = self._get_trit_trend_7d()

        # Pricing suggestions (for RELIST actions)
        pricing_suggestions = self._get_pricing_suggestions()

        # Blueprint data (SDE) — loaded for chain planning
        blueprint_data = self._get_blueprint_data(overview_rows)

        # BPO and BPC asset indexes
        bpo_assets_by_type_id, bpc_assets_by_type_id = self._index_blueprint_assets(corp_assets)

        return {
            "corp_wallet": corp_wallet,
            "overview_rows": overview_rows,
            "input_rows": self._build_input_rows(overview_rows),
            "industry_jobs": industry_jobs,
            "corp_assets": corp_assets,
            "corp_orders": corp_orders,
            "weights": weights,
            "sell_velocities": sell_velocities,
            "market_depth_cache": market_depth,
            "margin_correlations": margin_correlations,
            "invention_success_rates": invention_rates,
            "trit_trend_7d": trit_trend_7d,
            "pricing_suggestions": pricing_suggestions,
            "blueprint_data": blueprint_data,
            "bpo_assets_by_type_id": bpo_assets_by_type_id,
            "bpc_assets_by_type_id": bpc_assets_by_type_id,
            "hub": hub,
        }

    def _build_input_rows(self, overview_rows: list[dict[str, Any]]) -> list[PlannerInputRow]:
        """Validate every overview row against the planner's input contract.

        Prefetches type metadata in one SDE query, then converts. A contract
        violation propagates: a plan built from the rows that happened to parse
        would silently omit whatever failed, which is the exact failure mode
        this contract exists to end.
        """
        type_ids = [int(r.get("type_id") or 0) for r in overview_rows if isinstance(r, dict)]
        self._meta_resolver.prefetch([tid for tid in type_ids if tid > 0])
        return [
            PlannerInputRow.from_overview(row, meta_groups=self._meta_resolver)
            for row in overview_rows
        ]

    def _phase_2_pipeline(self, phase1: dict[str, Any]) -> list[Any]:
        logger.info("DailyPlannerService: Phase 2 — pipeline analysis")
        return self._pipeline_analyzer.analyze(
            input_rows=phase1["input_rows"],
            industry_jobs=phase1["industry_jobs"],
            corp_assets=phase1["corp_assets"],
            market_depth_cache=phase1["market_depth_cache"],
            weights=phase1["weights"],
            sell_velocities=phase1["sell_velocities"],
            meta_resolver=self._meta_resolver,
        )

    def _phase_3_score(
        self,
        pipeline_states: list[Any],
        phase1: dict[str, Any],
    ) -> list[Any]:
        logger.info("DailyPlannerService: Phase 3 — profitability scoring")
        input_by_type = {r.type_id: r for r in phase1["input_rows"]}
        trit_trend_7d: float | None = phase1.get("trit_trend_7d")
        scored = []
        for ps in pipeline_states:
            row = input_by_type.get(ps.type_id)
            if row is None:
                continue
            weights = phase1["weights"].get(ps.type_id)
            market_depth = phase1["market_depth_cache"].get(ps.type_id)
            margin_corr = phase1["margin_correlations"].get(ps.type_id)
            try:
                scored_item = self._profitability_scorer.score(
                    pipeline=ps,
                    row=row,
                    weights=weights,
                    market_depth=market_depth,
                    margin_correlation=margin_corr,
                    trit_trend_7d=trit_trend_7d,
                )
                scored.append(scored_item)
            except Exception:
                logger.exception("DailyPlannerService: Phase 3 error for type_id=%s", ps.type_id)
        return scored

    def _phase_4_decide(
        self,
        scored_items: list[Any],
        pipeline_states: list[Any],
        phase1: dict[str, Any],
    ) -> list[Any]:
        logger.info("DailyPlannerService: Phase 4 — item decisions (Pass 1)")
        pipeline_by_type = {ps.type_id: ps for ps in pipeline_states}
        overview_by_type = {int(r["type_id"]): r for r in phase1["overview_rows"] if r.get("type_id")}
        input_by_type = {r.type_id: r for r in phase1["input_rows"]}
        decisions = []
        for scored in scored_items:
            ps = pipeline_by_type.get(scored.type_id)
            if ps is None:
                continue
            row = overview_by_type.get(scored.type_id, {})
            input_row = input_by_type.get(scored.type_id)
            try:
                decision = self._decision_engine.decide(
                    scored=scored,
                    pipeline=ps,
                    overview_row=row,
                    admin_settings=self._admin,
                    # The overview row never carries a numeric meta group id
                    # (only meta_group_name); the resolved SDE value lives on
                    # the validated input row instead.
                    meta_group_id=input_row.meta_group_id if input_row is not None else None,
                )
                decisions.append(decision)
            except Exception:
                logger.exception("DailyPlannerService: Phase 4 error for type_id=%s", scored.type_id)
        return decisions

    def _phase_5_chain(
        self,
        top_level_decisions: list[Any],
        phase1: dict[str, Any],
    ) -> Any:
        logger.info("DailyPlannerService: Phase 5 — chain planning (Pass 2)")
        return self._chain_planner.plan_chain(
            top_level_decisions=top_level_decisions,
            phase1_data=phase1,
        )

    def _phase_6_assign(
        self,
        chain_plan: Any,
        phase1: dict[str, Any],
    ) -> list[Any]:
        logger.info("DailyPlannerService: Phase 6 — character assignment")
        return self._character_assigner.assign(
            chain_plan=chain_plan,
            industry_jobs=phase1["industry_jobs"],
            characters_service=self._characters,
            admin_settings=self._admin,
        )

    def _phase_7_shopping(
        self,
        assigned_actions: list[Any],
        phase1: dict[str, Any],
    ) -> list[Any]:
        logger.info("DailyPlannerService: Phase 7 — shopping list")
        return self._shopping_list_builder.build(
            assigned_actions=assigned_actions,
            corp_assets=phase1["corp_assets"],
            market_depth_cache=phase1["market_depth_cache"],
            admin_settings=self._admin,
            blueprint_data=phase1["blueprint_data"],
            meta_resolver=self._meta_resolver,
        )

    def _phase_8_actions(
        self,
        assigned_actions: list[Any],
        shopping_items: list[Any],
        phase1: dict[str, Any],
        chain_plan: Any = None,
    ) -> list[Any]:
        logger.info("DailyPlannerService: Phase 8 — action list")
        bpo_opportunities = getattr(chain_plan, "bpo_opportunities", None) if chain_plan is not None else None
        # DELIVER rows need the pilot name for installer_id and the item name
        # for product_type_id -- neither is a real column on either job ORM
        # model. Resolve both here rather than have ActionPlanBuilder reach
        # for characters_service itself: the name map mirrors what Phase 6's
        # CharacterAssigner already resolves for char_slots.
        character_name_map = self._character_assigner.get_character_name_map(self._characters)
        # plan_id is set in Phase 9 (not known yet); use 0 as placeholder
        return self._action_plan_builder.build(
            plan_id=0,
            assigned_actions=assigned_actions,
            shopping_items=shopping_items,
            pricing_suggestions=phase1["pricing_suggestions"],
            industry_jobs=phase1["industry_jobs"],
            admin_settings=self._admin,
            bpo_opportunities=bpo_opportunities,
            character_name_map=character_name_map,
            meta_resolver=self._meta_resolver,
        )

    def _phase_9_persist(
        self,
        phase1_data: dict[str, Any],
        chain_plan: Any,
        action_log_rows: list[Any],
    ) -> None:
        logger.info("DailyPlannerService: Phase 9 — persistence")

        # Archive current active plan
        self._repo.archive_active_plan()

        # Compute market snapshot hash
        market_depth_cache = phase1_data["market_depth_cache"]
        overview_rows = phase1_data["overview_rows"]
        snapshot_hash = self._compute_snapshot_hash(overview_rows, market_depth_cache)

        # Corp wallet
        corp_wallet = float(phase1_data.get("corp_wallet") or 0.0)

        now = _now()
        plan = BuildPlanModel(
            created_at=now,
            updated_at=now,
            status="active",
            corp_wallet_snapshot=corp_wallet,
            market_snapshot_hash=snapshot_hash,
            freshness_score=1.0,
            plan_summary_json=None,
        )
        plan_id = self._repo.insert_plan(plan)

        # Build plan items (each stamped with the snapshot price it was
        # planned against, taken from the same market_depth_cache used for
        # the hash above) *before* persisting them. Freshness must be
        # computed from these in-memory objects, not from the list
        # insert_plan_items() returns afterwards -- SQLAlchemy expires ORM
        # attributes on commit, and insert_plan_items() closes its session,
        # so reading .snapshot_sell_price / .type_id post-insert raises
        # DetachedInstanceError.
        plan_items = self._build_plan_items(plan_id, chain_plan.decisions, market_depth_cache)

        drift_threshold_pct = float(_adm(self._admin, "planner_price_drift_threshold_pct", 5.0))
        freshness = compute_freshness_score(plan_items, market_depth_cache, drift_threshold_pct)

        if plan_items:
            self._repo.insert_plan_items(plan_items)

        # This is the only place freshness_score is written -- a plain GET
        # of the active plan (get_active_plan) must never have a write
        # side effect.
        self._repo.update_freshness_score(plan_id, freshness)

        # Fix plan_id on action rows (was 0 in Phase 8)
        for row in action_log_rows:
            row.plan_id = plan_id
        if action_log_rows:
            self._repo.insert_actions(action_log_rows)

        # Cleanup old action log rows
        cutoff_days = int(_adm(self._admin, "planner_history_days", 90))
        deleted = self._repo.purge_old_action_log_rows(cutoff_days)
        if deleted > 0:
            logger.info("DailyPlannerService: purged %d old action log rows", deleted)

    # ──────────────────────────────────────────────────────────────────────────
    # Helper methods
    # ──────────────────────────────────────────────────────────────────────────

    def _get_corp_wallet(self) -> float:
        """Master wallet (division 1) balance, or 0.0 when unavailable."""
        try:
            corps = self._corporations.list_corporations()
        except (KeyError, TypeError, ValueError):
            logger.exception("DailyPlannerService: failed to list corporations")
            return 0.0

        if not corps:
            return 0.0
        corp = corps[0] if isinstance(corps, list) else corps
        wallets = corp.get("wallets") if isinstance(corp, dict) else getattr(corp, "wallets", None)
        return _select_division_one_balance(wallets)

    def _get_overview_rows(self) -> list[dict[str, Any]]:
        """Get IndustryService overview rows from cache.

        Returns an empty list when no cached overview is available — callers
        must treat this as a soft abort and surface a user-visible message.
        """
        try:
            result = self._industry.get_cached_overview_rows()
            if isinstance(result, list):
                return result
            # None → not yet computed
            logger.warning(
                "DailyPlannerService: no cached product overview available — "
                "open the Industry Builder and refresh the product overview first"
            )
            return []
        except Exception:
            logger.exception("DailyPlannerService: failed to get overview rows")
            return []

    def _get_industry_jobs(self) -> list[Any]:
        """Active corp + character industry jobs -- delivered/cancelled/reverted jobs are not active.

        Slot capacity in EVE is per character, so a personally-installed job
        (CharacterIndustryJobsModel) occupies one of that pilot's slots exactly
        like a corp job they installed (CorporationIndustryJobsModel, keyed by
        installer_id) -- querying only the corp table under-counts used slots
        and lets the planner assign work EVE will then refuse to start.

        Mirrors IndustryService.industry_active_jobs() (industry/service.py
        ~7876-7891), which combines both tables for the same reason, but
        deliberately diverges from its filter: that method uses
        status == "active", while this keeps the deny-list
        (_TERMINAL_JOB_STATUSES) on both queries -- a "paused" job still holds
        a slot, and the deny-list (unlike an allow-list of "active") keeps it
        counted as active.
        """
        try:
            session = self._session_provider.app_session()
            try:
                from eve_online_industry_tracker.infrastructure.models import (
                    CharacterIndustryJobsModel,
                    CorporationIndustryJobsModel,
                )

                corp_jobs = (
                    session.query(CorporationIndustryJobsModel)
                    .filter(
                        CorporationIndustryJobsModel.status.notin_(_TERMINAL_JOB_STATUSES)
                        | CorporationIndustryJobsModel.status.is_(None)
                    )
                    .all()
                )
                char_jobs = (
                    session.query(CharacterIndustryJobsModel)
                    .filter(
                        CharacterIndustryJobsModel.status.notin_(_TERMINAL_JOB_STATUSES)
                        | CharacterIndustryJobsModel.status.is_(None)
                    )
                    .all()
                )
                return list(char_jobs) + list(corp_jobs)
            finally:
                session.close()
        except (KeyError, TypeError, ValueError):
            logger.exception("DailyPlannerService: failed to get industry jobs")
            return []

    def _get_corp_assets(self) -> list[Any]:
        """Get corp assets."""
        try:
            session = self._session_provider.app_session()
            try:
                from eve_online_industry_tracker.infrastructure.models import CorporationAssetsModel
                return session.query(CorporationAssetsModel).all()
            finally:
                session.close()
        except Exception:
            logger.exception("DailyPlannerService: failed to get corp assets")
            return []

    def _get_corp_orders(self) -> list[Any]:
        """Get corp market orders."""
        try:
            session = self._session_provider.app_session()
            try:
                from eve_online_industry_tracker.infrastructure.models import CorporationMarketOrdersModel
                return session.query(CorporationMarketOrdersModel).filter(
                    CorporationMarketOrdersModel.is_buy_order == False  # noqa: E712
                ).all()
            finally:
                session.close()
        except Exception:
            logger.exception("DailyPlannerService: failed to get corp orders")
            return []

    def _get_sell_velocities(self, type_ids: list[int]) -> dict[int, float]:
        """Compute sell velocity per day for each type_id using SalesHistoryService."""
        velocities: dict[int, float] = {}
        try:
            # Get corp_id from corporations service
            corp_id = self._get_corp_id()
        except Exception:
            return velocities

        for type_id in type_ids:
            try:
                txs = self._sales_history.get_sold_history(
                    character_id=0,
                    corporation_id=corp_id,
                    type_id=type_id,
                    days=30,
                )
                total_sold = sum(int(tx.get("quantity") or 0) for tx in txs)
                velocities[type_id] = total_sold / 30.0
            except Exception:
                velocities[type_id] = 0.0
        return velocities

    def _get_corp_id(self) -> int:
        """Get corp ID from corporations service."""
        try:
            corps = self._corporations.list_corporations()
            corp = corps[0] if isinstance(corps, list) else corps
            return int(getattr(corp, "corporation_id", 0) or corp.get("corporation_id", 0) if isinstance(corp, dict) else 0)
        except Exception:
            return 0

    def _get_margin_correlations(self, type_ids: list[int]) -> dict[int, Any]:
        """Get margin correlation cache entries."""
        if not type_ids:
            return {}
        try:
            session = self._session_provider.app_session()
            try:
                from eve_online_industry_tracker.infrastructure.models import MarginCorrelationCacheModel
                rows = session.query(MarginCorrelationCacheModel).filter(
                    MarginCorrelationCacheModel.type_id.in_(type_ids)
                ).all()
                return {int(r.type_id): r for r in rows}
            finally:
                session.close()
        except Exception:
            return {}

    def _get_trit_trend_7d(self) -> float | None:
        """Compute Tritanium 7-day price trend % from market history (type_id=34, Jita region)."""
        try:
            from eve_online_industry_tracker.infrastructure.models import MarketHistoryModel
            session = self._session_provider.app_session()
            try:
                cutoff = (datetime.utcnow() - timedelta(days=14)).date().isoformat()
                rows = session.query(MarketHistoryModel).filter(
                    MarketHistoryModel.type_id == 34,
                    MarketHistoryModel.region_id == 10000002,
                    MarketHistoryModel.date >= cutoff,
                ).order_by(MarketHistoryModel.date.desc()).limit(14).all()
            finally:
                session.close()
            prices = [float(r.close) for r in rows if r.close and float(r.close) > 0]
            if len(prices) < 8:
                return None
            recent_avg = sum(prices[:7]) / 7.0
            older_avg = sum(prices[7:]) / len(prices[7:])
            if older_avg <= 0:
                return None
            return (recent_avg - older_avg) / older_avg * 100.0
        except Exception:
            return None

    def _get_pricing_suggestions(self) -> list[Any]:
        """Get relist suggestions from PricingSuggestionService."""
        try:
            result = self._pricing_suggestions.get_suggestions()
            return result if isinstance(result, list) else []
        except AttributeError:
            return []
        except Exception:
            logger.exception("DailyPlannerService: failed to get pricing suggestions")
            return []

    def _get_blueprint_data(self, overview_rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
        """Load blueprint SDE data for all blueprints in overview rows."""
        from eve_online_industry_tracker.infrastructure.sde.blueprints import get_blueprint_manufacturing_data
        bp_type_ids = {int(r.get("blueprint_type_id") or 0) for r in overview_rows if r.get("blueprint_type_id")}
        bp_type_ids.discard(0)
        if not bp_type_ids:
            return {}
        try:
            sde_session = self._session_provider.sde_session()
            try:
                return get_blueprint_manufacturing_data(sde_session, "en", list(bp_type_ids))
            finally:
                sde_session.close()
        except Exception:
            logger.exception("DailyPlannerService: failed to load blueprint data")
            return {}

    def _index_blueprint_assets(
        self, corp_assets: list[Any]
    ) -> tuple[dict[int, list[Any]], dict[int, list[Any]]]:
        """Separate corp assets into BPO and BPC indexes."""
        return index_blueprint_assets(corp_assets, self._meta_resolver)

    def _build_plan_items(
        self,
        plan_id: int,
        decisions: list[Any],
        market_depth_cache: dict[int, Any] | None = None,
    ) -> list[BuildPlanItemModel]:
        """Convert ItemDecision list to BuildPlanItemModel list.

        `snapshot_sell_price` is taken from the same market-depth entry that
        `_compute_snapshot_hash` reads (`spot_sell_price`), so the whole-plan
        hash and each item's per-item price describe the same observation.
        """
        market_depth_cache = market_depth_cache or {}
        items = []
        for d in decisions:
            items.append(BuildPlanItemModel(
                plan_id=plan_id,
                type_id=d.type_id,
                type_name=d.type_name,
                meta_group_id=d.meta_group_id,
                decision=d.decision,
                decision_reason=d.decision_reason,
                target_batches=None,
                priority_score=d.adjusted_score,
                isk_per_hour=d.isk_per_hour,
                margin_pct=d.margin_pct,
                days_of_supply_current=d.days_of_supply_current,
                pipeline_stage=d.pipeline_stage,
                bpo_investment_recommended=d.bpo_investment_recommended,
                bpo_market_price=d.bpo_market_price,
                break_even_days=d.break_even_days,
                projected_annual_savings=d.projected_annual_savings,
                effective_velocity=d.effective_velocity,
                snapshot_sell_price=_spot_sell_price(market_depth_cache.get(d.type_id)),
            ))
        return items

    def _persist_plan_items(
        self,
        plan_id: int,
        decisions: list[Any],
        market_depth_cache: dict[int, Any] | None = None,
    ) -> list[BuildPlanItemModel]:
        """Build BuildPlanItemModel rows from decisions and write them via the repo.

        Split out so the plan-item write (including the resolved
        meta_group_id -- see ItemDecisionEngine.decide()) is independently
        testable without running the whole compute pipeline.

        Returns the items after they have been inserted and their owning
        session closed -- callers must not read attributes off them (that
        raises DetachedInstanceError); use the return value only for its
        length/identity. `_phase_9_persist` computes freshness from a
        separate, not-yet-persisted build instead, for exactly this reason.
        """
        plan_items = self._build_plan_items(plan_id, decisions, market_depth_cache)
        if plan_items:
            self._repo.insert_plan_items(plan_items)
        return plan_items

    def _compute_freshness_score(
        self, plan: Any, plan_items: list[Any] | None = None
    ) -> tuple[float, int, int]:
        """Compare current spot prices to each item's snapshot price.

        Delegates the actual comparison to the pure `compute_freshness_stats`
        function; this method only wires up the repo-backed inputs (plan
        items and the current market depth) that function needs.

        Returns `(score, comparable, total)`. A 1.0 score by itself does not
        say whether nothing drifted or nothing was comparable (e.g. every
        item's `snapshot_sell_price` write silently broke) -- `comparable`
        is what tells those apart, so callers should surface it alongside
        the score rather than trusting 1.0 on its own.
        """
        snapshot_hash = getattr(plan, "market_snapshot_hash", None)
        if snapshot_hash is None:
            return 1.0, 0, len(plan_items) if plan_items is not None else 0

        try:
            if plan_items is None:
                plan_items = self._repo.get_plan_items(int(plan.id))
            if not plan_items:
                return 1.0, 0, 0

            hub = str(_adm(self._admin, "planner_market_hub", "jita"))
            type_ids = [int(i.type_id) for i in plan_items]
            market_depth = self._repo.get_market_depth(type_ids, hub)
            drift_threshold_pct = float(_adm(self._admin, "planner_price_drift_threshold_pct", 5.0))

            return compute_freshness_stats(plan_items, market_depth, drift_threshold_pct)
        except (KeyError, TypeError, ValueError, AttributeError):
            logger.exception("DailyPlannerService: failed to compute freshness score")
            return 1.0, 0, len(plan_items) if plan_items is not None else 0

    def _compute_snapshot_hash(
        self,
        overview_rows: list[dict[str, Any]],
        market_depth_cache: dict[int, Any],
    ) -> str:
        """Compute SHA-256 hash of spot prices for staleness detection."""
        # Collect (type_id, spot_sell_price) pairs for build items
        pairs: list[tuple[int, float]] = []
        for row in overview_rows:
            type_id = int(row.get("type_id") or 0)
            if type_id <= 0:
                continue
            entry = market_depth_cache.get(type_id)
            if entry is None:
                continue
            price = _get_attr(entry, "spot_sell_price")
            if price is None:
                continue
            pairs.append((type_id, float(price)))

        # Sort by type_id ascending
        pairs.sort(key=lambda x: x[0])
        serialized = ",".join(f"{tid}:{price:.2f}" for tid, price in pairs)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _model_to_dict(model: Any) -> dict[str, Any]:
    """Convert SQLAlchemy model to plain dict."""
    if model is None:
        return {}
    result = {}
    for col in model.__table__.columns:
        val = getattr(model, col.name, None)
        if isinstance(val, datetime):
            val = val.isoformat()
        result[col.name] = val
    return result


def _asset_attr(asset: Any, attr: str) -> Any:
    if isinstance(asset, dict):
        return asset.get(attr)
    return getattr(asset, attr, None)


def _asset_attr_bool(asset: Any, attr: str) -> bool:
    v = _asset_attr(asset, attr)
    return bool(v) if v is not None else False


def _get_attr(obj: Any, attr: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(attr)
    return getattr(obj, attr, None)


def index_blueprint_assets(
    corp_assets: list[Any], meta_resolver: Any
) -> tuple[dict[int, list[Any]], dict[int, list[Any]]]:
    """Split blueprint assets into (BPOs, BPCs), both keyed by blueprint type_id.

    Blueprint-ness comes from the SDE category — no asset column says it. A BPC
    is a blueprint with is_blueprint_copy=True; anything else blueprint-shaped is
    a BPO.
    """
    bpos: dict[int, list[Any]] = {}
    bpcs: dict[int, list[Any]] = {}

    # Pre-warm the resolver's cache with every distinct asset type_id in one batched
    # SDE query. Without this, TypeMetadataResolver._entry() self-heals a cache miss by
    # calling prefetch() for a single id, so is_blueprint() inside the per-asset loop
    # below would otherwise open one SDE session (with its metaGroups table reflection)
    # per distinct type_id -- 881 sessions for a live corp_assets table of 4263 rows in
    # this app's own database. prefetch() is idempotent (skips ids already cached or
    # already marked missing), so this is safe even if a caller already warmed it.
    type_ids = {int(_asset_attr(a, "type_id") or 0) for a in corp_assets}
    meta_resolver.prefetch({t for t in type_ids if t > 0})

    for asset in corp_assets:
        type_id = int(_asset_attr(asset, "type_id") or 0)
        if type_id <= 0 or not meta_resolver.is_blueprint(type_id):
            continue
        target = bpcs if _asset_attr_bool(asset, "is_blueprint_copy") else bpos
        target.setdefault(type_id, []).append(asset)
    return bpos, bpcs


#: Bound on repeated JSON decoding in _select_division_one_balance. Existing
#: `corporations.wallets`/`standings` rows are double-JSON-encoded (a
#: pre-fix bug in corporation.py's save path stored `json.dumps(...)` into a
#: column SQLAlchemy's JSON type already serializes), so a single decode can
#: still leave a str. A handful of iterations comfortably covers that and any
#: one-off re-encoding, without risking an infinite loop on adversarial input.
_MAX_WALLET_DECODE_ITERATIONS = 5


def _select_division_one_balance(wallets: Any) -> float:
    """Master wallet (division 1) balance from either ESI shape.

    `wallets` may arrive already decoded (list/dict), singly JSON-encoded
    (a str), or -- for existing double-encoded `corporations.wallets` rows --
    a JSON string of a JSON string. Decode repeatedly while the value is
    still a str, bounded by _MAX_WALLET_DECODE_ITERATIONS, so this is correct
    for both already-stored double-encoded rows and newly written
    single-encoded ones.
    """
    import json as _json

    for _ in range(_MAX_WALLET_DECODE_ITERATIONS):
        if not isinstance(wallets, str):
            break
        try:
            wallets = _json.loads(wallets)
        except ValueError:
            return 0.0

    if isinstance(wallets, list):
        for entry in wallets:
            if isinstance(entry, dict) and int(entry.get("division") or 0) == 1:
                return _parse_isk(entry.get("balance"))
    elif isinstance(wallets, dict):
        raw = wallets.get("1", wallets.get(1))
        if raw is not None:
            return _parse_isk(raw)
    return 0.0


def _parse_isk(raw: Any) -> float:
    try:
        return float(str(raw).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0
