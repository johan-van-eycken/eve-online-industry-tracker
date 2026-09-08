"""DailyPlannerService — orchestrator for all nine computation phases.

Background thread pattern mirrors IndustryService (threading.Thread + daemon=True).
Does NOT use AppState; uses a simple Lock + instance attributes for status tracking.
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.application.daily_planner.action_plan_builder import ActionPlanBuilder
from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.feedback_processor import FeedbackProcessor
from eve_online_industry_tracker.application.daily_planner.item_decision_engine import ItemDecisionEngine
from eve_online_industry_tracker.application.daily_planner.pipeline_analyzer import PipelineAnalyzer
from eve_online_industry_tracker.application.daily_planner.profitability_scorer import ProfitabilityScorer
from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder
from eve_online_industry_tracker.infrastructure.models import (
    BuildPlanItemModel,
    BuildPlanModel,
)

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


def _adm(admin_settings: Any, key: str, fallback: Any) -> Any:
    try:
        return admin_settings.get("daily_planner", key)
    except Exception:
        return fallback


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
        self._industry = industry_service
        self._corporations = corporations_service
        self._characters = characters_service
        self._sales_history = sales_history_service
        self._pricing_suggestions = pricing_suggestion_service
        self._market_pricing = market_pricing_service
        self._realized_profit = realized_profit_service
        self._repo = repo
        self._admin = admin_settings
        self._session_provider = session_provider

        # Thread-safe status tracking (no AppState dependency)
        self._lock = threading.Lock()
        self._status: str = "idle"   # "idle" | "running" | "done" | "failed"
        self._error: str | None = None

        # Sub-components
        self._feedback_processor = FeedbackProcessor(
            realized_profit_service=realized_profit_service,
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
        """Fetch the active plan; recompute freshness_score and write back."""
        plan = self._repo.get_active_plan()
        if plan is None:
            return {"plan": None, "items": [], "actions": []}

        # Recompute freshness score
        freshness = self._compute_freshness_score(plan)
        self._repo.update_freshness_score(int(plan.id), freshness)

        items = self._repo.get_plan_items(int(plan.id))
        actions = self._repo.get_actions(int(plan.id))

        return {
            "plan": _model_to_dict(plan),
            "items": [_model_to_dict(i) for i in items],
            "actions": [_model_to_dict(a) for a in actions],
            "freshness_score": freshness,
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

        # Plan history (last 90 days)
        try:
            from datetime import timedelta  # noqa: PLC0415
            cutoff = (datetime.utcnow() - timedelta(days=90)).isoformat()
            ph_session = self._session_provider.app_session()
            try:
                plan_history_rows = ph_session.query(BuildPlanModel).filter(
                    BuildPlanModel.created_at >= cutoff
                ).order_by(BuildPlanModel.created_at.desc()).limit(90).all()
            finally:
                ph_session.close()
            plan_history = [
                {
                    "id": int(p.id),
                    "created_at": str(p.created_at),
                    "status": str(p.status),
                    "freshness_score": float(p.freshness_score or 1.0),
                }
                for p in plan_history_rows
            ]
        except Exception:
            plan_history = []

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

    def _phase_2_pipeline(self, phase1: dict[str, Any]) -> list[Any]:
        logger.info("DailyPlannerService: Phase 2 — pipeline analysis")
        return self._pipeline_analyzer.analyze(
            overview_rows=phase1["overview_rows"],
            industry_jobs=phase1["industry_jobs"],
            corp_assets=phase1["corp_assets"],
            market_depth_cache=phase1["market_depth_cache"],
            weights=phase1["weights"],
            sell_velocities=phase1["sell_velocities"],
        )

    def _phase_3_score(
        self,
        pipeline_states: list[Any],
        phase1: dict[str, Any],
    ) -> list[Any]:
        logger.info("DailyPlannerService: Phase 3 — profitability scoring")
        overview_by_type = {int(r["type_id"]): r for r in phase1["overview_rows"] if r.get("type_id")}
        trit_trend_7d: float | None = phase1.get("trit_trend_7d")
        scored = []
        for ps in pipeline_states:
            row = overview_by_type.get(ps.type_id, {})
            weights = phase1["weights"].get(ps.type_id)
            market_depth = phase1["market_depth_cache"].get(ps.type_id)
            margin_corr = phase1["margin_correlations"].get(ps.type_id)
            try:
                scored_item = self._profitability_scorer.score(
                    pipeline=ps,
                    overview_row=row,
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
        decisions = []
        for scored in scored_items:
            ps = pipeline_by_type.get(scored.type_id)
            if ps is None:
                continue
            row = overview_by_type.get(scored.type_id, {})
            try:
                decision = self._decision_engine.decide(
                    scored=scored,
                    pipeline=ps,
                    overview_row=row,
                    admin_settings=self._admin,
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
        # plan_id is set in Phase 9 (not known yet); use 0 as placeholder
        return self._action_plan_builder.build(
            plan_id=0,
            assigned_actions=assigned_actions,
            shopping_items=shopping_items,
            pricing_suggestions=phase1["pricing_suggestions"],
            industry_jobs=phase1["industry_jobs"],
            admin_settings=self._admin,
            bpo_opportunities=bpo_opportunities,
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

        # Build plan items
        plan_items = self._build_plan_items(plan_id, chain_plan.decisions)
        if plan_items:
            self._repo.insert_plan_items(plan_items)

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
        """Read corp master wallet balance (Division 1)."""
        try:
            corps = self._corporations.get_corporations()
            if not corps:
                return 0.0
            corp = corps[0] if isinstance(corps, list) else corps
            wallets_raw = getattr(corp, "wallets", None) or {}
            if isinstance(wallets_raw, str):
                import json as _json
                wallets = _json.loads(wallets_raw)
            else:
                wallets = wallets_raw

            if isinstance(wallets, list):
                # List of wallet entries — find division 1
                for w in wallets:
                    if isinstance(w, dict) and int(w.get("division", 0)) == 1:
                        return float(str(w.get("balance", "0")).replace(",", ""))
            elif isinstance(wallets, dict):
                balance = wallets.get("1") or wallets.get(1)
                if balance is not None:
                    return float(str(balance).replace(",", ""))
            return 0.0
        except Exception:
            logger.exception("DailyPlannerService: failed to read corp wallet")
            return 0.0

    def _get_overview_rows(self) -> list[dict[str, Any]]:
        """Get IndustryService overview rows."""
        try:
            result = self._industry.get_cached_overview_rows()
            if isinstance(result, list):
                return result
            return []
        except AttributeError:
            pass
        try:
            store = self._industry._get_industry_overview_refresh_store()
            jobs = store.jobs if hasattr(store, "jobs") else {}
            if not jobs:
                return []
            latest = max(jobs.values(), key=lambda j: j.get("updated_at", ""), default=None)
            if latest and latest.get("result"):
                return list(latest["result"])
            return []
        except Exception:
            logger.exception("DailyPlannerService: failed to get overview rows")
            return []

    def _get_industry_jobs(self) -> list[Any]:
        """Get active corp industry jobs."""
        try:
            session = self._session_provider.app_session()
            try:
                from eve_online_industry_tracker.infrastructure.models import CorporationIndustryJobsModel
                return session.query(CorporationIndustryJobsModel).all()
            finally:
                session.close()
        except Exception:
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
            corps = self._corporations.get_corporations()
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
        from datetime import timedelta
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
    ) -> tuple[dict[int, list], dict[int, list]]:
        """Separate corp assets into BPO and BPC indexes."""
        bpo_by_type: dict[int, list] = {}
        bpc_by_type: dict[int, list] = {}

        for asset in corp_assets:
            if not _asset_attr_bool(asset, "is_blueprint"):
                continue
            type_id = int(_asset_attr(asset, "type_id") or 0)
            if type_id <= 0:
                continue
            runs = _asset_attr(asset, "runs")
            try:
                runs_int = int(runs or 0)
            except (TypeError, ValueError):
                runs_int = 0

            if runs_int < 0 or runs_int == 0:
                # BPO: runs = -1 (unlimited) or 0 in some representations
                bpo_by_type.setdefault(type_id, []).append(asset)
            else:
                bpc_by_type.setdefault(type_id, []).append(asset)

        return bpo_by_type, bpc_by_type

    def _build_plan_items(
        self, plan_id: int, decisions: list[Any]
    ) -> list[BuildPlanItemModel]:
        """Convert ItemDecision list to BuildPlanItemModel list."""
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
            ))
        return items

    def _compute_freshness_score(self, plan: Any) -> float:
        """Compare current spot prices to snapshot; return freshness (0.0–1.0)."""
        snapshot_hash = getattr(plan, "market_snapshot_hash", None)
        if snapshot_hash is None:
            return 1.0

        # Re-read current prices from market_depth_cache
        try:
            plan_items = self._repo.get_plan_items(int(plan.id))
            hub = str(_adm(self._admin, "planner_market_hub", "jita"))
            type_ids = [int(i.type_id) for i in plan_items]
            if not type_ids:
                return 1.0

            market_depth = self._repo.get_market_depth(type_ids, hub)
            drift_threshold = float(_adm(self._admin, "planner_price_drift_threshold_pct", 5.0)) / 100.0

            drifted = 0
            for item in plan_items:
                tid = int(item.type_id)
                entry = market_depth.get(tid)
                if entry is None:
                    continue
                current_price = _get_attr(entry, "spot_sell_price")
                if current_price is None:
                    continue
                # Extract snapshot price from hash (simplified: use current to reconstruct)
                # True staleness check would parse the hash; approximate here
                # For now just check if we have prices (freshness always 1.0 in Phase A)
                pass

            total = len(type_ids)
            if total == 0:
                return 1.0
            return 1.0 - (drifted / total)
        except Exception:
            return 1.0

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
