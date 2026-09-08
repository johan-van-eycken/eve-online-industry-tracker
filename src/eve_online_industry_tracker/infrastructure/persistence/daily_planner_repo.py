from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from eve_online_industry_tracker.infrastructure.models import (
    BuildPlanModel,
    BuildPlanItemModel,
    DailyActionLogModel,
    InventionOutcomeLogModel,
    MarginCorrelationCacheModel,
    MarketDepthCacheModel,
    PlanItemOutcomeModel,
    PlanLearningWeightsModel,
)
from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider


class DailyPlannerRepository:
    """CRUD layer for all Daily Planner tables."""

    def __init__(self, session_provider: SessionProvider) -> None:
        self._session_provider = session_provider

    # ------------------------------------------------------------------
    # build_plan
    # ------------------------------------------------------------------

    def get_active_plan(self) -> BuildPlanModel | None:
        session = self._session_provider.app_session()
        try:
            return (
                session.query(BuildPlanModel)
                .filter(BuildPlanModel.status == "active")
                .order_by(BuildPlanModel.created_at.desc())
                .first()
            )
        finally:
            session.close()

    def archive_active_plan(self) -> None:
        session = self._session_provider.app_session()
        try:
            session.query(BuildPlanModel).filter(
                BuildPlanModel.status == "active"
            ).update(
                {"status": "archived", "updated_at": _now()},
                synchronize_session="fetch",
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def insert_plan(self, plan: BuildPlanModel) -> int:
        session = self._session_provider.app_session()
        try:
            session.add(plan)
            session.flush()
            plan_id = int(plan.id)
            session.commit()
            return plan_id
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def update_freshness_score(self, plan_id: int, score: float) -> None:
        session = self._session_provider.app_session()
        try:
            session.query(BuildPlanModel).filter(
                BuildPlanModel.id == plan_id
            ).update(
                {"freshness_score": score, "updated_at": _now()},
                synchronize_session="fetch",
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # build_plan_item
    # ------------------------------------------------------------------

    def insert_plan_items(self, items: list[BuildPlanItemModel]) -> None:
        if not items:
            return
        session = self._session_provider.app_session()
        try:
            session.add_all(items)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_plan_items(self, plan_id: int) -> list[BuildPlanItemModel]:
        session = self._session_provider.app_session()
        try:
            return (
                session.query(BuildPlanItemModel)
                .filter(BuildPlanItemModel.plan_id == plan_id)
                .all()
            )
        finally:
            session.close()

    # ------------------------------------------------------------------
    # daily_action_log
    # ------------------------------------------------------------------

    def insert_actions(self, actions: list[DailyActionLogModel]) -> None:
        if not actions:
            return
        session = self._session_provider.app_session()
        try:
            session.add_all(actions)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_actions(self, plan_id: int) -> list[DailyActionLogModel]:
        session = self._session_provider.app_session()
        try:
            return (
                session.query(DailyActionLogModel)
                .filter(DailyActionLogModel.plan_id == plan_id)
                .all()
            )
        finally:
            session.close()

    def mark_action_done(self, action_id: int) -> None:
        self.set_action_status(action_id, "done")

    def set_action_status(self, action_id: int, status: str) -> None:
        session = self._session_provider.app_session()
        try:
            session.query(DailyActionLogModel).filter(
                DailyActionLogModel.id == action_id
            ).update({"status": status}, synchronize_session="fetch")
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_unprocessed_done_actions(self) -> list[DailyActionLogModel]:
        session = self._session_provider.app_session()
        try:
            return (
                session.query(DailyActionLogModel)
                .filter(
                    DailyActionLogModel.status == "done",
                    DailyActionLogModel.processed_for_feedback == False,  # noqa: E712
                )
                .all()
            )
        finally:
            session.close()

    def mark_action_feedback_processed(self, action_id: int) -> None:
        session = self._session_provider.app_session()
        try:
            session.query(DailyActionLogModel).filter(
                DailyActionLogModel.id == action_id
            ).update(
                {"processed_for_feedback": True},
                synchronize_session="fetch",
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # plan_item_outcome
    # ------------------------------------------------------------------

    def insert_outcome(self, outcome: PlanItemOutcomeModel) -> None:
        session = self._session_provider.app_session()
        try:
            session.add(outcome)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_outcomes(self, type_id: int, limit: int = 20) -> list[PlanItemOutcomeModel]:
        session = self._session_provider.app_session()
        try:
            return (
                session.query(PlanItemOutcomeModel)
                .filter(PlanItemOutcomeModel.type_id == type_id)
                .order_by(PlanItemOutcomeModel.completed_at.desc())
                .limit(limit)
                .all()
            )
        finally:
            session.close()

    # ------------------------------------------------------------------
    # plan_learning_weights
    # ------------------------------------------------------------------

    def get_weights(self, type_ids: list[int]) -> dict[int, PlanLearningWeightsModel]:
        if not type_ids:
            return {}
        session = self._session_provider.app_session()
        try:
            rows = (
                session.query(PlanLearningWeightsModel)
                .filter(PlanLearningWeightsModel.type_id.in_(type_ids))
                .all()
            )
            return {int(r.type_id): r for r in rows}
        finally:
            session.close()

    def upsert_weights(self, weights: PlanLearningWeightsModel) -> None:
        session = self._session_provider.app_session()
        try:
            stmt = (
                sqlite_insert(PlanLearningWeightsModel)
                .values(
                    type_id=weights.type_id,
                    accuracy_ema=weights.accuracy_ema,
                    velocity_multiplier=weights.velocity_multiplier,
                    cost_multiplier=weights.cost_multiplier,
                    sample_count=weights.sample_count,
                    last_updated=weights.last_updated,
                    confidence_tier=weights.confidence_tier,
                )
                .on_conflict_do_update(
                    index_elements=["type_id"],
                    set_={
                        "accuracy_ema": weights.accuracy_ema,
                        "velocity_multiplier": weights.velocity_multiplier,
                        "cost_multiplier": weights.cost_multiplier,
                        "sample_count": weights.sample_count,
                        "last_updated": weights.last_updated,
                        "confidence_tier": weights.confidence_tier,
                    },
                )
            )
            session.execute(stmt)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # market_depth_cache — INSERT OR REPLACE on UNIQUE(type_id, hub)
    # ------------------------------------------------------------------

    def upsert_market_depth(self, rows: list[MarketDepthCacheModel]) -> None:
        if not rows:
            return
        session = self._session_provider.app_session()
        try:
            for row in rows:
                session.execute(
                    text(
                        "INSERT OR REPLACE INTO market_depth_cache "
                        "(type_id, hub, competitor_units, vwap_5d, spot_sell_price, "
                        "competition_index, snapshot_at) "
                        "VALUES (:type_id, :hub, :competitor_units, :vwap_5d, "
                        ":spot_sell_price, :competition_index, :snapshot_at)"
                    ),
                    {
                        "type_id": row.type_id,
                        "hub": row.hub,
                        "competitor_units": row.competitor_units,
                        "vwap_5d": row.vwap_5d,
                        "spot_sell_price": row.spot_sell_price,
                        "competition_index": row.competition_index,
                        "snapshot_at": row.snapshot_at,
                    },
                )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_market_depth(
        self, type_ids: list[int], hub: str
    ) -> dict[int, MarketDepthCacheModel]:
        if not type_ids:
            return {}
        session = self._session_provider.app_session()
        try:
            rows = (
                session.query(MarketDepthCacheModel)
                .filter(
                    MarketDepthCacheModel.type_id.in_(type_ids),
                    MarketDepthCacheModel.hub == hub,
                )
                .all()
            )
            return {int(r.type_id): r for r in rows}
        finally:
            session.close()

    # ------------------------------------------------------------------
    # invention_outcome_log — INSERT OR IGNORE on (type_id, character_id, completed_at)
    # ------------------------------------------------------------------

    def log_invention_outcome(self, outcome: InventionOutcomeLogModel) -> None:
        session = self._session_provider.app_session()
        try:
            session.execute(
                text(
                    "INSERT OR IGNORE INTO invention_outcome_log "
                    "(type_id, blueprint_type_id, decryptor_type_id, theoretical_success_pct, "
                    "was_success, character_id, completed_at) "
                    "VALUES (:type_id, :blueprint_type_id, :decryptor_type_id, "
                    ":theoretical_success_pct, :was_success, :character_id, :completed_at)"
                ),
                {
                    "type_id": outcome.type_id,
                    "blueprint_type_id": outcome.blueprint_type_id,
                    "decryptor_type_id": outcome.decryptor_type_id,
                    "theoretical_success_pct": outcome.theoretical_success_pct,
                    "was_success": int(outcome.was_success),
                    "character_id": outcome.character_id,
                    "completed_at": outcome.completed_at,
                },
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_invention_success_rates(
        self, type_ids: list[int], min_attempts: int = 10
    ) -> dict[int, float]:
        """Return {type_id: actual_success_rate} for items with >= min_attempts attempts."""
        if not type_ids:
            return {}
        session = self._session_provider.app_session()
        try:
            placeholders = ",".join(str(int(t)) for t in type_ids)
            rows = session.execute(
                text(
                    f"SELECT type_id, "
                    f"CAST(SUM(CASE WHEN was_success THEN 1 ELSE 0 END) AS REAL) / COUNT(*) AS actual_success_rate, "
                    f"COUNT(*) AS total_attempts "
                    f"FROM invention_outcome_log "
                    f"WHERE type_id IN ({placeholders}) "
                    f"GROUP BY type_id "
                    f"HAVING COUNT(*) >= :min_attempts"
                ),
                {"min_attempts": min_attempts},
            ).fetchall()
            return {int(row[0]): float(row[1]) for row in rows}
        finally:
            session.close()

    # ------------------------------------------------------------------
    # cleanup
    # ------------------------------------------------------------------

    def purge_old_action_log_rows(self, cutoff_days: int) -> int:
        """Delete processed feedback rows from plans older than cutoff_days.

        Uses created_at on build_plan (not computed_at).
        Only deletes rows where processed_for_feedback=1.
        Returns number of rows deleted.
        """
        session = self._session_provider.app_session()
        try:
            result = session.execute(
                text(
                    "DELETE FROM daily_action_log "
                    "WHERE processed_for_feedback = 1 "
                    "  AND plan_id IN ("
                    "      SELECT id FROM build_plan "
                    "      WHERE created_at < datetime('now', :cutoff)"
                    "  )"
                ),
                {"cutoff": f"-{int(cutoff_days)} days"},
            )
            session.commit()
            return int(result.rowcount)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def _now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)
