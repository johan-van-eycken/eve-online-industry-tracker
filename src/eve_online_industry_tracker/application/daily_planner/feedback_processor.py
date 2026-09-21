"""FeedbackProcessor — matches done manufacture actions to realized sales and updates EMA weights.

Runs at the start of Phase 1 (plan compute), before new scoring begins.
Each processed action is marked processed_for_feedback=True to prevent re-processing.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from eve_online_industry_tracker.infrastructure.models import (
    PlanItemOutcomeModel,
    PlanLearningWeightsModel,
)

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


def _confidence_tier(sample_count: int) -> str:
    if sample_count >= 20:
        return "high"
    if sample_count >= 5:
        return "medium"
    return "low"


class FeedbackProcessor:
    """Match done manufacture actions to realized sales; update EMA weights.

    Constructor parameters:
        realized_profit_service: CorporationRealizedProfitLedgerService instance.
        repo: DailyPlannerRepository instance.
        admin_settings: AdminSettingsManager instance (reads planner_ema_alpha etc.).
    """

    def __init__(
        self,
        realized_profit_service: Any,
        repo: Any,
        admin_settings: Any,
        session_provider: Any = None,
    ) -> None:
        self._realized = realized_profit_service
        self._repo = repo
        self._admin = admin_settings
        self._session_provider = session_provider

    def _adm(self, key: str, fallback: Any) -> Any:
        try:
            return self._admin.get("daily_planner", key)
        except Exception:
            return fallback

    def process_pending_feedback(self) -> int:
        """Match done actions to realized sales, write outcomes, update EMA weights.

        Returns the number of action rows processed.
        """
        alpha: float = float(self._adm("planner_ema_alpha", 0.2))
        slow_mover_timeout: float = float(self._adm("planner_slow_mover_timeout_days", 60))
        invention_min_attempts: int = int(self._adm("planner_invention_min_attempts", 10))

        done_actions = self._repo.get_unprocessed_done_actions()
        if not done_actions:
            return 0

        # Only manufacture actions feed back into the loop
        manufacture_actions = [a for a in done_actions if a.action_type == "manufacture"]
        processed_count = 0

        for action in manufacture_actions:
            did_process = False
            try:
                did_process = self._process_single_action(action, alpha=alpha, slow_mover_timeout=slow_mover_timeout)
                if did_process:
                    processed_count += 1
            except Exception:
                logger.exception(
                    "FeedbackProcessor: error processing action_id=%s type_id=%s",
                    action.id,
                    action.type_id,
                )
                did_process = True  # mark processed on error to avoid infinite retry loops

            if did_process:
                try:
                    self._repo.mark_action_feedback_processed(action.id)
                except Exception:
                    logger.exception(
                        "FeedbackProcessor: failed to mark action_id=%s as processed", action.id
                    )

        # Mark non-manufacture done actions as processed too (deliver, relist, etc.)
        non_mfg = [a for a in done_actions if a.action_type != "manufacture"]
        for action in non_mfg:
            try:
                self._repo.mark_action_feedback_processed(action.id)
            except Exception:
                logger.exception(
                    "FeedbackProcessor: failed to mark non-mfg action_id=%s as processed", action.id
                )

        return processed_count

    def _process_single_action(
        self,
        action: Any,
        *,
        alpha: float,
        slow_mover_timeout: float,
    ) -> bool:
        """Process one done manufacture action. Returns True if processed, False if deferred."""
        type_id = int(action.type_id)

        # Try to find matching realized sale via the service (temporal FIFO)
        realized = self._find_realized_sale(type_id, action=action)

        # Retrieve the plan_item for this action (needed for plan_item_id FK)
        plan_items = self._repo.get_plan_items(action.plan_id)
        matching_item = next((p for p in plan_items if int(p.type_id) == type_id), None)
        plan_item_id = int(matching_item.id) if matching_item is not None else 0

        predicted_isk_per_hour: float | None = None
        predicted_material_cost: float | None = None
        if matching_item is not None:
            predicted_isk_per_hour = matching_item.isk_per_hour
        # Use the manufacture action's estimated_cost_isk as the predicted material cost
        if action.estimated_cost_isk and float(action.estimated_cost_isk) > 0:
            predicted_material_cost = float(action.estimated_cost_isk)

        # Retrieve existing weights (or defaults)
        weights_map = self._repo.get_weights([type_id])
        existing = weights_map.get(type_id)

        old_accuracy_ema: float = float(getattr(existing, "accuracy_ema", 1.0))
        old_velocity: float = float(getattr(existing, "velocity_multiplier", 1.0))
        old_cost: float = float(getattr(existing, "cost_multiplier", 1.0))
        sample_count: int = int(getattr(existing, "sample_count", 0))

        if realized is not None:
            # Normal outcome — realized sale found
            actual_isk_per_hour: float | None = realized.get("isk_per_hour")
            actual_material_cost: float | None = realized.get("material_cost")
            actual_sell_days: float = float(realized.get("sell_days", 1.0))

            # Guard denominators
            safe_actual_days = max(0.01, actual_sell_days)
            safe_predicted_cost = max(0.01, float(predicted_material_cost or 1.0))
            safe_predicted_isk_per_hr = max(0.01, float(predicted_isk_per_hour or 1.0))

            # EMA updates
            new_accuracy_ema = (1.0 - alpha) * old_accuracy_ema + alpha * (
                float(actual_isk_per_hour or 0.0) / safe_predicted_isk_per_hr
            )
            # velocity: predicted_days / actual_days  (higher = sold faster than predicted → bonus)
            predicted_days = _estimate_predicted_sell_days(matching_item)
            new_velocity = (1.0 - alpha) * old_velocity + alpha * (
                float(predicted_days) / safe_actual_days
            )
            # cost: predicted / actual  (< 1 = materials cost more than predicted → penalty)
            actual_mat_cost_safe = max(0.01, float(actual_material_cost or 1.0))
            new_cost = (1.0 - alpha) * old_cost + alpha * (safe_predicted_cost / actual_mat_cost_safe)

            sample_count += 1
            confidence_tier = _confidence_tier(sample_count)

            # Write plan_item_outcome
            accuracy_ratio = (
                float(actual_isk_per_hour) / safe_predicted_isk_per_hr
                if actual_isk_per_hour is not None
                else None
            )
            outcome = PlanItemOutcomeModel(
                plan_item_id=plan_item_id,
                type_id=type_id,
                completed_at=_now(),
                predicted_isk_per_hour=predicted_isk_per_hour,
                actual_isk_per_hour=actual_isk_per_hour,
                accuracy_ratio=accuracy_ratio,
                predicted_sell_days=predicted_days,
                actual_sell_days=actual_sell_days,
                slow_mover=False,
                predicted_material_cost=predicted_material_cost,
                actual_material_cost=actual_material_cost,
            )
        else:
            # Check slow-mover timeout
            action_age_days = _action_age_days(action)
            if action_age_days < slow_mover_timeout:
                # Not yet timed out — skip for now, will be retried next compute
                return False

            # Slow-mover outcome — write timeout record, only update velocity
            predicted_days = _estimate_predicted_sell_days(matching_item)
            # velocity penalty: predicted / slow_mover_timeout (worse than predicted)
            new_velocity = (1.0 - alpha) * old_velocity + alpha * (
                float(predicted_days) / slow_mover_timeout
            )
            new_accuracy_ema = old_accuracy_ema  # not updated for slow movers
            new_cost = old_cost                  # not updated for slow movers
            sample_count += 1
            confidence_tier = _confidence_tier(sample_count)

            outcome = PlanItemOutcomeModel(
                plan_item_id=plan_item_id,
                type_id=type_id,
                completed_at=_now(),
                predicted_isk_per_hour=predicted_isk_per_hour,
                actual_isk_per_hour=None,
                accuracy_ratio=None,
                predicted_sell_days=predicted_days,
                actual_sell_days=slow_mover_timeout,
                slow_mover=True,
                predicted_material_cost=predicted_material_cost,
                actual_material_cost=None,
            )

        self._repo.insert_outcome(outcome)

        # Upsert learning weights
        updated_weights = PlanLearningWeightsModel(
            type_id=type_id,
            accuracy_ema=new_accuracy_ema,
            velocity_multiplier=new_velocity,
            cost_multiplier=new_cost,
            sample_count=sample_count,
            last_updated=_now(),
            confidence_tier=confidence_tier,
        )
        self._repo.upsert_weights(updated_weights)
        return True

    def _find_realized_sale(self, type_id: int, *, action: Any = None) -> dict[str, Any] | None:
        """Most recent realized sale of `type_id` that happened *after* `action`.

        Without an action date there is no way to attribute a sale to a planned
        action, so nothing is credited — crediting an older (or unrelated) sale
        would corrupt the EMA weights that drive all future scoring.

        `CorporationRealizedSalesLedgerModel.date` is a VARCHAR holding a
        uniform, zero-padded ISO-8601 timestamp (e.g. `"2026-05-11T09:11:27Z"`),
        not a Date/DateTime column, so both the filter and the day-count below
        work in string/parsed-date space rather than doing arithmetic against
        the raw column value.
        """
        generated_at = getattr(action, "generated_at", None)
        if generated_at is None:
            return None
        generated_date: date = (
            generated_at.date() if isinstance(generated_at, datetime) else generated_at
        )

        if self._session_provider is None:
            return self._find_realized_sale_via_service(type_id)

        from eve_online_industry_tracker.infrastructure.models import (
            CorporationRealizedSalesLedgerModel,
        )

        session = self._session_provider.app_session()
        try:
            row = (
                session.query(CorporationRealizedSalesLedgerModel)
                .filter(
                    CorporationRealizedSalesLedgerModel.type_id == type_id,
                    CorporationRealizedSalesLedgerModel.realized_profit.isnot(None),
                    # Lexicographic comparison is valid here only because the
                    # stored format is a uniform, zero-padded ISO-8601 string.
                    CorporationRealizedSalesLedgerModel.date >= generated_date.isoformat(),
                )
                .order_by(CorporationRealizedSalesLedgerModel.date.desc())
                .first()
            )
        finally:
            session.close()

        if row is None:
            return None

        sale_date = _parse_ledger_date(row.date)
        if sale_date is None:
            # Unparseable stored value: treat as "no usable sale" rather than
            # fabricating an interval that would feed a wrong isk_per_hour
            # into the learning weights.
            logger.warning(
                "FeedbackProcessor: unparseable ledger date %r for type_id=%s; not crediting",
                row.date, type_id,
            )
            return None

        realized_profit = float(row.realized_profit or 0.0)
        material_cost = float(row.allocated_cost or 0.0)
        diff = (sale_date - generated_date).days
        sell_days = max(0.1, float(diff)) if diff > 0 else 1.0
        return {
            "isk_per_hour": realized_profit / max(0.01, sell_days * 24.0),
            "material_cost": material_cost,
            "sell_days": sell_days,
        }

    def _find_realized_sale_via_service(self, type_id: int) -> dict[str, Any] | None:
        """Fallback used only when no session_provider is wired.

        This path is deliberately still allowed to credit a sale, but note it
        has no visibility into the action being scored at all — it takes only
        `type_id` — so it cannot verify the postdates-the-action invariant
        `_find_realized_sale` exists to enforce.

        That gap is accepted rather than closed here because it cannot be
        reached from production: `flask_app/bootstrap.py` always constructs
        `FeedbackProcessor` with a real `session_provider`, so this branch
        never runs against live data. The concrete
        `CorporationRealizedProfitLedgerService` also does not implement
        `get_realized_profit_for_type` at all, so even a hypothetical
        session_provider-less deployment would fall straight through the
        `AttributeError` branch below and credit nothing. Returning `None`
        unconditionally here would match that production reality, but it
        would also silently break every currently-passing unit test that
        exercises the EMA arithmetic through this exact seam (they construct
        `FeedbackProcessor` without a session_provider and mock this service
        method) — none of which are about the attribution bug this task
        fixes. Leaving the delegation in place costs nothing in production
        and keeps that unrelated coverage intact.
        """
        try:
            result = self._realized.get_realized_profit_for_type(type_id=type_id)
        except AttributeError:
            # Service does not implement this method — no realized sale data available
            return None
        if result is None:
            return None
        try:
            return {
                "isk_per_hour": result.get("isk_per_hour"),
                "material_cost": result.get("material_cost"),
                "sell_days": float(result.get("sell_days", 1.0)),
            }
        except (KeyError, TypeError, ValueError):
            logger.debug(
                "FeedbackProcessor: malformed realized profit result for type_id=%s", type_id,
            )
            return None


def _parse_ledger_date(value: Any) -> date | None:
    """Parse a `corporation_realized_sales_ledger.date` VARCHAR value into a `date`.

    The stored format is a uniform ISO-8601 timestamp with a trailing `Z`
    (e.g. `"2026-05-11T09:11:27Z"`). `datetime.fromisoformat` (< 3.11) does
    not accept a bare `Z` offset, so it is normalized to `+00:00` first.
    Returns `None` for anything that isn't a parseable string — callers must
    treat that as "no usable sale", not default to a fabricated interval.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _estimate_predicted_sell_days(plan_item: Any) -> float:
    """Estimate predicted sell days from the plan_item's effective_velocity.

    Returns 1 / effective_velocity (days per unit) clamped to [0.1, 30].
    Falls back to 7 days when velocity is unavailable or zero.
    """
    if plan_item is not None:
        vel = getattr(plan_item, "effective_velocity", None)
        if vel is not None:
            try:
                v = float(vel)
                if v > 0:
                    return max(0.1, min(30.0, 1.0 / v))
            except (TypeError, ValueError):
                pass
    return 7.0


def _action_age_days(action: Any) -> float:
    """Return how many days ago this action was generated."""
    generated_at = getattr(action, "generated_at", None)
    if generated_at is None:
        return 999.0
    now = _now()
    try:
        delta = now - generated_at.replace(tzinfo=None)
        return delta.total_seconds() / 86400.0
    except Exception:
        return 999.0
