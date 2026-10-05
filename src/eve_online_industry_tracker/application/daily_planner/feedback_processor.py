"""FeedbackProcessor — matches done manufacture actions to realized sales and updates EMA weights.

Runs at the start of Phase 1 (plan compute), before new scoring begins.
Each processed action is marked processed_for_feedback=True to prevent re-processing.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from eve_online_industry_tracker.infrastructure.models import (
    PlanItemOutcomeModel,
    PlanLearningWeightsModel,
)
from eve_online_industry_tracker.application.daily_planner.learning_weights import (  # noqa: F401 -- MIN/MAX re-exported for callers
    LEARNING_WEIGHT_MAX,
    LEARNING_WEIGHT_MIN,
    clamp_weight,
    read_weight,
)

logger = logging.getLogger(__name__)

def _positive_number(value: Any) -> float | None:
    """`value` as a float when it is a real, positive number; else None.

    Deliberately not float(value): that accepts strings and MagicMock (as
    1.0), and the point here is to not invent a cost.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if value > 0 else None


def _per_unit(total: Any, units: Any) -> float | None:
    """total / units, or None when either side is unknown or not positive."""
    total_f = _positive_number(total)
    units_f = _positive_number(units)
    if total_f is None or units_f is None:
        return None
    return total_f / units_f


#: Allocation sources whose unit_cost is a manufacturing job's build cost.
#: industry_build: the corp's own job (realized_profit re-snapshots it with
#: asset_provenance.resolve_industry_job_cost_snapshot; the persisted
#: unit_build_cost wins), also via corp asset history.
#: industry_build_transferred(_avg): a character job's unit_build_cost, FIFO
#: matched or quantity-weighted (realized_profit._load_character_source_lots,
#: scripts/backfill_corp_transfer_costs.py). Market buys, opening inventory
#: and untracked units are not build costs and are left out.
_INDUSTRY_BUILD_SOURCES = frozenset({
    "industry_build", "industry_build_transferred", "industry_build_transferred_avg",
})


def _industry_build_unit_cost(allocation_details: Any) -> tuple[float | None, str | None]:
    """(per-unit build cost of the sale's industry-built units, why unknown).

    Market-bought and untracked lots are excluded: their unit price is not a
    build cost, so mixing them in would compare unlike things. The value is
    None when there is nothing to compare, and the reason then says which
    case it was, for the skip log; the reason is None when the value is known.
    """
    if not isinstance(allocation_details, list):
        return None, "allocation_details missing or not a list"
    total = 0.0
    units = 0
    build_lots = 0
    sources: set[str] = set()
    for entry in allocation_details:
        if not isinstance(entry, dict):
            continue
        source = entry.get("source")
        sources.add(str(source))
        if source not in _INDUSTRY_BUILD_SOURCES:
            continue
        build_lots += 1
        quantity = _positive_number(entry.get("quantity"))
        cost = _positive_number(entry.get("total_cost"))
        if quantity is None or cost is None:
            continue
        total += cost
        units += int(quantity)
    if units > 0:
        return total / units, None
    if build_lots == 0:
        return None, f"no industry-built lots in this sale (sources: {sorted(sources)})"
    return None, "industry-built lots have no positive cost and quantity"


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
        repo: DailyPlannerRepository instance.
        admin_settings: AdminSettingsManager instance (reads planner_ema_alpha etc.).
    """

    def __init__(
        self,
        repo: Any,
        admin_settings: Any,
        session_provider: Any = None,
    ) -> None:
        self._repo = repo
        self._admin = admin_settings
        self._session_provider = session_provider

    def _adm(self, key: str, fallback: Any) -> Any:
        """A daily_planner setting, or `fallback` when there is no settings store.

        AttributeError covers stub/None admin objects; KeyError is what
        AdminSettingsManager.get raises for an unknown key (every key read
        here is pinned to the schema by tests/test_daily_planner_fail_loud.py).
        """
        try:
            return self._admin.get("daily_planner", key)
        except (AttributeError, KeyError):
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
            except (SQLAlchemyError, TypeError, ValueError):
                # Repo/ledger queries and the float()/int() work on stored
                # values. Anything else is a bug and must abort the compute.
                logger.exception(
                    "FeedbackProcessor: error processing action_id=%s type_id=%s",
                    action.id,
                    action.type_id,
                )
                did_process = True  # mark processed on error to avoid infinite retry loops

            if did_process:
                try:
                    self._repo.mark_action_feedback_processed(action.id)
                except SQLAlchemyError:
                    logger.exception(
                        "FeedbackProcessor: failed to mark action_id=%s as processed", action.id
                    )

        # Mark non-manufacture done actions as processed too (deliver, etc.)
        non_mfg = [a for a in done_actions if a.action_type != "manufacture"]
        for action in non_mfg:
            try:
                self._repo.mark_action_feedback_processed(action.id)
            except SQLAlchemyError:
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

        # Try to find matching realized sale via the session provider (temporal FIFO)
        realized = self._find_realized_sale(type_id, action=action)

        # Retrieve the plan_item for this action (needed for plan_item_id FK)
        plan_items = self._repo.get_plan_items(action.plan_id)
        matching_item = next((p for p in plan_items if int(p.type_id) == type_id), None)
        plan_item_id = int(matching_item.id) if matching_item is not None else 0

        predicted_isk_per_hour: float | None = None
        if matching_item is not None:
            predicted_isk_per_hour = matching_item.isk_per_hour
        # Predicted BUILD cost per unit: materials + job costs for the batch
        # (estimated_build_cost_isk, the producer's manufacturing_job.total_cost)
        # over the batch's units. A sale's industry-build allocated cost
        # includes the manufacturing install fee, so materials only against it
        # biased cost_multiplier below 1 by construction. The outcome columns
        # keep their *_material_cost names but hold this build cost per unit.
        #
        # The two sides are closer, not identical. Accepted residual biases:
        #
        # Realized = materials + the manufacturing install fee (job.cost)
        #   + for an invented T2 item, the invention cost: the matched actual
        #     invention job's materials + install fee when the job snapshot was
        #     persisted at refresh (corporation.py / character.py pass
        #     invention_unit_cost_per_run), otherwise the expected invention
        #     MATERIALS only (asset_provenance, the expected-invention estimate).
        #   Copy cost is always 0: nothing writes copy_cost; asset_provenance
        #   only reads it back.
        # Predicted = materials + the producer's total_job_cost, which adds the
        #   copy fee, invention fee, source-copy fee and (SDE fallback)
        #   research-chain fees (industry/service.py).
        #
        # 1. T2, towards < 1: the producer's total_cost appears to drop the
        #    top-level invention materials (its procurement list is replaced
        #    by the recursive plan's, industry/service.py), while the realized
        #    side carries them. Fixing it belongs in the producer.
        # 2. BPC-copied T1 and SDE-fallback rows, towards > 1: predicted
        #    includes copy/research job fees that the realized side never has.
        #    Probably small (job fees are a few % of materials).
        predicted_material_cost: float | None = _per_unit(
            getattr(action, "estimated_build_cost_isk", None), getattr(action, "quantity", None)
        )

        # Retrieve existing weights (or defaults)
        weights_map = self._repo.get_weights([type_id])
        existing = weights_map.get(type_id)

        old_accuracy_ema: float = read_weight(existing, "accuracy_ema")
        old_velocity: float = read_weight(existing, "velocity_multiplier")
        old_cost: float = read_weight(existing, "cost_multiplier")
        sample_count: int = int(getattr(existing, "sample_count", 0))

        if realized is not None:
            # Normal outcome — realized sale found
            actual_isk_per_hour: float | None = realized.get("isk_per_hour")
            # Actual BUILD cost per unit, from the sale's industry-built lots
            # only (see _industry_build_unit_cost).
            actual_material_cost, actual_unknown_reason = _industry_build_unit_cost(
                realized.get("allocation_details")
            )
            actual_sell_days: float = float(realized.get("sell_days", 1.0))

            # Guard denominators
            safe_actual_days = max(0.01, actual_sell_days)
            safe_predicted_isk_per_hr = max(0.01, float(predicted_isk_per_hour or 1.0))

            # EMA updates
            new_accuracy_ema = clamp_weight((1.0 - alpha) * old_accuracy_ema + alpha * (
                float(actual_isk_per_hour or 0.0) / safe_predicted_isk_per_hr
            ))
            # velocity: predicted_days / actual_days  (higher = sold faster than predicted → bonus)
            predicted_days = _estimate_predicted_sell_days(matching_item)
            new_velocity = clamp_weight((1.0 - alpha) * old_velocity + alpha * (
                float(predicted_days) / safe_actual_days
            ))
            # cost: predicted / actual build cost per unit (< 1 = building
            # cost more than predicted → penalty). Either side unknown or zero means
            # there is nothing to compare: keep the old weight. Substituting
            # 1 ISK (the old fallback) turned a 0 allocated cost into a
            # ratio in the millions.
            if predicted_material_cost is None or actual_material_cost is None:
                logger.info(
                    "FeedbackProcessor: cost update skipped for action_id=%s type_id=%s: "
                    "predicted per-unit cost %s, actual per-unit cost %s "
                    "(batch build cost=%r units=%r; actual: %s)",
                    getattr(action, "id", None), type_id,
                    "unknown" if predicted_material_cost is None else predicted_material_cost,
                    "unknown" if actual_material_cost is None else actual_material_cost,
                    getattr(action, "estimated_build_cost_isk", None), getattr(action, "quantity", None),
                    actual_unknown_reason or "known",
                )
                new_cost = old_cost
            else:
                new_cost = clamp_weight((1.0 - alpha) * old_cost + alpha * (
                    predicted_material_cost / actual_material_cost
                ))

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
            new_velocity = clamp_weight((1.0 - alpha) * old_velocity + alpha * (
                float(predicted_days) / slow_mover_timeout
            ))
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
        """Earliest realized sale of `type_id` at or after `action`'s timestamp.

        Without an action timestamp there is no way to attribute a sale to a
        planned action, so nothing is credited — crediting an unrelated sale
        would corrupt the EMA weights that drive all future scoring.

        Ordering picks the *earliest* qualifying sale, not the latest: for a
        type manufactured repeatedly (the normal case), the most-recent sale
        is shared by every earlier action's lookup, crediting one sale to an
        unbounded number of actions and inflating `sell_days` arbitrarily for
        the older ones — finding 14's own defect reappearing inside its fix.
        Earliest-after is bounded to the action's own window and cannot be
        shared that way.

        This still does not give true per-unit attribution: for a fungible
        commodity with no lot tracking, the earliest sale after this action
        may equally be pre-existing inventory sold off, or output from a
        different (earlier) manufacture run of the same type, rather than
        this action's own output. A precise fix would bound each action's
        attribution window by the next action for the same type_id, which is
        real design work and out of scope here.

        `CorporationRealizedSalesLedgerModel.date` is a VARCHAR holding a
        uniform, zero-padded ISO-8601 timestamp (e.g. `"2026-05-11T09:11:27Z"`),
        not a Date/DateTime column. The filter compares against the action's
        full timestamp (not just its calendar date) so that a same-day sale
        which happened *before* the action is correctly excluded — a
        date-only filter would let same-day-earlier sales through, which is
        exactly the pre-dating-sale defect this method exists to prevent, at
        intraday granularity. The day-count below still truncates to dates
        (kept as pre-existing behavior for the `sell_days` estimate).
        """
        generated_at = getattr(action, "generated_at", None)
        if generated_at is None:
            return None
        generated_date: date = (
            generated_at.date() if isinstance(generated_at, datetime) else generated_at
        )
        # Both `date` and `datetime` support strftime (a bare date fills the
        # time fields with zero), so this works whether generated_at carries
        # a time component or not.
        generated_ts = generated_at.strftime("%Y-%m-%dT%H:%M:%S")

        if self._session_provider is None:
            return None

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
                    # stored format is a uniform, zero-padded ISO-8601 string,
                    # and generated_ts is formatted to match its precision.
                    CorporationRealizedSalesLedgerModel.date >= generated_ts,
                )
                .order_by(CorporationRealizedSalesLedgerModel.date.asc())
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
        diff = (sale_date - generated_date).days
        sell_days = max(0.1, float(diff)) if diff > 0 else 1.0
        return {
            "isk_per_hour": realized_profit / max(0.01, sell_days * 24.0),
            # allocation_details carries each consumed lot's source, quantity
            # and cost; the caller keeps the industry-built lots only, so the
            # actual cost is a build cost like the predicted one.
            "allocation_details": row.allocation_details,
            "sell_days": sell_days,
        }


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
    except (TypeError, AttributeError):
        # generated_at is not a datetime (a bare date or a string): treat the
        # action as old, which is what the None case above already does.
        logger.warning(
            "FeedbackProcessor: action_id=%s has a non-datetime generated_at %r",
            getattr(action, "id", None), generated_at,
        )
        return 999.0
