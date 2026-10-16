"""Tests for FeedbackProcessor — EMA update logic."""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.daily_planner.feedback_processor import FeedbackProcessor
from eve_online_industry_tracker.infrastructure.models import (
    PlanLearningWeightsModel,
    PlanItemOutcomeModel,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class _StaticSessionProvider:
    """Session provider that always hands back one already-open session.

    conftest.py only defines `_EngineSessionProvider`, which wraps an engine
    -- not an existing `Session` -- so it can't be handed the `app_session`
    fixture directly. This wraps that fixture's session as-is: data seeded
    on it before construction is visible to the query `_find_realized_sale`
    runs through this provider, on the same connection/transaction.
    """

    def __init__(self, session) -> None:
        self._session = session

    def app_session(self):
        return self._session


def _processor(session, repo) -> FeedbackProcessor:
    """Build a FeedbackProcessor wired to `session` for direct-DB lookups.

    `repo` is the real `planner_repo` fixture (per conftest.py's caution: it
    was previously constructed but never exercised) even though
    `_find_realized_sale` does not call into it either -- it matches
    FeedbackProcessor's real production wiring shape.
    """
    return FeedbackProcessor(
        repo=repo,
        admin_settings=_make_admin(),
        session_provider=_StaticSessionProvider(session),
    )


def _make_admin(alpha: float = 0.2, slow_mover_timeout: float = 60.0) -> MagicMock:
    admin = MagicMock()
    settings = {
        "planner_ema_alpha": alpha,
        "planner_slow_mover_timeout_days": slow_mover_timeout,
        "planner_invention_min_attempts": 10,
    }
    admin.get.side_effect = lambda section, key: settings.get(key, None)
    return admin


def _make_action(
    action_id: int = 1,
    type_id: int = 100,
    plan_id: int = 1,
    action_type: str = "manufacture",
    status: str = "done",
    processed: bool = False,
    generated_at: datetime | None = None,
    estimated_cost_isk: float | None = None,
    quantity: int | None = None,
    estimated_build_cost_isk: float | None = None,
) -> MagicMock:
    action = MagicMock()
    action.id = action_id
    action.type_id = type_id
    action.plan_id = plan_id
    action.action_type = action_type
    action.status = status
    action.processed_for_feedback = processed
    action.generated_at = generated_at or _utcnow() - timedelta(days=1)
    # Set explicitly: a bare MagicMock attribute satisfies float() as 1.0
    # (see _make_plan_item's docstring), which would silently fabricate a
    # 1 ISK batch cost over 1 unit.
    action.estimated_cost_isk = estimated_cost_isk
    action.estimated_build_cost_isk = estimated_build_cost_isk
    action.quantity = quantity
    return action


def _make_plan_item(
    type_id: int = 100,
    isk_per_hour: float = 10_000_000.0,
    effective_velocity: float | None = 1.0,
) -> MagicMock:
    """Build a plan_item mock.

    `effective_velocity` defaults to `1.0` (a 1.0-day predicted sell time)
    to match the value every caller of this helper got *by accident* before
    this parameter existed: a bare `MagicMock()` numeric attribute silently
    satisfies `float(...)` as `1.0` instead of raising -- e.g.
    `float(MagicMock().effective_velocity) == 1.0` -- so
    `_estimate_predicted_sell_days` was returning 1.0 for every plan_item
    built here, not its documented 7.0-day fallback. That silent trap is
    exactly what hid the EMA-velocity test bugs for so long: they assumed
    the 7.0 fallback in their `expected_velocity` math while the code under
    test was actually consuming 1.0.

    Pass `effective_velocity=None` explicitly to exercise an unknown
    velocity, which gives no predicted sell time at all (no 7.0 fallback).
    """
    item = MagicMock()
    item.id = 1
    item.type_id = type_id
    item.isk_per_hour = isk_per_hour
    item.effective_velocity = effective_velocity
    return item


def _make_weights(
    accuracy_ema: float = 1.0,
    velocity_multiplier: float = 1.0,
    cost_multiplier: float = 1.0,
    sample_count: int = 5,
) -> MagicMock:
    w = MagicMock()
    w.accuracy_ema = accuracy_ema
    w.velocity_multiplier = velocity_multiplier
    w.cost_multiplier = cost_multiplier
    w.sample_count = sample_count
    w.confidence_tier = "medium"
    return w


class TestFeedbackProcessor:
    def setup_method(self):
        self.repo = MagicMock()
        self.admin = _make_admin(alpha=0.2, slow_mover_timeout=60.0)
        self.processor = FeedbackProcessor(self.repo, self.admin)
        # `_find_realized_sale` is the real method that looks up a sale for
        # an action; these tests are about EMA arithmetic, not sale lookup
        # (that is covered separately by TestFindRealizedSaleAttribution's
        # temporal tests), so it is stubbed directly per test below.
        self.processor._find_realized_sale = MagicMock()

    def test_no_done_actions_returns_0(self):
        """No unprocessed done actions → process_pending_feedback returns 0."""
        self.repo.get_unprocessed_done_actions.return_value = []
        result = self.processor.process_pending_feedback()
        assert result == 0

    def test_normal_outcome_ema_accuracy_update(self):
        """Normal outcome: accuracy_ema updated correctly."""
        action = _make_action(type_id=100, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=100, isk_per_hour=10_000_000)]
        self.repo.get_weights.return_value = {
            100: _make_weights(accuracy_ema=1.0, velocity_multiplier=1.0, cost_multiplier=1.0, sample_count=5)
        }

        # Realized sale: actual ISK/hour = 8M
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 8_000_000.0,
            "sell_days": 10.0,
        }

        result = self.processor.process_pending_feedback()
        assert result == 1

        # Verify upsert_weights was called
        assert self.repo.upsert_weights.called

        # Extract the PlanLearningWeightsModel passed to upsert_weights
        weights_arg = self.repo.upsert_weights.call_args[0][0]

        # accuracy_ema = 0.8 * 1.0 + 0.2 * (8M / 10M) = 0.8 + 0.2*0.8 = 0.8 + 0.16 = 0.96
        expected_accuracy = 0.8 * 1.0 + 0.2 * (8_000_000.0 / 10_000_000.0)
        assert abs(weights_arg.accuracy_ema - expected_accuracy) < 0.001, (
            f"Expected accuracy_ema={expected_accuracy}, got {weights_arg.accuracy_ema}"
        )

    def test_an_out_of_band_stored_weight_is_clamped_before_the_ema(self):
        """A hand-edited 10.0 must enter the EMA as 4.0: 0.8*4.0 + 0.2*0.8 = 3.36,
        not 0.8*10.0 + 0.2*0.8 = 8.16 clamped to 4.0."""
        action = _make_action(type_id=220)
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_weights.return_value = {220: _make_weights(accuracy_ema=10.0)}
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=220, isk_per_hour=10_000_000)]
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 8_000_000.0, "sell_days": 5.0,
        }
        self.processor.process_pending_feedback()
        weights = self.repo.upsert_weights.call_args[0][0]
        assert abs(weights.accuracy_ema - (0.8 * 4.0 + 0.2 * 0.8)) < 1e-9

    def _run_cost_case(self, *, build_cost, action_qty, allocations, old_cost=1.0):
        action = _make_action(type_id=200, estimated_build_cost_isk=build_cost, quantity=action_qty)
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_weights.return_value = {200: _make_weights(cost_multiplier=old_cost)}
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=200, isk_per_hour=10_000_000)]
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 8_000_000.0, "allocation_details": allocations, "sell_days": 5.0,
        }
        self.processor.process_pending_feedback()
        return (self.repo.upsert_weights.call_args[0][0],
                self.repo.insert_outcome.call_args[0][0])

    def test_cost_update_compares_full_build_cost_per_unit(self):
        """Both sides are materials + job fees per unit: 55M / 100 = 550k
        predicted vs 6M / 10 = 600k actual from the sale's industry-built units."""
        weights, outcome = self._run_cost_case(
            build_cost=55_000_000.0, action_qty=100,
            allocations=[{"source": "industry_build", "quantity": 10, "total_cost": 6_000_000.0}],
        )
        expected = 0.8 * 1.0 + 0.2 * (550_000.0 / 600_000.0)
        assert abs(weights.cost_multiplier - expected) < 1e-9
        assert outcome.predicted_material_cost == 550_000.0
        assert outcome.actual_material_cost == 600_000.0

    def test_only_industry_built_units_count_as_actual_cost(self):
        """A market-bought lot's unit price is not a build cost."""
        weights, outcome = self._run_cost_case(
            build_cost=55_000_000.0, action_qty=100,
            allocations=[
                {"source": "industry_build", "quantity": 10, "total_cost": 6_000_000.0},
                {"source": "market_buy", "quantity": 90, "total_cost": 1.0},
            ],
        )
        assert outcome.actual_material_cost == 600_000.0

    def test_transferred_build_lots_count_as_actual_cost(self):
        """Character-built units moved to the corp carry the job's
        unit_build_cost (FIFO-matched or quantity-weighted average)."""
        weights, outcome = self._run_cost_case(
            build_cost=55_000_000.0, action_qty=100,
            allocations=[
                {"source": "industry_build_transferred", "quantity": 4, "total_cost": 2_000_000.0},
                {"source": "industry_build_transferred_avg", "quantity": 6, "total_cost": 4_000_000.0},
                {"source": "opening_inventory", "quantity": 5, "total_cost": 1.0},
            ],
        )
        assert outcome.actual_material_cost == 600_000.0

    @staticmethod
    def _skip_messages(caplog):
        return [r.getMessage() for r in caplog.records if "cost update skipped" in r.getMessage()]

    def test_a_sale_with_no_industry_built_units_skips_the_cost_update(self, caplog):
        with caplog.at_level(logging.INFO):
            weights, outcome = self._run_cost_case(
                build_cost=55_000_000.0, action_qty=100, old_cost=0.9,
                allocations=[{"source": "market_buy", "quantity": 10, "total_cost": 6_000_000.0}],
            )
        assert weights.cost_multiplier == 0.9
        assert outcome.actual_material_cost is None
        (message,) = self._skip_messages(caplog)
        assert "no industry-built lots in this sale" in message
        assert "market_buy" in message

    def test_zero_allocated_cost_skips_the_cost_update(self, caplog):
        with caplog.at_level(logging.INFO):
            weights, outcome = self._run_cost_case(
                build_cost=55_000_000.0, action_qty=100, old_cost=0.9,
                allocations=[{"source": "industry_build", "quantity": 10, "total_cost": 0.0}],
            )
        assert weights.cost_multiplier == 0.9
        assert outcome.actual_material_cost is None
        (message,) = self._skip_messages(caplog)
        assert "industry-built lots have no positive cost and quantity" in message

    @pytest.mark.parametrize("allocations", [None, "not-a-list", {"source": "industry_build"}])
    def test_missing_or_malformed_allocation_details_skips_the_cost_update(self, caplog, allocations):
        with caplog.at_level(logging.INFO):
            weights, outcome = self._run_cost_case(
                build_cost=55_000_000.0, action_qty=100, old_cost=0.9, allocations=allocations,
            )
        assert weights.cost_multiplier == 0.9
        assert outcome.actual_material_cost is None
        (message,) = self._skip_messages(caplog)
        assert "allocation_details missing or not a list" in message

    def test_unknown_predicted_build_cost_skips_the_cost_update(self, caplog):
        for cost, qty in ((None, 100), (55_000_000.0, None), (55_000_000.0, 0)):
            caplog.clear()
            with caplog.at_level(logging.INFO):
                weights, outcome = self._run_cost_case(
                    build_cost=cost, action_qty=qty, old_cost=1.1,
                    allocations=[{"source": "industry_build", "quantity": 10, "total_cost": 6e6}],
                )
            assert weights.cost_multiplier == 1.1, (cost, qty)
            assert outcome.predicted_material_cost is None
            assert any("cost update skipped" in r.getMessage() for r in caplog.records)

    def test_cost_multiplier_is_clamped_to_the_band(self):
        from eve_online_industry_tracker.application.daily_planner.feedback_processor import (
            LEARNING_WEIGHT_MAX, LEARNING_WEIGHT_MIN,
        )
        high, _ = self._run_cost_case(
            build_cost=1_000_000_000.0, action_qty=1,
            allocations=[{"source": "industry_build", "quantity": 1, "total_cost": 1.0}],
        )
        assert high.cost_multiplier == LEARNING_WEIGHT_MAX
        self.repo.reset_mock()
        low, _ = self._run_cost_case(
            build_cost=1.0, action_qty=1, old_cost=0.0,
            allocations=[{"source": "industry_build", "quantity": 1, "total_cost": 1e9}],
        )
        assert low.cost_multiplier == LEARNING_WEIGHT_MIN

    def test_velocity_and_accuracy_are_clamped_to_the_band(self):
        from eve_online_industry_tracker.application.daily_planner.feedback_processor import (
            LEARNING_WEIGHT_MAX,
        )
        action = _make_action(type_id=210)
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_weights.return_value = {
            210: _make_weights(accuracy_ema=LEARNING_WEIGHT_MAX, velocity_multiplier=LEARNING_WEIGHT_MAX)
        }
        # 30-day predicted sell time vs a 0.1-day sale; 1 ISK/h predicted vs 1B.
        self.repo.get_plan_items.return_value = [
            _make_plan_item(type_id=210, isk_per_hour=1.0, effective_velocity=1 / 30.0)
        ]
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 1_000_000_000.0, "sell_days": 0.1,
        }
        self.processor.process_pending_feedback()
        weights = self.repo.upsert_weights.call_args[0][0]
        assert weights.velocity_multiplier == LEARNING_WEIGHT_MAX
        assert weights.accuracy_ema == LEARNING_WEIGHT_MAX

    @staticmethod
    def _accuracy_skip_messages(caplog):
        return [
            r.getMessage() for r in caplog.records
            if r.levelno == logging.INFO and "accuracy update skipped" in r.getMessage()
        ]

    @pytest.mark.parametrize("plan_items, realized_isk, reason", [
        ([], 8_000_000.0, "predicted ISK/h is unknown (no plan item"),
        ([_make_plan_item(type_id=320, isk_per_hour=None)], 8_000_000.0, "predicted ISK/h is unknown"),
        ([_make_plan_item(type_id=320, isk_per_hour=0.0)], 8_000_000.0, "predicted ISK/h is not a positive"),
        ([_make_plan_item(type_id=320, isk_per_hour=10_000_000.0)], None, "realized ISK/h is unknown"),
        ([_make_plan_item(type_id=320, isk_per_hour=10_000_000.0)], float("nan"), "realized ISK/h is not a finite"),
    ])
    def test_a_sale_with_unknown_isk_per_hour_keeps_the_accuracy_weight(
        self, caplog, plan_items, realized_isk, reason,
    ):
        """No invented 1 ISK/h prediction or 0 ISK/h realization: accuracy stays,
        velocity and cost still learn from their own inputs."""
        action = _make_action(type_id=320, estimated_build_cost_isk=55_000_000.0, quantity=100)
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_weights.return_value = {
            320: _make_weights(accuracy_ema=1.3, velocity_multiplier=1.0, cost_multiplier=1.0)
        }
        self.repo.get_plan_items.return_value = plan_items
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": realized_isk, "sell_days": 4.0,
            "allocation_details": [{"source": "industry_build", "quantity": 10, "total_cost": 6_000_000.0}],
        }
        with caplog.at_level(logging.INFO):
            assert self.processor.process_pending_feedback() == 1

        weights = self.repo.upsert_weights.call_args[0][0]
        outcome = self.repo.insert_outcome.call_args[0][0]
        assert weights.accuracy_ema == 1.3
        assert outcome.accuracy_ratio is None
        assert abs(weights.cost_multiplier - (0.8 + 0.2 * (550_000.0 / 600_000.0))) < 1e-9
        if plan_items:
            # velocity learns from the plan item's own velocity (1.0 -> 1 day / 4 days)
            assert abs(weights.velocity_multiplier - (0.8 + 0.2 * 0.25)) < 1e-9
        (message,) = self._accuracy_skip_messages(caplog)
        assert reason in message
        assert "type_id=320" in message

    @pytest.mark.parametrize("realized_isk", [0.0, -5e6])
    def test_a_zero_or_negative_realized_isk_per_hour_still_updates_accuracy(self, caplog, realized_isk):
        """A sale at zero or a loss is real data: it pulls accuracy down, not skipped."""
        from eve_online_industry_tracker.application.daily_planner.feedback_processor import clamp_weight

        action = _make_action(type_id=322)
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_weights.return_value = {322: _make_weights(accuracy_ema=1.0)}
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=322, isk_per_hour=10_000_000.0)]
        self.processor._find_realized_sale.return_value = {"isk_per_hour": realized_isk, "sell_days": 4.0}
        with caplog.at_level(logging.INFO):
            assert self.processor.process_pending_feedback() == 1

        weights = self.repo.upsert_weights.call_args[0][0]
        outcome = self.repo.insert_outcome.call_args[0][0]
        ratio = realized_isk / 10_000_000.0
        assert weights.accuracy_ema == clamp_weight(0.8 * 1.0 + 0.2 * ratio)
        assert outcome.accuracy_ratio == ratio
        assert outcome.accuracy_ratio <= 0
        assert self._accuracy_skip_messages(caplog) == []

    def test_a_sale_with_no_plan_item_stores_no_predicted_isk_per_hour(self):
        action = _make_action(type_id=321)
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_weights.return_value = {321: _make_weights(accuracy_ema=1.1)}
        self.repo.get_plan_items.return_value = []
        self.processor._find_realized_sale.return_value = {"isk_per_hour": 5.0, "sell_days": 2.0}
        self.processor.process_pending_feedback()
        outcome = self.repo.insert_outcome.call_args[0][0]
        assert outcome.predicted_isk_per_hour is None
        assert outcome.actual_isk_per_hour == 5.0
        assert outcome.accuracy_ratio is None

    def test_normal_outcome_ema_velocity_update(self):
        """velocity_multiplier = 0.8*old + 0.2*(predicted_days/actual_days),
        predicted_days = 1 / effective_velocity (0.2 units/day -> 5 days)."""
        action = _make_action(type_id=300, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [action]
        old_velocity = 1.0
        self.repo.get_weights.return_value = {
            300: _make_weights(velocity_multiplier=old_velocity)
        }
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=300, effective_velocity=0.2)]

        actual_days = 4.0
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 10_000_000.0,
            "sell_days": actual_days,
        }

        self.processor.process_pending_feedback()

        weights_arg = self.repo.upsert_weights.call_args[0][0]
        expected_velocity = 0.8 * old_velocity + 0.2 * (5.0 / actual_days)
        assert abs(weights_arg.velocity_multiplier - expected_velocity) < 1e-9

    @staticmethod
    def _velocity_skip_messages(caplog):
        return [
            r.getMessage() for r in caplog.records
            if r.levelno == logging.INFO and "velocity update skipped" in r.getMessage()
        ]

    @pytest.mark.parametrize("plan_items, reason", [
        ([_make_plan_item(type_id=310, effective_velocity=None)], "effective_velocity is unknown"),
        ([], "no plan item for this type"),
    ])
    def test_a_sale_with_unknown_predicted_velocity_keeps_the_velocity_weight(
        self, caplog, plan_items, reason,
    ):
        """No invented 7-day prediction: velocity stays, accuracy and cost still learn."""
        action = _make_action(type_id=310, estimated_build_cost_isk=55_000_000.0, quantity=100)
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_weights.return_value = {
            310: _make_weights(accuracy_ema=1.0, velocity_multiplier=1.3, cost_multiplier=1.0)
        }
        self.repo.get_plan_items.return_value = plan_items
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 8_000_000.0, "sell_days": 5.0,
            "allocation_details": [{"source": "industry_build", "quantity": 10, "total_cost": 6_000_000.0}],
        }
        with caplog.at_level(logging.INFO):
            assert self.processor.process_pending_feedback() == 1

        weights = self.repo.upsert_weights.call_args[0][0]
        outcome = self.repo.insert_outcome.call_args[0][0]
        assert weights.velocity_multiplier == 1.3
        assert outcome.predicted_sell_days is None
        assert outcome.actual_sell_days == 5.0
        # Cost has its own inputs and still updates: 0.8 + 0.2 * (550k / 600k).
        assert abs(weights.cost_multiplier - (0.8 + 0.2 * (550_000.0 / 600_000.0))) < 1e-9
        if plan_items:
            # Accuracy needs the plan item's isk_per_hour (10M): 0.8 + 0.2 * 0.8.
            assert abs(weights.accuracy_ema - (0.8 + 0.2 * 0.8)) < 1e-9
        (message,) = self._velocity_skip_messages(caplog)
        assert reason in message
        assert "type_id=310" in message

    @pytest.mark.parametrize("plan_items, reason", [
        ([_make_plan_item(type_id=400, effective_velocity=None)], "effective_velocity is unknown"),
        ([], "no plan item for this type"),
    ])
    def test_slow_mover_with_unknown_predicted_velocity_keeps_every_weight(
        self, caplog, plan_items, reason,
    ):
        """Slow mover path: accuracy and cost are never updated, and with no
        predicted velocity there is no velocity penalty to compute either."""
        old_action = _make_action(type_id=400, generated_at=_utcnow() - timedelta(days=65))
        self.repo.get_unprocessed_done_actions.return_value = [old_action]
        self.repo.get_weights.return_value = {
            400: _make_weights(accuracy_ema=0.95, cost_multiplier=1.05, velocity_multiplier=1.2)
        }
        self.repo.get_plan_items.return_value = plan_items
        self.processor._find_realized_sale.return_value = None

        with caplog.at_level(logging.INFO):
            assert self.processor.process_pending_feedback() == 1

        weights = self.repo.upsert_weights.call_args[0][0]
        outcome = self.repo.insert_outcome.call_args[0][0]
        assert (weights.accuracy_ema, weights.cost_multiplier, weights.velocity_multiplier) == (
            0.95, 1.05, 1.2,
        )
        assert outcome.slow_mover is True
        assert outcome.predicted_sell_days is None
        (message,) = self._velocity_skip_messages(caplog)
        assert reason in message

    def test_slow_mover_skips_accuracy_and_cost(self):
        """Slow mover path: only velocity_multiplier updated; accuracy_ema and cost_multiplier unchanged."""
        old_accuracy = 0.95
        old_cost = 1.05
        old_velocity = 1.0
        slow_mover_timeout = 60.0

        # Action is old enough to be a slow mover (generated 65 days ago)
        old_action = _make_action(
            type_id=400,
            action_type="manufacture",
            generated_at=_utcnow() - timedelta(days=65),
        )
        self.repo.get_unprocessed_done_actions.return_value = [old_action]
        self.repo.get_weights.return_value = {
            400: _make_weights(accuracy_ema=old_accuracy, cost_multiplier=old_cost, velocity_multiplier=old_velocity)
        }
        # 0.125 units/day -> an 8-day predicted sell time.
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=400, effective_velocity=0.125)]

        # No realized sale (slow mover)
        self.processor._find_realized_sale.return_value = None

        self.processor.process_pending_feedback()

        weights_arg = self.repo.upsert_weights.call_args[0][0]

        # accuracy_ema NOT updated → should equal old_accuracy
        assert abs(weights_arg.accuracy_ema - old_accuracy) < 0.001, (
            f"accuracy_ema should not change for slow_mover, got {weights_arg.accuracy_ema}"
        )

        # cost_multiplier NOT updated → should equal old_cost
        assert abs(weights_arg.cost_multiplier - old_cost) < 0.001, (
            f"cost_multiplier should not change for slow_mover, got {weights_arg.cost_multiplier}"
        )

        # velocity_multiplier IS updated: 0.8 * 1.0 + 0.2 * (8.0 / 60.0)
        expected_velocity = 0.8 * old_velocity + 0.2 * (8.0 / slow_mover_timeout)
        assert abs(weights_arg.velocity_multiplier - expected_velocity) < 1e-9

    def test_processed_for_feedback_marked_true(self):
        """After processing, action is marked processed_for_feedback=True."""
        action = _make_action(type_id=500, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=500)]
        self.repo.get_weights.return_value = {500: _make_weights()}
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 10_000_000.0,
            "sell_days": 7.0,
        }

        self.processor.process_pending_feedback()

        self.repo.mark_action_feedback_processed.assert_called_once_with(action.id)

    def test_non_manufacture_actions_marked_processed_but_not_counted(self):
        """deliver actions are marked processed but not counted in return value."""
        deliver = _make_action(type_id=600, action_type="deliver")
        manufacture = _make_action(type_id=601, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [deliver, manufacture]
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=601)]
        self.repo.get_weights.return_value = {601: _make_weights()}
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 10_000_000.0,
            "sell_days": 7.0,
        }

        result = self.processor.process_pending_feedback()

        # Only manufacture actions counted
        assert result == 1
        # Both should be marked processed
        assert self.repo.mark_action_feedback_processed.call_count == 2

    def test_legacy_relist_order_row_is_marked_processed_and_does_not_crash(self):
        """The RELIST phase is gone, but plans persisted before its removal may still
        hold relist_order rows; the feedback reader must consume them like any other
        non-manufacture action."""
        legacy = _make_action(type_id=610, action_type="relist_order")
        self.repo.get_unprocessed_done_actions.return_value = [legacy]

        result = self.processor.process_pending_feedback()

        assert result == 0
        self.repo.mark_action_feedback_processed.assert_called_once_with(legacy.id)

    def test_action_not_yet_slow_mover_skipped_without_realized_sale(self):
        """Action generated recently (within timeout) with no sale → not processed as slow_mover."""
        recent_action = _make_action(
            type_id=700,
            action_type="manufacture",
            generated_at=_utcnow() - timedelta(days=5),  # only 5 days old, timeout=60
        )
        self.repo.get_unprocessed_done_actions.return_value = [recent_action]
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=700)]
        self.repo.get_weights.return_value = {700: _make_weights()}
        self.processor._find_realized_sale.return_value = None  # no sale yet

        result = self.processor.process_pending_feedback()

        # Should return 0 (not processed as slow_mover yet)
        assert result == 0
        # upsert_weights should NOT have been called (no update)
        self.repo.upsert_weights.assert_not_called()
        # mark_action_feedback_processed should NOT have been called (action deferred)
        self.repo.mark_action_feedback_processed.assert_not_called()

    def test_sample_count_incremented(self):
        """sample_count is incremented after each processed action."""
        action = _make_action(type_id=800, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=800)]
        old_count = 4
        self.repo.get_weights.return_value = {800: _make_weights(sample_count=old_count)}
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 10_000_000.0,
            "sell_days": 7.0,
        }

        self.processor.process_pending_feedback()

        weights_arg = self.repo.upsert_weights.call_args[0][0]
        assert weights_arg.sample_count == old_count + 1

    def test_confidence_tier_upgrades_at_5_samples(self):
        """At 5 samples (after increment from 4), confidence_tier becomes 'medium'."""
        action = _make_action(type_id=900, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=900)]
        self.repo.get_weights.return_value = {900: _make_weights(sample_count=4)}  # 4 → 5 after increment
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 10_000_000.0,
            "sell_days": 7.0,
        }

        self.processor.process_pending_feedback()

        weights_arg = self.repo.upsert_weights.call_args[0][0]
        assert weights_arg.confidence_tier == "medium", f"Got {weights_arg.confidence_tier}"

    def test_confidence_tier_high_at_20_samples(self):
        """At 20+ samples, confidence_tier='high'."""
        action = _make_action(type_id=901, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=901)]
        self.repo.get_weights.return_value = {901: _make_weights(sample_count=19)}  # 19 → 20
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 10_000_000.0,
            "sell_days": 7.0,
        }

        self.processor.process_pending_feedback()

        weights_arg = self.repo.upsert_weights.call_args[0][0]
        assert weights_arg.confidence_tier == "high", f"Got {weights_arg.confidence_tier}"


class TestEstimatePredictedSellDays:
    """`_estimate_predicted_sell_days` has no constant fallback: an unknown
    velocity is an unknown prediction (None), never an invented 7 days."""

    @pytest.mark.parametrize("velocity", [None, 0.0, -1.0, float("nan"), float("inf"), "abc"])
    def test_an_unknown_or_unusable_velocity_gives_no_prediction(self, velocity):
        from eve_online_industry_tracker.application.daily_planner.feedback_processor import (
            _estimate_predicted_sell_days,
        )

        assert _estimate_predicted_sell_days(_make_plan_item(effective_velocity=velocity)) is None

    def test_no_plan_item_gives_no_prediction(self):
        from eve_online_industry_tracker.application.daily_planner.feedback_processor import (
            _estimate_predicted_sell_days,
        )

        assert _estimate_predicted_sell_days(None) is None

    @pytest.mark.parametrize("velocity, days", [(0.25, 4.0), (0.01, 30.0), (100.0, 0.1)])
    def test_a_known_velocity_is_inverted_and_clamped(self, velocity, days):
        from eve_online_industry_tracker.application.daily_planner.feedback_processor import (
            _estimate_predicted_sell_days,
        )

        assert _estimate_predicted_sell_days(_make_plan_item(effective_velocity=velocity)) == days


class TestFindRealizedSaleAttribution:
    """Finding 14: a realized sale must be attributable to the action it is credited to.

    `_find_realized_sale` must not credit a sale that happened before the
    action it is scoring -- that would corrupt the EMA learning weights.
    These use realistic stored values: full ISO-8601 timestamps with a
    trailing `Z`, matching every row in the live
    `corporation_realized_sales_ledger` table, because `date` is a VARCHAR
    column, not a Date/DateTime one. Seeding with bare `date(...)` objects
    (as an earlier draft of this test suite did) would pass against a
    lexicographic-comparison implementation for the wrong reason -- SQLite
    would coerce them to a comparable form -- without ever exercising the
    string-vs-string comparison this fix depends on.
    """

    def test_a_sale_predating_the_action_is_not_credited_to_it(self, planner_repo, app_session):
        from eve_online_industry_tracker.infrastructure.models import (
            CorporationRealizedSalesLedgerModel,
        )

        app_session.add(CorporationRealizedSalesLedgerModel(
            corporation_id=1, transaction_id=1, quantity=1,
            type_id=12345, realized_profit=500.0, allocated_cost=100.0,
            date="2026-09-01T09:11:27Z",
        ))
        app_session.commit()

        processor = _processor(app_session, planner_repo)
        action = SimpleNamespace(type_id=12345, generated_at=datetime(2026, 9, 10))
        assert processor._find_realized_sale(12345, action=action) is None

    def test_a_sale_after_the_action_is_credited(self, planner_repo, app_session):
        from eve_online_industry_tracker.infrastructure.models import (
            CorporationRealizedSalesLedgerModel,
        )

        app_session.add(CorporationRealizedSalesLedgerModel(
            corporation_id=1, transaction_id=2, quantity=1,
            allocation_details=[{"source": "industry_build", "quantity": 1, "total_cost": 100.0}],
            type_id=12345, realized_profit=500.0, allocated_cost=100.0,
            date="2026-09-12T09:11:27Z",
        ))
        app_session.commit()

        processor = _processor(app_session, planner_repo)
        action = SimpleNamespace(type_id=12345, generated_at=datetime(2026, 9, 10))
        found = processor._find_realized_sale(12345, action=action)
        assert found is not None
        assert found["allocation_details"][0]["total_cost"] == 100.0
        assert found["sell_days"] == 2.0

    def test_the_sale_carries_its_allocation_details(self, planner_repo, app_session):
        """The actual unit cost is read from the sale's per-lot allocations,
        so only industry-built lots are compared with the predicted build cost."""
        from eve_online_industry_tracker.infrastructure.models import (
            CorporationRealizedSalesLedgerModel,
        )

        allocations = [
            {"source": "industry_build", "quantity": 6, "unit_cost": 500.0, "total_cost": 3_000.0},
            {"source": "market_buy", "quantity": 4, "unit_cost": 750.0, "total_cost": 3_000.0},
        ]
        app_session.add(CorporationRealizedSalesLedgerModel(
            corporation_id=1, transaction_id=7, quantity=12, priced_quantity=10,
            unpriced_quantity=2, type_id=12345, realized_profit=500.0,
            allocated_cost=6_000.0, allocation_details=allocations,
            date="2026-09-12T09:11:27Z",
        ))
        app_session.commit()

        processor = _processor(app_session, planner_repo)
        action = SimpleNamespace(type_id=12345, generated_at=datetime(2026, 9, 10))
        found = processor._find_realized_sale(12345, action=action)
        assert found["allocation_details"] == allocations
        assert "material_cost" not in found

    def test_a_sale_on_the_same_day_after_the_action_is_credited(self, planner_repo, app_session):
        """Same calendar day, later time-of-day: credited. The filter compares
        full timestamps (not just the calendar date), but 23:59 is still at
        or after the action's 01:00, so this sale qualifies. `sell_days` uses
        the pre-existing zero/negative-diff fallback of a flat 1.0-day
        interval -- unchanged by this fix, which only governs whether a sale
        is attributable at all, not this same-day edge case."""
        from eve_online_industry_tracker.infrastructure.models import (
            CorporationRealizedSalesLedgerModel,
        )

        app_session.add(CorporationRealizedSalesLedgerModel(
            corporation_id=1, transaction_id=3, quantity=1,
            allocation_details=[{"source": "industry_build", "quantity": 1, "total_cost": 40.0}],
            type_id=12345, realized_profit=240.0, allocated_cost=40.0,
            date="2026-09-10T23:59:00Z",
        ))
        app_session.commit()

        processor = _processor(app_session, planner_repo)
        action = SimpleNamespace(type_id=12345, generated_at=datetime(2026, 9, 10, 1, 0, 0))
        found = processor._find_realized_sale(12345, action=action)
        assert found is not None
        assert found["sell_days"] == 1.0
        assert found["allocation_details"][0]["total_cost"] == 40.0

    def test_a_sale_on_the_same_day_before_the_action_is_not_credited(self, planner_repo, app_session):
        """Same calendar date, earlier time-of-day: must NOT be credited.

        A date-only filter (comparing calendar dates instead of full
        timestamps) would let this sale through, since it shares the
        action's date -- but 01:00 is strictly before the action's 09:00, so
        it is exactly a pre-dating sale, the defect this method exists to
        prevent, just at intraday granularity. This is the discriminating
        test for the full-timestamp filter: it fails (wrongly credits) if
        the filter is ever loosened back to comparing calendar dates.
        """
        from eve_online_industry_tracker.infrastructure.models import (
            CorporationRealizedSalesLedgerModel,
        )

        app_session.add(CorporationRealizedSalesLedgerModel(
            corporation_id=1, transaction_id=4, quantity=1,
            type_id=12345, realized_profit=240.0, allocated_cost=40.0,
            date="2026-05-11T01:00:00Z",
        ))
        app_session.commit()

        processor = _processor(app_session, planner_repo)
        action = SimpleNamespace(type_id=12345, generated_at=datetime(2026, 5, 11, 9, 0, 0))
        assert processor._find_realized_sale(12345, action=action) is None

    def test_earliest_qualifying_sale_is_credited_not_the_latest(self, planner_repo, app_session):
        """When several sales postdate the action, the EARLIEST is credited.

        Ordering by most-recent would take the globally latest sale of this
        type_id regardless of which action asked -- so every earlier
        action's lookup for a repeatedly-manufactured type would collide on
        the same row, crediting one sale to many outcomes and inflating
        sell_days arbitrarily for the older ones. Earliest-after is bounded
        to this action's own window and cannot be shared that way.
        """
        from eve_online_industry_tracker.infrastructure.models import (
            CorporationRealizedSalesLedgerModel,
        )

        app_session.add(CorporationRealizedSalesLedgerModel(
            corporation_id=1, transaction_id=5, quantity=1,
            allocation_details=[{"source": "industry_build", "quantity": 1, "total_cost": 10.0}],
            type_id=12345, realized_profit=100.0, allocated_cost=10.0,
            date="2026-09-11T00:00:00Z",  # earliest qualifying sale
        ))
        app_session.add(CorporationRealizedSalesLedgerModel(
            corporation_id=1, transaction_id=6, quantity=1,
            allocation_details=[{"source": "industry_build", "quantity": 1, "total_cost": 999.0}],
            type_id=12345, realized_profit=999.0, allocated_cost=999.0,
            date="2026-09-20T00:00:00Z",  # latest -- must NOT be picked
        ))
        app_session.commit()

        processor = _processor(app_session, planner_repo)
        action = SimpleNamespace(type_id=12345, generated_at=datetime(2026, 9, 10))
        found = processor._find_realized_sale(12345, action=action)
        assert found is not None
        assert found["allocation_details"][0]["total_cost"] == 10.0
        assert found["sell_days"] == 1.0

    def test_without_an_action_date_no_sale_is_credited(self, planner_repo, app_session):
        processor = _processor(app_session, planner_repo)
        assert processor._find_realized_sale(12345, action=None) is None
