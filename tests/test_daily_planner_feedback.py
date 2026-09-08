"""Tests for FeedbackProcessor — EMA update logic."""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, call, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.daily_planner.feedback_processor import FeedbackProcessor
from eve_online_industry_tracker.infrastructure.models import (
    PlanLearningWeightsModel,
    PlanItemOutcomeModel,
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
) -> MagicMock:
    action = MagicMock()
    action.id = action_id
    action.type_id = type_id
    action.plan_id = plan_id
    action.action_type = action_type
    action.status = status
    action.processed_for_feedback = processed
    action.generated_at = generated_at or datetime.utcnow() - timedelta(days=1)
    return action


def _make_plan_item(type_id: int = 100, isk_per_hour: float = 10_000_000.0) -> MagicMock:
    item = MagicMock()
    item.id = 1
    item.type_id = type_id
    item.isk_per_hour = isk_per_hour
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
        self.realized = MagicMock()
        self.repo = MagicMock()
        self.admin = _make_admin(alpha=0.2, slow_mover_timeout=60.0)
        self.processor = FeedbackProcessor(self.realized, self.repo, self.admin)

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
        self.realized.get_realized_profit_for_type.return_value = {
            "isk_per_hour": 8_000_000.0,
            "material_cost": 5_000_000.0,
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

    def test_normal_outcome_ema_cost_update(self):
        """Normal outcome: cost_multiplier = 0.8*old + 0.2*(predicted/actual)."""
        action = _make_action(type_id=200, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [action]
        old_cost = 1.0
        self.repo.get_weights.return_value = {
            200: _make_weights(cost_multiplier=old_cost)
        }
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=200, isk_per_hour=10_000_000)]

        # Predicted cost = 1.0 (no plan_item material cost), actual = 6M
        # cost_multiplier = 0.8 * 1.0 + 0.2 * (max(0.01, 1.0) / max(0.01, 6M))
        actual_cost = 6_000_000.0
        self.realized.get_realized_profit_for_type.return_value = {
            "isk_per_hour": 8_000_000.0,
            "material_cost": actual_cost,
            "sell_days": 5.0,
        }

        self.processor.process_pending_feedback()

        weights_arg = self.repo.upsert_weights.call_args[0][0]
        # safe_predicted_cost = max(0.01, None→1.0) = 1.0
        # cost_multiplier = 0.8 * 1.0 + 0.2 * (1.0 / 6000000) ≈ 0.8 + very small ≈ 0.8
        # Note: predicted_material_cost is None (not stored on plan_item) → defaults to 1.0
        # This is a limitation; test the direction of the formula
        assert weights_arg.cost_multiplier < 1.0, (
            f"cost_multiplier should be < 1.0 when actual > predicted, got {weights_arg.cost_multiplier}"
        )

    def test_normal_outcome_ema_velocity_update(self):
        """velocity_multiplier = 0.8*old + 0.2*(predicted_days/actual_days)."""
        action = _make_action(type_id=300, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [action]
        old_velocity = 1.0
        self.repo.get_weights.return_value = {
            300: _make_weights(velocity_multiplier=old_velocity)
        }
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=300)]

        # Actual sell_days = 5.0; predicted = 7.0 (fixed default)
        actual_days = 5.0
        self.realized.get_realized_profit_for_type.return_value = {
            "isk_per_hour": 10_000_000.0,
            "material_cost": 5_000_000.0,
            "sell_days": actual_days,
        }

        self.processor.process_pending_feedback()

        weights_arg = self.repo.upsert_weights.call_args[0][0]
        # predicted_days = 7.0 (fixed default from _estimate_predicted_sell_days)
        # velocity = 0.8 * 1.0 + 0.2 * (7.0 / 5.0) = 0.8 + 0.28 = 1.08
        expected_velocity = 0.8 * old_velocity + 0.2 * (7.0 / actual_days)
        assert abs(weights_arg.velocity_multiplier - expected_velocity) < 0.001, (
            f"Expected {expected_velocity}, got {weights_arg.velocity_multiplier}"
        )

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
            generated_at=datetime.utcnow() - timedelta(days=65),
        )
        self.repo.get_unprocessed_done_actions.return_value = [old_action]
        self.repo.get_weights.return_value = {
            400: _make_weights(accuracy_ema=old_accuracy, cost_multiplier=old_cost, velocity_multiplier=old_velocity)
        }
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=400)]

        # No realized sale (slow mover)
        self.realized.get_realized_profit_for_type.return_value = None

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

        # velocity_multiplier IS updated: 0.8 * 1.0 + 0.2 * (7.0 / 60.0)
        expected_velocity = 0.8 * old_velocity + 0.2 * (7.0 / slow_mover_timeout)
        assert abs(weights_arg.velocity_multiplier - expected_velocity) < 0.001, (
            f"Expected velocity_multiplier={expected_velocity}, got {weights_arg.velocity_multiplier}"
        )

    def test_processed_for_feedback_marked_true(self):
        """After processing, action is marked processed_for_feedback=True."""
        action = _make_action(type_id=500, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=500)]
        self.repo.get_weights.return_value = {500: _make_weights()}
        self.realized.get_realized_profit_for_type.return_value = {
            "isk_per_hour": 10_000_000.0,
            "material_cost": 5_000_000.0,
            "sell_days": 7.0,
        }

        self.processor.process_pending_feedback()

        self.repo.mark_action_feedback_processed.assert_called_once_with(action.id)

    def test_non_manufacture_actions_marked_processed_but_not_counted(self):
        """deliver / relist actions are marked processed but not counted in return value."""
        deliver = _make_action(type_id=600, action_type="deliver")
        manufacture = _make_action(type_id=601, action_type="manufacture")
        self.repo.get_unprocessed_done_actions.return_value = [deliver, manufacture]
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=601)]
        self.repo.get_weights.return_value = {601: _make_weights()}
        self.realized.get_realized_profit_for_type.return_value = {
            "isk_per_hour": 10_000_000.0,
            "material_cost": 5_000_000.0,
            "sell_days": 7.0,
        }

        result = self.processor.process_pending_feedback()

        # Only manufacture actions counted
        assert result == 1
        # Both should be marked processed
        assert self.repo.mark_action_feedback_processed.call_count == 2

    def test_action_not_yet_slow_mover_skipped_without_realized_sale(self):
        """Action generated recently (within timeout) with no sale → not processed as slow_mover."""
        recent_action = _make_action(
            type_id=700,
            action_type="manufacture",
            generated_at=datetime.utcnow() - timedelta(days=5),  # only 5 days old, timeout=60
        )
        self.repo.get_unprocessed_done_actions.return_value = [recent_action]
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=700)]
        self.repo.get_weights.return_value = {700: _make_weights()}
        self.realized.get_realized_profit_for_type.return_value = None  # no sale yet

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
        self.realized.get_realized_profit_for_type.return_value = {
            "isk_per_hour": 10_000_000.0,
            "material_cost": 5_000_000.0,
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
        self.realized.get_realized_profit_for_type.return_value = {
            "isk_per_hour": 10_000_000.0,
            "material_cost": 5_000_000.0,
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
        self.realized.get_realized_profit_for_type.return_value = {
            "isk_per_hour": 10_000_000.0,
            "material_cost": 5_000_000.0,
            "sell_days": 7.0,
        }

        self.processor.process_pending_feedback()

        weights_arg = self.repo.upsert_weights.call_args[0][0]
        assert weights_arg.confidence_tier == "high", f"Got {weights_arg.confidence_tier}"
