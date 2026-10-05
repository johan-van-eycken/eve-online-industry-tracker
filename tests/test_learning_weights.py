from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.daily_planner.learning_weights import (
    LEARNING_WEIGHT_MAX,
    LEARNING_WEIGHT_MIN,
    NEUTRAL_WEIGHT,
    clamp_weight,
    read_weight,
)


def test_the_band_is_unchanged():
    assert (LEARNING_WEIGHT_MIN, LEARNING_WEIGHT_MAX, NEUTRAL_WEIGHT) == (0.25, 4.0, 1.0)
    assert clamp_weight(9.0) == 4.0
    assert clamp_weight(0.1) == 0.25


def test_no_weights_row_reads_as_neutral():
    assert read_weight(None, "accuracy_ema") == 1.0


@pytest.mark.parametrize("stored, expected", [
    (6.8, 4.0), (0.0, 0.25), (-2.0, 0.25), (1.3, 1.3), (2, 2.0),
])
def test_a_stored_weight_is_clamped_on_read(stored, expected):
    w = SimpleNamespace(type_id=1, velocity_multiplier=stored)
    assert read_weight(w, "velocity_multiplier") == expected


@pytest.mark.parametrize("stored", [
    float("nan"), float("inf"), float("-inf"), "abc", "1.5", None, True,
])
def test_a_non_numeric_stored_weight_is_neutral_and_warned(stored, caplog):
    w = SimpleNamespace(type_id=7, cost_multiplier=stored)
    with caplog.at_level(logging.WARNING):
        assert read_weight(w, "cost_multiplier") == 1.0
    assert any(
        "cost_multiplier" in r.getMessage() and "type_id=7" in r.getMessage()
        for r in caplog.records if r.levelno == logging.WARNING
    )


def test_no_weights_row_reads_as_neutral_without_logging(caplog):
    """No row is the normal case for a new item. Only a corrupt stored value warns."""
    with caplog.at_level(logging.WARNING):
        assert read_weight(None, "accuracy_ema") == 1.0
    assert caplog.records == []
