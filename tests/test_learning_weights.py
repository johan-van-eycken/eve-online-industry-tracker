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
    weight_warnings_once_per_compute,
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


def _corrupt_warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records
            if r.levelno == logging.WARNING and "not a finite number" in r.getMessage()]


def test_a_corrupt_weight_warns_once_per_compute_however_many_readers(caplog):
    """FU-04: the scorer, the pipeline analyzer and the feedback processor each
    read the same row; inside one compute its corrupt weight warns once."""
    w = SimpleNamespace(type_id=7, cost_multiplier=float("nan"), accuracy_ema="abc")
    with caplog.at_level(logging.WARNING):
        with weight_warnings_once_per_compute():
            for _ in range(3):
                assert read_weight(w, "cost_multiplier") == 1.0
                assert read_weight(w, "accuracy_ema") == 1.0
    messages = _corrupt_warnings(caplog)
    assert len(messages) == 2
    assert any("cost_multiplier" in m for m in messages) and any("accuracy_ema" in m for m in messages)


def test_each_compute_warns_again_and_outside_a_compute_every_read_warns(caplog):
    w = SimpleNamespace(type_id=7, cost_multiplier=float("nan"))
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            with weight_warnings_once_per_compute():
                read_weight(w, "cost_multiplier")
                read_weight(w, "cost_multiplier")
        read_weight(w, "cost_multiplier")
        read_weight(w, "cost_multiplier")
    assert len(_corrupt_warnings(caplog)) == 4
