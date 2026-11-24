"""resize_sub_components(scale=...): a parent's cut batch shrinks its sub-component requests."""
from __future__ import annotations

import pytest

from test_chain_planner_keying import _SUB_PHASE1, _batch_materials, _decision, _planner, _row


def _plan():
    row = _row(materials=_batch_materials(**{"54321": 100}))
    return _planner().plan_chain([_decision(row)], _SUB_PHASE1)


def _requested(subs):
    (sub,) = subs
    return sub.overview_row["quantity_requested"]


def test_scale_shrinks_the_parents_request_rounded_up():
    plan = _plan()
    assert _requested(plan.resize_sub_components(set())) == 100
    assert _requested(plan.resize_sub_components(set(), scale={12345: 0.3})) == 30
    assert _requested(plan.resize_sub_components(set(), scale={12345: 0.333})) == 34
    # The stored requests are not mutated: an unscaled re-size is back at 100.
    assert _requested(plan.resize_sub_components(set())) == 100


def test_unscaled_parents_are_unchanged():
    plan = _plan()
    assert _requested(plan.resize_sub_components(set(), scale={99999: 0.1})) == 100


def test_scale_over_one_grows_the_request_for_the_parallel_jobs():
    """First job + parallel jobs (2026-11-24: 14 Mobile Large Warp Disruptor I
    bought for the second MLWD II job while the first job's were sub-built)."""
    assert _requested(_plan().resize_sub_components(set(), scale={12345: 1.5})) == 150


@pytest.mark.parametrize("bad", [0, -0.5])
def test_a_scale_not_above_zero_raises_naming_the_parent(bad):
    with pytest.raises(ValueError, match="12345"):
        _plan().resize_sub_components(set(), scale={12345: bad})
