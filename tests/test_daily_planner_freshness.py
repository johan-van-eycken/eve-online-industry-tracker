from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.service import (
    compute_freshness_score,
)


def _item(type_id, snapshot):
    return SimpleNamespace(type_id=type_id, snapshot_sell_price=snapshot)


def test_no_drift_scores_one():
    items = [_item(1, 100.0), _item(2, 200.0)]
    depth = {1: {"spot_sell_price": 100.0}, 2: {"spot_sell_price": 200.0}}
    assert compute_freshness_score(items, depth, 5.0) == 1.0


def test_all_items_drifted_scores_zero():
    items = [_item(1, 100.0), _item(2, 200.0)]
    depth = {1: {"spot_sell_price": 150.0}, 2: {"spot_sell_price": 100.0}}
    assert compute_freshness_score(items, depth, 5.0) == 0.0


def test_half_drifted_scores_one_half():
    items = [_item(1, 100.0), _item(2, 200.0)]
    depth = {1: {"spot_sell_price": 100.0}, 2: {"spot_sell_price": 300.0}}
    assert compute_freshness_score(items, depth, 5.0) == 0.5


def test_drift_just_under_the_threshold_is_still_fresh():
    items = [_item(1, 100.0)]
    assert compute_freshness_score(items, {1: {"spot_sell_price": 104.9}}, 5.0) == 1.0


def test_drift_just_over_the_threshold_is_stale():
    items = [_item(1, 100.0)]
    assert compute_freshness_score(items, {1: {"spot_sell_price": 105.1}}, 5.0) == 0.0


def test_drift_is_symmetric_for_price_drops():
    items = [_item(1, 100.0)]
    assert compute_freshness_score(items, {1: {"spot_sell_price": 94.9}}, 5.0) == 0.0


def test_items_without_a_comparable_price_are_excluded_not_counted_stale():
    items = [_item(1, 100.0), _item(2, None), _item(3, 300.0)]
    depth = {1: {"spot_sell_price": 100.0}}  # 3 has no current price
    assert compute_freshness_score(items, depth, 5.0) == 1.0


def test_no_comparable_items_scores_one():
    assert compute_freshness_score([_item(1, None)], {}, 5.0) == 1.0
    assert compute_freshness_score([], {}, 5.0) == 1.0


def test_a_zero_snapshot_price_is_not_divided_by():
    assert compute_freshness_score([_item(1, 0.0)], {1: {"spot_sell_price": 50.0}}, 5.0) == 1.0


def test_the_model_has_a_snapshot_price_column():
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    assert "snapshot_sell_price" in BuildPlanItemModel.__table__.columns
