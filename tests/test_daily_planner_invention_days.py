"""Invention jobs packed to the daily check (2026-11-24): four 1-attempt jobs
took four science slots for a day each, where one job fits before the next
11:00 check. Fake data."""
from __future__ import annotations

from eve_online_industry_tracker.application.daily_planner.assigned_invention import AssignedInvention
from eve_online_industry_tracker.application.daily_planner.character_assigner import _pack_invention_lanes

HOUR = 3600
CHECKS = [21.5 * HOUR, 45.5 * HOUR, 69.5 * HOUR]   # planned at 13:30, checks at 11:00


def _lane(char_id, bpc_runs, attempts=1, cost=80_000.0, item=1):
    inv = AssignedInvention(probability=0.279, best_probability=0.279, expected_attempts=float(attempts),
                            whole_attempts=attempts, cost_isk=cost * attempts, note=f"lane {item} of 4")
    return (char_id, inv, ("key", item, bpc_runs))


def test_four_one_attempt_lanes_become_one_job_until_the_next_check():
    lanes = [_lane(1, 75, item=1), _lane(1, 100, item=2), _lane(2, 100, item=3), _lane(2, 100, item=4)]
    (char_id, inv, bpc) = _pack_invention_lanes(lanes, 4, HOUR, CHECKS)[0:1][0]
    packed = _pack_invention_lanes(lanes, 4, HOUR, CHECKS)
    assert len(packed) == 1
    assert inv.whole_attempts == 21                       # filled up to the first check (21.5 h)
    assert inv.cost_isk == 80_000.0 * 21
    assert bpc[2] == 100                                   # the largest BPC
    assert "lane 1 of 1" in inv.note and "ends before a daily check" in inv.note


def test_a_need_beyond_the_next_check_runs_until_a_later_check():
    (_, inv, _), = _pack_invention_lanes([_lane(1, 200)], 30, HOUR, CHECKS)
    assert inv.whole_attempts == 45                        # the second check, filled


def test_a_bpc_too_small_spreads_over_more_jobs():
    lanes = [_lane(1, 10, item=1), _lane(2, 8, item=2), _lane(2, 5, item=3)]
    packed = _pack_invention_lanes(lanes, 15, HOUR, CHECKS)
    assert [inv.whole_attempts for _c, inv, _b in packed] == [10, 8]


def test_without_an_attempt_time_the_lanes_stay():
    lanes = [_lane(1, 75), _lane(2, 100)]
    assert _pack_invention_lanes(lanes, 2, None, CHECKS) == lanes
