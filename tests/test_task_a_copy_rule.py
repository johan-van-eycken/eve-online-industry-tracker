"""Task A item 5: how many copies one copy job may make.

The verified EVE rule (see character_assigner.max_copies_per_job): runs per
copy at most the blueprint's maxProductionLimit (CCP support, "Copying"),
copies per job limited by the job's duration: copies x runs per copy x the
copy time per run at most 30 days, at least one copy. The earlier cap
(copies <= maxProductionLimit) stays on top, conservatively. Fake numbers.
"""
from __future__ import annotations

import copy

import test_daily_planner_assigned_inventor as A  # noqa: E402
import test_blueprint_ledger as L  # noqa: E402

from eve_online_industry_tracker.application.daily_planner.character_assigner import (  # noqa: E402
    MAX_JOB_SECONDS,
    _shape_copy_jobs,
    max_copies_per_job,
)

DAY = 24 * 3600


def test_the_30_day_job_limit_caps_the_copies_per_job():
    # 10 runs per copy at 1 day per run: 3 copies fill 30 days.
    assert max_copies_per_job(10, DAY, 600) == 3


def test_the_max_production_limit_still_caps_the_copies_when_it_is_lower():
    # 1 run per copy at 240 s: 10800 copies by duration, 600 by the old cap.
    assert max_copies_per_job(1, 240, 600) == 600


def test_one_copy_is_always_allowed_even_beyond_30_days():
    assert max_copies_per_job(10, 4 * DAY, 30) == 1


def test_an_unknown_copy_time_keeps_the_old_cap_only():
    assert max_copies_per_job(10, None, 30) == 30
    assert max_copies_per_job(10, None, None) is None


def test_a_ship_blueprint_copy_job_no_longer_exceeds_30_days():
    """The SDE's Rifter-like blueprint: max 30 runs per copy, 4800 s copy
    time per run. 30 copies x 30 runs (900 runs) would take 50 days; 30 days
    hold 540 copied runs, so 18 copies of 30 runs."""
    assert MAX_JOB_SECONDS // (30 * 4800) == 18
    (job,) = _shape_copy_jobs(30 * 30, [30] * 30, 1, 30, 4800)
    assert job == {"copies": 18, "runs_per_copy": 30, "short": 12}


def test_without_a_copy_time_the_shape_is_unchanged():
    (job,) = _shape_copy_jobs(30 * 30, [30] * 30, 1, 30)
    assert job == {"copies": 30, "runs_per_copy": 30, "short": 0}


def test_the_copy_action_follows_the_duration_cap_and_says_what_is_left():
    """One invent lane of 4 attempts, at most 2 runs per copy: 2 copies of 2
    runs (the old cap allows 2 copies). A copied run takes 10 days, so one
    copy of 2 runs is 20 days and a second would pass 30: one job makes 1
    copy, 1 waits for a later plan."""
    row = A._row(t1_bpo=True)
    row["t1_max_runs_per_copy"] = 2
    row["t1_copy_seconds_per_run"] = 10 * DAY
    (copy_action,) = [a for a in A._assign(row, [A._best_inventor(), A._copier()]) if a.action_type == "copy"]
    assert (copy_action.quantity, copy_action.runs) == (1, 2)
    assert "make 1 copy × 2 runs" in copy_action.notes
    assert "more copies are needed than one job makes: 1 more in a later plan" in copy_action.notes
    assert "copy time is unknown" not in copy_action.notes


def test_one_long_copy_is_still_planned():
    """4 runs per copy allowed, 10 days a run: one copy of 4 runs is 40
    days, which EVE allows for a single-run job."""
    row = A._row(t1_bpo=True)
    row["t1_max_runs_per_copy"] = 30
    row["t1_copy_seconds_per_run"] = 10 * DAY
    (copy_action,) = [a for a in A._assign(row, [A._best_inventor(), A._copier()]) if a.action_type == "copy"]
    assert (copy_action.quantity, copy_action.runs) == (1, 4)
    assert "more copies are needed" not in copy_action.notes


def test_an_unknown_copy_time_is_said_on_the_copy_action():
    row = A._row(t1_bpo=True)
    row["t1_max_runs_per_copy"] = 30
    (copy_action,) = [a for a in A._assign(row, [A._best_inventor(), A._copier()]) if a.action_type == "copy"]
    assert "the SDE copy time is unknown: copies per job not checked against the 30-day job limit" in copy_action.notes


def test_the_chain_planner_publishes_the_sde_copy_time():
    bp = copy.deepcopy(L.BP_B)
    bp[998]["copying"] = 4800
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    L._run([t2], {998: [L._bpo(77, 998)]}, {}, blueprints=bp)
    assert t2.overview_row["t1_copy_seconds_per_run"] == 4800
    assert t2.overview_row["t1_max_runs_per_copy"] == 30


def test_the_chain_planner_leaves_an_unknown_copy_time_unknown():
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    L._run([t2], {998: [L._bpo(77, 998)]}, {})
    assert t2.overview_row["t1_copy_seconds_per_run"] is None
