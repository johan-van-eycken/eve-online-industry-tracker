"""The overview and portfolio refresh jobs: an identical start while one is
active joins it instead of starting a second recompute, even when two starts
race; finished jobs are pruned so a long session does not keep every result."""
from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.industry import overview_jobs
from eve_online_industry_tracker.application.industry.service import IndustryService
from flask_app.state import IndustryOverviewRefreshJobState, IndustryPortfolioCandidatesJobState

_RealThread = threading.Thread  # the fixture replaces the service's Thread; the test's own threads stay real


@pytest.fixture
def service(monkeypatch):
    started: list[str] = []

    class _NoThread:
        def __init__(self, *, target, args, daemon, name):
            self.name = name

        def start(self):
            started.append(self.name)

    monkeypatch.setattr(overview_jobs.threading, "Thread", _NoThread)
    monkeypatch.setattr(overview_jobs, "register_thread", lambda state, name, thread: None)
    svc = IndustryService(
        state=SimpleNamespace(jobs=SimpleNamespace(
            industry_overview_refresh=IndustryOverviewRefreshJobState(),
            industry_portfolio_candidates=IndustryPortfolioCandidatesJobState(),
        )),
    )
    svc.started = started  # type: ignore[attr-defined]
    return svc


@pytest.mark.parametrize("start, store_name", [
    ("start_industry_manufacturing_product_overview_refresh", "industry_overview_refresh"),
    ("start_industry_manufacturing_portfolio_candidates_refresh", "industry_portfolio_candidates"),
])
def test_racing_identical_starts_run_one_job(service, start, store_name):
    barrier = threading.Barrier(8)
    job_ids: list[str] = []

    def click():
        barrier.wait()
        job_ids.append(getattr(service, start)()["job_id"])

    threads = [_RealThread(target=click) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(job_ids)) == 1
    assert len(service.started) == 1
    assert len(getattr(service._state.jobs, store_name).jobs) == 1


def test_finished_jobs_are_pruned_to_the_newest_few(service):
    store = service._state.jobs.industry_overview_refresh
    for i in range(10):
        store.jobs[f"done-{i}"] = {"job_id": f"done-{i}", "status": "completed", "request_params": {"i": i},
                                   "created_at": f"2026-01-01T00:00:{i:02d}+00:00", "result": [{}] * 100}
    store.jobs["running"] = {"job_id": "running", "status": "running", "request_params": {"x": 1},
                             "created_at": "2025-01-01T00:00:00+00:00"}

    new_id = service.start_industry_manufacturing_product_overview_refresh()["job_id"]

    finished = sorted(k for k, j in store.jobs.items() if j["status"] == "completed")
    assert finished == [f"done-{i}" for i in range(5, 10)]   # the newest five
    assert "running" in store.jobs and new_id in store.jobs  # active jobs are never pruned


def test_the_runner_passes_the_normalized_params_through(service):
    params = IndustryService._normalize_portfolio_candidates_params(include_reactions=True, planning_horizon_hours=6)
    job, _ = service._claim_job(service._state.jobs.industry_portfolio_candidates, params, step_count=11)
    seen: dict = {}

    def payload(**kw):
        seen.update(kw)
        return {"candidates": [{"id": 1}]}

    service.industry_manufacturing_portfolio_candidates_payload = payload
    service._run_portfolio_candidates_refresh_job(job["job_id"], params)
    assert {k: v for k, v in seen.items() if k != "progress_callback"} == params
    assert service.industry_manufacturing_portfolio_candidates_refresh_status(job_id=job["job_id"])["status"] == "completed"
