"""The overview refresh jobs: an identical start while one is
active joins it instead of starting a second recompute, even when two starts
race; finished jobs are pruned so a long session does not keep every result."""
from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.industry import overview_jobs
from eve_online_industry_tracker.application.industry.service import IndustryService
from flask_app.state import IndustryOverviewRefreshJobState

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
        )),
    )
    svc.started = started  # type: ignore[attr-defined]
    return svc


@pytest.mark.parametrize("start, store_name", [
    ("start_industry_manufacturing_product_overview_refresh", "industry_overview_refresh"),
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


