"""MarketIntelligenceJob.start
sets _running only once the thread is registered, so a failed registration
leaves the job startable again instead of "running" with no thread; without
the Flask package (ImportError) the thread still starts. Fake state."""
from __future__ import annotations

import sys
import threading
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.market_intelligence.job import MarketIntelligenceJob


def _job(ran):
    job = MarketIntelligenceJob(state=SimpleNamespace(admin_settings=None))
    job._loop = lambda: ran.set()   # no real cycle
    return job


def test_a_failed_registration_leaves_the_job_not_running(monkeypatch):
    from flask_app import background_jobs

    def _fail(state, name, thread):
        raise AttributeError("fake state without background_threads_lock")

    monkeypatch.setattr(background_jobs, "register_thread", _fail)
    ran = threading.Event()
    job = _job(ran)
    with pytest.raises(AttributeError):
        job.start()
    assert job.get_status()["running"] is False
    assert not ran.wait(0.05)                                   # no orphan thread started

    registered = {}
    monkeypatch.setattr(background_jobs, "register_thread",
                        lambda state, name, thread: registered.setdefault(name, thread))
    job.start()                                                 # startable again
    assert ran.wait(1.0)
    assert MarketIntelligenceJob.thread_name in registered


def test_without_flask_the_thread_still_starts(monkeypatch):
    monkeypatch.setitem(sys.modules, "flask_app.background_jobs", None)   # import raises ImportError
    ran = threading.Event()
    job = _job(ran)
    job.start()
    assert ran.wait(1.0)


def test_start_is_idempotent(monkeypatch):
    from flask_app import background_jobs

    calls = []
    monkeypatch.setattr(background_jobs, "register_thread", lambda state, name, thread: calls.append(name))
    release = threading.Event()
    job = MarketIntelligenceJob(state=SimpleNamespace(admin_settings=None))
    job._loop = lambda: release.wait(1.0)
    job.start()
    job.start()
    release.set()
    assert calls == [MarketIntelligenceJob.thread_name]
