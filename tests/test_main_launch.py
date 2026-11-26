import sys

import pytest

from eve_online_industry_tracker import cli as main


class _FakeFlask:
    def __init__(self, stops_on_terminate=True):
        self.stops_on_terminate = stops_on_terminate
        self.terminated = False
        self.killed = False
        self._alive = True
        self.exitcode = None

    def is_alive(self):
        return self._alive

    def terminate(self):
        self.terminated = True
        self._alive = not self.stops_on_terminate

    def join(self, timeout=None):
        pass

    def kill(self):
        self.killed = True
        self._alive = False


def test_run_streamlit_uses_running_interpreter(monkeypatch):
    calls = []
    monkeypatch.setattr(main.subprocess, "Popen", lambda argv, **kw: calls.append(argv) or object())
    main.run_streamlit()
    assert calls == [[sys.executable, "-m", "streamlit", "run", "streamlit_app.py",
                      "--server.showEmailPrompt=false"]]


def test_wait_for_flask_ready_stops_on_failed_init(monkeypatch):
    class _Resp:
        status_code = 503
        headers = {"content-type": "application/json"}
        def json(self):
            return {"init_state": "Initialization Failed at step: Loading Config"}
    monkeypatch.setattr(main.requests, "get", lambda *a, **kw: _Resp())
    monkeypatch.setattr(main.time, "sleep", lambda s: pytest.fail("kept polling after a failed init"))
    with pytest.raises(RuntimeError, match="Loading Config"):
        main.wait_for_flask_ready(timeout=300)


def test_init_db_sde_names_a_missing_sde(tmp_path):
    from utils.app_init import init_db_sde
    missing = tmp_path / "eve_sde.db"
    cfg = type("C", (), {"all": lambda s: {"app": {"database_sde_uri": f"sqlite:///{missing}", "language": "en"}}})()
    with pytest.raises(FileNotFoundError, match="SDE not imported yet"):
        init_db_sde(cfg)
    assert not missing.exists()


def _patch_flask(monkeypatch, fake):
    monkeypatch.setattr(main, "configure_logging", lambda **kw: None)
    monkeypatch.setattr(main.multiprocessing, "Process", lambda target: type("P", (), {
        "start": lambda s: None, "is_alive": fake.is_alive, "terminate": fake.terminate,
        "join": fake.join, "kill": fake.kill,
        "exitcode": property(lambda s: fake.exitcode)})())
    monkeypatch.setattr(main, "wait_for_flask_ready", lambda **kw: True)


def test_flask_is_stopped_when_streamlit_fails_to_start(monkeypatch):
    fake = _FakeFlask()
    _patch_flask(monkeypatch, fake)

    def boom():
        raise FileNotFoundError("streamlit")

    monkeypatch.setattr(main, "run_streamlit", boom)
    with pytest.raises(SystemExit) as exc:
        main.main([])
    assert exc.value.code == 1
    assert fake.terminated


def test_flask_is_killed_when_terminate_does_not_stop_it(monkeypatch):
    fake = _FakeFlask(stops_on_terminate=False)
    _patch_flask(monkeypatch, fake)
    monkeypatch.setattr(main, "run_streamlit", lambda: (_ for _ in ()).throw(FileNotFoundError("streamlit")))
    with pytest.raises(SystemExit) as exc:
        main.main([])
    assert exc.value.code == 1
    assert fake.terminated and fake.killed


class _DeadStreamlit:
    def poll(self):
        return 1


def test_streamlit_relaunch_is_limited_and_flask_is_reaped(monkeypatch):
    fake = _FakeFlask()
    _patch_flask(monkeypatch, fake)
    monkeypatch.setattr(main.time, "sleep", lambda s: None)
    launches = []
    monkeypatch.setattr(main, "run_streamlit", lambda: launches.append(1) or _DeadStreamlit())
    with pytest.raises(SystemExit) as exc:
        main.main([])
    assert exc.value.code == 1
    # The first launch plus one relaunch per failure short of the limit.
    assert len(launches) == main.STREAMLIT_MAX_CONSECUTIVE_FAILURES
    assert fake.terminated


def test_slow_streamlit_exits_do_not_count_as_failures(monkeypatch):
    fake = _FakeFlask()
    _patch_flask(monkeypatch, fake)
    clock = {"t": 0.0}
    monkeypatch.setattr(main.time, "monotonic", lambda: clock.__setitem__("t", clock["t"] + 1000) or clock["t"])
    monkeypatch.setattr(main.time, "sleep", lambda s: None)
    launches = []

    def launch():
        launches.append(1)
        if len(launches) > 20:
            raise KeyboardInterrupt
        return _DeadStreamlit()

    monkeypatch.setattr(main, "run_streamlit", launch)
    # The KeyboardInterrupt path POSTs /shutdown to Flask's real port; never touch it.
    monkeypatch.setattr(main.requests, "post", lambda *a, **kw: None)
    main.main([])
    assert len(launches) > main.STREAMLIT_MAX_CONSECUTIVE_FAILURES


class _LiveStreamlit:
    def __init__(self):
        self.terminated = False

    def poll(self):
        return None if not self.terminated else 0

    def terminate(self):
        self.terminated = True


def test_giving_up_on_flask_exits_non_zero_after_cleanup(monkeypatch):
    fake = _FakeFlask()
    _patch_flask(monkeypatch, fake)
    fake._alive = False  # Flask dies on every check and never comes back
    ready_calls = []

    def ready(**kw):
        ready_calls.append(1)
        if len(ready_calls) > 1:
            raise RuntimeError("Flask failed to become ready")
        return True

    monkeypatch.setattr(main, "wait_for_flask_ready", ready)
    monkeypatch.setattr(main.time, "sleep", lambda s: None)
    streamlit = _LiveStreamlit()
    monkeypatch.setattr(main, "run_streamlit", lambda: streamlit)
    with pytest.raises(SystemExit) as exc:
        main.main([])
    assert exc.value.code == 1
    assert streamlit.terminated


def test_ctrl_c_shutdown_still_exits_normally(monkeypatch):
    fake = _FakeFlask()
    _patch_flask(monkeypatch, fake)
    monkeypatch.setattr(main, "run_streamlit", lambda: (_ for _ in ()).throw(KeyboardInterrupt()))
    monkeypatch.setattr(main.requests, "post", lambda *a, **kw: None)
    main.main([])  # returns: exit status 0
    assert fake.terminated


def _run_one_flask_exit(monkeypatch, exitcode):
    """Flask dies once with `exitcode`, the relaunch stays up, Ctrl+C ends the run."""
    fake = _FakeFlask()
    _patch_flask(monkeypatch, fake)
    fake._alive = False
    fake.exitcode = exitcode
    sleeps = []

    calls = []

    def ready(**kw):
        calls.append(1)
        fake._alive = len(calls) > 1  # first call is the initial launch
        return True

    def sleep(s):
        sleeps.append(s)
        if len(sleeps) >= 4:
            raise KeyboardInterrupt

    monkeypatch.setattr(main, "wait_for_flask_ready", ready)
    monkeypatch.setattr(main.time, "sleep", sleep)
    monkeypatch.setattr(main.requests, "post", lambda *a, **kw: None)
    main.main(["--no-streamlit"])
    return sleeps


def test_exit_75_restarts_without_backoff(monkeypatch):
    # Dead on the first poll, relaunched with no backoff sleep: only the 5 s polls.
    assert _run_one_flask_exit(monkeypatch, 75) == [5, 5, 5, 5]


def test_other_exit_still_backs_off(monkeypatch):
    assert _run_one_flask_exit(monkeypatch, 1)[:2] == [5, 2]
