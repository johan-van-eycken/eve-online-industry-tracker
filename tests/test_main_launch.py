import sys

import main


class _FakeFlask:
    def __init__(self, stops_on_terminate=True):
        self.stops_on_terminate = stops_on_terminate
        self.terminated = False
        self.killed = False
        self._alive = True

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
    assert calls == [[sys.executable, "-m", "streamlit", "run", "streamlit_app.py"]]


def _patch_flask(monkeypatch, fake):
    monkeypatch.setattr(main, "configure_logging", lambda **kw: None)
    monkeypatch.setattr(main.multiprocessing, "Process", lambda target: type("P", (), {
        "start": lambda s: None, "is_alive": fake.is_alive, "terminate": fake.terminate,
        "join": fake.join, "kill": fake.kill})())
    monkeypatch.setattr(main, "wait_for_flask_ready", lambda **kw: True)


def test_flask_is_stopped_when_streamlit_fails_to_start(monkeypatch):
    fake = _FakeFlask()
    _patch_flask(monkeypatch, fake)

    def boom():
        raise FileNotFoundError("streamlit")

    monkeypatch.setattr(main, "run_streamlit", boom)
    main.main()
    assert fake.terminated


def test_flask_is_killed_when_terminate_does_not_stop_it(monkeypatch):
    fake = _FakeFlask(stops_on_terminate=False)
    _patch_flask(monkeypatch, fake)
    monkeypatch.setattr(main, "run_streamlit", lambda: (_ for _ in ()).throw(FileNotFoundError("streamlit")))
    main.main()
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
    main.main()
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
    main.main()
    assert len(launches) > main.STREAMLIT_MAX_CONSECUTIVE_FAILURES
