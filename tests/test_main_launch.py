import sys

import main


class _FakeFlask:
    def __init__(self):
        self.terminated = False
        self.killed = False
        self._alive = True

    def is_alive(self):
        return self._alive

    def terminate(self):
        self.terminated = True
        self._alive = False

    def join(self, timeout=None):
        pass

    def kill(self):
        self.killed = True


def test_run_streamlit_uses_running_interpreter(monkeypatch):
    calls = []
    monkeypatch.setattr(main.subprocess, "Popen", lambda argv, **kw: calls.append(argv) or object())
    main.run_streamlit()
    assert calls == [[sys.executable, "-m", "streamlit", "run", "streamlit_app.py"]]


def test_flask_is_stopped_when_streamlit_fails_to_start(monkeypatch):
    fake = _FakeFlask()
    monkeypatch.setattr(main, "configure_logging", lambda **kw: None)
    monkeypatch.setattr(main.multiprocessing, "Process", lambda target: type("P", (), {
        "start": lambda s: None, "is_alive": fake.is_alive, "terminate": fake.terminate,
        "join": fake.join, "kill": fake.kill})())
    monkeypatch.setattr(main, "wait_for_flask_ready", lambda **kw: True)

    def boom():
        raise FileNotFoundError("streamlit")

    monkeypatch.setattr(main, "run_streamlit", boom)
    main.main()
    assert fake.terminated
