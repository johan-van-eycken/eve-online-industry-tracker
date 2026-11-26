from __future__ import annotations

import multiprocessing
import os
import threading
import time

from flask_app.background_jobs import stop_background_jobs
from flask_app.deps import get_state

# The launcher restarts Flask when it exits with this code.
RESTART_EXIT_CODE = 75


def request_restart(*, delay_seconds: float = 0.5) -> bool:
    """Exit with RESTART_EXIT_CODE after `delay_seconds` (so the response goes out first).
    False, and nothing happens, without a launcher (no parent process) to restart Flask."""
    if multiprocessing.parent_process() is None:
        return False

    def _exit() -> None:
        time.sleep(delay_seconds)
        try:
            stop_background_jobs(get_state())
        finally:
            os._exit(RESTART_EXIT_CODE)

    threading.Thread(target=_exit, daemon=True, name="restart").start()
    return True
