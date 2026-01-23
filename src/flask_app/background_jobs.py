from __future__ import annotations

import time
from typing import TYPE_CHECKING

import threading

if TYPE_CHECKING:
    from flask_app.state import AppState


def register_thread(app_state: AppState, name: str, thread: threading.Thread) -> None:
    try:
        with app_state.background_threads_lock:
            app_state.background_threads[name] = thread
    except Exception:
        # Best-effort only.
        pass


def stop_background_jobs(app_state: AppState, *, join_timeout_seconds: float = 2.0) -> None:
    """Signal background jobs to stop and best-effort join them."""

    try:
        app_state.shutdown_event.set()
    except Exception:
        return

    # Snapshot threads to avoid holding the lock during joins.
    threads: dict[str, threading.Thread] = {}
    try:
        with app_state.background_threads_lock:
            threads = dict(app_state.background_threads)
    except Exception:
        threads = {}

    deadline = time.time() + float(join_timeout_seconds)
    for _, t in threads.items():
        if not t:
            continue
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        try:
            t.join(timeout=remaining)
        except Exception:
            pass


def stop_public_structures_scan(app_state: AppState) -> bool:
    """Signal the global public-structures scan to stop."""

    try:
        app_state.public_structures_global_scan_stop_event.set()
    except Exception:
        return False
    return True


def clear_public_structures_scan_stop(app_state: AppState) -> bool:
    """Clear the global public-structures scan stop signal."""

    try:
        app_state.public_structures_global_scan_stop_event.clear()
    except Exception:
        return False
    return True
