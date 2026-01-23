from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class PublicStructuresScanConfig:
    scan_cap: int = 5000
    max_workers: int = 10
    time_budget_seconds: float = 60.0
    batch_size: int = 100
    pause_seconds: float = 5.0
    request_timeout_seconds: float = 5.0


class PublicStructuresGlobalScanJob:
    """Owns lifecycle of the background global public-structures scan.

    Metrics are still stored on `state` for backward compatibility.
    """

    thread_name = "public-structures-global-scan"

    def start(self, *, state: Any, config: PublicStructuresScanConfig) -> bool:
        if getattr(state, "db_app", None) is None or getattr(state, "esi_service", None) is None:
            return False

        # Clear any previous stop signal.
        try:
            ev = getattr(state, "public_structures_global_scan_stop_event", None)
            if ev is not None:
                ev.clear()
        except Exception:
            pass

        stop_event = _CompositeStopEvent(
            getattr(state, "shutdown_event", None),
            getattr(state, "public_structures_global_scan_stop_event", None),
        )

        if stop_event.is_set():
            return False

        with state.public_structures_global_scan_lock:
            if state.public_structures_global_scan_running:
                return False
            state.public_structures_global_scan_running = True
            state.public_structures_global_scan_started_at = datetime.utcnow()
            state.public_structures_global_scan_finished_at = None
            state.public_structures_global_scan_error = None
            state.public_structures_global_scan_attempted = 0
            state.public_structures_global_scan_rows_written = 0

        def _run() -> None:
            try:
                # Local import to avoid circular dependency.
                from eve_online_industry_tracker.infrastructure.public_structures_cache_service import _global_scan_loop

                _global_scan_loop(
                    state=state,
                    scan_cap=int(config.scan_cap),
                    max_workers=int(config.max_workers),
                    time_budget_seconds=float(config.time_budget_seconds),
                    batch_size=int(config.batch_size),
                    pause_seconds=float(config.pause_seconds),
                    stop_event=stop_event,
                    request_timeout_seconds=float(config.request_timeout_seconds),
                )
            except Exception as e:
                state.public_structures_global_scan_error = str(e)
                logging.warning("Global public structures scan failed: %s", e, exc_info=True)
            finally:
                with state.public_structures_global_scan_lock:
                    state.public_structures_global_scan_running = False
                    state.public_structures_global_scan_finished_at = datetime.utcnow()

        t = threading.Thread(target=_run, daemon=True, name=self.thread_name)
        self._register_thread(state, t)
        t.start()
        return True

    def stop(self, *, state: Any) -> bool:
        ev = getattr(state, "public_structures_global_scan_stop_event", None)
        if ev is None:
            return False
        try:
            ev.set()
            return True
        except Exception:
            return False

    def _register_thread(self, state: Any, thread: threading.Thread) -> None:
        # Best-effort: if state supports the registry, record this thread.
        try:
            lock = getattr(state, "background_threads_lock", None)
            threads = getattr(state, "background_threads", None)
            if lock is not None and threads is not None:
                with lock:
                    threads[self.thread_name] = thread
        except Exception:
            pass


class _CompositeStopEvent:
    def __init__(self, *events: threading.Event | None):
        self._events = [e for e in events if e is not None]

    def is_set(self) -> bool:
        for e in self._events:
            try:
                if e.is_set():
                    return True
            except Exception:
                continue
        return False
