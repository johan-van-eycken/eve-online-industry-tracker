from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any

from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA, AdminSettingInvalid


class MarketIntelligenceJob:
    """Background job that collects market depth data every N hours.

    Starts immediately on bootstrap, then sleeps for the configured interval.
    Writes to market_depth_cache and margin_correlation_cache (every 24h).
    """

    thread_name = "market-intelligence-job"

    def __init__(self, *, state: Any):
        self._state = state
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._running = False
        self._last_completed_at: datetime | None = None
        self._last_error: str | None = None

    def start(self) -> None:
        """Start the background daemon thread. Idempotent."""
        with self._lock:
            if self._running:
                return
            self._running = True

        t = threading.Thread(target=self._loop, daemon=True, name=self.thread_name)
        # Register with app state background threads for graceful shutdown tracking
        try:
            from flask_app.background_jobs import register_thread
            register_thread(self._state, self.thread_name, t)
        except Exception:
            pass
        t.start()

    def stop(self) -> None:
        self._stop_event.set()

    def get_status(self) -> dict:
        with self._lock:
            return {
                "running": self._running,
                "last_completed_at": self._last_completed_at.isoformat() if self._last_completed_at else None,
                "last_error": self._last_error,
            }

    def _loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                self._run_cycle_safe()
                # Sleep for the configured interval, waking every 60s to check stop
                deadline = self._refresh_interval_hours() * 3600
                elapsed = 0.0
                while elapsed < deadline and not self._stop_event.is_set():
                    self._stop_event.wait(timeout=min(60.0, deadline - elapsed))
                    elapsed += 60.0
        finally:
            with self._lock:
                self._running = False

    def _refresh_interval_hours(self) -> float:
        """daily_planner/planner_market_refresh_interval_hours. The setting is
        startup-critical: a stored invalid value reads as the schema default
        (the user's choice), with the ERROR at load, a WARNING at first use,
        /health and the Admin Settings page. Any other failure to read it is
        logged as a WARNING and the schema default is used, so the loop keeps
        running; without a settings store the default is used."""
        default = float(ADMIN_SETTINGS_SCHEMA["daily_planner"]["settings"]["planner_market_refresh_interval_hours"]
                        ["default"])
        admin = getattr(self._state, "admin_settings", None)
        if admin is None:
            return default
        try:
            return float(admin.get("daily_planner", "planner_market_refresh_interval_hours"))
        except AdminSettingInvalid as exc:
            # Unreachable while the setting is startup_critical (get() then gives the default);
            # kept so that dropping the flag still logs and keeps the loop on the default.
            logging.warning("MarketIntelligenceJob: %s; using the default %g h", exc.message, default)
        except Exception as exc:
            logging.warning("MarketIntelligenceJob: reading daily_planner/planner_market_refresh_interval_hours "
                            "failed (%s); using the default %g h", exc, default)
        return default

    def _run_cycle_safe(self) -> None:
        try:
            self._run_cycle()
            with self._lock:
                self._last_completed_at = datetime.utcnow()
                self._last_error = None
        except Exception as e:
            logging.exception("MarketIntelligenceJob cycle failed: %s", e)
            with self._lock:
                self._last_error = str(e)

    def _run_cycle(self) -> None:
        from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import MarketDepthCollector
        from eve_online_industry_tracker.application.market_intelligence.margin_correlator import MarginCorrelator

        admin = getattr(self._state, "admin_settings", None)

        collector = MarketDepthCollector(state=self._state, admin_settings=admin)
        collector.run()

        correlator = MarginCorrelator(state=self._state, admin_settings=admin)
        correlator.run_if_due()
