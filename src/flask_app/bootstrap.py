from __future__ import annotations

import logging
import threading

from flask_app.deps import get_state
from flask_app.state import AppState
from flask_app.background_jobs import register_thread, stop_background_jobs
from flask_app.settings import (
    public_structures_startup_scan_batch_size,
    public_structures_startup_scan_enabled,
    public_structures_esi_request_timeout_seconds,
    public_structures_startup_scan_max_workers,
    public_structures_startup_scan_pause_seconds,
    public_structures_startup_scan_scan_cap,
    public_structures_startup_scan_time_budget_seconds,
)

# App initialization imports
from utils.app_init import (
    load_config,
    init_db_managers,
    init_char_manager,
    init_corp_manager,
)

from eve_online_industry_tracker.esi_service import ESIService

from eve_online_industry_tracker.infrastructure.public_structures_cache_service import trigger_global_public_structures_scan


def _register_thread(app_state: AppState, name: str, thread: threading.Thread) -> None:
    register_thread(app_state, name, thread)


def initialize_application(app_state: AppState | None = None, *, refresh_metadata: bool = True) -> None:
    """Perform heavy initialization.

    Runs DB + ESI refreshes, then wires up adapters.
    """
    state = app_state or get_state()
    try:
        if getattr(state, "shutdown_event", None) is not None and state.shutdown_event.is_set():
            state.init_state = "Shutdown"
            return

        state.init_state = "Starting Initialization"

        logging.info("Loading config...")
        state.init_state = "Loading Config"
        state.cfg_manager = load_config()

        if getattr(state, "shutdown_event", None) is not None and state.shutdown_event.is_set():
            state.init_state = "Shutdown"
            return

        logging.info("Initializing databases...")
        state.init_state = "Initializing Databases"
        state.db_oauth, state.db_app, state.db_sde = init_db_managers(
            state.cfg_manager,
            refresh_metadata=refresh_metadata,
        )

        if getattr(state, "shutdown_event", None) is not None and state.shutdown_event.is_set():
            state.init_state = "Shutdown"
            return

        logging.info("Initializing characters...")
        state.init_state = "Initializing Characters"
        state.char_manager = init_char_manager(
            state.cfg_manager, state.db_oauth, state.db_app, state.db_sde
        )
        try:
            state.char_manager.refresh_all()
        except Exception as e:
            try:
                state.init_warnings.append(f"Character refresh failed: {e}")
            except Exception:
                pass
            logging.warning("Character refresh failed; continuing startup: %s", e, exc_info=True)

        if getattr(state, "shutdown_event", None) is not None and state.shutdown_event.is_set():
            state.init_state = "Shutdown"
            return

        logging.info("Initializing corporations...")
        state.init_state = "Initializing Corporations"
        state.corp_manager = init_corp_manager(
            state.cfg_manager,
            state.db_oauth,
            state.db_app,
            state.db_sde,
            state.char_manager,
        )
        try:
            state.corp_manager.refresh_all()
        except Exception as e:
            try:
                state.init_warnings.append(f"Corporation refresh failed: {e}")
            except Exception:
                pass
            logging.warning("Corporation refresh failed; continuing startup: %s", e, exc_info=True)

        if getattr(state, "shutdown_event", None) is not None and state.shutdown_event.is_set():
            state.init_state = "Shutdown"
            return

        logging.info("Initializing data adapters...")
        state.init_state = "Initializing Data Adapters"
        main_character = state.char_manager.get_main_character()
        state.esi_service = ESIService(main_character.esi_client)
        # Adapters no longer keep module-level globals; they read from state + request-scoped sessions.

        # Start background global scan to populate public_structures. This is best-effort and
        # bounded by conservative limits so it doesn't block readiness.
        if public_structures_startup_scan_enabled():
            trigger_global_public_structures_scan(
                state=state,
                scan_cap=public_structures_startup_scan_scan_cap(),
                max_workers=public_structures_startup_scan_max_workers(),
                time_budget_seconds=float(public_structures_startup_scan_time_budget_seconds()),
                batch_size=public_structures_startup_scan_batch_size(),
                pause_seconds=float(public_structures_startup_scan_pause_seconds()),
                request_timeout_seconds=float(public_structures_esi_request_timeout_seconds()),
            )

        chars_initialized = len(state.char_manager._character_list)
        corps_initialized = len(state.corp_manager._corporation_ids)
        logging.info("All done. Characters: %s, Corporations: %s", chars_initialized, corps_initialized)

        state.init_state = "Ready"
        state.init_error = None
    except Exception as e:
        state.init_error = str(e)
        logging.error("Failed to initialize application: %s", e, exc_info=True)
        state.init_state = f"Initialization Failed at step: {state.init_state}"


def start_background_initialization(app_state: AppState | None = None, *, refresh_metadata: bool = True) -> None:
    """Start initialization in a background thread (idempotent)."""
    state = app_state or get_state()
    if getattr(state, "shutdown_event", None) is not None and state.shutdown_event.is_set():
        return

    with state.init_lock:
        if state.init_started:
            return
        state.init_started = True

    t = threading.Thread(
        target=initialize_application,
        kwargs={"app_state": state, "refresh_metadata": refresh_metadata},
        daemon=True,
        name="app-initializer",
    )
    _register_thread(state, "app-initializer", t)
    t.start()


def require_ready(app_state: AppState | None = None) -> None:
    s = app_state or get_state()
    if s.init_state != "Ready":
        raise RuntimeError(f"Application not ready: {s.init_state}")


def require_sde_ready(app_state: AppState | None = None) -> None:
    s = app_state or get_state()
    if getattr(s, "db_sde", None) is None:
        raise RuntimeError("SDE DB not initialized")
