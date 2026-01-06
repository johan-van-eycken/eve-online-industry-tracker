from __future__ import annotations

import logging
import threading

from flask_app.state import state

# App initialization imports
from utils.app_init import (
    load_config,
    init_db_managers,
    init_char_manager,
    init_corp_manager,
)

from classes.esi_service import ESIService


def initialize_application(*, refresh_metadata: bool = True) -> None:
    """Perform heavy initialization.

    Runs DB + ESI refreshes, then wires up adapters.
    """
    try:
        state.init_state = "Starting Initialization"

        logging.info("Loading config...")
        state.init_state = "Loading Config"
        state.cfg_manager = load_config()

        logging.info("Initializing databases...")
        state.init_state = "Initializing Databases"
        state.db_oauth, state.db_app, state.db_sde = init_db_managers(
            state.cfg_manager,
            refresh_metadata=refresh_metadata,
        )

        logging.info("Initializing characters...")
        state.init_state = "Initializing Characters"
        state.char_manager = init_char_manager(
            state.cfg_manager, state.db_oauth, state.db_app, state.db_sde
        )
        state.char_manager.refresh_all()

        logging.info("Initializing corporations...")
        state.init_state = "Initializing Corporations"
        state.corp_manager = init_corp_manager(
            state.cfg_manager,
            state.db_oauth,
            state.db_app,
            state.db_sde,
            state.char_manager,
        )
        state.corp_manager.refresh_all()

        logging.info("Initializing data adapters...")
        state.init_state = "Initializing Data Adapters"
        main_character = state.char_manager.get_main_character()
        state.esi_service = ESIService(main_character.esi_client)
        # Adapters no longer keep module-level globals; they read from state + request-scoped sessions.

        chars_initialized = len(state.char_manager._character_list)
        corps_initialized = len(state.corp_manager._corporation_ids)
        logging.info("All done. Characters: %s, Corporations: %s", chars_initialized, corps_initialized)

        state.init_state = "Ready"
        state.init_error = None
    except Exception as e:
        state.init_error = str(e)
        logging.error("Failed to initialize application: %s", e, exc_info=True)
        state.init_state = f"Initialization Failed at step: {state.init_state}"


def start_background_initialization(*, refresh_metadata: bool = True) -> None:
    """Start initialization in a background thread (idempotent)."""
    with state.init_lock:
        if state.init_started:
            return
        state.init_started = True

    t = threading.Thread(
        target=initialize_application,
        kwargs={"refresh_metadata": refresh_metadata},
        daemon=True,
        name="app-initializer",
    )
    t.start()


def require_ready() -> None:
    if state.init_state != "Ready":
        raise RuntimeError(f"Application not ready: {state.init_state}")
