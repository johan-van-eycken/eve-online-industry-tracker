from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import threading


@dataclass
class AppState:
    # Lifecycle
    init_state: str = "Not Started"
    init_error: Optional[str] = None
    init_lock: threading.Lock = threading.Lock()
    init_started: bool = False

    # Managers/services (set during initialization)
    cfg_manager: Any = None
    db_oauth: Any = None
    db_app: Any = None
    db_sde: Any = None
    char_manager: Any = None
    corp_manager: Any = None
    esi_service: Any = None

    # Simple in-memory caches (used by routes)
    materials_cache: Any = None


state = AppState()
