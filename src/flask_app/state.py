from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any, Optional

from datetime import datetime

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

    # Background refresh tracking
    public_structures_refresh_lock: threading.Lock = field(default_factory=threading.Lock)
    public_structures_refreshing_system_ids: set[int] = field(default_factory=set)

    public_structures_last_refresh_started_at: Optional[datetime] = None
    public_structures_last_refresh_finished_at: Optional[datetime] = None
    public_structures_last_refresh_error: Optional[str] = None
    public_structures_last_refresh_system_id: Optional[int] = None
    public_structures_last_refresh_facilities_count: Optional[int] = None
    public_structures_last_refresh_structure_facilities_count: Optional[int] = None
    public_structures_last_refresh_rows_written: Optional[int] = None
    
    # Per-system throttling for refresh attempts.
    public_structures_last_refresh_by_system: dict[int, datetime] = field(default_factory=dict)

    # Global background scan (startup)
    public_structures_global_scan_lock: threading.Lock = field(default_factory=threading.Lock)
    public_structures_global_scan_running: bool = False
    public_structures_global_scan_started_at: Optional[datetime] = None
    public_structures_global_scan_last_heartbeat_at: Optional[datetime] = None
    public_structures_global_scan_finished_at: Optional[datetime] = None
    public_structures_global_scan_error: Optional[str] = None
    public_structures_global_scan_total_ids: Optional[int] = None
    public_structures_global_scan_cursor: Optional[int] = None
    public_structures_global_scan_attempted: int = 0
    public_structures_global_scan_rows_written: int = 0


state = AppState()
