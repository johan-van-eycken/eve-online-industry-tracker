from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any, Optional

from datetime import datetime

import threading


@dataclass
class RuntimeState:
    cfg_manager: Any = None
    db_oauth: Any = None
    db_app: Any = None
    db_sde: Any = None
    char_manager: Any = None
    corp_manager: Any = None
    esi_service: Any = None


@dataclass
class CacheState:
    materials_cache: Any = None
    # (cached_at_epoch_seconds, cache_version, payload)
    structure_rigs_cache: Optional[tuple[float, int, list[dict[str, Any]]]] = None


@dataclass
class PublicStructuresJobState:
    # Background refresh tracking
    refresh_lock: threading.Lock = field(default_factory=threading.Lock)
    refreshing_system_ids: set[int] = field(default_factory=set)

    last_refresh_started_at: Optional[datetime] = None
    last_refresh_finished_at: Optional[datetime] = None
    last_refresh_error: Optional[str] = None
    last_refresh_system_id: Optional[int] = None
    last_refresh_facilities_count: Optional[int] = None
    last_refresh_structure_facilities_count: Optional[int] = None
    last_refresh_rows_written: Optional[int] = None

    # Per-system throttling for refresh attempts.
    last_refresh_by_system: dict[int, datetime] = field(default_factory=dict)

    # Global background scan
    global_scan_lock: threading.Lock = field(default_factory=threading.Lock)
    global_scan_running: bool = False
    global_scan_started_at: Optional[datetime] = None
    global_scan_last_heartbeat_at: Optional[datetime] = None
    global_scan_finished_at: Optional[datetime] = None
    global_scan_error: Optional[str] = None
    global_scan_total_ids: Optional[int] = None
    global_scan_cursor: Optional[int] = None
    global_scan_attempted: int = 0
    global_scan_rows_written: int = 0

    # Stop just the scan (without shutting down the whole app)
    global_scan_stop_event: threading.Event = field(default_factory=threading.Event)


@dataclass
class IndustryBuilderJobState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    # job_id -> job dict (status/progress/result)
    jobs: dict[str, dict] = field(default_factory=dict)
    # key (character/profile/maximize_runs) -> job_id (latest)
    jobs_by_key: dict[str, str] = field(default_factory=dict)


@dataclass
class JobsState:
    public_structures: PublicStructuresJobState = field(default_factory=PublicStructuresJobState)
    industry_builder: IndustryBuilderJobState = field(default_factory=IndustryBuilderJobState)


@dataclass
class AppState:
    # Lifecycle
    init_state: str = "Not Started"
    init_error: Optional[str] = None
    init_warnings: list[str] = field(default_factory=list)
    init_lock: threading.Lock = field(default_factory=threading.Lock)
    init_started: bool = False

    # Background job lifecycle
    shutdown_event: threading.Event = field(default_factory=threading.Event)
    background_threads_lock: threading.Lock = field(default_factory=threading.Lock)
    background_threads: dict[str, threading.Thread] = field(default_factory=dict)

    # Nested state
    runtime: RuntimeState = field(default_factory=RuntimeState)
    jobs: JobsState = field(default_factory=JobsState)
    caches: CacheState = field(default_factory=CacheState)

    # --- Legacy attribute access (compat) ---
    @property
    def cfg_manager(self) -> Any:  # noqa: D401
        return self.runtime.cfg_manager

    @cfg_manager.setter
    def cfg_manager(self, v: Any) -> None:
        self.runtime.cfg_manager = v

    @property
    def db_oauth(self) -> Any:
        return self.runtime.db_oauth

    @db_oauth.setter
    def db_oauth(self, v: Any) -> None:
        self.runtime.db_oauth = v

    @property
    def db_app(self) -> Any:
        return self.runtime.db_app

    @db_app.setter
    def db_app(self, v: Any) -> None:
        self.runtime.db_app = v

    @property
    def db_sde(self) -> Any:
        return self.runtime.db_sde

    @db_sde.setter
    def db_sde(self, v: Any) -> None:
        self.runtime.db_sde = v

    @property
    def char_manager(self) -> Any:
        return self.runtime.char_manager

    @char_manager.setter
    def char_manager(self, v: Any) -> None:
        self.runtime.char_manager = v

    @property
    def corp_manager(self) -> Any:
        return self.runtime.corp_manager

    @corp_manager.setter
    def corp_manager(self, v: Any) -> None:
        self.runtime.corp_manager = v

    @property
    def esi_service(self) -> Any:
        return self.runtime.esi_service

    @esi_service.setter
    def esi_service(self, v: Any) -> None:
        self.runtime.esi_service = v

    @property
    def materials_cache(self) -> Any:
        return self.caches.materials_cache

    @materials_cache.setter
    def materials_cache(self, v: Any) -> None:
        self.caches.materials_cache = v

    @property
    def _structure_rigs_cache(self) -> Optional[tuple[float, int, list[dict[str, Any]]]]:
        return self.caches.structure_rigs_cache

    @_structure_rigs_cache.setter
    def _structure_rigs_cache(self, v: Optional[tuple[float, int, list[dict[str, Any]]]]) -> None:
        self.caches.structure_rigs_cache = v

    # Public structures job legacy fields
    @property
    def public_structures_refresh_lock(self) -> threading.Lock:
        return self.jobs.public_structures.refresh_lock

    @property
    def public_structures_refreshing_system_ids(self) -> set[int]:
        return self.jobs.public_structures.refreshing_system_ids

    @property
    def public_structures_last_refresh_started_at(self) -> Optional[datetime]:
        return self.jobs.public_structures.last_refresh_started_at

    @public_structures_last_refresh_started_at.setter
    def public_structures_last_refresh_started_at(self, v: Optional[datetime]) -> None:
        self.jobs.public_structures.last_refresh_started_at = v

    @property
    def public_structures_last_refresh_finished_at(self) -> Optional[datetime]:
        return self.jobs.public_structures.last_refresh_finished_at

    @public_structures_last_refresh_finished_at.setter
    def public_structures_last_refresh_finished_at(self, v: Optional[datetime]) -> None:
        self.jobs.public_structures.last_refresh_finished_at = v

    @property
    def public_structures_last_refresh_error(self) -> Optional[str]:
        return self.jobs.public_structures.last_refresh_error

    @public_structures_last_refresh_error.setter
    def public_structures_last_refresh_error(self, v: Optional[str]) -> None:
        self.jobs.public_structures.last_refresh_error = v

    @property
    def public_structures_last_refresh_system_id(self) -> Optional[int]:
        return self.jobs.public_structures.last_refresh_system_id

    @public_structures_last_refresh_system_id.setter
    def public_structures_last_refresh_system_id(self, v: Optional[int]) -> None:
        self.jobs.public_structures.last_refresh_system_id = v

    @property
    def public_structures_last_refresh_facilities_count(self) -> Optional[int]:
        return self.jobs.public_structures.last_refresh_facilities_count

    @public_structures_last_refresh_facilities_count.setter
    def public_structures_last_refresh_facilities_count(self, v: Optional[int]) -> None:
        self.jobs.public_structures.last_refresh_facilities_count = v

    @property
    def public_structures_last_refresh_structure_facilities_count(self) -> Optional[int]:
        return self.jobs.public_structures.last_refresh_structure_facilities_count

    @public_structures_last_refresh_structure_facilities_count.setter
    def public_structures_last_refresh_structure_facilities_count(self, v: Optional[int]) -> None:
        self.jobs.public_structures.last_refresh_structure_facilities_count = v

    @property
    def public_structures_last_refresh_rows_written(self) -> Optional[int]:
        return self.jobs.public_structures.last_refresh_rows_written

    @public_structures_last_refresh_rows_written.setter
    def public_structures_last_refresh_rows_written(self, v: Optional[int]) -> None:
        self.jobs.public_structures.last_refresh_rows_written = v

    @property
    def public_structures_last_refresh_by_system(self) -> dict[int, datetime]:
        return self.jobs.public_structures.last_refresh_by_system

    @property
    def public_structures_global_scan_lock(self) -> threading.Lock:
        return self.jobs.public_structures.global_scan_lock

    @property
    def public_structures_global_scan_running(self) -> bool:
        return self.jobs.public_structures.global_scan_running

    @public_structures_global_scan_running.setter
    def public_structures_global_scan_running(self, v: bool) -> None:
        self.jobs.public_structures.global_scan_running = v

    @property
    def public_structures_global_scan_started_at(self) -> Optional[datetime]:
        return self.jobs.public_structures.global_scan_started_at

    @public_structures_global_scan_started_at.setter
    def public_structures_global_scan_started_at(self, v: Optional[datetime]) -> None:
        self.jobs.public_structures.global_scan_started_at = v

    @property
    def public_structures_global_scan_last_heartbeat_at(self) -> Optional[datetime]:
        return self.jobs.public_structures.global_scan_last_heartbeat_at

    @public_structures_global_scan_last_heartbeat_at.setter
    def public_structures_global_scan_last_heartbeat_at(self, v: Optional[datetime]) -> None:
        self.jobs.public_structures.global_scan_last_heartbeat_at = v

    @property
    def public_structures_global_scan_finished_at(self) -> Optional[datetime]:
        return self.jobs.public_structures.global_scan_finished_at

    @public_structures_global_scan_finished_at.setter
    def public_structures_global_scan_finished_at(self, v: Optional[datetime]) -> None:
        self.jobs.public_structures.global_scan_finished_at = v

    @property
    def public_structures_global_scan_error(self) -> Optional[str]:
        return self.jobs.public_structures.global_scan_error

    @public_structures_global_scan_error.setter
    def public_structures_global_scan_error(self, v: Optional[str]) -> None:
        self.jobs.public_structures.global_scan_error = v

    @property
    def public_structures_global_scan_total_ids(self) -> Optional[int]:
        return self.jobs.public_structures.global_scan_total_ids

    @public_structures_global_scan_total_ids.setter
    def public_structures_global_scan_total_ids(self, v: Optional[int]) -> None:
        self.jobs.public_structures.global_scan_total_ids = v

    @property
    def public_structures_global_scan_cursor(self) -> Optional[int]:
        return self.jobs.public_structures.global_scan_cursor

    @public_structures_global_scan_cursor.setter
    def public_structures_global_scan_cursor(self, v: Optional[int]) -> None:
        self.jobs.public_structures.global_scan_cursor = v

    @property
    def public_structures_global_scan_attempted(self) -> int:
        return self.jobs.public_structures.global_scan_attempted

    @public_structures_global_scan_attempted.setter
    def public_structures_global_scan_attempted(self, v: int) -> None:
        self.jobs.public_structures.global_scan_attempted = v

    @property
    def public_structures_global_scan_rows_written(self) -> int:
        return self.jobs.public_structures.global_scan_rows_written

    @public_structures_global_scan_rows_written.setter
    def public_structures_global_scan_rows_written(self, v: int) -> None:
        self.jobs.public_structures.global_scan_rows_written = v

    @property
    def public_structures_global_scan_stop_event(self) -> threading.Event:
        return self.jobs.public_structures.global_scan_stop_event

    # Industry builder job legacy fields
    @property
    def industry_builder_jobs_lock(self) -> threading.Lock:
        return self.jobs.industry_builder.lock

    @property
    def industry_builder_jobs(self) -> dict[str, dict]:
        return self.jobs.industry_builder.jobs

    @property
    def industry_builder_jobs_by_key(self) -> dict[str, str]:
        return self.jobs.industry_builder.jobs_by_key


state = AppState()
