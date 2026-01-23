from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
from typing import Any

from eve_online_industry_tracker.db_models import PublicStructuresModel, PublicStructuresScanStateModel

from eve_online_industry_tracker.infrastructure.persistence import public_structures_repo, public_structures_scan_state_repo
from eve_online_industry_tracker.infrastructure.public_structures_scan_job import (
    PublicStructuresGlobalScanJob,
    PublicStructuresScanConfig,
)


def _ensure_table_exists(session) -> None:
    try:
        engine = session.get_bind()
        PublicStructuresModel.__table__.create(bind=engine, checkfirst=True)
    except Exception:
        pass


def _ensure_scan_state_table_exists(session) -> None:
    try:
        engine = session.get_bind()
        # Imported at module level via eve_online_industry_tracker.db_models

        PublicStructuresScanStateModel.__table__.create(bind=engine, checkfirst=True)
    except Exception:
        pass


def get_cached_public_structures(*, state: Any, system_id: int, ttl_seconds: int) -> tuple[list[dict], bool]:
    """Return cached structures for a system + whether the cache is fresh."""

    if state.db_app is None:
        raise RuntimeError("db_app not initialized")

    session = state.db_app.Session()
    try:
        _ensure_table_exists(session)
        newer_than = datetime.utcnow() - timedelta(seconds=ttl_seconds)
        fresh_rows = public_structures_repo.list_by_system_id(session, system_id, newer_than=newer_than)
        rows = fresh_rows or public_structures_repo.list_by_system_id(session, system_id)

        payload = [
            {
                "station_id": int(r.structure_id),
                "station_name": r.structure_name,
                "system_id": int(r.system_id),
                "owner_id": r.owner_id,
                "type_id": r.type_id,
                "services": r.services,
            }
            for r in rows
        ]
        return payload, bool(fresh_rows)
    finally:
        try:
            session.close()
        except Exception:
            pass


def refresh_public_structures_for_system(
    *,
    state: Any,
    system_id: int,
    scan_cap: int = 250,
    max_workers: int = 5,
    time_budget_seconds: float = 8.0,
    max_results: int = 50,
) -> int:
    """Fetch public structures from ESI (bounded/time-budgeted) and upsert into db_app."""

    if state.db_app is None or state.esi_service is None:
        raise RuntimeError("db_app or esi_service not initialized")

    state.public_structures_last_refresh_started_at = datetime.utcnow()
    state.public_structures_last_refresh_finished_at = None
    state.public_structures_last_refresh_error = None
    state.public_structures_last_refresh_system_id = system_id
    state.public_structures_last_refresh_facilities_count = None
    state.public_structures_last_refresh_structure_facilities_count = None
    state.public_structures_last_refresh_rows_written = None

    fetched = state.esi_service.get_public_structures(
        system_id=system_id,
        filter=None,
        max_structure_ids_to_scan=int(scan_cap),
        max_workers=int(max_workers),
        time_budget_seconds=float(time_budget_seconds),
        max_results=int(max_results),
    )

    state.public_structures_last_refresh_facilities_count = len(fetched or [])
    state.public_structures_last_refresh_structure_facilities_count = len(fetched or [])

    session = state.db_app.Session()
    try:
        _ensure_table_exists(session)
        public_structures_repo.delete_by_system_id(session, system_id)
        written = public_structures_repo.upsert_many(
            session,
            [
                {
                    "structure_id": s.get("station_id"),
                    "system_id": s.get("system_id"),
                    "owner_id": s.get("owner_id"),
                    "type_id": s.get("type_id"),
                    "structure_name": s.get("station_name"),
                    "services": s.get("services"),
                }
                for s in (fetched or [])
                if isinstance(s, dict) and s.get("station_id") is not None and s.get("system_id") is not None
            ],
        )

        state.public_structures_last_refresh_rows_written = written
        return written
    except Exception as e:
        state.public_structures_last_refresh_error = str(e)
        raise
    finally:
        state.public_structures_last_refresh_finished_at = datetime.utcnow()
        state.public_structures_last_refresh_by_system[system_id] = state.public_structures_last_refresh_finished_at
        try:
            session.close()
        except Exception:
            pass


def trigger_refresh_public_structures_for_system(
    *,
    state: Any,
    system_id: int,
    scan_cap: int = 250,
    max_workers: int = 5,
    time_budget_seconds: float = 8.0,
    max_results: int = 50,
    cooldown_seconds: float = 60.0,
) -> bool:
    """Spawn a background refresh for a system if not already running.

    Returns True if a refresh was started, False if already in progress.
    """

    stop_event = getattr(state, "shutdown_event", None)
    if stop_event is not None and getattr(stop_event, "is_set", lambda: False)():
        return False

    now = datetime.utcnow()
    last = state.public_structures_last_refresh_by_system.get(system_id)
    if last is not None and (now - last).total_seconds() < float(cooldown_seconds):
        return False

    with state.public_structures_refresh_lock:
        if system_id in state.public_structures_refreshing_system_ids:
            return False
        state.public_structures_refreshing_system_ids.add(system_id)

    def _run() -> None:
        try:
            if stop_event is not None and stop_event.is_set():
                return
            refresh_public_structures_for_system(
                state=state,
                system_id=system_id,
                scan_cap=scan_cap,
                max_workers=max_workers,
                time_budget_seconds=time_budget_seconds,
                max_results=max_results,
            )
        except Exception:
            pass
        finally:
            with state.public_structures_refresh_lock:
                state.public_structures_refreshing_system_ids.discard(system_id)

    threading.Thread(target=_run, daemon=True, name=f"public-structures-refresh-{system_id}").start()
    return True


def trigger_global_public_structures_scan(
    *,
    state: Any,
    scan_cap: int = 5000,
    max_workers: int = 10,
    time_budget_seconds: float = 60.0,
    batch_size: int = 100,
    pause_seconds: float = 5.0,
    stop_event: threading.Event | None = None,
    request_timeout_seconds: float = 5.0,
) -> bool:
    """Start a background global scan that tries to populate public_structures for all accessible structures.

    Backward-compatible wrapper around `PublicStructuresGlobalScanJob`.
    """

    # `stop_event` is kept for API compatibility but is no longer used; the job
    # consults shutdown + scan stop events from state.
    _ = stop_event

    job = getattr(state, "_public_structures_global_scan_job", None)
    if job is None:
        job = PublicStructuresGlobalScanJob()
        try:
            setattr(state, "_public_structures_global_scan_job", job)
        except Exception:
            pass

    cfg = PublicStructuresScanConfig(
        scan_cap=int(scan_cap),
        max_workers=int(max_workers),
        time_budget_seconds=float(time_budget_seconds),
        batch_size=int(batch_size),
        pause_seconds=float(pause_seconds),
        request_timeout_seconds=float(request_timeout_seconds),
    )
    return bool(job.start(state=state, config=cfg))


def stop_global_public_structures_scan(*, state: Any) -> bool:
    """Signal the currently running global scan (if any) to stop.

    Returns True if a stop signal was sent, False if there's no stop event available.
    """

    job = getattr(state, "_public_structures_global_scan_job", None)
    if job is None:
        job = PublicStructuresGlobalScanJob()
        try:
            setattr(state, "_public_structures_global_scan_job", job)
        except Exception:
            pass
    return bool(job.stop(state=state))


def _persist_global_cursor(*, state: Any, cursor: int) -> None:
    if state.db_app is None:
        return
    session = state.db_app.Session()
    try:
        _ensure_scan_state_table_exists(session)
        public_structures_scan_state_repo.set_cursor(session, int(cursor))
    finally:
        try:
            session.close()
        except Exception:
            pass


def _commit_global_batch(*, state: Any, rows: list[dict], new_cursor: int) -> None:
    if state.db_app is None:
        return
    session = state.db_app.Session()
    try:
        _ensure_table_exists(session)
        _ensure_scan_state_table_exists(session)
        public_structures_repo.upsert_many(session, rows)
        public_structures_scan_state_repo.set_cursor(session, int(new_cursor))
    finally:
        try:
            session.close()
        except Exception:
            pass


def _global_scan_loop(
    *,
    state: Any,
    scan_cap: int,
    max_workers: int,
    time_budget_seconds: float,
    batch_size: int,
    pause_seconds: float,
    stop_event: threading.Event | None,
    request_timeout_seconds: float,
) -> None:
    if state.db_app is None or state.esi_service is None:
        return

    structure_ids = state.esi_service.list_universe_structure_ids(filter=None)
    total = len(structure_ids)
    state.public_structures_global_scan_total_ids = total
    if total == 0:
        return

    session = state.db_app.Session()
    try:
        _ensure_table_exists(session)
        _ensure_scan_state_table_exists(session)
        cursor = public_structures_scan_state_repo.get_cursor(session)
    finally:
        session.close()

    if cursor < 0 or cursor >= total:
        cursor = 0

    max_workers = max(1, int(max_workers))
    batch_size = max(1, int(batch_size))
    scan_cap = max(1, int(scan_cap))

    ex: ThreadPoolExecutor | None = None
    try:
        ex = ThreadPoolExecutor(max_workers=max_workers)

        while True:
            if stop_event is not None and stop_event.is_set():
                try:
                    ex.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
                return

            state.public_structures_global_scan_cursor = int(cursor)
            state.public_structures_global_scan_last_heartbeat_at = datetime.utcnow()

            slice_start = time.time()
            slice_end_cursor = min(total, cursor + scan_cap)
            ids_slice = structure_ids[cursor:slice_end_cursor]
            if not ids_slice:
                session = state.db_app.Session()
                try:
                    _ensure_scan_state_table_exists(session)
                    public_structures_scan_state_repo.mark_completed(session)
                finally:
                    session.close()
                logging.info("Global public structures scan completed a full pass (total_ids=%s).", total)
                return

            def fetch_one(structure_id: int):
                try:
                    data = state.esi_service.get_universe_structure(
                        int(structure_id),
                        timeout_seconds=float(request_timeout_seconds),
                    )
                    if not isinstance(data, dict):
                        return None
                    val = data.get("solar_system_id")
                    if isinstance(val, (int, str)):
                        try:
                            system_id = int(val)
                        except Exception:
                            system_id = None
                    else:
                        system_id = None
                    return {
                        "structure_id": int(structure_id),
                        "system_id": system_id,
                        "owner_id": data.get("owner_id"),
                        "type_id": data.get("type_id"),
                        "structure_name": data.get("name"),
                        "services": data.get("services"),
                    }
                except Exception:
                    return None

            rows_batch: list[dict] = []
            attempted_in_slice = 0

            it = iter(ids_slice)
            in_flight = set()

            for _ in range(max_workers):
                try:
                    sid = next(it)
                except StopIteration:
                    break
                in_flight.add(ex.submit(fetch_one, sid))

            while in_flight:
                if stop_event is not None and stop_event.is_set():
                    # Cancel what we can and exit promptly.
                    try:
                        for fut in in_flight:
                            fut.cancel()
                        ex.shutdown(wait=False, cancel_futures=True)
                    except Exception:
                        pass
                    return

                remaining = time_budget_seconds - (time.time() - slice_start)
                if remaining <= 0:
                    break

                done, in_flight = wait(
                    in_flight,
                    timeout=min(0.25, remaining),
                    return_when=FIRST_COMPLETED,
                )

                for fut in done:
                    attempted_in_slice += 1
                    state.public_structures_global_scan_attempted += 1
                    try:
                        item = fut.result()
                    except Exception:
                        item = None
                    if item and item.get("structure_id") and item.get("system_id"):
                        rows_batch.append(item)
                    if len(rows_batch) >= batch_size:
                        _commit_global_batch(state=state, rows=rows_batch, new_cursor=cursor + attempted_in_slice)
                        state.public_structures_global_scan_rows_written += len(rows_batch)
                        rows_batch = []

                    try:
                        sid = next(it)
                    except StopIteration:
                        continue
                    in_flight.add(ex.submit(fetch_one, sid))

            # If we broke due to time budget, cancel pending work so we don't block shutdown
            # or waste requests after we've moved on.
            if in_flight:
                try:
                    for fut in in_flight:
                        fut.cancel()
                except Exception:
                    pass

            if rows_batch:
                _commit_global_batch(state=state, rows=rows_batch, new_cursor=cursor + attempted_in_slice)
                state.public_structures_global_scan_rows_written += len(rows_batch)

            cursor = min(total, cursor + attempted_in_slice)
            _persist_global_cursor(state=state, cursor=cursor)

            if pause_seconds > 0:
                if stop_event is not None and stop_event.is_set():
                    return
                time.sleep(pause_seconds)

            if attempted_in_slice == 0:
                if stop_event is not None and stop_event.is_set():
                    return
                time.sleep(max(1.0, pause_seconds))
    finally:
        if ex is not None:
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
