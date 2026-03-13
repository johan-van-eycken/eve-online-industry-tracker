from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import logging
import threading
from typing import Any
import uuid

from flask_app.background_jobs import register_thread

from eve_online_industry_tracker.infrastructure.session_provider import (
    SessionProvider,
    StateSessionProvider,
)
from eve_online_industry_tracker.infrastructure.sde.blueprints import get_blueprint_manufacturing_data


class IndustryJobManager:
    """Background manager for industry-planning state.

    Today it maintains a cached SDE blueprint overview and the queue structure that
    future manufacturing, reaction, copying, research, and invention planning will use.
    """

    _THREAD_NAME = "industry-job-manager"
    _SNAPSHOT_REFRESH_INTERVAL_SECONDS = 6 * 3600

    def _excluded_blueprint_type_ids(self) -> set[int]:
        cfg_manager = getattr(self._state, "cfg_manager", None)
        if cfg_manager is None:
            return set()

        try:
            cfg = cfg_manager.all() or {}
        except Exception:
            return set()

        raw_ids = (((cfg.get("defaults") or {}).get("industry") or {}).get("excluded_blueprint_type_ids") or [])
        excluded: set[int] = set()
        for raw_id in raw_ids:
            try:
                excluded.add(int(raw_id))
            except Exception:
                continue
        return excluded

    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)

        self._snapshot_lock = threading.Lock()
        self._queue_lock = threading.Lock()
        self._thread_lock = threading.Lock()
        self._refresh_requested = threading.Event()

        self._thread: threading.Thread | None = None
        self._blueprint_overview: list[dict[str, Any]] = []
        self._last_snapshot_at: datetime | None = None
        self._last_refresh_started_at: datetime | None = None
        self._last_refresh_finished_at: datetime | None = None
        self._last_refresh_error: str | None = None

        self._job_queues: dict[str, deque[dict[str, Any]]] = {
            "manufacturing": deque(),
            "reaction": deque(),
            "copying": deque(),
            "research_material": deque(),
            "research_time": deque(),
            "invention": deque(),
        }

    def start(self) -> None:
        with self._thread_lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self._thread = threading.Thread(
                target=self._run,
                daemon=True,
                name=self._THREAD_NAME,
            )
            register_thread(self._state, self._THREAD_NAME, self._thread)
            self._thread.start()

    def request_refresh(self) -> None:
        self._refresh_requested.set()

    def get_blueprint_overview(self, *, force_refresh: bool = False) -> list[dict[str, Any]]:
        with self._snapshot_lock:
            has_snapshot = bool(self._blueprint_overview)
            snapshot_at = self._last_snapshot_at

        if force_refresh or not has_snapshot:
            self._refresh_blueprint_overview()
        elif snapshot_at is None:
            self.request_refresh()
        else:
            age_seconds = (datetime.now(timezone.utc) - snapshot_at).total_seconds()
            if age_seconds >= self._SNAPSHOT_REFRESH_INTERVAL_SECONDS:
                self.request_refresh()

        with self._snapshot_lock:
            return [dict(row) for row in self._blueprint_overview]

    def get_status(self) -> dict[str, Any]:
        with self._snapshot_lock:
            snapshot_count = len(self._blueprint_overview)
            last_snapshot_at = self._last_snapshot_at
            last_refresh_started_at = self._last_refresh_started_at
            last_refresh_finished_at = self._last_refresh_finished_at
            last_refresh_error = self._last_refresh_error

        with self._queue_lock:
            queue_counts = {name: len(items) for name, items in self._job_queues.items()}

        return {
            "managed_activity_types": [
                "manufacturing",
                "reaction",
                "copying",
                "research_material",
                "research_time",
                "invention",
            ],
            "snapshot_count": snapshot_count,
            "last_snapshot_at": last_snapshot_at.isoformat() if last_snapshot_at else None,
            "last_refresh_started_at": last_refresh_started_at.isoformat() if last_refresh_started_at else None,
            "last_refresh_finished_at": last_refresh_finished_at.isoformat() if last_refresh_finished_at else None,
            "last_refresh_error": last_refresh_error,
            "queue_counts": queue_counts,
            "is_running": bool(self._thread and self._thread.is_alive()),
        }

    def enqueue_job(self, *, activity_type: str, payload: dict[str, Any]) -> str:
        activity_key = str(activity_type).strip().lower()
        if activity_key not in self._job_queues:
            raise ValueError(f"Unsupported activity type: {activity_type}")

        job_id = str(uuid.uuid4())
        job = {
            "id": job_id,
            "activity_type": activity_key,
            "payload": dict(payload or {}),
            "status": "queued",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        with self._queue_lock:
            self._job_queues[activity_key].append(job)

        return job_id

    def _run(self) -> None:
        self._refresh_requested.set()
        while not self._state.shutdown_event.is_set():
            requested = self._refresh_requested.wait(timeout=self._SNAPSHOT_REFRESH_INTERVAL_SECONDS)
            if self._state.shutdown_event.is_set():
                break

            if not requested:
                self._refresh_requested.set()

            self._refresh_requested.clear()
            try:
                self._refresh_blueprint_overview()
            except Exception as e:
                logging.warning("Industry job manager refresh failed: %s", str(e), exc_info=True)

    def _refresh_blueprint_overview(self) -> None:
        session = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"

        started_at = datetime.now(timezone.utc)
        with self._snapshot_lock:
            self._last_refresh_started_at = started_at

        try:
            raw_blueprints = get_blueprint_manufacturing_data(session, language)
            excluded_blueprint_type_ids = self._excluded_blueprint_type_ids()
            if excluded_blueprint_type_ids:
                raw_blueprints = {
                    blueprint_type_id: blueprint
                    for blueprint_type_id, blueprint in raw_blueprints.items()
                    if int(blueprint_type_id) not in excluded_blueprint_type_ids
                }
            overview_rows = self._build_blueprint_overview_rows(raw_blueprints)
        except Exception as e:
            with self._snapshot_lock:
                self._last_refresh_error = str(e)
                self._last_refresh_finished_at = datetime.now(timezone.utc)
            raise
        finally:
            try:
                session.close()
            except Exception:
                pass

        finished_at = datetime.now(timezone.utc)
        with self._snapshot_lock:
            self._blueprint_overview = overview_rows
            self._last_snapshot_at = finished_at
            self._last_refresh_finished_at = finished_at
            self._last_refresh_error = None

    @staticmethod
    def _join_type_names(entries: list[dict[str, Any]]) -> str:
        names = sorted(
            {
                str(entry.get("type_name") or "").strip()
                for entry in entries
                if str(entry.get("type_name") or "").strip()
            }
        )
        return ", ".join(names)

    @staticmethod
    def _join_group_names(entries: list[dict[str, Any]]) -> str:
        names = sorted(
            {
                str(entry.get("group_name") or "").strip()
                for entry in entries
                if str(entry.get("group_name") or "").strip()
            }
        )
        return ", ".join(names)

    @staticmethod
    def _join_category_names(entries: list[dict[str, Any]]) -> str:
        names = sorted(
            {
                str(entry.get("category_name") or "").strip()
                for entry in entries
                if str(entry.get("category_name") or "").strip()
            }
        )
        return ", ".join(names)

    @staticmethod
    def _join_skill_names(entries: list[dict[str, Any]]) -> str:
        names = [str(entry.get("type_name") or "").strip() for entry in entries if str(entry.get("type_name") or "").strip()]
        return ", ".join(names)

    @staticmethod
    def _build_invention_products(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for entry in entries:
            product = dict(entry)
            out.append(
                {
                    "probability_pct": float(product.get("probability") or 0.0) * 100.0,
                    "quantity": int(product.get("quantity") or 0),
                    "product": product,
                }
            )
        return out

    @staticmethod
    def _build_blueprint_overview_rows(raw_blueprints: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for blueprint in raw_blueprints.values():
            manufacturing = blueprint.get("manufacturing") or {}
            reaction = blueprint.get("reaction") or {}
            research_material = blueprint.get("research_material") or {}
            research_time = blueprint.get("research_time") or {}
            invention = blueprint.get("invention") or {}

            manufacturing_products = manufacturing.get("products") or []
            reaction_products = reaction.get("products") or []
            invention_products = invention.get("products") or []

            primary_product = manufacturing_products[0] if manufacturing_products else {}
            primary_reaction_product = reaction_products[0] if reaction_products else {}

            can_manufacture = bool(manufacturing_products)
            can_react = bool(reaction_products or int(reaction.get("time") or 0) > 0)
            can_copy = bool(int(blueprint.get("copying") or 0) > 0)
            can_research_material = bool(int(research_material.get("time") or 0) > 0)
            can_research_time = bool(int(research_time.get("time") or 0) > 0)
            can_invent = bool(invention_products or int(invention.get("time") or 0) > 0)

            manufacturing_materials = [dict(entry) for entry in (manufacturing.get("materials") or [])]
            manufacturing_skills = [dict(entry) for entry in (manufacturing.get("skills") or [])]
            manufacturing_products_out = [dict(entry) for entry in manufacturing_products]

            reaction_materials = [dict(entry) for entry in (reaction.get("materials") or [])]
            reaction_skills = [dict(entry) for entry in (reaction.get("skills") or [])]
            reaction_products_out = [dict(entry) for entry in reaction_products]

            invention_materials = [dict(entry) for entry in (invention.get("materials") or [])]
            invention_skills = [dict(entry) for entry in (invention.get("skills") or [])]
            invention_products_out = IndustryJobManager._build_invention_products(
                [dict(entry) for entry in invention_products]
            )

            row: dict[str, Any] = {
                "blueprint_type_id": blueprint.get("type_id"),
                "blueprint_name": blueprint.get("type_name") or "",
                "blueprint": dict(blueprint.get("blueprint") or {}),
                "can_manufacture": can_manufacture,
                "can_react": can_react,
                "can_copy": can_copy,
                "can_research_material": can_research_material,
                "can_research_time": can_research_time,
                "can_invent": can_invent,
            }

            if can_manufacture:
                row["manufacturing_job"] = {
                    "material_count": len(manufacturing_materials),
                    "materials": manufacturing_materials,
                    "skill_count": len(manufacturing_skills),
                    "skills": IndustryJobManager._join_skill_names(manufacturing_skills),
                    "skill_entries": manufacturing_skills,
                    "time_seconds": int(manufacturing.get("time") or 0),
                    "max_production_limit": int(blueprint.get("max_production_limit") or 0),
                    "products": manufacturing_products_out,
                }

            if can_react:
                row["reaction_job"] = {
                    "material_count": len(reaction_materials),
                    "materials": reaction_materials,
                    "skill_count": len(reaction_skills),
                    "skills": IndustryJobManager._join_skill_names(reaction_skills),
                    "skill_entries": reaction_skills,
                    "time_seconds": int(reaction.get("time") or 0),
                    "products": reaction_products_out,
                }

            if can_copy:
                row["copying_job"] = {
                    "time_seconds": int(blueprint.get("copying") or 0),
                }

            if can_research_material:
                row["research_material_job"] = {
                    "time_seconds": int(research_material.get("time") or 0),
                }

            if can_research_time:
                row["research_time_job"] = {
                    "time_seconds": int(research_time.get("time") or 0),
                }

            if can_invent:
                row["invention_job"] = {
                    "material_count": len(invention_materials),
                    "materials": invention_materials,
                    "probability_pct": float(invention.get("probability") or 0.0) * 100.0,
                    "skill_count": len(invention_skills),
                    "skills": IndustryJobManager._join_skill_names(invention_skills),
                    "skill_entries": invention_skills,
                    "time_seconds": int(invention.get("time") or 0),
                    "products": invention_products_out,
                }

            rows.append(row)

        rows.sort(
            key=lambda row: (
                str(
                    (((row.get("manufacturing_job") or {}).get("products") or [{}])[0].get("type_name") or "")
                    or (((row.get("reaction_job") or {}).get("products") or [{}])[0].get("type_name") or "")
                ).lower(),
                str(row.get("blueprint_name") or "").lower(),
            )
        )
        return rows