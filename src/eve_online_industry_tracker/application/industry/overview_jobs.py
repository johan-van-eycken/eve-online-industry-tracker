"""IndustryService: the background jobs behind the product-overview refreshes.

A mixin of IndustryService (service.py); it holds no state of its own."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
import threading
from typing import Any
import uuid

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService
from flask_app.background_jobs import register_thread
from eve_online_industry_tracker.application.industry.job_manager import IndustryJobManager


# The service module's logger: it carries the QuietWarningsFilter (see service.py).
logger = logging.getLogger("eve_online_industry_tracker.application.industry.service")


class OverviewJobsMixin:
    def _ensure_industry_job_manager(self) -> IndustryJobManager:
        mgr = getattr(self._state, "industry_job_manager", None)
        if mgr is None:
            mgr = IndustryJobManager(state=self._state)
            mgr.start()
            self._state.industry_job_manager = mgr
        return mgr

    def _get_industry_overview_refresh_store(self) -> Any:
        jobs_state = getattr(self._state, "jobs", None)
        if jobs_state is None or not hasattr(jobs_state, "industry_overview_refresh"):
            raise RuntimeError("Industry overview refresh state is not initialized")
        return jobs_state.industry_overview_refresh


    @staticmethod
    def _normalize_overview_refresh_params(
        *,
        force_refresh: bool = False,
        maximize_bp_runs: bool = False,
        group_identical_bpcs: bool = True,
        build_from_bpc: bool = True,
        have_blueprint_source_only: bool = True,
        include_reactions: bool = False,
        market_hub: str = "jita",
        material_price_side: str = "sell",
        product_price_side: str = "sell",
        industry_profile_id: int | None = None,
        owned_blueprints_scope: str = "all_characters",
        character_id: int | None = None,
    ) -> dict[str, Any]:
        return {
            "force_refresh": bool(force_refresh),
            "maximize_bp_runs": bool(maximize_bp_runs),
            "group_identical_bpcs": bool(group_identical_bpcs),
            "build_from_bpc": bool(build_from_bpc),
            "have_blueprint_source_only": bool(have_blueprint_source_only),
            "include_reactions": bool(include_reactions),
            "market_hub": MarketPricingService.normalize_market_hub(market_hub),
            "material_price_side": MarketPricingService.normalize_order_side(material_price_side),
            "product_price_side": MarketPricingService.normalize_order_side(product_price_side),
            "industry_profile_id": int(industry_profile_id) if industry_profile_id is not None else None,
            "owned_blueprints_scope": str(owned_blueprints_scope),
            "character_id": int(character_id) if character_id is not None else None,
        }

    @staticmethod
    def _job_is_active(job: dict[str, Any]) -> bool:
        return str(job.get("status") or "").strip().lower() in {"queued", "running"}

    _overview_refresh_job_is_active = _job_is_active


    # Finished jobs kept per store: the latest results, not every refresh of the session.
    _FINISHED_JOBS_KEPT = 5

    def _claim_job(
        self, store: Any, params: dict[str, Any], *, step_count: int,
    ) -> tuple[dict[str, Any], bool]:
        """(job, created): the active job with these params, else a new queued
        one. Check, prune and insert under one lock, so racing identical starts
        run one recompute."""
        with store.lock:
            for job in store.jobs.values():
                if isinstance(job, dict) and self._job_is_active(job) and dict(job.get("request_params") or {}) == params:
                    return dict(job), False
            finished = sorted(
                (job_id for job_id, job in store.jobs.items() if not self._job_is_active(job)),
                key=lambda job_id: str(store.jobs[job_id].get("created_at") or ""),
            )
            for job_id in finished[:-self._FINISHED_JOBS_KEPT]:
                del store.jobs[job_id]
            job_id = str(uuid.uuid4())
            created_at = datetime.now(timezone.utc).isoformat()
            store.jobs[job_id] = {
                "job_id": job_id,
                "request_params": dict(params),
                "status": "queued",
                "progress_fraction": 0.0,
                "progress_label": "Queued",
                "progress_meta": {
                    "stage": "queued",
                    "step": 0,
                    "step_count": step_count,
                },
                "created_at": created_at,
                "updated_at": created_at,
                "result": None,
                "result_meta": {},
                "result_count": 0,
                "error_message": None,
            }
            return dict(store.jobs[job_id]), True

    def _update_job(
        self,
        store: Any,
        job_id: str,
        *,
        job_kind: str = "job",
        status: str | None = None,
        progress_fraction: float | None = None,
        progress_label: str | None = None,
        result: list[dict[str, Any]] | None = None,
        result_meta: dict[str, Any] | None = None,
        error_message: str | None = None,
        progress_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with store.lock:
            job = store.jobs.get(str(job_id))
            if job is None:
                raise RuntimeError(f"Unknown {job_kind}: {job_id}")
            if status is not None:
                job["status"] = str(status)
            if progress_fraction is not None:
                job["progress_fraction"] = max(0.0, min(1.0, float(progress_fraction)))
            if progress_label is not None:
                job["progress_label"] = str(progress_label)
            if progress_meta is not None:
                job["progress_meta"] = dict(progress_meta)
            if result is not None:
                job["result"] = result
                job["result_count"] = len(result)
            if result_meta is not None:
                job["result_meta"] = dict(result_meta)
            if error_message is not None:
                job["error_message"] = str(error_message)
            job["updated_at"] = datetime.now(timezone.utc).isoformat()
            return dict(job)

    def _update_overview_refresh_job(self, job_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._update_job(self._get_industry_overview_refresh_store(), job_id, job_kind="overview refresh job", **kwargs)


    def start_industry_manufacturing_product_overview_refresh(
        self,
        *,
        force_refresh: bool = False,
        maximize_bp_runs: bool = False,
        group_identical_bpcs: bool = True,
        build_from_bpc: bool = True,
        have_blueprint_source_only: bool = True,
        include_reactions: bool = False,
        market_hub: str = "jita",
        material_price_side: str = "sell",
        product_price_side: str = "sell",
        industry_profile_id: int | None = None,
        owned_blueprints_scope: str = "all_characters",
        character_id: int | None = None,
    ) -> dict[str, Any]:
        params = self._normalize_overview_refresh_params(
            force_refresh=force_refresh,
            maximize_bp_runs=maximize_bp_runs,
            group_identical_bpcs=group_identical_bpcs,
            build_from_bpc=build_from_bpc,
            have_blueprint_source_only=have_blueprint_source_only,
            include_reactions=include_reactions,
            market_hub=market_hub,
            material_price_side=material_price_side,
            product_price_side=product_price_side,
            industry_profile_id=industry_profile_id,
            owned_blueprints_scope=owned_blueprints_scope,
            character_id=character_id,
        )
        existing_job, created = self._claim_job(self._get_industry_overview_refresh_store(), params, step_count=9)
        if not created:
            return {
                "job_id": str(existing_job.get("job_id") or ""),
                "created_at": existing_job.get("created_at"),
                "updated_at": existing_job.get("updated_at"),
                "progress_label": str(existing_job.get("progress_label") or "Queued"),
                "progress_meta": dict(existing_job.get("progress_meta") or {}),
            }
        job_id = str(existing_job["job_id"])
        created_at = existing_job["created_at"]

        thread = threading.Thread(
            target=self._run_overview_refresh_job,
            args=(job_id, params),
            daemon=True,
            name=f"industry-overview-refresh-{job_id[:8]}",
        )
        register_thread(self._state, thread.name, thread)
        thread.start()
        return {
            "job_id": job_id,
            "created_at": created_at,
            "updated_at": created_at,
            "progress_label": "Queued",
            "progress_meta": {"step": 0, "step_count": 9, "stage": "queued"},
        }

    def _run_overview_refresh_job(self, job_id: str, params: dict[str, Any]) -> None:
        self._update_overview_refresh_job(
            job_id,
            status="running",
            progress_fraction=0.01,
            progress_label="Starting overview refresh",
            progress_meta={"stage": "startup"},
        )
        try:
            def report_progress(
                progress_fraction: float,
                progress_label: str,
                progress_meta: dict[str, Any] | None = None,
            ) -> None:
                self._update_overview_refresh_job(
                    job_id,
                    status="running",
                    progress_fraction=progress_fraction,
                    progress_label=progress_label,
                    progress_meta=progress_meta,
                )

            payload = self.industry_manufacturing_product_overview_payload(
                **params,
                progress_callback=report_progress,
            )
            self._update_overview_refresh_job(
                job_id,
                status="completed",
                progress_fraction=1.0,
                progress_label="Overview refresh completed",
                result=(payload.get("rows") or []) if isinstance(payload, dict) else [],
                result_meta=((payload.get("pricing_batch") or {}) if isinstance(payload, dict) else {}),
            )
        except Exception as e:
            logger.exception("Overview refresh job %s failed: %s", job_id, e)
            self._update_overview_refresh_job(
                job_id,
                status="failed",
                progress_label="Overview refresh failed",
                error_message=str(e),
            )

    def industry_manufacturing_product_overview_refresh_status(self, *, job_id: str) -> dict[str, Any]:
        store = self._get_industry_overview_refresh_store()
        with store.lock:
            job = store.jobs.get(str(job_id))
            if job is None:
                raise ServiceError(f"Unknown overview refresh job: {job_id}", status_code=404)
            return dict(job)


