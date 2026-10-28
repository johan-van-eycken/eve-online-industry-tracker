"""IndustryService: the background jobs behind the product-overview and portfolio-candidates refreshes.

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

    def _get_industry_portfolio_candidates_store(self) -> Any:
        jobs_state = getattr(self._state, "jobs", None)
        if jobs_state is None or not hasattr(jobs_state, "industry_portfolio_candidates"):
            raise RuntimeError("Industry portfolio candidates state is not initialized")
        return jobs_state.industry_portfolio_candidates

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
    _portfolio_candidates_job_is_active = _job_is_active

    @staticmethod
    def _normalize_portfolio_candidates_params(
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
        planning_horizon_hours: float = 24.0,
        enabled_meta_groups: list[str] | tuple[str, ...] | None = None,
        have_skills_only: bool = False,
        positive_profit_only: bool = False,
        min_margin_pct: float = 0.0,
        min_isk_per_hour: float = 0.0,
        min_region_daily_volume: int = 0,
    ) -> dict[str, Any]:
        if enabled_meta_groups is None:
            normalized_enabled_meta_groups = None
        else:
            normalized_enabled_meta_groups = tuple(
                sorted(
                    {
                        str(meta_group_name or "").strip()
                        for meta_group_name in enabled_meta_groups
                        if str(meta_group_name or "").strip()
                    },
                    key=str.lower,
                )
            )
        return {
            **OverviewJobsMixin._normalize_overview_refresh_params(
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
            ),
            "planning_horizon_hours": float(planning_horizon_hours or 0.0),
            "enabled_meta_groups": normalized_enabled_meta_groups,
            "have_skills_only": bool(have_skills_only),
            "positive_profit_only": bool(positive_profit_only),
            "min_margin_pct": float(min_margin_pct or 0.0),
            "min_isk_per_hour": float(min_isk_per_hour or 0.0),
            "min_region_daily_volume": int(min_region_daily_volume or 0),
        }

    # Finished jobs kept per store: the latest results (a portfolio job is also the
    # candidate snapshot a plan is built from), not every refresh of the session.
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

    def _update_portfolio_candidates_job(self, job_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._update_job(self._get_industry_portfolio_candidates_store(), job_id, job_kind="portfolio candidates job", **kwargs)

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

    def start_industry_manufacturing_portfolio_candidates_refresh(
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
        planning_horizon_hours: float = 24.0,
        enabled_meta_groups: list[str] | tuple[str, ...] | None = None,
        have_skills_only: bool = False,
        positive_profit_only: bool = False,
        min_margin_pct: float = 0.0,
        min_isk_per_hour: float = 0.0,
        min_region_daily_volume: int = 0,
    ) -> dict[str, Any]:
        params = self._normalize_portfolio_candidates_params(
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
            planning_horizon_hours=planning_horizon_hours,
            enabled_meta_groups=enabled_meta_groups,
            have_skills_only=have_skills_only,
            positive_profit_only=positive_profit_only,
            min_margin_pct=min_margin_pct,
            min_isk_per_hour=min_isk_per_hour,
            min_region_daily_volume=min_region_daily_volume,
        )
        existing_job, created = self._claim_job(self._get_industry_portfolio_candidates_store(), params, step_count=11)
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
            target=self._run_portfolio_candidates_refresh_job,
            args=(job_id, params),
            daemon=True,
            name=f"industry-portfolio-candidates-{job_id[:8]}",
        )
        register_thread(self._state, thread.name, thread)
        thread.start()
        return {
            "job_id": job_id,
            "created_at": created_at,
            "updated_at": created_at,
            "progress_label": "Queued",
            "progress_meta": {"step": 0, "step_count": 11, "stage": "queued"},
        }

    def _run_portfolio_candidates_refresh_job(self, job_id: str, params: dict[str, Any]) -> None:
        self._update_portfolio_candidates_job(
            job_id,
            status="running",
            progress_fraction=0.01,
            progress_label="Step 1/11: Starting portfolio candidate build",
            progress_meta={"step": 1, "step_count": 11, "stage": "startup"},
        )
        try:
            def report_progress(
                progress_fraction: float,
                progress_label: str,
                progress_meta: dict[str, Any] | None = None,
            ) -> None:
                self._update_portfolio_candidates_job(
                    job_id,
                    status="running",
                    progress_fraction=progress_fraction,
                    progress_label=progress_label,
                    progress_meta=progress_meta,
                )

            payload = self.industry_manufacturing_portfolio_candidates_payload(
                **params,
                progress_callback=report_progress,
            )
            self._update_portfolio_candidates_job(
                job_id,
                status="completed",
                progress_fraction=1.0,
                progress_label="Portfolio candidates ready",
                result=(payload.get("candidates") or []) if isinstance(payload, dict) else [],
                result_meta={
                    "summary": (payload.get("summary") or {}) if isinstance(payload, dict) else {},
                    "pricing_batch": (payload.get("pricing_batch") or {}) if isinstance(payload, dict) else {},
                },
            )
        except Exception as e:
            logger.exception("Portfolio candidate build job %s failed: %s", job_id, e)
            self._update_portfolio_candidates_job(
                job_id,
                status="failed",
                progress_label="Portfolio candidate build failed",
                error_message=str(e),
            )

    def industry_manufacturing_portfolio_candidates_refresh_status(self, *, job_id: str) -> dict[str, Any]:
        store = self._get_industry_portfolio_candidates_store()
        with store.lock:
            job = store.jobs.get(str(job_id))
            if job is None:
                raise ServiceError(f"Unknown portfolio candidates job: {job_id}", status_code=404)
            return dict(job)

    def industry_manufacturing_portfolio_candidate_snapshot(
        self,
        *,
        snapshot_id: str,
        character_id: int | None = None,
    ) -> dict[str, Any]:
        normalized_snapshot_id = str(snapshot_id or "").strip()
        if not normalized_snapshot_id:
            raise ServiceError("Candidate snapshot id is required.", status_code=400)

        store = self._get_industry_portfolio_candidates_store()
        with store.lock:
            job = store.jobs.get(normalized_snapshot_id)
            if job is None:
                raise ServiceError(f"Unknown portfolio candidates snapshot: {normalized_snapshot_id}", status_code=404)
            snapshot_job = dict(job)

        status = str(snapshot_job.get("status") or "")
        if status != "completed":
            if status == "failed":
                raise ServiceError(
                    str(snapshot_job.get("error_message") or "Candidate snapshot build failed."),
                    status_code=409,
                )
            raise ServiceError("Candidate snapshot is not ready yet.", status_code=409)

        request_params = dict(snapshot_job.get("request_params") or {})
        if character_id is not None:
            request_character_id = int(request_params.get("character_id") or 0)
            if request_character_id > 0 and request_character_id != int(character_id):
                raise ServiceError("Candidate snapshot does not belong to the requested character.", status_code=400)

        result_meta = dict(snapshot_job.get("result_meta") or {})
        return {
            "snapshot_id": normalized_snapshot_id,
            "created_at": snapshot_job.get("created_at"),
            "updated_at": snapshot_job.get("updated_at"),
            "request_params": request_params,
            "candidates": list(snapshot_job.get("result") or []),
            "summary": dict(result_meta.get("summary") or {}),
            "pricing_batch": dict(result_meta.get("pricing_batch") or {}),
            "result_count": int(snapshot_job.get("result_count") or 0),
        }
