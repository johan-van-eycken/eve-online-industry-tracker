from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import threading
import time
from typing import Any, Callable, cast
import uuid

from sqlalchemy import bindparam, text
from eve_online_industry_tracker.db_models import (
    Blueprints,
    CharacterAssetHistoryModel,
    CharacterAssetsModel,
    CorporationAssetHistoryModel,
    CorporationAssetsModel,
    NpcCorporations,
    NpcStations,
)

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService
from flask_app.background_jobs import register_thread
from eve_online_industry_tracker.infrastructure.session_provider import (
    SessionProvider,
    StateSessionProvider,
)

from eve_online_industry_tracker.infrastructure.industry_adapter import (
    corporation_structures_list_by_corporation_id,
    get_cached_public_structures,
    get_npc_stations,
    get_rig_effects_for_type_ids,
    get_solar_systems,
    get_type_data,
    industry_profile_create,
    industry_profile_delete,
    industry_profile_get_by_id,
    industry_profile_get_default_for_character_id,
    industry_profile_list_by_character_id,
    industry_profile_update,
    public_structures_cache_ttl_seconds,
    trigger_refresh_public_structures_for_system,
)
from eve_online_industry_tracker.application.industry.job_manager import IndustryJobManager
from eve_online_industry_tracker.infrastructure.persistence import blueprints_repo


ProgressCallback = Callable[[float, str, dict[str, Any] | None], None]


class IndustryService:
    _SKILL_ID_ACCOUNTING = 16622
    _SKILL_ID_BROKER_RELATIONS = 3446
    _STRUCTURE_RIG_MFG_CACHE_TTL_SECONDS = 24 * 3600
    _STRUCTURE_RIGS_CACHE_VERSION = 3
    _RIG_ATTR_TIME_REDUCTION = 2593
    _RIG_ATTR_MATERIAL_REDUCTION = 2594
    _RIG_ATTR_COST_REDUCTION = 2595

    _RIG_GROUP_TOKEN_LABELS: dict[str, str] = {
        # Manufacturing
        "Equipment": "Modules",
        "Ammo": "Ammo & Charges",
        "Drone": "Drones",
        "Smallship": "Basic Small Ships",
        "Mediumship": "Basic Medium Ships",
        "Mediumships": "Basic Medium Ships",
        "Largeship": "Basic Large Ships",
        "AdvComponent": "Advanced Components",
        "AdvSmship": "Advanced Small Ships",
        "AdvMedShip": "Advanced Medium Ships",
        "AdvLarShip": "Advanced Large Ships",
        "BasCapComp": "Capital Components",
        "CapShip": "Capital Ships",
        "AdvCapComponent": "Advanced Capital Components",
        "Structure": "Structures",
        # Reactions
        "ReactionBio": "Biochemical Reactions",
        "ReactionComp": "Composite Reactions",
        "ReactionHyb": "Hybrid Reactions",
        # Some variants that may show up in SDE
        "ThukkerAdvCapComp": "Advanced Capital Components",
        "ThukkerBasCapComp": "Capital Components",
        "AllShip": "All Ships",
    }
    _INDUSTRY_CHARACTER_MODIFIER_SKILL_NAMES: tuple[str, ...] = (
        "Industry",
        "Advanced Industry",
        "Science",
        "Research",
        "Metallurgy",
        "Laboratory Operation",
        "Advanced Laboratory Operation",
        "Mass Production",
        "Advanced Mass Production",
    )
    _RESEARCH_RANK1_TARGET_DURATION_SECONDS: dict[int, int] = {
        1: 105,
        2: 250,
        3: 595,
        4: 1414,
        5: 3360,
        6: 8000,
        7: 19000,
        8: 45255,
        9: 107700,
        10: 256000,
    }
    _ACTIVITY_COST_INDEX_ALIASES: dict[str, tuple[str, ...]] = {
        "manufacturing": ("manufacturing",),
        "reaction": ("reaction",),
        "copying": ("copying",),
        "research_material": ("research_material", "researching_material_efficiency"),
        "research_time": ("research_time", "researching_time_efficiency"),
        "invention": ("invention",),
    }
    _ACTIVITY_EFFECT_ALIASES: dict[str, tuple[str, ...]] = {
        "manufacturing": ("manufacturing",),
        "reaction": ("manufacturing",),
        "copying": ("copying",),
        "research_material": ("research_me", "research"),
        "research_time": ("research_te", "research"),
        "invention": ("invention",),
    }
    _ACTIVITY_LABELS: dict[str, str] = {
        "manufacturing": "Manufacturing Job",
        "reaction": "Reaction Job",
        "copying": "Blueprint Copying",
        "research_material": "ME Research",
        "research_time": "TE Research",
        "invention": "Invention",
        "materials": "Required Materials",
        "material": "Material",
        "product": "Product Overview",
    }
    _MAX_RESEARCH_LEVEL = 10
    _MAX_BLUEPRINT_MATERIAL_EFFICIENCY = 10
    _MAX_BLUEPRINT_TIME_EFFICIENCY = 20

    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)

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
    def _overview_refresh_job_is_active(job: dict[str, Any]) -> bool:
        return str(job.get("status") or "").strip().lower() in {"queued", "running"}

    @staticmethod
    def _portfolio_candidates_job_is_active(job: dict[str, Any]) -> bool:
        return str(job.get("status") or "").strip().lower() in {"queued", "running"}

    @staticmethod
    def _exclude_from_product_overview(row: dict[str, Any]) -> bool:
        type_name = str(row.get("type_name") or "").strip().lower()
        if not type_name:
            return False
        return "expired" in type_name

    @staticmethod
    def _normalized_overview_meta_group_name(row: dict[str, Any]) -> str:
        raw_name = str(row.get("meta_group_name") or "").strip()
        normalized = raw_name.lower()
        if normalized in {"tech i", "structure tech i", "abyssal"}:
            return "Tech I"
        if normalized in {"tech ii", "structure tech ii"}:
            return "Tech II"
        if normalized in {"tech iii", "structure tech iii"}:
            return "Tech III"
        if normalized in {"faction", "structure faction"}:
            return "Faction"
        if normalized in {"storyline", "limited time"}:
            return "Storyline"
        return raw_name

    @staticmethod
    def _overview_row_skill_requirements_met(row: dict[str, Any]) -> bool:
        manufacturing_job = row.get("manufacturing_job") or {}
        if not isinstance(manufacturing_job, dict):
            return False
        skills = manufacturing_job.get("skills") or {}
        if not isinstance(skills, dict):
            return False
        return bool(skills.get("skill_requirements_met", False))

    @classmethod
    def _filter_overview_rows_for_portfolio_candidates(
        cls,
        overview_rows: list[dict[str, Any]],
        *,
        enabled_meta_groups: list[str] | tuple[str, ...] | None = None,
        have_skills_only: bool = False,
        positive_profit_only: bool = False,
        min_margin_pct: float = 0.0,
        min_isk_per_hour: float = 0.0,
        min_region_daily_volume: int = 0,
    ) -> list[dict[str, Any]]:
        enabled_meta_group_set = set(enabled_meta_groups) if enabled_meta_groups is not None else None
        filtered_rows: list[dict[str, Any]] = []
        for row in overview_rows:
            if enabled_meta_group_set is not None and cls._normalized_overview_meta_group_name(row) not in enabled_meta_group_set:
                continue
            if have_skills_only and not cls._overview_row_skill_requirements_met(row):
                continue
            if positive_profit_only and float(row.get("profit_amount") or 0.0) <= 0.0:
                continue
            if float(row.get("profit_margin_fraction") or 0.0) < (float(min_margin_pct or 0.0) / 100.0):
                continue
            if float(row.get("isk_per_hour") or 0.0) < float(min_isk_per_hour or 0.0):
                continue
            if int(row.get("region_daily_volume") or 0) < int(min_region_daily_volume or 0):
                continue
            filtered_rows.append(row)
        return filtered_rows

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
            **IndustryService._normalize_overview_refresh_params(
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

    def _find_matching_overview_refresh_job(self, *, params: dict[str, Any]) -> dict[str, Any] | None:
        store = self._get_industry_overview_refresh_store()
        with store.lock:
            for job in store.jobs.values():
                if not isinstance(job, dict):
                    continue
                if not self._overview_refresh_job_is_active(job):
                    continue
                if dict(job.get("request_params") or {}) != params:
                    continue
                return dict(job)
        return None

    def _find_matching_portfolio_candidates_job(self, *, params: dict[str, Any]) -> dict[str, Any] | None:
        store = self._get_industry_portfolio_candidates_store()
        with store.lock:
            for job in store.jobs.values():
                if not isinstance(job, dict):
                    continue
                if not self._portfolio_candidates_job_is_active(job):
                    continue
                if dict(job.get("request_params") or {}) != params:
                    continue
                return dict(job)
        return None

    def _update_overview_refresh_job(
        self,
        job_id: str,
        *,
        status: str | None = None,
        progress_fraction: float | None = None,
        progress_label: str | None = None,
        result: list[dict[str, Any]] | None = None,
        result_meta: dict[str, Any] | None = None,
        error_message: str | None = None,
        progress_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        store = self._get_industry_overview_refresh_store()
        with store.lock:
            job = store.jobs.get(str(job_id))
            if job is None:
                raise RuntimeError(f"Unknown overview refresh job: {job_id}")
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

    def _update_portfolio_candidates_job(
        self,
        job_id: str,
        *,
        status: str | None = None,
        progress_fraction: float | None = None,
        progress_label: str | None = None,
        result: list[dict[str, Any]] | None = None,
        result_meta: dict[str, Any] | None = None,
        error_message: str | None = None,
        progress_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        store = self._get_industry_portfolio_candidates_store()
        with store.lock:
            job = store.jobs.get(str(job_id))
            if job is None:
                raise RuntimeError(f"Unknown portfolio candidates job: {job_id}")
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
        existing_job = self._find_matching_overview_refresh_job(params=params)
        if existing_job is not None:
            return {
                "job_id": str(existing_job.get("job_id") or ""),
                "created_at": existing_job.get("created_at"),
                "updated_at": existing_job.get("updated_at"),
                "progress_label": str(existing_job.get("progress_label") or "Queued"),
                "progress_meta": dict(existing_job.get("progress_meta") or {}),
            }

        job_id = str(uuid.uuid4())
        store = self._get_industry_overview_refresh_store()
        created_at = datetime.now(timezone.utc).isoformat()
        with store.lock:
            store.jobs[job_id] = {
                "job_id": job_id,
                "request_params": dict(params),
                "status": "queued",
                "progress_fraction": 0.0,
                "progress_label": "Queued",
                "progress_meta": {
                    "stage": "queued",
                    "step": 0,
                    "step_count": 9,
                },
                "created_at": created_at,
                "updated_at": created_at,
                "result": None,
                "result_meta": {},
                "result_count": 0,
                "error_message": None,
            }

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
            progress_label="Step 1/9: Starting overview refresh",
            progress_meta={"step": 1, "step_count": 9, "stage": "startup"},
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
                force_refresh=bool(params.get("force_refresh", False)),
                maximize_bp_runs=bool(params.get("maximize_bp_runs", False)),
                group_identical_bpcs=bool(params.get("group_identical_bpcs", True)),
                build_from_bpc=bool(params.get("build_from_bpc", True)),
                have_blueprint_source_only=bool(params.get("have_blueprint_source_only", True)),
                include_reactions=bool(params.get("include_reactions", False)),
                market_hub=str(params.get("market_hub") or "jita"),
                material_price_side=str(params.get("material_price_side") or "sell"),
                product_price_side=str(params.get("product_price_side") or "sell"),
                industry_profile_id=params.get("industry_profile_id"),
                owned_blueprints_scope=str(params.get("owned_blueprints_scope") or "all_characters"),
                character_id=params.get("character_id"),
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
        existing_job = self._find_matching_portfolio_candidates_job(params=params)
        if existing_job is not None:
            return {
                "job_id": str(existing_job.get("job_id") or ""),
                "created_at": existing_job.get("created_at"),
                "updated_at": existing_job.get("updated_at"),
                "progress_label": str(existing_job.get("progress_label") or "Queued"),
                "progress_meta": dict(existing_job.get("progress_meta") or {}),
            }

        job_id = str(uuid.uuid4())
        store = self._get_industry_portfolio_candidates_store()
        created_at = datetime.now(timezone.utc).isoformat()
        with store.lock:
            store.jobs[job_id] = {
                "job_id": job_id,
                "request_params": dict(params),
                "status": "queued",
                "progress_fraction": 0.0,
                "progress_label": "Queued",
                "progress_meta": {
                    "stage": "queued",
                    "step": 0,
                    "step_count": 11,
                },
                "created_at": created_at,
                "updated_at": created_at,
                "result": None,
                "result_meta": {},
                "result_count": 0,
                "error_message": None,
            }

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
                force_refresh=bool(params.get("force_refresh", False)),
                maximize_bp_runs=bool(params.get("maximize_bp_runs", False)),
                group_identical_bpcs=bool(params.get("group_identical_bpcs", True)),
                build_from_bpc=bool(params.get("build_from_bpc", True)),
                have_blueprint_source_only=bool(params.get("have_blueprint_source_only", True)),
                include_reactions=bool(params.get("include_reactions", False)),
                market_hub=str(params.get("market_hub") or "jita"),
                material_price_side=str(params.get("material_price_side") or "sell"),
                product_price_side=str(params.get("product_price_side") or "sell"),
                industry_profile_id=params.get("industry_profile_id"),
                owned_blueprints_scope=str(params.get("owned_blueprints_scope") or "all_characters"),
                character_id=params.get("character_id"),
                planning_horizon_hours=float(params.get("planning_horizon_hours") or 24.0),
                enabled_meta_groups=params.get("enabled_meta_groups"),
                have_skills_only=bool(params.get("have_skills_only", False)),
                positive_profit_only=bool(params.get("positive_profit_only", False)),
                min_margin_pct=float(params.get("min_margin_pct") or 0.0),
                min_isk_per_hour=float(params.get("min_isk_per_hour") or 0.0),
                min_region_daily_volume=int(params.get("min_region_daily_volume") or 0),
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

    # -----------------
    # Endpoints logic
    # -----------------

    @staticmethod
    def _extract_structure_industry_bonuses_from_type_bonus(row: dict, *, language: str) -> dict:
        role_raw = row.get("roleBonuses")
        try:
            role = json.loads(role_raw) if isinstance(role_raw, str) else (role_raw or [])
        except Exception:
            role = []

        material_pct = 0.0
        time_pct = 0.0
        cost_pct = 0.0

        for b in role or []:
            if not isinstance(b, dict):
                continue
            bonus = b.get("bonus")
            if bonus is None:
                continue
            try:
                bonus_val = abs(float(bonus))
            except Exception:
                continue

            txt_obj = b.get("bonusText") or {}
            if isinstance(txt_obj, dict):
                txt = txt_obj.get(language) or txt_obj.get("en") or ""
            else:
                txt = ""
            t = str(txt).lower()

            if "material requirements" in t:
                material_pct = max(material_pct, bonus_val)
            elif "isk requirements" in t:
                cost_pct = max(cost_pct, bonus_val)
            elif "time requirements" in t:
                time_pct = max(time_pct, bonus_val)

        return {
            "material_reduction": max(0.0, material_pct) / 100.0,
            "time_reduction": max(0.0, time_pct) / 100.0,
            "cost_reduction": max(0.0, cost_pct) / 100.0,
        }

    def structure_type_bonuses(self, *, type_id: int) -> dict:
        if not type_id:
            raise ServiceError("Type ID is required.", status_code=400)

        session: Any = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"

        row = session.execute(
            text("SELECT id, roleBonuses, miscBonuses, types FROM typeBonus WHERE id = :id"),
            {"id": int(type_id)},
        ).mappings().fetchone()

        if not row:
            return {"type_id": int(type_id), "bonuses": {"material_reduction": 0.0, "time_reduction": 0.0, "cost_reduction": 0.0}}

        bonuses = self._extract_structure_industry_bonuses_from_type_bonus(dict(row), language=language)
        return {"type_id": int(type_id), "bonuses": bonuses}

    def solar_systems(self) -> Any:
        session: Any = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        return get_solar_systems(session, language)

    def npc_stations(self, *, system_id: int) -> Any:
        if not system_id:
            raise ServiceError("System ID is required to fetch NPC stations.", status_code=400)

        session: Any = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        return get_npc_stations(session, language, system_id)

    def public_structures(self, *, system_id: int) -> tuple[list[dict], dict]:
        if not system_id:
            raise ServiceError("System ID is required to fetch public structures.", status_code=400)

        if self._state.esi_service is None:
            raise ServiceError(
                "ESI service is not initialized (application not fully ready for ESI-backed endpoints)",
                status_code=503,
            )

        ttl_seconds = public_structures_cache_ttl_seconds()
        public_structures, is_fresh = get_cached_public_structures(
            state=self._state,
            system_id=int(system_id),
            ttl_seconds=int(ttl_seconds),
        )
        refreshing = False
        if not is_fresh:
            refreshing = trigger_refresh_public_structures_for_system(state=self._state, system_id=int(system_id))

        type_ids = list({s["type_id"] for s in public_structures if s.get("type_id") is not None})
        owner_ids = list({int(s["owner_id"]) for s in public_structures if s.get("owner_id") is not None})
        session = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        type_map = get_type_data(session, language, type_ids)

        owner_name_map: dict[int, str] = {}
        try:
            if owner_ids:
                resolved = self._state.esi_service.get_universe_names(owner_ids)
                owner_name_map = {int(k): ((v or {}).get("name") or "") for k, v in resolved.items()}
        except Exception:
            owner_name_map = {}

        enriched_structures = []
        for s in public_structures:
            type_id = s.get("type_id")
            extra = type_map.get(int(type_id), {}) if type_id is not None else {}
            owner_id = s.get("owner_id")
            owner_name = owner_name_map.get(int(owner_id)) if owner_id is not None else None
            enriched_structures.append({**s, **extra, "owner_name": owner_name})

        meta = {"refreshing": refreshing, "cache_fresh": is_fresh}
        return enriched_structures, meta

    def corporation_structures(self, *, character_id: int) -> list[dict]:
        if not character_id:
            raise ServiceError("Character ID is required to fetch corporation structures.", status_code=400)

        character = self._state.char_manager.get_character_by_id(character_id)
        if not character:
            raise ServiceError(f"Character ID {character_id} not found", status_code=400)
        if not character.corporation_id:
            raise ServiceError("Character has no corporation_id", status_code=400)

        session: Any = self._sessions.app_session()
        structures = corporation_structures_list_by_corporation_id(session, int(character.corporation_id))

        out = [s.to_dict() for s in structures]

        try:
            type_ids: list[int] = []
            for row in out:
                tid = row.get("type_id")
                if tid is None:
                    continue
                try:
                    type_ids.append(int(tid))
                except Exception:
                    continue
            type_ids = sorted(set(type_ids))
            if type_ids:
                sde_session: Any = self._sessions.sde_session()
                language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
                type_map = get_type_data(sde_session, language, type_ids)
                for s in out:
                    tid = s.get("type_id")
                    extra = type_map.get(int(tid), {}) if tid is not None else {}
                    s.update(extra)
        except Exception:
            pass

        try:
            corp_ids: list[int] = []
            for row in out:
                cid = row.get("corporation_id")
                if cid is None:
                    continue
                try:
                    corp_ids.append(int(cid))
                except Exception:
                    continue
            corp_ids = sorted(set(corp_ids))
            if corp_ids and self._state.esi_service is not None:
                resolved = self._state.esi_service.get_universe_names(corp_ids)
                corp_name_map = {int(k): (v or {}).get("name") for k, v in resolved.items()}
                for s in out:
                    cid = s.get("corporation_id")
                    if cid is None:
                        continue
                    s["owner_name"] = corp_name_map.get(int(cid))
        except Exception:
            pass

        return out

    def industry_profiles(self, *, character_id: int) -> list[dict]:
        session: Any = self._sessions.app_session()
        profiles = industry_profile_list_by_character_id(session, character_id)
        return [p.to_dict() for p in profiles]

    def _get_character(self, *, character_id: int) -> Any:
        if not character_id:
            raise ServiceError("Character ID is required.", status_code=400)

        character = self._state.char_manager.get_character_by_id(int(character_id))
        if not character:
            raise ServiceError(f"Character ID {character_id} not found", status_code=400)
        return character

    @staticmethod
    def _classify_industry_modifier_attribute(attribute_name: str) -> tuple[str | None, str]:
        normalized = str(attribute_name or "").strip().lower()
        if not normalized:
            return None, "other"

        if "manufact" in normalized:
            activity = "manufacturing"
        elif "invent" in normalized:
            activity = "invention"
        elif "copy" in normalized:
            activity = "copying"
        elif "research" in normalized and "material" in normalized:
            activity = "research_me"
        elif "research" in normalized and "time" in normalized:
            activity = "research_te"
        elif "research" in normalized:
            activity = "research"
        else:
            activity = None

        if any(token in normalized for token in ["material", "efficiency", "waste"]):
            metric = "material"
        elif any(token in normalized for token in ["time", "duration"]):
            metric = "time"
        elif any(token in normalized for token in ["cost", "installation", "isk"]):
            metric = "cost"
        else:
            metric = "other"

        return activity, metric

    @classmethod
    def _is_industry_modifier_attribute(cls, attribute_name: str) -> bool:
        normalized = str(attribute_name or "").strip().lower()
        if not normalized:
            return False
        return any(
            token in normalized
            for token in ["manufact", "copy", "invent", "research", "material", "time", "cost", "installation"]
        )

    def _get_character_implant_industry_modifiers(self, *, character_id: int) -> list[dict[str, Any]]:
        character = self._get_character(character_id=character_id)
        raw_implants = getattr(character, "implants", None) or []

        implant_type_ids: list[int] = []
        for raw_implant_id in raw_implants:
            try:
                implant_type_id = int(raw_implant_id)
            except Exception:
                continue
            if implant_type_id > 0:
                implant_type_ids.append(implant_type_id)
        implant_type_ids = sorted(set(implant_type_ids))
        if not implant_type_ids:
            return []

        session: Any = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        type_map = get_type_data(session, language, implant_type_ids)

        dogma_rows = session.execute(
            text("SELECT id, dogmaAttributes FROM typeDogma WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
            {"ids": implant_type_ids},
        ).fetchall()

        attr_map_by_type_id: dict[int, dict[int, float]] = {}
        attribute_ids: set[int] = set()
        for type_id, attributes_raw in dogma_rows:
            attr_map: dict[int, float] = {}
            try:
                attributes = json.loads(attributes_raw) if isinstance(attributes_raw, str) else (attributes_raw or [])
            except Exception:
                attributes = []
            for attribute in attributes or []:
                if not isinstance(attribute, dict):
                    continue
                attribute_id = attribute.get("attributeID")
                raw_value = attribute.get("value")
                if attribute_id is None or raw_value is None:
                    continue
                try:
                    parsed_attribute_id = int(attribute_id)
                    parsed_value = float(raw_value)
                except Exception:
                    continue
                attr_map[parsed_attribute_id] = parsed_value
                attribute_ids.add(parsed_attribute_id)
            attr_map_by_type_id[int(type_id)] = attr_map

        attribute_name_by_id: dict[int, str] = {}
        if attribute_ids:
            attribute_rows = session.execute(
                text("SELECT id, name FROM dogmaAttributes WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
                {"ids": sorted(attribute_ids)},
            ).fetchall()
            attribute_name_by_id = {int(row[0]): str(row[1] or "") for row in attribute_rows}

        implants_out: list[dict[str, Any]] = []
        for implant_type_id in implant_type_ids:
            type_payload = type_map.get(int(implant_type_id)) or {}
            modifier_entries: list[dict[str, Any]] = []
            for attribute_id, raw_value in (attr_map_by_type_id.get(int(implant_type_id)) or {}).items():
                attribute_name = attribute_name_by_id.get(int(attribute_id)) or ""
                if not self._is_industry_modifier_attribute(attribute_name):
                    continue
                activity, metric = self._classify_industry_modifier_attribute(attribute_name)
                if activity is None:
                    continue
                if metric not in {"material", "time", "cost"}:
                    continue
                modifier_entries.append(
                    {
                        "attribute_id": int(attribute_id),
                        "attribute_name": attribute_name,
                        "activity": activity,
                        "metric": metric,
                        "value": float(raw_value),
                    }
                )

            if not modifier_entries:
                continue

            implants_out.append(
                {
                    "type_id": int(implant_type_id),
                    "type_name": type_payload.get("type_name") or str(implant_type_id),
                    "modifiers": sorted(
                        modifier_entries,
                        key=lambda entry: (
                            str(entry.get("activity") or ""),
                            str(entry.get("metric") or ""),
                            str(entry.get("attribute_name") or ""),
                        ),
                    ),
                }
            )

        return implants_out

    def _get_character_industry_modifier_payload(self, *, character_id: int | None) -> dict[str, Any] | None:
        if character_id is None:
            return None

        character = self._get_character(character_id=int(character_id))
        skills_payload = getattr(character, "skills", None) or {}
        skill_entries = skills_payload.get("skills") if isinstance(skills_payload, dict) else []
        if not isinstance(skill_entries, list):
            skill_entries = []

        modifier_skill_names = {name.lower(): name for name in self._INDUSTRY_CHARACTER_MODIFIER_SKILL_NAMES}
        modifier_skills: list[dict[str, Any]] = []
        for entry in skill_entries:
            if not isinstance(entry, dict):
                continue
            skill_name = str(entry.get("skill_name") or entry.get("type_name") or "").strip()
            if not skill_name or skill_name.lower() not in modifier_skill_names:
                continue
            modifier_skills.append(
                {
                    "type_id": int(entry.get("skill_id") or entry.get("type_id") or 0),
                    "type_name": skill_name,
                    "trained_skill_level": int(entry.get("trained_skill_level") or 0),
                    "group_name": entry.get("group_name"),
                }
            )

        modifier_skills.sort(key=lambda entry: str(entry.get("type_name") or ""))

        implants = self._get_character_implant_industry_modifiers(character_id=int(character_id))
        return {
            "character_id": int(getattr(character, "character_id", None) or character_id),
            "character_name": getattr(character, "character_name", None),
            "modifier_skills": modifier_skills,
            "implant_count": len(implants),
            "implants": implants,
        }

    def _resolve_industry_profile_context(
        self,
        *,
        character_id: int | None,
        industry_profile_id: int | None,
    ) -> dict[str, Any] | None:
        if character_id is None and industry_profile_id is None:
            return None

        session: Any = self._sessions.app_session()
        profile = None
        if industry_profile_id is not None:
            profile = industry_profile_get_by_id(session, int(industry_profile_id))
            if profile is None:
                raise ServiceError(f"Industry profile ID {industry_profile_id} not found", status_code=400)
            if character_id is not None and int(profile.character_id) != int(character_id):
                raise ServiceError(
                    f"Industry profile ID {industry_profile_id} does not belong to character ID {character_id}",
                    status_code=400,
                )
        elif character_id is not None:
            profile = industry_profile_get_default_for_character_id(session, int(character_id))

        if profile is None:
            return None

        profile_payload = profile.to_dict()
        if profile.system_id is not None:
            try:
                system_payload = self.industry_system_cost_index(system_id=int(profile.system_id))
            except Exception:
                system_payload = {"solar_system_id": int(profile.system_id), "cost_indices": []}
            try:
                solar_system_payload = next(
                    (
                        entry
                        for entry in (self.solar_systems() or [])
                        if isinstance(entry, dict) and int(entry.get("id") or 0) == int(profile.system_id)
                    ),
                    None,
                )
            except Exception:
                solar_system_payload = None
            profile_payload["industry_system"] = system_payload
            profile_payload["system_cost_indices"] = system_payload.get("cost_indices") or []
            if isinstance(solar_system_payload, dict):
                profile_payload["system_security_status"] = solar_system_payload.get("security_status")
                profile_payload["industry_system"] = {
                    **system_payload,
                    "security_status": solar_system_payload.get("security_status"),
                }

        if profile.facility_id is not None:
            try:
                profile_payload["facility"] = self.industry_facility(facility_id=int(profile.facility_id))
            except Exception:
                profile_payload["facility"] = {"facility_id": int(profile.facility_id), "tax": profile.facility_tax}

        if profile.structure_type_id is not None:
            try:
                structure_bonus_payload = self.structure_type_bonuses(type_id=int(profile.structure_type_id))
            except Exception:
                structure_bonus_payload = {"type_id": int(profile.structure_type_id), "bonuses": {}}
            profile_payload["structure_type_bonuses"] = structure_bonus_payload.get("bonuses") or {}

        rig_slot_type_ids = [
            profile.rig_slot0_type_id,
            profile.rig_slot1_type_id,
            profile.rig_slot2_type_id,
        ]
        selected_rig_type_ids = [int(type_id) for type_id in rig_slot_type_ids if type_id is not None and int(type_id) > 0]
        if selected_rig_type_ids:
            sde_session: Any = self._sessions.sde_session()
            language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
            rig_type_map = get_type_data(sde_session, language, selected_rig_type_ids)
            rig_effects_by_type_id = {
                int(entry.get("type_id") or 0): entry
                for entry in get_rig_effects_for_type_ids(sde_session, selected_rig_type_ids)
                if isinstance(entry, dict) and int(entry.get("type_id") or 0) > 0
            }
            structure_rigs: list[dict[str, Any]] = []
            for slot_index, raw_type_id in enumerate(rig_slot_type_ids):
                if raw_type_id is None:
                    continue
                rig_type_id = int(raw_type_id)
                if rig_type_id <= 0:
                    continue
                rig_type_payload = rig_type_map.get(rig_type_id) or {}
                rig_effect_payload = rig_effects_by_type_id.get(rig_type_id) or {}
                structure_rigs.append(
                    {
                        "slot_index": slot_index,
                        "type_id": rig_type_id,
                        "type_name": rig_type_payload.get("type_name") or str(rig_type_id),
                        "material_reduction": rig_effect_payload.get("material_reduction"),
                        "time_reduction": rig_effect_payload.get("time_reduction"),
                        "cost_reduction": rig_effect_payload.get("cost_reduction"),
                        "effects": rig_effect_payload.get("effects") or [],
                    }
                )
            profile_payload["structure_rigs"] = structure_rigs

        return profile_payload

    def _get_character_trained_skill_levels(self, *, character_id: int) -> dict[int, int]:
        character = self._get_character(character_id=character_id)

        skills_payload = getattr(character, "skills", None) or {}
        if not isinstance(skills_payload, dict):
            return {}

        skill_entries = skills_payload.get("skills") or []
        if not isinstance(skill_entries, list):
            return {}

        trained_skill_levels: dict[int, int] = {}
        for entry in skill_entries:
            if not isinstance(entry, dict):
                continue
            try:
                skill_type_id = int(entry.get("skill_id") or entry.get("type_id") or 0)
            except Exception:
                continue
            if skill_type_id <= 0:
                continue
            try:
                trained_skill_level = int(entry.get("trained_skill_level") or 0)
            except Exception:
                trained_skill_level = 0
            trained_skill_levels[skill_type_id] = max(trained_skill_level, trained_skill_levels.get(skill_type_id, 0))

        return trained_skill_levels

    def _get_character_standing(self, *, character_id: int, from_type: str, from_id: int | None) -> float:
        if not from_id:
            return 0.0

        character = self._get_character(character_id=character_id)
        standings = getattr(character, "standings", None)
        if not isinstance(standings, list):
            return 0.0

        for standing in standings:
            if not isinstance(standing, dict):
                continue
            try:
                if str(standing.get("from_type") or "") != str(from_type):
                    continue
                if int(standing.get("from_id") or 0) != int(from_id):
                    continue
                return float(standing.get("standing") or 0.0)
            except Exception:
                continue
        return 0.0

    def _resolve_npc_market_fee_context(
        self,
        *,
        character_id: int | None,
        market_hub: str,
        product_price_side: str,
    ) -> dict[str, Any]:
        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        hub_context = pricing_service._market_hub_context(market_hub)
        normalized_product_price_side = MarketPricingService.normalize_order_side(product_price_side)

        sales_tax_fraction = None
        broker_fee_fraction = None
        broker_fee_applies = normalized_product_price_side == "sell"
        broker_fee_kind = "order_creation" if broker_fee_applies else "direct_sale"
        accounting_level = 0
        broker_relations_level = 0
        owner_corp_id: int | None = None
        owner_faction_id: int | None = None
        faction_standing = 0.0
        corp_standing = 0.0

        if character_id is not None:
            trained_skill_levels = self._get_character_trained_skill_levels(character_id=int(character_id))
            accounting_level = int(trained_skill_levels.get(self._SKILL_ID_ACCOUNTING, 0) or 0)
            broker_relations_level = int(trained_skill_levels.get(self._SKILL_ID_BROKER_RELATIONS, 0) or 0)
            sales_tax_fraction = max(0.0, min(1.0, 0.075 * (1.0 - (0.11 * float(accounting_level)))))

            sde_session: Any = self._sessions.sde_session()
            station = (
                sde_session.query(NpcStations)
                .filter(NpcStations.id == int(hub_context.get("station_id") or 0))
                .first()
            )
            if station is not None:
                try:
                    owner_corp_id = int(station.ownerID)
                except Exception:
                    owner_corp_id = None
            if owner_corp_id is not None:
                corporation = (
                    sde_session.query(NpcCorporations)
                    .filter(NpcCorporations.id == int(owner_corp_id))
                    .first()
                )
                if corporation is not None and getattr(corporation, "factionID", None) is not None:
                    try:
                        owner_faction_id = int(corporation.factionID)
                    except Exception:
                        owner_faction_id = None

            faction_standing = self._get_character_standing(
                character_id=int(character_id),
                from_type="faction",
                from_id=owner_faction_id,
            )
            corp_standing = self._get_character_standing(
                character_id=int(character_id),
                from_type="npc_corp",
                from_id=owner_corp_id,
            )
            broker_fee_fraction = max(
                0.01,
                min(
                    0.03,
                    0.03
                    - (0.003 * float(broker_relations_level))
                    - (0.0003 * float(faction_standing))
                    - (0.0002 * float(corp_standing)),
                ),
            )

        return {
            "market_hub": hub_context.get("hub"),
            "market_hub_label": hub_context.get("label"),
            "station_id": hub_context.get("station_id"),
            "region_id": hub_context.get("region_id"),
            "station_type": "npc_station",
            "output_price_side": normalized_product_price_side,
            "broker_fee_applies": broker_fee_applies,
            "broker_fee_kind": broker_fee_kind,
            "owner_corp_id": owner_corp_id,
            "owner_faction_id": owner_faction_id,
            "skills": {
                "accounting_level": accounting_level,
                "broker_relations_level": broker_relations_level,
            },
            "standings": {
                "faction_standing": faction_standing,
                "corp_standing": corp_standing,
            },
            "rates": {
                "sales_tax_fraction": sales_tax_fraction,
                "broker_fee_fraction": broker_fee_fraction,
            },
        }

    def _enrich_product_rows_with_sale_proceeds(
        self,
        product_rows: list[dict[str, Any]],
        *,
        character_id: int | None,
        market_hub: str,
        product_price_side: str,
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        fee_context = self._resolve_npc_market_fee_context(
            character_id=character_id,
            market_hub=market_hub,
            product_price_side=product_price_side,
        )
        fee_rates = fee_context.get("rates") or {}
        sales_tax_fraction = self._as_float(fee_rates.get("sales_tax_fraction"))
        broker_fee_fraction = self._as_float(fee_rates.get("broker_fee_fraction"))
        broker_fee_applies = bool(fee_context.get("broker_fee_applies"))

        for row in product_rows:
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue

            market_unit_price = self._as_float(row.get("market_unit_price"))
            product_quantity = int(row.get("quantity") or 0)
            if market_unit_price is None or product_quantity <= 0:
                manufacturing_job["gross_sale_value"] = None
                manufacturing_job["broker_fee_amount"] = None
                manufacturing_job["sales_tax_amount"] = None
                manufacturing_job["net_proceeds"] = None
                manufacturing_job["market_fee_context"] = fee_context
                row["gross_sale_value"] = None
                row["broker_fee_amount"] = None
                row["sales_tax_amount"] = None
                row["net_proceeds"] = None
                continue

            gross_sale_value = float(market_unit_price) * float(product_quantity)
            broker_fee_amount = (
                float(gross_sale_value) * float(broker_fee_fraction)
                if broker_fee_applies and broker_fee_fraction is not None
                else 0.0
            )
            sales_tax_amount = (
                float(gross_sale_value) * float(sales_tax_fraction)
                if sales_tax_fraction is not None
                else None
            )
            net_proceeds = None
            if sales_tax_amount is not None:
                net_proceeds = float(gross_sale_value) - float(broker_fee_amount or 0.0) - float(sales_tax_amount)

            manufacturing_job["gross_sale_value"] = gross_sale_value
            manufacturing_job["broker_fee_amount"] = broker_fee_amount if broker_fee_fraction is not None else None
            manufacturing_job["sales_tax_amount"] = sales_tax_amount
            manufacturing_job["net_proceeds"] = net_proceeds
            manufacturing_job["market_fee_context"] = fee_context

            row["gross_sale_value"] = gross_sale_value
            row["broker_fee_amount"] = broker_fee_amount if broker_fee_fraction is not None else None
            row["sales_tax_amount"] = sales_tax_amount
            row["net_proceeds"] = net_proceeds

        return product_rows

    def _enrich_product_rows_with_market_activity(
        self,
        product_rows: list[dict[str, Any]],
        *,
        market_hub: str,
        progress_callback: ProgressCallback | None = None,
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        product_type_ids = sorted(
            {
                int(row.get("type_id") or 0)
                for row in product_rows
                if isinstance(row, dict) and int(row.get("type_id") or 0) > 0
            }
        )
        if not product_type_ids:
            return product_rows

        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        if progress_callback is not None:
            progress_callback(
                0.82,
                "Step 6/9: Loading regional market history",
                {"step": 6, "step_count": 9, "stage": "market_history", "type_count": len(product_type_ids)},
            )
        region_daily_volume_map = pricing_service.get_region_daily_volume_map(type_ids=product_type_ids, hub=market_hub)
        if progress_callback is not None:
            progress_callback(
                0.90,
                "Step 7/9: Loading hub liquidity snapshots",
                {"step": 7, "step_count": 9, "stage": "liquidity", "type_count": len(product_type_ids)},
            )
        hub_liquidity_map = pricing_service.get_hub_liquidity_map(type_ids=product_type_ids, hub=market_hub)

        for row in product_rows:
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue

            type_id = int(row.get("type_id") or 0)
            daily_volume = region_daily_volume_map.get(type_id) or {}
            hub_liquidity = hub_liquidity_map.get(type_id) or {}

            manufacturing_job["region_daily_volume"] = int(daily_volume.get("daily_volume") or 0)
            manufacturing_job["region_daily_volume_7d_avg"] = self._as_float(daily_volume.get("daily_volume_7d_avg"))
            manufacturing_job["region_daily_volume_7d_sample_size"] = int(daily_volume.get("daily_volume_7d_sample_size") or 0)
            manufacturing_job["region_daily_order_count"] = int(daily_volume.get("daily_order_count") or 0)
            manufacturing_job["region_daily_volume_date"] = daily_volume.get("daily_volume_date")
            manufacturing_job["hub_buy_liquidity"] = int(hub_liquidity.get("buy_volume_total") or 0)
            manufacturing_job["hub_sell_liquidity"] = int(hub_liquidity.get("sell_volume_total") or 0)
            manufacturing_job["hub_buy_order_count"] = int(hub_liquidity.get("buy_order_count") or 0)
            manufacturing_job["hub_sell_order_count"] = int(hub_liquidity.get("sell_order_count") or 0)

            row["region_daily_volume"] = int(daily_volume.get("daily_volume") or 0)
            row["region_daily_volume_7d_avg"] = self._as_float(daily_volume.get("daily_volume_7d_avg"))
            row["region_daily_volume_7d_sample_size"] = int(daily_volume.get("daily_volume_7d_sample_size") or 0)
            row["region_daily_order_count"] = int(daily_volume.get("daily_order_count") or 0)
            row["region_daily_volume_date"] = daily_volume.get("daily_volume_date")
            row["hub_buy_liquidity"] = int(hub_liquidity.get("buy_volume_total") or 0)
            row["hub_sell_liquidity"] = int(hub_liquidity.get("sell_volume_total") or 0)
            row["hub_buy_order_count"] = int(hub_liquidity.get("buy_order_count") or 0)
            row["hub_sell_order_count"] = int(hub_liquidity.get("sell_order_count") or 0)

        if progress_callback is not None:
            progress_callback(
                0.93,
                "Step 7/9: Applied market activity signals",
                {"step": 7, "step_count": 9, "stage": "liquidity", "rows": len(product_rows)},
            )

        return product_rows

    def _enrich_product_rows_with_pricing_confidence(
        self,
        product_rows: list[dict[str, Any]],
        *,
        product_price_side: str,
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        normalized_product_price_side = MarketPricingService.normalize_order_side(product_price_side)
        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        target_sample_size = max(2, pricing_service.orderbook_depth())
        ttl_minutes = max(1.0, float(pricing_service.material_price_cache_ttl_seconds()) / 60.0)
        now = time.time()

        for row in product_rows:
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue

            reasons: list[str] = []
            score = 0

            market_unit_price = self._as_float(row.get("market_unit_price"))
            market_price_sample_size = int(row.get("market_price_sample_size") or 0)
            market_price_fetched_at = self._as_float(row.get("market_price_fetched_at"))
            market_price_age_minutes = None
            if market_price_fetched_at is not None and market_price_fetched_at > 0:
                market_price_age_minutes = max(0.0, (float(now) - float(market_price_fetched_at)) / 60.0)

            if normalized_product_price_side == "buy":
                relevant_liquidity = int(row.get("hub_buy_liquidity") or 0)
                relevant_order_count = int(row.get("hub_buy_order_count") or 0)
            else:
                relevant_liquidity = int(row.get("hub_sell_liquidity") or 0)
                relevant_order_count = int(row.get("hub_sell_order_count") or 0)

            region_daily_volume = int(row.get("region_daily_volume") or 0)
            region_daily_volume_7d_avg = self._as_float(row.get("region_daily_volume_7d_avg"))

            if market_unit_price is not None and market_unit_price > 0:
                score += 2
                reasons.append("Product has a usable market price.")
            else:
                reasons.append("Product has no usable market price.")

            if market_price_age_minutes is None:
                reasons.append("Price freshness timestamp is unavailable.")
            elif market_price_age_minutes <= min(30.0, ttl_minutes):
                score += 2
                reasons.append(f"Price snapshot is fresh ({market_price_age_minutes:.0f} min old).")
            elif market_price_age_minutes <= ttl_minutes:
                score += 1
                reasons.append(f"Price snapshot is moderately fresh ({market_price_age_minutes:.0f} min old).")
            else:
                reasons.append(f"Price snapshot is aging ({market_price_age_minutes:.0f} min old).")

            if market_price_sample_size >= target_sample_size:
                score += 2
                reasons.append(f"Orderbook sample covers {market_price_sample_size} levels.")
            elif market_price_sample_size > 0:
                score += 1
                reasons.append(f"Orderbook sample is thin at {market_price_sample_size} levels.")
            else:
                reasons.append("Orderbook sample size is missing.")

            if relevant_liquidity >= 100 or relevant_order_count >= 5:
                score += 2
                reasons.append(
                    f"{normalized_product_price_side.title()}-side hub liquidity is healthy ({relevant_liquidity:,} units / {relevant_order_count} orders)."
                )
            elif relevant_liquidity > 0 or relevant_order_count > 0:
                score += 1
                reasons.append(
                    f"{normalized_product_price_side.title()}-side hub liquidity is present but limited ({relevant_liquidity:,} units / {relevant_order_count} orders)."
                )
            else:
                reasons.append(f"No {normalized_product_price_side}-side hub liquidity is currently visible.")

            if (region_daily_volume_7d_avg is not None and region_daily_volume_7d_avg >= 20.0) or region_daily_volume >= 20:
                score += 2
                reasons.append("Regional trade volume is active.")
            elif (region_daily_volume_7d_avg is not None and region_daily_volume_7d_avg > 0.0) or region_daily_volume > 0:
                score += 1
                reasons.append("Regional trade volume exists but is limited.")
            else:
                reasons.append("Regional trade volume is absent.")

            if market_unit_price is None or market_unit_price <= 0:
                confidence = "Low"
            elif score >= 7:
                confidence = "High"
            elif score >= 4:
                confidence = "Medium"
            else:
                confidence = "Low"

            manufacturing_job["pricing_confidence"] = confidence
            manufacturing_job["pricing_confidence_reasons"] = reasons
            manufacturing_job["market_price_age_minutes"] = market_price_age_minutes

            row["pricing_confidence"] = confidence
            row["pricing_confidence_reasons"] = reasons
            row["market_price_age_minutes"] = market_price_age_minutes

        return product_rows

    def _enrich_product_rows_with_profit_metrics(self, product_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        for row in product_rows:
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue

            net_proceeds = self._as_float(row.get("net_proceeds"))
            total_cost = self._as_float(manufacturing_job.get("total_cost"))
            time_seconds = self._as_float(manufacturing_job.get("time_seconds"))

            profit_amount = None
            margin_fraction = None
            isk_per_hour = None

            if net_proceeds is not None and total_cost is not None:
                profit_amount = float(net_proceeds) - float(total_cost)
                if net_proceeds > 0:
                    margin_fraction = float(profit_amount) / float(net_proceeds)

            if profit_amount is not None and time_seconds is not None and time_seconds > 0:
                isk_per_hour = float(profit_amount) / (float(time_seconds) / 3600.0)

            manufacturing_job["profit_amount"] = profit_amount
            manufacturing_job["profit_margin_fraction"] = margin_fraction
            manufacturing_job["isk_per_hour"] = isk_per_hour

            row["profit_amount"] = profit_amount
            row["profit_margin_fraction"] = margin_fraction
            row["isk_per_hour"] = isk_per_hour

        return product_rows

    @staticmethod
    def _format_timestamp(value: Any) -> str | None:
        try:
            timestamp = float(value or 0.0)
        except Exception:
            return None
        if timestamp <= 0:
            return None
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

    def _build_overview_batch_meta(
        self,
        *,
        product_rows: list[dict[str, Any]],
        market_hub: str,
        material_price_side: str,
        product_price_side: str,
    ) -> dict[str, Any]:
        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        hub_context = pricing_service._market_hub_context(market_hub)
        now = time.time()

        def summarize_entries(entries: list[dict[str, Any]]) -> dict[str, Any]:
            fetched_timestamps = [
                fetched_at
                for entry in entries
                for fetched_at in [self._as_float(entry.get("fetched_at"))]
                if fetched_at is not None
            ]
            cached_count = sum(1 for entry in entries if bool(entry.get("cached")))
            live_count = sum(1 for entry in entries if entry.get("cached") is False)
            priced_count = sum(1 for entry in entries if self._as_float(entry.get("unit_price")) is not None)
            return {
                "type_count": len(entries),
                "priced_type_count": priced_count,
                "missing_type_count": max(0, len(entries) - priced_count),
                "cached_type_count": cached_count,
                "live_type_count": live_count,
                "oldest_fetched_at": self._format_timestamp(min(fetched_timestamps)) if fetched_timestamps else None,
                "newest_fetched_at": self._format_timestamp(max(fetched_timestamps)) if fetched_timestamps else None,
                "oldest_age_minutes": (
                    round(max(0.0, (now - min(fetched_timestamps)) / 60.0), 1) if fetched_timestamps else None
                ),
                "newest_age_minutes": (
                    round(max(0.0, (now - max(fetched_timestamps)) / 60.0), 1) if fetched_timestamps else None
                ),
            }

        material_entries_by_type_id: dict[int, dict[str, Any]] = {}
        product_entries: list[dict[str, Any]] = []
        confidence_distribution = {"High": 0, "Medium": 0, "Low": 0}
        positive_region_volume_count = 0
        positive_buy_liquidity_count = 0
        positive_sell_liquidity_count = 0

        for row in product_rows:
            if not isinstance(row, dict):
                continue
            product_entries.append(
                {
                    "type_id": int(row.get("type_id") or 0),
                    "type_name": str(row.get("type_name") or row.get("type_id") or ""),
                    "fetched_at": self._as_float(row.get("market_price_fetched_at")),
                    "cached": row.get("market_price_cached"),
                    "sample_size": int(row.get("market_price_sample_size") or 0),
                    "unit_price": self._as_float(row.get("market_unit_price")),
                }
            )

            confidence = str(row.get("pricing_confidence") or "").strip().title()
            if confidence in confidence_distribution:
                confidence_distribution[confidence] += 1
            if int(row.get("region_daily_volume") or 0) > 0:
                positive_region_volume_count += 1
            if int(row.get("hub_buy_liquidity") or 0) > 0:
                positive_buy_liquidity_count += 1
            if int(row.get("hub_sell_liquidity") or 0) > 0:
                positive_sell_liquidity_count += 1

            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue
            procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
            if not isinstance(procurement_materials, dict):
                continue
            for material in procurement_materials.values():
                if not isinstance(material, dict):
                    continue
                type_id = int(material.get("type_id") or 0)
                if type_id <= 0:
                    continue
                existing = material_entries_by_type_id.get(type_id)
                candidate = {
                    "type_id": type_id,
                    "type_name": str(material.get("type_name") or type_id),
                    "fetched_at": self._as_float(material.get("price_fetched_at")),
                    "cached": material.get("price_cached"),
                    "sample_size": int(material.get("price_sample_size") or 0),
                    "unit_price": self._as_float(material.get("unit_price")),
                }
                if existing is None:
                    material_entries_by_type_id[type_id] = candidate
                else:
                    if existing.get("fetched_at") is None:
                        existing["fetched_at"] = candidate.get("fetched_at")
                    if existing.get("cached") is None:
                        existing["cached"] = candidate.get("cached")
                    existing["sample_size"] = max(int(existing.get("sample_size") or 0), int(candidate.get("sample_size") or 0))
                    if existing.get("unit_price") is None:
                        existing["unit_price"] = candidate.get("unit_price")

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "market_hub": hub_context.get("hub"),
            "market_hub_label": hub_context.get("label"),
            "region_id": hub_context.get("region_id"),
            "material_price_side": MarketPricingService.normalize_order_side(material_price_side),
            "product_price_side": MarketPricingService.normalize_order_side(product_price_side),
            "orderbook_depth": pricing_service.orderbook_depth(),
            "orderbook_smoothing": pricing_service.orderbook_smoothing(),
            "cache_ttl_seconds": pricing_service.material_price_cache_ttl_seconds(),
            "row_count": len(product_rows),
            "confidence_distribution": confidence_distribution,
            "material_pricing": summarize_entries(list(material_entries_by_type_id.values())),
            "product_pricing": summarize_entries(product_entries),
            "market_activity": {
                "positive_region_daily_volume_count": positive_region_volume_count,
                "positive_buy_liquidity_count": positive_buy_liquidity_count,
                "positive_sell_liquidity_count": positive_sell_liquidity_count,
            },
        }

    def industry_manufacturing_product_overview_payload(
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
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        rows = self.industry_manufacturing_product_overview(
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
            progress_callback=progress_callback,
        )
        return {
            "rows": rows,
            "pricing_batch": self._build_overview_batch_meta(
                product_rows=rows,
                market_hub=market_hub,
                material_price_side=material_price_side,
                product_price_side=product_price_side,
            ),
        }

    @staticmethod
    def _portfolio_confidence_penalty(confidence: Any) -> float:
        normalized = str(confidence or "").strip().lower()
        if normalized == "high":
            return 1.0
        if normalized == "medium":
            return 0.85
        if normalized == "low":
            return 0.65
        return 0.75

    @classmethod
    def _owned_input_coverage_fraction(cls, procurement_materials: Any) -> float:
        if not isinstance(procurement_materials, dict) or not procurement_materials:
            return 0.0
        total_quantity = 0
        owned_quantity = 0
        for material in procurement_materials.values():
            if not isinstance(material, dict):
                continue
            quantity = max(0, int(material.get("quantity") or 0))
            if quantity <= 0:
                continue
            total_quantity += quantity
            price_source = str(material.get("price_source") or "").strip().lower()
            if price_source.startswith("owned_asset"):
                owned_quantity += quantity
        if total_quantity <= 0:
            return 0.0
        return float(owned_quantity) / float(total_quantity)

    @classmethod
    def _build_portfolio_candidate(
        cls,
        row: dict[str, Any],
        *,
        planning_horizon_hours: float,
    ) -> dict[str, Any]:
        manufacturing_job = row.get("manufacturing_job") or {}
        if not isinstance(manufacturing_job, dict):
            manufacturing_job = {}
        procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
        if not isinstance(procurement_materials, dict):
            procurement_materials = {}

        quantity_per_batch = max(0, int(row.get("quantity") or 0))
        total_cost = cls._as_float(manufacturing_job.get("total_cost"))
        profit_amount = cls._as_float(row.get("profit_amount"))
        profit_margin_fraction = cls._as_float(row.get("profit_margin_fraction"))
        isk_per_hour = cls._as_float(row.get("isk_per_hour"))
        total_time_seconds = cls._as_float(manufacturing_job.get("time_seconds"))
        manufacturing_time_seconds = cls._as_float(manufacturing_job.get("manufacturing_time_seconds"))
        preparation_time_seconds = cls._as_float(manufacturing_job.get("preparation_time_seconds"))
        region_daily_volume = max(0, int(row.get("region_daily_volume") or 0))
        region_daily_volume_7d_avg = cls._as_float(row.get("region_daily_volume_7d_avg"))
        confidence_penalty_factor = cls._portfolio_confidence_penalty(row.get("pricing_confidence"))
        slot_hours_per_batch = (
            float(total_time_seconds) / 3600.0
            if total_time_seconds is not None and total_time_seconds > 0
            else None
        )
        manufacturing_slot_hours_per_batch = (
            float(manufacturing_time_seconds) / 3600.0
            if manufacturing_time_seconds is not None and manufacturing_time_seconds > 0
            else slot_hours_per_batch
        )
        preparation_slot_hours_per_batch = (
            float(preparation_time_seconds) / 3600.0
            if preparation_time_seconds is not None and preparation_time_seconds > 0
            else None
        )

        horizon_days = max(0.0, float(planning_horizon_hours or 0.0)) / 24.0
        effective_daily_volume = (
            float(region_daily_volume_7d_avg)
            if region_daily_volume_7d_avg is not None and region_daily_volume_7d_avg > 0
            else float(region_daily_volume)
        )
        estimated_market_absorption_units = int(math.floor(max(0.0, effective_daily_volume) * horizon_days))
        max_batches_total = (
            int(math.floor(float(estimated_market_absorption_units) / float(quantity_per_batch)))
            if quantity_per_batch > 0 and estimated_market_absorption_units > 0
            else 0
        )
        effective_profit_per_batch = (
            float(profit_amount) * confidence_penalty_factor
            if profit_amount is not None
            else None
        )
        effective_isk_per_hour = (
            float(isk_per_hour) * confidence_penalty_factor
            if isk_per_hour is not None
            else None
        )
        owned_input_coverage_fraction = cls._owned_input_coverage_fraction(procurement_materials)
        is_portfolio_candidate = bool(
            profit_amount is not None
            and profit_amount > 0
            and total_cost is not None
            and total_cost > 0
            and quantity_per_batch > 0
            and slot_hours_per_batch is not None
            and slot_hours_per_batch > 0
            and max_batches_total > 0
        )

        return {
            "overview_row_id": row.get("overview_row_id"),
            "type_id": int(row.get("type_id") or 0),
            "type_name": row.get("type_name"),
            "category_name": row.get("category_name") or row.get("type_category_name"),
            "meta_group_name": row.get("meta_group_name"),
            "quantity_per_batch": quantity_per_batch,
            "profit_amount": profit_amount,
            "profit_margin_fraction": profit_margin_fraction,
            "isk_per_hour": isk_per_hour,
            "net_proceeds": cls._as_float(row.get("net_proceeds")),
            "cash_outlay_per_batch": total_cost,
            "material_cost": cls._as_float(manufacturing_job.get("material_cost")),
            "total_job_cost": cls._as_float(manufacturing_job.get("total_job_cost")),
            "slot_hours_per_batch": slot_hours_per_batch,
            "manufacturing_slot_hours_per_batch": manufacturing_slot_hours_per_batch,
            "preparation_slot_hours_per_batch": preparation_slot_hours_per_batch,
            "time_seconds": total_time_seconds,
            "manufacturing_time_seconds": manufacturing_time_seconds,
            "preparation_time_seconds": preparation_time_seconds,
            "region_daily_volume": region_daily_volume,
            "region_daily_volume_7d_avg": region_daily_volume_7d_avg,
            "hub_buy_liquidity": int(row.get("hub_buy_liquidity") or 0),
            "hub_sell_liquidity": int(row.get("hub_sell_liquidity") or 0),
            "pricing_confidence": row.get("pricing_confidence"),
            "pricing_confidence_reasons": row.get("pricing_confidence_reasons") or [],
            "market_price_age_minutes": cls._as_float(row.get("market_price_age_minutes")),
            "estimated_market_absorption_units": estimated_market_absorption_units,
            "max_batches_total": max_batches_total,
            "blueprint_source_kind": manufacturing_job.get("blueprint_source_kind"),
            "owned_input_coverage_fraction": owned_input_coverage_fraction,
            "confidence_penalty_factor": confidence_penalty_factor,
            "effective_profit_per_batch": effective_profit_per_batch,
            "effective_isk_per_hour": effective_isk_per_hour,
            "is_portfolio_candidate": is_portfolio_candidate,
        }

    @classmethod
    def _build_portfolio_candidates(
        cls,
        rows: list[dict[str, Any]],
        *,
        planning_horizon_hours: float,
    ) -> list[dict[str, Any]]:
        candidates = [
            cls._build_portfolio_candidate(row, planning_horizon_hours=planning_horizon_hours)
            for row in rows
            if isinstance(row, dict)
        ]
        return sorted(
            candidates,
            key=lambda candidate: (
                float(candidate.get("effective_profit_per_batch") or 0.0),
                float(candidate.get("effective_isk_per_hour") or 0.0),
            ),
            reverse=True,
        )

    def industry_manufacturing_portfolio_candidates_payload(
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
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        if progress_callback is not None:
            progress_callback(
                0.03,
                "Step 2/11: Preparing portfolio candidate request",
                {"step": 2, "step_count": 11, "stage": "startup"},
            )
        overview_payload = self.industry_manufacturing_product_overview_payload(
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
            progress_callback=(
                None
                if progress_callback is None
                else lambda progress_fraction, progress_label, progress_meta=None: progress_callback(
                    0.05 + (0.82 * float(progress_fraction)),
                    str(progress_label),
                    {
                        **(dict(progress_meta) if isinstance(progress_meta, dict) else {}),
                        "step_count": 11,
                    },
                )
            ),
        )
        rows = self._filter_overview_rows_for_portfolio_candidates(
            cast(list[dict[str, Any]], overview_payload.get("rows") or []),
            enabled_meta_groups=enabled_meta_groups,
            have_skills_only=have_skills_only,
            positive_profit_only=positive_profit_only,
            min_margin_pct=min_margin_pct,
            min_isk_per_hour=min_isk_per_hour,
            min_region_daily_volume=min_region_daily_volume,
        )
        if progress_callback is not None:
            progress_callback(
                0.90,
                "Step 10/11: Building portfolio candidates",
                {"step": 10, "step_count": 11, "stage": "candidates", "rows": len(rows)},
            )
        candidates = self._build_portfolio_candidates(rows, planning_horizon_hours=planning_horizon_hours)
        if progress_callback is not None:
            progress_callback(
                1.0,
                "Step 11/11: Portfolio candidates ready",
                {
                    "step": 11,
                    "step_count": 11,
                    "stage": "completed",
                    "rows": len(rows),
                    "candidate_count": len(candidates),
                },
            )
        return {
            "rows": rows,
            "candidates": candidates,
            "pricing_batch": overview_payload.get("pricing_batch") or {},
            "summary": {
                "row_count": len(rows),
                "candidate_count": len(candidates),
                "portfolio_candidate_count": sum(1 for candidate in candidates if bool(candidate.get("is_portfolio_candidate"))),
                "planning_horizon_hours": float(planning_horizon_hours or 0.0),
            },
        }

    @staticmethod
    def _normalize_fraction(value: Any) -> float:
        try:
            normalized = abs(float(value or 0.0))
        except Exception:
            return 0.0
        while normalized >= 1.0:
            normalized /= 100.0
        return max(0.0, min(normalized, 0.99))

    @classmethod
    def _combine_reductions(cls, reductions: list[Any]) -> float:
        multiplier = 1.0
        for raw_reduction in reductions:
            reduction = cls._normalize_fraction(raw_reduction)
            if reduction <= 0.0:
                continue
            multiplier *= max(0.0, 1.0 - reduction)
        return max(0.0, min(1.0 - multiplier, 0.99))

    @staticmethod
    def _round_duration_seconds(raw_seconds: float) -> int:
        if raw_seconds <= 0:
            return 0
        return max(1, int(math.ceil(raw_seconds)))

    @staticmethod
    def _round_material_quantity(raw_quantity: float, *, minimum_quantity: int) -> int:
        if raw_quantity <= 0:
            return 0
        return max(int(minimum_quantity), int(math.ceil(raw_quantity)))

    @staticmethod
    def _skill_levels_by_name(character_modifier_payload: dict[str, Any] | None) -> dict[str, int]:
        if not isinstance(character_modifier_payload, dict):
            return {}
        out: dict[str, int] = {}
        for entry in character_modifier_payload.get("modifier_skills") or []:
            if not isinstance(entry, dict):
                continue
            skill_name = str(entry.get("type_name") or "").strip()
            if not skill_name:
                continue
            try:
                trained_skill_level = int(entry.get("trained_skill_level") or 0)
            except Exception:
                trained_skill_level = 0
            out[skill_name] = max(trained_skill_level, out.get(skill_name, 0))
        return out

    @classmethod
    def _manufacturing_required_skill_time_reduction(cls, required_skill_entries: list[dict[str, Any]]) -> float:
        reductions: list[float] = []
        for entry in required_skill_entries:
            if not isinstance(entry, dict):
                continue
            skill_name = str(entry.get("type_name") or "").strip()
            if skill_name in {"Industry", "Advanced Industry"}:
                continue
            try:
                trained_skill_level = int(entry.get("trained_skill_level") or 0)
            except Exception:
                trained_skill_level = 0
            if trained_skill_level <= 0:
                continue
            reductions.append(0.01 * trained_skill_level)
        return cls._combine_reductions(reductions)

    @classmethod
    def _skill_time_reduction(
        cls,
        *,
        activity: str,
        skill_levels_by_name: dict[str, int],
        required_skill_entries: list[dict[str, Any]] | None = None,
    ) -> float:
        reductions: list[float] = []
        if activity in {"manufacturing", "reaction"}:
            reductions.append(0.04 * int(skill_levels_by_name.get("Industry", 0) or 0))
            reductions.append(0.03 * int(skill_levels_by_name.get("Advanced Industry", 0) or 0))
            reductions.append(
                cls._manufacturing_required_skill_time_reduction(required_skill_entries or [])
            )
        elif activity == "copying":
            reductions.append(0.05 * int(skill_levels_by_name.get("Science", 0) or 0))
            reductions.append(0.03 * int(skill_levels_by_name.get("Advanced Industry", 0) or 0))
        elif activity == "research_material":
            reductions.append(0.05 * int(skill_levels_by_name.get("Metallurgy", 0) or 0))
            reductions.append(0.03 * int(skill_levels_by_name.get("Advanced Industry", 0) or 0))
        elif activity == "research_time":
            reductions.append(0.05 * int(skill_levels_by_name.get("Research", 0) or 0))
            reductions.append(0.03 * int(skill_levels_by_name.get("Advanced Industry", 0) or 0))
        elif activity == "invention":
            reductions.append(0.03 * int(skill_levels_by_name.get("Advanced Industry", 0) or 0))
        return cls._combine_reductions(reductions)

    @classmethod
    def _implant_reduction(
        cls,
        *,
        character_modifier_payload: dict[str, Any] | None,
        activity: str,
        metric: str,
    ) -> float:
        if not isinstance(character_modifier_payload, dict):
            return 0.0
        reductions: list[float] = []
        valid_activities = set(cls._ACTIVITY_EFFECT_ALIASES.get(activity, (activity,)))
        for implant in character_modifier_payload.get("implants") or []:
            if not isinstance(implant, dict):
                continue
            for modifier in implant.get("modifiers") or []:
                if not isinstance(modifier, dict):
                    continue
                if str(modifier.get("metric") or "") != metric:
                    continue
                modifier_activity = str(modifier.get("activity") or "")
                if modifier_activity not in valid_activities:
                    continue
                reductions.append(modifier.get("value"))
        return cls._combine_reductions(reductions)

    @classmethod
    def _profile_base_reduction(
        cls,
        *,
        profile_payload: dict[str, Any] | None,
        activity: str,
        metric: str,
    ) -> float:
        if not isinstance(profile_payload, dict):
            return 0.0
        if metric == "material":
            if activity not in {"manufacturing", "reaction"}:
                return 0.0
            return cls._normalize_fraction(profile_payload.get("material_efficiency_bonus"))
        if metric == "time":
            return cls._normalize_fraction(profile_payload.get("time_efficiency_bonus"))
        if metric == "cost":
            return cls._normalize_fraction(profile_payload.get("facility_cost_bonus"))
        return 0.0

    @staticmethod
    def _profile_installation_surcharge(profile_payload: dict[str, Any] | None) -> float:
        if not isinstance(profile_payload, dict):
            return 0.0
        try:
            return max(0.0, float(profile_payload.get("installation_cost_modifier") or 0.0))
        except Exception:
            return 0.0

    @staticmethod
    def _infer_manufacturing_group(product_entry: dict[str, Any]) -> str | None:
        group_name = str(product_entry.get("group_name") or "").lower()
        category_name = str(product_entry.get("category_name") or "").lower()
        meta_group_name = str(product_entry.get("meta_group_name") or "").lower()

        if "structure" in category_name or "structure" in group_name:
            return "Structures"
        if "drone" in category_name or "drone" in group_name:
            return "Drones"
        if any(token in group_name for token in ["ammo", "charge", "crystal", "missile", "bomb"]) or "charge" in category_name:
            return "Ammo & Charges"
        if "component" in group_name:
            if "capital" in group_name:
                if "advanced" in group_name or meta_group_name == "tech ii":
                    return "Advanced Capital Components"
                return "Capital Components"
            if "advanced" in group_name or meta_group_name == "tech ii":
                return "Advanced Components"
            return "Advanced Components"

        ship_tokens = [
            "frigate",
            "destroyer",
            "cruiser",
            "battlecruiser",
            "battleship",
            "industrial",
            "shuttle",
            "freighter",
            "carrier",
            "dreadnought",
            "capital",
            "jump freighter",
            "titan",
            "supercarrier",
        ]
        if category_name == "ship" or any(token in group_name for token in ship_tokens):
            advanced = meta_group_name in {"tech ii", "tech iii"} or any(
                token in group_name
                for token in [
                    "assault",
                    "command",
                    "interdictor",
                    "interceptor",
                    "logistics",
                    "marauder",
                    "recon",
                    "strategic",
                    "transport",
                    "covert",
                    "black ops",
                    "expedition",
                    "exhumer",
                ]
            )
            if any(token in group_name for token in ["freighter", "carrier", "dreadnought", "capital", "jump freighter", "titan", "supercarrier"]):
                return "Capital Ships"
            if any(token in group_name for token in ["battleship", "battlecruiser", "orca"]):
                return "Advanced Large Ships" if advanced else "Basic Large Ships"
            if any(token in group_name for token in ["cruiser", "industrial", "barge", "hauler"]):
                return "Advanced Medium Ships" if advanced else "Basic Medium Ships"
            return "Advanced Small Ships" if advanced else "Basic Small Ships"

        if category_name in {"module", "subsystem"} or group_name:
            return "Modules"
        return None

    @classmethod
    def _profile_rig_reduction(
        cls,
        *,
        profile_payload: dict[str, Any] | None,
        activity: str,
        metric: str,
        manufacturing_group: str | None = None,
    ) -> float:
        if not isinstance(profile_payload, dict):
            return 0.0
        valid_activities = set(cls._ACTIVITY_EFFECT_ALIASES.get(activity, (activity,)))
        reductions: list[float] = []
        for rig in profile_payload.get("structure_rigs") or []:
            if not isinstance(rig, dict):
                continue
            for effect in rig.get("effects") or []:
                if not isinstance(effect, dict):
                    continue
                if str(effect.get("metric") or "") != metric:
                    continue
                if str(effect.get("activity") or "") not in valid_activities:
                    continue
                effect_group = str(effect.get("group") or "All")
                if activity == "manufacturing" and manufacturing_group is not None and effect_group not in {"All", manufacturing_group}:
                    continue
                if activity != "manufacturing" and effect_group not in {"All", ""}:
                    continue
                reductions.append(effect.get("value"))

        if reductions:
            return cls._combine_reductions(reductions)

        aggregate_key_by_metric = {
            "material": "structure_rig_material_bonus",
            "time": "structure_rig_time_bonus",
            "cost": "structure_rig_cost_bonus",
        }
        if activity == "manufacturing":
            return cls._normalize_fraction(profile_payload.get(aggregate_key_by_metric.get(metric, "")))
        return 0.0

    @classmethod
    def _system_cost_index(cls, *, profile_payload: dict[str, Any] | None, activity: str) -> float:
        if not isinstance(profile_payload, dict):
            return 0.0
        aliases = set(cls._ACTIVITY_COST_INDEX_ALIASES.get(activity, (activity,)))
        for entry in profile_payload.get("system_cost_indices") or []:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("activity") or "") not in aliases:
                continue
            try:
                return max(0.0, float(entry.get("cost_index") or 0.0))
            except Exception:
                return 0.0
        return 0.0

    def _get_adjusted_market_price_map(self) -> dict[int, dict[str, Any]]:
        now = time.time()
        cache = getattr(self._state, "_industry_adjusted_market_price_cache", None)
        if cache and isinstance(cache, tuple) and len(cache) == 2 and (now - float(cache[0]) < 3600):
            return cache[1]

        if self._state.esi_service is None:
            return {}

        price_rows = self._state.esi_service.get_market_prices()
        price_map: dict[int, dict[str, Any]] = {}
        for row in price_rows or []:
            if not isinstance(row, dict):
                continue
            try:
                type_id = int(row.get("type_id") or 0)
            except Exception:
                continue
            if type_id <= 0:
                continue
            price_map[type_id] = {
                "adjusted_price": row.get("adjusted_price"),
                "average_price": row.get("average_price"),
            }

        self._state._industry_adjusted_market_price_cache = (now, price_map)
        return price_map

    @staticmethod
    def _resolve_eiv_pricing(
        *,
        type_id: int,
        type_payload: dict[str, Any] | None,
        adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[float | None, str | None]:
        pricing = adjusted_price_map.get(int(type_id)) or {}
        adjusted_price = pricing.get("adjusted_price")
        average_price = pricing.get("average_price")
        if adjusted_price is not None:
            try:
                return float(adjusted_price), "esi_adjusted_price"
            except Exception:
                pass
        if average_price is not None:
            try:
                return float(average_price), "esi_average_price"
            except Exception:
                pass
        if isinstance(type_payload, dict) and type_payload.get("base_price") is not None:
            try:
                return float(type_payload.get("base_price")), "sde_base_price"
            except Exception:
                pass
        return None, None

    @classmethod
    def _sum_estimated_item_value(
        cls,
        entries: list[dict[str, Any]],
        *,
        quantity_key: str,
        adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[float | None, int]:
        total = 0.0
        priced_count = 0
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                type_id = int(entry.get("type_id") or 0)
            except Exception:
                type_id = 0
            if type_id <= 0:
                continue
            unit_value, _ = cls._resolve_eiv_pricing(
                type_id=type_id,
                type_payload=entry,
                adjusted_price_map=adjusted_price_map,
            )
            if unit_value is None:
                continue
            try:
                quantity = int(entry.get(quantity_key) or 0)
            except Exception:
                quantity = 0
            total += float(unit_value) * quantity
            priced_count += 1
        if priced_count == 0:
            return None, 0
        return total, priced_count

    @classmethod
    def _resolve_preferred_unit_value(
        cls,
        *,
        type_id: int,
        type_payload: dict[str, Any] | None,
        sell_price_map: dict[int, dict[str, Any]] | None,
        adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[float | None, str | None]:
        payload = type_payload if isinstance(type_payload, dict) else {}

        explicit_unit_price = cls._as_float(payload.get("unit_price"))
        if explicit_unit_price is not None and explicit_unit_price > 0:
            return explicit_unit_price, str(payload.get("price_source") or "explicit_unit_price")

        acquisition_unit_cost = cls._as_float(payload.get("acquisition_unit_cost"))
        if acquisition_unit_cost is not None and acquisition_unit_cost > 0:
            return acquisition_unit_cost, "owned_asset_acquisition_cost"

        sell_pricing = (sell_price_map or {}).get(int(type_id)) or {}
        sell_unit_price = cls._as_float(sell_pricing.get("unit_price"))
        if sell_unit_price is not None and sell_unit_price > 0:
            return sell_unit_price, str(sell_pricing.get("price_source") or "market_sell_price")

        average_price = cls._as_float(payload.get("type_average_price"))
        if average_price is not None and average_price > 0:
            return average_price, "asset_average_price"

        adjusted_price = cls._as_float(payload.get("type_adjusted_price"))
        if adjusted_price is not None and adjusted_price > 0:
            return adjusted_price, "asset_adjusted_price"

        return cls._resolve_eiv_pricing(
            type_id=type_id,
            type_payload=payload,
            adjusted_price_map=adjusted_price_map,
        )

    @classmethod
    def _sum_preferred_item_value(
        cls,
        entries: list[dict[str, Any]],
        *,
        quantity_key: str,
        sell_price_map: dict[int, dict[str, Any]] | None,
        adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[float | None, int]:
        total = 0.0
        priced_count = 0
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                type_id = int(entry.get("type_id") or 0)
            except Exception:
                type_id = 0
            if type_id <= 0:
                continue
            unit_value, _ = cls._resolve_preferred_unit_value(
                type_id=type_id,
                type_payload=entry,
                sell_price_map=sell_price_map,
                adjusted_price_map=adjusted_price_map,
            )
            if unit_value is None:
                continue
            try:
                quantity = int(entry.get(quantity_key) or 0)
            except Exception:
                quantity = 0
            if quantity <= 0:
                continue
            total += float(unit_value) * float(quantity)
            priced_count += 1
        if priced_count == 0:
            return None, 0
        return total, priced_count

    @classmethod
    def _plan_take_or_buy_material_nodes(
        cls,
        entries: list[dict[str, Any]],
        *,
        available_owned_item_quantity_by_type_id: dict[int, int] | None,
        owned_item_unit_cost_by_type_id: dict[int, float] | None,
        sell_price_map: dict[int, dict[str, Any]] | None,
        adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        procurement_materials: list[dict[str, Any]] = []
        material_nodes: list[dict[str, Any]] = []
        available_owned_quantities = available_owned_item_quantity_by_type_id or {}
        owned_item_unit_costs = owned_item_unit_cost_by_type_id or {}

        for entry in entries:
            if not isinstance(entry, dict):
                continue
            type_id = int(entry.get("type_id") or 0)
            quantity = int(entry.get("quantity") or 0)
            if type_id <= 0 or quantity <= 0:
                continue

            available_owned_quantity = int(available_owned_quantities.get(type_id, 0))
            preferred_owned_unit_cost = cls._as_float(owned_item_unit_costs.get(type_id))
            buy_unit_price, buy_price_source = cls._resolve_preferred_unit_value(
                type_id=type_id,
                type_payload=entry,
                sell_price_map=sell_price_map,
                adjusted_price_map=adjusted_price_map,
            )

            recommendation_action = "buy"
            unit_price = buy_unit_price
            price_source = buy_price_source
            if available_owned_quantity >= quantity:
                available_owned_quantities[type_id] = max(0, available_owned_quantity - quantity)
                unit_price = preferred_owned_unit_cost if preferred_owned_unit_cost is not None else buy_unit_price
                price_source = "owned_asset_item_value" if preferred_owned_unit_cost is not None else buy_price_source
                recommendation_action = "take"

            line_total = (
                float(unit_price) * float(quantity)
                if unit_price is not None and quantity > 0
                else None
            )
            procurement_materials.append(
                {
                    **dict(entry),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "price_source": price_source,
                    "sourcing_strategy": recommendation_action,
                    "owned_cost_basis_known": (preferred_owned_unit_cost is not None),
                    "uses_unknown_owned_cost_basis": bool(
                        recommendation_action == "take" and preferred_owned_unit_cost is None
                    ),
                    "line_total": line_total,
                }
            )
            material_nodes.append(
                cls._job_tree_node(
                    label=str(entry.get("type_name") or type_id or "Material"),
                    node_type="material",
                    activity="material",
                    sourcing_strategy=recommendation_action,
                    recommendation_action=recommendation_action,
                    category_name=str(entry.get("category_name") or "") or None,
                    meta_group_name=str(entry.get("meta_group_name") or "") or None,
                    type_id=type_id,
                    quantity=quantity,
                    runs=None,
                    duration_seconds=None,
                    job_cost=None,
                    total_job_cost=None,
                    material_cost=line_total,
                    total_cost=line_total,
                    unit_price=unit_price,
                    price_source=price_source,
                    children=[],
                )
            )

        return procurement_materials, material_nodes

    @classmethod
    def _owned_blueprint_copy_consumption_cost(
        cls,
        asset: CharacterAssetsModel | CorporationAssetsModel | None,
        *,
        consumed_runs: int,
    ) -> float | None:
        if asset is None or consumed_runs <= 0:
            return None
        acquisition_total_cost = cls._as_float(getattr(asset, "acquisition_total_cost", None))
        acquisition_unit_cost = cls._as_float(getattr(asset, "acquisition_unit_cost", None))
        blueprint_runs = max(0, int(getattr(asset, "blueprint_runs", 0) or 0))

        if acquisition_total_cost is not None and acquisition_total_cost > 0:
            if blueprint_runs > 0:
                return float(acquisition_total_cost) * (float(min(consumed_runs, blueprint_runs)) / float(blueprint_runs))
            return float(acquisition_total_cost)
        if acquisition_unit_cost is not None and acquisition_unit_cost > 0:
            if blueprint_runs > 0:
                return float(acquisition_unit_cost) * (float(min(consumed_runs, blueprint_runs)) / float(blueprint_runs))
            return float(acquisition_unit_cost)

        fallback_asset_value = cls._as_float(getattr(asset, "type_average_price", None))
        if fallback_asset_value is None or fallback_asset_value <= 0:
            fallback_asset_value = cls._as_float(getattr(asset, "type_adjusted_price", None))
        if fallback_asset_value is not None and fallback_asset_value > 0:
            if blueprint_runs > 0:
                return float(fallback_asset_value) * (float(min(consumed_runs, blueprint_runs)) / float(blueprint_runs))
            return float(fallback_asset_value)

        return None

    @classmethod
    def _owned_blueprint_copy_node_fields(
        cls,
        asset: CharacterAssetsModel | CorporationAssetsModel | None,
        *,
        blueprint_name: str,
        recommendation_action: str,
        runs_required: int,
        category_name: str | None,
        meta_group_name: str | None,
        use_invention_label: bool,
        character_name_by_id: dict[int, str] | None = None,
        corporation_name_by_id: dict[int, str] | None = None,
    ) -> dict[str, Any]:
        allocated_cost = cls._owned_blueprint_copy_consumption_cost(asset, consumed_runs=runs_required)
        blueprint_copy_payload = cls._compact_owned_blueprint_asset(
            asset,
            character_name_by_id=character_name_by_id,
            corporation_name_by_id=corporation_name_by_id,
        )
        return {
            "item_id": int(getattr(asset, "item_id", 0) or 0) if asset is not None else None,
            "quantity": 1 if asset is not None else None,
            "runs": max(0, int(runs_required or 0)) or None,
            "duration_seconds": None,
            "job_cost": None,
            "total_job_cost": None,
            "material_cost": allocated_cost,
            "total_cost": allocated_cost,
            "recommendation_action": recommendation_action,
            "blueprint_name": blueprint_name,
            "blueprint_copy": blueprint_copy_payload,
            "category_name": category_name,
            "meta_group_name": meta_group_name,
            "node_type": "activity",
            "activity": "invention" if use_invention_label else "copying",
            "children": [],
        }

    @classmethod
    def _allocate_owned_blueprint_copy_run_usage(
        cls,
        assets: list[CharacterAssetsModel | CorporationAssetsModel] | None,
        *,
        requested_runs: int,
        available_total_runs: int,
    ) -> list[tuple[CharacterAssetsModel | CorporationAssetsModel, int]]:
        if not assets or requested_runs <= 0 or available_total_runs <= 0:
            return []

        remaining_requested_runs = max(0, int(requested_runs))
        remaining_available_runs = max(0, int(available_total_runs))
        allocations: list[tuple[CharacterAssetsModel | CorporationAssetsModel, int]] = []

        for asset in assets:
            if remaining_requested_runs <= 0 or remaining_available_runs <= 0:
                break
            asset_runs = max(0, int(getattr(asset, "blueprint_runs", 0) or 0))
            if asset_runs <= 0:
                continue
            consumed_runs = min(asset_runs, remaining_requested_runs, remaining_available_runs)
            if consumed_runs <= 0:
                continue
            allocations.append((asset, consumed_runs))
            remaining_requested_runs -= consumed_runs
            remaining_available_runs -= consumed_runs

        return allocations

    def _normalize_nested_owned_blueprint_copy_tree(
        self,
        tree_node: dict[str, Any] | None,
        *,
        blueprint_type_id: int,
        planned_runs: int,
        blueprint_copy_assets_by_type_id: dict[int, list[Any]],
        category_name: str | None,
        meta_group_name: str | None,
        character_name_by_id: dict[int, str] | None,
        corporation_name_by_id: dict[int, str] | None,
    ) -> dict[str, Any] | None:
        if not isinstance(tree_node, dict):
            return tree_node
        if blueprint_type_id <= 0 or planned_runs <= 0:
            return tree_node

        normalized_tree_node = dict(tree_node)
        matched_assets = cast(
            list[CharacterAssetsModel | CorporationAssetsModel],
            blueprint_copy_assets_by_type_id.get(int(blueprint_type_id)) or [],
        )
        allocations = self._allocate_owned_blueprint_copy_run_usage(
            matched_assets,
            requested_runs=planned_runs,
            available_total_runs=planned_runs,
        )
        if not allocations:
            return normalized_tree_node

        existing_children = [child for child in (tree_node.get("children") or []) if isinstance(child, dict)]
        removed_take_nodes = [
            child
            for child in existing_children
            if str(child.get("activity") or "").strip().lower() in {"copying", "invention"}
            and str(child.get("recommendation_action") or "").strip().lower() == "take"
            and int(child.get("item_id") or 0) > 0
        ]
        preserved_children = [child for child in existing_children if child not in removed_take_nodes]
        use_invention_label = any(
            str(child.get("activity") or "").strip().lower() == "invention"
            for child in removed_take_nodes
        )
        blueprint_name = "Blueprint"
        for child in removed_take_nodes:
            candidate_name = str(child.get("blueprint_name") or "").strip()
            if candidate_name:
                blueprint_name = candidate_name
                break

        normalized_take_nodes = [
            self._job_tree_node(
                label=self._ACTIVITY_LABELS["invention"] if use_invention_label else self._ACTIVITY_LABELS["copying"],
                **self._owned_blueprint_copy_node_fields(
                    asset,
                    blueprint_name=blueprint_name,
                    recommendation_action="take",
                    runs_required=consumed_runs,
                    category_name=category_name,
                    meta_group_name=meta_group_name,
                    use_invention_label=use_invention_label,
                    character_name_by_id=character_name_by_id,
                    corporation_name_by_id=corporation_name_by_id,
                ),
            )
            for asset, consumed_runs in allocations
        ]

        normalized_tree_node["blueprint_copy"] = self._compact_owned_blueprint_assets(
            [asset for asset, _ in allocations],
            character_name_by_id=character_name_by_id,
            corporation_name_by_id=corporation_name_by_id,
        )
        normalized_tree_node["children"] = [*normalized_take_nodes, *preserved_children]
        return normalized_tree_node

    @classmethod
    def _job_cost_total(
        cls,
        *,
        process_value: float | None,
        cost_index: float,
        cost_reduction: float,
        installation_surcharge: float,
    ) -> dict[str, Any]:
        if process_value is None or process_value <= 0 or cost_index <= 0:
            return {
                "process_value": process_value,
                "base_job_cost": None,
                "job_cost_before_surcharge": None,
                "installation_surcharge": None,
                "total_job_cost": None,
            }

        base_job_cost = float(process_value) * float(cost_index)
        discounted_job_cost = base_job_cost * max(0.0, 1.0 - float(cost_reduction))
        surcharge_cost = discounted_job_cost * max(0.0, float(installation_surcharge))
        return {
            "process_value": process_value,
            "base_job_cost": base_job_cost,
            "job_cost_before_surcharge": discounted_job_cost,
            "installation_surcharge": surcharge_cost,
            "total_job_cost": discounted_job_cost + surcharge_cost,
        }

    @classmethod
    def _research_target_duration_seconds(cls, *, level_one_duration_seconds: int, target_level: int) -> int:
        if level_one_duration_seconds <= 0 or target_level <= 0:
            return 0
        rank = float(level_one_duration_seconds) / float(cls._RESEARCH_RANK1_TARGET_DURATION_SECONDS[1])
        rank1_target_seconds = cls._RESEARCH_RANK1_TARGET_DURATION_SECONDS.get(int(target_level), 0)
        return cls._round_duration_seconds(rank * float(rank1_target_seconds))

    @classmethod
    def _job_tree_node(
        cls,
        *,
        label: str,
        node_type: str,
        children: list[dict[str, Any]] | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        return {
            "label": label,
            "node_type": node_type,
            "children": list(children or []),
            **fields,
        }

    @staticmethod
    def _as_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    @classmethod
    def _apply_material_pricing_to_job_tree(
        cls,
        node: dict[str, Any] | None,
        *,
        price_by_type_id: dict[int, dict[str, Any]],
    ) -> float | None:
        if not isinstance(node, dict):
            return None

        children = node.get("children") or []
        child_material_costs: list[float] = []
        if isinstance(children, list):
            for child in children:
                if not isinstance(child, dict):
                    continue
                child_material_cost = cls._apply_material_pricing_to_job_tree(
                    child,
                    price_by_type_id=price_by_type_id,
                )
                if child_material_cost is not None:
                    child_material_costs.append(float(child_material_cost))

        node_type = str(node.get("node_type") or "").strip().lower()
        material_cost = cls._as_float(node.get("material_cost"))

        if node_type == "material":
            if child_material_costs:
                material_cost = sum(child_material_costs)
            else:
                type_id = int(node.get("type_id") or 0)
                quantity = int(node.get("quantity") or 0)
                explicit_unit_price = cls._as_float(node.get("unit_price"))
                if explicit_unit_price is not None and explicit_unit_price > 0:
                    unit_price = explicit_unit_price
                else:
                    pricing = price_by_type_id.get(type_id) or {}
                    unit_price = cls._as_float(pricing.get("unit_price"))
                    node["unit_price"] = unit_price
                    node["price_source"] = pricing.get("price_source")
                    node["price_sample_size"] = pricing.get("sample_size")
                    node["price_cached"] = pricing.get("cached")
                material_cost = (float(unit_price) * quantity) if unit_price is not None and quantity > 0 else None
                node["line_total"] = material_cost
        elif node_type in {"materials", "activity", "product"} and child_material_costs:
            material_cost = sum(child_material_costs)

        node["material_cost"] = material_cost

        job_cost = cls._as_float(node.get("total_job_cost"))
        if job_cost is None:
            job_cost = cls._as_float(node.get("job_cost"))
        node["total_cost"] = (float(job_cost or 0.0) + float(material_cost or 0.0)) if (job_cost is not None or material_cost is not None) else None

        return material_cost

    @staticmethod
    def _profile_system_security_status(profile_payload: dict[str, Any] | None) -> float | None:
        if not isinstance(profile_payload, dict):
            return None
        industry_system = profile_payload.get("industry_system") or {}
        if isinstance(industry_system, dict):
            try:
                security_status = industry_system.get("security_status")
                if security_status is not None:
                    return float(security_status)
            except Exception:
                pass
        try:
            security_status = profile_payload.get("system_security_status")
            if security_status is not None:
                return float(security_status)
        except Exception:
            pass
        return None

    @classmethod
    def _reactions_allowed_for_profile(cls, profile_payload: dict[str, Any] | None) -> bool:
        security_status = cls._profile_system_security_status(profile_payload)
        if security_status is None:
            return True
        return security_status < 0.5

    @staticmethod
    def _aggregate_material_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        aggregated: dict[int, dict[str, Any]] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                type_id = int(entry.get("type_id") or 0)
            except Exception:
                type_id = 0
            if type_id <= 0:
                continue
            existing = aggregated.get(type_id)
            if existing is None:
                aggregated[type_id] = dict(entry)
                continue
            previous_quantity = int(existing.get("quantity") or 0)
            existing_line_total = IndustryService._as_float(existing.get("line_total"))
            if existing_line_total is None:
                existing_unit_price = IndustryService._as_float(existing.get("unit_price"))
                if existing_unit_price is not None and previous_quantity > 0:
                    existing_line_total = float(existing_unit_price) * float(previous_quantity)

            entry_quantity = int(entry.get("quantity") or 0)
            entry_line_total = IndustryService._as_float(entry.get("line_total"))
            if entry_line_total is None:
                entry_unit_price = IndustryService._as_float(entry.get("unit_price"))
                if entry_unit_price is not None and entry_quantity > 0:
                    entry_line_total = float(entry_unit_price) * float(entry_quantity)

            existing["quantity"] = int(existing.get("quantity") or 0) + int(entry.get("quantity") or 0)
            if entry.get("quantity_per_run") is not None or existing.get("quantity_per_run") is not None:
                existing["quantity_per_run"] = int(existing.get("quantity_per_run") or 0) + int(entry.get("quantity_per_run") or 0)
            if entry.get("base_quantity") is not None or existing.get("base_quantity") is not None:
                existing["base_quantity"] = int(existing.get("base_quantity") or 0) + int(entry.get("base_quantity") or 0)
            combined_line_total = None
            if existing_line_total is not None or entry_line_total is not None:
                combined_line_total = float(existing_line_total or 0.0) + float(entry_line_total or 0.0)
                existing["line_total"] = combined_line_total
                total_quantity = int(existing.get("quantity") or 0)
                existing["unit_price"] = (
                    float(combined_line_total) / float(total_quantity)
                    if total_quantity > 0
                    else None
                )
            existing_price_source = str(existing.get("price_source") or "").strip()
            entry_price_source = str(entry.get("price_source") or "").strip()
            if existing_price_source and entry_price_source and existing_price_source != entry_price_source:
                existing["price_source"] = "mixed"
            elif entry_price_source:
                existing["price_source"] = entry_price_source
        return list(aggregated.values())

    @staticmethod
    def _build_blueprint_row_indexes(
        blueprint_rows: list[dict[str, Any]],
    ) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
        manufacturing_row_by_product_type_id: dict[int, dict[str, Any]] = {}
        reaction_row_by_product_type_id: dict[int, dict[str, Any]] = {}
        invention_row_by_blueprint_type_id: dict[int, dict[str, Any]] = {}
        for row in blueprint_rows:
            if not isinstance(row, dict):
                continue
            for product in (row.get("manufacturing_job") or {}).get("products") or []:
                if not isinstance(product, dict):
                    continue
                try:
                    product_type_id = int(product.get("type_id") or 0)
                except Exception:
                    product_type_id = 0
                if product_type_id > 0 and product_type_id not in manufacturing_row_by_product_type_id:
                    manufacturing_row_by_product_type_id[product_type_id] = row
            for product in (row.get("reaction_job") or {}).get("products") or []:
                if not isinstance(product, dict):
                    continue
                try:
                    product_type_id = int(product.get("type_id") or 0)
                except Exception:
                    product_type_id = 0
                if product_type_id > 0 and product_type_id not in reaction_row_by_product_type_id:
                    reaction_row_by_product_type_id[product_type_id] = row
            for product in (row.get("invention_job") or {}).get("products") or []:
                if not isinstance(product, dict):
                    continue
                invention_product = product.get("product") or {}
                if not isinstance(invention_product, dict):
                    invention_product = {}
                try:
                    product_type_id = int(invention_product.get("type_id") or product.get("type_id") or 0)
                except Exception:
                    product_type_id = 0
                if product_type_id > 0 and product_type_id not in invention_row_by_blueprint_type_id:
                    invention_row_by_blueprint_type_id[product_type_id] = row
        return (
            manufacturing_row_by_product_type_id,
            reaction_row_by_product_type_id,
            invention_row_by_blueprint_type_id,
        )

    def _plan_blueprint_chain_for_quantity(
        self,
        *,
        blueprint_row: dict[str, Any],
        activity: str,
        desired_product_type_id: int,
        required_quantity: int,
        build_from_bpc: bool,
        include_reactions: bool,
        selected_industry_profile: dict[str, Any] | None,
        selected_character_modifiers: dict[str, Any] | None,
        character_skill_levels_by_name: dict[str, int],
        adjusted_market_price_map: dict[int, dict[str, Any]],
        sell_price_map: dict[int, dict[str, Any]] | None,
        blueprint_copy_assets_by_type_id: dict[int, list[Any]],
        available_blueprint_copy_runs_by_type_id: dict[int, int] | None,
        available_owned_item_quantity_by_type_id: dict[int, int] | None,
        owned_item_unit_cost_by_type_id: dict[int, float] | None,
        blueprint_original_assets_by_type_id: dict[int, list[Any]],
        character_name_by_id: dict[int, str] | None = None,
        corporation_name_by_id: dict[int, str] | None = None,
        manufacturing_row_by_product_type_id: dict[int, dict[str, Any]],
        reaction_row_by_product_type_id: dict[int, dict[str, Any]],
        invention_row_by_blueprint_type_id: dict[int, dict[str, Any]],
        visited: set[tuple[str, int]] | None = None,
        depth: int = 0,
    ) -> dict[str, Any] | None:
        if depth >= 8 or required_quantity <= 0:
            return None
        visit_key = (activity, int(desired_product_type_id))
        if visited is not None and visit_key in visited:
            return None
        next_visited = set(visited or set())
        next_visited.add(visit_key)

        job = blueprint_row.get(f"{activity}_job") or {}
        if not isinstance(job, dict):
            return None
        products = job.get("products") or []
        if not isinstance(products, list) or not products:
            return None

        selected_product = None
        for product in products:
            if not isinstance(product, dict):
                continue
            if int(product.get("type_id") or 0) == int(desired_product_type_id):
                selected_product = dict(product)
                break
        if selected_product is None:
            selected_product = dict(products[0]) if isinstance(products[0], dict) else None
        if selected_product is None:
            return None

        product_quantity_per_run = max(1, int(selected_product.get("quantity") or 1))
        requested_runs = max(1, int(math.ceil(float(required_quantity) / float(product_quantity_per_run))))
        runs = requested_runs
        planned_quantity = required_quantity

        blueprint_type_id = int(blueprint_row.get("blueprint_type_id") or 0)
        blueprint_payload = blueprint_row.get("blueprint") or {}
        if not isinstance(blueprint_payload, dict):
            blueprint_payload = {}
        blueprint_display_name = str(
            blueprint_payload.get("type_name")
            or blueprint_payload.get("name")
            or blueprint_type_id
            or "Blueprint"
        )
        blueprint_material_efficiency = 0
        blueprint_time_efficiency = 0
        blueprint_source_kind = activity
        include_copying_job = False
        include_sde_research_chain = False
        available_copy_runs = available_blueprint_copy_runs_by_type_id or {}
        available_owned_item_quantities = available_owned_item_quantity_by_type_id or {}
        owned_item_unit_costs = owned_item_unit_cost_by_type_id or {}
        matched_blueprint_copies = blueprint_copy_assets_by_type_id.get(blueprint_type_id) or []
        matched_blueprint_originals = blueprint_original_assets_by_type_id.get(blueprint_type_id) or []
        owned_copy_runs_used = 0
        missing_blueprint_copy_runs = 0
        if activity == "manufacturing":
            if bool(build_from_bpc):
                available_target_copy_runs = int(available_copy_runs.get(blueprint_type_id, 0))
                owned_copy_runs_used = min(max(0, available_target_copy_runs), requested_runs)
                missing_blueprint_copy_runs = max(0, requested_runs - owned_copy_runs_used)
                available_copy_runs[blueprint_type_id] = max(0, available_target_copy_runs - owned_copy_runs_used)
                if owned_copy_runs_used > 0 and matched_blueprint_copies:
                    blueprint_material_efficiency = int(matched_blueprint_copies[0].blueprint_material_efficiency or 0)
                    blueprint_time_efficiency = int(matched_blueprint_copies[0].blueprint_time_efficiency or 0)
                    blueprint_source_kind = "owned_blueprint_copy"
                    if missing_blueprint_copy_runs > 0 and bool(matched_blueprint_originals):
                        include_copying_job = True
                    elif missing_blueprint_copy_runs > 0:
                        runs = owned_copy_runs_used
                        planned_quantity = min(required_quantity, product_quantity_per_run * runs)
                        missing_blueprint_copy_runs = 0
                elif matched_blueprint_originals:
                    blueprint_material_efficiency = int(matched_blueprint_originals[0].blueprint_material_efficiency or 0)
                    blueprint_time_efficiency = int(matched_blueprint_originals[0].blueprint_time_efficiency or 0)
                    blueprint_source_kind = "copied_from_owned_blueprint_original"
                    include_copying_job = missing_blueprint_copy_runs > 0
                else:
                    blueprint_source_kind = "unowned_blueprint_copy"
                    include_copying_job = missing_blueprint_copy_runs > 0
                if runs <= 0:
                    return None
            else:
                if matched_blueprint_originals:
                    blueprint_material_efficiency = int(matched_blueprint_originals[0].blueprint_material_efficiency or 0)
                    blueprint_time_efficiency = int(matched_blueprint_originals[0].blueprint_time_efficiency or 0)
                    blueprint_source_kind = "owned_blueprint_original"
                else:
                    blueprint_material_efficiency = self._MAX_BLUEPRINT_MATERIAL_EFFICIENCY
                    blueprint_time_efficiency = self._MAX_BLUEPRINT_TIME_EFFICIENCY
                    blueprint_source_kind = "blueprint_sde_fallback"
                    include_sde_research_chain = True

        required_skill_entries = [
            dict(skill)
            for skill in (job.get("skill_entries") or [])
            if isinstance(skill, dict)
        ]
        manufacturing_group = self._infer_manufacturing_group(selected_product) if activity == "manufacturing" else None

        material_reduction = self._combine_reductions(
            [
                (float(blueprint_material_efficiency) / 100.0) if activity == "manufacturing" else 0.0,
                self._profile_base_reduction(
                    profile_payload=selected_industry_profile,
                    activity=activity,
                    metric="material",
                ),
                self._profile_rig_reduction(
                    profile_payload=selected_industry_profile,
                    activity=activity,
                    metric="material",
                    manufacturing_group=manufacturing_group,
                ),
                self._implant_reduction(
                    character_modifier_payload=selected_character_modifiers,
                    activity=activity,
                    metric="material",
                ),
            ]
        )
        time_reduction = self._combine_reductions(
            [
                (float(blueprint_time_efficiency) / 100.0) if activity == "manufacturing" else 0.0,
                self._skill_time_reduction(
                    activity=activity,
                    skill_levels_by_name=character_skill_levels_by_name,
                    required_skill_entries=required_skill_entries,
                ),
                self._profile_base_reduction(
                    profile_payload=selected_industry_profile,
                    activity=activity,
                    metric="time",
                ),
                self._profile_rig_reduction(
                    profile_payload=selected_industry_profile,
                    activity=activity,
                    metric="time",
                    manufacturing_group=manufacturing_group,
                ),
                self._implant_reduction(
                    character_modifier_payload=selected_character_modifiers,
                    activity=activity,
                    metric="time",
                ),
            ]
        )
        cost_reduction = self._combine_reductions(
            [
                self._profile_base_reduction(
                    profile_payload=selected_industry_profile,
                    activity=activity,
                    metric="cost",
                ),
                self._profile_rig_reduction(
                    profile_payload=selected_industry_profile,
                    activity=activity,
                    metric="cost",
                    manufacturing_group=manufacturing_group,
                ),
                self._implant_reduction(
                    character_modifier_payload=selected_character_modifiers,
                    activity=activity,
                    metric="cost",
                ),
            ]
        )

        adjusted_material_entries: list[dict[str, Any]] = []
        base_material_entries: list[dict[str, Any]] = []
        for raw_material in [dict(entry) for entry in (job.get("materials") or []) if isinstance(entry, dict)]:
            material = dict(raw_material)
            quantity_per_run = int(material.get("quantity") or 0)
            base_total_quantity = quantity_per_run * runs
            adjusted_total_quantity = self._round_material_quantity(
                float(base_total_quantity) * max(0.0, 1.0 - material_reduction),
                minimum_quantity=(runs if quantity_per_run > 0 else 0),
            )
            material["quantity_per_run"] = quantity_per_run
            material["base_quantity"] = base_total_quantity
            material["quantity"] = adjusted_total_quantity
            material["material_reduction"] = material_reduction
            adjusted_material_entries.append(material)
            base_material_entries.append({**material, "quantity": base_total_quantity})

        process_value, priced_material_count = self._sum_estimated_item_value(
            base_material_entries,
            quantity_key="quantity",
            adjusted_price_map=adjusted_market_price_map,
        )
        cost_index = self._system_cost_index(profile_payload=selected_industry_profile, activity=activity)
        installation_surcharge = self._profile_installation_surcharge(selected_industry_profile)
        job_cost = self._job_cost_total(
            process_value=process_value,
            cost_index=cost_index,
            cost_reduction=cost_reduction,
            installation_surcharge=installation_surcharge,
        )
        direct_duration_seconds = self._round_duration_seconds(
            int(job.get("time_seconds") or 0) * runs * max(0.0, 1.0 - time_reduction)
        )

        total_time_seconds = direct_duration_seconds
        total_job_cost = float(job_cost.get("total_job_cost") or 0.0)
        priced_job_count = 1 if job_cost.get("total_job_cost") is not None else 0
        leaf_materials: list[dict[str, Any]] = []
        recursive_activity_breakdown: dict[str, Any] = {}
        prerequisite_nodes: list[dict[str, Any]] = []
        material_nodes: list[dict[str, Any]] = []

        per_run_material_eiv, _ = self._sum_estimated_item_value(
            adjusted_material_entries,
            quantity_key="quantity_per_run",
            adjusted_price_map=adjusted_market_price_map,
        )

        invention_source_row = invention_row_by_blueprint_type_id.get(blueprint_type_id) if activity == "manufacturing" else None
        has_invention_path = isinstance((invention_source_row or {}).get("invention_job"), dict)
        owned_blueprint_copy_allocations = self._allocate_owned_blueprint_copy_run_usage(
            cast(list[CharacterAssetsModel | CorporationAssetsModel], matched_blueprint_copies),
            requested_runs=int(owned_copy_runs_used or 0),
            available_total_runs=int(owned_copy_runs_used or 0),
        )
        selected_blueprint_copy_asset = (
            owned_blueprint_copy_allocations[0][0]
            if owned_blueprint_copy_allocations
            else (matched_blueprint_copies[0] if matched_blueprint_copies else None)
        )
        selected_blueprint_original_asset = matched_blueprint_originals[0] if matched_blueprint_originals else None
        blueprint_copy_payload = self._compact_owned_blueprint_assets(
            [asset for asset, _ in owned_blueprint_copy_allocations]
            if owned_blueprint_copy_allocations
            else ([selected_blueprint_copy_asset] if selected_blueprint_copy_asset is not None else []),
            character_name_by_id=character_name_by_id,
            corporation_name_by_id=corporation_name_by_id,
        )
        uses_blueprint_original = (
            blueprint_source_kind in {"copied_from_owned_blueprint_original", "owned_blueprint_original"}
            or (include_copying_job and missing_blueprint_copy_runs > 0)
        )
        blueprint_original_payload = self._compact_owned_blueprint_asset(
            selected_blueprint_original_asset if uses_blueprint_original else None,
            character_name_by_id=character_name_by_id,
            corporation_name_by_id=corporation_name_by_id,
        )
        if activity == "manufacturing" and bool(build_from_bpc) and owned_blueprint_copy_allocations:
            for consumed_blueprint_copy_asset, consumed_copy_runs in owned_blueprint_copy_allocations:
                current_copy_node_fields = self._owned_blueprint_copy_node_fields(
                    consumed_blueprint_copy_asset,
                    blueprint_name=blueprint_display_name,
                    recommendation_action="take",
                    runs_required=consumed_copy_runs,
                    category_name=str(selected_product.get("category_name") or "") or None,
                    meta_group_name=str(selected_product.get("meta_group_name") or "") or None,
                    use_invention_label=bool(has_invention_path),
                    character_name_by_id=character_name_by_id,
                    corporation_name_by_id=corporation_name_by_id,
                )
                prerequisite_nodes.append(
                    self._job_tree_node(
                        label=self._ACTIVITY_LABELS["invention"] if has_invention_path else self._ACTIVITY_LABELS["copying"],
                        **current_copy_node_fields,
                    )
                )

        if include_copying_job and missing_blueprint_copy_runs > 0:
            copying_time_reduction = self._combine_reductions(
                [
                    self._skill_time_reduction(
                        activity="copying",
                        skill_levels_by_name=character_skill_levels_by_name,
                    ),
                    self._profile_base_reduction(
                        profile_payload=selected_industry_profile,
                        activity="copying",
                        metric="time",
                    ),
                    self._profile_rig_reduction(
                        profile_payload=selected_industry_profile,
                        activity="copying",
                        metric="time",
                    ),
                    self._implant_reduction(
                        character_modifier_payload=selected_character_modifiers,
                        activity="copying",
                        metric="time",
                    ),
                ]
            )
            copying_cost_reduction = self._combine_reductions(
                [
                    self._profile_base_reduction(
                        profile_payload=selected_industry_profile,
                        activity="copying",
                        metric="cost",
                    ),
                    self._profile_rig_reduction(
                        profile_payload=selected_industry_profile,
                        activity="copying",
                        metric="cost",
                    ),
                    self._implant_reduction(
                        character_modifier_payload=selected_character_modifiers,
                        activity="copying",
                        metric="cost",
                    ),
                ]
            )
            copying_cost_index = self._system_cost_index(
                profile_payload=selected_industry_profile,
                activity="copying",
            )
            base_copy_time_seconds = int(((blueprint_row.get("copying_job") or {}).get("time_seconds") or 0)) * missing_blueprint_copy_runs
            copy_process_value = (
                float(per_run_material_eiv or 0.0) * missing_blueprint_copy_runs * 0.02
                if per_run_material_eiv is not None
                else None
            )
            copying_job_cost = self._job_cost_total(
                process_value=copy_process_value,
                cost_index=copying_cost_index,
                cost_reduction=copying_cost_reduction,
                installation_surcharge=installation_surcharge,
            )
            copying_duration_seconds = self._round_duration_seconds(
                float(base_copy_time_seconds) * max(0.0, 1.0 - copying_time_reduction)
            )
            total_time_seconds += copying_duration_seconds
            if copying_job_cost.get("total_job_cost") is not None:
                total_job_cost += float(copying_job_cost.get("total_job_cost") or 0.0)
                priced_job_count += 1
            recursive_activity_breakdown["copying"] = {
                "activity": "copying",
                "duration_seconds": copying_duration_seconds,
                "job_cost": copying_job_cost.get("total_job_cost"),
                "runs": missing_blueprint_copy_runs,
            }
            prerequisite_nodes.append(
                self._job_tree_node(
                    label=self._ACTIVITY_LABELS["copying"],
                    node_type="activity",
                    activity="copying",
                    blueprint_name=blueprint_display_name,
                    runs=missing_blueprint_copy_runs,
                    quantity=1,
                    duration_seconds=copying_duration_seconds,
                    direct_duration_seconds=copying_duration_seconds,
                    job_cost=copying_job_cost.get("total_job_cost"),
                    total_job_cost=copying_job_cost.get("total_job_cost"),
                    category_name=str(selected_product.get("category_name") or "") or None,
                    meta_group_name=str(selected_product.get("meta_group_name") or "") or None,
                    recommendation_action="copy",
                    children=[],
                )
            )

        if include_sde_research_chain:
            for activity_name, source_field in [
                ("research_material", "research_material_job"),
                ("research_time", "research_time_job"),
            ]:
                base_level_one_duration = int(((blueprint_row.get(source_field) or {}).get("time_seconds") or 0))
                target_duration_seconds = self._research_target_duration_seconds(
                    level_one_duration_seconds=base_level_one_duration,
                    target_level=self._MAX_RESEARCH_LEVEL,
                )
                if target_duration_seconds <= 0:
                    continue
                research_time_reduction = self._combine_reductions(
                    [
                        self._skill_time_reduction(
                            activity=activity_name,
                            skill_levels_by_name=character_skill_levels_by_name,
                        ),
                        self._profile_base_reduction(
                            profile_payload=selected_industry_profile,
                            activity=activity_name,
                            metric="time",
                        ),
                        self._profile_rig_reduction(
                            profile_payload=selected_industry_profile,
                            activity=activity_name,
                            metric="time",
                        ),
                        self._implant_reduction(
                            character_modifier_payload=selected_character_modifiers,
                            activity=activity_name,
                            metric="time",
                        ),
                    ]
                )
                research_cost_reduction = self._combine_reductions(
                    [
                        self._profile_base_reduction(
                            profile_payload=selected_industry_profile,
                            activity=activity_name,
                            metric="cost",
                        ),
                        self._profile_rig_reduction(
                            profile_payload=selected_industry_profile,
                            activity=activity_name,
                            metric="cost",
                        ),
                        self._implant_reduction(
                            character_modifier_payload=selected_character_modifiers,
                            activity=activity_name,
                            metric="cost",
                        ),
                    ]
                )
                research_job_cost = self._job_cost_total(
                    process_value=(
                        float(per_run_material_eiv or 0.0) * 0.02105 * float(target_duration_seconds)
                        if per_run_material_eiv is not None
                        else None
                    ),
                    cost_index=self._system_cost_index(
                        profile_payload=selected_industry_profile,
                        activity=activity_name,
                    ),
                    cost_reduction=research_cost_reduction,
                    installation_surcharge=installation_surcharge,
                )
                research_duration_seconds = self._round_duration_seconds(
                    float(target_duration_seconds) * max(0.0, 1.0 - research_time_reduction)
                )
                total_time_seconds += research_duration_seconds
                if research_job_cost.get("total_job_cost") is not None:
                    total_job_cost += float(research_job_cost.get("total_job_cost") or 0.0)
                    priced_job_count += 1
                recursive_activity_breakdown[activity_name] = {
                    "activity": activity_name,
                    "duration_seconds": research_duration_seconds,
                    "job_cost": research_job_cost.get("total_job_cost"),
                }
                prerequisite_nodes.append(
                    self._job_tree_node(
                        label=self._ACTIVITY_LABELS.get(activity_name, activity_name),
                        node_type="activity",
                        activity=activity_name,
                        duration_seconds=research_duration_seconds,
                        direct_duration_seconds=research_duration_seconds,
                        job_cost=research_job_cost.get("total_job_cost"),
                        total_job_cost=research_job_cost.get("total_job_cost"),
                        children=[],
                    )
                )

        for material in adjusted_material_entries:
            child_type_id = int(material.get("type_id") or 0)
            child_quantity = int(material.get("quantity") or 0)
            if child_type_id <= 0 or child_quantity <= 0:
                continue

            available_owned_quantity = int(available_owned_item_quantities.get(child_type_id, 0))
            preferred_owned_unit_cost = self._as_float(owned_item_unit_costs.get(child_type_id))
            buy_unit_price, buy_price_source = self._resolve_preferred_unit_value(
                type_id=child_type_id,
                type_payload=material,
                sell_price_map=sell_price_map,
                adjusted_price_map=adjusted_market_price_map,
            )

            owned_quantity_to_take = min(max(0, available_owned_quantity), child_quantity)
            if owned_quantity_to_take > 0:
                available_owned_item_quantities[child_type_id] = max(0, available_owned_quantity - owned_quantity_to_take)
                take_unit_price = preferred_owned_unit_cost if preferred_owned_unit_cost is not None else buy_unit_price
                take_price_source = "owned_asset_item_value" if preferred_owned_unit_cost is not None else buy_price_source
                line_total = (
                    float(take_unit_price) * float(owned_quantity_to_take)
                    if take_unit_price is not None and owned_quantity_to_take > 0
                    else None
                )
                leaf_materials.append(
                    {
                        **dict(material),
                        "quantity": owned_quantity_to_take,
                        "unit_price": take_unit_price,
                        "price_source": take_price_source,
                        "line_total": line_total,
                    }
                )
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or child_type_id),
                        node_type="material",
                        activity="material",
                        sourcing_strategy="take",
                        recommendation_action="take",
                        category_name=str(material.get("category_name") or "") or None,
                        meta_group_name=str(material.get("meta_group_name") or "") or None,
                        type_id=child_type_id,
                        quantity=owned_quantity_to_take,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=None,
                        material_cost=line_total,
                        total_cost=line_total,
                        unit_price=take_unit_price,
                        price_source=take_price_source,
                        children=[],
                    )
                )
            remaining_child_quantity = max(0, child_quantity - owned_quantity_to_take)
            if remaining_child_quantity <= 0:
                continue

            child_row = None
            child_activity = "manufacturing"
            if include_reactions and child_type_id in reaction_row_by_product_type_id:
                child_row = reaction_row_by_product_type_id.get(child_type_id)
                child_activity = "reaction"
            elif child_type_id in manufacturing_row_by_product_type_id:
                child_row = manufacturing_row_by_product_type_id.get(child_type_id)

            child_plan = None
            if child_row is not None and child_type_id != int(desired_product_type_id):
                child_plan = self._plan_blueprint_chain_for_quantity(
                    blueprint_row=child_row,
                    activity=child_activity,
                    desired_product_type_id=child_type_id,
                    required_quantity=remaining_child_quantity,
                    build_from_bpc=build_from_bpc,
                    include_reactions=include_reactions,
                    selected_industry_profile=selected_industry_profile,
                    selected_character_modifiers=selected_character_modifiers,
                    character_skill_levels_by_name=character_skill_levels_by_name,
                    adjusted_market_price_map=adjusted_market_price_map,
                    sell_price_map=sell_price_map,
                    blueprint_copy_assets_by_type_id=blueprint_copy_assets_by_type_id,
                    available_blueprint_copy_runs_by_type_id=available_blueprint_copy_runs_by_type_id,
                    available_owned_item_quantity_by_type_id=available_owned_item_quantity_by_type_id,
                    owned_item_unit_cost_by_type_id=owned_item_unit_cost_by_type_id,
                    blueprint_original_assets_by_type_id=blueprint_original_assets_by_type_id,
                    character_name_by_id=character_name_by_id,
                    corporation_name_by_id=corporation_name_by_id,
                    manufacturing_row_by_product_type_id=manufacturing_row_by_product_type_id,
                    reaction_row_by_product_type_id=reaction_row_by_product_type_id,
                    invention_row_by_blueprint_type_id=invention_row_by_blueprint_type_id,
                    visited=next_visited,
                    depth=depth + 1,
                )
            if child_plan:
                planned_child_quantity = min(
                    remaining_child_quantity,
                    max(0, int(child_plan.get("planned_quantity") or remaining_child_quantity)),
                )
                uncovered_child_quantity = max(0, remaining_child_quantity - planned_child_quantity)
                build_total_cost = self._as_float(child_plan.get("estimated_total_cost"))
                buy_total_cost = (
                    float(buy_unit_price) * float(planned_child_quantity)
                    if buy_unit_price is not None and planned_child_quantity > 0
                    else None
                )
                choose_build = (
                    planned_child_quantity > 0
                    and child_plan.get("tree_node") is not None
                    and
                    build_total_cost is not None
                    and (buy_total_cost is None or build_total_cost <= buy_total_cost)
                )

                if choose_build:
                    total_time_seconds += int(child_plan.get("total_time_seconds") or 0)
                    if child_plan.get("total_job_cost") is not None:
                        total_job_cost += float(child_plan.get("total_job_cost") or 0.0)
                        priced_job_count += int(child_plan.get("priced_job_count") or 0)
                    leaf_materials.extend([dict(entry) for entry in (child_plan.get("leaf_materials") or []) if isinstance(entry, dict)])
                    recursive_activity_breakdown[f"{child_activity}:{child_type_id}"] = {
                        "activity": child_activity,
                        "type_id": child_type_id,
                        "type_name": material.get("type_name"),
                        "quantity": planned_child_quantity,
                        "time_seconds": child_plan.get("total_time_seconds"),
                        "job_cost": child_plan.get("total_job_cost"),
                        "direct_job_cost": child_plan.get("direct_job_cost"),
                        "recommended_action": "build",
                    }
                    child_tree_node = self._normalize_nested_owned_blueprint_copy_tree(
                        cast(dict[str, Any] | None, child_plan.get("tree_node")),
                        blueprint_type_id=int(child_plan.get("blueprint_type_id") or 0),
                        planned_runs=int(child_plan.get("runs") or 0),
                        blueprint_copy_assets_by_type_id=blueprint_copy_assets_by_type_id,
                        category_name=str(child_plan.get("category_name") or material.get("category_name") or "") or None,
                        meta_group_name=str(child_plan.get("meta_group_name") or material.get("meta_group_name") or "") or None,
                        character_name_by_id=character_name_by_id,
                        corporation_name_by_id=corporation_name_by_id,
                    )
                    material_nodes.append(
                        self._job_tree_node(
                            label=str(material.get("type_name") or child_type_id),
                            node_type="material",
                            activity="material",
                            sourcing_strategy="build",
                            recommendation_action="build",
                            category_name=str(child_plan.get("category_name") or material.get("category_name") or "") or None,
                            meta_group_name=str(child_plan.get("meta_group_name") or material.get("meta_group_name") or "") or None,
                            type_id=child_type_id,
                            quantity=planned_child_quantity,
                            runs=child_plan.get("runs"),
                            duration_seconds=child_plan.get("total_time_seconds"),
                            job_cost=None,
                            total_job_cost=child_plan.get("total_job_cost"),
                            blueprint_copy=child_plan.get("blueprint_copy") or {},
                            blueprint_original=child_plan.get("blueprint_original") or {},
                            children=[child_tree_node] if isinstance(child_tree_node, dict) else [],
                        )
                    )
                    if uncovered_child_quantity > 0:
                        uncovered_buy_total_cost = (
                            float(buy_unit_price) * float(uncovered_child_quantity)
                            if buy_unit_price is not None and uncovered_child_quantity > 0
                            else None
                        )
                        leaf_materials.append(
                            {
                                **dict(material),
                                "quantity": uncovered_child_quantity,
                                "unit_price": buy_unit_price,
                                "price_source": buy_price_source,
                                "line_total": uncovered_buy_total_cost,
                            }
                        )
                        material_nodes.append(
                            self._job_tree_node(
                                label=str(material.get("type_name") or child_type_id),
                                node_type="material",
                                activity="material",
                                sourcing_strategy="buy",
                                recommendation_action="buy",
                                category_name=str(material.get("category_name") or "") or None,
                                meta_group_name=str(material.get("meta_group_name") or "") or None,
                                type_id=child_type_id,
                                quantity=uncovered_child_quantity,
                                runs=None,
                                duration_seconds=None,
                                job_cost=None,
                                total_job_cost=None,
                                material_cost=uncovered_buy_total_cost,
                                total_cost=uncovered_buy_total_cost,
                                unit_price=buy_unit_price,
                                price_source=buy_price_source,
                                children=[],
                            )
                        )
                else:
                    leaf_materials.append(
                        {
                            **dict(material),
                            "quantity": remaining_child_quantity,
                            "unit_price": buy_unit_price,
                            "price_source": buy_price_source,
                            "line_total": buy_total_cost,
                        }
                    )
                    material_nodes.append(
                        self._job_tree_node(
                            label=str(material.get("type_name") or child_type_id),
                            node_type="material",
                            activity="material",
                            sourcing_strategy="buy",
                            recommendation_action="buy",
                            category_name=str(material.get("category_name") or "") or None,
                            meta_group_name=str(material.get("meta_group_name") or "") or None,
                            type_id=child_type_id,
                            quantity=remaining_child_quantity,
                            runs=None,
                            duration_seconds=None,
                            job_cost=None,
                            total_job_cost=None,
                            material_cost=buy_total_cost,
                            total_cost=buy_total_cost,
                            unit_price=buy_unit_price,
                            price_source=buy_price_source,
                            children=[],
                        )
                    )
            else:
                buy_total_cost = (
                    float(buy_unit_price) * float(remaining_child_quantity)
                    if buy_unit_price is not None and remaining_child_quantity > 0
                    else None
                )
                leaf_materials.append(
                    {
                        **dict(material),
                        "quantity": remaining_child_quantity,
                        "unit_price": buy_unit_price,
                        "price_source": buy_price_source,
                        "line_total": buy_total_cost,
                    }
                )
                sourcing_strategy = (
                    "buy_reaction_available"
                    if not include_reactions and child_type_id in reaction_row_by_product_type_id
                    else "buy"
                )
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or child_type_id),
                        node_type="material",
                        activity="material",
                        sourcing_strategy=sourcing_strategy,
                        recommendation_action="buy",
                        category_name=str(material.get("category_name") or "") or None,
                        meta_group_name=str(material.get("meta_group_name") or "") or None,
                        type_id=child_type_id,
                        quantity=remaining_child_quantity,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=None,
                        material_cost=buy_total_cost,
                        total_cost=buy_total_cost,
                        unit_price=buy_unit_price,
                        price_source=buy_price_source,
                        children=[],
                    )
                )

        if activity == "manufacturing" and missing_blueprint_copy_runs > 0 and not matched_blueprint_originals:
            invention_source_row = invention_row_by_blueprint_type_id.get(blueprint_type_id)
            invention_job = (invention_source_row or {}).get("invention_job") or {}
            if invention_source_row and isinstance(invention_job, dict):
                invention_blueprint_payload = invention_source_row.get("blueprint") or {}
                if not isinstance(invention_blueprint_payload, dict):
                    invention_blueprint_payload = {}
                invention_blueprint_name = str(
                    invention_blueprint_payload.get("type_name")
                    or invention_blueprint_payload.get("name")
                    or invention_source_row.get("blueprint_type_id")
                    or "Blueprint"
                )
                target_entry = None
                for product in invention_job.get("products") or []:
                    if not isinstance(product, dict):
                        continue
                    invention_product = product.get("product") or {}
                    if not isinstance(invention_product, dict):
                        invention_product = {}
                    if int(invention_product.get("type_id") or product.get("type_id") or 0) == blueprint_type_id:
                        target_entry = product
                        break
                probability = 0.0
                if isinstance(target_entry, dict):
                    try:
                        probability = float(target_entry.get("probability_pct") or 0.0) / 100.0
                    except Exception:
                        probability = 0.0
                target_blueprint_name = str(
                    ((target_entry or {}).get("product") or {}).get("type_name")
                    if isinstance((target_entry or {}).get("product"), dict)
                    else ""
                ).strip() or blueprint_display_name
                target_blueprint_copy_runs = max(1, int((job.get("max_production_limit") or 0) or 1))
                successful_invention_jobs = max(
                    1,
                    int(math.ceil(float(missing_blueprint_copy_runs) / float(target_blueprint_copy_runs))),
                )
                invention_attempts = max(
                    successful_invention_jobs,
                    int(math.ceil(float(successful_invention_jobs) / max(probability, 0.01))),
                )
                invention_materials = [
                    dict(entry)
                    for entry in (invention_job.get("materials") or [])
                    if isinstance(entry, dict)
                ]
                invention_process_value, _ = self._sum_estimated_item_value(
                    [{**entry, "quantity": int(entry.get("quantity") or 0) * invention_attempts} for entry in invention_materials],
                    quantity_key="quantity",
                    adjusted_price_map=adjusted_market_price_map,
                )
                invention_time_reduction = self._combine_reductions(
                    [
                        self._skill_time_reduction(activity="invention", skill_levels_by_name=character_skill_levels_by_name),
                        self._profile_base_reduction(
                            profile_payload=selected_industry_profile,
                            activity="invention",
                            metric="time",
                        ),
                        self._profile_rig_reduction(
                            profile_payload=selected_industry_profile,
                            activity="invention",
                            metric="time",
                        ),
                        self._implant_reduction(
                            character_modifier_payload=selected_character_modifiers,
                            activity="invention",
                            metric="time",
                        ),
                    ]
                )
                invention_cost_reduction = self._combine_reductions(
                    [
                        self._profile_base_reduction(
                            profile_payload=selected_industry_profile,
                            activity="invention",
                            metric="cost",
                        ),
                        self._profile_rig_reduction(
                            profile_payload=selected_industry_profile,
                            activity="invention",
                            metric="cost",
                        ),
                        self._implant_reduction(
                            character_modifier_payload=selected_character_modifiers,
                            activity="invention",
                            metric="cost",
                        ),
                    ]
                )
                invention_job_cost = self._job_cost_total(
                    process_value=invention_process_value,
                    cost_index=self._system_cost_index(profile_payload=selected_industry_profile, activity="invention"),
                    cost_reduction=invention_cost_reduction,
                    installation_surcharge=installation_surcharge,
                )
                invention_duration_seconds = self._round_duration_seconds(
                    int(invention_job.get("time_seconds") or 0) * invention_attempts * max(0.0, 1.0 - invention_time_reduction)
                )
                total_time_seconds += invention_duration_seconds
                if invention_job_cost.get("total_job_cost") is not None:
                    total_job_cost += float(invention_job_cost.get("total_job_cost") or 0.0)
                    priced_job_count += 1
                recursive_activity_breakdown[f"invention:{blueprint_type_id}"] = {
                    "activity": "invention",
                    "blueprint_type_id": blueprint_type_id,
                    "attempts": invention_attempts,
                    "successful_jobs": successful_invention_jobs,
                    "time_seconds": invention_duration_seconds,
                    "job_cost": invention_job_cost.get("total_job_cost"),
                }
                leaf_materials.extend(
                    [{**entry, "quantity": int(entry.get("quantity") or 0) * invention_attempts} for entry in invention_materials]
                )
                generated_target_copy_runs = successful_invention_jobs * target_blueprint_copy_runs
                if generated_target_copy_runs > missing_blueprint_copy_runs:
                    available_copy_runs[blueprint_type_id] = max(
                        0,
                        int(available_copy_runs.get(blueprint_type_id, 0))
                        + (generated_target_copy_runs - missing_blueprint_copy_runs),
                    )
                prerequisite_nodes.append(
                    self._job_tree_node(
                        label=self._ACTIVITY_LABELS["invention"],
                        node_type="activity",
                        activity="invention",
                        blueprint_name=target_blueprint_name,
                        runs=invention_attempts,
                        duration_seconds=invention_duration_seconds,
                        direct_duration_seconds=invention_duration_seconds,
                        job_cost=invention_job_cost.get("total_job_cost"),
                        total_job_cost=invention_job_cost.get("total_job_cost"),
                        category_name=str(selected_product.get("category_name") or "") or None,
                        meta_group_name=str(selected_product.get("meta_group_name") or "") or None,
                        recommendation_action="invent",
                        children=[
                            self._job_tree_node(
                                label=self._ACTIVITY_LABELS["materials"],
                                node_type="materials",
                                activity="materials",
                                children=[
                                    self._job_tree_node(
                                        label=str(entry.get("type_name") or entry.get("type_id") or "Material"),
                                        node_type="material",
                                        activity="material",
                                        type_id=int(entry.get("type_id") or 0),
                                        quantity=int(entry.get("quantity") or 0) * invention_attempts,
                                        runs=None,
                                        duration_seconds=None,
                                        job_cost=None,
                                        total_job_cost=None,
                                        children=[],
                                    )
                                    for entry in invention_materials
                                ],
                            )
                        ],
                    )
                )

                source_copying_job = (invention_source_row.get("copying_job") or {}) if isinstance(invention_source_row, dict) else {}
                source_blueprint_type_id = int((invention_source_row or {}).get("blueprint_type_id") or 0)
                available_source_copy_runs = int(available_copy_runs.get(source_blueprint_type_id, 0))
                source_copy_runs_used = min(max(0, available_source_copy_runs), invention_attempts)
                available_copy_runs[source_blueprint_type_id] = max(0, available_source_copy_runs - source_copy_runs_used)
                missing_source_copy_runs = max(0, invention_attempts - source_copy_runs_used)
                if isinstance(source_copying_job, dict) and int(source_copying_job.get("time_seconds") or 0) > 0 and missing_source_copy_runs > 0:
                    source_manufacturing_materials = [
                        dict(entry)
                        for entry in ((invention_source_row.get("manufacturing_job") or {}).get("materials") or [])
                        if isinstance(entry, dict)
                    ]
                    source_copy_process_value, _ = self._sum_estimated_item_value(
                        source_manufacturing_materials,
                        quantity_key="quantity",
                        adjusted_price_map=adjusted_market_price_map,
                    )
                    source_copy_job_cost = self._job_cost_total(
                        process_value=(
                            float(source_copy_process_value) * float(missing_source_copy_runs) * 0.02
                            if source_copy_process_value is not None
                            else None
                        ),
                        cost_index=self._system_cost_index(profile_payload=selected_industry_profile, activity="copying"),
                        cost_reduction=self._combine_reductions(
                            [
                                self._profile_base_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="copying",
                                    metric="cost",
                                ),
                                self._profile_rig_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="copying",
                                    metric="cost",
                                ),
                                self._implant_reduction(
                                    character_modifier_payload=selected_character_modifiers,
                                    activity="copying",
                                    metric="cost",
                                ),
                            ]
                        ),
                        installation_surcharge=installation_surcharge,
                    )
                    source_copy_duration_seconds = self._round_duration_seconds(
                        int(source_copying_job.get("time_seconds") or 0)
                        * missing_source_copy_runs
                        * max(
                            0.0,
                            1.0
                            - self._combine_reductions(
                                [
                                    self._skill_time_reduction(activity="copying", skill_levels_by_name=character_skill_levels_by_name),
                                    self._profile_base_reduction(
                                        profile_payload=selected_industry_profile,
                                        activity="copying",
                                        metric="time",
                                    ),
                                    self._profile_rig_reduction(
                                        profile_payload=selected_industry_profile,
                                        activity="copying",
                                        metric="time",
                                    ),
                                    self._implant_reduction(
                                        character_modifier_payload=selected_character_modifiers,
                                        activity="copying",
                                        metric="time",
                                    ),
                                ]
                            ),
                        )
                    )
                    total_time_seconds += source_copy_duration_seconds
                    if source_copy_job_cost.get("total_job_cost") is not None:
                        total_job_cost += float(source_copy_job_cost.get("total_job_cost") or 0.0)
                        priced_job_count += 1
                    recursive_activity_breakdown[f"copying:{source_blueprint_type_id}"] = {
                        "activity": "copying",
                        "blueprint_type_id": source_blueprint_type_id,
                        "attempts": missing_source_copy_runs,
                        "time_seconds": source_copy_duration_seconds,
                        "job_cost": source_copy_job_cost.get("total_job_cost"),
                    }
                    prerequisite_nodes.append(
                        self._job_tree_node(
                            label=self._ACTIVITY_LABELS["copying"],
                            node_type="activity",
                            activity="copying",
                            blueprint_name=invention_blueprint_name,
                            runs=missing_source_copy_runs,
                            duration_seconds=source_copy_duration_seconds,
                            direct_duration_seconds=source_copy_duration_seconds,
                            job_cost=source_copy_job_cost.get("total_job_cost"),
                            total_job_cost=source_copy_job_cost.get("total_job_cost"),
                            category_name=str(selected_product.get("category_name") or "") or None,
                            meta_group_name=str(selected_product.get("meta_group_name") or "") or None,
                            recommendation_action="copy",
                            children=[],
                        )
                    )

        materials_container_node = self._job_tree_node(
            label=self._ACTIVITY_LABELS["materials"],
            node_type="materials",
            activity="materials",
            children=material_nodes,
        )
        activity_tree_node = self._job_tree_node(
            label=self._ACTIVITY_LABELS.get(activity, activity.title()),
            node_type="activity",
            activity=activity,
            blueprint_type_id=blueprint_type_id,
            type_id=int(selected_product.get("type_id") or desired_product_type_id),
            category_name=str(selected_product.get("category_name") or "") or None,
            meta_group_name=str(selected_product.get("meta_group_name") or "") or None,
            quantity=planned_quantity,
            runs=runs,
            duration_seconds=total_time_seconds,
            direct_duration_seconds=direct_duration_seconds,
            job_cost=job_cost.get("total_job_cost"),
            total_job_cost=(total_job_cost if priced_job_count > 0 else None),
            blueprint_source_kind=blueprint_source_kind,
            blueprint_copy=blueprint_copy_payload,
            blueprint_original=blueprint_original_payload,
            children=[*prerequisite_nodes, materials_container_node],
        )

        estimated_material_cost, _ = self._sum_preferred_item_value(
            leaf_materials,
            quantity_key="quantity",
            sell_price_map=sell_price_map,
            adjusted_price_map=adjusted_market_price_map,
        )

        return {
            "activity": activity,
            "blueprint_type_id": blueprint_type_id,
            "type_id": int(selected_product.get("type_id") or desired_product_type_id),
            "category_name": str(selected_product.get("category_name") or "") or None,
            "meta_group_name": str(selected_product.get("meta_group_name") or "") or None,
            "required_quantity": required_quantity,
            "planned_quantity": planned_quantity,
            "runs": runs,
            "direct_time_seconds": direct_duration_seconds,
            "total_time_seconds": total_time_seconds,
            "direct_job_cost": job_cost.get("total_job_cost"),
            "total_job_cost": (total_job_cost if priced_job_count > 0 else None),
            "priced_job_count": priced_job_count,
            "estimated_item_value": process_value,
            "estimated_item_value_priced_material_count": priced_material_count,
            "estimated_material_cost": estimated_material_cost,
            "estimated_total_cost": (
                float(total_job_cost or 0.0) + float(estimated_material_cost or 0.0)
                if total_job_cost is not None or estimated_material_cost is not None
                else None
            ),
            "leaf_materials": self._aggregate_material_entries(leaf_materials),
            "recursive_activity_breakdown": recursive_activity_breakdown,
            "blueprint_source_kind": blueprint_source_kind,
            "blueprint_copy": blueprint_copy_payload,
            "blueprint_original": blueprint_original_payload,
            "tree_node": activity_tree_node,
        }

    def _build_recursive_prerequisite_plan(
        self,
        *,
        adjusted_material_entries: list[dict[str, Any]],
        blueprint_type_id: int,
        build_from_bpc: bool,
        include_reactions: bool,
        selected_industry_profile: dict[str, Any] | None,
        selected_character_modifiers: dict[str, Any] | None,
        character_skill_levels_by_name: dict[str, int],
        adjusted_market_price_map: dict[int, dict[str, Any]],
        sell_price_map: dict[int, dict[str, Any]] | None,
        blueprint_copy_assets_by_type_id: dict[int, list[Any]],
        available_blueprint_copy_runs_by_type_id: dict[int, int] | None,
        available_owned_item_quantity_by_type_id: dict[int, int] | None,
        owned_item_unit_cost_by_type_id: dict[int, float] | None,
        blueprint_original_assets_by_type_id: dict[int, list[Any]],
        character_name_by_id: dict[int, str] | None = None,
        corporation_name_by_id: dict[int, str] | None = None,
        manufacturing_row_by_product_type_id: dict[int, dict[str, Any]],
        reaction_row_by_product_type_id: dict[int, dict[str, Any]],
        invention_row_by_blueprint_type_id: dict[int, dict[str, Any]],
        include_current_blueprint_prerequisites: bool = True,
    ) -> dict[str, Any]:
        reactions_enabled = bool(include_reactions) and self._reactions_allowed_for_profile(selected_industry_profile)
        total_time_seconds = 0
        total_job_cost = 0.0
        priced_job_count = 0
        procurement_materials: list[dict[str, Any]] = []
        recursive_activity_breakdown: dict[str, Any] = {}
        tree_children: list[dict[str, Any]] = []
        material_nodes: list[dict[str, Any]] = []
        available_owned_item_quantities = available_owned_item_quantity_by_type_id or {}
        owned_item_unit_costs = owned_item_unit_cost_by_type_id or {}

        matched_blueprint_copies = blueprint_copy_assets_by_type_id.get(int(blueprint_type_id)) or []
        matched_blueprint_originals = blueprint_original_assets_by_type_id.get(int(blueprint_type_id)) or []
        if include_current_blueprint_prerequisites and not matched_blueprint_copies and not matched_blueprint_originals:
            invention_source_row = invention_row_by_blueprint_type_id.get(int(blueprint_type_id))
            invention_job = (invention_source_row or {}).get("invention_job") or {}
            if invention_source_row and isinstance(invention_job, dict):
                invention_blueprint_payload = invention_source_row.get("blueprint") or {}
                if not isinstance(invention_blueprint_payload, dict):
                    invention_blueprint_payload = {}
                invention_blueprint_name = str(
                    invention_blueprint_payload.get("type_name")
                    or invention_blueprint_payload.get("name")
                    or invention_source_row.get("blueprint_type_id")
                    or "Blueprint"
                )
                target_entry = None
                for product in invention_job.get("products") or []:
                    if not isinstance(product, dict):
                        continue
                    invention_product = product.get("product") or {}
                    if not isinstance(invention_product, dict):
                        invention_product = {}
                    if int(invention_product.get("type_id") or product.get("type_id") or 0) == int(blueprint_type_id):
                        target_entry = product
                        break
                probability = 0.0
                if isinstance(target_entry, dict):
                    try:
                        probability = float(target_entry.get("probability_pct") or 0.0) / 100.0
                    except Exception:
                        probability = 0.0
                target_blueprint_name = str(
                    ((target_entry or {}).get("product") or {}).get("type_name")
                    if isinstance((target_entry or {}).get("product"), dict)
                    else ""
                ).strip() or str(blueprint_type_id)
                invention_attempts = max(1, int(math.ceil(1.0 / max(probability, 0.01))))
                invention_materials = [
                    {**dict(entry), "quantity": int(entry.get("quantity") or 0) * invention_attempts}
                    for entry in (invention_job.get("materials") or [])
                    if isinstance(entry, dict)
                ]
                invention_process_value, _ = self._sum_estimated_item_value(
                    invention_materials,
                    quantity_key="quantity",
                    adjusted_price_map=adjusted_market_price_map,
                )
                invention_time_reduction = self._combine_reductions(
                    [
                        self._skill_time_reduction(activity="invention", skill_levels_by_name=character_skill_levels_by_name),
                        self._profile_base_reduction(
                            profile_payload=selected_industry_profile,
                            activity="invention",
                            metric="time",
                        ),
                        self._profile_rig_reduction(
                            profile_payload=selected_industry_profile,
                            activity="invention",
                            metric="time",
                        ),
                        self._implant_reduction(
                            character_modifier_payload=selected_character_modifiers,
                            activity="invention",
                            metric="time",
                        ),
                    ]
                )
                invention_cost_reduction = self._combine_reductions(
                    [
                        self._profile_base_reduction(
                            profile_payload=selected_industry_profile,
                            activity="invention",
                            metric="cost",
                        ),
                        self._profile_rig_reduction(
                            profile_payload=selected_industry_profile,
                            activity="invention",
                            metric="cost",
                        ),
                        self._implant_reduction(
                            character_modifier_payload=selected_character_modifiers,
                            activity="invention",
                            metric="cost",
                        ),
                    ]
                )
                installation_surcharge = self._profile_installation_surcharge(selected_industry_profile)
                invention_job_cost = self._job_cost_total(
                    process_value=invention_process_value,
                    cost_index=self._system_cost_index(profile_payload=selected_industry_profile, activity="invention"),
                    cost_reduction=invention_cost_reduction,
                    installation_surcharge=installation_surcharge,
                )
                invention_duration_seconds = self._round_duration_seconds(
                    int(invention_job.get("time_seconds") or 0) * invention_attempts * max(0.0, 1.0 - invention_time_reduction)
                )
                planned_invention_materials, invention_material_nodes = self._plan_take_or_buy_material_nodes(
                    invention_materials,
                    available_owned_item_quantity_by_type_id=available_owned_item_quantities,
                    owned_item_unit_cost_by_type_id=owned_item_unit_costs,
                    sell_price_map=sell_price_map,
                    adjusted_price_map=adjusted_market_price_map,
                )
                total_time_seconds += invention_duration_seconds
                if invention_job_cost.get("total_job_cost") is not None:
                    total_job_cost += float(invention_job_cost.get("total_job_cost") or 0.0)
                    priced_job_count += 1
                procurement_materials.extend(planned_invention_materials)
                recursive_activity_breakdown[f"invention:{blueprint_type_id}"] = {
                    "activity": "invention",
                    "blueprint_type_id": blueprint_type_id,
                    "attempts": invention_attempts,
                    "time_seconds": invention_duration_seconds,
                    "job_cost": invention_job_cost.get("total_job_cost"),
                }
                tree_children.append(
                    self._job_tree_node(
                        label=self._ACTIVITY_LABELS["invention"],
                        node_type="activity",
                        activity="invention",
                        blueprint_name=target_blueprint_name,
                        blueprint_type_id=blueprint_type_id,
                        runs=invention_attempts,
                        duration_seconds=invention_duration_seconds,
                        direct_duration_seconds=invention_duration_seconds,
                        job_cost=invention_job_cost.get("total_job_cost"),
                        total_job_cost=invention_job_cost.get("total_job_cost"),
                        children=[
                            self._job_tree_node(
                                label=self._ACTIVITY_LABELS["materials"],
                                node_type="materials",
                                activity="materials",
                                children=invention_material_nodes,
                            )
                        ],
                    )
                )

                source_copying_job = (invention_source_row.get("copying_job") or {}) if isinstance(invention_source_row, dict) else {}
                source_blueprint_type_id = int((invention_source_row or {}).get("blueprint_type_id") or 0)
                source_has_copy = bool(blueprint_copy_assets_by_type_id.get(source_blueprint_type_id))
                if isinstance(source_copying_job, dict) and int(source_copying_job.get("time_seconds") or 0) > 0 and not source_has_copy:
                    source_manufacturing_materials = [
                        dict(entry)
                        for entry in ((invention_source_row.get("manufacturing_job") or {}).get("materials") or [])
                        if isinstance(entry, dict)
                    ]
                    source_copy_process_value, _ = self._sum_estimated_item_value(
                        source_manufacturing_materials,
                        quantity_key="quantity",
                        adjusted_price_map=adjusted_market_price_map,
                    )
                    source_copy_job_cost = self._job_cost_total(
                        process_value=(float(source_copy_process_value) * 0.02 if source_copy_process_value is not None else None),
                        cost_index=self._system_cost_index(profile_payload=selected_industry_profile, activity="copying"),
                        cost_reduction=self._combine_reductions(
                            [
                                self._profile_base_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="copying",
                                    metric="cost",
                                ),
                                self._profile_rig_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="copying",
                                    metric="cost",
                                ),
                                self._implant_reduction(
                                    character_modifier_payload=selected_character_modifiers,
                                    activity="copying",
                                    metric="cost",
                                ),
                            ]
                        ),
                        installation_surcharge=installation_surcharge,
                    )
                    source_copy_duration_seconds = self._round_duration_seconds(
                        int(source_copying_job.get("time_seconds") or 0)
                        * invention_attempts
                        * max(
                            0.0,
                            1.0
                            - self._combine_reductions(
                                [
                                    self._skill_time_reduction(activity="copying", skill_levels_by_name=character_skill_levels_by_name),
                                    self._profile_base_reduction(
                                        profile_payload=selected_industry_profile,
                                        activity="copying",
                                        metric="time",
                                    ),
                                    self._profile_rig_reduction(
                                        profile_payload=selected_industry_profile,
                                        activity="copying",
                                        metric="time",
                                    ),
                                    self._implant_reduction(
                                        character_modifier_payload=selected_character_modifiers,
                                        activity="copying",
                                        metric="time",
                                    ),
                                ]
                            ),
                        )
                    )
                    total_time_seconds += source_copy_duration_seconds
                    if source_copy_job_cost.get("total_job_cost") is not None:
                        total_job_cost += float(source_copy_job_cost.get("total_job_cost") or 0.0)
                        priced_job_count += 1
                    recursive_activity_breakdown[f"copying:{source_blueprint_type_id}"] = {
                        "activity": "copying",
                        "blueprint_type_id": source_blueprint_type_id,
                        "attempts": invention_attempts,
                        "time_seconds": source_copy_duration_seconds,
                        "job_cost": source_copy_job_cost.get("total_job_cost"),
                    }
                    tree_children.append(
                        self._job_tree_node(
                            label=self._ACTIVITY_LABELS["copying"],
                            node_type="activity",
                            activity="copying",
                            blueprint_name=invention_blueprint_name,
                            blueprint_type_id=source_blueprint_type_id,
                            runs=invention_attempts,
                            duration_seconds=source_copy_duration_seconds,
                            direct_duration_seconds=source_copy_duration_seconds,
                            job_cost=source_copy_job_cost.get("total_job_cost"),
                            total_job_cost=source_copy_job_cost.get("total_job_cost"),
                            children=[],
                        )
                    )

        for material in adjusted_material_entries:
            if not isinstance(material, dict):
                continue
            material_type_id = int(material.get("type_id") or 0)
            material_quantity = int(material.get("quantity") or 0)
            if material_type_id <= 0 or material_quantity <= 0:
                continue
            available_owned_quantity = int(available_owned_item_quantities.get(material_type_id, 0))
            preferred_owned_unit_cost = self._as_float(owned_item_unit_costs.get(material_type_id))
            buy_unit_price, buy_price_source = self._resolve_preferred_unit_value(
                type_id=material_type_id,
                type_payload=material,
                sell_price_map=sell_price_map,
                adjusted_price_map=adjusted_market_price_map,
            )
            owned_quantity_to_take = min(max(0, available_owned_quantity), material_quantity)
            if owned_quantity_to_take > 0:
                available_owned_item_quantities[material_type_id] = max(0, available_owned_quantity - owned_quantity_to_take)
                take_unit_price = preferred_owned_unit_cost if preferred_owned_unit_cost is not None else buy_unit_price
                take_price_source = "owned_asset_item_value" if preferred_owned_unit_cost is not None else buy_price_source
                line_total = (
                    float(take_unit_price) * float(owned_quantity_to_take)
                    if take_unit_price is not None and owned_quantity_to_take > 0
                    else None
                )
                procurement_materials.append(
                    {
                        **dict(material),
                        "quantity": owned_quantity_to_take,
                        "unit_price": take_unit_price,
                        "price_source": take_price_source,
                        "sourcing_strategy": "take",
                        "owned_cost_basis_known": (preferred_owned_unit_cost is not None),
                        "uses_unknown_owned_cost_basis": bool(preferred_owned_unit_cost is None),
                        "line_total": line_total,
                    }
                )
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or material_type_id),
                        node_type="material",
                        activity="material",
                        sourcing_strategy="take",
                        recommendation_action="take",
                        category_name=str(material.get("category_name") or "") or None,
                        meta_group_name=str(material.get("meta_group_name") or "") or None,
                        type_id=material_type_id,
                        quantity=owned_quantity_to_take,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=None,
                        material_cost=line_total,
                        total_cost=line_total,
                        unit_price=take_unit_price,
                        price_source=take_price_source,
                        children=[],
                    )
                )
            remaining_material_quantity = max(0, material_quantity - owned_quantity_to_take)
            if remaining_material_quantity <= 0:
                continue
            child_row = None
            child_activity = "manufacturing"
            if reactions_enabled and material_type_id in reaction_row_by_product_type_id:
                child_row = reaction_row_by_product_type_id.get(material_type_id)
                child_activity = "reaction"
            elif material_type_id in manufacturing_row_by_product_type_id:
                child_row = manufacturing_row_by_product_type_id.get(material_type_id)
            if child_row is None:
                line_total = (
                    float(buy_unit_price) * float(remaining_material_quantity)
                    if buy_unit_price is not None and remaining_material_quantity > 0
                    else None
                )
                procurement_materials.append(
                    {
                        **dict(material),
                        "quantity": remaining_material_quantity,
                        "unit_price": buy_unit_price,
                        "price_source": buy_price_source,
                        "sourcing_strategy": "buy",
                        "owned_cost_basis_known": False,
                        "uses_unknown_owned_cost_basis": False,
                        "line_total": line_total,
                    }
                )
                sourcing_strategy = (
                    "buy_reaction_available"
                    if not reactions_enabled and material_type_id in reaction_row_by_product_type_id
                    else "buy"
                )
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or material_type_id),
                        node_type="material",
                        activity="material",
                        sourcing_strategy=sourcing_strategy,
                        recommendation_action="buy",
                        category_name=str(material.get("category_name") or "") or None,
                        meta_group_name=str(material.get("meta_group_name") or "") or None,
                        type_id=material_type_id,
                        quantity=remaining_material_quantity,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=None,
                        material_cost=line_total,
                        total_cost=line_total,
                        unit_price=buy_unit_price,
                        price_source=buy_price_source,
                        children=[],
                    )
                )
                continue

            child_plan = self._plan_blueprint_chain_for_quantity(
                blueprint_row=child_row,
                activity=child_activity,
                desired_product_type_id=material_type_id,
                required_quantity=remaining_material_quantity,
                build_from_bpc=build_from_bpc,
                include_reactions=reactions_enabled,
                selected_industry_profile=selected_industry_profile,
                selected_character_modifiers=selected_character_modifiers,
                character_skill_levels_by_name=character_skill_levels_by_name,
                adjusted_market_price_map=adjusted_market_price_map,
                sell_price_map=sell_price_map,
                blueprint_copy_assets_by_type_id=blueprint_copy_assets_by_type_id,
                available_blueprint_copy_runs_by_type_id=available_blueprint_copy_runs_by_type_id,
                available_owned_item_quantity_by_type_id=available_owned_item_quantity_by_type_id,
                owned_item_unit_cost_by_type_id=owned_item_unit_cost_by_type_id,
                blueprint_original_assets_by_type_id=blueprint_original_assets_by_type_id,
                character_name_by_id=character_name_by_id,
                corporation_name_by_id=corporation_name_by_id,
                manufacturing_row_by_product_type_id=manufacturing_row_by_product_type_id,
                reaction_row_by_product_type_id=reaction_row_by_product_type_id,
                invention_row_by_blueprint_type_id=invention_row_by_blueprint_type_id,
                visited={("manufacturing", blueprint_type_id)},
                depth=1,
            )
            if child_plan is None:
                line_total = (
                    float(buy_unit_price) * float(remaining_material_quantity)
                    if buy_unit_price is not None and remaining_material_quantity > 0
                    else None
                )
                procurement_materials.append(
                    {
                        **dict(material),
                        "quantity": remaining_material_quantity,
                        "unit_price": buy_unit_price,
                        "price_source": buy_price_source,
                        "line_total": line_total,
                    }
                )
                sourcing_strategy = (
                    "buy_reaction_available"
                    if not reactions_enabled and material_type_id in reaction_row_by_product_type_id
                    else "buy"
                )
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or material_type_id),
                        node_type="material",
                        activity="material",
                        sourcing_strategy=sourcing_strategy,
                        recommendation_action="buy",
                        category_name=str(material.get("category_name") or "") or None,
                        meta_group_name=str(material.get("meta_group_name") or "") or None,
                        type_id=material_type_id,
                        quantity=remaining_material_quantity,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=None,
                        material_cost=line_total,
                        total_cost=line_total,
                        unit_price=buy_unit_price,
                        price_source=buy_price_source,
                        children=[],
                    )
                )
                continue
            planned_child_quantity = min(
                remaining_material_quantity,
                max(0, int(child_plan.get("planned_quantity") or remaining_material_quantity)),
            )
            uncovered_child_quantity = max(0, remaining_material_quantity - planned_child_quantity)
            build_total_cost = self._as_float(child_plan.get("estimated_total_cost"))
            buy_total_cost = (
                float(buy_unit_price) * float(planned_child_quantity)
                if buy_unit_price is not None and planned_child_quantity > 0
                else None
            )
            choose_build = (
                planned_child_quantity > 0
                and child_plan.get("tree_node") is not None
                and
                build_total_cost is not None
                and (buy_total_cost is None or build_total_cost <= buy_total_cost)
            )
            if choose_build:
                total_time_seconds += int(child_plan.get("total_time_seconds") or 0)
                if child_plan.get("total_job_cost") is not None:
                    total_job_cost += float(child_plan.get("total_job_cost") or 0.0)
                    priced_job_count += int(child_plan.get("priced_job_count") or 0)
                procurement_materials.extend(
                    [dict(entry) for entry in (child_plan.get("leaf_materials") or []) if isinstance(entry, dict)]
                )
                recursive_activity_breakdown[f"{child_activity}:{material_type_id}"] = {
                    "activity": child_activity,
                    "type_id": material_type_id,
                    "type_name": material.get("type_name"),
                    "quantity": planned_child_quantity,
                    "time_seconds": child_plan.get("total_time_seconds"),
                    "job_cost": child_plan.get("total_job_cost"),
                    "nested": child_plan.get("recursive_activity_breakdown") or {},
                    "recommended_action": "build",
                }
                child_tree_node = self._normalize_nested_owned_blueprint_copy_tree(
                    cast(dict[str, Any] | None, child_plan.get("tree_node")),
                    blueprint_type_id=int(child_plan.get("blueprint_type_id") or 0),
                    planned_runs=int(child_plan.get("runs") or 0),
                    blueprint_copy_assets_by_type_id=blueprint_copy_assets_by_type_id,
                    category_name=str(child_plan.get("category_name") or material.get("category_name") or "") or None,
                    meta_group_name=str(child_plan.get("meta_group_name") or material.get("meta_group_name") or "") or None,
                    character_name_by_id=character_name_by_id,
                    corporation_name_by_id=corporation_name_by_id,
                )
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or material_type_id),
                        node_type="material",
                        activity="material",
                        sourcing_strategy="build",
                        recommendation_action="build",
                        category_name=str(child_plan.get("category_name") or material.get("category_name") or "") or None,
                        meta_group_name=str(child_plan.get("meta_group_name") or material.get("meta_group_name") or "") or None,
                        type_id=material_type_id,
                        quantity=planned_child_quantity,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=child_plan.get("total_job_cost"),
                        children=[child_tree_node] if isinstance(child_tree_node, dict) else [],
                    )
                )
                if uncovered_child_quantity > 0:
                    uncovered_line_total = (
                        float(buy_unit_price) * float(uncovered_child_quantity)
                        if buy_unit_price is not None and uncovered_child_quantity > 0
                        else None
                    )
                    procurement_materials.append(
                        {
                            **dict(material),
                            "quantity": uncovered_child_quantity,
                            "unit_price": buy_unit_price,
                            "price_source": buy_price_source,
                            "line_total": uncovered_line_total,
                        }
                    )
                    material_nodes.append(
                        self._job_tree_node(
                            label=str(material.get("type_name") or material_type_id),
                            node_type="material",
                            activity="material",
                            sourcing_strategy="buy",
                            recommendation_action="buy",
                            category_name=str(material.get("category_name") or "") or None,
                            meta_group_name=str(material.get("meta_group_name") or "") or None,
                            type_id=material_type_id,
                            quantity=uncovered_child_quantity,
                            runs=None,
                            duration_seconds=None,
                            job_cost=None,
                            total_job_cost=None,
                            material_cost=uncovered_line_total,
                            total_cost=uncovered_line_total,
                            unit_price=buy_unit_price,
                            price_source=buy_price_source,
                            children=[],
                        )
                    )
            else:
                procurement_materials.append(
                    {
                        **dict(material),
                        "quantity": remaining_material_quantity,
                        "unit_price": buy_unit_price,
                        "price_source": buy_price_source,
                        "line_total": buy_total_cost,
                    }
                )
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or material_type_id),
                        node_type="material",
                        activity="material",
                        sourcing_strategy="buy",
                        recommendation_action="buy",
                        category_name=str(material.get("category_name") or "") or None,
                        meta_group_name=str(material.get("meta_group_name") or "") or None,
                        type_id=material_type_id,
                        quantity=remaining_material_quantity,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=None,
                        material_cost=buy_total_cost,
                        total_cost=buy_total_cost,
                        unit_price=buy_unit_price,
                        price_source=buy_price_source,
                        children=[],
                    )
                )

        if not procurement_materials:
            procurement_materials = [dict(entry) for entry in adjusted_material_entries if isinstance(entry, dict)]

        return {
            "enabled": bool(recursive_activity_breakdown),
            "reactions_enabled": reactions_enabled,
            "time_seconds": total_time_seconds,
            "job_cost": (total_job_cost if priced_job_count > 0 else None),
            "priced_job_count": priced_job_count,
            "procurement_materials": self._aggregate_material_entries(procurement_materials),
            "activity_breakdown": recursive_activity_breakdown,
            "tree_children": tree_children,
            "material_nodes": material_nodes,
        }

    @staticmethod
    def _compact_owned_blueprint_asset(
        asset: CharacterAssetsModel | CorporationAssetsModel | None,
        *,
        character_name_by_id: dict[int, str] | None = None,
        corporation_name_by_id: dict[int, str] | None = None,
        top_location_name_by_id: dict[int, str] | None = None,
    ) -> dict[str, Any]:
        if asset is None:
            return {}

        payload = {
            "item_id": int(asset.item_id),
            "location_id": int(asset.location_id) if asset.location_id is not None else None,
            "location_type": asset.location_type,
            "location_flag": asset.location_flag,
            "top_location_id": int(asset.top_location_id) if asset.top_location_id is not None else None,
            "top_location_name": (
                (top_location_name_by_id or {}).get(int(asset.top_location_id))
                if asset.top_location_id is not None
                else None
            ),
            "container_name": asset.container_name,
            "ship_name": asset.ship_name,
            "is_singleton": bool(asset.is_singleton),
            "is_blueprint_copy": bool(asset.is_blueprint_copy),
            "runs": int(asset.blueprint_runs) if asset.blueprint_runs is not None else None,
            "material_efficiency": int(asset.blueprint_material_efficiency)
            if asset.blueprint_material_efficiency is not None
            else None,
            "time_efficiency": int(asset.blueprint_time_efficiency)
            if asset.blueprint_time_efficiency is not None
            else None,
            "quantity": int(asset.quantity),
            "acquisition_unit_cost": IndustryService._as_float(getattr(asset, "acquisition_unit_cost", None)),
            "acquisition_total_cost": IndustryService._as_float(getattr(asset, "acquisition_total_cost", None)),
        }
        if isinstance(asset, CharacterAssetsModel):
            character_id = int(asset.character_id)
            payload["character_id"] = character_id
            payload["character_name"] = (character_name_by_id or {}).get(character_id)
            payload["owner_type"] = "character"
        elif isinstance(asset, CorporationAssetsModel):
            corporation_id = int(asset.corporation_id)
            payload["corporation_id"] = corporation_id
            payload["corporation_name"] = (corporation_name_by_id or {}).get(corporation_id)
            payload["owner_type"] = "corporation"
        return payload

    @classmethod
    def _compact_owned_blueprint_assets(
        cls,
        assets: list[CharacterAssetsModel | CorporationAssetsModel] | None,
        *,
        character_name_by_id: dict[int, str] | None = None,
        corporation_name_by_id: dict[int, str] | None = None,
        top_location_name_by_id: dict[int, str] | None = None,
    ) -> dict[str, Any]:
        if not assets:
            return {}
        if len(assets) == 1:
            return cls._compact_owned_blueprint_asset(
                assets[0],
                character_name_by_id=character_name_by_id,
                corporation_name_by_id=corporation_name_by_id,
                top_location_name_by_id=top_location_name_by_id,
            )

        payloads = [
            cls._compact_owned_blueprint_asset(
                asset,
                character_name_by_id=character_name_by_id,
                corporation_name_by_id=corporation_name_by_id,
                top_location_name_by_id=top_location_name_by_id,
            )
            for asset in assets
        ]

        def common_value(field_name: str) -> Any:
            values = [payload.get(field_name) for payload in payloads if payload.get(field_name) not in {None, ""}]
            if not values:
                return None
            first_value = values[0]
            return first_value if all(value == first_value for value in values[1:]) else None

        character_name = common_value("character_name")
        corporation_name = common_value("corporation_name")
        owner_type = common_value("owner_type")
        if not character_name and not corporation_name:
            character_name = f"{len(payloads)} owned BPCs"
            owner_type = "multiple"

        return {
            "item_id": None,
            "item_ids": [payload.get("item_id") for payload in payloads if payload.get("item_id") is not None],
            "owner_type": owner_type,
            "character_id": common_value("character_id"),
            "character_name": character_name,
            "corporation_id": common_value("corporation_id"),
            "corporation_name": corporation_name,
            "location_id": common_value("location_id"),
            "location_type": common_value("location_type"),
            "location_flag": common_value("location_flag"),
            "top_location_id": common_value("top_location_id"),
            "top_location_name": common_value("top_location_name"),
            "container_name": common_value("container_name"),
            "ship_name": common_value("ship_name"),
            "is_singleton": False,
            "is_blueprint_copy": True,
            "runs": sum(max(0, int(payload.get("runs") or 0)) for payload in payloads) or None,
            "material_efficiency": common_value("material_efficiency"),
            "time_efficiency": common_value("time_efficiency"),
            "quantity": len(payloads),
            "acquisition_unit_cost": None,
            "acquisition_total_cost": sum(
                float(payload.get("acquisition_total_cost") or 0.0)
                for payload in payloads
                if payload.get("acquisition_total_cost") is not None
            ) or None,
        }

    @staticmethod
    def _latest_history_rows_by_item(rows: list[Any]) -> list[Any]:
        latest_by_item_id: dict[int, Any] = {}
        for row in sorted(
            rows,
            key=lambda value: (
                str(getattr(value, "observed_at", "") or ""),
                int(getattr(value, "id", 0) or 0),
            ),
            reverse=True,
        ):
            try:
                item_id = int(getattr(row, "item_id", 0) or 0)
            except Exception:
                item_id = 0
            if item_id <= 0 or item_id in latest_by_item_id:
                continue
            latest_by_item_id[item_id] = row
        return list(latest_by_item_id.values())

    @staticmethod
    def _materialize_historical_blueprint_asset(
        row: CharacterAssetHistoryModel | CorporationAssetHistoryModel,
        *,
        owner_kind: str,
    ) -> CharacterAssetsModel | CorporationAssetsModel | None:
        try:
            item_id = int(getattr(row, "item_id", 0) or 0)
            type_id = int(getattr(row, "type_id", 0) or 0)
        except Exception:
            return None
        if item_id <= 0 or type_id <= 0:
            return None

        payload = {
            "item_id": item_id,
            "type_id": type_id,
            "type_name": getattr(row, "type_name", None),
            "type_category_name": "Blueprint",
            "location_id": int(getattr(row, "location_id", 0) or 0),
            "location_type": getattr(row, "location_type", None),
            "location_flag": getattr(row, "location_flag", None),
            "top_location_id": int(getattr(row, "location_id", 0) or 0) or None,
            "is_singleton": bool(getattr(row, "is_singleton", True) if getattr(row, "is_singleton", None) is not None else True),
            "is_blueprint_copy": bool(getattr(row, "is_blueprint_copy", False)),
            "blueprint_runs": getattr(row, "blueprint_runs", None),
            "blueprint_time_efficiency": getattr(row, "blueprint_time_efficiency", None),
            "blueprint_material_efficiency": getattr(row, "blueprint_material_efficiency", None),
            "quantity": int(getattr(row, "quantity", 1) or 1),
            "is_container": False,
            "is_asset_safety_wrap": False,
            "is_ship": False,
            "is_office_folder": False,
            "acquisition_source": getattr(row, "acquisition_source", None),
            "acquisition_unit_cost": getattr(row, "acquisition_unit_cost", None),
            "acquisition_total_cost": getattr(row, "acquisition_total_cost", None),
            "acquisition_reference_type": getattr(row, "acquisition_reference_type", None),
            "acquisition_reference_id": getattr(row, "acquisition_reference_id", None),
            "acquisition_date": getattr(row, "acquisition_date", None),
        }

        if owner_kind == "character":
            payload["character_id"] = int(getattr(row, "character_id", 0) or 0)
            return CharacterAssetsModel(**payload)

        payload["corporation_id"] = int(getattr(row, "corporation_id", 0) or 0)
        return CorporationAssetsModel(**payload)

    def _load_historical_blueprint_assets(
        self,
        *,
        app_session: Any,
        sde_session: Any,
        character_ids: list[int],
        corporation_ids: list[int],
        current_item_ids: set[int],
    ) -> tuple[list[CharacterAssetsModel], list[CorporationAssetsModel]]:
        candidate_character_rows: list[CharacterAssetHistoryModel] = []
        candidate_corporation_rows: list[CorporationAssetHistoryModel] = []

        normalized_character_ids = sorted({int(owner_id) for owner_id in character_ids if int(owner_id) > 0})
        normalized_corporation_ids = sorted({int(owner_id) for owner_id in corporation_ids if int(owner_id) > 0})

        if normalized_character_ids:
            candidate_character_rows = (
                app_session.query(CharacterAssetHistoryModel)
                .filter(CharacterAssetHistoryModel.character_id.in_(normalized_character_ids))
                .all()
            )
        if normalized_corporation_ids:
            candidate_corporation_rows = (
                app_session.query(CorporationAssetHistoryModel)
                .filter(CorporationAssetHistoryModel.corporation_id.in_(normalized_corporation_ids))
                .all()
            )

        latest_character_rows = self._latest_history_rows_by_item(candidate_character_rows)
        latest_corporation_rows = self._latest_history_rows_by_item(candidate_corporation_rows)
        candidate_type_ids = sorted(
            {
                int(getattr(row, "type_id", 0) or 0)
                for row in [*latest_character_rows, *latest_corporation_rows]
                if int(getattr(row, "type_id", 0) or 0) > 0
            }
        )
        if not candidate_type_ids:
            return [], []

        blueprint_type_ids = {
            int(blueprint_type_id)
            for (blueprint_type_id,) in (
                sde_session.query(Blueprints.blueprintTypeID)
                .filter(Blueprints.blueprintTypeID.in_(candidate_type_ids))
                .all()
            )
        }
        if not blueprint_type_ids:
            return [], []

        historical_character_assets: list[CharacterAssetsModel] = []
        for row in latest_character_rows:
            item_id = int(getattr(row, "item_id", 0) or 0)
            type_id = int(getattr(row, "type_id", 0) or 0)
            if item_id <= 0 or type_id not in blueprint_type_ids or item_id in current_item_ids:
                continue
            asset = self._materialize_historical_blueprint_asset(row, owner_kind="character")
            if isinstance(asset, CharacterAssetsModel):
                historical_character_assets.append(asset)

        historical_corporation_assets: list[CorporationAssetsModel] = []
        for row in latest_corporation_rows:
            item_id = int(getattr(row, "item_id", 0) or 0)
            type_id = int(getattr(row, "type_id", 0) or 0)
            if item_id <= 0 or type_id not in blueprint_type_ids or item_id in current_item_ids:
                continue
            asset = self._materialize_historical_blueprint_asset(row, owner_kind="corporation")
            if isinstance(asset, CorporationAssetsModel):
                historical_corporation_assets.append(asset)

        return historical_character_assets, historical_corporation_assets

    def _load_historical_item_unit_costs(
        self,
        *,
        app_session: Any,
        character_ids: list[int],
        corporation_ids: list[int],
        type_ids: list[int],
    ) -> dict[int, float]:
        normalized_type_ids = sorted({int(type_id) for type_id in type_ids if int(type_id) > 0})
        if not normalized_type_ids:
            return {}

        out: dict[int, float] = {}

        def consume_rows(rows: list[Any]) -> None:
            for row in sorted(
                rows,
                key=lambda value: (
                    str(getattr(value, "observed_at", "") or ""),
                    int(getattr(value, "id", 0) or 0),
                ),
                reverse=True,
            ):
                type_id = int(getattr(row, "type_id", 0) or 0)
                if type_id <= 0 or type_id in out:
                    continue
                unit_cost = self._as_float(getattr(row, "acquisition_unit_cost", None))
                if unit_cost is None or unit_cost <= 0:
                    continue
                out[type_id] = float(unit_cost)

        normalized_character_ids = sorted({int(owner_id) for owner_id in character_ids if int(owner_id) > 0})
        if normalized_character_ids:
            consume_rows(
                app_session.query(CharacterAssetHistoryModel)
                .filter(CharacterAssetHistoryModel.character_id.in_(normalized_character_ids))
                .filter(CharacterAssetHistoryModel.type_id.in_(normalized_type_ids))
                .filter(CharacterAssetHistoryModel.acquisition_unit_cost.isnot(None))
                .all()
            )

        normalized_corporation_ids = sorted({int(owner_id) for owner_id in corporation_ids if int(owner_id) > 0})
        if normalized_corporation_ids:
            consume_rows(
                app_session.query(CorporationAssetHistoryModel)
                .filter(CorporationAssetHistoryModel.corporation_id.in_(normalized_corporation_ids))
                .filter(CorporationAssetHistoryModel.type_id.in_(normalized_type_ids))
                .filter(CorporationAssetHistoryModel.acquisition_unit_cost.isnot(None))
                .all()
            )

        return out

    def _get_owned_blueprint_assets(
        self,
        *,
        owned_blueprints_scope: str,
    ) -> tuple[
        list[CharacterAssetsModel],
        list[CorporationAssetsModel],
        dict[int, str],
        dict[int, str],
        dict[int, str],
    ]:
        session: Any = self._sessions.app_session()
        sde_session: Any = self._sessions.sde_session()
        try:
            characters = self._state.char_manager.get_characters() or []
            character_ids: list[int] = []
            corporation_ids: list[int] = []
            for character in characters:
                if not isinstance(character, dict):
                    continue
                try:
                    character_id = int(character.get("character_id") or 0)
                except Exception:
                    character_id = 0
                try:
                    corporation_id = int(character.get("corporation_id") or 0)
                except Exception:
                    corporation_id = 0
                if character_id > 0:
                    character_ids.append(character_id)
                if corporation_id > 0:
                    corporation_ids.append(corporation_id)

            character_name_by_id = blueprints_repo.get_character_name_map(session, character_ids)
            corporation_name_by_id = blueprints_repo.get_corporation_name_map(session, corporation_ids)

            for character in characters:
                if not isinstance(character, dict):
                    continue
                try:
                    character_id = int(character.get("character_id") or 0)
                except Exception:
                    character_id = 0
                character_name = str(character.get("character_name") or character.get("name") or "").strip()
                if character_id > 0 and character_name:
                    character_name_by_id[character_id] = character_name

            try:
                corporations = self._state.corp_manager.get_corporations() or []
            except Exception:
                corporations = []
            for corporation in corporations:
                if not isinstance(corporation, dict):
                    continue
                try:
                    corporation_id = int(corporation.get("corporation_id") or 0)
                except Exception:
                    corporation_id = 0
                corporation_name = str(corporation.get("corporation_name") or corporation.get("name") or "").strip()
                if corporation_id > 0 and corporation_name:
                    corporation_name_by_id[corporation_id] = corporation_name

            def resolve_top_location_name_map(
                assets: list[CharacterAssetsModel | CorporationAssetsModel],
            ) -> dict[int, str]:
                if self._state.esi_service is None:
                    return {}
                top_location_ids: set[int] = set()
                for asset in assets:
                    raw_top_location_id = getattr(asset, "top_location_id", None)
                    if raw_top_location_id is None:
                        continue
                    top_location_id = int(raw_top_location_id)
                    if top_location_id > 0:
                        top_location_ids.add(top_location_id)
                out: dict[int, str] = {}
                for top_location_id in sorted(top_location_ids):
                    try:
                        location_info = self._state.esi_service.get_location_info(
                            int(top_location_id),
                            suppress_forbidden_log=True,
                            suppress_not_found_log=True,
                        )
                    except Exception:
                        continue
                    if not isinstance(location_info, dict):
                        continue
                    name = location_info.get("name")
                    if isinstance(name, str) and name:
                        out[int(top_location_id)] = name
                return out

            normalized_scope = (owned_blueprints_scope or "all_characters").strip().lower()
            if normalized_scope.startswith("character:"):
                try:
                    selected_character_id = int(normalized_scope.split(":", 1)[1])
                except Exception:
                    selected_character_id = 0
                character_assets = blueprints_repo.get_character_blueprint_assets_for_ids(session, [selected_character_id])
                historical_character_assets, _ = self._load_historical_blueprint_assets(
                    app_session=session,
                    sde_session=sde_session,
                    character_ids=[selected_character_id],
                    corporation_ids=[],
                    current_item_ids={int(getattr(asset, "item_id", 0) or 0) for asset in character_assets},
                )
                character_assets.extend(historical_character_assets)
                top_location_name_by_id = resolve_top_location_name_map(character_assets)
                return (
                    character_assets,
                    [],
                    character_name_by_id,
                    corporation_name_by_id,
                    top_location_name_by_id,
                )
            if normalized_scope.startswith("character_and_corporation:"):
                parts = normalized_scope.split(":")
                try:
                    selected_character_id = int(parts[1]) if len(parts) > 1 else 0
                except Exception:
                    selected_character_id = 0
                try:
                    selected_corporation_id = int(parts[2]) if len(parts) > 2 else 0
                except Exception:
                    selected_corporation_id = 0
                character_assets = blueprints_repo.get_character_blueprint_assets_for_ids(session, [selected_character_id])
                corporation_assets = blueprints_repo.get_corporation_blueprint_assets_for_ids(session, [selected_corporation_id])
                historical_character_assets, historical_corporation_assets = self._load_historical_blueprint_assets(
                    app_session=session,
                    sde_session=sde_session,
                    character_ids=[selected_character_id],
                    corporation_ids=[selected_corporation_id],
                    current_item_ids={
                        *[int(getattr(asset, "item_id", 0) or 0) for asset in character_assets],
                        *[int(getattr(asset, "item_id", 0) or 0) for asset in corporation_assets],
                    },
                )
                character_assets.extend(historical_character_assets)
                corporation_assets.extend(historical_corporation_assets)
                top_location_name_by_id = resolve_top_location_name_map([*character_assets, *corporation_assets])
                return (
                    character_assets,
                    corporation_assets,
                    character_name_by_id,
                    corporation_name_by_id,
                    top_location_name_by_id,
                )
            if normalized_scope.startswith("corporation:"):
                try:
                    selected_corporation_id = int(normalized_scope.split(":", 1)[1])
                except Exception:
                    selected_corporation_id = 0
                corporation_assets = blueprints_repo.get_corporation_blueprint_assets_for_ids(session, [selected_corporation_id])
                _, historical_corporation_assets = self._load_historical_blueprint_assets(
                    app_session=session,
                    sde_session=sde_session,
                    character_ids=[],
                    corporation_ids=[selected_corporation_id],
                    current_item_ids={int(getattr(asset, "item_id", 0) or 0) for asset in corporation_assets},
                )
                corporation_assets.extend(historical_corporation_assets)
                top_location_name_by_id = resolve_top_location_name_map(corporation_assets)
                return (
                    [],
                    corporation_assets,
                    character_name_by_id,
                    corporation_name_by_id,
                    top_location_name_by_id,
                )
            if normalized_scope == "all_characters":
                character_assets = blueprints_repo.get_character_blueprints(session)
                historical_character_assets, _ = self._load_historical_blueprint_assets(
                    app_session=session,
                    sde_session=sde_session,
                    character_ids=character_ids,
                    corporation_ids=[],
                    current_item_ids={int(getattr(asset, "item_id", 0) or 0) for asset in character_assets},
                )
                character_assets.extend(historical_character_assets)
                top_location_name_by_id = resolve_top_location_name_map(character_assets)
                return (
                    character_assets,
                    [],
                    character_name_by_id,
                    corporation_name_by_id,
                    top_location_name_by_id,
                )
            if normalized_scope == "character_and_corporations":
                character_assets = blueprints_repo.get_character_blueprints(session)
                corporation_assets = blueprints_repo.get_corporation_blueprint_assets_for_ids(session, corporation_ids)
                historical_character_assets, historical_corporation_assets = self._load_historical_blueprint_assets(
                    app_session=session,
                    sde_session=sde_session,
                    character_ids=character_ids,
                    corporation_ids=corporation_ids,
                    current_item_ids={
                        *[int(getattr(asset, "item_id", 0) or 0) for asset in character_assets],
                        *[int(getattr(asset, "item_id", 0) or 0) for asset in corporation_assets],
                    },
                )
                character_assets.extend(historical_character_assets)
                corporation_assets.extend(historical_corporation_assets)
                top_location_name_by_id = resolve_top_location_name_map([*character_assets, *corporation_assets])
                return (
                    character_assets,
                    corporation_assets,
                    character_name_by_id,
                    corporation_name_by_id,
                    top_location_name_by_id,
                )
            if normalized_scope == "all_corporations":
                corporation_assets = blueprints_repo.get_corporation_blueprints(session)
                _, historical_corporation_assets = self._load_historical_blueprint_assets(
                    app_session=session,
                    sde_session=sde_session,
                    character_ids=[],
                    corporation_ids=corporation_ids,
                    current_item_ids={int(getattr(asset, "item_id", 0) or 0) for asset in corporation_assets},
                )
                corporation_assets.extend(historical_corporation_assets)
                top_location_name_by_id = resolve_top_location_name_map(corporation_assets)
                return (
                    [],
                    corporation_assets,
                    character_name_by_id,
                    corporation_name_by_id,
                    top_location_name_by_id,
                )
            if normalized_scope == "all":
                character_assets = blueprints_repo.get_character_blueprints(session)
                corporation_assets = blueprints_repo.get_corporation_blueprints(session)
                historical_character_assets, historical_corporation_assets = self._load_historical_blueprint_assets(
                    app_session=session,
                    sde_session=sde_session,
                    character_ids=character_ids,
                    corporation_ids=corporation_ids,
                    current_item_ids={
                        *[int(getattr(asset, "item_id", 0) or 0) for asset in character_assets],
                        *[int(getattr(asset, "item_id", 0) or 0) for asset in corporation_assets],
                    },
                )
                character_assets.extend(historical_character_assets)
                corporation_assets.extend(historical_corporation_assets)
                top_location_name_by_id = resolve_top_location_name_map([*character_assets, *corporation_assets])
                return (
                    character_assets,
                    corporation_assets,
                    character_name_by_id,
                    corporation_name_by_id,
                    top_location_name_by_id,
                )
            character_assets = blueprints_repo.get_character_blueprints(session)
            historical_character_assets, _ = self._load_historical_blueprint_assets(
                app_session=session,
                sde_session=sde_session,
                character_ids=character_ids,
                corporation_ids=[],
                current_item_ids={int(getattr(asset, "item_id", 0) or 0) for asset in character_assets},
            )
            character_assets.extend(historical_character_assets)
            top_location_name_by_id = resolve_top_location_name_map(character_assets)
            return (
                character_assets,
                [],
                character_name_by_id,
                corporation_name_by_id,
                top_location_name_by_id,
            )
        finally:
            try:
                session.close()
            except Exception:
                pass
            try:
                sde_session.close()
            except Exception:
                pass

    def _get_owned_item_inventory(
        self,
        *,
        owned_blueprints_scope: str,
    ) -> tuple[dict[int, int], dict[int, float]]:
        session: Any = self._sessions.app_session()
        try:
            characters = self._state.char_manager.get_characters() or []
            character_ids: list[int] = []
            corporation_ids: list[int] = []
            for character in characters:
                if not isinstance(character, dict):
                    continue
                try:
                    character_id = int(character.get("character_id") or 0)
                except Exception:
                    character_id = 0
                try:
                    corporation_id = int(character.get("corporation_id") or 0)
                except Exception:
                    corporation_id = 0
                if character_id > 0:
                    character_ids.append(character_id)
                if corporation_id > 0:
                    corporation_ids.append(corporation_id)

            normalized_scope = (owned_blueprints_scope or "all_characters").strip().lower()

            def query_character_assets(ids: list[int]) -> list[CharacterAssetsModel]:
                normalized_ids = sorted({int(asset_id) for asset_id in ids if int(asset_id) > 0})
                if not normalized_ids:
                    return []
                return (
                    session.query(CharacterAssetsModel)
                    .filter(CharacterAssetsModel.character_id.in_(normalized_ids))
                    .all()
                )

            def query_corporation_assets(ids: list[int]) -> list[CorporationAssetsModel]:
                normalized_ids = sorted({int(asset_id) for asset_id in ids if int(asset_id) > 0})
                if not normalized_ids:
                    return []
                return (
                    session.query(CorporationAssetsModel)
                    .filter(CorporationAssetsModel.corporation_id.in_(normalized_ids))
                    .all()
                )

            character_assets: list[CharacterAssetsModel] = []
            corporation_assets: list[CorporationAssetsModel] = []
            if normalized_scope.startswith("character:"):
                try:
                    selected_character_id = int(normalized_scope.split(":", 1)[1])
                except Exception:
                    selected_character_id = 0
                character_assets = query_character_assets([selected_character_id])
            elif normalized_scope.startswith("character_and_corporation:"):
                parts = normalized_scope.split(":")
                try:
                    selected_character_id = int(parts[1]) if len(parts) > 1 else 0
                except Exception:
                    selected_character_id = 0
                try:
                    selected_corporation_id = int(parts[2]) if len(parts) > 2 else 0
                except Exception:
                    selected_corporation_id = 0
                character_assets = query_character_assets([selected_character_id])
                corporation_assets = query_corporation_assets([selected_corporation_id])
            elif normalized_scope.startswith("corporation:"):
                try:
                    selected_corporation_id = int(normalized_scope.split(":", 1)[1])
                except Exception:
                    selected_corporation_id = 0
                corporation_assets = query_corporation_assets([selected_corporation_id])
            elif normalized_scope == "all_characters":
                character_assets = query_character_assets(character_ids)
            elif normalized_scope == "character_and_corporations":
                character_assets = query_character_assets(character_ids)
                corporation_assets = query_corporation_assets(corporation_ids)
            elif normalized_scope == "all_corporations":
                corporation_assets = query_corporation_assets(corporation_ids)
            elif normalized_scope == "all":
                character_assets = query_character_assets(character_ids)
                corporation_assets = query_corporation_assets(corporation_ids)
            else:
                character_assets = query_character_assets(character_ids)

            quantity_by_type_id: dict[int, int] = {}
            exact_cost_total_by_type_id: dict[int, float] = {}
            exact_cost_quantity_by_type_id: dict[int, int] = {}
            fallback_cost_total_by_type_id: dict[int, float] = {}
            fallback_cost_quantity_by_type_id: dict[int, int] = {}
            for asset in [*character_assets, *corporation_assets]:
                try:
                    type_id = int(asset.type_id or 0)
                except Exception:
                    type_id = 0
                if type_id <= 0 or str(getattr(asset, "type_category_name", "") or "") == "Blueprint":
                    continue
                try:
                    quantity = int(getattr(asset, "quantity", 0) or 0)
                except Exception:
                    quantity = 0
                if quantity <= 0:
                    continue
                quantity_by_type_id[type_id] = int(quantity_by_type_id.get(type_id, 0)) + quantity

                unit_cost = self._as_float(getattr(asset, "acquisition_unit_cost", None))
                if unit_cost is not None and unit_cost > 0:
                    exact_cost_total_by_type_id[type_id] = float(exact_cost_total_by_type_id.get(type_id, 0.0)) + (float(unit_cost) * quantity)
                    exact_cost_quantity_by_type_id[type_id] = int(exact_cost_quantity_by_type_id.get(type_id, 0)) + quantity
                    continue

                fallback_unit_cost = self._as_float(getattr(asset, "type_average_price", None))
                if fallback_unit_cost is None or fallback_unit_cost <= 0:
                    fallback_unit_cost = self._as_float(getattr(asset, "type_adjusted_price", None))
                if fallback_unit_cost is None or fallback_unit_cost <= 0:
                    continue
                fallback_cost_total_by_type_id[type_id] = float(fallback_cost_total_by_type_id.get(type_id, 0.0)) + (float(fallback_unit_cost) * quantity)
                fallback_cost_quantity_by_type_id[type_id] = int(fallback_cost_quantity_by_type_id.get(type_id, 0)) + quantity

            unit_cost_by_type_id: dict[int, float] = {}
            for type_id, total_cost in exact_cost_total_by_type_id.items():
                quantity = int(exact_cost_quantity_by_type_id.get(type_id, 0))
                if quantity > 0:
                    unit_cost_by_type_id[type_id] = float(total_cost) / float(quantity)

            historical_cost_by_type_id = self._load_historical_item_unit_costs(
                app_session=session,
                character_ids=[asset.character_id for asset in character_assets if getattr(asset, "character_id", None) is not None],
                corporation_ids=[asset.corporation_id for asset in corporation_assets if getattr(asset, "corporation_id", None) is not None],
                type_ids=list(quantity_by_type_id.keys()),
            )
            for type_id, quantity in quantity_by_type_id.items():
                if type_id in unit_cost_by_type_id:
                    continue
                historical_unit_cost = self._as_float(historical_cost_by_type_id.get(type_id))
                if historical_unit_cost is not None and historical_unit_cost > 0:
                    unit_cost_by_type_id[type_id] = float(historical_unit_cost)
                    continue
                fallback_total_cost = float(fallback_cost_total_by_type_id.get(type_id, 0.0))
                fallback_quantity = int(fallback_cost_quantity_by_type_id.get(type_id, 0))
                if fallback_quantity > 0:
                    unit_cost_by_type_id[type_id] = float(fallback_total_cost) / float(fallback_quantity)

            return quantity_by_type_id, unit_cost_by_type_id
        finally:
            try:
                session.close()
            except Exception:
                pass
            try:
                sde_session.close()
            except Exception:
                pass

    def industry_manufacturing_product_overview(
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
        progress_callback: ProgressCallback | None = None,
    ) -> list[dict[str, Any]]:
        mgr = self._ensure_industry_job_manager()
        if progress_callback is not None:
            progress_callback(0.05, "Step 2/9: Loading blueprint snapshot", {"step": 2, "step_count": 9, "stage": "blueprints"})
        blueprint_rows = mgr.get_blueprint_overview(force_refresh=bool(force_refresh))
        if progress_callback is not None:
            progress_callback(
                0.10,
                "Step 3/9: Resolving character profile and pricing context",
                {"step": 3, "step_count": 9, "stage": "context"},
            )
        character_skill_levels = (
            self._get_character_trained_skill_levels(character_id=int(character_id))
            if character_id is not None
            else {}
        )
        selected_industry_profile = self._resolve_industry_profile_context(
            character_id=character_id,
            industry_profile_id=industry_profile_id,
        )
        include_reactions = bool(include_reactions) and self._reactions_allowed_for_profile(selected_industry_profile)
        selected_character_modifiers = self._get_character_industry_modifier_payload(character_id=character_id)
        character_skill_levels_by_name = self._skill_levels_by_name(selected_character_modifiers)
        adjusted_market_price_map = self._get_adjusted_market_price_map()
        (
            manufacturing_row_by_product_type_id,
            reaction_row_by_product_type_id,
            invention_row_by_blueprint_type_id,
        ) = self._build_blueprint_row_indexes(blueprint_rows)
        normalized_market_hub = MarketPricingService.normalize_market_hub(market_hub)
        normalized_material_price_side = MarketPricingService.normalize_order_side(material_price_side)
        normalized_product_price_side = MarketPricingService.normalize_order_side(product_price_side)
        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        product_sell_price_type_ids = sorted(
            {
                *[int(type_id) for type_id in manufacturing_row_by_product_type_id.keys()],
                *[int(type_id) for type_id in reaction_row_by_product_type_id.keys()],
            }
        )
        material_price_type_ids = sorted(
            {
                int(entry.get("type_id") or 0)
                for row in blueprint_rows
                if isinstance(row, dict)
                for job_name in ["manufacturing_job", "reaction_job", "invention_job"]
                for entry in (((row.get(job_name) or {}) if isinstance(row.get(job_name), dict) else {}).get("materials") or [])
                if isinstance(entry, dict) and int(entry.get("type_id") or 0) > 0
            }
        )
        product_sell_price_map = pricing_service.get_type_price_map(
            type_ids=product_sell_price_type_ids,
            hub=normalized_market_hub,
            side=normalized_product_price_side,
        )
        material_price_map = pricing_service.get_type_price_map(
            type_ids=material_price_type_ids,
            hub=normalized_market_hub,
            side=normalized_material_price_side,
        )
        blueprint_copy_assets_by_type_id: dict[int, list[CharacterAssetsModel | CorporationAssetsModel]] = {}
        blueprint_original_assets_by_type_id: dict[int, list[CharacterAssetsModel | CorporationAssetsModel]] = {}
        available_owned_item_quantity_by_type_id_base, owned_item_unit_cost_by_type_id = self._get_owned_item_inventory(
            owned_blueprints_scope=owned_blueprints_scope,
        )
        (
            character_blueprint_assets,
            corporation_blueprint_assets,
            character_name_by_id,
            corporation_name_by_id,
            top_location_name_by_id,
        ) = self._get_owned_blueprint_assets(owned_blueprints_scope=owned_blueprints_scope)
        if progress_callback is not None:
            progress_callback(
                0.25,
                "Step 4/9: Resolved owned blueprint assets",
                {
                    "step": 4,
                    "step_count": 9,
                    "stage": "assets",
                    "character_assets": len(character_blueprint_assets),
                    "corporation_assets": len(corporation_blueprint_assets),
                },
            )

        for asset in [*character_blueprint_assets, *corporation_blueprint_assets]:
            try:
                blueprint_type_id = int(asset.type_id)
            except Exception:
                continue
            if blueprint_type_id <= 0:
                continue
            if bool(getattr(asset, "is_blueprint_copy", False)):
                if not bool(build_from_bpc):
                    continue
                blueprint_copy_assets_by_type_id.setdefault(blueprint_type_id, []).append(asset)
            else:
                blueprint_original_assets_by_type_id.setdefault(blueprint_type_id, []).append(asset)

        for assets in blueprint_copy_assets_by_type_id.values():
            assets.sort(
                key=lambda asset: (
                    -int(asset.blueprint_material_efficiency or 0),
                    -int(asset.blueprint_time_efficiency or 0),
                    -int(asset.blueprint_runs or 0),
                    int(asset.item_id or 0),
                )
            )
        for assets in blueprint_original_assets_by_type_id.values():
            assets.sort(
                key=lambda asset: (
                    -int(asset.blueprint_material_efficiency or 0),
                    -int(asset.blueprint_time_efficiency or 0),
                    int(asset.item_id or 0),
                )
            )

        available_blueprint_copy_runs_by_type_id_base: dict[int, int] = {
            int(blueprint_type_id): sum(max(0, int(asset.blueprint_runs or 0)) for asset in assets)
            for blueprint_type_id, assets in blueprint_copy_assets_by_type_id.items()
        }

        def compact_material(entry: dict[str, Any]) -> dict[str, Any]:
            return {
                "base_price": entry.get("base_price"),
                "category_icon_id": entry.get("category_icon_id"),
                "category_id": entry.get("category_id"),
                "category_name": entry.get("category_name"),
                "group_icon_id": entry.get("group_icon_id"),
                "group_id": entry.get("group_id"),
                "group_name": entry.get("group_name"),
                "group_repackaged_volume": entry.get("group_repackaged_volume"),
                "group_use_base_price": entry.get("group_use_base_price"),
                "icon_id": entry.get("icon_id"),
                "portion_size": entry.get("portion_size"),
                "quantity": entry.get("quantity"),
                "repackaged_volume": entry.get("repackaged_volume"),
                "type_id": entry.get("type_id"),
                "type_name": entry.get("type_name"),
                "volume": entry.get("volume"),
            }

        def compact_skill(entry: dict[str, Any]) -> dict[str, Any]:
            return {
                "icon_id": entry.get("icon_id"),
                "level": entry.get("level"),
                "type_id": entry.get("type_id"),
                "type_name": entry.get("type_name"),
                "description": entry.get("description"),
            }

        def compact_required_skill(entry: dict[str, Any]) -> dict[str, Any]:
            required_skill = compact_skill(entry)
            skill_type_id = int(required_skill.get("type_id") or 0)
            required_level = int(required_skill.get("level") or 0)
            trained_skill_level = int(character_skill_levels.get(skill_type_id, 0))
            return {
                **required_skill,
                "trained_skill_level": trained_skill_level,
                "skill_requirement_met": trained_skill_level >= required_level,
            }

        def keyed_entries(entries: list[dict[str, Any]], *, compactor) -> dict[str, dict[str, Any]]:
            out: dict[str, dict[str, Any]] = {}
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                try:
                    type_id = int(entry.get("type_id") or 0)
                except Exception:
                    continue
                if type_id <= 0:
                    continue
                out[str(type_id)] = compactor(entry)
            return out

        def compact_invention_products(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                product = entry.get("product") or {}
                if not isinstance(product, dict):
                    product = {}
                probability = product.get("probability")
                if probability is None:
                    try:
                        probability = float(entry.get("probability_pct") or 0.0) / 100.0
                    except Exception:
                        probability = None
                out.append(
                    {
                        "probability": probability,
                        "quantity": entry.get("quantity"),
                        "type_id": product.get("type_id"),
                        "type_name": product.get("type_name"),
                    }
                )
            return out

        product_rows: list[dict[str, Any]] = []
        for row_index, row in enumerate(blueprint_rows, start=1):
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue

            blueprint_type_id = int(row.get("blueprint_type_id") or 0)
            matched_blueprint_copies = blueprint_copy_assets_by_type_id.get(blueprint_type_id) or []
            matched_blueprint_originals = blueprint_original_assets_by_type_id.get(blueprint_type_id) or []

            raw_products = manufacturing_job.get("products") or []
            if not isinstance(raw_products, list) or not raw_products:
                continue

            for product_index, raw_product in enumerate(raw_products, start=1):
                if not isinstance(raw_product, dict):
                    continue

                product_type_id = int(raw_product.get("type_id") or 0)
                if product_type_id <= 0:
                    continue

                product_quantity_per_run = int(raw_product.get("quantity") or 0)
                max_production_limit = int(manufacturing_job.get("max_production_limit") or 0)

                blueprint_sde_payload: dict[str, Any] = {
                    **dict(row.get("blueprint") or {}),
                    "blueprint_type_id": blueprint_type_id,
                    "copying": {"time": int(((row.get("copying_job") or {}).get("time_seconds") or 0))},
                    "research_material": {"time": int(((row.get("research_material_job") or {}).get("time_seconds") or 0))},
                    "research_time": {"time": int(((row.get("research_time_job") or {}).get("time_seconds") or 0))},
                }

                invention_job = row.get("invention_job") or {}
                if isinstance(invention_job, dict) and invention_job:
                    blueprint_sde_payload["invention"] = {
                        "materials": keyed_entries(
                            [dict(entry) for entry in (invention_job.get("materials") or [])],
                            compactor=compact_material,
                        ),
                        "products": compact_invention_products(
                            [dict(entry) for entry in (invention_job.get("products") or [])]
                        ),
                        "skills": keyed_entries(
                            [dict(entry) for entry in (invention_job.get("skill_entries") or [])],
                            compactor=compact_skill,
                        ),
                        "time": int(invention_job.get("time_seconds") or 0),
                    }

                if bool(build_from_bpc) and matched_blueprint_copies:
                    if bool(group_identical_bpcs):
                        selected_blueprint_variants: list[dict[str, Any]] = [
                            {
                                "blueprint_copy_assets": list(matched_blueprint_copies),
                                "blueprint_original_asset": None,
                            }
                        ]
                    else:
                        selected_blueprint_variants = [
                            {
                                "blueprint_copy_assets": [blueprint_copy_asset],
                                "blueprint_original_asset": None,
                            }
                            for blueprint_copy_asset in matched_blueprint_copies
                        ]
                elif matched_blueprint_originals:
                    selected_blueprint_variants = [
                        {
                            "blueprint_copy_assets": [],
                            "blueprint_original_asset": blueprint_original_asset,
                        }
                        for blueprint_original_asset in matched_blueprint_originals
                    ]
                else:
                    selected_blueprint_variants = [
                        {
                            "blueprint_copy_assets": [],
                            "blueprint_original_asset": None,
                        }
                    ]

                if bool(have_blueprint_source_only):
                    selected_blueprint_variants = [
                        variant
                        for variant in selected_blueprint_variants
                        if (variant.get("blueprint_copy_assets") or []) or variant.get("blueprint_original_asset") is not None
                    ]
                    if not selected_blueprint_variants:
                        continue

                for blueprint_variant in selected_blueprint_variants:
                    blueprint_copy_assets = cast(
                        list[CharacterAssetsModel | CorporationAssetsModel],
                        list(blueprint_variant.get("blueprint_copy_assets") or []),
                    )
                    blueprint_original_asset = cast(
                        CharacterAssetsModel | CorporationAssetsModel | None,
                        blueprint_variant.get("blueprint_original_asset"),
                    )
                    blueprint_copy_asset = blueprint_copy_assets[0] if blueprint_copy_assets else None
                    blueprint_copy_runs = sum(
                        max(0, int(getattr(copy_asset, "blueprint_runs", 0) or 0))
                        for copy_asset in blueprint_copy_assets
                    )
                    effective_runs = 1
                    if bool(maximize_bp_runs):
                        if blueprint_copy_runs > 0:
                            effective_runs = blueprint_copy_runs
                        elif max_production_limit > 0:
                            effective_runs = max_production_limit

                    available_blueprint_copy_runs_by_type_id = dict(available_blueprint_copy_runs_by_type_id_base)
                    available_owned_item_quantity_by_type_id = dict(available_owned_item_quantity_by_type_id_base)
                    if bool(build_from_bpc):
                        if blueprint_copy_assets:
                            available_blueprint_copy_runs_by_type_id[blueprint_type_id] = max(0, blueprint_copy_runs)
                        elif blueprint_original_asset is not None:
                            available_blueprint_copy_runs_by_type_id[blueprint_type_id] = 0

                    compact_product = dict(raw_product)
                    compact_product["quantity"] = product_quantity_per_run * effective_runs

                    manufacturing_skill_entries = keyed_entries(
                        [dict(entry) for entry in (manufacturing_job.get("skill_entries") or [])],
                        compactor=compact_required_skill,
                    )
                    manufacturing_skill_requirements_met = all(
                        bool(skill_entry.get("skill_requirement_met", False))
                        for skill_entry in manufacturing_skill_entries.values()
                        if isinstance(skill_entry, dict)
                    )
                    manufacturing_skills: dict[str, Any] = {
                        **manufacturing_skill_entries,
                        "skill_requirements_met": manufacturing_skill_requirements_met,
                    }
                    manufacturing_skill_entry_list = [
                        skill_entry
                        for skill_entry in manufacturing_skill_entries.values()
                        if isinstance(skill_entry, dict)
                    ]

                    blueprint_material_efficiency = 0
                    blueprint_time_efficiency = 0
                    blueprint_source_kind = "unowned"
                    include_copying_job = False
                    include_sde_research_chain = False
                    if bool(build_from_bpc):
                        if blueprint_copy_asset is not None:
                            blueprint_material_efficiency = int(blueprint_copy_asset.blueprint_material_efficiency or 0)
                            blueprint_time_efficiency = int(blueprint_copy_asset.blueprint_time_efficiency or 0)
                            blueprint_source_kind = "owned_blueprint_copy"
                            include_copying_job = max(0, blueprint_copy_runs) < effective_runs and bool(matched_blueprint_originals)
                        elif blueprint_original_asset is not None:
                            blueprint_material_efficiency = int(blueprint_original_asset.blueprint_material_efficiency or 0)
                            blueprint_time_efficiency = int(blueprint_original_asset.blueprint_time_efficiency or 0)
                            blueprint_source_kind = "copied_from_owned_blueprint_original"
                            include_copying_job = True
                        else:
                            blueprint_source_kind = "unowned_blueprint_copy"
                    else:
                        if blueprint_original_asset is not None:
                            blueprint_material_efficiency = int(blueprint_original_asset.blueprint_material_efficiency or 0)
                            blueprint_time_efficiency = int(blueprint_original_asset.blueprint_time_efficiency or 0)
                            blueprint_source_kind = "owned_blueprint_original"
                        else:
                            blueprint_material_efficiency = self._MAX_BLUEPRINT_MATERIAL_EFFICIENCY
                            blueprint_time_efficiency = self._MAX_BLUEPRINT_TIME_EFFICIENCY
                            blueprint_source_kind = "blueprint_sde_fallback"
                            include_sde_research_chain = True

                    manufacturing_group = self._infer_manufacturing_group(compact_product)
                    manufacturing_material_reduction = self._combine_reductions(
                        [
                            float(blueprint_material_efficiency) / 100.0,
                            self._profile_base_reduction(
                                profile_payload=selected_industry_profile,
                                activity="manufacturing",
                                metric="material",
                            ),
                            self._profile_rig_reduction(
                                profile_payload=selected_industry_profile,
                                activity="manufacturing",
                                metric="material",
                                manufacturing_group=manufacturing_group,
                            ),
                            self._implant_reduction(
                                character_modifier_payload=selected_character_modifiers,
                                activity="manufacturing",
                                metric="material",
                            ),
                        ]
                    )
                    manufacturing_time_reduction = self._combine_reductions(
                        [
                            float(blueprint_time_efficiency) / 100.0,
                            self._skill_time_reduction(
                                activity="manufacturing",
                                skill_levels_by_name=character_skill_levels_by_name,
                                required_skill_entries=manufacturing_skill_entry_list,
                            ),
                            self._profile_base_reduction(
                                profile_payload=selected_industry_profile,
                                activity="manufacturing",
                                metric="time",
                            ),
                            self._profile_rig_reduction(
                                profile_payload=selected_industry_profile,
                                activity="manufacturing",
                                metric="time",
                                manufacturing_group=manufacturing_group,
                            ),
                            self._implant_reduction(
                                character_modifier_payload=selected_character_modifiers,
                                activity="manufacturing",
                                metric="time",
                            ),
                        ]
                    )
                    manufacturing_cost_reduction = self._combine_reductions(
                        [
                            self._profile_base_reduction(
                                profile_payload=selected_industry_profile,
                                activity="manufacturing",
                                metric="cost",
                            ),
                            self._profile_rig_reduction(
                                profile_payload=selected_industry_profile,
                                activity="manufacturing",
                                metric="cost",
                                manufacturing_group=manufacturing_group,
                            ),
                            self._implant_reduction(
                                character_modifier_payload=selected_character_modifiers,
                                activity="manufacturing",
                                metric="cost",
                            ),
                        ]
                    )

                    base_material_entries: list[dict[str, Any]] = []
                    adjusted_material_entries: list[dict[str, Any]] = []
                    for raw_material in [dict(entry) for entry in (manufacturing_job.get("materials") or [])]:
                        material = compact_material(raw_material)
                        quantity_per_run = int(material.get("quantity") or 0)
                        base_total_quantity = quantity_per_run * effective_runs
                        adjusted_total_quantity = self._round_material_quantity(
                            float(base_total_quantity) * max(0.0, 1.0 - manufacturing_material_reduction),
                            minimum_quantity=(effective_runs if quantity_per_run > 0 else 0),
                        )
                        material["quantity_per_run"] = quantity_per_run
                        material["base_quantity"] = base_total_quantity
                        material["quantity"] = adjusted_total_quantity
                        material["material_reduction"] = manufacturing_material_reduction
                        adjusted_material_entries.append(material)
                        base_material_entries.append({**material, "quantity": base_total_quantity})

                    base_material_eiv_total, base_material_eiv_priced_count = self._sum_estimated_item_value(
                        base_material_entries,
                        quantity_key="quantity",
                        adjusted_price_map=adjusted_market_price_map,
                    )
                    per_run_material_eiv, _ = self._sum_estimated_item_value(
                        adjusted_material_entries,
                        quantity_key="quantity_per_run",
                        adjusted_price_map=adjusted_market_price_map,
                    )
                    product_eiv_unit, product_eiv_source = self._resolve_eiv_pricing(
                        type_id=product_type_id,
                        type_payload=compact_product,
                        adjusted_price_map=adjusted_market_price_map,
                    )
                    product_eiv_total = (
                        float(product_eiv_unit) * int(compact_product.get("quantity") or 0)
                        if product_eiv_unit is not None
                        else None
                    )

                    manufacturing_cost_index = self._system_cost_index(
                        profile_payload=selected_industry_profile,
                        activity="manufacturing",
                    )
                    installation_surcharge = self._profile_installation_surcharge(selected_industry_profile)
                    manufacturing_job_cost = self._job_cost_total(
                        process_value=base_material_eiv_total,
                        cost_index=manufacturing_cost_index,
                        cost_reduction=manufacturing_cost_reduction,
                        installation_surcharge=installation_surcharge,
                    )

                    direct_manufacturing_time_seconds = self._round_duration_seconds(
                        int(manufacturing_job.get("time_seconds") or 0)
                        * effective_runs
                        * max(0.0, 1.0 - manufacturing_time_reduction)
                    )

                    activity_breakdown: dict[str, Any] = {
                        "manufacturing": {
                            "activity": "manufacturing",
                            "duration_seconds": direct_manufacturing_time_seconds,
                            "base_duration_seconds": int(manufacturing_job.get("time_seconds") or 0) * effective_runs,
                            "time_reduction": manufacturing_time_reduction,
                            "material_reduction": manufacturing_material_reduction,
                            "cost_reduction": manufacturing_cost_reduction,
                            "cost_index": manufacturing_cost_index,
                            "estimated_item_value": base_material_eiv_total,
                            "estimated_item_value_priced_material_count": base_material_eiv_priced_count,
                            **manufacturing_job_cost,
                        }
                    }
                    total_time_seconds = direct_manufacturing_time_seconds
                    total_job_cost = float(manufacturing_job_cost.get("total_job_cost") or 0.0)
                    priced_job_count = 1 if manufacturing_job_cost.get("total_job_cost") is not None else 0
                    prerequisite_tree_nodes: list[dict[str, Any]] = []
                    top_level_procurement_materials = cast(list[dict[str, Any]], [*adjusted_material_entries])
                    invention_source_row = invention_row_by_blueprint_type_id.get(int(blueprint_type_id))
                    invention_job = (invention_source_row or {}).get("invention_job") or {}
                    has_top_level_invention_path = invention_source_row and isinstance(invention_job, dict)
                    available_target_copy_runs = (
                        int(available_blueprint_copy_runs_by_type_id.get(blueprint_type_id, 0)) if bool(build_from_bpc) else 0
                    )
                    owned_target_copy_runs_used = min(max(0, available_target_copy_runs), effective_runs) if bool(build_from_bpc) else 0
                    if bool(build_from_bpc):
                        available_blueprint_copy_runs_by_type_id[blueprint_type_id] = max(
                            0,
                            available_target_copy_runs - owned_target_copy_runs_used,
                        )
                    missing_top_level_copy_runs = max(0, effective_runs - owned_target_copy_runs_used) if bool(build_from_bpc) else 0
                    requires_copying_job = bool(include_copying_job) and missing_top_level_copy_runs > 0
                    requires_invention_chain = missing_top_level_copy_runs > 0 and not matched_blueprint_originals
                    owned_top_level_copy_allocations = self._allocate_owned_blueprint_copy_run_usage(
                        blueprint_copy_assets,
                        requested_runs=owned_target_copy_runs_used,
                        available_total_runs=owned_target_copy_runs_used,
                    )

                    if bool(build_from_bpc) and owned_top_level_copy_allocations:
                        for consumed_blueprint_copy_asset, consumed_copy_runs in owned_top_level_copy_allocations:
                            current_copy_node_fields = self._owned_blueprint_copy_node_fields(
                                consumed_blueprint_copy_asset,
                                blueprint_name=str((row.get("blueprint") or {}).get("type_name") or blueprint_type_id),
                                recommendation_action="take",
                                runs_required=consumed_copy_runs,
                                category_name=str(compact_product.get("category_name") or "") or None,
                                meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                                use_invention_label=bool(has_top_level_invention_path),
                                character_name_by_id=character_name_by_id,
                                corporation_name_by_id=corporation_name_by_id,
                            )
                            prerequisite_tree_nodes.append(
                                self._job_tree_node(
                                    label=(self._ACTIVITY_LABELS["invention"] if has_top_level_invention_path else self._ACTIVITY_LABELS["copying"]),
                                    **current_copy_node_fields,
                                )
                            )

                    if int(((row.get("copying_job") or {}).get("time_seconds") or 0)) > 0 and not (
                        has_top_level_invention_path and not matched_blueprint_originals
                    ):
                        copying_time_reduction = self._combine_reductions(
                            [
                                self._skill_time_reduction(
                                    activity="copying",
                                    skill_levels_by_name=character_skill_levels_by_name,
                                ),
                                self._profile_base_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="copying",
                                    metric="time",
                                ),
                                self._profile_rig_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="copying",
                                    metric="time",
                                ),
                                self._implant_reduction(
                                    character_modifier_payload=selected_character_modifiers,
                                    activity="copying",
                                    metric="time",
                                ),
                            ]
                        )
                        copying_cost_reduction = self._combine_reductions(
                            [
                                self._profile_base_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="copying",
                                    metric="cost",
                                ),
                                self._profile_rig_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="copying",
                                    metric="cost",
                                ),
                                self._implant_reduction(
                                    character_modifier_payload=selected_character_modifiers,
                                    activity="copying",
                                    metric="cost",
                                ),
                            ]
                        )
                        copying_cost_index = self._system_cost_index(
                            profile_payload=selected_industry_profile,
                            activity="copying",
                        )
                        base_copy_time_seconds = int(((row.get("copying_job") or {}).get("time_seconds") or 0)) * max(
                            0,
                            missing_top_level_copy_runs,
                        )
                        copy_process_value = (
                            float(per_run_material_eiv or 0.0) * max(0, missing_top_level_copy_runs) * 0.02
                            if per_run_material_eiv is not None
                            else None
                        )
                        copying_job_cost = self._job_cost_total(
                            process_value=copy_process_value,
                            cost_index=copying_cost_index,
                            cost_reduction=copying_cost_reduction,
                            installation_surcharge=installation_surcharge,
                        )
                        copying_duration_seconds = self._round_duration_seconds(
                            float(base_copy_time_seconds) * max(0.0, 1.0 - copying_time_reduction)
                        )
                        activity_breakdown["copying"] = {
                            "activity": "copying",
                            "duration_seconds": copying_duration_seconds,
                            "base_duration_seconds": base_copy_time_seconds,
                            "time_reduction": copying_time_reduction,
                            "cost_reduction": copying_cost_reduction,
                            "cost_index": copying_cost_index,
                            "estimated_item_value": per_run_material_eiv,
                            "runs": max(0, missing_top_level_copy_runs),
                            **copying_job_cost,
                        }
                        if requires_copying_job:
                            total_time_seconds += copying_duration_seconds
                            if copying_job_cost.get("total_job_cost") is not None:
                                total_job_cost += float(copying_job_cost.get("total_job_cost") or 0.0)
                                priced_job_count += 1
                        else:
                            activity_breakdown.pop("copying", None)
                        if requires_copying_job or owned_target_copy_runs_used <= 0:
                            prerequisite_tree_nodes.append(
                                self._job_tree_node(
                                    label=self._ACTIVITY_LABELS["copying"],
                                    node_type="activity",
                                    activity="copying",
                                    blueprint_name=str(
                                        ((row.get("blueprint") or {}) if isinstance(row.get("blueprint"), dict) else {}).get("type_name")
                                        or ((row.get("blueprint") or {}) if isinstance(row.get("blueprint"), dict) else {}).get("name")
                                        or blueprint_type_id
                                        or "Blueprint"
                                    ),
                                    quantity=1,
                                    runs=max(0, missing_top_level_copy_runs),
                                    duration_seconds=copying_duration_seconds,
                                    direct_duration_seconds=copying_duration_seconds,
                                    job_cost=copying_job_cost.get("total_job_cost"),
                                    total_job_cost=copying_job_cost.get("total_job_cost"),
                                    category_name=str(compact_product.get("category_name") or "") or None,
                                    meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                                    recommendation_action="copy",
                                    children=[],
                                )
                            )

                    if invention_source_row and isinstance(invention_job, dict):
                        invention_blueprint_payload = invention_source_row.get("blueprint") or {}
                        if not isinstance(invention_blueprint_payload, dict):
                            invention_blueprint_payload = {}
                        invention_blueprint_name = str(
                            invention_blueprint_payload.get("type_name")
                            or invention_blueprint_payload.get("name")
                            or invention_source_row.get("blueprint_type_id")
                            or "Blueprint"
                        )
                        target_entry = None
                        for product in invention_job.get("products") or []:
                            if not isinstance(product, dict):
                                continue
                            invention_product = product.get("product") or {}
                            if not isinstance(invention_product, dict):
                                invention_product = {}
                            if int(invention_product.get("type_id") or product.get("type_id") or 0) == int(blueprint_type_id):
                                target_entry = product
                                break

                        probability = 0.0
                        if isinstance(target_entry, dict):
                            try:
                                probability = float(target_entry.get("probability_pct") or 0.0) / 100.0
                            except Exception:
                                probability = 0.0
                        target_blueprint_name = str(
                            ((target_entry or {}).get("product") or {}).get("type_name")
                            if isinstance((target_entry or {}).get("product"), dict)
                            else ""
                        ).strip() or str((row.get("blueprint") or {}).get("type_name") or blueprint_type_id)

                        successful_invention_jobs = (
                            max(
                                1,
                                int(
                                    math.ceil(
                                        float(max(1, missing_top_level_copy_runs))
                                        / float(max(1, max_production_limit or 1))
                                    )
                                ),
                            )
                            if requires_invention_chain
                            else 0
                        )
                        invention_attempts = (
                            max(
                                successful_invention_jobs,
                                int(math.ceil(float(successful_invention_jobs) / max(probability, 0.01))),
                            )
                            if successful_invention_jobs > 0
                            else 0
                        )
                        invention_materials = [
                            {**dict(entry), "quantity": int(entry.get("quantity") or 0) * invention_attempts}
                            for entry in (invention_job.get("materials") or [])
                            if isinstance(entry, dict)
                        ]
                        invention_process_value, _ = self._sum_estimated_item_value(
                            invention_materials,
                            quantity_key="quantity",
                            adjusted_price_map=adjusted_market_price_map,
                        )
                        invention_time_reduction = self._combine_reductions(
                            [
                                self._skill_time_reduction(
                                    activity="invention",
                                    skill_levels_by_name=character_skill_levels_by_name,
                                ),
                                self._profile_base_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="invention",
                                    metric="time",
                                ),
                                self._profile_rig_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="invention",
                                    metric="time",
                                ),
                                self._implant_reduction(
                                    character_modifier_payload=selected_character_modifiers,
                                    activity="invention",
                                    metric="time",
                                ),
                            ]
                        )
                        invention_cost_index = self._system_cost_index(
                            profile_payload=selected_industry_profile,
                            activity="invention",
                        )
                        invention_cost_reduction = self._combine_reductions(
                            [
                                self._profile_base_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="invention",
                                    metric="cost",
                                ),
                                self._profile_rig_reduction(
                                    profile_payload=selected_industry_profile,
                                    activity="invention",
                                    metric="cost",
                                ),
                                self._implant_reduction(
                                    character_modifier_payload=selected_character_modifiers,
                                    activity="invention",
                                    metric="cost",
                                ),
                            ]
                        )
                        invention_job_cost = self._job_cost_total(
                            process_value=invention_process_value,
                            cost_index=invention_cost_index,
                            cost_reduction=invention_cost_reduction,
                            installation_surcharge=installation_surcharge,
                        )
                        invention_duration_seconds = (
                            self._round_duration_seconds(
                                int(invention_job.get("time_seconds") or 0)
                                * invention_attempts
                                * max(0.0, 1.0 - invention_time_reduction)
                            )
                            if invention_attempts > 0
                            else 0
                        )
                        planned_invention_materials, invention_material_nodes = self._plan_take_or_buy_material_nodes(
                            invention_materials,
                            available_owned_item_quantity_by_type_id=(
                                available_owned_item_quantity_by_type_id
                                if requires_invention_chain
                                else dict(available_owned_item_quantity_by_type_id)
                            ),
                            owned_item_unit_cost_by_type_id=owned_item_unit_cost_by_type_id,
                            sell_price_map=material_price_map,
                            adjusted_price_map=adjusted_market_price_map,
                        )
                        activity_breakdown["invention"] = {
                            "activity": "invention",
                            "duration_seconds": invention_duration_seconds,
                            "base_duration_seconds": int(invention_job.get("time_seconds") or 0) * invention_attempts,
                            "time_reduction": invention_time_reduction,
                            "cost_reduction": invention_cost_reduction,
                            "cost_index": invention_cost_index,
                            "estimated_item_value": invention_process_value,
                            "runs": invention_attempts,
                            **invention_job_cost,
                        }
                        if requires_invention_chain:
                            total_time_seconds += invention_duration_seconds
                            if invention_job_cost.get("total_job_cost") is not None:
                                total_job_cost += float(invention_job_cost.get("total_job_cost") or 0.0)
                                priced_job_count += 1
                            top_level_procurement_materials.extend(planned_invention_materials)
                            generated_target_copy_runs = successful_invention_jobs * max(1, max_production_limit or 1)
                            if generated_target_copy_runs > missing_top_level_copy_runs:
                                available_blueprint_copy_runs_by_type_id[blueprint_type_id] = max(
                                    0,
                                    int(available_blueprint_copy_runs_by_type_id.get(blueprint_type_id, 0))
                                    + (generated_target_copy_runs - missing_top_level_copy_runs),
                                )
                        else:
                            activity_breakdown.pop("invention", None)
                        if requires_invention_chain or owned_target_copy_runs_used <= 0:
                            prerequisite_tree_nodes.append(
                                self._job_tree_node(
                                    label=self._ACTIVITY_LABELS["invention"],
                                    node_type="activity",
                                    activity="invention",
                                    blueprint_name=target_blueprint_name,
                                    blueprint_type_id=blueprint_type_id,
                                    runs=invention_attempts,
                                    duration_seconds=invention_duration_seconds,
                                    direct_duration_seconds=invention_duration_seconds,
                                    job_cost=invention_job_cost.get("total_job_cost"),
                                    total_job_cost=invention_job_cost.get("total_job_cost"),
                                    category_name=str(compact_product.get("category_name") or "") or None,
                                    meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                                    recommendation_action=("invent" if invention_attempts > 0 else "take"),
                                    children=(
                                        [
                                            self._job_tree_node(
                                                label=self._ACTIVITY_LABELS["materials"],
                                                node_type="materials",
                                                activity="materials",
                                                children=invention_material_nodes,
                                            )
                                        ]
                                        if invention_attempts > 0 and invention_material_nodes
                                        else []
                                    ),
                                )
                            )

                        source_copying_job = (
                            (invention_source_row.get("copying_job") or {}) if isinstance(invention_source_row, dict) else {}
                        )
                        source_blueprint_type_id = int((invention_source_row or {}).get("blueprint_type_id") or 0)
                        available_source_copy_runs = int(available_blueprint_copy_runs_by_type_id.get(source_blueprint_type_id, 0))
                        source_copy_runs_used = min(max(0, available_source_copy_runs), invention_attempts)
                        available_blueprint_copy_runs_by_type_id[source_blueprint_type_id] = max(
                            0,
                            available_source_copy_runs - source_copy_runs_used,
                        )
                        missing_source_copy_runs = max(0, invention_attempts - source_copy_runs_used)
                        if requires_invention_chain and isinstance(source_copying_job, dict) and int(source_copying_job.get("time_seconds") or 0) > 0:
                                source_manufacturing_materials = [
                                    dict(entry)
                                    for entry in ((invention_source_row.get("manufacturing_job") or {}).get("materials") or [])
                                    if isinstance(entry, dict)
                                ]
                                source_copy_process_value, _ = self._sum_estimated_item_value(
                                    source_manufacturing_materials,
                                    quantity_key="quantity",
                                    adjusted_price_map=adjusted_market_price_map,
                                )
                                source_copy_job_cost = self._job_cost_total(
                                    process_value=(
                                        float(source_copy_process_value) * float(max(0, missing_source_copy_runs)) * 0.02
                                        if source_copy_process_value is not None
                                        else None
                                    ),
                                    cost_index=self._system_cost_index(
                                        profile_payload=selected_industry_profile,
                                        activity="copying",
                                    ),
                                    cost_reduction=self._combine_reductions(
                                        [
                                            self._profile_base_reduction(
                                                profile_payload=selected_industry_profile,
                                                activity="copying",
                                                metric="cost",
                                            ),
                                            self._profile_rig_reduction(
                                                profile_payload=selected_industry_profile,
                                                activity="copying",
                                                metric="cost",
                                            ),
                                            self._implant_reduction(
                                                character_modifier_payload=selected_character_modifiers,
                                                activity="copying",
                                                metric="cost",
                                            ),
                                        ]
                                    ),
                                    installation_surcharge=installation_surcharge,
                                )
                                source_copy_duration_seconds = (
                                    self._round_duration_seconds(
                                        int(source_copying_job.get("time_seconds") or 0)
                                        * max(0, missing_source_copy_runs)
                                        * max(
                                            0.0,
                                            1.0
                                            - self._combine_reductions(
                                                [
                                                    self._skill_time_reduction(
                                                        activity="copying",
                                                        skill_levels_by_name=character_skill_levels_by_name,
                                                    ),
                                                    self._profile_base_reduction(
                                                        profile_payload=selected_industry_profile,
                                                        activity="copying",
                                                        metric="time",
                                                    ),
                                                    self._profile_rig_reduction(
                                                        profile_payload=selected_industry_profile,
                                                        activity="copying",
                                                        metric="time",
                                                    ),
                                                    self._implant_reduction(
                                                        character_modifier_payload=selected_character_modifiers,
                                                        activity="copying",
                                                        metric="time",
                                                    ),
                                                ]
                                            ),
                                        )
                                    )
                                    if missing_source_copy_runs > 0
                                    else 0
                                )
                                if requires_invention_chain and missing_source_copy_runs > 0:
                                    total_time_seconds += source_copy_duration_seconds
                                    if source_copy_job_cost.get("total_job_cost") is not None:
                                        total_job_cost += float(source_copy_job_cost.get("total_job_cost") or 0.0)
                                        priced_job_count += 1
                                prerequisite_tree_nodes.append(
                                    self._job_tree_node(
                                        label=self._ACTIVITY_LABELS["copying"],
                                        node_type="activity",
                                        activity="copying",
                                        blueprint_name=invention_blueprint_name,
                                        runs=max(0, missing_source_copy_runs),
                                        duration_seconds=source_copy_duration_seconds,
                                        direct_duration_seconds=source_copy_duration_seconds,
                                        job_cost=source_copy_job_cost.get("total_job_cost"),
                                        total_job_cost=source_copy_job_cost.get("total_job_cost"),
                                        category_name=str(compact_product.get("category_name") or "") or None,
                                        meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                                        recommendation_action="copy",
                                        children=[],
                                    )
                                )

                    if include_sde_research_chain:
                        for activity_name, source_field in [
                            ("research_material", "research_material_job"),
                            ("research_time", "research_time_job"),
                        ]:
                            base_level_one_duration = int(((row.get(source_field) or {}).get("time_seconds") or 0))
                            target_duration_seconds = self._research_target_duration_seconds(
                                level_one_duration_seconds=base_level_one_duration,
                                target_level=self._MAX_RESEARCH_LEVEL,
                            )
                            if target_duration_seconds <= 0:
                                continue
                            research_time_reduction = self._combine_reductions(
                                [
                                    self._skill_time_reduction(
                                        activity=activity_name,
                                        skill_levels_by_name=character_skill_levels_by_name,
                                    ),
                                    self._profile_base_reduction(
                                        profile_payload=selected_industry_profile,
                                        activity=activity_name,
                                        metric="time",
                                    ),
                                    self._profile_rig_reduction(
                                        profile_payload=selected_industry_profile,
                                        activity=activity_name,
                                        metric="time",
                                    ),
                                    self._implant_reduction(
                                        character_modifier_payload=selected_character_modifiers,
                                        activity=activity_name,
                                        metric="time",
                                    ),
                                ]
                            )
                            research_cost_reduction = self._combine_reductions(
                                [
                                    self._profile_base_reduction(
                                        profile_payload=selected_industry_profile,
                                        activity=activity_name,
                                        metric="cost",
                                    ),
                                    self._profile_rig_reduction(
                                        profile_payload=selected_industry_profile,
                                        activity=activity_name,
                                        metric="cost",
                                    ),
                                    self._implant_reduction(
                                        character_modifier_payload=selected_character_modifiers,
                                        activity=activity_name,
                                        metric="cost",
                                    ),
                                ]
                            )
                            research_cost_index = self._system_cost_index(
                                profile_payload=selected_industry_profile,
                                activity=activity_name,
                            )
                            research_process_value = (
                                float(per_run_material_eiv or 0.0) * 0.02105 * float(target_duration_seconds)
                                if per_run_material_eiv is not None
                                else None
                            )
                            research_job_cost = self._job_cost_total(
                                process_value=research_process_value,
                                cost_index=research_cost_index,
                                cost_reduction=research_cost_reduction,
                                installation_surcharge=installation_surcharge,
                            )
                            research_duration_seconds = self._round_duration_seconds(
                                float(target_duration_seconds) * max(0.0, 1.0 - research_time_reduction)
                            )
                            activity_breakdown[activity_name] = {
                                "activity": activity_name,
                                "duration_seconds": research_duration_seconds,
                                "base_duration_seconds": target_duration_seconds,
                                "time_reduction": research_time_reduction,
                                "cost_reduction": research_cost_reduction,
                                "cost_index": research_cost_index,
                                "estimated_item_value": per_run_material_eiv,
                                **research_job_cost,
                            }
                            total_time_seconds += research_duration_seconds
                            if research_job_cost.get("total_job_cost") is not None:
                                total_job_cost += float(research_job_cost.get("total_job_cost") or 0.0)
                                priced_job_count += 1
                            prerequisite_tree_nodes.append(
                                self._job_tree_node(
                                    label=self._ACTIVITY_LABELS.get(activity_name, activity_name),
                                    node_type="activity",
                                    activity=activity_name,
                                    runs=None,
                                    duration_seconds=research_duration_seconds,
                                    direct_duration_seconds=research_duration_seconds,
                                    job_cost=research_job_cost.get("total_job_cost"),
                                    total_job_cost=research_job_cost.get("total_job_cost"),
                                    children=[],
                                )
                            )

                    selected_blueprint_copy_asset = (
                        owned_top_level_copy_allocations[0][0]
                        if owned_top_level_copy_allocations
                        else blueprint_copy_asset
                    )
                    blueprint_copy_payload = self._compact_owned_blueprint_assets(
                        blueprint_copy_assets if len(blueprint_copy_assets) > 1 else ([selected_blueprint_copy_asset] if selected_blueprint_copy_asset is not None else []),
                        character_name_by_id=character_name_by_id,
                        corporation_name_by_id=corporation_name_by_id,
                        top_location_name_by_id=top_location_name_by_id,
                    )
                    blueprint_copy_variant_key = (
                        "-".join(
                            str(int(getattr(copy_asset, "item_id", 0) or 0))
                            for copy_asset in blueprint_copy_assets
                            if int(getattr(copy_asset, "item_id", 0) or 0) > 0
                        )
                        or "none"
                    )
                    uses_blueprint_original = (
                        blueprint_source_kind in {"copied_from_owned_blueprint_original", "owned_blueprint_original"}
                        or requires_copying_job
                    )
                    blueprint_original_payload = self._compact_owned_blueprint_asset(
                        blueprint_original_asset if uses_blueprint_original else None,
                        character_name_by_id=character_name_by_id,
                        corporation_name_by_id=corporation_name_by_id,
                        top_location_name_by_id=top_location_name_by_id,
                    )
                    blueprint_original_item_id = (
                        int(blueprint_original_asset.item_id) if uses_blueprint_original and blueprint_original_asset is not None else None
                    )

                    recursive_prerequisite_plan = self._build_recursive_prerequisite_plan(
                        adjusted_material_entries=adjusted_material_entries,
                        blueprint_type_id=blueprint_type_id,
                        build_from_bpc=build_from_bpc,
                        include_reactions=include_reactions,
                        selected_industry_profile=selected_industry_profile,
                        selected_character_modifiers=selected_character_modifiers,
                        character_skill_levels_by_name=character_skill_levels_by_name,
                        adjusted_market_price_map=adjusted_market_price_map,
                        sell_price_map=material_price_map,
                        blueprint_copy_assets_by_type_id=blueprint_copy_assets_by_type_id,
                        available_blueprint_copy_runs_by_type_id=available_blueprint_copy_runs_by_type_id,
                        available_owned_item_quantity_by_type_id=available_owned_item_quantity_by_type_id,
                        owned_item_unit_cost_by_type_id=owned_item_unit_cost_by_type_id,
                        blueprint_original_assets_by_type_id=blueprint_original_assets_by_type_id,
                        character_name_by_id=character_name_by_id,
                        corporation_name_by_id=corporation_name_by_id,
                        manufacturing_row_by_product_type_id=manufacturing_row_by_product_type_id,
                        reaction_row_by_product_type_id=reaction_row_by_product_type_id,
                        invention_row_by_blueprint_type_id=invention_row_by_blueprint_type_id,
                        include_current_blueprint_prerequisites=False,
                    )
                    top_level_procurement_materials = cast(
                        list[dict[str, Any]],
                        recursive_prerequisite_plan.get("procurement_materials") or top_level_procurement_materials,
                    )
                    total_time_seconds += int(recursive_prerequisite_plan.get("time_seconds") or 0)
                    if recursive_prerequisite_plan.get("job_cost") is not None:
                        total_job_cost += float(recursive_prerequisite_plan.get("job_cost") or 0.0)
                        priced_job_count += int(recursive_prerequisite_plan.get("priced_job_count") or 0)
                    if recursive_prerequisite_plan.get("enabled"):
                        activity_breakdown["recursive_prerequisites"] = {
                            "activity": "recursive_prerequisites",
                            "duration_seconds": int(recursive_prerequisite_plan.get("time_seconds") or 0),
                            "total_job_cost": recursive_prerequisite_plan.get("job_cost"),
                            "reactions_enabled": bool(recursive_prerequisite_plan.get("reactions_enabled", False)),
                            "activities": recursive_prerequisite_plan.get("activity_breakdown") or {},
                        }

                    prerequisite_tree_nodes.extend(
                        [
                            node
                            for node in (recursive_prerequisite_plan.get("tree_children") or [])
                            if isinstance(node, dict)
                        ]
                    )
                    top_level_material_nodes = cast(
                        list[dict[str, Any]],
                        recursive_prerequisite_plan.get("material_nodes") or [],
                    )
                    if not top_level_material_nodes:
                        top_level_material_nodes = [
                            self._job_tree_node(
                                label=str(material.get("type_name") or material.get("type_id") or "Material"),
                                node_type="material",
                                activity="material",
                                sourcing_strategy=(
                                    "take_or_buy_reaction_available"
                                    if not (bool(include_reactions) and self._reactions_allowed_for_profile(selected_industry_profile))
                                    and int(material.get("type_id") or 0) in reaction_row_by_product_type_id
                                    else "take_or_buy"
                                ),
                                category_name=str(material.get("category_name") or "") or None,
                                meta_group_name=str(material.get("meta_group_name") or "") or None,
                                type_id=int(material.get("type_id") or 0),
                                quantity=int(material.get("quantity") or 0),
                                runs=None,
                                duration_seconds=None,
                                job_cost=None,
                                total_job_cost=None,
                                children=[],
                            )
                            for material in adjusted_material_entries
                            if isinstance(material, dict)
                        ]
                    manufacturing_tree = self._job_tree_node(
                        label=self._ACTIVITY_LABELS["manufacturing"],
                        node_type="activity",
                        activity="manufacturing",
                        blueprint_type_id=blueprint_type_id,
                        type_id=product_type_id,
                        quantity=int(compact_product.get("quantity") or 0),
                        runs=effective_runs,
                        duration_seconds=total_time_seconds,
                        direct_duration_seconds=direct_manufacturing_time_seconds,
                        job_cost=manufacturing_job_cost.get("total_job_cost"),
                        total_job_cost=(total_job_cost if priced_job_count > 0 else None),
                        blueprint_source_kind=blueprint_source_kind,
                        children=[
                            *prerequisite_tree_nodes,
                            self._job_tree_node(
                                label=self._ACTIVITY_LABELS["materials"],
                                node_type="materials",
                                activity="materials",
                                children=top_level_material_nodes,
                            ),
                        ],
                    )
                    product_tree = self._job_tree_node(
                        label=str(compact_product.get("type_name") or compact_product.get("type_id") or "Product"),
                        node_type="product",
                        activity="product",
                        type_id=product_type_id,
                        quantity=int(compact_product.get("quantity") or 0),
                        runs=effective_runs,
                        duration_seconds=total_time_seconds,
                        job_cost=(total_job_cost if priced_job_count > 0 else None),
                        total_job_cost=(total_job_cost if priced_job_count > 0 else None),
                        material_cost=None,
                        total_cost=None,
                        children=[manufacturing_tree],
                    )
                    product_market_pricing = product_sell_price_map.get(product_type_id) or {}

                    product_rows.append(
                        {
                            **compact_product,
                            "overview_row_id": (
                                f"product:{row_index}:{product_index}:bpc:{blueprint_copy_variant_key}:bpo:{blueprint_original_item_id or 'none'}"
                            ),
                            "manufacturing_job": {
                                "materials": keyed_entries(adjusted_material_entries, compactor=lambda entry: entry),
                                "skills": manufacturing_skills,
                                "character_modifiers": selected_character_modifiers,
                                "time_seconds": total_time_seconds,
                                "manufacturing_time_seconds": direct_manufacturing_time_seconds,
                                "preparation_time_seconds": max(0, total_time_seconds - direct_manufacturing_time_seconds),
                                "max_production_limit": max_production_limit,
                                "runs": effective_runs,
                                "product_quantity_per_run": product_quantity_per_run,
                                "material_reduction": manufacturing_material_reduction,
                                "time_reduction": manufacturing_time_reduction,
                                "cost_reduction": manufacturing_cost_reduction,
                                "job_cost": manufacturing_job_cost.get("total_job_cost"),
                                "manufacturing_job_cost": manufacturing_job_cost.get("total_job_cost"),
                                "total_job_cost": (total_job_cost if priced_job_count > 0 else None),
                                "estimated_item_value": base_material_eiv_total,
                                "estimated_item_value_source": "materials_adjusted_price",
                                "product_estimated_item_value": product_eiv_total,
                                "product_estimated_item_value_source": product_eiv_source,
                                "product_market_price": self._as_float(product_market_pricing.get("unit_price")),
                                "product_market_price_source": product_market_pricing.get("price_source"),
                                "product_market_price_side": product_market_pricing.get("side"),
                                "product_market_price_hub": product_market_pricing.get("hub"),
                                "product_market_price_hub_label": product_market_pricing.get("hub_label"),
                                "product_market_price_cached": product_market_pricing.get("cached"),
                                "product_market_price_fetched_at": product_market_pricing.get("fetched_at"),
                                "product_market_price_sample_size": product_market_pricing.get("sample_size"),
                                "product_market_volume_total": product_market_pricing.get("volume_total"),
                                "activity_breakdown": activity_breakdown,
                                "recursive_activity_breakdown": recursive_prerequisite_plan.get("activity_breakdown") or {},
                                "procurement_materials": keyed_entries(
                                    top_level_procurement_materials,
                                    compactor=lambda entry: entry,
                                ),
                                "activity_cost_indices": {
                                    "manufacturing": manufacturing_cost_index,
                                    "copying": self._system_cost_index(
                                        profile_payload=selected_industry_profile,
                                        activity="copying",
                                    ),
                                    "research_material": self._system_cost_index(
                                        profile_payload=selected_industry_profile,
                                        activity="research_material",
                                    ),
                                    "research_time": self._system_cost_index(
                                        profile_payload=selected_industry_profile,
                                        activity="research_time",
                                    ),
                                    "invention": self._system_cost_index(
                                        profile_payload=selected_industry_profile,
                                        activity="invention",
                                    ),
                                    "reaction": self._system_cost_index(
                                        profile_payload=selected_industry_profile,
                                        activity="reaction",
                                    ),
                                },
                                "blueprint_material_efficiency": blueprint_material_efficiency,
                                "blueprint_time_efficiency": blueprint_time_efficiency,
                                "blueprint_source_kind": blueprint_source_kind,
                                "industry_profile": selected_industry_profile,
                                "blueprint_sde": blueprint_sde_payload,
                                "blueprint_copy": blueprint_copy_payload,
                                "blueprint_original": blueprint_original_payload,
                                "job_tree": product_tree,
                            },
                            "market_unit_price": self._as_float(product_market_pricing.get("unit_price")),
                            "market_price_source": product_market_pricing.get("price_source"),
                            "market_price_side": product_market_pricing.get("side"),
                            "market_hub": product_market_pricing.get("hub"),
                            "market_hub_label": product_market_pricing.get("hub_label"),
                            "market_price_cached": product_market_pricing.get("cached"),
                            "market_price_fetched_at": product_market_pricing.get("fetched_at"),
                            "market_price_sample_size": product_market_pricing.get("sample_size"),
                            "market_volume_total": product_market_pricing.get("volume_total"),
                        }
                    )

        product_rows = [
            row for row in product_rows if isinstance(row, dict) and not self._exclude_from_product_overview(row)
        ]

        if progress_callback is not None:
            progress_callback(
                0.5,
                "Step 5/9: Built manufacturing product rows",
                {"step": 5, "step_count": 9, "stage": "rows", "rows": len(product_rows)},
            )

        product_rows = self._enrich_product_rows_with_material_prices(
            product_rows,
            market_hub=normalized_market_hub,
            material_price_side=normalized_material_price_side,
            progress_callback=progress_callback,
        )
        product_rows = self._enrich_product_rows_with_market_activity(
            product_rows,
            market_hub=normalized_market_hub,
        )
        if progress_callback is not None:
            progress_callback(
                0.95,
                "Step 8/9: Calculating sale proceeds and profit metrics",
                {"step": 8, "step_count": 9, "stage": "profit"},
            )
        product_rows = self._enrich_product_rows_with_sale_proceeds(
            product_rows,
            character_id=character_id,
            market_hub=normalized_market_hub,
            product_price_side=normalized_product_price_side,
        )
        product_rows = self._enrich_product_rows_with_profit_metrics(product_rows)
        if progress_callback is not None:
            progress_callback(
                0.98,
                "Step 9/9: Scoring pricing confidence and finalizing payload",
                {"step": 9, "step_count": 9, "stage": "finalize"},
            )
        product_rows = self._enrich_product_rows_with_pricing_confidence(
            product_rows,
            product_price_side=normalized_product_price_side,
        )

        product_rows.sort(
            key=lambda row: (
                str(row.get("type_name") or "").lower(),
                str(row.get("overview_row_id") or ""),
            )
        )
        if progress_callback is not None:
            progress_callback(1.0, "Step 9/9: Product overview ready", {"step": 9, "step_count": 9, "stage": "completed", "rows": len(product_rows)})
        return product_rows

    def _enrich_product_rows_with_material_prices(
        self,
        product_rows: list[dict[str, Any]],
        *,
        market_hub: str = "jita",
        material_price_side: str = "sell",
        progress_callback: ProgressCallback | None = None,
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        material_type_ids = sorted(
            {
                int(material.get("type_id") or 0)
                for row in product_rows
                for material in list(
                    ((((row.get("manufacturing_job") or {}).get("procurement_materials") or {}) or ((row.get("manufacturing_job") or {}).get("materials") or {})).values())
                )
                if isinstance(material, dict) and int(material.get("type_id") or 0) > 0
            }
        )
        if progress_callback is not None:
            progress_callback(
                0.55,
                "Collected unique manufacturing materials",
                {"material_type_count": len(material_type_ids)},
            )
        if not material_type_ids:
            return product_rows

        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        price_by_type_id = pricing_service.get_type_price_map(
            type_ids=material_type_ids,
            hub=market_hub,
            side=material_price_side,
            progress_callback=(
                None
                if progress_callback is None
                else lambda progress_fraction, progress_label, progress_meta=None: progress_callback(
                    0.55 + (0.4 * progress_fraction),
                    progress_label,
                    progress_meta,
                )
            ),
        )

        for row in product_rows:
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue
            materials = manufacturing_job.get("materials") or {}
            if not isinstance(materials, dict):
                continue
            procurement_materials = manufacturing_job.get("procurement_materials") or materials
            if not isinstance(procurement_materials, dict):
                procurement_materials = materials
            material_cost = 0.0
            priced_material_count = 0
            for material in procurement_materials.values():
                if not isinstance(material, dict):
                    continue
                type_id = int(material.get("type_id") or 0)
                explicit_unit_price = self._as_float(material.get("unit_price"))
                if explicit_unit_price is not None and explicit_unit_price > 0:
                    unit_price = explicit_unit_price
                else:
                    pricing = price_by_type_id.get(type_id) or {}
                    unit_price = pricing.get("unit_price")
                    material["unit_price"] = unit_price
                    material["price_source"] = pricing.get("price_source")
                    material["price_volume_total"] = pricing.get("volume_total")
                    material["market_hub"] = pricing.get("hub")
                    material["market_hub_label"] = pricing.get("hub_label")
                    material["price_sample_size"] = pricing.get("sample_size")
                    material["price_cached"] = pricing.get("cached")
                    material["price_fetched_at"] = pricing.get("fetched_at")
                line_total = None
                if unit_price is not None:
                    line_total = float(unit_price) * int(material.get("quantity") or 0)
                    material_cost += float(line_total)
                    priced_material_count += 1
                material["line_total"] = line_total
            manufacturing_job["material_cost"] = material_cost if priced_material_count > 0 else None
            manufacturing_job["priced_material_count"] = priced_material_count
            manufacturing_job["material_type_count"] = len(procurement_materials)
            total_job_cost = manufacturing_job.get("total_job_cost")
            if total_job_cost is not None or priced_material_count > 0:
                manufacturing_job["total_cost"] = float(total_job_cost or 0.0) + float(material_cost or 0.0)
            else:
                manufacturing_job["total_cost"] = None

            job_tree = manufacturing_job.get("job_tree") or {}
            if isinstance(job_tree, dict) and job_tree:
                self._apply_material_pricing_to_job_tree(job_tree, price_by_type_id=price_by_type_id)

        if progress_callback is not None:
            progress_callback(0.97, "Applied material prices to manufacturing rows", {"rows": len(product_rows)})

        return product_rows

    def industry_job_manager_status(self) -> dict:
        mgr = self._ensure_industry_job_manager()
        return mgr.get_status()

    def industry_system_cost_index(self, *, system_id: int) -> dict:
        if not system_id:
            raise ServiceError("System ID is required.", status_code=400)

        if self._state.esi_service is None:
            raise ServiceError("ESI service not initialized.", status_code=503)

        systems = self._state.esi_service.get_industry_systems()
        row = next((s for s in systems if s.get("solar_system_id") == system_id), None)
        if not row:
            return {"solar_system_id": system_id, "cost_indices": []}
        return row

    def industry_facility(self, *, facility_id: int) -> dict:
        if not facility_id:
            raise ServiceError("Facility ID is required.", status_code=400)

        if self._state.esi_service is None:
            raise ServiceError("ESI service not initialized.", status_code=503)

        facilities = self._state.esi_service.get_industry_facilities()
        row = next((f for f in facilities if f.get("facility_id") == facility_id), None)
        if not row:
            return {"facility_id": facility_id, "tax": None}
        out: dict[str, Any] = dict(row) if isinstance(row, dict) else {"facility_id": facility_id}
        if "tax" not in out:
            out["tax"] = None
        return out

    @staticmethod
    def _camel_to_words(s: str) -> str:
        out: list[str] = []
        buf = ""
        for ch in s:
            if buf and ch.isupper() and (not buf[-1].isupper()):
                out.append(buf)
                buf = ch
            else:
                buf += ch
        if buf:
            out.append(buf)
        return " ".join(out).strip()

    @classmethod
    def _parse_rig_effect(cls, effect_name: str) -> tuple[str | None, str | None, str | None]:
        if not effect_name or not effect_name.startswith("rig"):
            return None, None, None

        metric: str | None
        if ("Material" in effect_name) or ("MatBonus" in effect_name):
            metric = "material"
        elif "Time" in effect_name:
            metric = "time"
        elif "Cost" in effect_name:
            metric = "cost"
        else:
            metric = None

        activity: str | None = None
        group_token: str | None = None

        rest = effect_name[3:]

        if rest.startswith("ME") and "Research" in rest:
            return "research_me", None, metric
        if rest.startswith("TE") and "Research" in rest:
            return "research_te", None, metric

        if rest.startswith("Reaction"):
            group_part = rest
            for suffix in ["MatBonus", "TimeBonus", "CostBonus", "MaterialBonus", "TimeBonus", "CostBonus"]:
                if suffix in group_part:
                    group_part = group_part.split(suffix, 1)[0]
                    break
            return "manufacturing", group_part or None, metric

        for token, activity_key in [
            ("Manufacture", "manufacturing"),
            ("Invention", "invention"),
            ("Research", "research"),
            ("Copying", "copying"),
            ("Copy", "copying"),
            ("Reaction", "manufacturing"),
        ]:
            idx = rest.find(token)
            if idx >= 0:
                activity = activity_key
                if idx > 0:
                    group_token = rest[:idx]
                break

        return activity, group_token, metric

    def structure_rigs(self) -> list[dict]:
        cache = getattr(self._state, "_structure_rigs_cache", None)
        now = time.time()
        if (
            cache
            and isinstance(cache, tuple)
            and len(cache) == 3
            and cache[1] == self._STRUCTURE_RIGS_CACHE_VERSION
            and (now - cache[0] < self._STRUCTURE_RIG_MFG_CACHE_TTL_SECONDS)
        ):
            return cache[2]

        session: Any = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"

        rows = session.execute(
            text(
                """
                SELECT id, name
                FROM types
                WHERE published = 1
                  AND name LIKE '%Standup%'
                  AND (
                        name LIKE '%Manufacturing%'
                     OR name LIKE '%Research%'
                     OR name LIKE '%Invention%'
                  )
                  AND name NOT LIKE '%Blueprint%'
                ORDER BY id ASC
                """
            )
        ).fetchall()

        type_ids = [int(r[0]) for r in rows]

        dogma_by_type_id: dict[int, dict] = {}
        if type_ids:
            dogma_rows = session.execute(
                text("SELECT id, dogmaAttributes, dogmaEffects FROM typeDogma WHERE id IN :ids").bindparams(
                    bindparam("ids", expanding=True)
                ),
                {"ids": type_ids},
            ).fetchall()

            for tid, attrs_raw, effects_raw in dogma_rows:
                attr_map: dict[int, float] = {}
                effect_ids: list[int] = []
                try:
                    attrs = json.loads(attrs_raw) if isinstance(attrs_raw, str) else (attrs_raw or [])
                    for a in attrs or []:
                        if not isinstance(a, dict):
                            continue
                        aid = a.get("attributeID")
                        val = a.get("value")
                        if aid is None or val is None:
                            continue
                        attr_map[int(aid)] = float(val)
                except Exception:
                    attr_map = {}

                try:
                    effs = json.loads(effects_raw) if isinstance(effects_raw, str) else (effects_raw or [])
                    for e in effs or []:
                        if not isinstance(e, dict):
                            continue
                        eid = e.get("effectID")
                        if eid is None:
                            continue
                        effect_ids.append(int(eid))
                except Exception:
                    effect_ids = []

                dogma_by_type_id[int(tid)] = {"attr_map": attr_map, "effect_ids": effect_ids}

        all_effect_ids = sorted(
            {eid for d in dogma_by_type_id.values() for eid in (d.get("effect_ids") or []) if eid is not None}
        )
        effect_name_by_id: dict[int, str] = {}
        if all_effect_ids:
            eff_rows = session.execute(
                text("SELECT id, name FROM dogmaEffects WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
                {"ids": all_effect_ids},
            ).fetchall()
            effect_name_by_id = {int(r[0]): (r[1] or "") for r in eff_rows}

        rigs_out: list[dict] = []
        for type_id, name_json in rows:
            try:
                name_obj = json.loads(name_json) if isinstance(name_json, str) else {}
                rig_name = name_obj.get(language) or name_obj.get("en") or str(type_id)
            except Exception:
                rig_name = str(type_id)

            d = dogma_by_type_id.get(int(type_id)) or {}
            attr_map = d.get("attr_map") or {}

            time_val = attr_map.get(self._RIG_ATTR_TIME_REDUCTION, 0.0)
            mat_val = attr_map.get(self._RIG_ATTR_MATERIAL_REDUCTION, 0.0)
            cost_val = attr_map.get(self._RIG_ATTR_COST_REDUCTION, 0.0)

            time_reduction = max(0.0, (-float(time_val)) / 100.0)
            material_reduction = max(0.0, (-float(mat_val)) / 100.0)
            cost_reduction = max(0.0, (-float(cost_val)) / 100.0)

            effects_out: list[dict] = []
            for eid in d.get("effect_ids") or []:
                ename = effect_name_by_id.get(int(eid)) or ""
                if ename in {"rigSlot"}:
                    continue
                if not ename.startswith("rig"):
                    continue
                if "Bonus" not in ename:
                    continue

                activity, group_token, metric = self._parse_rig_effect(ename)
                if activity is None or metric is None:
                    continue

                if metric == "material":
                    val = material_reduction
                elif metric == "time":
                    val = time_reduction
                else:
                    val = cost_reduction

                if val <= 0:
                    continue

                if not group_token:
                    group = "All"
                else:
                    group = self._RIG_GROUP_TOKEN_LABELS.get(group_token) or self._camel_to_words(group_token)

                effects_out.append(
                    {
                        "effect_id": int(eid),
                        "effect_name": ename,
                        "activity": activity,
                        "group": group,
                        "metric": metric,
                        "value": float(val),
                    }
                )

            rigs_out.append(
                {
                    "type_id": int(type_id),
                    "name": str(rig_name),
                    "time_reduction": time_reduction,
                    "material_reduction": material_reduction,
                    "cost_reduction": cost_reduction,
                    "effects": effects_out,
                }
            )

        rigs_out.sort(key=lambda r: r["name"])
        self._state._structure_rigs_cache = (now, self._STRUCTURE_RIGS_CACHE_VERSION, rigs_out)
        return rigs_out

    def create_industry_profile(self, *, data: dict) -> int:
        character_id = data.get("character_id")
        if not character_id:
            raise ServiceError("Character ID is required to create an industry profile.", status_code=400)

        session: Any = self._sessions.app_session()
        return industry_profile_create(session, data)

    def update_industry_profile(self, *, profile_id: int, data: dict) -> None:
        session: Any = self._sessions.app_session()
        industry_profile_update(session, profile_id, data)

    def delete_industry_profile(self, *, profile_id: int) -> None:
        session: Any = self._sessions.app_session()
        industry_profile_delete(session, profile_id)
