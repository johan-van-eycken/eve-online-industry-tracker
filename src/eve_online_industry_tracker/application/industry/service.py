from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import threading
import time
from typing import Any, Callable, cast
import uuid

from sqlalchemy import bindparam, text
from eve_online_industry_tracker.db_models import CharacterAssetsModel, CorporationAssetsModel

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

    def _update_overview_refresh_job(
        self,
        job_id: str,
        *,
        status: str | None = None,
        progress_fraction: float | None = None,
        progress_label: str | None = None,
        result: list[dict[str, Any]] | None = None,
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
            if error_message is not None:
                job["error_message"] = str(error_message)
            job["updated_at"] = datetime.now(timezone.utc).isoformat()
            return dict(job)

    def start_industry_manufacturing_product_overview_refresh(
        self,
        *,
        force_refresh: bool = False,
        maximize_bp_runs: bool = False,
        build_from_bpc: bool = True,
        have_blueprint_source_only: bool = True,
        include_reactions: bool = False,
        industry_profile_id: int | None = None,
        owned_blueprints_scope: str = "all_characters",
        character_id: int | None = None,
    ) -> dict[str, Any]:
        job_id = str(uuid.uuid4())
        store = self._get_industry_overview_refresh_store()
        created_at = datetime.now(timezone.utc).isoformat()
        with store.lock:
            store.jobs[job_id] = {
                "job_id": job_id,
                "status": "queued",
                "progress_fraction": 0.0,
                "progress_label": "Queued",
                "progress_meta": {},
                "created_at": created_at,
                "updated_at": created_at,
                "result": None,
                "result_count": 0,
                "error_message": None,
            }

        params = {
            "force_refresh": bool(force_refresh),
            "maximize_bp_runs": bool(maximize_bp_runs),
            "build_from_bpc": bool(build_from_bpc),
            "have_blueprint_source_only": bool(have_blueprint_source_only),
            "include_reactions": bool(include_reactions),
            "industry_profile_id": int(industry_profile_id) if industry_profile_id is not None else None,
            "owned_blueprints_scope": str(owned_blueprints_scope),
            "character_id": int(character_id) if character_id is not None else None,
        }

        thread = threading.Thread(
            target=self._run_overview_refresh_job,
            args=(job_id, params),
            daemon=True,
            name=f"industry-overview-refresh-{job_id[:8]}",
        )
        register_thread(self._state, thread.name, thread)
        thread.start()
        return {"job_id": job_id}

    def _run_overview_refresh_job(self, job_id: str, params: dict[str, Any]) -> None:
        self._update_overview_refresh_job(
            job_id,
            status="running",
            progress_fraction=0.01,
            progress_label="Starting overview refresh",
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

            result = self.industry_manufacturing_product_overview(
                force_refresh=bool(params.get("force_refresh", False)),
                maximize_bp_runs=bool(params.get("maximize_bp_runs", False)),
                build_from_bpc=bool(params.get("build_from_bpc", True)),
                have_blueprint_source_only=bool(params.get("have_blueprint_source_only", True)),
                include_reactions=bool(params.get("include_reactions", False)),
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
                result=result,
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
            existing["quantity"] = int(existing.get("quantity") or 0) + int(entry.get("quantity") or 0)
            if entry.get("quantity_per_run") is not None or existing.get("quantity_per_run") is not None:
                existing["quantity_per_run"] = int(existing.get("quantity_per_run") or 0) + int(entry.get("quantity_per_run") or 0)
            if entry.get("base_quantity") is not None or existing.get("base_quantity") is not None:
                existing["base_quantity"] = int(existing.get("base_quantity") or 0) + int(entry.get("base_quantity") or 0)
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
        blueprint_copy_assets_by_type_id: dict[int, list[Any]],
        blueprint_original_assets_by_type_id: dict[int, list[Any]],
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
        runs = max(1, int(math.ceil(float(required_quantity) / float(product_quantity_per_run))))

        blueprint_type_id = int(blueprint_row.get("blueprint_type_id") or 0)
        blueprint_material_efficiency = 0
        blueprint_time_efficiency = 0
        blueprint_source_kind = activity
        include_copying_job = False
        include_sde_research_chain = False
        matched_blueprint_copies = blueprint_copy_assets_by_type_id.get(blueprint_type_id) or []
        matched_blueprint_originals = blueprint_original_assets_by_type_id.get(blueprint_type_id) or []
        if activity == "manufacturing":
            if bool(build_from_bpc):
                if matched_blueprint_copies:
                    blueprint_material_efficiency = int(matched_blueprint_copies[0].blueprint_material_efficiency or 0)
                    blueprint_time_efficiency = int(matched_blueprint_copies[0].blueprint_time_efficiency or 0)
                    blueprint_source_kind = "owned_blueprint_copy"
                elif matched_blueprint_originals:
                    blueprint_material_efficiency = int(matched_blueprint_originals[0].blueprint_material_efficiency or 0)
                    blueprint_time_efficiency = int(matched_blueprint_originals[0].blueprint_time_efficiency or 0)
                    blueprint_source_kind = "copied_from_owned_blueprint_original"
                    include_copying_job = True
                else:
                    blueprint_source_kind = "unowned_blueprint_copy"
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

        if include_copying_job:
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
            base_copy_time_seconds = int(((blueprint_row.get("copying_job") or {}).get("time_seconds") or 0)) * runs
            copy_process_value = (
                float(per_run_material_eiv or 0.0) * runs * 0.02
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
                "runs": runs,
            }
            prerequisite_nodes.append(
                self._job_tree_node(
                    label=self._ACTIVITY_LABELS["copying"],
                    node_type="activity",
                    activity="copying",
                    runs=runs,
                    quantity=None,
                    duration_seconds=copying_duration_seconds,
                    direct_duration_seconds=copying_duration_seconds,
                    job_cost=copying_job_cost.get("total_job_cost"),
                    total_job_cost=copying_job_cost.get("total_job_cost"),
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
                    required_quantity=child_quantity,
                    build_from_bpc=build_from_bpc,
                    include_reactions=include_reactions,
                    selected_industry_profile=selected_industry_profile,
                    selected_character_modifiers=selected_character_modifiers,
                    character_skill_levels_by_name=character_skill_levels_by_name,
                    adjusted_market_price_map=adjusted_market_price_map,
                    blueprint_copy_assets_by_type_id=blueprint_copy_assets_by_type_id,
                    blueprint_original_assets_by_type_id=blueprint_original_assets_by_type_id,
                    manufacturing_row_by_product_type_id=manufacturing_row_by_product_type_id,
                    reaction_row_by_product_type_id=reaction_row_by_product_type_id,
                    invention_row_by_blueprint_type_id=invention_row_by_blueprint_type_id,
                    visited=next_visited,
                    depth=depth + 1,
                )
            if child_plan:
                total_time_seconds += int(child_plan.get("total_time_seconds") or 0)
                if child_plan.get("total_job_cost") is not None:
                    total_job_cost += float(child_plan.get("total_job_cost") or 0.0)
                    priced_job_count += int(child_plan.get("priced_job_count") or 0)
                leaf_materials.extend([dict(entry) for entry in (child_plan.get("leaf_materials") or []) if isinstance(entry, dict)])
                recursive_activity_breakdown[f"{child_activity}:{child_type_id}"] = {
                    "activity": child_activity,
                    "type_id": child_type_id,
                    "type_name": material.get("type_name"),
                    "quantity": child_quantity,
                    "time_seconds": child_plan.get("total_time_seconds"),
                    "job_cost": child_plan.get("total_job_cost"),
                    "direct_job_cost": child_plan.get("direct_job_cost"),
                }
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or child_type_id),
                        node_type="material",
                        activity="material",
                        type_id=child_type_id,
                        quantity=child_quantity,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=child_plan.get("total_job_cost"),
                        children=[child_plan.get("tree_node")],
                    )
                )
            else:
                leaf_materials.append(dict(material))
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or child_type_id),
                        node_type="material",
                        activity="material",
                        type_id=child_type_id,
                        quantity=child_quantity,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=None,
                        children=[],
                    )
                )

        if activity == "manufacturing" and not matched_blueprint_copies and not matched_blueprint_originals:
            invention_source_row = invention_row_by_blueprint_type_id.get(blueprint_type_id)
            invention_job = (invention_source_row or {}).get("invention_job") or {}
            if invention_source_row and isinstance(invention_job, dict):
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
                invention_attempts = max(1, int(math.ceil(1.0 / max(probability, 0.01))))
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
                    "time_seconds": invention_duration_seconds,
                    "job_cost": invention_job_cost.get("total_job_cost"),
                }
                leaf_materials.extend(
                    [{**entry, "quantity": int(entry.get("quantity") or 0) * invention_attempts} for entry in invention_materials]
                )
                prerequisite_nodes.append(
                    self._job_tree_node(
                        label=self._ACTIVITY_LABELS["invention"],
                        node_type="activity",
                        activity="invention",
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
                    prerequisite_nodes.append(
                        self._job_tree_node(
                            label=self._ACTIVITY_LABELS["copying"],
                            node_type="activity",
                            activity="copying",
                            runs=invention_attempts,
                            duration_seconds=source_copy_duration_seconds,
                            direct_duration_seconds=source_copy_duration_seconds,
                            job_cost=source_copy_job_cost.get("total_job_cost"),
                            total_job_cost=source_copy_job_cost.get("total_job_cost"),
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
            quantity=required_quantity,
            runs=runs,
            duration_seconds=total_time_seconds,
            direct_duration_seconds=direct_duration_seconds,
            job_cost=job_cost.get("total_job_cost"),
            total_job_cost=(total_job_cost if priced_job_count > 0 else None),
            blueprint_source_kind=blueprint_source_kind,
            children=[*prerequisite_nodes, materials_container_node],
        )

        return {
            "activity": activity,
            "blueprint_type_id": blueprint_type_id,
            "type_id": int(selected_product.get("type_id") or desired_product_type_id),
            "required_quantity": required_quantity,
            "runs": runs,
            "direct_time_seconds": direct_duration_seconds,
            "total_time_seconds": total_time_seconds,
            "direct_job_cost": job_cost.get("total_job_cost"),
            "total_job_cost": (total_job_cost if priced_job_count > 0 else None),
            "priced_job_count": priced_job_count,
            "estimated_item_value": process_value,
            "estimated_item_value_priced_material_count": priced_material_count,
            "leaf_materials": self._aggregate_material_entries(leaf_materials),
            "recursive_activity_breakdown": recursive_activity_breakdown,
            "blueprint_source_kind": blueprint_source_kind,
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
        blueprint_copy_assets_by_type_id: dict[int, list[Any]],
        blueprint_original_assets_by_type_id: dict[int, list[Any]],
        manufacturing_row_by_product_type_id: dict[int, dict[str, Any]],
        reaction_row_by_product_type_id: dict[int, dict[str, Any]],
        invention_row_by_blueprint_type_id: dict[int, dict[str, Any]],
    ) -> dict[str, Any]:
        reactions_enabled = bool(include_reactions) and self._reactions_allowed_for_profile(selected_industry_profile)
        total_time_seconds = 0
        total_job_cost = 0.0
        priced_job_count = 0
        procurement_materials: list[dict[str, Any]] = []
        recursive_activity_breakdown: dict[str, Any] = {}
        tree_children: list[dict[str, Any]] = []
        material_nodes: list[dict[str, Any]] = []

        matched_blueprint_copies = blueprint_copy_assets_by_type_id.get(int(blueprint_type_id)) or []
        matched_blueprint_originals = blueprint_original_assets_by_type_id.get(int(blueprint_type_id)) or []
        if not matched_blueprint_copies and not matched_blueprint_originals:
            invention_source_row = invention_row_by_blueprint_type_id.get(int(blueprint_type_id))
            invention_job = (invention_source_row or {}).get("invention_job") or {}
            if invention_source_row and isinstance(invention_job, dict):
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
                total_time_seconds += invention_duration_seconds
                if invention_job_cost.get("total_job_cost") is not None:
                    total_job_cost += float(invention_job_cost.get("total_job_cost") or 0.0)
                    priced_job_count += 1
                procurement_materials.extend(invention_materials)
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
                                children=[
                                    self._job_tree_node(
                                        label=str(entry.get("type_name") or entry.get("type_id") or "Material"),
                                        node_type="material",
                                        activity="material",
                                        type_id=int(entry.get("type_id") or 0),
                                        quantity=int(entry.get("quantity") or 0),
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
            child_row = None
            child_activity = "manufacturing"
            if reactions_enabled and material_type_id in reaction_row_by_product_type_id:
                child_row = reaction_row_by_product_type_id.get(material_type_id)
                child_activity = "reaction"
            elif material_type_id in manufacturing_row_by_product_type_id:
                child_row = manufacturing_row_by_product_type_id.get(material_type_id)
            if child_row is None:
                procurement_materials.append(dict(material))
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or material_type_id),
                        node_type="material",
                        activity="material",
                        type_id=material_type_id,
                        quantity=material_quantity,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=None,
                        children=[],
                    )
                )
                continue

            child_plan = self._plan_blueprint_chain_for_quantity(
                blueprint_row=child_row,
                activity=child_activity,
                desired_product_type_id=material_type_id,
                required_quantity=material_quantity,
                build_from_bpc=build_from_bpc,
                include_reactions=reactions_enabled,
                selected_industry_profile=selected_industry_profile,
                selected_character_modifiers=selected_character_modifiers,
                character_skill_levels_by_name=character_skill_levels_by_name,
                adjusted_market_price_map=adjusted_market_price_map,
                blueprint_copy_assets_by_type_id=blueprint_copy_assets_by_type_id,
                blueprint_original_assets_by_type_id=blueprint_original_assets_by_type_id,
                manufacturing_row_by_product_type_id=manufacturing_row_by_product_type_id,
                reaction_row_by_product_type_id=reaction_row_by_product_type_id,
                invention_row_by_blueprint_type_id=invention_row_by_blueprint_type_id,
                visited={("manufacturing", blueprint_type_id)},
                depth=1,
            )
            if child_plan is None:
                procurement_materials.append(dict(material))
                material_nodes.append(
                    self._job_tree_node(
                        label=str(material.get("type_name") or material_type_id),
                        node_type="material",
                        activity="material",
                        type_id=material_type_id,
                        quantity=material_quantity,
                        runs=None,
                        duration_seconds=None,
                        job_cost=None,
                        total_job_cost=None,
                        children=[],
                    )
                )
                continue
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
                "quantity": material_quantity,
                "time_seconds": child_plan.get("total_time_seconds"),
                "job_cost": child_plan.get("total_job_cost"),
                "nested": child_plan.get("recursive_activity_breakdown") or {},
            }
            material_nodes.append(
                self._job_tree_node(
                    label=str(material.get("type_name") or material_type_id),
                    node_type="material",
                    activity="material",
                    type_id=material_type_id,
                    quantity=material_quantity,
                    runs=None,
                    duration_seconds=None,
                    job_cost=None,
                    total_job_cost=child_plan.get("total_job_cost"),
                    children=[child_plan.get("tree_node")],
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
                        location_info = self._state.esi_service.get_location_info(int(top_location_id))
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
                top_location_name_by_id = resolve_top_location_name_map([*character_assets, *corporation_assets])
                return (
                    character_assets,
                    corporation_assets,
                    character_name_by_id,
                    corporation_name_by_id,
                    top_location_name_by_id,
                )
            character_assets = blueprints_repo.get_character_blueprints(session)
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

    def industry_manufacturing_product_overview(
        self,
        *,
        force_refresh: bool = False,
        maximize_bp_runs: bool = False,
        build_from_bpc: bool = True,
        have_blueprint_source_only: bool = True,
        include_reactions: bool = False,
        industry_profile_id: int | None = None,
        owned_blueprints_scope: str = "all_characters",
        character_id: int | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> list[dict[str, Any]]:
        mgr = self._ensure_industry_job_manager()
        if progress_callback is not None:
            progress_callback(0.05, "Loading blueprint snapshot", None)
        blueprint_rows = mgr.get_blueprint_overview(force_refresh=bool(force_refresh))
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
        blueprint_copy_assets_by_type_id: dict[int, list[CharacterAssetsModel | CorporationAssetsModel]] = {}
        blueprint_original_assets_by_type_id: dict[int, list[CharacterAssetsModel | CorporationAssetsModel]] = {}
        (
            character_blueprint_assets,
            corporation_blueprint_assets,
            character_name_by_id,
            corporation_name_by_id,
            top_location_name_by_id,
        ) = self._get_owned_blueprint_assets(owned_blueprints_scope=owned_blueprints_scope)
        if progress_callback is not None:
            progress_callback(
                0.15,
                "Resolved owned blueprint assets",
                {
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
                    selected_blueprint_variants: list[tuple[CharacterAssetsModel | None, CharacterAssetsModel | None]] = [
                        (blueprint_copy_asset, None) for blueprint_copy_asset in matched_blueprint_copies
                    ]
                elif matched_blueprint_originals:
                    selected_blueprint_variants = [
                        (None, blueprint_original_asset) for blueprint_original_asset in matched_blueprint_originals
                    ]
                else:
                    selected_blueprint_variants = [(None, None)]

                if bool(have_blueprint_source_only):
                    selected_blueprint_variants = [
                        (blueprint_copy_asset, blueprint_original_asset)
                        for blueprint_copy_asset, blueprint_original_asset in selected_blueprint_variants
                        if blueprint_copy_asset is not None or blueprint_original_asset is not None
                    ]
                    if not selected_blueprint_variants:
                        continue

                for blueprint_copy_asset, blueprint_original_asset in selected_blueprint_variants:
                    blueprint_copy_runs = (
                        int(blueprint_copy_asset.blueprint_runs or 0)
                        if blueprint_copy_asset is not None
                        else 0
                    )
                    effective_runs = 1
                    if bool(maximize_bp_runs):
                        if blueprint_copy_runs > 0:
                            effective_runs = blueprint_copy_runs
                        elif max_production_limit > 0:
                            effective_runs = max_production_limit

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

                    if include_copying_job:
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
                        base_copy_time_seconds = int(((row.get("copying_job") or {}).get("time_seconds") or 0)) * effective_runs
                        copy_process_value = (
                            float(per_run_material_eiv or 0.0) * effective_runs * 0.02
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
                            "runs": effective_runs,
                            **copying_job_cost,
                        }
                        total_time_seconds += copying_duration_seconds
                        if copying_job_cost.get("total_job_cost") is not None:
                            total_job_cost += float(copying_job_cost.get("total_job_cost") or 0.0)
                            priced_job_count += 1

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

                    blueprint_copy_payload = self._compact_owned_blueprint_asset(
                        blueprint_copy_asset,
                        character_name_by_id=character_name_by_id,
                        corporation_name_by_id=corporation_name_by_id,
                        top_location_name_by_id=top_location_name_by_id,
                    )
                    blueprint_copy_item_id = int(blueprint_copy_asset.item_id) if blueprint_copy_asset is not None else None
                    blueprint_original_payload = self._compact_owned_blueprint_asset(
                        blueprint_original_asset,
                        character_name_by_id=character_name_by_id,
                        corporation_name_by_id=corporation_name_by_id,
                        top_location_name_by_id=top_location_name_by_id,
                    )
                    blueprint_original_item_id = (
                        int(blueprint_original_asset.item_id) if blueprint_original_asset is not None else None
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
                        blueprint_copy_assets_by_type_id=blueprint_copy_assets_by_type_id,
                        blueprint_original_assets_by_type_id=blueprint_original_assets_by_type_id,
                        manufacturing_row_by_product_type_id=manufacturing_row_by_product_type_id,
                        reaction_row_by_product_type_id=reaction_row_by_product_type_id,
                        invention_row_by_blueprint_type_id=invention_row_by_blueprint_type_id,
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

                    prerequisite_tree_nodes: list[dict[str, Any]] = []
                    for activity_name in ["copying", "research_material", "research_time"]:
                        activity_payload = activity_breakdown.get(activity_name) or {}
                        if not isinstance(activity_payload, dict) or not activity_payload:
                            continue
                        prerequisite_tree_nodes.append(
                            self._job_tree_node(
                                label=self._ACTIVITY_LABELS.get(activity_name, activity_name),
                                node_type="activity",
                                activity=activity_name,
                                runs=activity_payload.get("runs"),
                                duration_seconds=activity_payload.get("duration_seconds"),
                                direct_duration_seconds=activity_payload.get("duration_seconds"),
                                job_cost=activity_payload.get("total_job_cost"),
                                total_job_cost=activity_payload.get("total_job_cost"),
                                children=[],
                            )
                        )
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

                    product_rows.append(
                        {
                            **compact_product,
                            "overview_row_id": (
                                f"product:{row_index}:{product_index}:bpc:{blueprint_copy_item_id or 'none'}:bpo:{blueprint_original_item_id or 'none'}"
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
                                "activity_breakdown": activity_breakdown,
                                "recursive_activity_breakdown": recursive_prerequisite_plan.get("activity_breakdown") or {},
                                "procurement_materials": keyed_entries(
                                    cast(list[dict[str, Any]], recursive_prerequisite_plan.get("procurement_materials") or adjusted_material_entries),
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
                        }
                    )

        if progress_callback is not None:
            progress_callback(
                0.5,
                "Built manufacturing product rows",
                {"rows": len(product_rows)},
            )

        product_rows = self._enrich_product_rows_with_material_prices(
            product_rows,
            progress_callback=progress_callback,
        )

        product_rows.sort(
            key=lambda row: (
                str(row.get("type_name") or "").lower(),
                str(row.get("overview_row_id") or ""),
            )
        )
        if progress_callback is not None:
            progress_callback(1.0, "Product overview ready", {"rows": len(product_rows)})
        return product_rows

    def _enrich_product_rows_with_material_prices(
        self,
        product_rows: list[dict[str, Any]],
        *,
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
        price_by_type_id = pricing_service.get_material_sell_price_map(
            material_type_ids=material_type_ids,
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
                pricing = price_by_type_id.get(type_id) or {}
                unit_price = pricing.get("unit_price")
                material["unit_price"] = unit_price
                material["price_source"] = pricing.get("price_source")
                material["price_sample_size"] = pricing.get("sample_size")
                material["price_cached"] = pricing.get("cached")
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
