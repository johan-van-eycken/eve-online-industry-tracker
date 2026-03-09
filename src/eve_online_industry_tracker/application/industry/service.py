from __future__ import annotations

import json
import time
from typing import Any

from sqlalchemy import bindparam, text
from classes.database_models import Blueprints
from eve_online_industry_tracker.db_models import CharacterAssetsModel, CorporationAssetsModel

from eve_online_industry_tracker.application.errors import ServiceError
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
from eve_online_industry_tracker.infrastructure.sde.blueprints import get_blueprint_manufacturing_data


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

    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)

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
