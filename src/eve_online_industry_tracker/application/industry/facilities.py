"""IndustryService: industry profiles and facilities: structures, rigs, gates, system cost indices.

A mixin of IndustryService (service.py); it holds no state of its own."""

from __future__ import annotations

import logging
import json
import threading
import time
from collections.abc import Collection
from typing import Any

import requests  # pyright: ignore[reportMissingModuleSource]
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.infrastructure.industry_adapter import (
    corporation_structures_list_by_corporation_id,
    get_cached_public_structures,
    get_npc_stations,
    get_rig_effects_for_type_ids,
    get_service_module_fits,
    get_solar_systems,
    get_type_data,
    get_type_group_id,
    industry_profile_create,
    industry_profile_delete,
    industry_profile_get_by_id,
    industry_profile_get_default_for_character_id,
    industry_profile_list_by_character_id,
    industry_profile_update,
    public_structures_cache_ttl_seconds,
    trigger_refresh_public_structures_for_system,
)
from eve_online_industry_tracker.application.industry.job_cost import JobSurchargeRates, profile_facility_tax
from eve_online_industry_tracker.application.industry.invention_model import warnings_are_quiet
from eve_online_industry_tracker.domain import rig_groups as _rig_groups
from eve_online_industry_tracker.domain import security_band as _security_band
from eve_online_industry_tracker.domain import industry_facility as _industry_facility
from eve_online_industry_tracker.infrastructure.sde import industry as _sde_industry


# The service module's logger: it carries the QuietWarningsFilter (see service.py).
logger = logging.getLogger("eve_online_industry_tracker.application.industry.service")


# ESI failures: esi_client raises RuntimeError after its retries; requests raises its own.
_ESI_ERRORS: tuple[type[Exception], ...] = (RuntimeError, requests.RequestException)


class FacilitiesMixin:
    _STRUCTURE_RIGS_CACHE_VERSION = 4
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
        "reaction": ("reaction",),
        "copying": ("copying",),
        "research_material": ("research_me", "research"),
        "research_time": ("research_te", "research"),
        "invention": ("invention",),
    }

    # Uncertain facility / reaction gate reasons already logged.
    _facility_gate_warned: set[str] = set()
    _reaction_gate_warned: set[str] = set()

    @staticmethod
    def _profile_is_npc_station(profile_payload: dict[str, Any]) -> bool:
        facility_type = str(profile_payload.get("facility_type") or "").strip().lower()
        location_type = str(profile_payload.get("location_type") or "").strip().lower()
        return location_type == "station" or facility_type == "npc_station"

    @classmethod
    def _profile_service_decision(cls, profile_payload: dict[str, Any], services: tuple[str, ...]) -> Any:
        """domain.industry_facility.service_allowed for the profile's structure:
        allowed when any of `services` can run there; else the first refusal."""
        fits = profile_payload.get("structure_service_fits")
        band, band_reason = _security_band.profile_security_band(profile_payload)
        npc = _industry_facility.npc_space(profile_payload)
        structure = profile_payload.get("structure_type_id")
        decisions = []
        for service in services:
            fit = fits.get(service) if isinstance(fits, dict) else None
            decision = _industry_facility.service_allowed(fit, band, npc=npc, service=service)
            if fit is None:
                decision = _industry_facility.Decision(False, (
                    f"facility unknown (structure type {structure}, no SDE service-module data)" if structure
                    else "facility unknown (the profile has no structure type)"
                ) + f": {_industry_facility.SERVICE_LABELS[service]} cannot be checked", True)
            elif decision.uncertain and band is None and band_reason:
                decision = _industry_facility.Decision(decision.allowed, f"{decision.reason} ({band_reason})", True)
            if decision.allowed:
                return decision
            decisions.append(decision)
        return decisions[0]

    @classmethod
    def _facility_gate(
        cls, *, manufacturing_group: str | None, profile_payload: dict[str, Any] | None, group_id: Any = None,
    ) -> Any:
        """domain.industry_facility.Decision for manufacturing a product in the
        profile's facility.

        Only the shipyard hulls are gated, by CCP's assembly lines
        (domain.industry_facility.required_shipyard): line 176 hulls
        (Dreadnought, Carrier, ...) need Standup Capital Shipyard I (or the
        Supercapital Shipyard, which also runs line 176), Titans and
        Supercarriers (line 177) Standup Supercapital Shipyard I. Everything
        else, Freighters, Jump Freighters and capital components included, is
        built wherever the profile manufactures (an NPC station, or any
        structure the Manufacturing Plant fits). A product without a group id
        whose rig group is "Capital Ships" is read as a line-176 hull.

        Whether the shipyard fits the structure and may be onlined in the
        system comes from the SDE (structure_service_fits; band, NPC space).
        An NPC station has no shipyard. With no profile there is no facility
        constraint. Unknown facility or band: refused, uncertain."""
        try:
            gid = int(group_id or 0)
        except (TypeError, ValueError):
            gid = 0
        required = _industry_facility.required_shipyard(gid, _sde_industry.shipyard_lines()) if gid > 0 else (
            _industry_facility.CAPITAL_SHIPYARD if manufacturing_group == "Capital Ships" else None)
        if required is None or profile_payload is None:
            return _industry_facility.Decision(True)
        label = _industry_facility.SERVICE_LABELS[required]
        if cls._profile_is_npc_station(profile_payload):
            return _industry_facility.Decision(False, f"needs {label}: an NPC station has none")
        decision = cls._profile_service_decision(
            profile_payload, _industry_facility.services_providing(required, _sde_industry.shipyard_lines()))
        if not decision.allowed and label not in str(decision.reason):
            decision = _industry_facility.Decision(False, f"needs {label}: {decision.reason}", decision.uncertain)
        return decision

    @staticmethod
    def _warn_gate_once(warned: set[str], prefix: str, decision: Any) -> None:
        if decision.uncertain and decision.reason and FacilitiesMixin._first_warning(warned, decision.reason):
            logger.warning("%s: %s", prefix, decision.reason)

    _ONCE_WARNING_LOCK = threading.Lock()

    @staticmethod
    def _first_warning(warned: set[Any], key: Any) -> bool:
        """True the first time `key` is met in this refresh (log it now).
        While warnings are quiet (a thrown-away plan, quiet_warnings) nothing
        is registered, so the plan that is kept still logs it, as
        InventionContext.warn_once does. The check-then-add holds a lock:
        the product rows are built on worker threads."""
        if warnings_are_quiet():
            return False
        with FacilitiesMixin._ONCE_WARNING_LOCK:
            if key in warned:
                return False
            warned.add(key)
            return True

    @classmethod
    def _reset_once_warnings(cls) -> None:
        """Scope the once-only WARNING sets to one refresh: cleared when an
        overview is computed (industry_manufacturing_product_overview), so a
        long-running process logs a still-missing input again each refresh."""
        with FacilitiesMixin._ONCE_WARNING_LOCK:
            for warned in (
                FacilitiesMixin._facility_gate_warned, FacilitiesMixin._reaction_gate_warned,
                FacilitiesMixin._unclassified_rig_group_warned, FacilitiesMixin._rig_security_warned,
                FacilitiesMixin._aggregate_rig_warned,
            ):
                warned.clear()

    @classmethod
    def _reaction_gate(cls, profile_payload: dict[str, Any] | None) -> Any:
        """Whether the profile's facility runs reactions: a Standup reactor
        (45537-45539) fits it and may be onlined in the band (SDE: Refineries
        only, not in highsec). No profile, an unknown facility or an unknown
        band: refused, uncertain (the take-or-buy path then buys the
        material). An NPC station runs no reactions."""
        if not isinstance(profile_payload, dict):
            return _industry_facility.Decision(False, "no industry profile: reactions are not planned", True)
        if cls._profile_is_npc_station(profile_payload):
            return _industry_facility.Decision(False, "an NPC station runs no reactions")
        return cls._profile_service_decision(profile_payload, (_industry_facility.REACTOR,))

    @classmethod
    def _reactions_allowed_for_profile(cls, profile_payload: dict[str, Any] | None) -> bool:
        decision = cls._reaction_gate(profile_payload)
        cls._warn_gate_once(FacilitiesMixin._reaction_gate_warned, "Reaction gate", decision)
        return decision.allowed

    # -----------------
    # Endpoints logic
    # -----------------

    @staticmethod
    def _extract_structure_industry_bonuses_from_type_bonus(row: dict, *, language: str) -> dict:
        role_raw = row.get("roleBonuses")
        try:
            role = json.loads(role_raw) if isinstance(role_raw, str) else (role_raw or [])
        except (TypeError, ValueError) as exc:
            logger.warning("Structure type %s: roleBonuses cannot be parsed (%s); no structure bonus applies",
                           row.get("id"), exc)
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
            except (TypeError, ValueError):
                logger.warning("Structure type %s: role bonus %r is not a number; it is left out", row.get("id"), bonus)
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
            if owner_ids and self._state.esi_service is not None:
                resolved = self._state.esi_service.get_universe_names(owner_ids)
                owner_name_map = {int(k): ((v or {}).get("name") or "") for k, v in resolved.items()}
        except _ESI_ERRORS as exc:
            logger.warning("Public structures: owner names cannot be resolved (%s); the structures keep their "
                           "owner IDs", exc)
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
                except (TypeError, ValueError):
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
        except SQLAlchemyError as exc:
            logger.warning("Corporation structures: type data cannot be read (%s); the structures keep their type IDs",
                           exc)

        try:
            corp_ids: list[int] = []
            for row in out:
                cid = row.get("corporation_id")
                if cid is None:
                    continue
                try:
                    corp_ids.append(int(cid))
                except (TypeError, ValueError):
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
        except _ESI_ERRORS as exc:
            logger.warning("Corporation structures: owner names cannot be resolved (%s); the structures keep their IDs",
                           exc)

        return out

    def industry_profiles(self, *, character_id: int) -> list[dict]:
        session: Any = self._sessions.app_session()
        profiles = industry_profile_list_by_character_id(session, character_id)
        return [p.to_dict() for p in profiles]

    def industry_profile_context(self, profile_id: int) -> dict[str, Any] | None:
        """The industry profile as the producer resolves it: the profile plus
        its system cost indices, facility and structure bonuses, with the
        reasons a part could not be read (lookup_error,
        cost_index_unknown_reason). An unknown profile raises ServiceError."""
        return self._resolve_industry_profile_context(character_id=None, industry_profile_id=int(profile_id))

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
            except (ServiceError, *_ESI_ERRORS) as exc:
                logger.warning("Industry profile: the cost indices of system %s cannot be read (%s); job fees are "
                               "unknown", profile.system_id, exc)
                system_payload = {"solar_system_id": int(profile.system_id), "cost_indices": [],
                                  "lookup_error": str(exc)}
            try:
                solar_system_payload = next(
                    (
                        entry
                        for entry in (self.solar_systems() or [])
                        if isinstance(entry, dict) and int(entry.get("id") or 0) == int(profile.system_id)
                    ),
                    None,
                )
            except SQLAlchemyError as exc:
                logger.warning("Industry profile: solar system %s cannot be read from the SDE (%s); its security band "
                               "is unknown", profile.system_id, exc)
                solar_system_payload = None
            profile_payload["industry_system"] = system_payload
            profile_payload["system_cost_indices"] = system_payload.get("cost_indices") or []
            if isinstance(solar_system_payload, dict):
                profile_payload["system_security_status"] = solar_system_payload.get("security_status")
                profile_payload["system_region_id"] = solar_system_payload.get("region_id")
                # The constellation's faction (SDE): set in NPC space.
                profile_payload["system_faction_id"] = solar_system_payload.get("faction_id")
                profile_payload["industry_system"] = {
                    **system_payload,
                    "security_status": solar_system_payload.get("security_status"),
                    "region_id": solar_system_payload.get("region_id"),
                }
            band, _ = _security_band.profile_security_band(profile_payload)
            if band is not None:
                profile_payload["system_security_band"] = band
        # The manufacturing cost index, or why it is unknown (the page shows it; the fees are then unknown).
        _index, profile_payload["cost_index_unknown_reason"] = (
            self._system_cost_index_with_reason(profile_payload=profile_payload, activity="manufacturing")
            if profile.system_id is not None
            else (None, "the industry profile has no solar system, so no manufacturing cost index")
        )

        if profile.facility_id is not None:
            try:
                profile_payload["facility"] = self.industry_facility(facility_id=int(profile.facility_id))
            except (ServiceError, *_ESI_ERRORS) as exc:
                logger.warning("Industry profile: facility %s cannot be read (%s); using the profile's own tax",
                               profile.facility_id, exc)
                profile_payload["facility"] = {"facility_id": int(profile.facility_id), "tax": profile.facility_tax,
                                               "lookup_error": str(exc)}

        if profile.structure_type_id is not None:
            try:
                structure_bonus_payload = self.structure_type_bonuses(type_id=int(profile.structure_type_id))
            except (ServiceError, SQLAlchemyError) as exc:
                logger.warning("Industry profile: the bonuses of structure type %s cannot be read (%s); no structure "
                               "bonus applies", profile.structure_type_id, exc)
                structure_bonus_payload = {"type_id": int(profile.structure_type_id), "bonuses": {},
                                           "lookup_error": str(exc)}
            profile_payload["structure_type_bonuses"] = structure_bonus_payload.get("bonuses") or {}
            try:
                profile_payload["structure_service_fits"] = self._structure_service_fits(int(profile.structure_type_id))
            except Exception:
                logger.warning(
                    "Facility gate: no SDE service-module data for structure type %s; capitals are not planned there",
                    profile.structure_type_id, exc_info=True,
                )

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
                        "security_multipliers": rig_effect_payload.get("security_multipliers"),
                        "effects": rig_effect_payload.get("effects") or [],
                    }
                )
            profile_payload["structure_rigs"] = structure_rigs

        return profile_payload

    def _structure_service_fits(self, structure_type_id: int) -> dict[str, dict[str, Any]]:
        """domain.industry_facility.structure_service_fits for one structure
        type, from the SDE; cached per type."""
        cache = self.__dict__.setdefault("_structure_service_fits_cache", {})
        if structure_type_id in cache:
            return cache[structure_type_id]
        session: Any = self._sessions.sde_session()
        module_ids = [t for ids in _industry_facility.SERVICE_MODULE_TYPE_IDS.values() for t in ids]
        fits = _industry_facility.structure_service_fits(
            structure_type_id, get_type_group_id(session, structure_type_id),
            get_service_module_fits(session, module_ids),
        )
        cache[structure_type_id] = fits
        return fits

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

    def _profile_job_surcharge_rates(self, profile_payload: dict[str, Any] | None) -> JobSurchargeRates:
        """The surcharges charged on every job's value (job_cost.py): the SCC
        rates from the admin settings, and the profile's facility tax
        (job_cost.profile_facility_tax). A tax the profile does not determine
        is unknown, never 0 %: every fee then reads unknown with the reason,
        and a WARNING is logged once per reason."""
        scc = max(0.0, float(self._adm("industry", "scc_surcharge")))
        research_scc = max(0.0, float(self._adm("industry", "research_scc_surcharge")))
        facility_tax, reason = profile_facility_tax(profile_payload, scc=scc)
        if reason is not None and isinstance(profile_payload, dict):
            warned = self.__dict__.setdefault("_facility_tax_warned", set())
            if self._first_warning(warned, reason):
                logger.warning("Job install fees unknown: %s", reason)
        return JobSurchargeRates(facility_tax=facility_tax, scc=scc, research_scc=research_scc, unknown_reason=reason)

    # (type_id, group, category) of products already logged as unclassifiable.
    _unclassified_rig_group_warned: set[tuple[Any, ...]] = set()

    @staticmethod
    def _infer_manufacturing_group(product_entry: dict[str, Any]) -> str | None:
        """The product's manufacturing group label (domain.rig_groups.primary_group):
        its most specific engineering-rig group by SDE category/group, used for
        the facility gate and shown on the row. Rig coverage itself is
        _manufacturing_rig_groups (a product can be in several groups)."""
        return _rig_groups.primary_group(product_entry, catalog=_sde_industry.rig_catalog())

    @staticmethod
    def _manufacturing_rig_groups(product_entry: Any, activity: str = "manufacturing") -> frozenset[str]:
        """Every rig group that covers the product, EVE's rule
        (domain.rig_groups: the industry target filters by SDE category and
        group): the engineering-rig groups for manufacturing, the reactor-rig
        groups for a reaction (activity="reaction"). A product whose entry
        has too little to classify gets no group (only "All" rigs apply, an
        over-buy rather than an under-buy) and a WARNING, once per product."""
        groups = _rig_groups.rig_groups_for_entry(product_entry, activity=activity,
                                                  catalog=_sde_industry.rig_catalog())
        if groups is not None:
            return groups
        entry = product_entry if isinstance(product_entry, dict) else {}
        key = (activity, entry.get("type_id"), entry.get("group_id"), entry.get("group_name"),
               entry.get("category_id"), entry.get("category_name"))
        if FacilitiesMixin._first_warning(FacilitiesMixin._unclassified_rig_group_warned, key):
            logger.warning(
                "No rig group for type %s (%s): its SDE group/category is unknown; only \"All\" rigs apply",
                entry.get("type_id"), entry.get("type_name") or "unnamed",
            )
        return frozenset()

    @classmethod
    def _profile_rig_reduction(
        cls,
        *,
        profile_payload: dict[str, Any] | None,
        activity: str,
        metric: str,
        manufacturing_group: str | Collection[str] | None = None,
    ) -> float:
        """Combined rig reduction for one activity and metric.

        `manufacturing_group`: the rig groups that cover the product
        (_manufacturing_rig_groups, with activity="reaction" for a reaction),
        or one group label. A manufacturing or reaction rig effect applies
        when its group is "All" or one of them. None applies every rig
        effect (no product given). Other activities apply "All" effects only.

        Each effect marked security_scaled is multiplied by its rig's
        multiplier for the profile's security band (domain.security_band);
        with the band unknown, by the smallest multiplier the rig can have,
        and a WARNING. The one reduction path of the producer and the daily
        planner (facility_bonus).

        The aggregate structure_rig_*_bonus is never applied: it names no
        rig, so a profile with only the aggregate gets no rig bonus, with a
        once-per-refresh WARNING (_aggregate_only_rig_reason)."""
        if not isinstance(profile_payload, dict):
            return 0.0
        valid_activities = set(cls._ACTIVITY_EFFECT_ALIASES.get(activity, (activity,)))
        covered_groups: frozenset[str] | None = (
            None if manufacturing_group is None
            else frozenset({manufacturing_group}) if isinstance(manufacturing_group, str)
            else frozenset(manufacturing_group)
        )
        band: str | None = None
        band_reason: str | None = None
        band_read = False
        reductions: list[float] = []
        for rig in profile_payload.get("structure_rigs") or []:
            if not isinstance(rig, dict):
                continue
            multiplier: float | None = None
            for effect in rig.get("effects") or []:
                if not isinstance(effect, dict):
                    continue
                if str(effect.get("metric") or "") != metric:
                    continue
                if str(effect.get("activity") or "") not in valid_activities:
                    continue
                effect_group = str(effect.get("group") or "All")
                if activity in {"manufacturing", "reaction"}:
                    if covered_groups is not None and effect_group != "All" and effect_group not in covered_groups:
                        continue
                elif effect_group not in {"All", ""}:
                    continue
                value = cls._normalize_fraction(effect.get("value"))
                if effect.get("security_scaled"):
                    if multiplier is None:
                        if not band_read:
                            band, band_reason = _security_band.profile_security_band(profile_payload)
                            band_read = True
                        multiplier, reason = _security_band.rig_security_multiplier(rig, band)
                        if reason is not None:
                            cls._warn_rig_security_once(
                                reason if band_reason is None or band is not None else f"{reason} ({band_reason})")
                    value *= multiplier
                reductions.append(value)

        if reductions:
            return cls._combine_reductions(reductions)
        # No per-effect rig covers this product: no rig bonus. The aggregate
        # structure_rig_*_bonus is no fallback. With per-effect rigs it
        # combines those same rigs (an under-buy); alone it names no rig, so
        # neither its groups nor its security multiplier is known (Task 15 M1,
        # superseding Task 11 I1 / 11b M2): no rig bonus, the same as the
        # daily planner's facility_bonus (an over-buy, never an under-buy),
        # with a WARNING here and a row reason (_aggregate_only_rig_reason).
        if activity == "manufacturing" and metric in cls._aggregate_only_rig_metrics(profile_payload):
            if FacilitiesMixin._first_warning(FacilitiesMixin._aggregate_rig_warned, metric):
                logger.warning(
                    "Rig bonus: %s (aggregate structure_rig_%s_bonus %s ignored)",
                    cls._aggregate_only_rig_reason(profile_payload), metric,
                    profile_payload.get(f"structure_rig_{metric}_bonus"),
                )
        return 0.0

    #: What leaving each aggregate rig metric out does to a row (Task 15 fix round 1).
    _AGGREGATE_RIG_IMPACT = {
        "material": "the material quantities and cost are overstated",
        "time": "the build time is overstated",
        "cost": "the job fees are overstated",
    }

    @classmethod
    def _aggregate_only_rig_metrics(cls, profile_payload: dict[str, Any] | None) -> tuple[str, ...]:
        """The metrics (material, time, cost) with a positive aggregate
        structure_rig_*_bonus on a profile with no per-effect rig entries:
        the bonuses the producer does not apply. Empty otherwise. The
        aggregate was only ever a manufacturing bonus (never reactions)."""
        if not isinstance(profile_payload, dict):
            return ()
        if any(
            isinstance(rig, dict) and any(isinstance(effect, dict) for effect in (rig.get("effects") or []))
            for rig in profile_payload.get("structure_rigs") or []
        ):
            return ()
        return tuple(
            metric for metric in ("material", "time", "cost")
            if cls._normalize_fraction(profile_payload.get(f"structure_rig_{metric}_bonus")) > 0.0
        )

    @classmethod
    def _aggregate_only_rig_reason(cls, profile_payload: dict[str, Any] | None) -> str | None:
        """The row reason when _aggregate_only_rig_metrics is not empty (the
        producer then applies no rig bonus to manufacturing), else None."""
        metrics = cls._aggregate_only_rig_metrics(profile_payload)
        if not metrics:
            return None
        return (
            f"rig bonus unknown: profile has no rig ids, only the aggregate structure rig "
            f"{', '.join(metrics)} bonus, so no rig bonus applies to manufacturing (re-save the profile with its rigs)"
        )

    # Rig security-multiplier / aggregate-fallback reasons already logged.
    _rig_security_warned: set[str] = set()
    _aggregate_rig_warned: set[str] = set()

    @classmethod
    def _warn_rig_security_once(cls, reason: str) -> None:
        if FacilitiesMixin._first_warning(FacilitiesMixin._rig_security_warned, reason):
            logger.warning("Rig bonus security multiplier: %s", reason)

    @classmethod
    def _system_cost_index(cls, *, profile_payload: dict[str, Any] | None, activity: str) -> float | None:
        """The activity's system cost index; None when it is unknown (see
        _system_cost_index_with_reason): never a silent 0."""
        index, _reason = cls._system_cost_index_with_reason(profile_payload=profile_payload, activity=activity)
        return index

    @classmethod
    def _system_cost_index_with_reason(
        cls, *, profile_payload: dict[str, Any] | None, activity: str,
    ) -> tuple[float | None, str | None]:
        """(index, None), or (None, why) when the profile has no index for
        the activity (no profile, a failed lookup, a system without the
        entry, a null or non-numeric value). The job fee is then unknown,
        with this reason (job_fee_unknown_reasons), never 0."""
        missing = f"the industry profile has no {activity} cost index"
        if not isinstance(profile_payload, dict):
            return None, missing
        aliases = set(cls._ACTIVITY_COST_INDEX_ALIASES.get(activity, (activity,)))
        system_id = profile_payload.get("system_id")
        for entry in profile_payload.get("system_cost_indices") or []:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("activity") or "") not in aliases:
                continue
            raw = entry.get("cost_index")
            if raw is None:
                return None, f"{missing} (the ESI entry has no value)"
            try:
                return max(0.0, float(raw)), None
            except (TypeError, ValueError):
                where = f" of system {system_id}" if system_id is not None else ""
                reason = f"the {activity} cost index {raw!r}{where} is not a number"
                logger.warning("System cost index: %s; the job fee is unknown", reason)
                return None, reason
        industry_system = profile_payload.get("industry_system")
        lookup_error = industry_system.get("lookup_error") if isinstance(industry_system, dict) else None
        if lookup_error:
            where = f" of system {system_id}" if system_id is not None else ""
            return None, f"{missing} (the cost indices{where} could not be read: {lookup_error})"
        return None, missing

    def industry_system_cost_index(self, *, system_id: int) -> dict:
        if not system_id:
            raise ServiceError("System ID is required.", status_code=400)

        if self._state.esi_service is None:
            raise ServiceError("ESI service not initialized.", status_code=503)

        systems = self._state.esi_service.get_industry_systems()
        row = next((s for s in systems if s.get("solar_system_id") == system_id), None)
        if not row:
            return {"solar_system_id": system_id, "cost_indices": [],
                    "cost_index_unknown_reason": f"ESI reports no cost indices for system {system_id}"}
        out = dict(row)   # a copy: the ESI rows are cached
        _index, out["cost_index_unknown_reason"] = self._system_cost_index_with_reason(
            profile_payload={"system_id": system_id, "system_cost_indices": out.get("cost_indices") or []},
            activity="manufacturing",
        )
        return out

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

    def structure_rigs(self) -> list[dict]:
        cache = getattr(self._state, "_structure_rigs_cache", None)
        now = time.time()
        if (
            cache
            and isinstance(cache, tuple)
            and len(cache) == 3
            and cache[1] == self._STRUCTURE_RIGS_CACHE_VERSION
            and (now - cache[0] < int(self._adm("cache_ttl", "structure_rig_cache_ttl_seconds")))
        ):
            return cache[2]

        session: Any = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"

        # Every published engineering and reactor rig, by SDE group (category
        # 66 Structure Module): the industry rigs a profile can fit.
        rows = session.execute(
            text(
                """
                SELECT t.id, t.name
                FROM types t JOIN groups g ON g.id = t.groupID
                WHERE t.published = 1
                  AND g.categoryID = 66
                  AND (
                        json_extract(g.name, '$.en') LIKE 'Structure Engineering Rig%'
                     OR json_extract(g.name, '$.en') LIKE 'Structure %Reactor Rig%'
                  )
                ORDER BY t.id ASC
                """
            )
        ).fetchall()

        name_by_type_id: dict[int, str] = {}
        for type_id, name_json in rows:
            try:
                name_obj = json.loads(name_json) if isinstance(name_json, str) else {}
            except ValueError:
                name_obj = {}  # cosmetic: the rig is named by its type ID
            if not isinstance(name_obj, dict):
                name_obj = {}
            name_by_type_id[int(type_id)] = name_obj.get(language) or name_obj.get("en") or str(type_id)

        rigs_out: list[dict] = [
            {**rig, "name": name_by_type_id.get(int(rig["type_id"]), str(rig["type_id"]))}
            for rig in get_rig_effects_for_type_ids(session, list(name_by_type_id))
            if rig.get("effects")
        ]

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
