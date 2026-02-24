from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import bindparam, or_, text
from sqlalchemy.sql import func

from classes.asset_provenance import build_fifo_remaining_lots_by_type
from classes.database_models import Blueprints
from eve_online_industry_tracker.db_models import (
    CharacterAssetsModel,
    CharacterIndustryJobsModel,
    CharacterWalletTransactionsModel,
    CorporationAssetsModel,
    CorporationIndustryJobsModel,
    CorporationWalletTransactionsModel,
)

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.infrastructure.session_provider import (
    SessionProvider,
    StateSessionProvider,
)

from eve_online_industry_tracker.infrastructure.industry_adapter import (
    corporation_structures_list_by_corporation_id,
    enrich_blueprints_for_character,
    get_blueprint_assets,
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
    plan_submanufacturing_tree,
    public_structures_cache_ttl_seconds,
    trigger_refresh_public_structures_for_system,
)

from eve_online_industry_tracker.infrastructure.invention_options_service import (
    compute_invention_options_for_blueprint,
    market_price_map_from_esi_prices,
)

from eve_online_industry_tracker.infrastructure.sde.blueprints import get_blueprint_manufacturing_data

from eve_online_industry_tracker.infrastructure.industry_builder_viewmodel import (
    compute_ui_build_tree_rows_by_product,
    compute_ui_copy_jobs,
    compute_ui_copy_invention_jobs_rows_for_best_option,
    compute_ui_invention_overview_row_from_summary,
    compute_ui_missing_blueprints,
)


class IndustryService:
    _INDUSTRY_BUILDER_JOB_TTL_SECONDS = 6 * 3600

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
    # Background jobs
    # -----------------

    @staticmethod
    def _industry_builder_job_key(
        *,
        character_id: int,
        profile_id: int | None,
        maximize_runs: bool,
        pricing_key: str,
        prefer_inventory_consumption: bool,
        assume_bpo_copy_overhead: bool,
    ) -> str:
        pid = int(profile_id or 0)
        return (
            f"{int(character_id)}:{pid}:{1 if maximize_runs else 0}:"
            f"{1 if prefer_inventory_consumption else 0}:{1 if assume_bpo_copy_overhead else 0}:{str(pricing_key)}"
        )

    def _cleanup_old_industry_builder_jobs(self) -> None:
        now = datetime.utcnow()
        with self._state.industry_builder_jobs_lock:
            to_delete: list[str] = []
            for job_id, job in list(self._state.industry_builder_jobs.items()):
                if not isinstance(job, dict):
                    to_delete.append(str(job_id))
                    continue
                created_at = job.get("created_at")
                if not isinstance(created_at, datetime):
                    continue
                age_s = (now - created_at).total_seconds()
                if age_s > self._INDUSTRY_BUILDER_JOB_TTL_SECONDS:
                    to_delete.append(str(job_id))

            for job_id in to_delete:
                self._state.industry_builder_jobs.pop(job_id, None)
                for k, v in list(self._state.industry_builder_jobs_by_key.items()):
                    if v == job_id:
                        self._state.industry_builder_jobs_by_key.pop(k, None)

    def _run_industry_builder_update_job(
        self,
        *,
        job_id: str,
        character_id: int,
        profile_id: int | None,
        maximize_runs: bool,
        pricing_preferences: dict | None,
        prefer_inventory_consumption: bool,
        assume_bpo_copy_overhead: bool,
    ) -> None:
        # Keep these as Any to avoid Optional-induced type noise; lifetime is managed
        # in the finally block below.
        session: Any = None
        sde_session: Any = None

        try:
            character = self._state.char_manager.get_character_by_id(int(character_id))
            if not character:
                raise ServiceError(f"Character ID {character_id} not found", status_code=400)

            session = self._sessions.app_session()
            language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"

            try:
                sde_session = self._sessions.sde_session()
            except Exception:
                sde_session = None

            selected_profile = None
            if profile_id:
                selected_profile = industry_profile_get_by_id(session, int(profile_id))
                if selected_profile and int(getattr(selected_profile, "character_id", 0) or 0) != int(character_id):
                    raise ServiceError("Industry profile does not belong to this character", status_code=400)
            else:
                selected_profile = industry_profile_get_default_for_character_id(session, int(character_id))

            manufacturing_ci = 0.0
            invention_ci = 0.0
            copying_ci = 0.0
            research_me_ci = 0.0
            research_te_ci = 0.0
            system_id_for_cost_index: int | None = None
            system_id_for_cost_index_source: str | None = None
            facility_id_for_cost_index: int | None = None

            if selected_profile is not None:
                try:
                    raw_facility_id = getattr(selected_profile, "facility_id", None)
                    if raw_facility_id is not None and int(raw_facility_id) > 0:
                        facility_id_for_cost_index = int(raw_facility_id)
                except Exception:
                    facility_id_for_cost_index = None

            # Prefer the facility's actual solar system (matches in-game quoting).
            if facility_id_for_cost_index is not None and self._state.esi_service is not None:
                try:
                    facilities = self._state.esi_service.get_industry_facilities() or []
                    fac_row = next((f for f in facilities if f.get("facility_id") == int(facility_id_for_cost_index)), None)
                    if isinstance(fac_row, dict):
                        fac_system_id = fac_row.get("solar_system_id")
                    else:
                        fac_system_id = None
                    if fac_system_id is not None:
                        system_id_for_cost_index = int(fac_system_id)
                        system_id_for_cost_index_source = "industry_facilities"
                except Exception:
                    system_id_for_cost_index = None
                    system_id_for_cost_index_source = None

            # Fallback: profile.system_id (older profiles / manual selection)
            if system_id_for_cost_index is None and selected_profile is not None and getattr(selected_profile, "system_id", None) is not None:
                try:
                    system_id_for_cost_index = int(getattr(selected_profile, "system_id"))
                    system_id_for_cost_index_source = "profile.system_id"
                except Exception:
                    system_id_for_cost_index = None
                    system_id_for_cost_index_source = None

            if system_id_for_cost_index is not None and self._state.esi_service is not None:
                try:
                    systems = self._state.esi_service.get_industry_systems() or []
                    row = next((s for s in systems if s.get("solar_system_id") == int(system_id_for_cost_index)), None)
                    if row:
                        for entry in (row.get("cost_indices") or []):
                            if entry.get("activity") == "manufacturing":
                                manufacturing_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "invention":
                                invention_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "copying":
                                copying_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_material_efficiency":
                                research_me_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_time_efficiency":
                                research_te_ci = float(entry.get("cost_index") or 0.0)
                except Exception:
                    manufacturing_ci = 0.0
                    invention_ci = 0.0
                    copying_ci = 0.0
                    research_me_ci = 0.0
                    research_te_ci = 0.0

            surcharge_rate = 0.0
            if selected_profile is not None:
                try:
                    facility_tax = float(getattr(selected_profile, "facility_tax", 0.0) or 0.0)
                except Exception:
                    facility_tax = 0.0
                try:
                    scc_surcharge = float(getattr(selected_profile, "scc_surcharge", 0.0) or 0.0)
                except Exception:
                    scc_surcharge = 0.0
                if facility_tax >= 1.0:
                    facility_tax = facility_tax / 100.0
                if scc_surcharge >= 1.0:
                    scc_surcharge = scc_surcharge / 100.0
                surcharge_rate = max(0.0, facility_tax + scc_surcharge)

            owned_bp_type_ids: set[int] = set()
            owned_bp_best_by_type_id: dict[int, dict] = {}

            def _consider_owned_bp(
                *,
                blueprint_type_id: int,
                is_blueprint_copy: bool,
                me_percent: int | None,
                te_percent: int | None,
                runs: int | None,
            ) -> None:
                tid = int(blueprint_type_id)
                if tid <= 0:
                    return
                owned_bp_type_ids.add(tid)

                try:
                    me_i = int(me_percent or 0)
                except Exception:
                    me_i = 0
                try:
                    te_i = int(te_percent or 0)
                except Exception:
                    te_i = 0

                rec = {
                    "is_blueprint_copy": bool(is_blueprint_copy),
                    "me_percent": int(me_i),
                    "te_percent": int(te_i),
                    "runs": (int(runs) if runs is not None else None),
                }

                cur = owned_bp_best_by_type_id.get(tid)
                if not isinstance(cur, dict):
                    owned_bp_best_by_type_id[tid] = rec
                    return

                cur_is_bpc = bool(cur.get("is_blueprint_copy"))
                new_is_bpc = bool(rec.get("is_blueprint_copy"))
                if new_is_bpc and not cur_is_bpc:
                    owned_bp_best_by_type_id[tid] = rec
                    return
                if new_is_bpc == cur_is_bpc:
                    cur_me = int(cur.get("me_percent") or 0)
                    cur_te = int(cur.get("te_percent") or 0)
                    new_me = int(rec.get("me_percent") or 0)
                    new_te = int(rec.get("te_percent") or 0)
                    if (new_me > cur_me) or (new_me == cur_me and new_te > cur_te):
                        owned_bp_best_by_type_id[tid] = rec

            try:
                rows = (
                    session.query(
                        CharacterAssetsModel.type_id,
                        CharacterAssetsModel.is_blueprint_copy,
                        CharacterAssetsModel.blueprint_material_efficiency,
                        CharacterAssetsModel.blueprint_time_efficiency,
                        CharacterAssetsModel.blueprint_runs,
                    )
                    .filter(
                        CharacterAssetsModel.type_category_name == "Blueprint",
                        CharacterAssetsModel.character_id == int(character_id),
                    )
                    .all()
                )
                for tid, is_bpc, me, te, runs in rows or []:
                    if tid is None:
                        continue
                    _consider_owned_bp(
                        blueprint_type_id=int(tid),
                        is_blueprint_copy=bool(is_bpc),
                        me_percent=(int(me) if me is not None else None),
                        te_percent=(int(te) if te is not None else None),
                        runs=(int(runs) if runs is not None else None),
                    )
            except Exception:
                pass

            try:
                corp_id = getattr(character, "corporation_id", None)
                if corp_id is not None:
                    rows = (
                        session.query(
                            CorporationAssetsModel.type_id,
                            CorporationAssetsModel.is_blueprint_copy,
                            CorporationAssetsModel.blueprint_material_efficiency,
                            CorporationAssetsModel.blueprint_time_efficiency,
                            CorporationAssetsModel.blueprint_runs,
                        )
                        .filter(
                            CorporationAssetsModel.type_category_name == "Blueprint",
                            CorporationAssetsModel.corporation_id == int(corp_id),
                        )
                        .all()
                    )
                    for tid, is_bpc, me, te, runs in rows or []:
                        if tid is None:
                            continue
                        _consider_owned_bp(
                            blueprint_type_id=int(tid),
                            is_blueprint_copy=bool(is_bpc),
                            me_percent=(int(me) if me is not None else None),
                            te_percent=(int(te) if te is not None else None),
                            runs=(int(runs) if runs is not None else None),
                        )
            except Exception:
                pass

            rig_payload: list[dict] = []
            try:
                if selected_profile is not None:
                    rig_ids = [
                        getattr(selected_profile, "rig_slot0_type_id", None),
                        getattr(selected_profile, "rig_slot1_type_id", None),
                        getattr(selected_profile, "rig_slot2_type_id", None),
                    ]
                    rig_ids = [int(x) for x in rig_ids if x is not None and int(x) != 0]
                    if rig_ids:
                        if sde_session is None:
                            sde_session = self._sessions.sde_session()
                        rig_payload = get_rig_effects_for_type_ids(sde_session, rig_ids)
            except Exception:
                rig_payload = []

            if sde_session is None:
                sde_session = self._sessions.sde_session()

            # Fetch ESI market prices once per update run (best-effort) and reuse.
            market_prices_rows: list[dict] | None = None
            try:
                if self._state.esi_service is not None:
                    market_prices_rows = self._state.esi_service.get_market_prices() or []
            except Exception:
                market_prices_rows = None

            market_price_map_shared: dict[int, dict[str, float | None]] = {}
            try:
                market_price_map_shared = market_price_map_from_esi_prices(
                    market_prices_rows if isinstance(market_prices_rows, list) else None
                )
            except Exception:
                market_price_map_shared = {}

            all_blueprints = get_blueprint_assets(
                session,
                self._state.esi_service,
                sde_session=sde_session,
                language=language,
                include_unowned=False,
            )
            all_blueprints = [bp for bp in all_blueprints if bp.get("owner_id") == int(character_id)]

            def _progress(done: int, total: int, _bp_type_id: int) -> None:
                with self._state.industry_builder_jobs_lock:
                    job = self._state.industry_builder_jobs.get(job_id)
                    if not isinstance(job, dict):
                        return
                    job["progress_done"] = int(done)
                    job["progress_total"] = int(total)
                    job["updated_at"] = datetime.utcnow()

            data = enrich_blueprints_for_character(
                all_blueprints,
                character,
                esi_service=self._state.esi_service,
                industry_profile=selected_profile,
                manufacturing_system_cost_index=manufacturing_ci,
                copying_system_cost_index=copying_ci,
                research_me_system_cost_index=research_me_ci,
                research_te_system_cost_index=research_te_ci,
                surcharge_rate_total_fraction=surcharge_rate,
                owned_blueprint_type_ids=owned_bp_type_ids,
                owned_blueprint_best_by_type_id=owned_bp_best_by_type_id,
                include_submanufacturing=True,
                submanufacturing_blueprint_type_id=None,
                maximize_blueprint_runs=bool(maximize_runs),
                rig_payload=rig_payload,
                db_app_session=session,
                db_sde_session=sde_session,
                language=language,
                progress_callback=_progress,
                prefer_inventory_consumption=bool(prefer_inventory_consumption),
                pricing_preferences=(pricing_preferences if isinstance(pricing_preferences, dict) else None),
                assume_bpo_copy_overhead=bool(assume_bpo_copy_overhead),
                esi_market_prices=market_prices_rows,
                market_price_map=market_price_map_shared,
            )

            # Precompute best-ROI invention summaries as part of the update job.
            # This avoids the UI calling /industry_invention_options for every blueprint on page load
            # (which can exhaust the SQLAlchemy connection pool).
            try:
                bp_type_ids: list[int] = []
                for bp in data or []:
                    if not isinstance(bp, dict):
                        continue
                    tid = bp.get("type_id")
                    if tid is None:
                        continue
                    try:
                        tid_i = int(tid)
                    except Exception:
                        continue
                    if tid_i > 0:
                        bp_type_ids.append(tid_i)
                bp_type_ids = sorted(set(bp_type_ids))
            except Exception:
                bp_type_ids = []

            supports_invention_by_bp_type_id: dict[int, bool] = {}
            if bp_type_ids and sde_session is not None:
                try:
                    rows = (
                        sde_session.query(Blueprints.blueprintTypeID, Blueprints.activities)
                        .filter(Blueprints.blueprintTypeID.in_(bp_type_ids))
                        .all()
                    )
                    for bp_tid, activities in rows or []:
                        if bp_tid is None:
                            continue
                        inv = activities.get("invention") if isinstance(activities, dict) else None
                        products = inv.get("products") if isinstance(inv, dict) else None
                        supports_invention_by_bp_type_id[int(bp_tid)] = bool(isinstance(products, list) and len(products) > 0)
                except Exception:
                    supports_invention_by_bp_type_id = {}

            # Prefetch blueprint data for invention computations (SDE) and prices (ESI) once.
            blueprint_data_map_for_invention: dict[int, dict] = {}
            market_price_map_for_invention: dict[int, dict[str, float | None]] = {}
            base_map_for_invention: dict[int, dict] = {}

            try:
                from eve_online_industry_tracker.infrastructure.sde.blueprints import get_blueprint_manufacturing_data

                inventable_bp_type_ids = [tid for tid in bp_type_ids if bool(supports_invention_by_bp_type_id.get(int(tid), False))]
                if inventable_bp_type_ids and sde_session is not None:
                    base_map = get_blueprint_manufacturing_data(sde_session, language, inventable_bp_type_ids) or {}
                    base_map_for_invention = base_map or {}

                    out_bp_type_ids: set[int] = set()
                    for _tid, _bp in (base_map or {}).items():
                        if not isinstance(_bp, dict):
                            continue
                        inv = _bp.get("invention") if isinstance(_bp.get("invention"), dict) else None
                        prods = inv.get("products") if isinstance(inv, dict) else None
                        if not isinstance(prods, list):
                            continue
                        for p in prods:
                            if not isinstance(p, dict):
                                continue
                            out_id = p.get("type_id")
                            if out_id is None:
                                continue
                            try:
                                out_bp_type_ids.add(int(out_id))
                            except Exception:
                                continue

                    out_map = {}
                    if out_bp_type_ids:
                        out_map = get_blueprint_manufacturing_data(sde_session, language, sorted(out_bp_type_ids)) or {}

                    blueprint_data_map_for_invention = {**(base_map or {}), **(out_map or {})}
            except Exception:
                blueprint_data_map_for_invention = {}
                base_map_for_invention = {}

            try:
                market_price_map_for_invention = market_price_map_shared
            except Exception:
                market_price_map_for_invention = {}

            # Inventory-aware valuation for invention summaries (datacores + decryptors).
            # Compute once per update run to avoid per-blueprint DB fan-out.
            inv_inventory_on_hand_by_type: dict[int, int] = {}
            inv_fifo_lots_by_type: dict[int, list] = {}
            use_fifo_for_invention = bool(prefer_inventory_consumption)
            try:
                def _chunks(values: list[int], *, size: int = 900):
                    # SQLite often limits bound params to 999. Keep a little headroom.
                    for i in range(0, len(values), int(size)):
                        yield values[i : i + int(size)]

                inv_type_ids: set[int] = set()
                for _bp in (base_map_for_invention or {}).values():
                    if not isinstance(_bp, dict):
                        continue
                    inv = _bp.get("invention") if isinstance(_bp.get("invention"), dict) else None
                    mats = inv.get("materials") if isinstance(inv, dict) else None
                    for m in mats or []:
                        if not isinstance(m, dict):
                            continue
                        tid = m.get("type_id")
                        if tid is None:
                            continue
                        try:
                            t = int(tid)
                        except Exception:
                            continue
                        if t > 0:
                            inv_type_ids.add(int(t))

                try:
                    from eve_online_industry_tracker.infrastructure.sde.decryptors import get_t2_invention_decryptors

                    for d in get_t2_invention_decryptors(sde_session, language=language) or []:
                        if not isinstance(d, dict):
                            continue
                        tid = d.get("type_id")
                        if tid is None:
                            continue
                        try:
                            t = int(tid)
                        except Exception:
                            continue
                        if t > 0:
                            inv_type_ids.add(int(t))
                except Exception:
                    pass

                if inv_type_ids:
                    char_id = getattr(character, "character_id", None)
                    corp_id = getattr(character, "corporation_id", None)

                    inv_type_ids_list = sorted({int(t) for t in inv_type_ids if t is not None and int(t) > 0})

                    # On-hand quantities (always gathered; FIFO lots are optional).
                    try:
                        if char_id is not None:
                            for chunk in _chunks(inv_type_ids_list):
                                rows = (
                                    session.query(CharacterAssetsModel.type_id, func.sum(CharacterAssetsModel.quantity))
                                    .filter(CharacterAssetsModel.character_id == int(char_id))
                                    .filter(CharacterAssetsModel.type_id.in_(chunk))
                                    .group_by(CharacterAssetsModel.type_id)
                                    .all()
                                )
                                for tid, qty_sum in rows or []:
                                    if tid is None:
                                        continue
                                    inv_inventory_on_hand_by_type[int(tid)] = int(qty_sum or 0)
                    except Exception:
                        pass

                    try:
                        if corp_id is not None:
                            for chunk in _chunks(inv_type_ids_list):
                                rows = (
                                    session.query(CorporationAssetsModel.type_id, func.sum(CorporationAssetsModel.quantity))
                                    .filter(CorporationAssetsModel.corporation_id == int(corp_id))
                                    .filter(CorporationAssetsModel.type_id.in_(chunk))
                                    .group_by(CorporationAssetsModel.type_id)
                                    .all()
                                )
                                for tid, qty_sum in rows or []:
                                    if tid is None:
                                        continue
                                    inv_inventory_on_hand_by_type[int(tid)] = int(
                                        inv_inventory_on_hand_by_type.get(int(tid), 0) or 0
                                    ) + int(qty_sum or 0)
                    except Exception:
                        pass

                    if use_fifo_for_invention:
                        # FIFO lots from wallet transactions + completed jobs.
                        tx_rows: list[Any] = []
                        try:
                            if char_id is not None:
                                for chunk in _chunks(inv_type_ids_list):
                                    tx_rows.extend(
                                        (
                                            session.query(CharacterWalletTransactionsModel)
                                            .filter(CharacterWalletTransactionsModel.character_id == int(char_id))
                                            .filter(CharacterWalletTransactionsModel.type_id.in_(chunk))
                                            .all()
                                        )
                                        or []
                                    )
                        except Exception:
                            pass

                        try:
                            if corp_id is not None:
                                for chunk in _chunks(inv_type_ids_list):
                                    tx_rows.extend(
                                        (
                                            session.query(CorporationWalletTransactionsModel)
                                            .filter(CorporationWalletTransactionsModel.corporation_id == int(corp_id))
                                            .filter(CorporationWalletTransactionsModel.type_id.in_(chunk))
                                            .all()
                                        )
                                        or []
                                    )
                        except Exception:
                            pass

                        job_rows: list[Any] = []
                        try:
                            if char_id is not None:
                                for chunk in _chunks(inv_type_ids_list):
                                    job_rows.extend(
                                        (
                                            session.query(CharacterIndustryJobsModel)
                                            .filter(CharacterIndustryJobsModel.character_id == int(char_id))
                                            .filter(
                                                or_(
                                                    CharacterIndustryJobsModel.product_type_id.in_(chunk),
                                                    CharacterIndustryJobsModel.blueprint_type_id.in_(chunk),
                                                )
                                            )
                                            .all()
                                        )
                                        or []
                                    )
                        except Exception:
                            pass

                        try:
                            if corp_id is not None:
                                for chunk in _chunks(inv_type_ids_list):
                                    job_rows.extend(
                                        (
                                            session.query(CorporationIndustryJobsModel)
                                            .filter(CorporationIndustryJobsModel.corporation_id == int(corp_id))
                                            .filter(
                                                or_(
                                                    CorporationIndustryJobsModel.product_type_id.in_(chunk),
                                                    CorporationIndustryJobsModel.blueprint_type_id.in_(chunk),
                                                )
                                            )
                                            .all()
                                        )
                                        or []
                                    )
                        except Exception:
                            pass

                        try:
                            inv_fifo_lots_by_type = build_fifo_remaining_lots_by_type(
                                wallet_transactions=tx_rows,
                                industry_jobs=job_rows,
                                sde_session=sde_session,
                                market_prices=(market_prices_rows if isinstance(market_prices_rows, list) else None),
                                on_hand_quantities_by_type=inv_inventory_on_hand_by_type,
                            )
                        except Exception:
                            inv_fifo_lots_by_type = {}
            except Exception:
                inv_inventory_on_hand_by_type = {}
                inv_fifo_lots_by_type = {}

            invention_summary_cache: dict[int, dict[str, Any] | None] = {}

            def _trim_facility_context(fc: Any, *, keys: set[str]) -> dict[str, Any] | None:
                if not isinstance(fc, dict):
                    return None
                out = {k: fc.get(k) for k in keys if k in fc}
                return out or None

            for bp in data or []:
                if not isinstance(bp, dict):
                    continue
                tid = bp.get("type_id")
                try:
                    tid_i = int(tid) if tid is not None else 0
                except Exception:
                    tid_i = 0
                if tid_i <= 0:
                    continue

                if tid_i in invention_summary_cache:
                    cached_summary = invention_summary_cache.get(tid_i)
                    if isinstance(cached_summary, dict):
                        bp["invention_best_summary"] = cached_summary
                        ui_row = compute_ui_invention_overview_row_from_summary(
                            bp=bp,
                            invention_best_summary=cached_summary,
                            pricing_preferences=(pricing_preferences if isinstance(pricing_preferences, dict) else None),
                        )
                        if isinstance(ui_row, dict):
                            bp["ui_invention_overview_row"] = ui_row
                    continue

                if not bool(supports_invention_by_bp_type_id.get(tid_i, False)):
                    invention_summary_cache[tid_i] = None
                    continue

                try:
                    inv_data, _inv_meta = compute_invention_options_for_blueprint(
                        sde_session=sde_session,
                        esi_service=self._state.esi_service,
                        language=language,
                        blueprint_type_id=int(tid_i),
                        character_skills=((character.skills or {}).get("skills") if isinstance(character.skills, dict) else None),
                        industry_profile=selected_profile,
                        rig_payload=rig_payload,
                        manufacturing_system_cost_index=float(manufacturing_ci),
                        invention_system_cost_index=float(invention_ci),
                        copying_system_cost_index=float(copying_ci) if copying_ci is not None else None,
                        blueprint_data_map=blueprint_data_map_for_invention,
                        market_price_map=market_price_map_for_invention,
                        inventory_on_hand_by_type=inv_inventory_on_hand_by_type,
                        inventory_fifo_lots_by_type=inv_fifo_lots_by_type,
                        use_fifo_inventory_costing=use_fifo_for_invention,
                    )

                    opts = inv_data.get("options") if isinstance(inv_data, dict) else None
                    best = next((o for o in (opts or []) if isinstance(o, dict)), None)
                    mfg = inv_data.get("manufacturing") if isinstance(inv_data, dict) else None
                    inv = inv_data.get("invention") if isinstance(inv_data, dict) else None
                    if not isinstance(best, dict) or not isinstance(mfg, dict) or not isinstance(inv, dict):
                        invention_summary_cache[tid_i] = None
                        continue

                    best_keep = {
                        "decryptor_type_id",
                        "decryptor_type_name",
                        "success_probability",
                        "invented_blueprint_type_id",
                        "invented_runs",
                        "invented_me",
                        "invented_te",
                        "invention_attempt_material_cost_isk",
                        "invention_job_fee_isk",
                        "copying_expected_runs",
                        "copying_expected_time_seconds",
                        "copying_job_fee_isk",
                        "manufacturing_material_cost_per_run_isk",
                        "manufacturing_job_fee_per_run_isk",
                        "manufacturing_revenue_per_run_isk",
                        "net_profit_per_run_after_invention_isk",
                        "roi_percent",
                    }
                    mfg_keep = {
                        "product_type_id",
                        "product_type_name",
                        "product_category_name",
                        "product_quantity_per_run",
                        "time_seconds",
                    }
                    inv_keep = {"time_seconds"}

                    mfg_fc = _trim_facility_context(
                        mfg.get("facility_context"),
                        keys={
                            "estimated_time_seconds_per_run",
                        },
                    )
                    inv_fc = _trim_facility_context(
                        inv.get("facility_context"),
                        keys={
                            "estimated_time_seconds",
                        },
                    )

                    summary = {
                        "best_option": {k: best.get(k) for k in best_keep if k in best},
                        "manufacturing": {
                            **{k: mfg.get(k) for k in mfg_keep if k in mfg},
                            "facility_context": mfg_fc,
                        },
                        "invention": {
                            **{k: inv.get(k) for k in inv_keep if k in inv},
                            "facility_context": inv_fc,
                        },
                    }

                    invention_summary_cache[tid_i] = summary
                    bp["invention_best_summary"] = summary

                    ui_row = compute_ui_invention_overview_row_from_summary(
                        bp=bp,
                        invention_best_summary=summary,
                        pricing_preferences=(pricing_preferences if isinstance(pricing_preferences, dict) else None),
                    )
                    if isinstance(ui_row, dict):
                        bp["ui_invention_overview_row"] = ui_row
                except Exception:
                    invention_summary_cache[tid_i] = None
                    continue

            meta = {
                "profile_id": (int(getattr(selected_profile, "id")) if selected_profile is not None else None),
                "profile_name": (getattr(selected_profile, "profile_name", None) if selected_profile is not None else None),
                "facility_id": int(facility_id_for_cost_index) if facility_id_for_cost_index is not None else None,
                "system_id_for_cost_index": int(system_id_for_cost_index) if system_id_for_cost_index is not None else None,
                "system_id_for_cost_index_source": system_id_for_cost_index_source,
                "system_cost_indices": {
                    "manufacturing": float(manufacturing_ci),
                    "invention": float(invention_ci),
                    "copying": float(copying_ci),
                    "research_me": float(research_me_ci),
                    "research_te": float(research_te_ci),
                },
                "pricing_preferences": (pricing_preferences if isinstance(pricing_preferences, dict) else None),
                "assume_bpo_copy_overhead": bool(assume_bpo_copy_overhead),
            }

            with self._state.industry_builder_jobs_lock:
                job = self._state.industry_builder_jobs.get(job_id)
                if isinstance(job, dict):
                    job["status"] = "done"
                    job["data"] = data
                    job["meta"] = meta
                    job["updated_at"] = datetime.utcnow()

        except Exception as e:
            with self._state.industry_builder_jobs_lock:
                job = self._state.industry_builder_jobs.get(job_id)
                if isinstance(job, dict):
                    job["status"] = "error"
                    job["error"] = str(e)
                    job["updated_at"] = datetime.utcnow()

        finally:
            # Best-effort: return connections to the pool promptly.
            try:
                if session is not None and hasattr(session, "close"):
                    session.close()
            except Exception:
                pass
            try:
                if sde_session is not None and hasattr(sde_session, "close"):
                    sde_session.close()
            except Exception:
                pass

    def start_industry_builder_update(self, *, character_id: int, payload: dict) -> dict:
        self._cleanup_old_industry_builder_jobs()

        character = self._state.char_manager.get_character_by_id(int(character_id))
        if not character:
            raise ServiceError(f"Character ID {character_id} not found", status_code=400)

        profile_id = payload.get("profile_id")
        try:
            profile_id_i = int(profile_id) if profile_id is not None else None
        except Exception:
            profile_id_i = None
        maximize_runs = bool(payload.get("maximize_runs", False))

        prefer_inventory_consumption = bool(payload.get("prefer_inventory_consumption", False))

        assume_bpo_copy_overhead = bool(payload.get("assume_bpo_copy_overhead", False))

        pricing_preferences = payload.get("pricing_preferences")
        if not isinstance(pricing_preferences, dict):
            pricing_preferences = None

        if isinstance(pricing_preferences, dict):
            try:
                depth_i = int(pricing_preferences.get("orderbook_depth") or 5)
            except Exception:
                depth_i = 5
            depth_i = max(1, min(depth_i, 20))

            smoothing = str(pricing_preferences.get("orderbook_smoothing") or "median_best_n").strip().lower()
            if smoothing not in {"median_best_n", "mean_best_n"}:
                smoothing = "median_best_n"

            pricing_key = (
                f"{pricing_preferences.get('hub','jita')}:"
                f"{pricing_preferences.get('material_price_source','jita_buy')}:"
                f"{pricing_preferences.get('product_price_source','jita_sell')}:"
                f"{smoothing}:"
                f"depth{depth_i}"
            )
        else:
            pricing_key = "default"

        key = self._industry_builder_job_key(
            character_id=int(character_id),
            profile_id=profile_id_i,
            maximize_runs=maximize_runs,
            pricing_key=str(pricing_key),
            prefer_inventory_consumption=bool(prefer_inventory_consumption),
            assume_bpo_copy_overhead=bool(assume_bpo_copy_overhead),
        )

        with self._state.industry_builder_jobs_lock:
            existing_job_id = self._state.industry_builder_jobs_by_key.get(key)
            if existing_job_id:
                existing = self._state.industry_builder_jobs.get(existing_job_id)
                if isinstance(existing, dict) and existing.get("status") in {"running", "done"}:
                    return {"job_id": str(existing_job_id)}

            job_id = uuid.uuid4().hex
            self._state.industry_builder_jobs_by_key[key] = job_id
            self._state.industry_builder_jobs[job_id] = {
                "status": "running",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "character_id": int(character_id),
                "profile_id": (int(profile_id_i) if profile_id_i is not None else None),
                "maximize_runs": bool(maximize_runs),
                "prefer_inventory_consumption": bool(prefer_inventory_consumption),
                "assume_bpo_copy_overhead": bool(assume_bpo_copy_overhead),
                "progress_done": 0,
                "progress_total": None,
                "error": None,
                "meta": None,
                "data": None,
            }

        t = threading.Thread(
            target=self._run_industry_builder_update_job,
            kwargs={
                "job_id": job_id,
                "character_id": int(character_id),
                "profile_id": profile_id_i,
                "maximize_runs": bool(maximize_runs),
                "pricing_preferences": (pricing_preferences if isinstance(pricing_preferences, dict) else None),
                "prefer_inventory_consumption": bool(prefer_inventory_consumption),
                "assume_bpo_copy_overhead": bool(assume_bpo_copy_overhead),
            },
            daemon=True,
        )
        t.start()

        return {"job_id": str(job_id)}

    def industry_builder_update_status(self, *, job_id: str) -> dict:
        with self._state.industry_builder_jobs_lock:
            job = self._state.industry_builder_jobs.get(str(job_id))
            if not isinstance(job, dict):
                raise ServiceError("Job not found", status_code=404)
            return {
                "job_id": str(job_id),
                "status": job.get("status"),
                "progress_done": job.get("progress_done"),
                "progress_total": job.get("progress_total"),
                "error": job.get("error"),
            }

    def industry_builder_update_result(self, *, job_id: str) -> tuple[Any, Any]:
        with self._state.industry_builder_jobs_lock:
            job = self._state.industry_builder_jobs.get(str(job_id))
            if not isinstance(job, dict):
                raise ServiceError("Job not found", status_code=404)
            if job.get("status") == "error":
                raise ServiceError(str(job.get("error") or "Job failed"), status_code=500)
            if job.get("status") != "done":
                raise ServiceError("Job not finished", status_code=409)

            data = job.get("data")
            meta = job.get("meta")

        return data, meta

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

    def industry_builder_data(
        self,
        *,
        character_id: int,
        profile_id: int | None,
        maximize_runs: bool,
        include_submanufacturing: bool,
        submanufacturing_blueprint_type_id: int | None,
    ) -> tuple[Any, dict]:
        character = self._state.char_manager.get_character_by_id(character_id)
        if not character:
            raise ServiceError(f"Character ID {character_id} not found", status_code=400)

        session: Any = self._sessions.app_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"

        sde_session = None
        try:
            sde_session: Any = self._sessions.sde_session()
        except Exception:
            sde_session = None

        selected_profile = None
        if profile_id:
            selected_profile = industry_profile_get_by_id(session, int(profile_id))
            if selected_profile and int(selected_profile.character_id) != int(character_id):
                raise ServiceError("Industry profile does not belong to this character.", status_code=400)
        else:
            selected_profile = industry_profile_get_default_for_character_id(session, int(character_id))

        manufacturing_ci = 0.0
        copying_ci = 0.0
        research_me_ci = 0.0
        research_te_ci = 0.0
        if selected_profile is not None and getattr(selected_profile, "system_id", None) is not None:
            if self._state.esi_service is not None:
                try:
                    systems = self._state.esi_service.get_industry_systems()
                    sid = int(getattr(selected_profile, "system_id"))
                    row = next((s for s in systems if s.get("solar_system_id") == sid), None)
                    if row:
                        for entry in (row.get("cost_indices") or []):
                            if entry.get("activity") == "manufacturing":
                                manufacturing_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "copying":
                                copying_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_material_efficiency":
                                research_me_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_time_efficiency":
                                research_te_ci = float(entry.get("cost_index") or 0.0)
                except Exception:
                    manufacturing_ci = 0.0
                    copying_ci = 0.0
                    research_me_ci = 0.0
                    research_te_ci = 0.0

        surcharge_rate = 0.0
        if selected_profile is not None:
            try:
                facility_tax = float(getattr(selected_profile, "facility_tax", 0.0) or 0.0)
            except Exception:
                facility_tax = 0.0
            try:
                scc_surcharge = float(getattr(selected_profile, "scc_surcharge", 0.0) or 0.0)
            except Exception:
                scc_surcharge = 0.0
            if facility_tax >= 1.0:
                facility_tax = facility_tax / 100.0
            if scc_surcharge >= 1.0:
                scc_surcharge = scc_surcharge / 100.0
            surcharge_rate = max(0.0, facility_tax + scc_surcharge)

        owned_bp_type_ids: set[int] = set()
        owned_bp_best_by_type_id: dict[int, dict] = {}

        def _consider_owned_bp(
            *,
            blueprint_type_id: int,
            is_blueprint_copy: bool,
            me_percent: int | None,
            te_percent: int | None,
            runs: int | None,
        ) -> None:
            tid = int(blueprint_type_id)
            if tid <= 0:
                return
            owned_bp_type_ids.add(tid)

            me_i = 0
            te_i = 0
            try:
                me_i = int(me_percent or 0)
            except Exception:
                me_i = 0
            try:
                te_i = int(te_percent or 0)
            except Exception:
                te_i = 0

            rec = {
                "is_blueprint_copy": bool(is_blueprint_copy),
                "me_percent": int(me_i),
                "te_percent": int(te_i),
                "runs": (int(runs) if runs is not None else None),
            }

            cur = owned_bp_best_by_type_id.get(tid)
            if not isinstance(cur, dict):
                owned_bp_best_by_type_id[tid] = rec
                return

            cur_is_bpc = bool(cur.get("is_blueprint_copy"))
            new_is_bpc = bool(rec.get("is_blueprint_copy"))
            if new_is_bpc and not cur_is_bpc:
                owned_bp_best_by_type_id[tid] = rec
                return
            if new_is_bpc == cur_is_bpc:
                cur_me = int(cur.get("me_percent") or 0)
                cur_te = int(cur.get("te_percent") or 0)
                new_me = int(rec.get("me_percent") or 0)
                new_te = int(rec.get("te_percent") or 0)
                if (new_me > cur_me) or (new_me == cur_me and new_te > cur_te):
                    owned_bp_best_by_type_id[tid] = rec

        try:
            rows = (
                session.query(
                    CharacterAssetsModel.type_id,
                    CharacterAssetsModel.is_blueprint_copy,
                    CharacterAssetsModel.blueprint_material_efficiency,
                    CharacterAssetsModel.blueprint_time_efficiency,
                    CharacterAssetsModel.blueprint_runs,
                )
                .filter(
                    CharacterAssetsModel.type_category_name == "Blueprint",
                    CharacterAssetsModel.character_id == int(character_id),
                )
                .all()
            )
            for tid, is_bpc, me, te, runs in rows or []:
                if tid is None:
                    continue
                _consider_owned_bp(
                    blueprint_type_id=int(tid),
                    is_blueprint_copy=bool(is_bpc),
                    me_percent=(int(me) if me is not None else None),
                    te_percent=(int(te) if te is not None else None),
                    runs=(int(runs) if runs is not None else None),
                )
        except Exception:
            pass

        try:
            corp_id = getattr(character, "corporation_id", None)
            if corp_id is not None:
                rows = (
                    session.query(
                        CorporationAssetsModel.type_id,
                        CorporationAssetsModel.is_blueprint_copy,
                        CorporationAssetsModel.blueprint_material_efficiency,
                        CorporationAssetsModel.blueprint_time_efficiency,
                        CorporationAssetsModel.blueprint_runs,
                    )
                    .filter(
                        CorporationAssetsModel.type_category_name == "Blueprint",
                        CorporationAssetsModel.corporation_id == int(corp_id),
                    )
                    .all()
                )
                for tid, is_bpc, me, te, runs in rows or []:
                    if tid is None:
                        continue
                    _consider_owned_bp(
                        blueprint_type_id=int(tid),
                        is_blueprint_copy=bool(is_bpc),
                        me_percent=(int(me) if me is not None else None),
                        te_percent=(int(te) if te is not None else None),
                        runs=(int(runs) if runs is not None else None),
                    )
        except Exception:
            pass

        rig_payload: list[dict] = []
        try:
            if selected_profile is not None:
                rig_ids = [
                    getattr(selected_profile, "rig_slot0_type_id", None),
                    getattr(selected_profile, "rig_slot1_type_id", None),
                    getattr(selected_profile, "rig_slot2_type_id", None),
                ]
                rig_ids = [int(x) for x in rig_ids if x is not None and int(x) != 0]
                if rig_ids:
                    sde_session = self._sessions.sde_session()
                    rig_payload = get_rig_effects_for_type_ids(sde_session, rig_ids)
        except Exception:
            rig_payload = []

        if sde_session is None:
            sde_session = self._sessions.sde_session()

        all_blueprints = get_blueprint_assets(
            session,
            self._state.esi_service,
            sde_session=sde_session,
            language=language,
            include_unowned=False,
        )
        if character_id:
            all_blueprints = [bp for bp in all_blueprints if bp.get("owner_id") == character_id]

        data = enrich_blueprints_for_character(
            all_blueprints,
            character,
            esi_service=self._state.esi_service,
            industry_profile=selected_profile,
            manufacturing_system_cost_index=manufacturing_ci,
            copying_system_cost_index=copying_ci,
            research_me_system_cost_index=research_me_ci,
            research_te_system_cost_index=research_te_ci,
            surcharge_rate_total_fraction=surcharge_rate,
            owned_blueprint_type_ids=owned_bp_type_ids,
            owned_blueprint_best_by_type_id=owned_bp_best_by_type_id,
            include_submanufacturing=include_submanufacturing,
            submanufacturing_blueprint_type_id=(int(submanufacturing_blueprint_type_id) if submanufacturing_blueprint_type_id else None),
            maximize_blueprint_runs=maximize_runs,
            rig_payload=rig_payload,
            db_app_session=session,
            db_sde_session=sde_session,
            language=language,
        )

        meta = {
            "profile_id": (int(getattr(selected_profile, "id")) if selected_profile is not None else None),
            "profile_name": (getattr(selected_profile, "profile_name", None) if selected_profile is not None else None),
        }

        return data, meta

    def industry_submanufacturing_plan(self, *, character_id: int, payload: dict) -> tuple[Any, dict]:
        if not character_id:
            raise ServiceError("Character ID is required.", status_code=400)

        character = self._state.char_manager.get_character_by_id(character_id)
        if not character:
            raise ServiceError(f"Character ID {character_id} not found", status_code=400)

        materials = payload.get("materials") or []
        if not isinstance(materials, list) or not materials:
            raise ServiceError("Request must include a non-empty 'materials' list.", status_code=400)

        profile_id = payload.get("profile_id")
        try:
            profile_id_i = int(profile_id) if profile_id is not None else None
        except Exception:
            profile_id_i = None

        max_depth = payload.get("max_depth")
        try:
            max_depth_i = int(max_depth) if max_depth is not None else 3
        except Exception:
            max_depth_i = 3
        max_depth_i = max(1, min(max_depth_i, 50))

        session: Any = self._sessions.app_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"

        selected_profile = None
        if profile_id_i:
            selected_profile = industry_profile_get_by_id(session, int(profile_id_i))
            if selected_profile and int(getattr(selected_profile, "character_id", 0) or 0) != int(character_id):
                raise ServiceError("Industry profile does not belong to this character.", status_code=400)
        else:
            selected_profile = industry_profile_get_default_for_character_id(session, int(character_id))

        manufacturing_ci = 0.0
        copying_ci = 0.0
        research_me_ci = 0.0
        research_te_ci = 0.0
        if selected_profile is not None and getattr(selected_profile, "system_id", None) is not None:
            if self._state.esi_service is not None:
                try:
                    systems = self._state.esi_service.get_industry_systems()
                    sid = int(getattr(selected_profile, "system_id"))
                    row = next((s for s in systems if s.get("solar_system_id") == sid), None)
                    if row:
                        for entry in (row.get("cost_indices") or []):
                            if entry.get("activity") == "manufacturing":
                                manufacturing_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "copying":
                                copying_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_material_efficiency":
                                research_me_ci = float(entry.get("cost_index") or 0.0)
                            elif entry.get("activity") == "researching_time_efficiency":
                                research_te_ci = float(entry.get("cost_index") or 0.0)
                except Exception:
                    manufacturing_ci = 0.0
                    copying_ci = 0.0
                    research_me_ci = 0.0
                    research_te_ci = 0.0

        surcharge_rate = 0.0
        if selected_profile is not None:
            try:
                facility_tax = float(getattr(selected_profile, "facility_tax", 0.0) or 0.0)
            except Exception:
                facility_tax = 0.0
            try:
                scc_surcharge = float(getattr(selected_profile, "scc_surcharge", 0.0) or 0.0)
            except Exception:
                scc_surcharge = 0.0
            if facility_tax >= 1.0:
                facility_tax = facility_tax / 100.0
            if scc_surcharge >= 1.0:
                scc_surcharge = scc_surcharge / 100.0
            surcharge_rate = max(0.0, facility_tax + scc_surcharge)

        sde_session: Any = self._sessions.sde_session()

        owned_bp_type_ids: set[int] = set()
        owned_bp_best_by_type_id: dict[int, dict] = {}

        def _consider_owned_bp(
            *,
            blueprint_type_id: int,
            is_blueprint_copy: bool,
            me_percent: int | None,
            te_percent: int | None,
            runs: int | None,
        ) -> None:
            tid = int(blueprint_type_id)
            if tid <= 0:
                return
            owned_bp_type_ids.add(tid)

            me_i = 0
            te_i = 0
            try:
                me_i = int(me_percent or 0)
            except Exception:
                me_i = 0
            try:
                te_i = int(te_percent or 0)
            except Exception:
                te_i = 0

            rec = {
                "is_blueprint_copy": bool(is_blueprint_copy),
                "me_percent": int(me_i),
                "te_percent": int(te_i),
                "runs": (int(runs) if runs is not None else None),
            }

            cur = owned_bp_best_by_type_id.get(tid)
            if not isinstance(cur, dict):
                owned_bp_best_by_type_id[tid] = rec
                return

            cur_is_bpc = bool(cur.get("is_blueprint_copy"))
            new_is_bpc = bool(rec.get("is_blueprint_copy"))
            if new_is_bpc and not cur_is_bpc:
                owned_bp_best_by_type_id[tid] = rec
                return
            if new_is_bpc == cur_is_bpc:
                cur_me = int(cur.get("me_percent") or 0)
                cur_te = int(cur.get("te_percent") or 0)
                new_me = int(rec.get("me_percent") or 0)
                new_te = int(rec.get("te_percent") or 0)
                if (new_me > cur_me) or (new_me == cur_me and new_te > cur_te):
                    owned_bp_best_by_type_id[tid] = rec

        try:
            rows = (
                session.query(
                    CharacterAssetsModel.type_id,
                    CharacterAssetsModel.is_blueprint_copy,
                    CharacterAssetsModel.blueprint_material_efficiency,
                    CharacterAssetsModel.blueprint_time_efficiency,
                    CharacterAssetsModel.blueprint_runs,
                )
                .filter(
                    CharacterAssetsModel.type_category_name == "Blueprint",
                    CharacterAssetsModel.character_id == int(character_id),
                )
                .all()
            )
            for tid, is_bpc, me, te, runs in rows or []:
                if tid is None:
                    continue
                _consider_owned_bp(
                    blueprint_type_id=int(tid),
                    is_blueprint_copy=bool(is_bpc),
                    me_percent=(int(me) if me is not None else None),
                    te_percent=(int(te) if te is not None else None),
                    runs=(int(runs) if runs is not None else None),
                )
        except Exception:
            pass

        try:
            corp_id = getattr(character, "corporation_id", None)
            if corp_id is not None:
                rows = (
                    session.query(
                        CorporationAssetsModel.type_id,
                        CorporationAssetsModel.is_blueprint_copy,
                        CorporationAssetsModel.blueprint_material_efficiency,
                        CorporationAssetsModel.blueprint_time_efficiency,
                        CorporationAssetsModel.blueprint_runs,
                    )
                    .filter(
                        CorporationAssetsModel.type_category_name == "Blueprint",
                        CorporationAssetsModel.corporation_id == int(corp_id),
                    )
                    .all()
                )
                for tid, is_bpc, me, te, runs in rows or []:
                    if tid is None:
                        continue
                    _consider_owned_bp(
                        blueprint_type_id=int(tid),
                        is_blueprint_copy=bool(is_bpc),
                        me_percent=(int(me) if me is not None else None),
                        te_percent=(int(te) if te is not None else None),
                        runs=(int(runs) if runs is not None else None),
                    )
        except Exception:
            pass

        material_type_ids: set[int] = set()
        for m in materials or []:
            if not isinstance(m, dict):
                continue
            tid = m.get("type_id")
            if tid is None:
                continue
            try:
                tid_i = int(tid)
            except Exception:
                continue
            if tid_i > 0:
                material_type_ids.add(tid_i)

        inventory_on_hand_by_type: dict[int, int] = {}
        fifo_lots_by_type: dict[int, list] = {}
        market_price_map: dict[int, dict[str, float | None]] = {}

        if material_type_ids:
            try:
                rows = (
                    session.query(CharacterAssetsModel.type_id, func.sum(CharacterAssetsModel.quantity))
                    .filter(CharacterAssetsModel.character_id == int(character_id))
                    .filter(CharacterAssetsModel.type_id.in_(sorted(material_type_ids)))
                    .group_by(CharacterAssetsModel.type_id)
                    .all()
                )
                for tid, qty_sum in rows or []:
                    if tid is None:
                        continue
                    try:
                        inventory_on_hand_by_type[int(tid)] = int(qty_sum or 0)
                    except Exception:
                        continue
            except Exception:
                inventory_on_hand_by_type = inventory_on_hand_by_type or {}

            try:
                corp_id = getattr(character, "corporation_id", None)
                if corp_id is not None:
                    rows = (
                        session.query(CorporationAssetsModel.type_id, func.sum(CorporationAssetsModel.quantity))
                        .filter(CorporationAssetsModel.corporation_id == int(corp_id))
                        .filter(CorporationAssetsModel.type_id.in_(sorted(material_type_ids)))
                        .group_by(CorporationAssetsModel.type_id)
                        .all()
                    )
                    for tid, qty_sum in rows or []:
                        if tid is None:
                            continue
                        try:
                            inventory_on_hand_by_type[int(tid)] = int(inventory_on_hand_by_type.get(int(tid), 0) or 0) + int(
                                qty_sum or 0
                            )
                        except Exception:
                            continue
            except Exception:
                pass

            tx_rows: list[object] = []
            try:
                tx_rows.extend(
                    (
                        session.query(CharacterWalletTransactionsModel)
                        .filter(CharacterWalletTransactionsModel.character_id == int(character_id))
                        .filter(CharacterWalletTransactionsModel.type_id.in_(sorted(material_type_ids)))
                        .all()
                    )
                    or []
                )
            except Exception:
                pass

            try:
                corp_id = getattr(character, "corporation_id", None)
                if corp_id is not None:
                    tx_rows.extend(
                        (
                            session.query(CorporationWalletTransactionsModel)
                            .filter(CorporationWalletTransactionsModel.corporation_id == int(corp_id))
                            .filter(CorporationWalletTransactionsModel.type_id.in_(sorted(material_type_ids)))
                            .all()
                        )
                        or []
                    )
            except Exception:
                pass

            try:
                job_rows: list[object] = []

                try:
                    job_rows.extend(
                        (
                            session.query(CharacterIndustryJobsModel)
                            .filter(CharacterIndustryJobsModel.character_id == int(character_id))
                            .filter(
                                or_(
                                    CharacterIndustryJobsModel.product_type_id.in_(sorted(material_type_ids)),
                                    CharacterIndustryJobsModel.blueprint_type_id.in_(sorted(material_type_ids)),
                                )
                            )
                            .all()
                        )
                        or []
                    )
                except Exception:
                    job_rows = job_rows or []

                try:
                    corp_id = getattr(character, "corporation_id", None)
                    if corp_id is not None:
                        job_rows.extend(
                            (
                                session.query(CorporationIndustryJobsModel)
                                .filter(CorporationIndustryJobsModel.corporation_id == int(corp_id))
                                .filter(
                                    or_(
                                        CorporationIndustryJobsModel.product_type_id.in_(sorted(material_type_ids)),
                                        CorporationIndustryJobsModel.blueprint_type_id.in_(sorted(material_type_ids)),
                                    )
                                )
                                .all()
                            )
                            or []
                        )
                except Exception:
                    pass

                market_prices: list[dict] | None = None
                try:
                    if self._state.esi_service is not None:
                        market_prices = (self._state.esi_service.get_market_prices() or [])
                except Exception:
                    market_prices = None

                # Build a price map once; the planner can reuse it.
                try:
                    for row in market_prices or []:
                        if not isinstance(row, dict):
                            continue
                        tid = row.get("type_id")
                        if tid is None:
                            continue
                        try:
                            type_id = int(tid)
                        except Exception:
                            continue

                        avg = row.get("average_price")
                        adj = row.get("adjusted_price")
                        try:
                            avg_f = float(avg) if avg is not None else None
                        except Exception:
                            avg_f = None
                        try:
                            adj_f = float(adj) if adj is not None else None
                        except Exception:
                            adj_f = None

                        market_price_map[type_id] = {
                            "average_price": avg_f,
                            "adjusted_price": adj_f,
                        }
                except Exception:
                    market_price_map = market_price_map or {}

                fifo_lots_by_type = build_fifo_remaining_lots_by_type(
                    wallet_transactions=tx_rows,
                    industry_jobs=job_rows,
                    sde_session=sde_session,
                    market_prices=market_prices,
                    on_hand_quantities_by_type=inventory_on_hand_by_type,
                )
            except Exception:
                fifo_lots_by_type = {}

        plan = plan_submanufacturing_tree(
            sde_session=sde_session,
            language=language,
            esi_service=self._state.esi_service,
            materials=materials,
            owned_blueprint_type_ids=owned_bp_type_ids,
            owned_blueprint_best_by_type_id=owned_bp_best_by_type_id,
            manufacturing_system_cost_index=manufacturing_ci,
            copying_system_cost_index=copying_ci,
            research_me_system_cost_index=research_me_ci,
            research_te_system_cost_index=research_te_ci,
            surcharge_rate_total_fraction=surcharge_rate,
            inventory_on_hand_by_type=inventory_on_hand_by_type,
            inventory_fifo_lots_by_type=fifo_lots_by_type,
            use_fifo_inventory_costing=True,
            max_depth=max_depth_i,
            price_map=market_price_map,
        )

        meta = {
            "profile_id": (int(getattr(selected_profile, "id")) if selected_profile is not None else None),
            "profile_name": (getattr(selected_profile, "profile_name", None) if selected_profile is not None else None),
            "manufacturing_system_cost_index": float(manufacturing_ci),
            "copying_system_cost_index": float(copying_ci),
            "research_me_system_cost_index": float(research_me_ci),
            "research_te_system_cost_index": float(research_te_ci),
            "surcharge_rate_total_fraction": float(surcharge_rate),
            "max_depth": int(max_depth_i),
        }

        return plan, meta

    def industry_invention_options(
        self,
        *,
        character_id: int,
        blueprint_type_id: int,
        payload: dict,
    ) -> tuple[Any, dict]:
        if not character_id:
            raise ServiceError("Character ID is required.", status_code=400)
        if not blueprint_type_id:
            raise ServiceError("Blueprint type ID is required.", status_code=400)

        character = self._state.char_manager.get_character_by_id(int(character_id))
        if not character:
            raise ServiceError(f"Character ID {character_id} not found", status_code=400)

        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        sde_session: Any = self._sessions.sde_session()

        profile_id = payload.get("profile_id") if isinstance(payload, dict) else None

        selected_profile = None
        rig_payload = None
        invention_ci = None
        manufacturing_ci = None
        copying_ci = None

        if profile_id is not None:
            try:
                profile_id_i = int(profile_id)
            except Exception:
                profile_id_i = None

            if profile_id_i is not None and profile_id_i > 0:
                try:
                    app_session: Any = self._sessions.app_session()
                    selected_profile = industry_profile_get_by_id(app_session, int(profile_id_i))
                except Exception:
                    selected_profile = None

        # Resolve rig effects from fitted rig slots (structure rigs).
        if selected_profile is not None:
            try:
                from eve_online_industry_tracker.infrastructure.sde.rig_effects import get_rig_effects_for_type_ids

                rig_ids: list[int] = []
                for attr in ["rig_slot0_type_id", "rig_slot1_type_id", "rig_slot2_type_id"]:
                    v = getattr(selected_profile, attr, None)
                    if v is None:
                        continue
                    try:
                        tid = int(v)
                    except Exception:
                        continue
                    if tid > 0:
                        rig_ids.append(int(tid))

                rig_payload = get_rig_effects_for_type_ids(sde_session, rig_ids)
            except Exception:
                rig_payload = None

        # Live system cost indices for invention/manufacturing.
        if selected_profile is not None and self._state.esi_service is not None:
            system_id = getattr(selected_profile, "system_id", None)
            try:
                system_id_i = int(system_id) if system_id is not None else None
            except Exception:
                system_id_i = None

            if system_id_i is not None and system_id_i > 0:
                try:
                    systems = self._state.esi_service.get_industry_systems() or []
                    row = next((s for s in systems if s.get("solar_system_id") == int(system_id_i)), None)
                    cost_indices = (row or {}).get("cost_indices") if isinstance(row, dict) else None
                    if isinstance(cost_indices, list):
                        for entry in cost_indices:
                            if not isinstance(entry, dict):
                                continue
                            act = str(entry.get("activity") or "")
                            if act == "invention":
                                invention_ci = float(entry.get("cost_index") or 0.0)
                            elif act == "manufacturing":
                                manufacturing_ci = float(entry.get("cost_index") or 0.0)
                            elif act == "copying":
                                copying_ci = float(entry.get("cost_index") or 0.0)
                except Exception:
                    invention_ci = None
                    manufacturing_ci = None
                    copying_ci = None

        # Inventory-aware valuation for invention inputs (datacores + decryptors).
        # This mirrors the manufacturing materials logic (FIFO lots + on-hand first).
        inventory_on_hand_by_type: dict[int, int] = {}
        fifo_lots_by_type: dict[int, list] = {}
        try:
            app_session: Any = self._sessions.app_session()

            # Determine relevant type IDs from SDE: invention materials + decryptors.
            bp_map = get_blueprint_manufacturing_data(sde_session, language, [int(blueprint_type_id)])
            bp = bp_map.get(int(blueprint_type_id)) if isinstance(bp_map, dict) else None
            inv = bp.get("invention") if isinstance(bp, dict) else None
            inv_mats = (inv.get("materials") if isinstance(inv, dict) else None) or []
            type_ids: set[int] = set()
            for m in inv_mats or []:
                if not isinstance(m, dict):
                    continue
                tid = m.get("type_id")
                if tid is None:
                    continue
                try:
                    t = int(tid)
                except Exception:
                    continue
                if t > 0:
                    type_ids.add(int(t))

            try:
                from eve_online_industry_tracker.infrastructure.sde.decryptors import get_t2_invention_decryptors

                for d in get_t2_invention_decryptors(sde_session, language=language) or []:
                    if not isinstance(d, dict):
                        continue
                    tid = d.get("type_id")
                    if tid is None:
                        continue
                    try:
                        t = int(tid)
                    except Exception:
                        continue
                    if t > 0:
                        type_ids.add(int(t))
            except Exception:
                pass

            if type_ids:
                char_id = getattr(character, "character_id", None)
                corp_id = getattr(character, "corporation_id", None)

                # On-hand quantities.
                try:
                    if char_id is not None:
                        rows = (
                            app_session.query(CharacterAssetsModel.type_id, func.sum(CharacterAssetsModel.quantity))
                            .filter(CharacterAssetsModel.character_id == int(char_id))
                            .filter(CharacterAssetsModel.type_id.in_(sorted(type_ids)))
                            .group_by(CharacterAssetsModel.type_id)
                            .all()
                        )
                        for tid, qty_sum in rows or []:
                            if tid is None:
                                continue
                            inventory_on_hand_by_type[int(tid)] = int(qty_sum or 0)
                except Exception:
                    pass

                try:
                    if corp_id is not None:
                        rows = (
                            app_session.query(CorporationAssetsModel.type_id, func.sum(CorporationAssetsModel.quantity))
                            .filter(CorporationAssetsModel.corporation_id == int(corp_id))
                            .filter(CorporationAssetsModel.type_id.in_(sorted(type_ids)))
                            .group_by(CorporationAssetsModel.type_id)
                            .all()
                        )
                        for tid, qty_sum in rows or []:
                            if tid is None:
                                continue
                            inventory_on_hand_by_type[int(tid)] = int(inventory_on_hand_by_type.get(int(tid), 0) or 0) + int(
                                qty_sum or 0
                            )
                except Exception:
                    pass

                # Wallet transactions + completed jobs for FIFO lot reconstruction.
                tx_rows: list[Any] = []
                try:
                    if char_id is not None:
                        tx_rows.extend(
                            (
                                app_session.query(CharacterWalletTransactionsModel)
                                .filter(CharacterWalletTransactionsModel.character_id == int(char_id))
                                .filter(CharacterWalletTransactionsModel.type_id.in_(sorted(type_ids)))
                                .all()
                            )
                            or []
                        )
                except Exception:
                    pass

                try:
                    if corp_id is not None:
                        tx_rows.extend(
                            (
                                app_session.query(CorporationWalletTransactionsModel)
                                .filter(CorporationWalletTransactionsModel.corporation_id == int(corp_id))
                                .filter(CorporationWalletTransactionsModel.type_id.in_(sorted(type_ids)))
                                .all()
                            )
                            or []
                        )
                except Exception:
                    pass

                job_rows: list[Any] = []
                try:
                    if char_id is not None:
                        job_rows.extend(
                            (
                                app_session.query(CharacterIndustryJobsModel)
                                .filter(CharacterIndustryJobsModel.character_id == int(char_id))
                                .filter(
                                    or_(
                                        CharacterIndustryJobsModel.product_type_id.in_(sorted(type_ids)),
                                        CharacterIndustryJobsModel.blueprint_type_id.in_(sorted(type_ids)),
                                    )
                                )
                                .all()
                            )
                            or []
                        )
                except Exception:
                    pass

                try:
                    if corp_id is not None:
                        job_rows.extend(
                            (
                                app_session.query(CorporationIndustryJobsModel)
                                .filter(CorporationIndustryJobsModel.corporation_id == int(corp_id))
                                .filter(
                                    or_(
                                        CorporationIndustryJobsModel.product_type_id.in_(sorted(type_ids)),
                                        CorporationIndustryJobsModel.blueprint_type_id.in_(sorted(type_ids)),
                                    )
                                )
                                .all()
                            )
                            or []
                        )
                except Exception:
                    pass

                market_prices_for_fifo: list[dict[str, Any]] | None = None
                try:
                    if self._state.esi_service is not None:
                        market_prices_for_fifo = self._state.esi_service.get_market_prices() or []
                except Exception:
                    market_prices_for_fifo = None

                try:
                    fifo_lots_by_type = build_fifo_remaining_lots_by_type(
                        wallet_transactions=tx_rows,
                        industry_jobs=job_rows,
                        sde_session=sde_session,
                        market_prices=market_prices_for_fifo,
                        on_hand_quantities_by_type=inventory_on_hand_by_type,
                    )
                except Exception:
                    fifo_lots_by_type = {}
        except Exception:
            inventory_on_hand_by_type = {}
            fifo_lots_by_type = {}

        data, meta = compute_invention_options_for_blueprint(
            sde_session=sde_session,
            esi_service=self._state.esi_service,
            language=language,
            blueprint_type_id=int(blueprint_type_id),
            character_skills=((character.skills or {}).get("skills") if isinstance(character.skills, dict) else None),
            industry_profile=selected_profile,
            rig_payload=rig_payload,
            manufacturing_system_cost_index=manufacturing_ci,
            invention_system_cost_index=invention_ci,
            copying_system_cost_index=copying_ci,
            inventory_on_hand_by_type=inventory_on_hand_by_type,
            inventory_fifo_lots_by_type=fifo_lots_by_type,
            use_fifo_inventory_costing=bool(payload.get("prefer_inventory_consumption", False)),
        )

        # Optional: include manufacturing submanufacturing plan + UI-ready Build Tree for the best option.
        # This removes the need for Streamlit to call /industry_submanufacturing_plan.
        try:
            if isinstance(data, dict):
                opts = data.get("options") if isinstance(data.get("options"), list) else []
                best = next((o for o in opts if isinstance(o, dict)), None) or None

                mfg_materials_per_run = (best or {}).get("manufacturing_materials_per_run")
                invented_runs = int((best or {}).get("invented_runs") or 0)

                mfg = data.get("manufacturing") if isinstance(data.get("manufacturing"), dict) else {}
                prod_qty_per_run = int(mfg.get("product_quantity_per_run") or 0)
                units_total = int(max(0, invented_runs * prod_qty_per_run))

                mats_payload: list[dict[str, Any]] = []
                synthesized_materials: list[dict[str, Any]] = []
                if isinstance(mfg_materials_per_run, list) and invented_runs > 0:
                    for mm in mfg_materials_per_run:
                        if not isinstance(mm, dict):
                            continue
                        try:
                            tid = int(mm.get("type_id") or 0)
                        except Exception:
                            tid = 0
                        try:
                            qty_eff = int(mm.get("quantity_after_efficiency") or 0)
                        except Exception:
                            qty_eff = 0
                        try:
                            qty_me0 = int(mm.get("quantity_me0") or 0)
                        except Exception:
                            qty_me0 = 0

                        if tid <= 0 or qty_eff <= 0:
                            continue

                        qty_total_eff = int(qty_eff) * int(invented_runs)
                        qty_total_me0 = (int(qty_me0) * int(invented_runs)) if qty_me0 > 0 else None

                        mats_payload.append({"type_id": int(tid), "quantity": int(qty_total_eff)})
                        synthesized_materials.append(
                            {
                                "type_id": int(tid),
                                "type_name": mm.get("type_name"),
                                "group_name": mm.get("group_name"),
                                "category_name": mm.get("category_name"),
                                "quantity_after_efficiency": int(qty_total_eff),
                                "quantity_me0": (int(qty_total_me0) if qty_total_me0 is not None else None),
                            }
                        )

                data["best_manufacturing_required_materials"] = synthesized_materials

                best_plan: list[dict[str, Any]] = []
                if mats_payload:
                    best_plan, _meta = self.industry_submanufacturing_plan(
                        character_id=int(character_id),
                        payload={"profile_id": profile_id, "materials": mats_payload, "max_depth": 10},
                    )
                    if not isinstance(best_plan, list):
                        best_plan = []

                data["best_manufacturing_submanufacturing_plan"] = best_plan
                data["best_ui_build_tree_rows"] = compute_ui_build_tree_rows_by_product(
                    plan_rows=best_plan,
                    required_materials=synthesized_materials,
                    root_required_quantity=int(units_total),
                )
                data["best_ui_blueprint_copy_jobs"] = compute_ui_copy_jobs(
                    blueprint_name=str((data.get("manufacturing") or {}).get("blueprint_type_name") or ""),
                    manufacture_job=None,
                    plan_rows=best_plan,
                )
                data["best_ui_missing_blueprints"] = compute_ui_missing_blueprints(best_plan)

                # UI-ready 'Copy & Invention Jobs' TreeData rows for the best option.
                inv_sec = data.get("invention") if isinstance(data.get("invention"), dict) else {}
                base_out = inv_sec.get("base_output") if isinstance(inv_sec.get("base_output"), dict) else {}
                try:
                    out_bp_type_id = int(base_out.get("blueprint_type_id") or 0)
                except Exception:
                    out_bp_type_id = 0
                out_bp_name = str(base_out.get("blueprint_type_name") or "")

                data["best_ui_copy_invention_jobs_rows"] = compute_ui_copy_invention_jobs_rows_for_best_option(
                    inv_data=data,
                    best_option=best,
                    output_blueprint_type_id=int(out_bp_type_id),
                    output_blueprint_type_name=str(out_bp_name),
                )
        except Exception:
            pass

        return data, meta

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
