from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import hashlib
import json
import logging
import math
import threading
import time
import dataclasses
from dataclasses import dataclass
from collections.abc import Collection
from typing import Any, Callable, cast
import uuid

from sqlalchemy import bindparam, func, text
from eve_online_industry_tracker.db_models import (
    Blueprints,
    CharacterAssetHistoryModel,
    CharacterAssetsModel,
    CharacterIndustryJobsModel,
    CharacterRealizedSalesLedgerModel,
    CharacterWalletTransactionsModel,
    CorporationAssetHistoryModel,
    CorporationAssetsModel,
    CorporationIndustryJobsModel,
    CorporationRealizedSalesLedgerModel,
    CorporationWalletTransactionsModel,
    NpcCorporations,
    NpcStations,
)
from eve_online_industry_tracker.infrastructure.models import (
    CharacterMarketOrdersModel,
    CorporationMarketOrdersModel,
)
from eve_online_industry_tracker.application.characters.asset_provenance import (
    build_fifo_remaining_lots_by_type,
    fifo_allocate_cost,
)

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService
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
from eve_online_industry_tracker.application.industry.job_manager import IndustryJobManager
from eve_online_industry_tracker.application.industry.job_cost import (
    DEFAULT_COPY_COST_EIV_MULTIPLIER,
    DEFAULT_INVENTION_COST_EIV_MULTIPLIER,
    DEFAULT_RESEARCH_COST_EIV_MULTIPLIER,
    DEFAULT_RESEARCH_SCC_SURCHARGE,
    DEFAULT_SCC_SURCHARGE,
    JobSurchargeRates,
    job_install_cost,
    profile_facility_tax,
    research_level_multiplier,
)
from eve_online_industry_tracker.application.industry.research_time import (
    research_skill_reductions,
)
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.industry import shopping_list as _shopping_list
from eve_online_industry_tracker.application.industry.shopping_list import LATER_BATCH_PLAN_VERSION
from eve_online_industry_tracker.infrastructure.persistence import blueprints_repo
from eve_online_industry_tracker.application.industry.invention_model import (
    ALL_INVENTORS_EXCLUDED,
    DEFAULT_INVENTION_PROBABILITY_FLOOR,
    DECRYPTOR_SELECTION_OFF_REASON,
    NO_CHARACTER_SKILLS,
    NO_DECRYPTOR,
    NO_DECRYPTOR_DATA_REASON,
    DecryptorChoice,
    DecryptorEvaluation,
    DecryptorOption,
    InventionContext,
    QuietWarningsFilter,
    decryptor_choice_payload,
    decryptor_options_from_rows,
    effective_invention_probability,
    invented_blueprint_efficiency,
    invented_runs_per_blueprint,
    invention_chance_payload,
    invention_probability,
    quiet_warnings,
    resolve_invention_chance,
    # Aliased: _expected_invention_costs has a local named invention_attempts.
    invention_attempts as expected_and_whole_invention_attempts,
    select_best_decryptor,
)
from eve_online_industry_tracker.infrastructure.sde.decryptors import get_t2_invention_decryptors
from eve_online_industry_tracker.domain import rig_groups as _rig_groups
from eve_online_industry_tracker.domain import security_band as _security_band
from eve_online_industry_tracker.domain import industry_facility as _industry_facility
from eve_online_industry_tracker.infrastructure.sde.skills import get_encryption_skill_type_ids


logger = logging.getLogger(__name__)
# Decryptor candidates and the extra-batch plan are evaluated inside
# invention_model.quiet_warnings(): only the plan that is kept logs.
logger.addFilter(QuietWarningsFilter())

ProgressCallback = Callable[[float, str, dict[str, Any] | None], None]


@dataclass(frozen=True)
class _BatchRunLayout:
    """Where each run of a manufacturing job comes from, over batches built one
    after another: the owned BPCs first, one job per BPC at its own ME/TE, then
    the next source. Materials and time are rounded once per job (EVE).

    An invented next source (`runs_per_blueprint` set) supplies BPCs of that
    many runs, shared across batches as the invention_bpc_plan sizes them, so a
    batch can straddle two BPCs and is then two jobs. A copied (owned BPO) or
    bought next source is one job per batch: its copy is made per batch.
    """

    owned: tuple[tuple[int, int, int, int | None], ...]  # (runs, ME, TE, item_id) per owned BPC, in use order
    next_material_efficiency: int
    next_time_efficiency: int
    next_source: str
    runs_per_blueprint: int | None = None

    @property
    def owned_runs(self) -> int:
        return sum(owned[0] for owned in self.owned)

    def segments(self, start_run: int, runs: int) -> list[dict[str, Any]]:
        """The jobs of the batch that covers runs [start_run, start_run + runs)."""
        out: list[dict[str, Any]] = []
        end = int(start_run) + int(runs)
        cursor = 0
        for owned_runs, me, te, item_id in self.owned:
            low, high = max(cursor, int(start_run)), min(cursor + owned_runs, end)
            if high > low:
                segment = _job_segment(high - low, me, te, "owned_blueprint_copy")
                if item_id is not None:
                    # The BPC this job runs on, so the daily planner can leave
                    # out the jobs of a BPC already in an active job (I1).
                    segment["blueprint_item_id"] = int(item_id)
                out.append(segment)
            cursor += owned_runs
        position = max(cursor, int(start_run))
        while position < end:
            if self.runs_per_blueprint:
                left_on_blueprint = self.runs_per_blueprint - (position - cursor) % self.runs_per_blueprint
                job_runs = min(left_on_blueprint, end - position)
            else:
                job_runs = end - position
            out.append(_job_segment(job_runs, self.next_material_efficiency, self.next_time_efficiency, self.next_source))
            position += job_runs
        return out

    def later_batch_keys(self, batch_runs: int) -> tuple[list[tuple[int, int, tuple]], list[tuple]]:
        """Batches 2..N as (lead, cycle) of job-segment keys.

        lead: [(first batch, count, key)] for the batches that still touch the
        owned BPCs, run-length coded; cycle: the keys of the batches after,
        which repeat with the period of the shared invented BPCs
        (runs_per_blueprint / gcd(batch runs, runs_per_blueprint)), else 1.
        """
        batch_runs = max(1, int(batch_runs))
        owned_runs = self.owned_runs
        # The first later batch that starts past every owned run.
        steady_batch = max(2, -(-owned_runs // batch_runs) + 1)
        lead: list[tuple[int, int, tuple]] = []
        batch = 2
        while batch < steady_batch:
            start = (batch - 1) * batch_runs
            key = _segments_key(self.segments(start, batch_runs))
            # Batches inside one owned BPC repeat this one's jobs: skip them.
            count = 1
            cursor = 0
            for runs, *_levels_and_item in self.owned:
                if cursor <= start and start + batch_runs <= cursor + runs:
                    count = max(1, min(steady_batch - batch, (cursor + runs - start) // batch_runs))
                    break
                cursor += runs
            if lead and lead[-1][2] == key:
                lead[-1] = (lead[-1][0], lead[-1][1] + count, key)
            else:
                lead.append((batch, count, key))
            batch += count
        period = (
            self.runs_per_blueprint // math.gcd(batch_runs, self.runs_per_blueprint)
            if self.runs_per_blueprint else 1
        )
        cycle = [
            _segments_key(self.segments((steady_batch + i - 1) * batch_runs, batch_runs)) for i in range(period)
        ]
        return lead, cycle


def _job_segment(runs: int, material_efficiency: int, time_efficiency: int, source: str) -> dict[str, Any]:
    return {
        "runs": int(runs), "blueprint_material_efficiency": int(material_efficiency),
        "blueprint_time_efficiency": int(time_efficiency), "source": source,
    }


def _segments_key(segments: list[dict[str, Any]]) -> tuple:
    return tuple(
        (int(s["runs"]), int(s["blueprint_material_efficiency"]), int(s["blueprint_time_efficiency"]), str(s["source"]))
        for s in segments
    )


@dataclass
class _ProductPlanningContext:
    """Immutable context shared across all product plans within a single refresh."""
    selected_industry_profile: Any
    selected_character_modifiers: Any
    character_skill_levels: dict
    character_skill_levels_by_name: dict
    adjusted_market_price_map: dict
    material_price_map: dict
    product_sell_price_map: dict
    manufacturing_row_by_product_type_id: dict
    reaction_row_by_product_type_id: dict
    invention_row_by_blueprint_type_id: dict
    blueprint_copy_assets_by_type_id: dict
    blueprint_original_assets_by_type_id: dict
    owned_item_unit_cost_by_type_id: dict
    character_name_by_id: Any
    corporation_name_by_id: Any
    top_location_name_by_id: Any
    available_blueprint_copy_runs_by_type_id_base: dict
    available_owned_item_quantity_by_type_id_base: dict
    build_from_bpc: bool
    include_reactions: bool
    maximize_bp_runs: bool
    group_identical_bpcs: bool
    have_blueprint_source_only: bool
    surcharge_rates: JobSurchargeRates
    blueprint_rows: list
    character_id: Any = None
    # Per-refresh memoization cache for _compute_activity_job results.
    # Key: (activity, base_time_seconds, runs, process_value, manufacturing_group, bp_me, bp_te)
    # Thread-safe on CPython (dict get/set are atomic under GIL).
    activity_job_cache: dict[tuple, dict[str, Any]] = dataclasses.field(default_factory=dict)
    # Invention (round 5): the SDE decryptors (NO_DECRYPTOR first), the
    # refresh's invention inputs, and the blueprint rows by blueprint type id.
    decryptor_options: tuple = (NO_DECRYPTOR,)
    invention_context: InventionContext = dataclasses.field(default_factory=InventionContext)
    blueprint_row_by_type_id: dict = dataclasses.field(default_factory=dict)
    # Task 3: the best decryptor per invented blueprint (admin switch).
    decryptor_selection_enabled: bool = True
    #: {blueprint type_id: DecryptorChoice}, filled lazily by _decryptor_choice_for.
    #: Worker threads write it with dict.setdefault (atomic under the GIL), so
    #: every caller of one refresh gets the same choice per blueprint.
    decryptor_choice_by_blueprint_type_id: dict = dataclasses.field(default_factory=dict)
    #: Per-thread set of blueprints whose decryptors are being evaluated.
    decryptor_evaluation_guard: threading.local = dataclasses.field(default_factory=threading.local)
    #: _resolve_npc_market_fee_context for this refresh: the decryptor cycle
    #: profit and the sale-proceeds step use the same sales tax / broker fee.
    market_fee_context: dict = dataclasses.field(default_factory=dict)
    #: One id per compute for available_owned_item_quantity_by_type_id_base
    #: (a hash of it), published on every row next to its stock so the
    #: shopping list can tell rows from different refreshes apart.
    owned_stock_pool_id: str = ""


class IndustryService:
    _SKILL_ID_ACCOUNTING = 16622
    _SKILL_ID_BROKER_RELATIONS = 3446
    _STRUCTURE_RIG_MFG_CACHE_TTL_SECONDS = 24 * 3600
    _STRUCTURE_RIGS_CACHE_VERSION = 4

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
        "reaction": ("reaction",),
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

    @property
    def _admin(self):
        return getattr(self._state, "admin_settings", None)

    def _adm(self, category: str, key: str, fallback: Any = None) -> Any:
        """Read an admin setting, returning *fallback* when the manager is unavailable."""
        admin = self._admin
        if admin is None:
            return fallback
        return admin.get(category, key)

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

    # (reason) of facility gate refusals already logged.
    _facility_gate_warned: set[str] = set()

    @classmethod
    def _facility_gate(
        cls, *, manufacturing_group: str | None, profile_payload: dict[str, Any] | None, group_id: Any = None,
    ) -> tuple[bool, str | None]:
        """(allowed, reason) for manufacturing a product in the profile's facility.

        Only capital and supercapital ships are gated: a capital hull (the
        Capital Ships rig group, or domain.rig_groups.CAPITAL_FACILITY_GROUP_IDS:
        Titan, Supercarrier, Freighter, Jump Freighter) needs Standup Capital
        Shipyard I, a supercapital (SUPERCAPITAL_GROUP_IDS) Standup
        Supercapital Shipyard I. Everything else, capital components
        included, is built wherever the profile manufactures (an NPC station,
        or any structure the Manufacturing Plant fits).

        Whether the shipyard fits the structure, and may be onlined in the
        system's band, comes from the SDE (domain.industry_facility, read into
        the profile as structure_service_fits). An NPC station
        (location_type "station" / facility_type "npc_station") has no
        shipyard. With no profile there is no facility constraint. A profile
        whose facility or band is unknown does not build capitals: reason
        says why."""
        try:
            gid = int(group_id or 0)
        except (TypeError, ValueError):
            gid = 0
        supercapital = gid in _rig_groups.SUPERCAPITAL_GROUP_IDS
        capital = supercapital or gid in _rig_groups.CAPITAL_FACILITY_GROUP_IDS or manufacturing_group == "Capital Ships"
        if not capital:
            return True, None
        if profile_payload is None:
            return True, None  # No profile constraint.
        facility_type = str(profile_payload.get("facility_type") or "").strip().lower()
        location_type = str(profile_payload.get("location_type") or "").strip().lower()
        if location_type == "station" or facility_type == "npc_station":
            return False, None  # NPC stations have no capital shipyard.
        service = _industry_facility.SUPERCAPITAL_SHIPYARD if supercapital else _industry_facility.CAPITAL_SHIPYARD
        fits = profile_payload.get("structure_service_fits")
        if not isinstance(fits, dict) or not isinstance(fits.get(service), dict):
            structure = profile_payload.get("structure_type_id")
            return False, (
                f"facility unknown (structure type {structure}, no SDE service-module data): "
                f"{'supercapital' if supercapital else 'capital'} ships are not planned there"
                if structure else
                f"facility unknown (the profile has no structure type): "
                f"{'supercapital' if supercapital else 'capital'} ships are not planned there"
            )
        band, band_reason = _security_band.profile_security_band(profile_payload)
        allowed, reason = _industry_facility.service_allowed(fits[service], band)
        if reason is not None and band_reason is not None and band is None:
            reason = f"{reason} ({band_reason})"
        return allowed, reason

    @classmethod
    def _facility_can_manufacture_group(
        cls, *, manufacturing_group: str | None, profile_payload: dict[str, Any] | None, group_id: Any = None,
    ) -> bool:
        """_facility_gate's answer; a refusal for an unknown facility or band
        logs a WARNING once per reason."""
        allowed, reason = cls._facility_gate(
            manufacturing_group=manufacturing_group, profile_payload=profile_payload, group_id=group_id)
        if reason is not None and reason not in IndustryService._facility_gate_warned:
            IndustryService._facility_gate_warned.add(reason)
            logger.warning("Facility gate: %s", reason)
        return allowed

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
        industry_profile_payload: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        enabled_meta_group_set = set(enabled_meta_groups) if enabled_meta_groups is not None else None
        filtered_rows: list[dict[str, Any]] = []
        for row in overview_rows:
            if enabled_meta_group_set is not None and cls._normalized_overview_meta_group_name(row) not in enabled_meta_group_set:
                continue
            if have_skills_only and not cls._overview_row_skill_requirements_met(row):
                continue
            if industry_profile_payload is not None and not cls._facility_can_manufacture_group(
                manufacturing_group=row.get("manufacturing_group"),
                profile_payload=industry_profile_payload, group_id=row.get("group_id"),
            ):
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

    def _find_matching_job(self, *, store: Any, params: dict[str, Any]) -> dict[str, Any] | None:
        with store.lock:
            for job in store.jobs.values():
                if not isinstance(job, dict):
                    continue
                if not self._job_is_active(job):
                    continue
                if dict(job.get("request_params") or {}) != params:
                    continue
                return dict(job)
        return None

    def _find_matching_overview_refresh_job(self, *, params: dict[str, Any]) -> dict[str, Any] | None:
        return self._find_matching_job(store=self._get_industry_overview_refresh_store(), params=params)

    def _find_matching_portfolio_candidates_job(self, *, params: dict[str, Any]) -> dict[str, Any] | None:
        return self._find_matching_job(store=self._get_industry_portfolio_candidates_store(), params=params)

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
                profile_payload["system_region_id"] = solar_system_payload.get("region_id")
                profile_payload["industry_system"] = {
                    **system_payload,
                    "security_status": solar_system_payload.get("security_status"),
                    "region_id": solar_system_payload.get("region_id"),
                }
            band, _ = _security_band.profile_security_band(profile_payload)
            if band is not None:
                profile_payload["system_security_band"] = band

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

    def _load_decryptor_options(self) -> tuple[DecryptorOption, ...]:
        """(NO_DECRYPTOR, every T2 invention decryptor in the SDE).

        Without SDE access, or with no decryptor rows: (NO_DECRYPTOR,) and a
        WARNING. Inventions are then planned without a decryptor, never with
        a guessed one.
        """
        if self._sessions is None:
            logger.warning(
                "Invention: no SDE session, so no decryptor data; inventions are planned without a decryptor"
            )
            return (NO_DECRYPTOR,)
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        session = self._sessions.sde_session()
        try:
            rows = get_t2_invention_decryptors(session, language=language)
        finally:
            session.close()
        options = decryptor_options_from_rows(rows)
        if len(options) == 1:
            logger.warning(
                "Invention: the SDE lists no T2 invention decryptors; inventions are planned without a decryptor"
            )
        return options

    def _corp_characters(self) -> list[dict[str, Any]]:
        """The character manager's characters, [] when there are none."""
        char_manager = getattr(self._state, "char_manager", None)
        if char_manager is None:
            return []
        try:
            characters = char_manager.get_characters() or []
        except ValueError:  # CharacterManager: no characters configured yet
            return []
        return [c for c in characters if isinstance(c, dict)]

    @staticmethod
    def _loaded_skill_levels(char: dict[str, Any]) -> tuple[int, dict[int, int]] | None:
        """(character_id, {skill type_id: trained level}), or None when the
        character has no loaded skill list (unknown, never level 0)."""
        skills = char.get("skills")
        entries = skills.get("skills") if isinstance(skills, dict) else None
        try:
            character_id = int(char.get("character_id") or 0)
        except (TypeError, ValueError):
            return None
        if character_id <= 0 or not isinstance(entries, list) or not entries:
            return None
        levels: dict[int, int] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                skill_id = int(entry.get("skill_id") or 0)
                level = int(entry.get("trained_skill_level") or 0)
            except (TypeError, ValueError):
                continue
            if skill_id > 0:
                levels[skill_id] = max(level, levels.get(skill_id, 0))
        return character_id, levels

    def _get_inventor_skill_levels(self) -> dict[int, dict[int, int]]:
        """{character_id: {skill type_id: trained level}} for every corp
        character whose skills are loaded. A character without a loaded skill
        list is unknown and left out, never read as level 0. A character with
        industry_enabled=False (excluded from industry by the user, the same
        flag the UI filters on) is never the inventor. No WARNING here:
        resolve_invention_chance warns once when nobody is left."""
        out: dict[int, dict[int, int]] = {}
        for char in self._corp_characters():
            if not char.get("industry_enabled", True):
                continue
            loaded = self._loaded_skill_levels(char)
            if loaded is not None:
                out[loaded[0]] = loaded[1]
        return out

    def _no_inventor_reason(self) -> str:
        """Why there is no inventor: skills are loaded only for characters
        excluded from industry, or for nobody."""
        excluded_with_skills = any(
            not char.get("industry_enabled", True) and self._loaded_skill_levels(char) is not None
            for char in self._corp_characters()
        )
        return ALL_INVENTORS_EXCLUDED if excluded_with_skills else NO_CHARACTER_SKILLS

    def _load_encryption_skill_type_ids(self) -> frozenset[int]:
        """The SDE's encryption skills; empty without SDE access (every chance
        is then the base chance, warned once by resolve_invention_chance)."""
        if self._sessions is None:
            return frozenset()
        session = self._sessions.sde_session()
        try:
            return get_encryption_skill_type_ids(session)
        finally:
            session.close()

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
            sales_tax_fraction = max(0.0, min(1.0, float(self._adm("industry", "base_sales_tax_rate", 0.075)) * (1.0 - (float(self._adm("industry", "accounting_skill_reduction", 0.11)) * float(accounting_level)))))

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
            _fee_min = float(self._adm("industry", "broker_fee_min", 0.01))
            _fee_base = float(self._adm("industry", "broker_fee_base", 0.03))
            _br_red = float(self._adm("industry", "broker_relations_reduction_per_level", 0.003))
            _fac_red = float(self._adm("industry", "faction_standing_reduction_per_point", 0.0003))
            _corp_red = float(self._adm("industry", "corp_standing_reduction_per_point", 0.0002))
            broker_fee_fraction = max(
                _fee_min,
                min(
                    _fee_base,
                    _fee_base
                    - (_br_red * float(broker_relations_level))
                    - (_fac_red * float(faction_standing))
                    - (_corp_red * float(corp_standing)),
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
        fee_context: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """`fee_context`: the refresh's resolved fee context (the planning
        context's), so the rows and the decryptor choice use the same fees;
        resolved here when None."""
        if not product_rows:
            return product_rows

        if fee_context is None:
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
                0.91,
                "Loading regional market history and hub liquidity",
                {"stage": "market_activity", "type_count": len(product_type_ids)},
            )
        with ThreadPoolExecutor(max_workers=2) as pool:
            volume_future = pool.submit(pricing_service.get_region_daily_volume_map, type_ids=product_type_ids, hub=market_hub)
            liquidity_future = pool.submit(pricing_service.get_hub_liquidity_map, type_ids=product_type_ids, hub=market_hub)
            region_daily_volume_map = volume_future.result()
            hub_liquidity_map = liquidity_future.result()

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
                0.92,
                "Applied market activity signals",
                {"stage": "liquidity", "rows": len(product_rows)},
            )

        return product_rows

    def _enrich_product_rows_with_liquidity_metrics(
        self,
        product_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        for row in product_rows:
            if not isinstance(row, dict):
                continue

            manufacturing_job = row.get("manufacturing_job") or {}
            hub_sell_liquidity = int(manufacturing_job.get("hub_sell_liquidity") or 0)
            region_daily_volume_7d_avg = self._as_float(manufacturing_job.get("region_daily_volume_7d_avg"))
            hub_sell_orders = int(manufacturing_job.get("hub_sell_order_count") or 0)

            row["days_of_supply"] = self._calculate_days_of_supply(
                hub_sell_liquidity=hub_sell_liquidity,
                region_daily_volume_7d_avg=region_daily_volume_7d_avg,
            )
            row["sell_through_rate"] = self._calculate_sell_through_rate(
                region_daily_volume_7d_avg=region_daily_volume_7d_avg,
                hub_sell_liquidity=hub_sell_liquidity,
            )
            row["liquidity_indicator"] = self._calculate_liquidity_indicator(
                hub_sell_liquidity=hub_sell_liquidity,
                region_daily_volume_7d_avg=region_daily_volume_7d_avg,
                hub_sell_orders=hub_sell_orders,
            )
            row["liquidity_score"] = self._calculate_liquidity_score(
                hub_sell_liquidity=hub_sell_liquidity,
                region_daily_volume_7d_avg=region_daily_volume_7d_avg,
                hub_sell_orders=hub_sell_orders,
            )

            vol_7d = self._as_float(row.get("region_daily_volume_7d_avg")) or 0
            mfg_job_liq = row.get("manufacturing_job", {})
            runs_liq = int(mfg_job_liq.get("runs") or 1)
            qty_per_run_liq = int(mfg_job_liq.get("product_quantity_per_run") or 1)
            prod_qty = runs_liq * qty_per_run_liq
            row["production_to_demand_ratio"] = (prod_qty / vol_7d) if vol_7d > 0 else None

        return product_rows

    def _enrich_product_rows_with_price_anomaly(
        self,
        product_rows: list[dict[str, Any]],
        *,
        market_hub: str,
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        hub_context = pricing_service._market_hub_context(market_hub)
        region_id = int(hub_context.get("region_id") or 10000002)

        history_service = MarketHistoryService(state=self._state, sessions=self._sessions)

        type_ids = sorted({
            int(row.get("type_id") or 0)
            for row in product_rows
            if isinstance(row, dict) and int(row.get("type_id") or 0) > 0
        })

        # Fetch ESI history for any type_id not yet in the DB (runs once; subsequent calls hit the DB)
        missing: list[int] = []
        for tid in type_ids:
            stats = history_service.get_price_stats(type_id=tid, region_id=region_id, days=60)
            if not stats.get("has_data"):
                missing.append(tid)

        if missing:
            with ThreadPoolExecutor(max_workers=20) as pool:
                futures = {
                    pool.submit(history_service.fetch_and_store_history, type_id=tid, region_id=region_id): tid
                    for tid in missing
                }
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception:
                        pass

        # Load price stats for all type_ids
        price_stats: dict[int, dict[str, Any]] = {}
        for tid in type_ids:
            try:
                price_stats[tid] = history_service.get_price_stats(type_id=tid, region_id=region_id, days=60)
            except Exception:
                price_stats[tid] = {"has_data": False}

        _risk_order: dict[str, int] = {"High": 3, "Medium": 2, "Low": 1}

        for row in product_rows:
            if not isinstance(row, dict):
                continue

            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                manufacturing_job = {}

            type_id = int(row.get("type_id") or 0)
            market_unit_price = self._as_float(row.get("market_unit_price"))
            quantity = max(1, int(row.get("quantity") or 1))
            material_cost = self._as_float(manufacturing_job.get("material_cost"))
            hub_sell_order_count = int(row.get("hub_sell_order_count") or 0)

            anomaly_reasons: list[str] = []
            risk_level: str | None = None

            def upgrade(level: str) -> str | None:
                if _risk_order.get(level, 0) > _risk_order.get(risk_level or "", 0):
                    return level
                return risk_level

            price_vs_material_ratio: float | None = None
            price_vs_history_ratio: float | None = None
            history_7d_avg: float | None = None

            if market_unit_price is not None and market_unit_price > 0:
                # Tier 1a: price vs unit material cost
                if material_cost is not None and material_cost > 0:
                    unit_material_cost = material_cost / quantity
                    if unit_material_cost > 0:
                        price_vs_material_ratio = market_unit_price / unit_material_cost
                        if price_vs_material_ratio > 1000:
                            anomaly_reasons.append(
                                f"Price is {price_vs_material_ratio:,.0f}× material cost — likely manipulated or data error"
                            )
                            risk_level = upgrade("High")
                        elif price_vs_material_ratio > 100:
                            anomaly_reasons.append(
                                f"Price is {price_vs_material_ratio:,.0f}× material cost — unusually high markup"
                            )
                            risk_level = upgrade("Medium")
                    if market_unit_price < material_cost / quantity:
                        anomaly_reasons.append("Market price is below manufacturing material cost")
                        risk_level = upgrade("Medium")

                # Tier 1b: thin orderbook
                if hub_sell_order_count < 3:
                    anomaly_reasons.append(
                        f"Only {hub_sell_order_count} sell order(s) at hub — thin market, easy to manipulate"
                    )
                    risk_level = upgrade("Low")

                # Tier 2: historical price comparison
                stats = price_stats.get(type_id) or {}
                if stats.get("has_data"):
                    avg_7d = self._as_float(stats.get("avg_7d"))
                    avg_60d = self._as_float(stats.get("avg_42w"))  # avg over the 60-day window
                    baseline = avg_7d or avg_60d
                    history_7d_avg = avg_7d
                    if baseline and baseline > 0:
                        price_vs_history_ratio = market_unit_price / baseline
                        if price_vs_history_ratio > 5:
                            anomaly_reasons.append(
                                f"Price is {price_vs_history_ratio:.1f}× 7-day historical average — strong manipulation signal"
                            )
                            risk_level = upgrade("High")
                        elif price_vs_history_ratio > 2:
                            anomaly_reasons.append(
                                f"Price is {price_vs_history_ratio:.1f}× 7-day historical average — elevated"
                            )
                            risk_level = upgrade("Medium")
                        elif price_vs_history_ratio < 0.3:
                            anomaly_reasons.append(
                                f"Price is {price_vs_history_ratio:.2f}× 7-day historical average — significantly below historical norm"
                            )
                            risk_level = upgrade("Medium")

            row["price_anomaly_risk"] = risk_level
            row["price_anomaly_reasons"] = anomaly_reasons
            row["price_vs_material_ratio"] = price_vs_material_ratio
            row["price_vs_history_ratio"] = price_vs_history_ratio
            row["history_7d_avg"] = history_7d_avg

        return product_rows

    def _enrich_product_rows_with_market_trends(
        self,
        rows: list[dict],
        *,
        market_history_svc: "MarketHistoryService",
        region_id: int = 10000002,
    ) -> None:
        app_session = self._sessions.app_session()

        # Batch fetch price stats for all product type_ids + their materials
        product_type_ids = [r["type_id"] for r in rows if r.get("type_id")]

        # Collect all material type_ids (top 5 by value per product)
        material_type_ids: set[int] = set()
        for row in rows:
            mfg_job = row.get("manufacturing_job", {})
            mats = mfg_job.get("procurement_materials", {})
            sorted_mats = sorted(
                mats.values(),
                key=lambda m: float(m.get("total_quantity") or m.get("quantity") or 0),
                reverse=True,
            )[:5]
            for m in sorted_mats:
                tid = m.get("type_id")
                if tid:
                    material_type_ids.add(int(tid))

        all_type_ids = set(product_type_ids) | material_type_ids

        # Fetch price stats from DB (no ESI fetch — use cached data only)
        price_stats_map: dict[int, dict] = {}
        for tid in all_type_ids:
            try:
                stats = market_history_svc.get_price_stats(type_id=tid, region_id=region_id)
                if stats.get("has_data"):
                    price_stats_map[tid] = stats
            except Exception:
                pass

        # Pipeline query — batch for all type_ids
        pipeline_jobs: dict[int, int] = {}
        pipeline_orders: dict[int, int] = {}
        historical_margins: dict[int, float | None] = {}

        if product_type_ids:
            for tid in product_type_ids:
                # Active/ready jobs
                char_jobs = (
                    app_session.query(func.sum(CharacterIndustryJobsModel.output_quantity))
                    .filter(
                        CharacterIndustryJobsModel.product_type_id == tid,
                        CharacterIndustryJobsModel.status.in_(["active", "ready"]),
                    )
                    .scalar() or 0
                )
                corp_jobs = (
                    app_session.query(func.sum(CorporationIndustryJobsModel.output_quantity))
                    .filter(
                        CorporationIndustryJobsModel.product_type_id == tid,
                        CorporationIndustryJobsModel.status.in_(["active", "ready"]),
                    )
                    .scalar() or 0
                )
                pipeline_jobs[tid] = int(char_jobs) + int(corp_jobs)

                # Market orders
                char_orders = (
                    app_session.query(func.sum(CharacterMarketOrdersModel.volume_remain))
                    .filter(
                        CharacterMarketOrdersModel.type_id == tid,
                        CharacterMarketOrdersModel.is_buy_order == False,  # noqa: E712
                    )
                    .scalar() or 0
                )
                corp_orders = (
                    app_session.query(func.sum(CorporationMarketOrdersModel.volume_remain))
                    .filter(
                        CorporationMarketOrdersModel.type_id == tid,
                        CorporationMarketOrdersModel.is_buy_order == False,  # noqa: E712
                    )
                    .scalar() or 0
                )
                pipeline_orders[tid] = int(char_orders) + int(corp_orders)

            # Historical realized margins — last 180 days
            cutoff = (datetime.utcnow() - timedelta(days=180)).strftime("%Y-%m-%d")
            for tid in product_type_ids:
                records: list[float] = []
                for Model in (CharacterRealizedSalesLedgerModel, CorporationRealizedSalesLedgerModel):
                    rows_q = (
                        app_session.query(Model.realized_margin_fraction)
                        .filter(
                            Model.type_id == tid,
                            Model.date >= cutoff,
                            Model.confidence != "Low",
                            Model.realized_margin_fraction.isnot(None),
                        )
                        .all()
                    )
                    records.extend([r[0] for r in rows_q])
                if len(records) >= 2:
                    historical_margins[tid] = sum(records) / len(records) * 100
                else:
                    historical_margins[tid] = None

        DEFAULT_BROKER_FEE = 0.035
        ORDER_DURATION_DAYS = 90.0

        for row in rows:
            tid = row.get("type_id")
            mfg_job = row.get("manufacturing_job", {})
            stats = price_stats_map.get(tid, {})

            # Price trend signals
            row["price_trend_7d_pct"] = stats.get("trend_pct")
            row["price_trend_30d_pct"] = stats.get("trend_30d_pct")
            row["price_avg_7d"] = stats.get("avg_7d")
            row["price_avg_42w"] = stats.get("avg_42w")
            row["price_volatility_pct"] = stats.get("volatility_pct")

            # Margin buffer
            market_price = self._as_float(row.get("market_unit_price"))
            total_cost = self._as_float(mfg_job.get("total_cost"))
            runs = int(mfg_job.get("runs") or 1)
            qty_per_run = int(mfg_job.get("product_quantity_per_run") or 1)
            total_units = runs * qty_per_run
            unit_build_cost = (total_cost / total_units) if (total_cost and total_units > 0) else None

            if market_price and unit_build_cost and unit_build_cost > 0:
                row["margin_buffer_pct"] = (market_price - unit_build_cost) / unit_build_cost * 100
            else:
                row["margin_buffer_pct"] = None

            # Projected price at delivery
            trend_pct = stats.get("trend_pct")
            mfg_seconds = self._as_float(mfg_job.get("manufacturing_time_seconds")) or 0
            mfg_days = mfg_seconds / 86400.0

            if market_price and trend_pct is not None:
                daily_trend = trend_pct / 100.0 / 30.0
                projected_price = market_price * (1.0 + daily_trend * mfg_days)
                row["projected_price_at_delivery"] = projected_price
                if unit_build_cost and unit_build_cost > 0 and projected_price > 0:
                    row["projected_margin_pct"] = (projected_price - unit_build_cost) / projected_price * 100
                else:
                    row["projected_margin_pct"] = None
            else:
                row["projected_price_at_delivery"] = None
                row["projected_margin_pct"] = None

            # Material cost trend (weighted avg of top 5 materials)
            mats = mfg_job.get("procurement_materials", {})
            sorted_mats = sorted(
                mats.values(),
                key=lambda m: float(m.get("total_quantity") or m.get("quantity") or 0),
                reverse=True,
            )[:5]
            mat_trends: list[float] = []
            mat_weights: list[float] = []
            for m in sorted_mats:
                mat_tid = m.get("type_id")
                mat_qty = float(m.get("total_quantity") or m.get("quantity") or 0)
                if mat_tid and mat_qty > 0:
                    mat_stats = price_stats_map.get(int(mat_tid), {})
                    t = mat_stats.get("trend_pct")
                    if t is not None:
                        mat_trends.append(t)
                        mat_weights.append(mat_qty)
            if mat_trends:
                total_w = sum(mat_weights)
                row["material_cost_trend_pct"] = (
                    sum(t * w for t, w in zip(mat_trends, mat_weights)) / total_w
                    if total_w > 0
                    else None
                )
            else:
                row["material_cost_trend_pct"] = None

            # Pipeline
            units_jobs = pipeline_jobs.get(tid, 0)
            units_orders = pipeline_orders.get(tid, 0)
            pipeline_total = units_jobs + units_orders
            row["pipeline_units_in_jobs"] = units_jobs
            row["pipeline_units_on_market"] = units_orders
            row["pipeline_total_units"] = pipeline_total
            vol_7d = self._as_float(row.get("region_daily_volume_7d_avg")) or 0
            row["pipeline_days_supply"] = (pipeline_total / vol_7d) if vol_7d > 0 else None

            # Historical realized margin
            hist_margin = historical_margins.get(tid)
            row["historical_realized_margin_pct"] = hist_margin
            proj_margin = self._as_float(row.get("profit_margin_fraction"))
            if hist_margin is not None and proj_margin is not None:
                row["historical_vs_projected_delta_pct"] = hist_margin - (proj_margin * 100)
            else:
                row["historical_vs_projected_delta_pct"] = None

            # Relist cost modeling
            vol_7d_avg = self._as_float(row.get("region_daily_volume_7d_avg")) or 0
            if total_units > 0 and vol_7d_avg > 0:
                selling_days: float | None = total_units / vol_7d_avg
            else:
                selling_days = None
            row["selling_time_days"] = selling_days

            # Extra relist broker fee if selling_days > ORDER_DURATION_DAYS
            broker_fee_frac = (
                self._as_float((row.get("market_fee_context") or {}).get("broker_fee_fraction"))
                or DEFAULT_BROKER_FEE
            )
            if selling_days and selling_days > ORDER_DURATION_DAYS and market_price:
                extra_listings = int(selling_days / ORDER_DURATION_DAYS)
                relist_cost_per_unit = market_price * broker_fee_frac * extra_listings
            else:
                relist_cost_per_unit = 0.0
            row["relist_cost_per_unit"] = relist_cost_per_unit

            # Effective profit after relist costs
            profit_per_unit = self._as_float(row.get("profit_amount"))
            if profit_per_unit is not None and total_units > 0:
                profit_per_unit_val = profit_per_unit / total_units
                effective_profit_per_unit = profit_per_unit_val - relist_cost_per_unit
                row["effective_profit_with_relist"] = effective_profit_per_unit * total_units
                net_rev = self._as_float(row.get("net_proceeds"))
                if net_rev and net_rev > 0:
                    row["effective_margin_with_relist_pct"] = (
                        effective_profit_per_unit * total_units / net_rev
                    ) * 100
                else:
                    row["effective_margin_with_relist_pct"] = None
            else:
                row["effective_profit_with_relist"] = None
                row["effective_margin_with_relist_pct"] = None

    _ASSUMES_OWNED_T2_BPO_REASON = "costed as if a T2 BPO is owned (build_from_bpc off)"

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

            _liq_threshold = int(self._adm("industry", "portfolio_liquidity_threshold", 100))
            _ord_threshold = int(self._adm("industry", "portfolio_order_count_threshold", 5))
            if relevant_liquidity >= _liq_threshold or relevant_order_count >= _ord_threshold:
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

            # Liquidity quality signal
            liquidity_indicator = str(row.get("liquidity_indicator") or "")
            if liquidity_indicator in ("Very High", "High"):
                score += 2
                reasons.append(f"Market liquidity is {liquidity_indicator.lower()} — sell-through is reliable.")
            elif liquidity_indicator == "Medium":
                score += 1
                reasons.append("Market liquidity is medium — some sell-side uncertainty.")
            elif liquidity_indicator in ("Low", "Very Low"):
                reasons.append(f"Market liquidity is {liquidity_indicator.lower()} — pricing may be unreliable.")

            # Price anomaly / manipulation signal
            price_anomaly_risk = str(row.get("price_anomaly_risk") or "")
            price_anomaly_reasons_list = row.get("price_anomaly_reasons") or []
            if price_anomaly_risk == "High":
                score = max(0, score - 6)
                detail = ("; ".join(price_anomaly_reasons_list[:1])) if price_anomaly_reasons_list else "see Price Anomaly Reasons column"
                reasons.append(f"High price anomaly risk — market may be manipulated ({detail}).")
            elif price_anomaly_risk == "Medium":
                score = max(0, score - 3)
                reasons.append("Price anomaly signals detected — treat pricing with caution.")
            elif price_anomaly_risk == "Low":
                score = max(0, score - 1)
                reasons.append("Minor price anomaly signal (thin orderbook).")

            if market_unit_price is None or market_unit_price <= 0:
                confidence = "Low"
            elif score >= 7:
                confidence = "High"
            elif score >= 4:
                confidence = "Medium"
            else:
                confidence = "Low"

            if manufacturing_job.get("assumes_owned_t2_bpo"):
                reasons.append(self._ASSUMES_OWNED_T2_BPO_REASON)

            manufacturing_job["pricing_confidence"] = confidence
            manufacturing_job["pricing_confidence_reasons"] = reasons
            manufacturing_job["market_price_age_minutes"] = market_price_age_minutes

            row["pricing_confidence"] = confidence
            row["pricing_confidence_reasons"] = reasons
            row["market_price_age_minutes"] = market_price_age_minutes

            # Material-side pricing confidence (#5)
            procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
            if isinstance(procurement_materials, dict):
                mat_total = 0
                mat_priced = 0
                mat_with_known_cost = 0
                unknown_cost_basis_count = 0
                for mat in procurement_materials.values():
                    if not isinstance(mat, dict):
                        continue
                    mat_total += 1
                    if self._as_float(mat.get("unit_price")) is not None:
                        mat_priced += 1
                    if bool(mat.get("uses_unknown_owned_cost_basis", False)):
                        unknown_cost_basis_count += 1
                    else:
                        mat_with_known_cost += 1
                # Expected invention inputs (invented T2) are material lines too.
                unpriced_invention_names: list[str] = []
                invention_materials = manufacturing_job.get("expected_invention_materials")
                for mat in (invention_materials.values() if isinstance(invention_materials, dict) else []):
                    if not isinstance(mat, dict):
                        continue
                    mat_total += 1
                    if self._as_float(mat.get("unit_price")) is not None:
                        mat_priced += 1
                    else:
                        unpriced_invention_names.append(str(mat.get("type_name") or mat.get("type_id") or "?"))
                    if bool(mat.get("uses_unknown_owned_cost_basis", False)):
                        unknown_cost_basis_count += 1
                    else:
                        mat_with_known_cost += 1
                if unpriced_invention_names:
                    reasons.append(
                        f"No price for invention input(s) {', '.join(unpriced_invention_names)} — "
                        f"left out of the material cost, so the T2 cost is understated."
                    )
                if mat_total == 0:
                    material_confidence = "Low"
                elif mat_priced == mat_total and mat_with_known_cost == mat_total:
                    material_confidence = "High"
                elif mat_priced >= mat_total * 0.8:
                    material_confidence = "Medium"
                else:
                    material_confidence = "Low"
                if unknown_cost_basis_count > 0:
                    reasons.append(
                        f"{unknown_cost_basis_count} of {mat_total} material(s) are taken from inventory "
                        f"with no acquisition cost on record — cost estimated at market price."
                    )
            else:
                material_confidence = "Low"
                unknown_cost_basis_count = 0
            invention_cost_unknown_reason = manufacturing_job.get("invention_cost_unknown_reason")
            if invention_cost_unknown_reason:
                material_confidence = "Low"
                datacore_need_unknown = manufacturing_job.get("invention_datacore_need_unknown")
                if isinstance(datacore_need_unknown, dict) and datacore_need_unknown.get("batch_one_cost_exact"):
                    # Only the batches past the owned BPC runs invent with
                    # unknown odds; batch 1 runs on the owned BPC.
                    reasons.append(
                        f"Datacore need unknown for batches past the owned BPC runs "
                        f"({invention_cost_unknown_reason}); batch 1 cost is exact, later batches will cost more."
                    )
                else:
                    reasons.append(
                        f"Invention cost unknown ({invention_cost_unknown_reason}) — the T2 cost leaves out "
                        f"datacores and invention fees, so it is understated."
                    )
            invention_chance = manufacturing_job.get("invention_chance")
            if isinstance(invention_chance, dict) and invention_chance.get("skills_unknown_reason"):
                reasons.append(
                    f"Invention chance uses the SDE base chance ({invention_chance['skills_unknown_reason']}) — "
                    f"inventor skills would raise it, so the T2 cost is overstated."
                )
            if isinstance(invention_chance, dict) and invention_chance.get("note"):
                # Informational (A3): the chance is exact, confidence unchanged.
                reasons.append(f"Invention chance: {invention_chance['note']}.")
            for nested_note in manufacturing_job.get("invention_chance_notes") or []:
                reasons.append(f"Invention chance: {nested_note}.")
            for later_batch_note in manufacturing_job.get("later_batch_notes") or []:
                # Informational: the shopping list may over-buy, never under-buy.
                reasons.append(f"Shopping list: {later_batch_note}.")

            overall_confidence_map = {"High": 2, "Medium": 1, "Low": 0}
            overall_score = min(
                overall_confidence_map.get(confidence, 0),
                overall_confidence_map.get(material_confidence, 0),
            )
            overall_confidence = {2: "High", 1: "Medium"}.get(overall_score, "Low")

            manufacturing_job["material_pricing_confidence"] = material_confidence
            manufacturing_job["overall_pricing_confidence"] = overall_confidence
            manufacturing_job["unknown_cost_basis_material_count"] = unknown_cost_basis_count
            row["material_pricing_confidence"] = material_confidence
            row["overall_pricing_confidence"] = overall_confidence
            row["unknown_cost_basis_material_count"] = unknown_cost_basis_count

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

            vol_7d_avg = self._as_float(row.get("region_daily_volume_7d_avg")) or 0
            runs_val = int(manufacturing_job.get("runs") or 1)
            qty_per_run_val = int(manufacturing_job.get("product_quantity_per_run") or 1)
            total_units_val = runs_val * qty_per_run_val
            if total_units_val > 0 and vol_7d_avg > 0:
                selling_time_seconds = (total_units_val / vol_7d_avg) * 86400.0
            else:
                selling_time_seconds = 0.0
            total_time_seconds = (time_seconds or 0) + selling_time_seconds
            if profit_amount is not None and total_time_seconds > 0:
                isk_per_hour_effective: float | None = profit_amount / (total_time_seconds / 3600.0)
            else:
                isk_per_hour_effective = None

            manufacturing_job["profit_amount"] = profit_amount
            manufacturing_job["profit_margin_fraction"] = margin_fraction
            manufacturing_job["isk_per_hour"] = isk_per_hour
            manufacturing_job["isk_per_hour_effective"] = isk_per_hour_effective

            row["profit_amount"] = profit_amount
            row["profit_margin_fraction"] = margin_fraction
            row["isk_per_hour"] = isk_per_hour
            row["isk_per_hour_effective"] = isk_per_hour_effective

        return product_rows

    def _enrich_product_rows_with_manufacturing_signals(
        self,
        product_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        for row in product_rows:
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                manufacturing_job = {}

            profit_amount = self._as_float(row.get("profit_amount"))
            total_cost = self._as_float(manufacturing_job.get("total_cost"))
            time_seconds = self._as_float(manufacturing_job.get("time_seconds"))
            preparation_time_seconds = self._as_float(manufacturing_job.get("preparation_time_seconds"))
            profit_margin_fraction = self._as_float(row.get("profit_margin_fraction"))
            days_of_supply = self._as_float(row.get("days_of_supply"))
            blueprint_source_kind = str(manufacturing_job.get("blueprint_source_kind") or row.get("blueprint_source_kind") or "")
            quantity = max(1, int(row.get("quantity") or 1))

            # Return on capital: profit / total_cost
            return_on_capital: float | None = None
            if profit_amount is not None and total_cost is not None and total_cost > 0:
                return_on_capital = profit_amount / total_cost

            # Manufacture window: will the hub sell through before the build finishes?
            manufacture_window_ok: bool | None = None
            elapsed_time_seconds = orow.get_elapsed_time_seconds(row)  # whole jobs, not amortized slot time
            if days_of_supply is not None and elapsed_time_seconds is not None and elapsed_time_seconds > 0:
                build_time_days = elapsed_time_seconds / 86400.0
                manufacture_window_ok = days_of_supply >= build_time_days

            # Blueprint ME level
            blueprint_me: int | None = None
            raw_me = manufacturing_job.get("blueprint_material_efficiency")
            if raw_me is not None:
                try:
                    blueprint_me = int(raw_me)
                except (TypeError, ValueError):
                    pass

            # Prep time fraction
            prep_time_fraction_pct: float | None = None
            if time_seconds is not None and time_seconds > 0:
                prep_secs = float(preparation_time_seconds or 0.0)
                prep_time_fraction_pct = (prep_secs / time_seconds) * 100.0

            # Fragile margin: profitable but margin < 5 %
            fragile_margin = (
                profit_margin_fraction is not None
                and profit_margin_fraction > 0
                and profit_margin_fraction < 0.05
            )

            # Blueprint SDE fallback: theoretical row, no owned blueprint
            blueprint_sde_fallback = blueprint_source_kind == "blueprint_sde_fallback"

            # Material contention: competing with higher-ranked builds for same owned stock
            material_contention = not bool(row.get("inventory_allocation_optimal", True))

            # Manufacturing cost index from activity breakdown
            manufacturing_cost_index: float | None = None
            activity_breakdown = manufacturing_job.get("activity_breakdown") or {}
            if isinstance(activity_breakdown, dict):
                mfg_activity = activity_breakdown.get("manufacturing") or {}
                if isinstance(mfg_activity, dict):
                    raw_ci = mfg_activity.get("cost_index")
                    if raw_ci is not None:
                        try:
                            manufacturing_cost_index = float(raw_ci)
                        except (TypeError, ValueError):
                            pass

            row["return_on_capital"] = return_on_capital
            row["manufacture_window_ok"] = manufacture_window_ok
            row["blueprint_me"] = blueprint_me
            row["prep_time_fraction_pct"] = prep_time_fraction_pct
            row["fragile_margin"] = fragile_margin
            row["blueprint_sde_fallback"] = blueprint_sde_fallback
            row["material_contention"] = material_contention
            row["manufacturing_cost_index"] = manufacturing_cost_index

        return product_rows

    def _score_inventory_allocation_priority(self, product_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Score inventory allocation priority based on ISK/hr ranking (#3).

        Products that consume owned inventory are ranked by profitability.
        Products using owned materials but with lower ISK/hr than other products
        that need the same materials get flagged with an inventory_priority_rank.
        """
        if not product_rows:
            return product_rows

        material_consumers: dict[int, list[tuple[int, float]]] = {}
        for row_index, row in enumerate(product_rows):
            if not isinstance(row, dict):
                continue
            isk_per_hour = self._as_float(row.get("isk_per_hour")) or 0.0
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue
            procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
            if not isinstance(procurement_materials, dict):
                continue
            for material in procurement_materials.values():
                if not isinstance(material, dict):
                    continue
                if str(material.get("sourcing_strategy") or "").lower() == "take":
                    type_id = int(material.get("type_id") or 0)
                    if type_id > 0:
                        material_consumers.setdefault(type_id, []).append((row_index, isk_per_hour))

        contended_materials: set[int] = set()
        for type_id, consumers in material_consumers.items():
            if len(consumers) > 1:
                contended_materials.add(type_id)

        if not contended_materials:
            for row in product_rows:
                if isinstance(row, dict):
                    row.setdefault("inventory_allocation_optimal", True)
                    manufacturing_job = row.get("manufacturing_job")
                    if isinstance(manufacturing_job, dict):
                        manufacturing_job.setdefault("inventory_allocation_optimal", True)
            return product_rows

        ranked_rows = sorted(
            enumerate(product_rows),
            key=lambda item: -(self._as_float(item[1].get("isk_per_hour")) or 0.0),
        )
        rank_by_row_index: dict[int, int] = {row_index: rank + 1 for rank, (row_index, _) in enumerate(ranked_rows)}

        for row_index, row in enumerate(product_rows):
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue
            procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
            if not isinstance(procurement_materials, dict):
                continue

            uses_contended = False
            for material in procurement_materials.values():
                if not isinstance(material, dict):
                    continue
                if str(material.get("sourcing_strategy") or "").lower() == "take":
                    type_id = int(material.get("type_id") or 0)
                    if type_id in contended_materials:
                        consumers = material_consumers[type_id]
                        best_isk_per_hour = max(c[1] for c in consumers)
                        current_isk_per_hour = self._as_float(row.get("isk_per_hour")) or 0.0
                        if current_isk_per_hour < best_isk_per_hour:
                            uses_contended = True
                            break

            is_optimal = not uses_contended
            row["inventory_allocation_optimal"] = is_optimal
            row["inventory_priority_rank"] = rank_by_row_index.get(row_index)
            manufacturing_job["inventory_allocation_optimal"] = is_optimal
            manufacturing_job["inventory_priority_rank"] = rank_by_row_index.get(row_index)

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

        meta_group = str(row.get("meta_group_name") or "").strip().lower()
        is_t2 = meta_group == "tech ii"
        threshold = 5 if is_t2 else 3
        bpc_count = int(row.get("bpc_count") or 0)
        has_bpo = bool(row.get("has_bpo"))

        if has_bpo and bpc_count == 0:
            bpc_status = "needed"          # Have BPO, need to run copy jobs
        elif has_bpo and bpc_count < threshold:
            bpc_status = "low_with_bpo"    # Have BPO + some copies, but below threshold
        elif has_bpo:
            bpc_status = "bpo"             # BPO owned; copies above threshold → treated as well-stocked
        elif bpc_count == 0:
            bpc_status = "invent"          # No BPO, no BPCs → need to invent (T2) or source externally
        elif bpc_count < threshold:
            bpc_status = "low"             # Some BPCs but below threshold
        else:
            bpc_status = "stocked"         # Enough BPCs

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

        # Capital Cycle ISK/day — total profit over planning horizon ÷ one capital cycle
        # Elapsed (whole-job) build time, not the amortized slot time.
        _elapsed_time_seconds = orow.get_elapsed_time_seconds(row)
        _build_days = float(_elapsed_time_seconds or 0) / 86400.0
        _effective_daily_volume = (
            float(region_daily_volume_7d_avg)
            if region_daily_volume_7d_avg is not None and float(region_daily_volume_7d_avg) > 0
            else float(region_daily_volume)
        )
        _sell_days = (
            (float(max_batches_total) * float(quantity_per_batch)) / _effective_daily_volume
            if _effective_daily_volume > 0 and quantity_per_batch > 0 and max_batches_total > 0
            else None
        )
        _cycle_days = (_build_days + _sell_days) if _sell_days is not None and _build_days > 0 else None
        isk_per_cycle_day: float | None = (
            (float(profit_amount) * float(max_batches_total)) / _cycle_days
            if _cycle_days is not None and _cycle_days > 0 and profit_amount is not None and max_batches_total > 0
            else None
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
            "elapsed_time_seconds": _elapsed_time_seconds,
            "manufacturing_time_seconds": manufacturing_time_seconds,
            "preparation_time_seconds": preparation_time_seconds,
            "region_daily_volume": region_daily_volume,
            "region_daily_volume_7d_avg": region_daily_volume_7d_avg,
            "hub_buy_liquidity": int(row.get("hub_buy_liquidity") or 0),
            "hub_sell_liquidity": int(row.get("hub_sell_liquidity") or 0),
            "days_of_supply": cls._calculate_days_of_supply(
                hub_sell_liquidity=int(row.get("hub_sell_liquidity") or 0),
                region_daily_volume_7d_avg=cls._as_float(region_daily_volume_7d_avg),
            ),
            "sell_through_rate": cls._calculate_sell_through_rate(
                region_daily_volume_7d_avg=cls._as_float(region_daily_volume_7d_avg),
                hub_sell_liquidity=int(row.get("hub_sell_liquidity") or 0),
            ),
            "liquidity_indicator": cls._calculate_liquidity_indicator(
                hub_sell_liquidity=int(row.get("hub_sell_liquidity") or 0),
                region_daily_volume_7d_avg=cls._as_float(region_daily_volume_7d_avg),
                hub_sell_orders=int(row.get("hub_sell_order_count") or 0),
            ),
            "liquidity_score": cls._calculate_liquidity_score(
                hub_sell_liquidity=int(row.get("hub_sell_liquidity") or 0),
                region_daily_volume_7d_avg=cls._as_float(region_daily_volume_7d_avg),
                hub_sell_orders=int(row.get("hub_sell_order_count") or 0),
            ),
            "pricing_confidence": row.get("pricing_confidence"),
            "pricing_confidence_reasons": row.get("pricing_confidence_reasons") or [],
            "market_price_age_minutes": cls._as_float(row.get("market_price_age_minutes")),
            "estimated_market_absorption_units": estimated_market_absorption_units,
            "max_batches_total": max_batches_total,
            "blueprint_source_kind": manufacturing_job.get("blueprint_source_kind"),
            "manufacturing_group": row.get("manufacturing_group"),
            "skill_requirements_met": cls._overview_row_skill_requirements_met(row),
            "owned_input_coverage_fraction": owned_input_coverage_fraction,
            "confidence_penalty_factor": confidence_penalty_factor,
            "effective_profit_per_batch": effective_profit_per_batch,
            "effective_isk_per_hour": effective_isk_per_hour,
            "is_portfolio_candidate": is_portfolio_candidate,
            "bpc_count": bpc_count,
            "has_bpo": has_bpo,
            "bpc_status": bpc_status,
            "bpc_threshold": threshold,
            "isk_per_cycle_day": isk_per_cycle_day,
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
            industry_profile_payload=self._resolve_industry_profile_context(
                character_id=character_id,
                industry_profile_id=industry_profile_id,
            ),
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

    _reduction_memo: dict[tuple, float] = {}

    @classmethod
    def _combine_reductions_memo(cls, reductions: list[Any]) -> float:
        key = tuple(reductions)
        cached = cls._reduction_memo.get(key)
        if cached is not None:
            return cached
        result = cls._combine_reductions(reductions)
        cls._reduction_memo[key] = result
        return result

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
        elif activity in ("research_material", "research_time"):
            # The one research-skill table, shared with the daily planner.
            reductions.extend(research_skill_reductions(activity, skill_levels_by_name))
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

    def _profile_job_surcharge_rates(self, profile_payload: dict[str, Any] | None) -> JobSurchargeRates:
        """The surcharges charged on every job's value (job_cost.py): the SCC
        rates from the admin settings, and the profile's facility tax
        (job_cost.profile_facility_tax). A tax the profile does not determine
        is unknown, never 0 %: every fee then reads unknown with the reason,
        and a WARNING is logged once per reason."""
        scc = max(0.0, float(self._adm("industry", "scc_surcharge", DEFAULT_SCC_SURCHARGE)))
        research_scc = max(0.0, float(self._adm("industry", "research_scc_surcharge", DEFAULT_RESEARCH_SCC_SURCHARGE)))
        facility_tax, reason = profile_facility_tax(profile_payload, scc=scc)
        if reason is not None and isinstance(profile_payload, dict):
            warned = self.__dict__.setdefault("_facility_tax_warned", set())
            if reason not in warned:
                warned.add(reason)
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
        return _rig_groups.primary_group(product_entry)

    @staticmethod
    def _manufacturing_rig_groups(product_entry: Any, activity: str = "manufacturing") -> frozenset[str]:
        """Every rig group that covers the product, EVE's rule
        (domain.rig_groups: the industry target filters by SDE category and
        group): the engineering-rig groups for manufacturing, the reactor-rig
        groups for a reaction (activity="reaction"). A product whose entry
        has too little to classify gets no group (only "All" rigs apply, an
        over-buy rather than an under-buy) and a WARNING, once per product."""
        groups = _rig_groups.rig_groups_for_entry(product_entry, activity=activity)
        if groups is not None:
            return groups
        entry = product_entry if isinstance(product_entry, dict) else {}
        key = (activity, entry.get("type_id"), entry.get("group_id"), entry.get("group_name"),
               entry.get("category_id"), entry.get("category_name"))
        if key not in IndustryService._unclassified_rig_group_warned:
            IndustryService._unclassified_rig_group_warned.add(key)
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

        The aggregate structure_rig_*_bonus is read only for a profile with
        no per-effect rig entries at all."""
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
        has_per_effect_rigs = any(
            isinstance(rig, dict) and any(isinstance(effect, dict) for effect in (rig.get("effects") or []))
            for rig in profile_payload.get("structure_rigs") or []
        )
        if has_per_effect_rigs:
            # The per-effect rigs say which groups they cover and none covers
            # this product. The aggregate structure_rig_*_bonus the UI saves
            # combines those same rigs, so it is no fallback (an under-buy).
            return 0.0

        aggregate_key_by_metric = {
            "material": "structure_rig_material_bonus",
            "time": "structure_rig_time_bonus",
            "cost": "structure_rig_cost_bonus",
        }
        if activity == "manufacturing":
            return cls._normalize_fraction(profile_payload.get(aggregate_key_by_metric.get(metric, "")))
        return 0.0

    # Rig security-multiplier reasons already logged.
    _rig_security_warned: set[str] = set()

    @classmethod
    def _warn_rig_security_once(cls, reason: str) -> None:
        if reason not in IndustryService._rig_security_warned:
            IndustryService._rig_security_warned.add(reason)
            logger.warning("Rig bonus security multiplier: %s", reason)

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
        if cache and isinstance(cache, tuple) and len(cache) == 2 and (now - float(cache[0]) < 86400):
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
        """(EIV, priced line count). The EIV is None when no line, or ANY line,
        lacks a price (fix round 1): a partial sum would price a job fee low
        and pass as exact, so the fee reads unknown instead."""
        total, priced_count, _line_count = cls._sum_estimated_item_value_counted(
            entries, quantity_key=quantity_key, adjusted_price_map=adjusted_price_map,
        )
        return total, priced_count

    @classmethod
    def _sum_estimated_item_value_counted(
        cls,
        entries: list[dict[str, Any]],
        *,
        quantity_key: str,
        adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[float | None, int, int]:
        """_sum_estimated_item_value plus the number of lines it counted
        (priced or not): the M of "N of M priced"."""
        total = 0.0
        priced_count = 0
        line_count = 0
        unpriced = False
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                type_id = int(entry.get("type_id") or 0)
            except Exception:
                type_id = 0
            if type_id <= 0:
                continue
            line_count += 1
            unit_value, _ = cls._resolve_eiv_pricing(
                type_id=type_id,
                type_payload=entry,
                adjusted_price_map=adjusted_price_map,
            )
            if unit_value is None:
                unpriced = True
                continue
            try:
                quantity = int(entry.get(quantity_key) or 0)
            except Exception:
                quantity = 0
            total += float(unit_value) * quantity
            priced_count += 1
        if priced_count == 0:
            return None, 0, line_count
        if unpriced:
            return None, priced_count, line_count
        return total, priced_count, line_count

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
            raw_quantity = entry.get("quantity") or 0
            # Expected (amortized) invention inputs are fractional; keep them so.
            quantity: int | float = raw_quantity if isinstance(raw_quantity, float) else int(raw_quantity)
            if type_id <= 0 or quantity <= 0:
                continue

            available_owned_quantity = available_owned_quantities.get(type_id, 0) or 0
            if not isinstance(available_owned_quantity, float):
                available_owned_quantity = int(available_owned_quantity)
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
            take_quantity = 0
            buy_quantity = quantity

            if available_owned_quantity >= quantity:
                # Full take from owned inventory
                available_owned_quantities[type_id] = max(0, available_owned_quantity - quantity)
                unit_price = preferred_owned_unit_cost if preferred_owned_unit_cost is not None else buy_unit_price
                price_source = "owned_asset_item_value" if preferred_owned_unit_cost is not None else buy_price_source
                recommendation_action = "take"
                take_quantity = quantity
                buy_quantity = 0
            elif available_owned_quantity > 0:
                # Partial: take what we own at FIFO cost, buy the remainder at market
                take_quantity = available_owned_quantity
                buy_quantity = quantity - take_quantity
                available_owned_quantities[type_id] = 0
                take_unit_price = preferred_owned_unit_cost if preferred_owned_unit_cost is not None else buy_unit_price
                if take_unit_price is not None and buy_unit_price is not None:
                    unit_price = (float(take_unit_price) * take_quantity + float(buy_unit_price) * buy_quantity) / quantity
                elif buy_unit_price is not None:
                    unit_price = buy_unit_price
                else:
                    unit_price = take_unit_price
                price_source = "split_owned_and_market"
                recommendation_action = "split"

            line_total = (
                float(unit_price) * float(quantity)
                if unit_price is not None and quantity > 0
                else None
            )
            procurement_materials.append(
                {
                    **dict(entry),
                    "quantity": quantity,
                    "take_quantity": take_quantity,
                    "buy_quantity": buy_quantity,
                    "unit_price": unit_price,
                    "price_source": price_source,
                    "sourcing_strategy": recommendation_action,
                    "owned_cost_basis_known": (preferred_owned_unit_cost is not None),
                    "uses_unknown_owned_cost_basis": bool(
                        recommendation_action in ("take", "split") and preferred_owned_unit_cost is None
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

    @staticmethod
    def _job_install_cost(
        *,
        activity: str,
        process_value: float | None,
        system_index: float,
        structure_cost_reduction: float,
        surcharge_rates: JobSurchargeRates,
    ) -> dict[str, Any]:
        """The one install-cost model of every activity (job_cost.py): job
        value x index x (1 - reduction) + job value x the activity's
        surcharge rate. `process_value` is the activity's job value."""
        return job_install_cost(
            activity=activity, process_value=process_value, system_index=system_index,
            structure_cost_reduction=structure_cost_reduction, surcharge_rates=surcharge_rates,
        )

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

    @staticmethod
    def _calculate_days_of_supply(hub_sell_liquidity: int, region_daily_volume_7d_avg: float | None) -> float | None:
        """Calculate Days of Supply (DOS) = Hub Sell Liquidity / Region Daily Volume (7d avg)"""
        try:
            if region_daily_volume_7d_avg is None or float(region_daily_volume_7d_avg) <= 0:
                return None
            return float(hub_sell_liquidity) / float(region_daily_volume_7d_avg)
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    @staticmethod
    def _calculate_sell_through_rate(region_daily_volume_7d_avg: float | None, hub_sell_liquidity: int) -> float | None:
        """Calculate Sell-Through Rate (%) = (Region Daily Volume 7d avg / Hub Sell Liquidity) * 100"""
        try:
            if region_daily_volume_7d_avg is None or hub_sell_liquidity is None or hub_sell_liquidity <= 0:
                return None
            return (float(region_daily_volume_7d_avg) / float(hub_sell_liquidity)) * 100.0
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    @staticmethod
    def _calculate_liquidity_indicator(hub_sell_liquidity: int, region_daily_volume_7d_avg: float | None, hub_sell_orders: int) -> str:
        """Calculate liquidity indicator based on DOS ranges: Very High, High, Medium, Low, Very Low"""
        try:
            if region_daily_volume_7d_avg is None or float(region_daily_volume_7d_avg) <= 0:
                return "Unknown"
            dos = float(hub_sell_liquidity) / float(region_daily_volume_7d_avg)

            if dos < 1:
                return "Very High"
            elif dos < 3:
                return "High"
            elif dos < 7:
                return "Medium"
            elif dos < 30:
                return "Low"
            else:
                return "Very Low"
        except (TypeError, ValueError, ZeroDivisionError):
            return "Unknown"

    @staticmethod
    def _calculate_liquidity_score(hub_sell_liquidity: int, region_daily_volume_7d_avg: float, hub_sell_orders: int) -> float:
        """Calculate combined liquidity score (0-100) based on DOS and order count"""
        try:
            if region_daily_volume_7d_avg is None or region_daily_volume_7d_avg <= 0:
                return 0.0

            dos = float(hub_sell_liquidity) / float(region_daily_volume_7d_avg)

            # DOS score: lower DOS is better (1 day = 100, 30 days = 0, linear scale)
            dos_score = max(0.0, min(100.0, 100.0 - (dos / 30.0 * 100.0)))

            # Order count score: more orders is better (log scale, 100 orders = 100)
            order_count_score = min(100.0, (float(hub_sell_orders) / 100.0) * 100.0) if hub_sell_orders > 0 else 0.0

            # Combined score: 70% DOS, 30% order count
            liquidity_score = (dos_score * 0.7) + (order_count_score * 0.3)
            return round(liquidity_score, 2)
        except Exception:
            return 0.0

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
                quantity = float(node.get("quantity") or 0)
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
            if "take_quantity" in entry or "take_quantity" in existing:
                # Take legs carry take_quantity; a leg without it is bought.
                take_quantity = int(existing.get("take_quantity") or 0) + int(entry.get("take_quantity") or 0)
                existing["take_quantity"] = take_quantity
                existing["buy_quantity"] = max(0, int(existing["quantity"]) - take_quantity)
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

        def _keep_lowest(index: dict[int, dict[str, Any]], key: int, row: dict[str, Any]) -> None:
            # One rule for a product several blueprints make: the lowest
            # blueprint type_id wins (same as chain_planner's index builders).
            current = index.get(key)
            if current is None or int(row.get("blueprint_type_id") or 0) < int(current.get("blueprint_type_id") or 0):
                index[key] = row

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
                if product_type_id > 0:
                    _keep_lowest(manufacturing_row_by_product_type_id, product_type_id, row)
            for product in (row.get("reaction_job") or {}).get("products") or []:
                if not isinstance(product, dict):
                    continue
                try:
                    product_type_id = int(product.get("type_id") or 0)
                except Exception:
                    product_type_id = 0
                if product_type_id > 0:
                    _keep_lowest(reaction_row_by_product_type_id, product_type_id, row)
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
                if product_type_id > 0:
                    _keep_lowest(invention_row_by_blueprint_type_id, product_type_id, row)
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
        invention_valuation_stock: dict[int, Any],
        owned_item_unit_cost_by_type_id: dict[int, float] | None,
        blueprint_original_assets_by_type_id: dict[int, list[Any]],
        character_name_by_id: dict[int, str] | None = None,
        corporation_name_by_id: dict[int, str] | None = None,
        manufacturing_row_by_product_type_id: dict[int, dict[str, Any]],
        reaction_row_by_product_type_id: dict[int, dict[str, Any]],
        invention_row_by_blueprint_type_id: dict[int, dict[str, Any]],
        invention_context: InventionContext,
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
        available_copy_runs = available_blueprint_copy_runs_by_type_id if available_blueprint_copy_runs_by_type_id is not None else {}
        available_owned_item_quantities = (
            available_owned_item_quantity_by_type_id if available_owned_item_quantity_by_type_id is not None else {}
        )
        owned_item_unit_costs = owned_item_unit_cost_by_type_id or {}
        matched_blueprint_copies = blueprint_copy_assets_by_type_id.get(blueprint_type_id) or []
        matched_blueprint_originals = blueprint_original_assets_by_type_id.get(blueprint_type_id) or []
        owned_copy_runs_used = 0
        second_job: tuple[int, int, int, str] | None = None  # (runs, ME, TE, source) of a D3 second job
        bought_past_owned_note: str | None = None  # batch 1 buys the runs past the owned BPC
        missing_blueprint_copy_runs = 0
        invention_source_row = invention_row_by_blueprint_type_id.get(blueprint_type_id) if activity == "manufacturing" else None
        has_invention_path = isinstance((invention_source_row or {}).get("invention_job"), dict)
        if activity == "manufacturing":
            if bool(build_from_bpc):
                # Whole runs only for a job; a fractional remainder an
                # invention left (0.7) is written back for later inventions.
                available_target_copy_runs_float = max(0.0, float(available_copy_runs.get(blueprint_type_id, 0) or 0))
                available_target_copy_runs = int(math.floor(available_target_copy_runs_float + 1e-9))
                owned_copy_runs_used = min(max(0, available_target_copy_runs), requested_runs)
                missing_blueprint_copy_runs = max(0, requested_runs - owned_copy_runs_used)
                available_copy_runs[blueprint_type_id] = max(0.0, available_target_copy_runs_float - owned_copy_runs_used)
                if owned_copy_runs_used > 0 and matched_blueprint_copies:
                    blueprint_material_efficiency = int(matched_blueprint_copies[0].blueprint_material_efficiency or 0)
                    blueprint_time_efficiency = int(matched_blueprint_copies[0].blueprint_time_efficiency or 0)
                    blueprint_source_kind = "owned_blueprint_copy"
                    if missing_blueprint_copy_runs > 0 and bool(matched_blueprint_originals):
                        include_copying_job = True
                        # D3: the copied runs are a second job at the BPO's ME/TE.
                        bpo = matched_blueprint_originals[0]
                        second_job = (
                            missing_blueprint_copy_runs,
                            int(bpo.blueprint_material_efficiency or 0), int(bpo.blueprint_time_efficiency or 0),
                            "copied_from_owned_blueprint_original",
                        )
                    elif missing_blueprint_copy_runs > 0:
                        # D3: the runs past the owned BPC are a second job on an
                        # invented BPC (invented below), no longer bought. With
                        # unknown odds that job can't be costed (the parent would
                        # buy the whole child, owned runs too), so the owned runs
                        # are still built and the rest is bought, with a reason.
                        second_job_decryptor = invention_context.decryptor_for_blueprint(blueprint_type_id) if has_invention_path else NO_DECRYPTOR
                        second_job_odds_unknown = (
                            self._full_batch_invention_whole_attempts(
                                invention_job=cast(dict[str, Any], (invention_source_row or {}).get("invention_job")),
                                blueprint_type_id=blueprint_type_id, runs=missing_blueprint_copy_runs,
                                max_production_limit=int(job.get("max_production_limit") or 0),
                                adjusted_price_map=adjusted_market_price_map,
                                decryptor=second_job_decryptor, invention_context=invention_context,
                            )[1]
                            if has_invention_path else None
                        )
                        if has_invention_path and second_job_odds_unknown is None:
                            second_job = (
                                missing_blueprint_copy_runs,
                                *invented_blueprint_efficiency(second_job_decryptor),
                                "invented_blueprint_copy",
                            )
                        else:
                            if second_job_odds_unknown is not None:
                                bought_past_owned_note = (
                                    f"{blueprint_display_name} sub-build: {missing_blueprint_copy_runs} runs past the "
                                    f"owned BPC bought, invention odds unknown ({second_job_odds_unknown})"
                                )
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
                    if has_invention_path:
                        # Invented: ME 2 / TE 4 plus the decryptor, and the
                        # invention supplies the BPC, so no copy job (FU-58).
                        blueprint_material_efficiency, blueprint_time_efficiency = invented_blueprint_efficiency(
                            invention_context.decryptor_for_blueprint(blueprint_type_id)
                        )
                    else:
                        include_copying_job = missing_blueprint_copy_runs > 0
                if runs <= 0:
                    return None
            else:
                if matched_blueprint_originals:
                    blueprint_material_efficiency = int(matched_blueprint_originals[0].blueprint_material_efficiency or 0)
                    blueprint_time_efficiency = int(matched_blueprint_originals[0].blueprint_time_efficiency or 0)
                    blueprint_source_kind = "owned_blueprint_original"
                else:
                    blueprint_material_efficiency = int(self._adm("industry", "max_blueprint_material_efficiency", self._MAX_BLUEPRINT_MATERIAL_EFFICIENCY))
                    blueprint_time_efficiency = int(self._adm("industry", "max_blueprint_time_efficiency", self._MAX_BLUEPRINT_TIME_EFFICIENCY))
                    blueprint_source_kind = "blueprint_sde_fallback"
                    include_sde_research_chain = True

        required_skill_entries = [
            dict(skill)
            for skill in (job.get("skill_entries") or [])
            if isinstance(skill, dict)
        ]
        manufacturing_group = (
            self._manufacturing_rig_groups(selected_product, activity=activity)
            if activity in {"manufacturing", "reaction"} else None
        )

        # The plan's jobs (see _BatchRunLayout), like the top-level row's
        # manufacturing_job.job_segments: one per owned BPC (D3), then the
        # second source, one job per invented BPC. A sub-build invents fresh
        # BPCs for each batch, so its invented runs start on a new BPC.
        if activity == "manufacturing" and bool(build_from_bpc):
            if second_job is not None:
                next_source: tuple[int, int, str] = (second_job[1], second_job[2], second_job[3])
            elif blueprint_source_kind == "owned_blueprint_copy":
                # Runs past the owned BPC with no BPO and no invention: an
                # unowned BPC, ME 0 / TE 0.
                next_source = (0, 0, "unowned_blueprint_copy")
            else:
                next_source = (int(blueprint_material_efficiency), int(blueprint_time_efficiency), blueprint_source_kind)
            nested_runs_per_blueprint = (
                self._runs_per_invented_blueprint(
                    self._invention_target_entry(
                        cast(dict[str, Any], (invention_source_row or {}).get("invention_job")), blueprint_type_id,
                    ),
                    max_production_limit=int(job.get("max_production_limit") or 0),
                    decryptor=invention_context.decryptor_for_blueprint(blueprint_type_id),
                )
                if has_invention_path and not matched_blueprint_originals
                and next_source[2] in ("invented_blueprint_copy", "unowned_blueprint_copy")
                else None
            )
            if nested_runs_per_blueprint:
                # Invented runs are labelled so on their jobs (the plan's
                # blueprint_source_kind stays "unowned_blueprint_copy").
                next_source = (next_source[0], next_source[1], "invented_blueprint_copy")
            layout = _BatchRunLayout(
                self._owned_blueprint_copy_jobs(matched_blueprint_copies, runs=owned_copy_runs_used)
                if blueprint_source_kind == "owned_blueprint_copy" else (),
                *next_source, nested_runs_per_blueprint,
            )
        else:
            layout = _BatchRunLayout((), int(blueprint_material_efficiency), int(blueprint_time_efficiency), blueprint_source_kind)
        job_segments = layout.segments(0, runs)
        activity_by_efficiency: dict[tuple[int, int], dict[str, Any]] = {}
        for job_segment in job_segments:
            efficiency = (int(job_segment["blueprint_material_efficiency"]), int(job_segment["blueprint_time_efficiency"]))
            if efficiency not in activity_by_efficiency:
                activity_by_efficiency[efficiency] = self._compute_activity_job(
                    activity=activity, base_time_seconds=0, runs=0, process_value=None,
                    selected_industry_profile=selected_industry_profile,
                    selected_character_modifiers=selected_character_modifiers,
                    character_skill_levels_by_name=character_skill_levels_by_name,
                    manufacturing_group=manufacturing_group, required_skill_entries=required_skill_entries,
                    blueprint_material_efficiency=efficiency[0], blueprint_time_efficiency=efficiency[1],
                    surcharge_rates=None,
                )
            job_activity = activity_by_efficiency[efficiency]
            job_segment["material_reduction"] = float(job_activity.get("material_reduction", 0.0))
            job_segment["time_reduction"] = float(job_activity["time_reduction"])
            job_segment["duration_seconds"] = self._round_duration_seconds(
                int(job.get("time_seconds") or 0) * int(job_segment["runs"]) * max(0.0, 1.0 - job_segment["time_reduction"])
            )
            job_segment["materials"] = {}
        cost_reduction = next(iter(activity_by_efficiency.values()))["cost_reduction"]
        # Runs-weighted over the jobs when they differ; job_segments has each job's own.
        material_reduction, material_differs = self._runs_weighted_reduction(job_segments, "material_reduction")
        _time_reduction, time_differs = self._runs_weighted_reduction(job_segments, "time_reduction")
        # Labels the runs-weighted material_reduction (see the top-level row).
        reduction_basis = "runs_weighted_job_segments" if material_differs or time_differs else None

        adjusted_material_entries: list[dict[str, Any]] = []
        base_material_entries: list[dict[str, Any]] = []
        for raw_material in [dict(entry) for entry in (job.get("materials") or []) if isinstance(entry, dict)]:
            material = dict(raw_material)
            quantity_per_run = int(material.get("quantity") or 0)
            base_total_quantity = quantity_per_run * runs
            adjusted_total_quantity = 0
            for job_segment in job_segments:
                job_runs = int(job_segment["runs"])
                job_quantity = self._round_material_quantity(
                    float(quantity_per_run * job_runs) * max(0.0, 1.0 - job_segment["material_reduction"]),
                    minimum_quantity=(job_runs if quantity_per_run > 0 else 0),
                )
                if job_quantity > 0:
                    job_segment["materials"][str(int(material.get("type_id") or 0))] = job_quantity
                adjusted_total_quantity += job_quantity
            material["quantity_per_run"] = quantity_per_run
            material["base_quantity"] = base_total_quantity
            material["quantity"] = adjusted_total_quantity
            material["material_reduction"] = material_reduction
            adjusted_material_entries.append(material)
            base_material_entries.append({**material, "quantity": base_total_quantity})

        process_value, priced_material_count, eiv_material_count = self._sum_estimated_item_value_counted(
            base_material_entries,
            quantity_key="quantity",
            adjusted_price_map=adjusted_market_price_map,
        )
        cost_index = self._system_cost_index(profile_payload=selected_industry_profile, activity=activity)
        surcharge_rates = self._profile_job_surcharge_rates(selected_industry_profile)
        job_cost = self._job_install_cost(
            activity=activity,
            process_value=process_value,
            system_index=cost_index,
            structure_cost_reduction=cost_reduction,
            surcharge_rates=surcharge_rates,
        )
        direct_duration_seconds = sum(int(job_segment["duration_seconds"]) for job_segment in job_segments)

        total_time_seconds = direct_duration_seconds
        total_job_cost = float(job_cost.get("total_job_cost") or 0.0)
        priced_job_count = 1 if job_cost.get("total_job_cost") is not None else 0
        leaf_materials: list[dict[str, Any]] = []
        recursive_activity_breakdown: dict[str, Any] = {}
        prerequisite_nodes: list[dict[str, Any]] = []
        material_nodes: list[dict[str, Any]] = []
        # Nested invention on the expected-value model (see _expected_invention_costs):
        # amortized inputs kept out of leaf_materials (the buy list), the
        # elapsed-minus-slot time, and why an invention cost is unknown. Carried
        # up through every parent plan into the product row.
        expected_invention_materials: list[dict[str, Any]] = []
        invention_requirements: list[dict[str, Any]] = []
        # One per nested invention (Task 10 fix round 1, I3): what the
        # shopping list needs to carry an invented BPC's spare runs across
        # batches (_plan_nested_invention's "bpc_plan").
        nested_invention_bpc_plans: list[dict[str, Any]] = []
        elapsed_time_adjustment: float | None = 0.0
        invention_cost_unknown_reasons: list[str] = []
        # Nested inventions' chance fallbacks and notes (row reasons, not unknown costs).
        invention_chance_notes: list[str] = []
        if bought_past_owned_note is not None:
            # Batch 1 itself buys these runs (a later batch, with no owned
            # runs, buys the whole sub-build: see _plan_extra_batch).
            invention_chance_notes.append(bought_past_owned_note)
        duration_is_amortized = False

        per_run_material_eiv, _ = self._sum_estimated_item_value(
            adjusted_material_entries,
            quantity_key="quantity_per_run",
            adjusted_price_map=adjusted_market_price_map,
        )

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
            base_copy_time_seconds = int(((blueprint_row.get("copying_job") or {}).get("time_seconds") or 0))
            copy_process_value = (
                float(per_run_material_eiv or 0.0) * missing_blueprint_copy_runs * float(self._adm("industry", "copy_cost_eiv_multiplier", DEFAULT_COPY_COST_EIV_MULTIPLIER))
                if per_run_material_eiv is not None
                else None
            )
            _copy_act = self._compute_activity_job(
                activity="copying",
                base_time_seconds=base_copy_time_seconds,
                runs=missing_blueprint_copy_runs,
                process_value=copy_process_value,
                selected_industry_profile=selected_industry_profile,
                selected_character_modifiers=selected_character_modifiers,
                character_skill_levels_by_name=character_skill_levels_by_name,
                surcharge_rates=surcharge_rates,
            )
            copying_job_cost = _copy_act
            copying_duration_seconds = _copy_act["duration_seconds"]
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
                research_process_value = self._research_process_value(per_run_material_eiv, self._MAX_RESEARCH_LEVEL)
                _res_act = self._compute_activity_job(
                    activity=activity_name,
                    base_time_seconds=target_duration_seconds,
                    runs=1,
                    process_value=research_process_value,
                    selected_industry_profile=selected_industry_profile,
                    selected_character_modifiers=selected_character_modifiers,
                    character_skill_levels_by_name=character_skill_levels_by_name,
                    surcharge_rates=surcharge_rates,
                )
                research_job_cost = _res_act
                research_duration_seconds = _res_act["duration_seconds"]
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
                        # Kept through the per-type merge so the shopping list
                        # knows how much of the merged line is owned stock.
                        "take_quantity": owned_quantity_to_take,
                        "buy_quantity": 0,
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
            trial_copy_runs: dict[int, Any] = {}
            trial_stock: dict[int, Any] = {}
            trial_valuation: dict[int, Any] = {}
            if child_row is not None and child_type_id != int(desired_product_type_id):
                # Evaluate the child on copies: a rejected build must not spend
                # stock, BPC runs or invention valuation stock (FU-61).
                trial_copy_runs = dict(available_copy_runs)
                trial_stock = dict(available_owned_item_quantities)
                trial_valuation = dict(invention_valuation_stock)
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
                    available_blueprint_copy_runs_by_type_id=trial_copy_runs,
                    available_owned_item_quantity_by_type_id=trial_stock,
                    invention_valuation_stock=trial_valuation,
                    owned_item_unit_cost_by_type_id=owned_item_unit_cost_by_type_id,
                    blueprint_original_assets_by_type_id=blueprint_original_assets_by_type_id,
                    character_name_by_id=character_name_by_id,
                    corporation_name_by_id=corporation_name_by_id,
                    manufacturing_row_by_product_type_id=manufacturing_row_by_product_type_id,
                    reaction_row_by_product_type_id=reaction_row_by_product_type_id,
                    invention_row_by_blueprint_type_id=invention_row_by_blueprint_type_id,
                    invention_context=invention_context,
                    visited=next_visited,
                    depth=depth + 1,
                )
            if child_plan:
                planned_child_quantity = min(
                    remaining_child_quantity,
                    max(0, int(child_plan.get("planned_quantity") or remaining_child_quantity)),
                )
                uncovered_child_quantity = max(0, remaining_child_quantity - planned_child_quantity)
                build_total_cost = self._build_total_cost_or_none(child_plan)
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
                    self._commit_trial(available_copy_runs, trial_copy_runs)
                    self._commit_trial(available_owned_item_quantities, trial_stock)
                    self._commit_trial(invention_valuation_stock, trial_valuation)
                    total_time_seconds += int(child_plan.get("total_time_seconds") or 0)
                    if child_plan.get("total_job_cost") is not None:
                        total_job_cost += float(child_plan.get("total_job_cost") or 0.0)
                        priced_job_count += int(child_plan.get("priced_job_count") or 0)
                    leaf_materials.extend([dict(entry) for entry in (child_plan.get("leaf_materials") or []) if isinstance(entry, dict)])
                    elapsed_time_adjustment = self._absorb_nested_invention(
                        child_plan, expected_invention_materials=expected_invention_materials,
                        invention_requirements=invention_requirements,
                        nested_invention_bpc_plans=nested_invention_bpc_plans,
                        invention_cost_unknown_reasons=invention_cost_unknown_reasons,
                        invention_chance_notes=invention_chance_notes,
                        elapsed_time_adjustment=elapsed_time_adjustment,
                    )
                    duration_is_amortized = duration_is_amortized or bool(child_plan.get("duration_is_amortized"))
                    recursive_activity_breakdown[f"{child_activity}:{child_type_id}"] = {
                        "activity": child_activity,
                        "type_id": child_type_id,
                        "type_name": material.get("type_name"),
                        "quantity": planned_child_quantity,
                        "job_segments": child_plan.get("job_segments"),
                        "material_reduction": child_plan.get("material_reduction"),
                        **({"reduction_basis": child_plan["reduction_basis"]} if child_plan.get("reduction_basis") else {}),
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
                target_entry = self._invention_target_entry(invention_job, blueprint_type_id)
                target_blueprint_name = str(
                    ((target_entry or {}).get("product") or {}).get("type_name")
                    if isinstance((target_entry or {}).get("product"), dict)
                    else ""
                ).strip() or blueprint_display_name
                nested_invention = self._plan_nested_invention(
                    invention_source_row=invention_source_row, invention_job=invention_job,
                    target_entry=target_entry, blueprint_type_id=blueprint_type_id,
                    blueprint_display_name=blueprint_display_name,
                    invented_runs=missing_blueprint_copy_runs,
                    max_production_limit=int(job.get("max_production_limit") or 0),
                    selected_industry_profile=selected_industry_profile,
                    selected_character_modifiers=selected_character_modifiers,
                    character_skill_levels_by_name=character_skill_levels_by_name,
                    surcharge_rates=surcharge_rates,
                    adjusted_market_price_map=adjusted_market_price_map,
                    sell_price_map=sell_price_map,
                    available_blueprint_copy_runs_by_type_id=available_copy_runs,
                    available_owned_item_quantity_by_type_id=available_owned_item_quantities,
                    invention_valuation_stock=invention_valuation_stock,
                    owned_item_unit_cost_by_type_id=owned_item_unit_costs,
                    source_copy_enabled=True,
                    decryptor=invention_context.decryptor_for_blueprint(blueprint_type_id),
                    invention_context=invention_context,
                    # This plan's own blueprint is the invented one.
                    invented_blueprint_eiv=per_run_material_eiv,
                )
                invention_job_cost = nested_invention["invention_costs"]
                source_copy_job_cost = nested_invention["source_copy_costs"]
                total_time_seconds += int(invention_job_cost["duration_seconds"])
                if invention_job_cost.get("total_job_cost") is not None:
                    total_job_cost += float(invention_job_cost.get("total_job_cost") or 0.0)
                    priced_job_count += 1
                recursive_activity_breakdown[f"invention:{blueprint_type_id}"] = nested_invention["breakdown"]
                expected_invention_materials.extend(nested_invention["expected_invention_materials"])
                invention_requirements.extend(nested_invention["invention_requirements"])
                if nested_invention["bpc_plan"] is not None:
                    nested_invention_bpc_plans.append(nested_invention["bpc_plan"])
                if nested_invention["cost_unknown_reason"] is not None:
                    invention_cost_unknown_reasons.append(nested_invention["cost_unknown_reason"])
                invention_chance_notes.extend(nested_invention["invention_chance_notes"])
                if elapsed_time_adjustment is not None:
                    adjustment = nested_invention["elapsed_time_adjustment"]
                    elapsed_time_adjustment = None if adjustment is None else elapsed_time_adjustment + adjustment
                duration_is_amortized = duration_is_amortized or bool(invention_job_cost.get("duration_is_amortized"))
                # The invented runs exactly cover the missing runs in the
                # expected-value model, so no surplus BPC runs are left over.
                prerequisite_nodes.append(
                    self._job_tree_node(
                        label=self._ACTIVITY_LABELS["invention"],
                        node_type="activity",
                        activity="invention",
                        blueprint_name=target_blueprint_name,
                        runs=invention_job_cost["runs"],
                        duration_seconds=invention_job_cost["duration_seconds"],
                        direct_duration_seconds=invention_job_cost["duration_seconds"],
                        job_duration_seconds=invention_job_cost["job_duration_seconds"],
                        elapsed_duration_seconds=invention_job_cost["elapsed_duration_seconds"],
                        duration_is_amortized=invention_job_cost["duration_is_amortized"],
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
                                children=nested_invention["material_nodes"],
                            )
                        ],
                    )
                )

                if source_copy_job_cost is not None and source_copy_job_cost.get("duration_seconds", 0) > 0:
                    source_blueprint_type_id = int((invention_source_row or {}).get("blueprint_type_id") or 0)
                    total_time_seconds += int(source_copy_job_cost["duration_seconds"])
                    if source_copy_job_cost.get("total_job_cost") is not None:
                        total_job_cost += float(source_copy_job_cost.get("total_job_cost") or 0.0)
                        priced_job_count += 1
                    recursive_activity_breakdown[f"copying:{source_blueprint_type_id}"] = nested_invention["source_copy_breakdown"]
                    prerequisite_nodes.append(
                        self._job_tree_node(
                            label=self._ACTIVITY_LABELS["copying"],
                            node_type="activity",
                            activity="copying",
                            blueprint_name=invention_blueprint_name,
                            runs=nested_invention["missing_source_runs"],
                            duration_seconds=source_copy_job_cost["duration_seconds"],
                            direct_duration_seconds=source_copy_job_cost["duration_seconds"],
                            job_duration_seconds=source_copy_job_cost["job_duration_seconds"],
                            elapsed_duration_seconds=source_copy_job_cost["elapsed_duration_seconds"],
                            duration_is_amortized=source_copy_job_cost["duration_is_amortized"],
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
        # Amortized invention inputs are part of the build cost the
        # build-vs-buy choice compares, though not of the buy list.
        priced_invention_totals = [
            float(entry["line_total"]) for entry in expected_invention_materials
            if self._as_float(entry.get("line_total")) is not None
        ]
        if priced_invention_totals:
            estimated_material_cost = float(estimated_material_cost or 0.0) + sum(priced_invention_totals)

        return {
            "expected_invention_materials": expected_invention_materials,
            "invention_requirements": invention_requirements,
            "nested_invention_bpc_plans": nested_invention_bpc_plans,
            "elapsed_time_adjustment": elapsed_time_adjustment,
            "invention_cost_unknown_reasons": invention_cost_unknown_reasons,
            "invention_chance_notes": invention_chance_notes,
            "duration_is_amortized": duration_is_amortized,
            "activity": activity,
            "blueprint_type_id": blueprint_type_id,
            "type_id": int(selected_product.get("type_id") or desired_product_type_id),
            "category_name": str(selected_product.get("category_name") or "") or None,
            "meta_group_name": str(selected_product.get("meta_group_name") or "") or None,
            "required_quantity": required_quantity,
            "planned_quantity": planned_quantity,
            "runs": runs,
            "job_segments": job_segments,
            "material_reduction": material_reduction,
            **({"reduction_basis": reduction_basis} if reduction_basis else {}),
            "direct_time_seconds": direct_duration_seconds,
            "total_time_seconds": total_time_seconds,
            "direct_job_cost": job_cost.get("total_job_cost"),
            "total_job_cost": (total_job_cost if priced_job_count > 0 else None),
            "priced_job_count": priced_job_count,
            "estimated_item_value": process_value,
            "estimated_item_value_priced_material_count": priced_material_count,
            "estimated_item_value_material_count": eiv_material_count,
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
        invention_valuation_stock: dict[int, Any],
        owned_item_unit_cost_by_type_id: dict[int, float] | None,
        blueprint_original_assets_by_type_id: dict[int, list[Any]],
        character_name_by_id: dict[int, str] | None = None,
        corporation_name_by_id: dict[int, str] | None = None,
        manufacturing_row_by_product_type_id: dict[int, dict[str, Any]],
        reaction_row_by_product_type_id: dict[int, dict[str, Any]],
        invention_row_by_blueprint_type_id: dict[int, dict[str, Any]],
        invention_context: InventionContext,
    ) -> dict[str, Any]:
        reactions_enabled = bool(include_reactions) and self._reactions_allowed_for_profile(selected_industry_profile)
        total_time_seconds = 0
        total_job_cost = 0.0
        priced_job_count = 0
        procurement_materials: list[dict[str, Any]] = []
        recursive_activity_breakdown: dict[str, Any] = {}
        tree_children: list[dict[str, Any]] = []
        material_nodes: list[dict[str, Any]] = []
        # Nested invention results (see _plan_blueprint_chain_for_quantity).
        expected_invention_materials: list[dict[str, Any]] = []
        invention_requirements: list[dict[str, Any]] = []
        # One per nested invention (Task 10 fix round 1, I3): what the
        # shopping list needs to carry an invented BPC's spare runs across
        # batches (_plan_nested_invention's "bpc_plan").
        nested_invention_bpc_plans: list[dict[str, Any]] = []
        elapsed_time_adjustment: float | None = 0.0
        invention_cost_unknown_reasons: list[str] = []
        # Nested inventions' chance fallbacks and notes (row reasons, not unknown costs).
        invention_chance_notes: list[str] = []
        duration_is_amortized = False
        available_owned_item_quantities = (
            available_owned_item_quantity_by_type_id if available_owned_item_quantity_by_type_id is not None else {}
        )
        owned_item_unit_costs = owned_item_unit_cost_by_type_id or {}

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
                        # Kept through the per-type merge so the shopping list
                        # knows how much of the merged line is owned stock.
                        "take_quantity": owned_quantity_to_take,
                        "buy_quantity": 0,
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

            # Evaluate the child on copies: a rejected build must not spend
            # stock, BPC runs or invention valuation stock (FU-61).
            trial_copy_runs = dict(available_blueprint_copy_runs_by_type_id or {})
            trial_stock = dict(available_owned_item_quantities)
            trial_valuation = dict(invention_valuation_stock)
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
                available_blueprint_copy_runs_by_type_id=trial_copy_runs,
                available_owned_item_quantity_by_type_id=trial_stock,
                invention_valuation_stock=trial_valuation,
                owned_item_unit_cost_by_type_id=owned_item_unit_cost_by_type_id,
                blueprint_original_assets_by_type_id=blueprint_original_assets_by_type_id,
                character_name_by_id=character_name_by_id,
                corporation_name_by_id=corporation_name_by_id,
                manufacturing_row_by_product_type_id=manufacturing_row_by_product_type_id,
                reaction_row_by_product_type_id=reaction_row_by_product_type_id,
                invention_row_by_blueprint_type_id=invention_row_by_blueprint_type_id,
                invention_context=invention_context,
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
            build_total_cost = self._build_total_cost_or_none(child_plan)
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
                self._commit_trial(available_blueprint_copy_runs_by_type_id, trial_copy_runs)
                self._commit_trial(available_owned_item_quantities, trial_stock)
                self._commit_trial(invention_valuation_stock, trial_valuation)
                total_time_seconds += int(child_plan.get("total_time_seconds") or 0)
                if child_plan.get("total_job_cost") is not None:
                    total_job_cost += float(child_plan.get("total_job_cost") or 0.0)
                    priced_job_count += int(child_plan.get("priced_job_count") or 0)
                procurement_materials.extend(
                    [dict(entry) for entry in (child_plan.get("leaf_materials") or []) if isinstance(entry, dict)]
                )
                elapsed_time_adjustment = self._absorb_nested_invention(
                    child_plan, expected_invention_materials=expected_invention_materials,
                    invention_requirements=invention_requirements,
                    nested_invention_bpc_plans=nested_invention_bpc_plans,
                    invention_cost_unknown_reasons=invention_cost_unknown_reasons,
                    invention_chance_notes=invention_chance_notes,
                    elapsed_time_adjustment=elapsed_time_adjustment,
                )
                duration_is_amortized = duration_is_amortized or bool(child_plan.get("duration_is_amortized"))
                recursive_activity_breakdown[f"{child_activity}:{material_type_id}"] = {
                    "activity": child_activity,
                    "type_id": material_type_id,
                    "type_name": material.get("type_name"),
                    "quantity": planned_child_quantity,
                    "job_segments": child_plan.get("job_segments"),
                    "material_reduction": child_plan.get("material_reduction"),
                    **({"reduction_basis": child_plan["reduction_basis"]} if child_plan.get("reduction_basis") else {}),
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
            "expected_invention_materials": expected_invention_materials,
            "invention_requirements": invention_requirements,
            "nested_invention_bpc_plans": nested_invention_bpc_plans,
            "elapsed_time_adjustment": elapsed_time_adjustment,
            "invention_cost_unknown_reasons": invention_cost_unknown_reasons,
            "invention_chance_notes": invention_chance_notes,
            "duration_is_amortized": duration_is_amortized,
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

    #: Industry job statuses that no longer hold a blueprint.
    _TERMINAL_JOB_STATUSES = ("delivered", "cancelled", "reverted")

    def _active_job_blueprint_item_ids(self) -> set[int]:
        """The blueprint item_ids installed in active (not delivered,
        cancelled or reverted) character and corporation industry jobs; a
        NULL status counts as active, as in the daily planner's job read
        (NOT IN alone drops NULL rows). No error handler: an empty set would
        plan with busy BPCs' runs."""
        session: Any = self._sessions.app_session()
        try:
            out: set[int] = set()
            for model in (CharacterIndustryJobsModel, CorporationIndustryJobsModel):
                for (item_id,) in (
                    session.query(model.blueprint_item_id)
                    .filter(model.status.notin_(self._TERMINAL_JOB_STATUSES) | model.status.is_(None))
                    .filter(model.blueprint_item_id.isnot(None))
                    .all()
                ):
                    if int(item_id or 0) > 0:
                        out.add(int(item_id))
            return out
        finally:
            session.close()

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

        # Only include historical blueprints that are currently locked inside an
        # active industry job.  A blueprint absent from both current assets and
        # any active job has been consumed (1-run BPC) or sold — not available.
        #
        # Track char-job and corp-job item IDs separately.  A blueprint installed
        # in a CORP job can appear in both character_asset_history (previous state)
        # and corporation_asset_history (state while at the corp structure).  Using a
        # single shared set would cause it to be emitted from both history sources,
        # producing a duplicate row in the product overview.
        _active_statuses = {"active", "paused", "ready"}
        in_active_char_job_item_ids: set[int] = set()
        in_active_corp_job_item_ids: set[int] = set()

        if normalized_character_ids:
            for (bp_item_id,) in (
                app_session.query(CharacterIndustryJobsModel.blueprint_item_id)
                .filter(CharacterIndustryJobsModel.character_id.in_(normalized_character_ids))
                .filter(CharacterIndustryJobsModel.status.in_(_active_statuses))
                .filter(CharacterIndustryJobsModel.blueprint_item_id.isnot(None))
                .all()
            ):
                in_active_char_job_item_ids.add(int(bp_item_id))

        if normalized_corporation_ids:
            for (bp_item_id,) in (
                app_session.query(CorporationIndustryJobsModel.blueprint_item_id)
                .filter(CorporationIndustryJobsModel.corporation_id.in_(normalized_corporation_ids))
                .filter(CorporationIndustryJobsModel.status.in_(_active_statuses))
                .filter(CorporationIndustryJobsModel.blueprint_item_id.isnot(None))
                .all()
            ):
                in_active_corp_job_item_ids.add(int(bp_item_id))

        # A blueprint locked in a corp job must NOT also appear as a historical char
        # asset — it would produce a duplicate product row.
        char_eligible_item_ids = in_active_char_job_item_ids - in_active_corp_job_item_ids

        historical_character_assets: list[CharacterAssetsModel] = []
        for row in latest_character_rows:
            item_id = int(getattr(row, "item_id", 0) or 0)
            type_id = int(getattr(row, "type_id", 0) or 0)
            if item_id <= 0 or type_id not in blueprint_type_ids or item_id in current_item_ids:
                continue
            if item_id not in char_eligible_item_ids:
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
            if item_id not in in_active_corp_job_item_ids:
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
        material_price_map: dict[int, dict[str, Any]] | None = None,
        corporation_hangar_flags: list[str] | None = None,
    ) -> tuple[dict[int, int], dict[int, float]]:
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

            # Hangar flag filter is solely controlled by the caller via corporation_hangar_flags.
            # None means no filter (return all assets); a non-empty list restricts to those flags.
            _hangar_flags: list[str] | None = corporation_hangar_flags

            def query_corporation_assets(ids: list[int]) -> list[CorporationAssetsModel]:
                normalized_ids = sorted({int(asset_id) for asset_id in ids if int(asset_id) > 0})
                if not normalized_ids:
                    return []
                q = (
                    session.query(CorporationAssetsModel)
                    .filter(CorporationAssetsModel.corporation_id.in_(normalized_ids))
                )
                if _hangar_flags:
                    q = q.filter(CorporationAssetsModel.location_flag.in_(_hangar_flags))
                return q.all()

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

            # Build on-hand quantity map (excluding blueprints)
            quantity_by_type_id: dict[int, int] = {}
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

            if not quantity_by_type_id:
                return {}, {}

            type_ids_list = list(quantity_by_type_id.keys())
            scoped_character_ids = sorted({int(getattr(a, "character_id")) for a in character_assets if getattr(a, "character_id", None) is not None})
            scoped_corporation_ids = sorted({int(getattr(a, "corporation_id")) for a in corporation_assets if getattr(a, "corporation_id", None) is not None})

            # Fetch wallet transactions and completed industry jobs for FIFO lot reconstruction
            wallet_transactions: list[Any] = []
            if scoped_character_ids:
                wallet_transactions.extend(
                    session.query(CharacterWalletTransactionsModel)
                    .filter(CharacterWalletTransactionsModel.character_id.in_(scoped_character_ids))
                    .filter(CharacterWalletTransactionsModel.type_id.in_(type_ids_list))
                    .all()
                )
            if scoped_corporation_ids:
                wallet_transactions.extend(
                    session.query(CorporationWalletTransactionsModel)
                    .filter(CorporationWalletTransactionsModel.corporation_id.in_(scoped_corporation_ids))
                    .filter(CorporationWalletTransactionsModel.type_id.in_(type_ids_list))
                    .all()
                )

            industry_jobs: list[Any] = []
            if scoped_character_ids:
                industry_jobs.extend(
                    session.query(CharacterIndustryJobsModel)
                    .filter(CharacterIndustryJobsModel.character_id.in_(scoped_character_ids))
                    .filter(CharacterIndustryJobsModel.product_type_id.in_(type_ids_list))
                    .all()
                )
            if scoped_corporation_ids:
                industry_jobs.extend(
                    session.query(CorporationIndustryJobsModel)
                    .filter(CorporationIndustryJobsModel.corporation_id.in_(scoped_corporation_ids))
                    .filter(CorporationIndustryJobsModel.product_type_id.in_(type_ids_list))
                    .all()
                )

            # Extract a flat {type_id: sell_price} map from the planning price map.
            # Used to (a) estimate build costs for jobs without a persisted unit_build_cost,
            # and (b) as a fallback before CCP type_average_price in the chain below.
            flat_market_price_map: dict[int, float] | None = None
            if material_price_map:
                flat_market_price_map = {
                    int(tid): float(v)
                    for tid, entry in material_price_map.items()
                    if (v := self._as_float((entry or {}).get("unit_price"))) and v > 0
                } or None

            # Build FIFO lots per type aligned to current on-hand quantity.
            # Jobs with a persisted unit_build_cost are used directly as lots;
            # jobs without one are estimated via flat_market_price_map when available.
            fifo_lots_by_type = build_fifo_remaining_lots_by_type(
                wallet_transactions=wallet_transactions,
                industry_jobs=industry_jobs,
                sde_session=sde_session,
                market_prices=None,
                market_price_map_direct=flat_market_price_map,
                on_hand_quantities_by_type=quantity_by_type_id,
            )

            # Build a weighted-average acquisition_unit_cost per type from asset records.
            # This is used below to back-fill FIFO gaps caused by ESI's 30-day transaction cap.
            asset_acq_cost_total: dict[int, float] = {}
            asset_acq_cost_qty: dict[int, int] = {}
            for asset in [*character_assets, *corporation_assets]:
                try:
                    a_type_id = int(asset.type_id or 0)
                except Exception:
                    a_type_id = 0
                if a_type_id <= 0 or a_type_id not in quantity_by_type_id:
                    continue
                acq_cost = self._as_float(getattr(asset, "acquisition_unit_cost", None))
                if acq_cost is None or acq_cost <= 0:
                    continue
                try:
                    a_qty = int(getattr(asset, "quantity", 0) or 0)
                except Exception:
                    a_qty = 0
                if a_qty <= 0:
                    continue
                asset_acq_cost_total[a_type_id] = float(asset_acq_cost_total.get(a_type_id, 0.0)) + acq_cost * a_qty
                asset_acq_cost_qty[a_type_id] = int(asset_acq_cost_qty.get(a_type_id, 0)) + a_qty

            # Derive FIFO-weighted average unit cost per type from the aligned lots.
            # When FIFO lots cover fewer units than are on hand (ESI transaction history gap),
            # back-fill the unpriced units using acquisition_unit_cost from asset records.
            unit_cost_by_type_id: dict[int, float] = {}
            for type_id, on_hand in quantity_by_type_id.items():
                lots = fifo_lots_by_type.get(type_id) or []
                if not lots:
                    continue
                total_cost, priced_qty = fifo_allocate_cost(lots=lots, quantity=on_hand)
                if priced_qty <= 0 or total_cost <= 0:
                    continue
                unpriced = on_hand - priced_qty
                if unpriced > 0:
                    acq_total = float(asset_acq_cost_total.get(type_id, 0.0))
                    acq_qty = int(asset_acq_cost_qty.get(type_id, 0))
                    if acq_qty > 0 and acq_total > 0:
                        backfill_unit = acq_total / acq_qty
                        total_cost += backfill_unit * unpriced
                        priced_qty += unpriced
                unit_cost_by_type_id[type_id] = total_cost / priced_qty

            # For types the FIFO rebuild couldn't price (no transaction/job history),
            # fall back to acquisition_unit_cost on the asset record → historical
            # snapshot → type average/adjusted price (existing behaviour).
            fallback_needed = [tid for tid in quantity_by_type_id if tid not in unit_cost_by_type_id]
            if fallback_needed:
                exact_cost_total: dict[int, float] = {}
                exact_cost_qty: dict[int, int] = {}
                fallback_cost_total: dict[int, float] = {}
                fallback_cost_qty: dict[int, int] = {}

                for asset in [*character_assets, *corporation_assets]:
                    try:
                        type_id = int(asset.type_id or 0)
                    except Exception:
                        type_id = 0
                    if type_id not in fallback_needed:
                        continue
                    try:
                        quantity = int(getattr(asset, "quantity", 0) or 0)
                    except Exception:
                        quantity = 0
                    if quantity <= 0:
                        continue

                    unit_cost = self._as_float(getattr(asset, "acquisition_unit_cost", None))
                    if unit_cost is not None and unit_cost > 0:
                        exact_cost_total[type_id] = float(exact_cost_total.get(type_id, 0.0)) + float(unit_cost) * quantity
                        exact_cost_qty[type_id] = int(exact_cost_qty.get(type_id, 0)) + quantity
                        continue

                    fallback_unit_cost = self._as_float(getattr(asset, "type_average_price", None))
                    if fallback_unit_cost is None or fallback_unit_cost <= 0:
                        fallback_unit_cost = self._as_float(getattr(asset, "type_adjusted_price", None))
                    if fallback_unit_cost is None or fallback_unit_cost <= 0:
                        continue
                    fallback_cost_total[type_id] = float(fallback_cost_total.get(type_id, 0.0)) + float(fallback_unit_cost) * quantity
                    fallback_cost_qty[type_id] = int(fallback_cost_qty.get(type_id, 0)) + quantity

                for type_id, total_cost in exact_cost_total.items():
                    qty = int(exact_cost_qty.get(type_id, 0))
                    if qty > 0:
                        unit_cost_by_type_id[type_id] = total_cost / qty

                historical_costs = self._load_historical_item_unit_costs(
                    app_session=session,
                    character_ids=scoped_character_ids,
                    corporation_ids=scoped_corporation_ids,
                    type_ids=fallback_needed,
                )
                for type_id in fallback_needed:
                    if type_id in unit_cost_by_type_id:
                        continue
                    hist = self._as_float(historical_costs.get(type_id))
                    if hist is not None and hist > 0:
                        unit_cost_by_type_id[type_id] = hist
                        continue
                    if flat_market_price_map:
                        market_price_val = flat_market_price_map.get(type_id)
                        if market_price_val is not None and market_price_val > 0:
                            unit_cost_by_type_id[type_id] = market_price_val
                            continue
                    fb_total = float(fallback_cost_total.get(type_id, 0.0))
                    fb_qty = int(fallback_cost_qty.get(type_id, 0))
                    if fb_qty > 0:
                        unit_cost_by_type_id[type_id] = fb_total / fb_qty

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

    # ----------------------------------------------------------------
    # Extracted helper methods for product overview (Steps 1-6 refactor)
    # ----------------------------------------------------------------

    @staticmethod
    def _compact_material_entry(entry: dict[str, Any]) -> dict[str, Any]:
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

    @staticmethod
    def _compact_skill_entry(entry: dict[str, Any]) -> dict[str, Any]:
        return {
            "icon_id": entry.get("icon_id"),
            "level": entry.get("level"),
            "type_id": entry.get("type_id"),
            "type_name": entry.get("type_name"),
            "description": entry.get("description"),
        }

    @staticmethod
    def _compact_required_skill_entry(
        entry: dict[str, Any], character_skill_levels: dict[int, int],
    ) -> dict[str, Any]:
        skill = {
            "icon_id": entry.get("icon_id"),
            "level": entry.get("level"),
            "type_id": entry.get("type_id"),
            "type_name": entry.get("type_name"),
            "description": entry.get("description"),
        }
        skill_type_id = int(skill.get("type_id") or 0)
        required_level = int(skill.get("level") or 0)
        trained = int(character_skill_levels.get(skill_type_id, 0))
        return {**skill, "trained_skill_level": trained, "skill_requirement_met": trained >= required_level}

    @staticmethod
    def _overview_keyed_entries(
        entries: list[dict[str, Any]], *, compactor: Any,
    ) -> dict[str, dict[str, Any]]:
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

    @staticmethod
    def _compact_invention_product_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
            out.append({"probability": probability, "quantity": entry.get("quantity"), "type_id": product.get("type_id"), "type_name": product.get("type_name")})
        return out

    def _compute_activity_job(
        self,
        *,
        activity: str,
        base_time_seconds: int,
        runs: int,
        process_value: float | None,
        selected_industry_profile: dict[str, Any] | None,
        selected_character_modifiers: dict[str, Any] | None,
        character_skill_levels_by_name: dict[str, int],
        manufacturing_group: str | Collection[str] | None = None,
        required_skill_entries: list[dict[str, Any]] | None = None,
        blueprint_material_efficiency: int = 0,
        blueprint_time_efficiency: int = 0,
        surcharge_rates: JobSurchargeRates | None = None,
    ) -> dict[str, Any]:
        """Unified activity cost calculator (context-free version)."""
        if surcharge_rates is None:
            surcharge_rates = self._profile_job_surcharge_rates(selected_industry_profile)
        material_reduction = None
        if activity in ("manufacturing", "reaction"):
            material_reduction = self._combine_reductions([
                float(blueprint_material_efficiency) / 100.0,
                self._profile_base_reduction(profile_payload=selected_industry_profile, activity=activity, metric="material"),
                self._profile_rig_reduction(profile_payload=selected_industry_profile, activity=activity, metric="material", manufacturing_group=manufacturing_group),
                self._implant_reduction(character_modifier_payload=selected_character_modifiers, activity=activity, metric="material"),
            ])
        time_factors: list[Any] = []
        if blueprint_time_efficiency > 0:
            time_factors.append(float(blueprint_time_efficiency) / 100.0)
        time_factors.extend([
            self._skill_time_reduction(activity=activity, skill_levels_by_name=character_skill_levels_by_name, required_skill_entries=required_skill_entries),
            self._profile_base_reduction(profile_payload=selected_industry_profile, activity=activity, metric="time"),
            self._profile_rig_reduction(profile_payload=selected_industry_profile, activity=activity, metric="time", manufacturing_group=manufacturing_group),
            self._implant_reduction(character_modifier_payload=selected_character_modifiers, activity=activity, metric="time"),
        ])
        time_reduction = self._combine_reductions(time_factors)
        cost_reduction = self._combine_reductions([
            self._profile_base_reduction(profile_payload=selected_industry_profile, activity=activity, metric="cost"),
            self._profile_rig_reduction(profile_payload=selected_industry_profile, activity=activity, metric="cost", manufacturing_group=manufacturing_group),
            self._implant_reduction(character_modifier_payload=selected_character_modifiers, activity=activity, metric="cost"),
        ])
        cost_index = self._system_cost_index(profile_payload=selected_industry_profile, activity=activity)
        job_cost = self._job_install_cost(activity=activity, process_value=process_value, system_index=cost_index, structure_cost_reduction=cost_reduction, surcharge_rates=surcharge_rates)
        duration_seconds = self._round_duration_seconds(base_time_seconds * runs * max(0.0, 1.0 - time_reduction))
        result: dict[str, Any] = {
            "activity": activity, "duration_seconds": duration_seconds,
            "base_duration_seconds": base_time_seconds * runs,
            "time_reduction": time_reduction, "cost_reduction": cost_reduction,
            "cost_index": cost_index, **job_cost,
        }
        if material_reduction is not None:
            result["material_reduction"] = material_reduction
        return result

    @classmethod
    def _owned_blueprint_copy_jobs(
        cls, assets: list[Any], *, runs: int,
    ) -> tuple[tuple[int, int, int, int | None], ...]:
        """(runs, ME, TE, item_id) per owned BPC for `runs` owned runs, in the
        order _allocate_owned_blueprint_copy_run_usage spends them: one job each."""
        return tuple(
            (int(used), int(getattr(asset, "blueprint_material_efficiency", 0) or 0),
             int(getattr(asset, "blueprint_time_efficiency", 0) or 0),
             int(getattr(asset, "item_id", 0) or 0) or None)
            for asset, used in cls._allocate_owned_blueprint_copy_run_usage(
                cast(list[CharacterAssetsModel | CorporationAssetsModel], list(assets or [])),
                requested_runs=max(0, int(runs)), available_total_runs=max(0, int(runs)),
            )
        )

    def _top_level_batch_run_layout(
        self,
        ctx: _ProductPlanningContext,
        *,
        blueprint_copy_assets: list,
        owned_copy_runs: int,
        blueprint_source_kind: str,
        matched_blueprint_originals: list,
        has_top_level_invention_path: bool,
        invention_job: dict[str, Any],
        blueprint_type_id: int,
        max_production_limit: int,
        decryptor: DecryptorOption,
        material_efficiency: int,
        time_efficiency: int,
    ) -> _BatchRunLayout:
        """The row's own job over all its batches (see _BatchRunLayout).

        The variant's owned BPC runs come first (all of them: the later
        batches use what batch 1 leaves, as the invention_bpc_plan assumes),
        then the next source: a copy of the owned BPO at its ME/TE, an
        invented BPC (invented ME/TE with the decryptor, its runs shared
        across batches), or, with neither, an unowned BPC at ME 0 / TE 0.
        A row with no owned BPC is its own source for every batch.
        """
        if not bool(ctx.build_from_bpc):
            return _BatchRunLayout((), int(material_efficiency), int(time_efficiency), blueprint_source_kind)
        invents = bool(has_top_level_invention_path) and not matched_blueprint_originals
        runs_per_blueprint = (
            self._runs_per_invented_blueprint(
                self._invention_target_entry(invention_job, int(blueprint_type_id)),
                max_production_limit=max_production_limit, decryptor=decryptor,
            )
            if invents else None
        )
        if blueprint_source_kind != "owned_blueprint_copy":
            if blueprint_source_kind == "unowned_blueprint_copy" and runs_per_blueprint:
                # Every run is invented: the jobs say so (the row's
                # blueprint_source_kind stays "unowned_blueprint_copy").
                return _BatchRunLayout(
                    (), int(material_efficiency), int(time_efficiency), "invented_blueprint_copy", runs_per_blueprint,
                )
            return _BatchRunLayout((), int(material_efficiency), int(time_efficiency), blueprint_source_kind)
        owned = self._owned_blueprint_copy_jobs(blueprint_copy_assets, runs=owned_copy_runs)
        if matched_blueprint_originals:
            bpo = matched_blueprint_originals[0]
            return _BatchRunLayout(
                owned, int(bpo.blueprint_material_efficiency or 0), int(bpo.blueprint_time_efficiency or 0),
                "copied_from_owned_blueprint_original",
            )
        if invents:
            return _BatchRunLayout(owned, *invented_blueprint_efficiency(decryptor), "invented_blueprint_copy", runs_per_blueprint)
        return _BatchRunLayout(owned, 0, 0, "unowned_blueprint_copy")

    @staticmethod
    def _runs_weighted_reduction(segments: list[dict[str, Any]], key: str) -> tuple[float, bool]:
        """A batch's reduction `key` over its jobs: the single value of a
        one-job batch (or of jobs that agree), else the runs-weighted mean,
        which is the batch's exact reduction before the per-job rounding.
        Returns (value, whether the jobs differ)."""
        values = [float(segment[key]) for segment in segments]
        if not values:
            return 0.0, False
        if len(set(values)) == 1:
            return values[0], False
        total_runs = sum(int(segment["runs"]) for segment in segments)
        weighted = sum(float(segment[key]) * int(segment["runs"]) for segment in segments)
        return (weighted / total_runs if total_runs > 0 else values[0]), True

    def _size_manufacturing_job_segments(
        self,
        ctx: _ProductPlanningContext,
        *,
        manufacturing_job: dict[str, Any],
        job_segments: list[dict[str, Any]],
        manufacturing_group: str | Collection[str] | None,
        required_skill_entries: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Size one batch's manufacturing jobs, each at its own ME/TE, with one
        ceiling per job and material, never per run (EVE). Fills each segment's
        reductions, duration_seconds and materials in place. Returns
        {"adjusted_material_entries", "base_material_entries", "costs" (the
        first job's activity costs), "material_reduction", "time_reduction"
        (runs-weighted when the jobs differ, see _runs_weighted_reduction),
        "reductions_differ"}."""
        base_time = int(manufacturing_job.get("time_seconds") or 0)
        segment_costs = [
            self._plan_activity_costs(
                ctx, activity="manufacturing", base_time_seconds=base_time,
                runs=int(segment["runs"]), process_value=None,  # process_value computed by the caller
                manufacturing_group=manufacturing_group, required_skill_entries=required_skill_entries,
                blueprint_material_efficiency=int(segment["blueprint_material_efficiency"]),
                blueprint_time_efficiency=int(segment["blueprint_time_efficiency"]),
            )
            for segment in job_segments
        ]
        for segment, segment_cost in zip(job_segments, segment_costs):
            segment["material_reduction"] = float(segment_cost.get("material_reduction", 0.0))
            segment["time_reduction"] = float(segment_cost["time_reduction"])
            segment["duration_seconds"] = self._round_duration_seconds(
                base_time * int(segment["runs"]) * max(0.0, 1.0 - segment["time_reduction"])
            )
            segment["materials"] = {}
        material_reduction, material_differs = self._runs_weighted_reduction(job_segments, "material_reduction")
        time_reduction, time_differs = self._runs_weighted_reduction(job_segments, "time_reduction")
        total_runs = sum(int(segment["runs"]) for segment in job_segments)
        adjusted: list[dict[str, Any]] = []
        base: list[dict[str, Any]] = []
        for raw_mat in [dict(e) for e in (manufacturing_job.get("materials") or []) if isinstance(e, dict)]:
            mat = self._compact_material_entry(raw_mat)
            qty_per_run = int(mat.get("quantity") or 0)
            adjusted_total = 0
            for segment in job_segments:
                segment_runs = int(segment["runs"])
                segment_quantity = self._round_material_quantity(
                    float(qty_per_run * segment_runs) * max(0.0, 1.0 - segment["material_reduction"]),
                    minimum_quantity=(segment_runs if qty_per_run > 0 else 0),
                )
                if segment_quantity > 0:
                    segment["materials"][str(int(mat.get("type_id") or 0))] = segment_quantity
                adjusted_total += segment_quantity
            mat["quantity_per_run"] = qty_per_run
            mat["base_quantity"] = qty_per_run * total_runs
            mat["quantity"] = adjusted_total
            mat["material_reduction"] = material_reduction
            adjusted.append(mat)
            base.append({**mat, "quantity": qty_per_run * total_runs})
        return {
            "adjusted_material_entries": adjusted,
            "base_material_entries": base,
            "costs": segment_costs[0],
            "material_reduction": material_reduction,
            "time_reduction": time_reduction,
            "reductions_differ": material_differs or time_differs,
        }

    @staticmethod
    def _runs_per_invented_blueprint(
        target_entry: dict[str, Any] | None, *, max_production_limit: int, decryptor: DecryptorOption,
    ) -> int:
        """Runs on one invented BPC: the SDE's runs of the invention product
        `target_entry` (else max_production_limit; _expected_invention_costs
        warns about that once per blueprint) plus the decryptor's, at least 1.
        The one source for the run layout's BPC boundaries, the invention
        costs and the shared-BPC plan, and the decryptor cycle."""
        base_runs = int((target_entry or {}).get("quantity") or 0)
        if base_runs <= 0:
            base_runs = max(1, int(max_production_limit or 0) or 1)
        return invented_runs_per_blueprint(base_runs, decryptor)

    def _plan_activity_costs(
        self,
        ctx: _ProductPlanningContext,
        *,
        activity: str,
        base_time_seconds: int,
        runs: int,
        process_value: float | None,
        manufacturing_group: str | Collection[str] | None = None,
        required_skill_entries: list[dict[str, Any]] | None = None,
        blueprint_material_efficiency: int = 0,
        blueprint_time_efficiency: int = 0,
    ) -> dict[str, Any]:
        """Unified activity cost calculator — delegates to _compute_activity_job with per-refresh caching."""
        # The required skills' trained levels change the time reduction
        # (_manufacturing_required_skill_time_reduction), so they are part of the key.
        skill_fingerprint = tuple(sorted(
            (int(entry.get("type_id") or 0), str(entry.get("type_name") or ""), str(entry.get("trained_skill_level") or 0))
            for entry in (required_skill_entries or []) if isinstance(entry, dict)
        ))
        cache_key = (activity, base_time_seconds, runs, process_value, manufacturing_group, blueprint_material_efficiency, blueprint_time_efficiency, skill_fingerprint)
        cached = ctx.activity_job_cache.get(cache_key)
        if cached is not None:
            return dict(cached)
        result = self._compute_activity_job(
            activity=activity,
            base_time_seconds=base_time_seconds,
            runs=runs,
            process_value=process_value,
            selected_industry_profile=ctx.selected_industry_profile,
            selected_character_modifiers=ctx.selected_character_modifiers,
            character_skill_levels_by_name=ctx.character_skill_levels_by_name,
            manufacturing_group=manufacturing_group,
            required_skill_entries=required_skill_entries,
            blueprint_material_efficiency=blueprint_material_efficiency,
            blueprint_time_efficiency=blueprint_time_efficiency,
            surcharge_rates=ctx.surcharge_rates,
        )
        ctx.activity_job_cache[cache_key] = result
        return dict(result)

    def _build_planning_context(
        self,
        *,
        force_refresh: bool,
        build_from_bpc: bool,
        include_reactions: bool,
        maximize_bp_runs: bool,
        group_identical_bpcs: bool,
        have_blueprint_source_only: bool,
        market_hub: str,
        material_price_side: str,
        product_price_side: str,
        industry_profile_id: int | None,
        owned_blueprints_scope: str,
        character_id: int | None,
        progress_callback: ProgressCallback | None,
    ) -> _ProductPlanningContext:
        """Build the immutable context shared by all product plans (Step 1)."""
        mgr = self._ensure_industry_job_manager()
        if progress_callback is not None:
            progress_callback(0.02, "Loading blueprint snapshot", {"stage": "blueprints"})
        blueprint_rows = mgr.get_blueprint_overview(force_refresh=bool(force_refresh))
        if progress_callback is not None:
            progress_callback(0.08, "Resolving character profile and skills", {"stage": "profile", "blueprints": len(blueprint_rows)})

        character_skill_levels = (
            self._get_character_trained_skill_levels(character_id=int(character_id))
            if character_id is not None else {}
        )
        selected_industry_profile = self._resolve_industry_profile_context(
            character_id=character_id, industry_profile_id=industry_profile_id,
        )
        effective_include_reactions = bool(include_reactions) and self._reactions_allowed_for_profile(selected_industry_profile)
        selected_character_modifiers = self._get_character_industry_modifier_payload(character_id=character_id)
        character_skill_levels_by_name = self._skill_levels_by_name(selected_character_modifiers)
        adjusted_market_price_map = self._get_adjusted_market_price_map()
        if progress_callback is not None:
            progress_callback(0.15, "Fetching adjusted market prices", {"stage": "adjusted_prices"})
        (
            manufacturing_row_by_product_type_id,
            reaction_row_by_product_type_id,
            invention_row_by_blueprint_type_id,
        ) = self._build_blueprint_row_indexes(blueprint_rows)
        normalized_market_hub = MarketPricingService.normalize_market_hub(market_hub)
        normalized_material_price_side = MarketPricingService.normalize_order_side(material_price_side)
        normalized_product_price_side = MarketPricingService.normalize_order_side(product_price_side)
        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        product_sell_price_type_ids = sorted({
            *[int(tid) for tid in manufacturing_row_by_product_type_id.keys()],
            *[int(tid) for tid in reaction_row_by_product_type_id.keys()],
        })
        # Resolved once: the decryptor cycle profit and the sale-proceeds step
        # deduct the same sales tax and broker fee.
        market_fee_context = self._resolve_npc_market_fee_context(
            character_id=character_id, market_hub=normalized_market_hub,
            product_price_side=normalized_product_price_side,
        )
        decryptor_selection_enabled = bool(self._adm("industry", "invention_decryptor_selection_enabled", True))
        decryptor_options = self._load_decryptor_options() if decryptor_selection_enabled else (NO_DECRYPTOR,)
        inventor_skill_levels = self._get_inventor_skill_levels()
        material_price_type_ids = sorted({
            int(entry.get("type_id") or 0)
            for row in blueprint_rows if isinstance(row, dict)
            for job_name in ["manufacturing_job", "reaction_job", "invention_job"]
            for entry in (((row.get(job_name) or {}) if isinstance(row.get(job_name), dict) else {}).get("materials") or [])
            if isinstance(entry, dict) and int(entry.get("type_id") or 0) > 0
        } | {
            # Decryptors are invention inputs: hub-priced like the datacores.
            int(option.type_id) for option in decryptor_options if option.type_id is not None
        })
        if progress_callback is not None:
            progress_callback(0.18, "Fetching product sell prices from market hub", {"stage": "product_prices", "product_types": len(product_sell_price_type_ids)})
        product_sell_price_map = pricing_service.get_type_price_map(
            type_ids=product_sell_price_type_ids, hub=normalized_market_hub, side=normalized_product_price_side,
        )
        if progress_callback is not None:
            progress_callback(0.30, "Fetching material prices from market hub", {"stage": "material_prices", "product_types": len(product_sell_price_type_ids), "material_types": len(material_price_type_ids)})
        material_price_map = pricing_service.get_type_price_map(
            type_ids=material_price_type_ids, hub=normalized_market_hub, side=normalized_material_price_side,
        )
        if progress_callback is not None:
            progress_callback(0.45, "Loading owned item inventory", {"stage": "inventory"})
        blueprint_copy_assets_by_type_id: dict[int, list] = {}
        blueprint_original_assets_by_type_id: dict[int, list] = {}
        _admin_hangar_flag = self._adm("industry", "industry_hangar_flag", None)
        _corporation_hangar_flags: list[str] | None = (
            [str(_admin_hangar_flag)] if _admin_hangar_flag is not None else None
        )
        available_owned_item_quantity_by_type_id_base, owned_item_unit_cost_by_type_id = self._get_owned_item_inventory(
            owned_blueprints_scope=owned_blueprints_scope,
            material_price_map=material_price_map,
            corporation_hangar_flags=_corporation_hangar_flags,
        )
        (
            character_blueprint_assets, corporation_blueprint_assets,
            character_name_by_id, corporation_name_by_id, top_location_name_by_id,
        ) = self._get_owned_blueprint_assets(owned_blueprints_scope=owned_blueprints_scope)
        if progress_callback is not None:
            progress_callback(0.55, "Resolved owned blueprint assets", {
                "stage": "assets",
                "character_assets": len(character_blueprint_assets),
                "corporation_assets": len(corporation_blueprint_assets),
            })
        # A BPC installed in an active job (the snapshot can still list it,
        # and the history adds job-locked blueprints back in) has no runs to
        # plan with: it is left out of every owned-BPC allocation and BPC
        # run pool, top level and nested (Task 10 fix round 2).
        busy_blueprint_item_ids = self._active_job_blueprint_item_ids() if bool(build_from_bpc) else set()
        busy_copies_left_out: list[int] = []
        for asset in [*character_blueprint_assets, *corporation_blueprint_assets]:
            try:
                bp_type_id = int(asset.type_id)
            except Exception:
                continue
            if bp_type_id <= 0:
                continue
            if bool(getattr(asset, "is_blueprint_copy", False)):
                if not bool(build_from_bpc):
                    continue
                if int(getattr(asset, "item_id", 0) or 0) in busy_blueprint_item_ids:
                    busy_copies_left_out.append(int(asset.item_id))
                    continue
                blueprint_copy_assets_by_type_id.setdefault(bp_type_id, []).append(asset)
            else:
                blueprint_original_assets_by_type_id.setdefault(bp_type_id, []).append(asset)
        if busy_copies_left_out:
            logger.info(
                "Product overview: %d owned BPC(s) in active industry jobs not planned with (item_ids %s)",
                len(busy_copies_left_out), sorted(busy_copies_left_out),
            )
        for assets in blueprint_copy_assets_by_type_id.values():
            assets.sort(key=lambda a: (-int(a.blueprint_material_efficiency or 0), -int(a.blueprint_time_efficiency or 0), -int(a.blueprint_runs or 0), int(a.item_id or 0)))
        for assets in blueprint_original_assets_by_type_id.values():
            assets.sort(key=lambda a: (-int(a.blueprint_material_efficiency or 0), -int(a.blueprint_time_efficiency or 0), int(a.item_id or 0)))
        # Owned BPC runs keyed by blueprint type id. Planning also writes a
        # negative key into the per-variant copies of this map: -source_bp_type_id
        # (_whole_source_runs_key) holds a T1 source's budget of WHOLE owned
        # runs for invention, beside the expected-value budget under the
        # positive id. It is never a real type id.
        available_blueprint_copy_runs_by_type_id_base: dict[int, int] = {
            int(bp_type_id): sum(max(0, int(a.blueprint_runs or 0)) for a in assets)
            for bp_type_id, assets in blueprint_copy_assets_by_type_id.items()
        }
        ctx = _ProductPlanningContext(
            selected_industry_profile=selected_industry_profile,
            selected_character_modifiers=selected_character_modifiers,
            character_skill_levels=character_skill_levels,
            character_skill_levels_by_name=character_skill_levels_by_name,
            adjusted_market_price_map=adjusted_market_price_map,
            material_price_map=material_price_map,
            product_sell_price_map=product_sell_price_map,
            manufacturing_row_by_product_type_id=manufacturing_row_by_product_type_id,
            reaction_row_by_product_type_id=reaction_row_by_product_type_id,
            invention_row_by_blueprint_type_id=invention_row_by_blueprint_type_id,
            decryptor_options=decryptor_options,
            decryptor_selection_enabled=decryptor_selection_enabled,
            market_fee_context=market_fee_context,
            # Read-only after construction; shared by the worker threads.
            invention_context=InventionContext(
                inventor_skill_levels_by_character_id=inventor_skill_levels,
                encryption_skill_type_ids=self._load_encryption_skill_type_ids(),
                no_inventor_reason=NO_CHARACTER_SKILLS if inventor_skill_levels else self._no_inventor_reason(),
            ),
            blueprint_row_by_type_id={
                int(row.get("blueprint_type_id") or 0): row
                for row in blueprint_rows
                if isinstance(row, dict) and int(row.get("blueprint_type_id") or 0) > 0
            },
            blueprint_copy_assets_by_type_id=blueprint_copy_assets_by_type_id,
            blueprint_original_assets_by_type_id=blueprint_original_assets_by_type_id,
            owned_item_unit_cost_by_type_id=owned_item_unit_cost_by_type_id,
            character_name_by_id=character_name_by_id,
            corporation_name_by_id=corporation_name_by_id,
            top_location_name_by_id=top_location_name_by_id,
            available_blueprint_copy_runs_by_type_id_base=available_blueprint_copy_runs_by_type_id_base,
            available_owned_item_quantity_by_type_id_base=available_owned_item_quantity_by_type_id_base,
            owned_stock_pool_id=self._owned_stock_pool_id(available_owned_item_quantity_by_type_id_base),
            build_from_bpc=build_from_bpc,
            include_reactions=effective_include_reactions,
            maximize_bp_runs=maximize_bp_runs,
            group_identical_bpcs=group_identical_bpcs,
            have_blueprint_source_only=have_blueprint_source_only,
            surcharge_rates=self._profile_job_surcharge_rates(selected_industry_profile),
            blueprint_rows=blueprint_rows,
            character_id=character_id,
        )
        # Every invented blueprint, top level or nested, uses its one best
        # decryptor (set before any worker thread reads the context).
        ctx.invention_context.decryptor_for_blueprint = (
            lambda blueprint_type_id: self._decryptor_choice_for(ctx, blueprint_type_id).option
        )
        return ctx

    def _iter_product_variants(
        self, ctx: _ProductPlanningContext,
    ) -> list[tuple[int, int, dict[str, Any], dict[str, Any], list, Any, int]]:
        """Flatten the triple loop into a list of (row_idx, prod_idx, row, product, copy_assets, original_asset, effective_runs) (Step 6)."""
        results: list[tuple[int, int, dict[str, Any], dict[str, Any], list, Any, int]] = []
        for row_index, row in enumerate(ctx.blueprint_rows, start=1):
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue
            blueprint_type_id = int(row.get("blueprint_type_id") or 0)
            matched_copies = ctx.blueprint_copy_assets_by_type_id.get(blueprint_type_id) or []
            matched_originals = ctx.blueprint_original_assets_by_type_id.get(blueprint_type_id) or []
            raw_products = manufacturing_job.get("products") or []
            if not isinstance(raw_products, list) or not raw_products:
                continue
            for product_index, raw_product in enumerate(raw_products, start=1):
                if not isinstance(raw_product, dict):
                    continue
                if int(raw_product.get("type_id") or 0) <= 0:
                    continue
                max_production_limit = int(manufacturing_job.get("max_production_limit") or 0)
                # Variant selection
                if bool(ctx.build_from_bpc) and matched_copies:
                    if bool(ctx.group_identical_bpcs):
                        variants = [{"blueprint_copy_assets": list(matched_copies), "blueprint_original_asset": None}]
                    else:
                        variants = [{"blueprint_copy_assets": [c], "blueprint_original_asset": None} for c in matched_copies]
                elif matched_originals:
                    variants = [{"blueprint_copy_assets": [], "blueprint_original_asset": o} for o in matched_originals]
                else:
                    variants = [{"blueprint_copy_assets": [], "blueprint_original_asset": None}]
                if bool(ctx.have_blueprint_source_only):
                    variants = [v for v in variants if (v.get("blueprint_copy_assets") or []) or v.get("blueprint_original_asset") is not None]
                    if not variants:
                        continue
                for variant in variants:
                    copy_assets = cast(list, list(variant.get("blueprint_copy_assets") or []))
                    original_asset = variant.get("blueprint_original_asset")
                    copy_runs = sum(max(0, int(getattr(a, "blueprint_runs", 0) or 0)) for a in copy_assets)
                    effective_runs = 1
                    if bool(ctx.maximize_bp_runs):
                        if copy_runs > 0:
                            effective_runs = copy_runs
                        elif max_production_limit > 0:
                            effective_runs = max_production_limit
                    results.append((row_index, product_index, row, raw_product, copy_assets, original_asset, effective_runs))
        return results

    def _plan_copying_for_variant(
        self,
        ctx: _ProductPlanningContext,
        *,
        row: dict[str, Any],
        blueprint_type_id: int,
        per_run_material_eiv: float | None,
        missing_copy_runs: int,
        requires_copying_job: bool,
        owned_target_copy_runs_used: int,
        compact_product: dict[str, Any],
        has_invention_path: bool,
        matched_blueprint_originals: list,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """Plan the copying activity for a variant (Step 4)."""
        copying_job_data = row.get("copying_job") or {}
        base_copy_time = int((copying_job_data.get("time_seconds") or 0))
        if base_copy_time <= 0 or (has_invention_path and not matched_blueprint_originals):
            return None, []

        costs = self._plan_activity_costs(ctx, activity="copying", base_time_seconds=base_copy_time, runs=max(0, missing_copy_runs), process_value=(float(per_run_material_eiv or 0.0) * max(0, missing_copy_runs) * float(self._adm("industry", "copy_cost_eiv_multiplier", DEFAULT_COPY_COST_EIV_MULTIPLIER)) if per_run_material_eiv is not None else None))
        bp_name = str(((row.get("blueprint") or {}) if isinstance(row.get("blueprint"), dict) else {}).get("type_name") or ((row.get("blueprint") or {}) if isinstance(row.get("blueprint"), dict) else {}).get("name") or blueprint_type_id or "Blueprint")
        costs["runs"] = max(0, missing_copy_runs)
        costs["estimated_item_value"] = per_run_material_eiv

        tree_nodes: list[dict[str, Any]] = []
        if requires_copying_job or owned_target_copy_runs_used <= 0:
            tree_nodes.append(self._job_tree_node(
                label=self._ACTIVITY_LABELS["copying"], node_type="activity", activity="copying",
                blueprint_name=bp_name, quantity=1, runs=max(0, missing_copy_runs),
                duration_seconds=costs["duration_seconds"], direct_duration_seconds=costs["duration_seconds"],
                job_cost=costs.get("total_job_cost"), total_job_cost=costs.get("total_job_cost"),
                category_name=str(compact_product.get("category_name") or "") or None,
                meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                recommendation_action="copy", children=[],
            ))
        return costs, tree_nodes

    _NO_INVENTION_PROBABILITY_REASON = "no invention success probability (SDE and admin floor are both 0)"

    @staticmethod
    def _invention_target_entry(invention_job: dict[str, Any], blueprint_type_id: int) -> dict[str, Any] | None:
        """The invention job's product entry for the invented blueprint, or None."""
        for product in invention_job.get("products") or []:
            if not isinstance(product, dict):
                continue
            inv_product = product.get("product") or {}
            if not isinstance(inv_product, dict):
                inv_product = {}
            if int(inv_product.get("type_id") or product.get("type_id") or 0) == int(blueprint_type_id):
                return product
        return None

    def _expected_invention_costs(
        self,
        *,
        compute_activity: Callable[..., dict[str, Any]],
        invention_job: dict[str, Any],
        target_entry: dict[str, Any] | None,
        blueprint_type_id: int,
        invented_runs: int,
        max_production_limit: int,
        adjusted_price_map: dict[int, dict[str, Any]],
        decryptor: DecryptorOption,
        invention_context: InventionContext,
        invented_blueprint_eiv: float | None,
        log_unknown_cost: bool = True,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Invention fees, time and inputs for `invented_runs` runs of an invented
        BPC, on the expected-value model. Shared by the top-level T2 row and the
        nested (sub-build) invention paths.

        BPCs needed = runs / runs_per_BPC, attempts = BPCs / probability. Inputs,
        fees and slot time scale with these fractional attempts, so a 1-run row
        carries 1/(p x runs_per_BPC) of an attempt. A separate elapsed time uses
        whole attempts: ceil(whole BPCs needed / probability) jobs in sequence.

        With no SDE probability and an admin floor of 0 the attempt count is
        unknown: never divides, logs a WARNING, and returns zero attempts with
        `cost_unknown_reason` set, total_job_cost and elapsed time None.

        `log_unknown_cost=False` logs that unknown-cost line at DEBUG: a caller
        that logs its own row-level WARNING for it (see
        _full_batch_invention_whole_attempts).

        `compute_activity(activity=, base_time_seconds=, runs=, process_value=)`
        prices one activity (the cached ctx calculator or the context-free one).

        `invented_blueprint_eiv` is the per-run EIV of the INVENTED blueprint's
        manufacturing materials (base quantities): EVE prices an attempt at
        invention_cost_eiv_multiplier (0.02) x that EIV (job_cost.py, Task 8b
        fit), not at the datacores' EIV. None = the fee is unknown.

        The success chance is EVE's: the SDE base chance x the corp's best
        inventor's skill factor x the decryptor multiplier, capped at 1, then
        the admin floor (resolve_invention_chance). Its breakdown is
        costs["invention_chance"] whenever something is invented.
        Returns (costs, invention_materials sized by the expected attempts).
        """
        base_probability = 0.0
        if isinstance(target_entry, dict):
            try:
                base_probability = float(target_entry.get("probability_pct") or 0.0) / 100.0
            except Exception:
                base_probability = 0.0
        # EVE: base x (1 + (sci1 + sci2)/30 + enc/40) x decryptor, for the
        # corp's best inventor of this blueprint (D2). Resolved only when
        # something is invented, so a row that invents nothing warns nothing.
        chance = (
            resolve_invention_chance(
                invention_context, blueprint_type_id=int(blueprint_type_id),
                skill_entries=[e for e in (invention_job.get("skill_entries") or []) if isinstance(e, dict)],
                base_probability=base_probability, decryptor=decryptor,
            )
            if invented_runs > 0 else None
        )
        probability = (
            chance.probability if chance is not None
            else invention_probability(base_probability, None, decryptor.probability_multiplier)
        )
        probability_floor = float(self._adm("industry", "invention_probability_floor", DEFAULT_INVENTION_PROBABILITY_FLOOR))
        runs_per_invented_blueprint = int((target_entry or {}).get("quantity") or 0)
        if invented_runs > 0:
            # Once per blueprint per refresh: batch 1, the later batches and
            # the D3 pre-check all evaluate the same blueprint's odds.
            if probability <= 0:
                invention_context.warn_once(
                    f"invention-no-sde-probability:{int(blueprint_type_id)}",
                    "Invention plan for blueprint %s: no SDE success probability; using the admin floor %s",
                    blueprint_type_id, probability_floor,
                )
            if runs_per_invented_blueprint <= 0:
                invention_context.warn_once(
                    f"invention-no-sde-runs:{int(blueprint_type_id)}",
                    "Invention plan for blueprint %s: no SDE runs per invented BPC; falling back to max_production_limit %s",
                    blueprint_type_id, max(1, max_production_limit or 1),
                )
        # A decryptor adds runs to every invented BPC (EVE).
        runs_per_invented_blueprint = self._runs_per_invented_blueprint(
            target_entry, max_production_limit=max_production_limit, decryptor=decryptor,
        )
        effective_probability = effective_invention_probability(probability, probability_floor)
        cost_unknown_reason: str | None = None
        if invented_runs > 0 and effective_probability <= 0:
            cost_unknown_reason = self._NO_INVENTION_PROBABILITY_REASON
            (logger.warning if log_unknown_cost else logger.debug)(
                "Invention plan for blueprint %s: %s; invention cost left unknown",
                blueprint_type_id, cost_unknown_reason,
            )
        if invented_runs > 0 and cost_unknown_reason is None:
            successful_jobs = float(invented_runs) / float(runs_per_invented_blueprint)
            # Shared with the daily planner's assigned-pilot attempts.
            invention_attempts, whole_attempts = expected_and_whole_invention_attempts(
                invented_runs, runs_per_invented_blueprint, effective_probability,
            )
        else:
            successful_jobs = 0.0
            invention_attempts = 0.0
            whole_attempts = 0

        per_attempt_materials = [dict(e) for e in (invention_job.get("materials") or []) if isinstance(e, dict)]
        # The decryptor is consumed like a datacore, one per attempt, and priced
        # like one (take-or-buy against the hub price). It is not part of the
        # fee's job value.
        per_attempt_inputs = [*per_attempt_materials, *self._decryptor_input_entries(decryptor)]
        invention_materials = [{**e, "quantity_per_attempt": int(e.get("quantity") or 0), "quantity": float(int(e.get("quantity") or 0)) * invention_attempts} for e in per_attempt_inputs]
        # Job value per attempt: 0.02 x the invented blueprint's per-run
        # manufacturing EIV (EVE), times the attempts.
        per_attempt_process_value = (
            float(invented_blueprint_eiv) * float(self._adm("industry", "invention_cost_eiv_multiplier", DEFAULT_INVENTION_COST_EIV_MULTIPLIER))
            if invented_blueprint_eiv is not None else None
        )
        invention_process_value = float(per_attempt_process_value) * invention_attempts if per_attempt_process_value is not None else None

        invention_base_time = int(invention_job.get("time_seconds") or 0)
        costs = compute_activity(activity="invention", base_time_seconds=invention_base_time, runs=cast(int, invention_attempts), process_value=invention_process_value)
        costs["runs"] = invention_attempts
        costs["estimated_item_value"] = invention_process_value
        costs["success_probability"] = effective_probability
        if chance is not None:
            costs["invention_chance"] = invention_chance_payload(chance, effective_probability)
        costs["runs_per_invented_blueprint"] = runs_per_invented_blueprint
        costs["invented_runs"] = invented_runs
        costs["expected_successful_blueprints"] = successful_jobs
        # One attempt's duration (per-job), and the whole-job elapsed time.
        costs["job_duration_seconds"] = compute_activity(activity="invention", base_time_seconds=invention_base_time, runs=1, process_value=None)["duration_seconds"]
        # The install fee of ONE attempt (the same EIV x index x reductions +
        # surcharge as above, at one attempt): the daily planner pays it per
        # whole attempt it launches (D4, Capital Reserved). None = unpriced.
        per_attempt_fee = compute_activity(
            activity="invention", base_time_seconds=invention_base_time, runs=1,
            process_value=per_attempt_process_value,
        )
        costs["job_cost_per_attempt"] = per_attempt_fee.get("total_job_cost")
        # Why an unknown fee is unknown (fix round 1): the planner names it.
        costs["job_cost_per_attempt_unknown_reason"] = (
            (per_attempt_fee.get("fee_unknown_reason") or "the invention fee could not be priced")
            if costs["job_cost_per_attempt"] is None else None
        )
        costs["whole_attempts"] = whole_attempts
        costs["elapsed_duration_seconds"] = (
            compute_activity(activity="invention", base_time_seconds=invention_base_time, runs=whole_attempts, process_value=None)["duration_seconds"]
            if whole_attempts > 0 else 0
        )
        costs["duration_is_amortized"] = invention_attempts > 0
        if cost_unknown_reason is not None:
            costs["cost_unknown_reason"] = cost_unknown_reason
            costs["total_job_cost"] = None
            costs["elapsed_duration_seconds"] = None
        if invention_attempts <= 0:
            costs["duration_seconds"] = 0
        return costs, invention_materials

    def _invention_source_copy_costs(
        self,
        *,
        compute_activity: Callable[..., dict[str, Any]],
        invention_source_row: dict[str, Any],
        invention_costs: dict[str, Any],
        available_blueprint_copy_runs_by_type_id: dict[int, Any],
        adjusted_price_map: dict[int, dict[str, Any]],
        enabled: bool,
    ) -> tuple[dict[str, Any] | None, float]:
        """Copying the T1 source blueprint for the invention attempts in
        `invention_costs` (from _expected_invention_costs): owned source-copy
        runs first, then fractional copy runs for the remaining expected
        attempts, plus a whole-run elapsed time. Consumes the owned runs from
        `available_blueprint_copy_runs_by_type_id`. Returns (costs or None when
        not `enabled` or the source has no copy time, missing source runs)."""
        attempts = float(invention_costs.get("runs") or 0.0)
        whole_attempts = int(invention_costs.get("whole_attempts") or 0)
        cost_unknown = invention_costs.get("cost_unknown_reason") is not None
        source_copying_job = invention_source_row.get("copying_job") or {}
        source_bp_type_id = int(invention_source_row.get("blueprint_type_id") or 0)
        # Expected-value model: the runs are fractional, read and kept as such
        # (int() here read a 0.7-run remainder as 0, FU-50).
        available_source_runs = max(0.0, float(available_blueprint_copy_runs_by_type_id.get(source_bp_type_id, 0) or 0))
        source_runs_used = min(available_source_runs, attempts)
        available_blueprint_copy_runs_by_type_id[source_bp_type_id] = available_source_runs - source_runs_used
        missing_source_runs = max(0.0, attempts - source_runs_used)
        # The whole-run elapsed time has its own budget of whole owned runs,
        # spent by whole attempts: the floor of the expected-value remainder
        # (2 - 0.5 - 0.5 = 1) undercounts after a second invention. It lives
        # in the same map under a negative key, so trial copies and commits
        # carry it, and it never exceeds the whole runs physically left.
        whole_source_runs_left = int(math.floor(available_source_runs + 1e-9))
        if source_bp_type_id > 0:
            whole_key = self._whole_source_runs_key(source_bp_type_id)
            whole_source_runs_left = min(
                int(available_blueprint_copy_runs_by_type_id.get(whole_key, whole_source_runs_left)), whole_source_runs_left,
            )
            available_blueprint_copy_runs_by_type_id[whole_key] = max(0, whole_source_runs_left - whole_attempts)
        whole_missing_source_runs = max(0, whole_attempts - whole_source_runs_left)
        if not (enabled and isinstance(source_copying_job, dict) and int(source_copying_job.get("time_seconds") or 0) > 0):
            return None, missing_source_runs

        source_mfg_materials = [dict(e) for e in ((invention_source_row.get("manufacturing_job") or {}).get("materials") or []) if isinstance(e, dict)]
        source_pv, _ = self._sum_estimated_item_value(source_mfg_materials, quantity_key="quantity", adjusted_price_map=adjusted_price_map)
        source_base_time = int(source_copying_job.get("time_seconds") or 0)
        source_copy_costs = compute_activity(
            activity="copying", base_time_seconds=source_base_time,
            runs=max(0, missing_source_runs),
            process_value=(float(source_pv) * float(max(0, missing_source_runs)) * float(self._adm("industry", "copy_cost_eiv_multiplier", DEFAULT_COPY_COST_EIV_MULTIPLIER)) if source_pv is not None else None),
        )
        source_copy_costs["job_duration_seconds"] = compute_activity(activity="copying", base_time_seconds=source_base_time, runs=1, process_value=None)["duration_seconds"]
        # The install fee of ONE source-copy run (D4): the planner's copy
        # action copies one run per whole invention attempt.
        source_copy_costs["job_cost_per_run"], source_copy_costs["job_cost_per_run_unknown_reason"] = self._source_copy_fee_per_run(
            compute_activity=compute_activity, invention_source_row=invention_source_row,
            adjusted_price_map=adjusted_price_map,
        )
        source_copy_costs["elapsed_duration_seconds"] = (
            compute_activity(activity="copying", base_time_seconds=source_base_time, runs=whole_missing_source_runs, process_value=None)["duration_seconds"]
            if whole_missing_source_runs > 0 else 0
        ) if not cost_unknown else None
        source_copy_costs["duration_is_amortized"] = missing_source_runs > 0
        return source_copy_costs, missing_source_runs

    def _source_copy_fee_per_run(
        self,
        *,
        compute_activity: Callable[..., dict[str, Any]],
        invention_source_row: dict[str, Any],
        adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[float | None, str | None]:
        """(fee, None) for one copy run of the T1 source blueprint: the same
        process value as _invention_source_copy_costs (source EIV x
        copy_cost_eiv_multiplier) at one run. (None, why) when the source has
        no copy time, no priced EIV or another unknown input (unknown, not
        free)."""
        source_copying_job = invention_source_row.get("copying_job") or {}
        source_base_time = int((source_copying_job if isinstance(source_copying_job, dict) else {}).get("time_seconds") or 0)
        if source_base_time <= 0:
            return None, "the T1 source blueprint has no copy time in the SDE"
        source_mfg_materials = [dict(e) for e in ((invention_source_row.get("manufacturing_job") or {}).get("materials") or []) if isinstance(e, dict)]
        source_pv, _ = self._sum_estimated_item_value(source_mfg_materials, quantity_key="quantity", adjusted_price_map=adjusted_price_map)
        if source_pv is None:
            return None, "no priced EIV for the source copy (a material has no adjusted price)"
        fee = compute_activity(
            activity="copying", base_time_seconds=source_base_time, runs=1,
            process_value=float(source_pv) * float(self._adm("industry", "copy_cost_eiv_multiplier", DEFAULT_COPY_COST_EIV_MULTIPLIER)),
        )
        total = fee.get("total_job_cost")
        return total, (None if total is not None else (fee.get("fee_unknown_reason") or "the copy fee could not be priced"))

    @staticmethod
    def _whole_source_runs_key(source_bp_type_id: int) -> int:
        """The BPC-run map key of a T1 source's whole-run budget (see
        _invention_source_copy_costs): the negated type id, never a real one."""
        return -int(source_bp_type_id)

    @staticmethod
    def _invention_elapsed_adjustment(
        invention_costs: dict[str, Any], source_copy_costs: dict[str, Any] | None,
    ) -> float | None:
        """What to add to the amortized slot time to get the whole-job elapsed
        time for one invention chain: (elapsed - amortized) for the invention
        and for its source copy. None when the elapsed time is unknown."""
        if invention_costs.get("elapsed_duration_seconds") is None:
            return None
        adjustment = float(invention_costs["elapsed_duration_seconds"]) - float(invention_costs["duration_seconds"])
        if source_copy_costs is not None:
            if source_copy_costs.get("elapsed_duration_seconds") is None:
                return None
            source_copy_counted = source_copy_costs.get("duration_seconds", 0) > 0
            adjustment += float(source_copy_costs["elapsed_duration_seconds"]) - (
                float(source_copy_costs["duration_seconds"]) if source_copy_counted else 0.0
            )
        return adjustment

    def _plan_nested_invention(
        self,
        *,
        invention_source_row: dict[str, Any],
        invention_job: dict[str, Any],
        target_entry: dict[str, Any] | None,
        blueprint_type_id: int,
        blueprint_display_name: str,
        invented_runs: int,
        max_production_limit: int,
        selected_industry_profile: dict[str, Any] | None,
        selected_character_modifiers: dict[str, Any] | None,
        character_skill_levels_by_name: dict[str, int],
        surcharge_rates: JobSurchargeRates | None,
        adjusted_market_price_map: dict[int, dict[str, Any]],
        sell_price_map: dict[int, dict[str, Any]] | None,
        available_blueprint_copy_runs_by_type_id: dict[int, Any],
        available_owned_item_quantity_by_type_id: dict[int, Any],
        invention_valuation_stock: dict[int, Any],
        owned_item_unit_cost_by_type_id: dict[int, float] | None,
        source_copy_enabled: bool,
        decryptor: DecryptorOption,
        invention_context: InventionContext,
        invented_blueprint_eiv: float | None,
    ) -> dict[str, Any]:
        """Invention of a T2 blueprint inside the recursive (sub-build) plan, on
        the same expected-value model as the top-level T2 row: fractional
        expected attempts for fees, slot time and the (amortized) inputs, whole
        attempts for the elapsed time, and no division by a zero probability."""
        def compute_activity(**kw: Any) -> dict[str, Any]:
            return self._compute_activity_job(
                **kw,
                selected_industry_profile=selected_industry_profile,
                selected_character_modifiers=selected_character_modifiers,
                character_skill_levels_by_name=character_skill_levels_by_name,
                surcharge_rates=surcharge_rates,
            )

        invention_costs, invention_materials = self._expected_invention_costs(
            compute_activity=compute_activity, invention_job=invention_job,
            target_entry=target_entry, blueprint_type_id=blueprint_type_id,
            invented_runs=invented_runs, max_production_limit=max_production_limit,
            adjusted_price_map=adjusted_market_price_map,
            decryptor=decryptor, invention_context=invention_context,
            invented_blueprint_eiv=invented_blueprint_eiv,
        )
        # Valued against the row's one invention valuation stock (FU-63): the
        # top-level invention spends it first, then each nested invention in
        # turn. The caller evaluates a child on a copy and commits it only when
        # the child is built. The whole attempts are taken from the real stock
        # in invention_procurement_materials (_build_single_product_row).
        planned_materials, material_nodes = self._plan_take_or_buy_material_nodes(
            invention_materials,
            available_owned_item_quantity_by_type_id=invention_valuation_stock,
            owned_item_unit_cost_by_type_id=owned_item_unit_cost_by_type_id,
            sell_price_map=sell_price_map,
            adjusted_price_map=adjusted_market_price_map,
        )
        invention_tags = {"invented_blueprint_type_id": int(blueprint_type_id), "invented_blueprint_name": blueprint_display_name}
        expected_inputs = [
            {**entry, **invention_tags}
            for entry in self._expected_invention_inputs(planned_materials=planned_materials, invention_costs=invention_costs)
        ]
        invention_requirements = [
            {**entry, **invention_tags}
            for entry in self._invention_whole_attempt_requirements(
                invention_job, int(invention_costs["whole_attempts"]), decryptor=decryptor,
            )
        ]
        bpc_plan = self._nested_invention_bpc_plan(
            invention_job=invention_job, invention_costs=invention_costs,
            blueprint_type_id=blueprint_type_id, blueprint_display_name=blueprint_display_name,
            max_production_limit=max_production_limit, adjusted_price_map=adjusted_market_price_map,
            decryptor=decryptor, invention_context=invention_context,
        )
        source_copy_costs, missing_source_runs = self._invention_source_copy_costs(
            compute_activity=compute_activity, invention_source_row=invention_source_row,
            invention_costs=invention_costs,
            available_blueprint_copy_runs_by_type_id=available_blueprint_copy_runs_by_type_id,
            adjusted_price_map=adjusted_market_price_map, enabled=source_copy_enabled,
        )
        cost_unknown_reason = invention_costs.get("cost_unknown_reason")
        breakdown: dict[str, Any] = {
            "activity": "invention",
            "blueprint_type_id": blueprint_type_id,
            "attempts": invention_costs["runs"],
            "whole_attempts": invention_costs["whole_attempts"],
            "successful_jobs": invention_costs["expected_successful_blueprints"],
            "success_probability": invention_costs["success_probability"],
            "runs_per_invented_blueprint": invention_costs["runs_per_invented_blueprint"],
            "invented_runs": invention_costs["invented_runs"],
            "time_seconds": invention_costs["duration_seconds"],
            "elapsed_time_seconds": invention_costs["elapsed_duration_seconds"],
            "duration_is_amortized": invention_costs["duration_is_amortized"],
            "job_cost": invention_costs.get("total_job_cost"),
        }
        if cost_unknown_reason is not None:
            breakdown["cost_unknown_reason"] = cost_unknown_reason
        nested_chance = invention_costs.get("invention_chance")
        invention_chance_notes: list[str] = []
        if isinstance(nested_chance, dict):
            breakdown["invention_chance"] = nested_chance
            if nested_chance.get("skills_unknown_reason"):
                invention_chance_notes.append(
                    f"{blueprint_display_name} sub-build: uses the SDE base chance "
                    f"({nested_chance['skills_unknown_reason']}), so its invention cost is overstated"
                )
            if nested_chance.get("note"):
                invention_chance_notes.append(f"{blueprint_display_name} sub-build: {nested_chance['note']}")
        source_copy_breakdown = None
        if source_copy_costs is not None:
            source_copy_breakdown = {
                "activity": "copying",
                "blueprint_type_id": int(invention_source_row.get("blueprint_type_id") or 0),
                "attempts": missing_source_runs,
                "time_seconds": source_copy_costs["duration_seconds"],
                "elapsed_time_seconds": source_copy_costs["elapsed_duration_seconds"],
                "duration_is_amortized": source_copy_costs["duration_is_amortized"],
                "job_cost": source_copy_costs.get("total_job_cost"),
            }
        return {
            "invention_costs": invention_costs,
            "source_copy_costs": source_copy_costs,
            "missing_source_runs": missing_source_runs,
            "expected_invention_materials": expected_inputs,
            "invention_requirements": invention_requirements,
            "bpc_plan": bpc_plan,
            "material_nodes": material_nodes,
            "breakdown": breakdown,
            "source_copy_breakdown": source_copy_breakdown,
            "cost_unknown_reason": (
                f"{blueprint_display_name} sub-build: {cost_unknown_reason}" if cost_unknown_reason is not None else None
            ),
            "elapsed_time_adjustment": self._invention_elapsed_adjustment(invention_costs, source_copy_costs),
            "invention_chance_notes": invention_chance_notes,
        }

    def _nested_invention_bpc_plan(
        self,
        *,
        invention_job: dict[str, Any],
        invention_costs: dict[str, Any],
        blueprint_type_id: int,
        blueprint_display_name: str,
        max_production_limit: int,
        adjusted_price_map: dict[int, dict[str, Any]],
        decryptor: DecryptorOption,
        invention_context: InventionContext,
    ) -> dict[str, Any] | None:
        """One nested invention as the shopping list carries it across
        batches (I3), on the top-level invention_bpc_plan's model: the runs
        invented, runs per invented BPC, whole attempts per BPC and the
        inputs of one attempt. None when nothing is invented or the odds are
        unknown (the row flags that need as unknown)."""
        invented_runs = int(invention_costs.get("invented_runs") or 0)
        if invented_runs <= 0 or int(invention_costs.get("whole_attempts") or 0) <= 0:
            return None
        runs_per_bpc = max(1, int(invention_costs.get("runs_per_invented_blueprint") or 1))
        attempts_per_bpc, unknown_reason, _chance = self._full_batch_invention_whole_attempts(
            invention_job=invention_job, blueprint_type_id=blueprint_type_id, runs=runs_per_bpc,
            max_production_limit=max_production_limit, adjusted_price_map=adjusted_price_map,
            decryptor=decryptor, invention_context=invention_context,
        )
        if unknown_reason is not None or attempts_per_bpc <= 0:
            return None
        return {
            "invented_blueprint_type_id": int(blueprint_type_id),
            "invented_blueprint_name": blueprint_display_name,
            "invented_runs": invented_runs,
            # The whole attempts batch 1 buys for it (fix round 3 netting).
            "whole_attempts": int(invention_costs.get("whole_attempts") or 0),
            "runs_per_invented_blueprint": runs_per_bpc,
            "whole_attempts_per_blueprint": int(attempts_per_bpc),
            "per_attempt_requirements": self._invention_whole_attempt_requirements(invention_job, 1, decryptor=decryptor),
        }

    @staticmethod
    def _decryptor_input_entries(decryptor: DecryptorOption) -> list[dict[str, Any]]:
        """The decryptor as an invention input, one per attempt; [] for none."""
        if decryptor.type_id is None:
            return []
        return [{
            "type_id": int(decryptor.type_id), "type_name": decryptor.type_name,
            "quantity": 1, "invention_decryptor": True,
        }]

    @classmethod
    def _invention_whole_attempt_requirements(
        cls, invention_job: dict[str, Any], whole_attempts: int, decryptor: DecryptorOption = NO_DECRYPTOR,
    ) -> list[dict[str, Any]]:
        """The invention inputs to actually buy or take: quantity_per_attempt x
        whole attempts, integer units (the amortized quantities are for cost
        only), the decryptor included. [] when the attempts are unknown or zero."""
        if whole_attempts <= 0:
            return []
        out: list[dict[str, Any]] = []
        for entry in [*(invention_job.get("materials") or []), *cls._decryptor_input_entries(decryptor)]:
            if not isinstance(entry, dict):
                continue
            per_attempt = int(entry.get("quantity") or 0)
            if int(entry.get("type_id") or 0) <= 0 or per_attempt <= 0:
                continue
            out.append({
                **dict(entry), "quantity": per_attempt * int(whole_attempts),
                "quantity_per_attempt": per_attempt, "whole_attempts": int(whole_attempts),
                "invention_input": True,
            })
        return out

    @classmethod
    def _build_total_cost_or_none(cls, child_plan: dict[str, Any]) -> float | None:
        """A sub-build's total cost for the build-vs-buy choice, or None (buy)
        when its invention cost is unknown: its estimated_total_cost then leaves
        the invention out, and building it would buy no datacores."""
        if child_plan.get("invention_cost_unknown_reasons"):
            return None
        return cls._as_float(child_plan.get("estimated_total_cost"))

    @staticmethod
    def _commit_trial(target: dict[int, Any] | None, trial: dict[int, Any]) -> None:
        """Make a built child's evaluation real: the trial map replaces the
        live one in place (the caller's dict object stays shared)."""
        if target is None:
            return
        target.clear()
        target.update(trial)

    @staticmethod
    def _merge_invention_requirements(requirements: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Whole-attempt invention requirements merged per type_id (quantities
        summed), without the per-invention tags."""
        merged: dict[int, dict[str, Any]] = {}
        for entry in requirements:
            type_id = int(entry.get("type_id") or 0)
            if type_id in merged:
                merged[type_id]["quantity"] += int(entry.get("quantity") or 0)
            else:
                merged[type_id] = {
                    key: value for key, value in entry.items()
                    if key not in ("invented_blueprint_type_id", "invented_blueprint_name", "whole_attempts", "quantity_per_attempt")
                }
        return list(merged.values())

    def _full_batch_invention_whole_attempts(
        self,
        *,
        invention_job: dict[str, Any],
        blueprint_type_id: int,
        runs: int,
        max_production_limit: int,
        adjusted_price_map: dict[int, dict[str, Any]],
        decryptor: DecryptorOption,
        invention_context: InventionContext,
    ) -> tuple[int, str | None, dict[str, Any] | None]:
        """Whole invention attempts to invent all `runs` of one batch, on the
        same model as the costed invention, why they are unknown (0 attempts
        and a reason when the odds are unknown, never a guess), and the chance
        payload. Only the attempt count is used, so the activity is not
        priced."""
        costs, _ = self._expected_invention_costs(
            compute_activity=lambda **kw: {"duration_seconds": 0, "total_job_cost": None},
            invention_job=invention_job,
            target_entry=self._invention_target_entry(invention_job, int(blueprint_type_id)),
            blueprint_type_id=int(blueprint_type_id), invented_runs=max(0, int(runs)),
            max_production_limit=max_production_limit, adjusted_price_map=adjusted_price_map,
            decryptor=decryptor, invention_context=invention_context,
            # Only the attempt count is used: the fee is not priced.
            invented_blueprint_eiv=None,
            # The row logs one "datacore need unknown" WARNING for this instead.
            log_unknown_cost=False,
        )
        return int(costs.get("whole_attempts") or 0), costs.get("cost_unknown_reason"), costs.get("invention_chance")

    @staticmethod
    def _blueprint_copy_runs_spent(before: dict[int, Any], after: dict[int, Any]) -> bool:
        """Whether planning batch 1 used any owned BPC run (any type)."""
        return any(float(after.get(type_id, 0) or 0) < float(runs or 0) for type_id, runs in before.items())

    @staticmethod
    def _later_batch_schedule(
        layout: _BatchRunLayout,
        *,
        batch_runs: int,
        batch_one_segments: list[dict[str, Any]],
        nested_copy_runs_spent: bool,
        plan_for: Callable[[list[dict[str, Any]]], dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Batches 2..N of a row: {"plans", "lead", "cycle", "unknown_reasons"},
        or None when every later batch repeats batch 1 exactly.

        A later batch's own jobs come from `layout` (it may still run on owned
        BPC runs batch 1 left, or straddle two shared invented BPCs), and its
        sub-builds have no owned BPC run batch 1 spent. `plan_for(segments)`
        plans one such batch (_plan_extra_batch); each distinct job list is
        planned once. lead = [[plan index, batches]] for batches 2, 3, ... in
        order, then cycle = [plan index, ...] repeats for every batch after
        (read by shopping_list._later_batch_plan_counts). unknown_reasons
        collects every plan's, for the row's one WARNING.
        """
        lead_keys, cycle_keys = layout.later_batch_keys(batch_runs)
        batch_one_key = _segments_key(batch_one_segments)
        if (
            not nested_copy_runs_spent
            and all(key == batch_one_key for _batch, _count, key in lead_keys)
            and all(key == batch_one_key for key in cycle_keys)
        ):
            return None
        plans: list[dict[str, Any]] = []
        index_by_key: dict[tuple, int] = {}
        unknown_reasons: list[str] = []

        def plan_index(start_batch: int, key: tuple) -> int:
            if key not in index_by_key:
                plan = plan_for(layout.segments((start_batch - 1) * max(1, int(batch_runs)), batch_runs))
                unknown_reasons.extend(r for r in plan.pop("unknown_reasons") if r not in unknown_reasons)
                index_by_key[key] = len(plans)
                plans.append(plan)
            return index_by_key[key]

        lead = [[plan_index(batch, key), count] for batch, count, key in lead_keys]
        steady_batch = 2 + sum(count for _batch, count, _key in lead_keys)
        cycle = [plan_index(steady_batch + i, key) for i, key in enumerate(cycle_keys)]
        return {"plans": plans, "lead": lead, "cycle": cycle, "unknown_reasons": unknown_reasons}

    def _plan_extra_batch(
        self,
        ctx: _ProductPlanningContext,
        *,
        manufacturing_job: dict[str, Any],
        blueprint_type_id: int,
        job_segments: list[dict[str, Any]],
        manufacturing_group: str | Collection[str] | None,
        required_skill_entries: list[dict[str, Any]],
        own_invention_source: str | None,
        invention_job: dict[str, Any],
        decryptor: DecryptorOption,
        max_production_limit: int,
        sub_build_plan_cache: dict[tuple, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """One batch after batch 1, as the shopping list needs it.

        The row's own jobs are `job_segments` (from the row's _BatchRunLayout),
        each sized at its own ME/TE with one ceiling per job. Every sub-build
        is planned afresh without owned BPC runs, so it invents, copies from an
        owned BPO or is bought; a sub-build with unknown invention odds is
        bought, as in batch 1 (FU-60). Planned against no stock: pure
        requirements (take 0), because the shopping list nets owned stock once
        over all batches.

        `own_invention_source`: when set, the runs of the jobs with that source
        are invented for this batch alone (whole attempts), for a row whose
        own invention has no shared-BPC plan.

        Returns {"job_segments", "procurement_materials",
        "invention_procurement_materials" (own + nested),
        "invention_procurement_materials_nested", "unknown_reasons"}, the lists
        keyed by type_id. Called inside quiet_warnings(): the row logs one
        WARNING for any unknown later-batch need.

        `sub_build_plan_cache` (one per row): later batches whose jobs differ
        but whose material quantities match share one sub-build plan.
        """
        segments = [dict(segment) for segment in job_segments]
        sized = self._size_manufacturing_job_segments(
            ctx, manufacturing_job=manufacturing_job, job_segments=segments,
            manufacturing_group=manufacturing_group, required_skill_entries=required_skill_entries,
        )
        materials_key = tuple(
            (int(m.get("type_id") or 0), int(m.get("quantity") or 0)) for m in sized["adjusted_material_entries"]
        )
        cached_plan = sub_build_plan_cache.get(materials_key) if sub_build_plan_cache is not None else None
        plan = cached_plan if cached_plan is not None else self._build_recursive_prerequisite_plan(
            adjusted_material_entries=cast(list[dict[str, Any]], sized["adjusted_material_entries"]),
            blueprint_type_id=blueprint_type_id,
            build_from_bpc=ctx.build_from_bpc, include_reactions=ctx.include_reactions,
            selected_industry_profile=ctx.selected_industry_profile,
            selected_character_modifiers=ctx.selected_character_modifiers,
            character_skill_levels_by_name=ctx.character_skill_levels_by_name,
            adjusted_market_price_map=ctx.adjusted_market_price_map,
            sell_price_map=ctx.material_price_map,
            blueprint_copy_assets_by_type_id=ctx.blueprint_copy_assets_by_type_id,
            available_blueprint_copy_runs_by_type_id={},
            available_owned_item_quantity_by_type_id={},
            invention_valuation_stock={},
            owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
            blueprint_original_assets_by_type_id=ctx.blueprint_original_assets_by_type_id,
            character_name_by_id=ctx.character_name_by_id, corporation_name_by_id=ctx.corporation_name_by_id,
            manufacturing_row_by_product_type_id=ctx.manufacturing_row_by_product_type_id,
            reaction_row_by_product_type_id=ctx.reaction_row_by_product_type_id,
            invention_row_by_blueprint_type_id=ctx.invention_row_by_blueprint_type_id,
            invention_context=ctx.invention_context,
        )
        if sub_build_plan_cache is not None:
            sub_build_plan_cache[materials_key] = plan
        nested_requirements = [dict(e) for e in (plan.get("invention_requirements") or []) if isinstance(e, dict)]
        unknown_reasons = [
            f"batches 2+: {reason}" for reason in dict.fromkeys(str(r) for r in (plan.get("invention_cost_unknown_reasons") or []))
        ]
        own_requirements: list[dict[str, Any]] = []
        invented_runs = sum(int(s["runs"]) for s in segments if own_invention_source and s["source"] == own_invention_source)
        if invented_runs > 0:
            attempts, _unknown_reason, _chance = self._full_batch_invention_whole_attempts(
                invention_job=invention_job, blueprint_type_id=blueprint_type_id, runs=invented_runs,
                max_production_limit=max_production_limit, adjusted_price_map=ctx.adjusted_market_price_map,
                decryptor=decryptor, invention_context=ctx.invention_context,
            )
            # Unknown odds give 0 attempts: the row already flags its own
            # later-batch datacore need as unknown, never a guessed count.
            own_requirements = self._invention_whole_attempt_requirements(invention_job, attempts, decryptor=decryptor)

        def keyed_requirements(requirements: list[dict[str, Any]]) -> dict[str, Any]:
            lines, _ = self._plan_take_or_buy_material_nodes(
                self._merge_invention_requirements(requirements),
                available_owned_item_quantity_by_type_id={},
                owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
                sell_price_map=ctx.material_price_map,
                adjusted_price_map=ctx.adjusted_market_price_map,
            )
            return self._overview_keyed_entries(lines, compactor=lambda e: e)

        return {
            "job_segments": segments,
            "procurement_materials": self._overview_keyed_entries(
                list(plan.get("procurement_materials") or []), compactor=lambda e: e,
            ),
            "invention_procurement_materials": keyed_requirements([*own_requirements, *nested_requirements]),
            "invention_procurement_materials_nested": keyed_requirements(nested_requirements),
            # I3: the nested inventions, carried across batches by the shopping list.
            "nested_invention_bpc_plans": self._compact_nested_invention_bpc_plans(
                ctx, plan.get("nested_invention_bpc_plans"),
            ),
            "unknown_reasons": unknown_reasons,
        }

    def _compact_nested_invention_bpc_plans(
        self, ctx: _ProductPlanningContext, plans: Any,
    ) -> list[dict[str, Any]]:
        """The nested inventions' BPC plans as published on the row (I3):
        each one's inputs of one attempt priced like the top-level
        invention_bpc_plan's per_attempt_materials (take nothing)."""
        out: list[dict[str, Any]] = []
        for plan in plans or []:
            if not isinstance(plan, dict):
                continue
            lines, _ = self._plan_take_or_buy_material_nodes(
                [dict(e) for e in plan.get("per_attempt_requirements") or [] if isinstance(e, dict)],
                available_owned_item_quantity_by_type_id={},
                owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
                sell_price_map=ctx.material_price_map,
                adjusted_price_map=ctx.adjusted_market_price_map,
            )
            out.append({
                **{k: v for k, v in plan.items() if k != "per_attempt_requirements"},
                "per_attempt_materials": self._overview_keyed_entries(lines, compactor=lambda e: e),
            })
        return out

    @staticmethod
    def _absorb_nested_invention(
        child_plan: dict[str, Any],
        *,
        expected_invention_materials: list[dict[str, Any]],
        invention_requirements: list[dict[str, Any]],
        invention_cost_unknown_reasons: list[str],
        invention_chance_notes: list[str],
        elapsed_time_adjustment: float | None,
        nested_invention_bpc_plans: list[dict[str, Any]] | None = None,
    ) -> float | None:
        """Carry a built child plan's nested-invention results into its parent:
        extends the lists in place and returns the combined elapsed-time
        adjustment (None once any part is unknown). Only built children are
        absorbed, so a sub-build bought in batch 1 adds nothing to any list."""
        expected_invention_materials.extend(
            dict(e) for e in (child_plan.get("expected_invention_materials") or []) if isinstance(e, dict)
        )
        invention_requirements.extend(
            dict(e) for e in (child_plan.get("invention_requirements") or []) if isinstance(e, dict)
        )
        if nested_invention_bpc_plans is not None:
            nested_invention_bpc_plans.extend(
                dict(e) for e in (child_plan.get("nested_invention_bpc_plans") or []) if isinstance(e, dict)
            )
        invention_cost_unknown_reasons.extend(str(r) for r in (child_plan.get("invention_cost_unknown_reasons") or []))
        invention_chance_notes.extend(str(r) for r in (child_plan.get("invention_chance_notes") or []))
        child_adjustment = child_plan.get("elapsed_time_adjustment", 0.0)
        if elapsed_time_adjustment is None or child_adjustment is None:
            return None
        return float(elapsed_time_adjustment) + float(child_adjustment)

    def _plan_invention_chain_for_variant(
        self,
        ctx: _ProductPlanningContext,
        *,
        row: dict[str, Any],
        blueprint_type_id: int,
        missing_top_level_copy_runs: int,
        requires_invention_chain: bool,
        owned_target_copy_runs_used: int,
        max_production_limit: int,
        compact_product: dict[str, Any],
        available_blueprint_copy_runs_by_type_id: dict[int, int],
        available_owned_item_quantity_by_type_id: dict[int, int],
        invention_valuation_stock: dict[int, Any],
        decryptor: DecryptorOption,
        per_run_material_eiv: float | None,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]], dict[str, Any] | None]:
        """Plan the invention chain for a variant (Step 4). Returns (invention_costs, tree_nodes, procurement_materials, source_copy_costs).
        `per_run_material_eiv` is this (invented) blueprint's per-run
        manufacturing EIV: the invention fee's basis."""
        invention_source_row = ctx.invention_row_by_blueprint_type_id.get(int(blueprint_type_id))
        invention_job = (invention_source_row or {}).get("invention_job")
        # The shared invention predicate: the source row really has an
        # invention job dict (a None job is no invention path, not {}).
        if not (invention_source_row and isinstance(invention_job, dict)):
            return None, [], [], None

        invention_bp_payload = invention_source_row.get("blueprint") or {}
        if not isinstance(invention_bp_payload, dict):
            invention_bp_payload = {}
        invention_bp_name = str(invention_bp_payload.get("type_name") or invention_bp_payload.get("name") or invention_source_row.get("blueprint_type_id") or "Blueprint")

        target_entry = self._invention_target_entry(invention_job, blueprint_type_id)
        target_bp_name = str(((target_entry or {}).get("product") or {}).get("type_name") if isinstance((target_entry or {}).get("product"), dict) else "").strip() or str((row.get("blueprint") or {}).get("type_name") or blueprint_type_id)

        invented_runs = max(0, missing_top_level_copy_runs) if requires_invention_chain else 0
        costs, invention_materials = self._expected_invention_costs(
            compute_activity=lambda **kw: self._plan_activity_costs(ctx, **kw),
            invention_job=invention_job, target_entry=target_entry,
            blueprint_type_id=blueprint_type_id, invented_runs=invented_runs,
            max_production_limit=max_production_limit,
            adjusted_price_map=ctx.adjusted_market_price_map,
            decryptor=decryptor, invention_context=ctx.invention_context,
            invented_blueprint_eiv=per_run_material_eiv,
        )
        invention_attempts = float(costs["runs"])

        planned_inv_materials, inv_material_nodes = self._plan_take_or_buy_material_nodes(
            invention_materials,
            # Values the amortized inputs only, against the row's invention
            # valuation stock (spent here first, then by the nested inventions,
            # FU-63); the whole attempts are taken from the real stock in
            # invention_procurement_materials (integer units).
            available_owned_item_quantity_by_type_id=invention_valuation_stock,
            owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
            sell_price_map=ctx.material_price_map,
            adjusted_price_map=ctx.adjusted_market_price_map,
        )

        tree_nodes: list[dict[str, Any]] = []
        if requires_invention_chain or owned_target_copy_runs_used <= 0:
            tree_nodes.append(self._job_tree_node(
                label=self._ACTIVITY_LABELS["invention"], node_type="activity", activity="invention",
                blueprint_name=target_bp_name, blueprint_type_id=blueprint_type_id,
                runs=invention_attempts, duration_seconds=costs["duration_seconds"],
                direct_duration_seconds=costs["duration_seconds"],
                job_duration_seconds=costs["job_duration_seconds"],
                elapsed_duration_seconds=costs["elapsed_duration_seconds"],
                duration_is_amortized=costs["duration_is_amortized"],
                job_cost=costs.get("total_job_cost"), total_job_cost=costs.get("total_job_cost"),
                category_name=str(compact_product.get("category_name") or "") or None,
                meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                recommendation_action=(
                    "invent" if invention_attempts > 0
                    else ("unknown" if costs.get("cost_unknown_reason") else "take")
                ),
                **({"cost_unknown_reason": costs["cost_unknown_reason"]} if costs.get("cost_unknown_reason") else {}),
                children=([self._job_tree_node(label=self._ACTIVITY_LABELS["materials"], node_type="materials", activity="materials", children=inv_material_nodes)] if invention_attempts > 0 and inv_material_nodes else []),
            ))

        # Source blueprint copying for invention
        source_copy_costs, missing_source_runs = self._invention_source_copy_costs(
            compute_activity=lambda **kw: self._plan_activity_costs(ctx, **kw),
            invention_source_row=invention_source_row, invention_costs=costs,
            available_blueprint_copy_runs_by_type_id=available_blueprint_copy_runs_by_type_id,
            adjusted_price_map=ctx.adjusted_market_price_map,
            enabled=requires_invention_chain,
        )
        # The planner's copy action copies one source run per attempt (D4).
        # Computed even when batch 1 copies nothing (an owned T2 BPC covers it):
        # the later batches' plan carries it too.
        costs["source_copy_job_cost_per_run"], costs["source_copy_job_cost_per_run_unknown_reason"] = (
            (source_copy_costs.get("job_cost_per_run"), source_copy_costs.get("job_cost_per_run_unknown_reason"))
            if source_copy_costs is not None
            else self._source_copy_fee_per_run(
                compute_activity=lambda **kw: self._plan_activity_costs(ctx, **kw),
                invention_source_row=invention_source_row, adjusted_price_map=ctx.adjusted_market_price_map,
            )
        )
        if source_copy_costs is not None:
            tree_nodes.append(self._job_tree_node(
                label=self._ACTIVITY_LABELS["copying"], node_type="activity", activity="copying",
                blueprint_name=invention_bp_name, runs=max(0, missing_source_runs),
                duration_seconds=source_copy_costs["duration_seconds"],
                direct_duration_seconds=source_copy_costs["duration_seconds"],
                job_duration_seconds=source_copy_costs["job_duration_seconds"],
                elapsed_duration_seconds=source_copy_costs["elapsed_duration_seconds"],
                duration_is_amortized=source_copy_costs["duration_is_amortized"],
                job_cost=source_copy_costs.get("total_job_cost"), total_job_cost=source_copy_costs.get("total_job_cost"),
                category_name=str(compact_product.get("category_name") or "") or None,
                meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                recommendation_action="copy", children=[],
            ))

        return costs, tree_nodes, planned_inv_materials if requires_invention_chain else [], source_copy_costs

    def _plan_research_chain_for_variant(
        self,
        ctx: _ProductPlanningContext,
        *,
        row: dict[str, Any],
        per_run_material_eiv: float | None,
    ) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
        """Plan ME/TE research activities (Step 4). Returns list of (activity_name, costs, tree_node)."""
        results: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        for activity_name, source_field in [("research_material", "research_material_job"), ("research_time", "research_time_job")]:
            base_level_one = int(((row.get(source_field) or {}).get("time_seconds") or 0))
            target_duration = self._research_target_duration_seconds(level_one_duration_seconds=base_level_one, target_level=self._MAX_RESEARCH_LEVEL)
            if target_duration <= 0:
                continue
            process_value = self._research_process_value(per_run_material_eiv, self._MAX_RESEARCH_LEVEL)
            costs = self._plan_activity_costs(ctx, activity=activity_name, base_time_seconds=target_duration, runs=1, process_value=process_value)
            costs["estimated_item_value"] = per_run_material_eiv
            tree_node = self._job_tree_node(
                label=self._ACTIVITY_LABELS.get(activity_name, activity_name),
                node_type="activity", activity=activity_name, runs=None,
                duration_seconds=costs["duration_seconds"], direct_duration_seconds=costs["duration_seconds"],
                job_cost=costs.get("total_job_cost"), total_job_cost=costs.get("total_job_cost"), children=[],
            )
            results.append((activity_name, costs, tree_node))
        return results

    def _with_component_job_fee(self, ctx: _ProductPlanningContext, entry: dict[str, Any]) -> dict[str, Any]:
        """A parent's material entry, plus `component_job_fee_per_run` when the
        material is itself manufactured (Task 8 fix round 1): the daily
        planner's sub_manufacture fee when the component has no overview row
        of its own (e.g. owned_blueprints_scope excludes corp assets)."""
        fee, reason = self._component_job_fee_per_run(ctx, int(entry.get("type_id") or 0))
        if fee is not None:
            return {**entry, "component_job_fee_per_run": fee}
        if reason is not None:
            return {**entry, "component_job_fee_per_run_unknown_reason": reason}
        return entry

    def _component_job_fee_per_run(self, ctx: _ProductPlanningContext, component_type_id: int) -> tuple[float | None, str | None]:
        """The install fee of ONE manufacturing run of a component in this
        refresh's industry profile (the parent's structure, F-Q2): the SDE
        base (ME 0) materials x adjusted prices as the EIV, priced by
        _plan_activity_costs -- the same formula as a row's own
        manufacturing_job_cost. (None, None) for a material with no
        manufacturing blueprint (bought, no fee); (None, why) when the
        component's time, EIV or another input is unknown (unknown, not free)."""
        component_row = ctx.manufacturing_row_by_product_type_id.get(int(component_type_id))
        job = (component_row or {}).get("manufacturing_job")
        if not isinstance(job, dict):
            return None, None
        base_time = int(job.get("time_seconds") or 0)
        if base_time <= 0:
            return None, "the component's blueprint has no manufacturing time in the SDE"
        materials = [dict(e) for e in (job.get("materials") or []) if isinstance(e, dict)]
        process_value, _ = self._sum_estimated_item_value(
            materials, quantity_key="quantity", adjusted_price_map=ctx.adjusted_market_price_map,
        )
        if process_value is None:
            return None, "no priced EIV for the component (a material has no adjusted price)"
        product = next(
            (p for p in (job.get("products") or []) if isinstance(p, dict) and int(p.get("type_id") or 0) == int(component_type_id)),
            {},
        )
        fee = self._plan_activity_costs(
            ctx, activity="manufacturing", base_time_seconds=base_time, runs=1, process_value=process_value,
            manufacturing_group=self._manufacturing_rig_groups(product),
        )
        total = fee.get("total_job_cost")
        return total, (None if total is not None else (fee.get("fee_unknown_reason") or "the component fee could not be priced"))

    def _research_process_value(self, per_run_material_eiv: float | None, target_level: int) -> float | None:
        """The job value of a research job from level 0 to `target_level`:
        per-run material EIV x research_cost_eiv_multiplier (0.02) x EVE's
        level multiplier (job_cost.research_level_multiplier: 1 at level 1 ...
        2438.1 at level 10). Rank-independent: real ESI jobs of rank 1 to 40
        cost the same per ISK of EIV x level multiplier (Task 8b fit). The
        one formula for every research fee (the SDE research chains and the
        daily planner's ME/TE research actions). None when the EIV is unknown."""
        if per_run_material_eiv is None:
            return None
        level_multiplier = research_level_multiplier(0, int(target_level))
        if level_multiplier <= 0:
            return None
        return float(per_run_material_eiv) * float(self._adm("industry", "research_cost_eiv_multiplier", DEFAULT_RESEARCH_COST_EIV_MULTIPLIER)) * level_multiplier

    def _research_job_cost_by_level(
        self,
        ctx: _ProductPlanningContext,
        *,
        row: dict[str, Any],
        per_run_material_eiv: float | None,
    ) -> tuple[dict[str, list[float] | None], dict[str, str]]:
        """(tables, reasons). The install fee of researching this blueprint from level 0 to each
        level 0..max (A2), per research activity: a list indexed by level,
        [0] = 0.0. Priced exactly like _plan_research_chain_for_variant (the
        same process value, cost index, structure/rig/implant reductions and
        surcharge). The fee is linear in the process value, so a job from
        level a to b costs [b] - [a]. None for an activity whose fee is
        unknown at any level (no research time, no EIV, no cost index or no
        facility tax); `reasons[activity]` then says why."""
        tables: dict[str, list[float] | None] = {}
        reasons: dict[str, str] = {}
        for activity_name, source_field in [("research_material", "research_material_job"), ("research_time", "research_time_job")]:
            base_level_one = int(((row.get(source_field) or {}).get("time_seconds") or 0))
            fees: list[float] | None = [0.0]
            for level in range(1, self._MAX_RESEARCH_LEVEL + 1):
                duration = self._research_target_duration_seconds(level_one_duration_seconds=base_level_one, target_level=level)
                if duration <= 0:
                    fees = None
                    reasons[activity_name] = "the blueprint has no research time in the SDE"
                    break
                priced = self._plan_activity_costs(
                    ctx, activity=activity_name, base_time_seconds=duration, runs=1,
                    process_value=self._research_process_value(per_run_material_eiv, level),
                )
                fee = priced.get("total_job_cost")
                if fee is None:
                    fees = None
                    reasons[activity_name] = priced.get("fee_unknown_reason") or "the research fee could not be priced"
                    break
                fees.append(float(fee))
            tables[activity_name] = fees
        return tables, reasons

    def _research_duration_by_level(
        self, ctx: _ProductPlanningContext, *, row: dict[str, Any], skilled: bool = True,
    ) -> dict[str, list[int] | None]:
        """The duration in seconds of researching this blueprint from level 0
        to each level 0..max, per research activity: a list indexed by level,
        [0] = 0. The model the research fee uses (Task 8b): the SDE rank
        (level-1 time / 105 s) x EVE's rank-1 level table, then the skill,
        structure, rig and implant time reductions of _plan_activity_costs.
        A job from level a to b takes [b] - [a] (EVE's table is cumulative).
        None for an activity without a research time in the SDE.

        skilled=False leaves the research skills out (structure, rig and
        implant reductions stay): the daily planner applies the ASSIGNED
        pilot's skills to it (research_time.research_skill_time_multiplier)."""
        tables: dict[str, list[int] | None] = {}
        for activity_name, source_field in [("research_material", "research_material_job"), ("research_time", "research_time_job")]:
            base_level_one = int(((row.get(source_field) or {}).get("time_seconds") or 0))
            if base_level_one <= 0:
                tables[activity_name] = None
                continue
            durations = [0]
            for level in range(1, self._MAX_RESEARCH_LEVEL + 1):
                target = self._research_target_duration_seconds(level_one_duration_seconds=base_level_one, target_level=level)
                if skilled:
                    priced = self._plan_activity_costs(ctx, activity=activity_name, base_time_seconds=target, runs=1, process_value=None)
                else:
                    priced = self._compute_activity_job(
                        activity=activity_name, base_time_seconds=target, runs=1, process_value=None,
                        selected_industry_profile=ctx.selected_industry_profile,
                        selected_character_modifiers=ctx.selected_character_modifiers,
                        character_skill_levels_by_name={}, surcharge_rates=ctx.surcharge_rates,
                    )
                durations.append(int(priced["duration_seconds"]))
            tables[activity_name] = durations
        return tables

    @staticmethod
    def _row_invents(ctx: _ProductPlanningContext, blueprint_type_id: int) -> bool:
        """Whether rows of this blueprint invent (in batch 1 or the later
        batches): the predicate _build_single_product_row resolves its
        decryptor under."""
        source = ctx.invention_row_by_blueprint_type_id.get(int(blueprint_type_id))
        return (
            bool(ctx.build_from_bpc)
            and isinstance((source or {}).get("invention_job"), dict)
            and not ctx.blueprint_original_assets_by_type_id.get(int(blueprint_type_id))
        )

    def _build_product_row(self, ctx: _ProductPlanningContext, **variant: Any) -> dict[str, Any] | None:
        """One overview row, invented with its blueprint's best decryptor; the
        choice is on manufacturing_job.invention_decryptor."""
        blueprint_type_id = int(variant["row"].get("blueprint_type_id") or 0)
        if not self._row_invents(ctx, blueprint_type_id):
            return self._build_single_product_row(ctx, **variant)
        choice = self._decryptor_choice_for(ctx, blueprint_type_id)
        row_out = self._build_single_product_row(ctx, decryptor=choice.option, **variant)
        if row_out is not None:
            row_out["manufacturing_job"]["invention_decryptor"] = decryptor_choice_payload(choice)
        return row_out

    def _decryptor_choice_for(self, ctx: _ProductPlanningContext, blueprint_type_id: int) -> DecryptorChoice:
        """The decryptor to invent `blueprint_type_id` with: chosen once per
        refresh by invention_model.select_best_decryptor and memoized on ctx.

        Thread-safe without a lock: two workers may evaluate the same
        blueprint at once (same inputs, same result), and setdefault keeps the
        first, so every caller uses one choice. No lock is held while
        evaluating, so nested evaluations on different threads cannot
        deadlock."""
        blueprint_type_id = int(blueprint_type_id)
        cached = ctx.decryptor_choice_by_blueprint_type_id.get(blueprint_type_id)
        if cached is not None:
            return cached
        if not ctx.decryptor_selection_enabled:
            choice = DecryptorChoice(NO_DECRYPTOR, DECRYPTOR_SELECTION_OFF_REASON)
        elif len(ctx.decryptor_options) <= 1:
            choice = DecryptorChoice(NO_DECRYPTOR, NO_DECRYPTOR_DATA_REASON)
        else:
            evaluating = getattr(ctx.decryptor_evaluation_guard, "blueprint_type_ids", None)
            if evaluating is None:
                evaluating = set()
                ctx.decryptor_evaluation_guard.blueprint_type_ids = evaluating
            if blueprint_type_id in evaluating:
                # Met again inside its own evaluation (a material cycle): this
                # inner use gets no decryptor, and nothing is cached for it.
                return DecryptorChoice(NO_DECRYPTOR, "no decryptor: met inside its own decryptor evaluation")
            evaluating.add(blueprint_type_id)
            try:
                choice = self._evaluate_decryptors(ctx, blueprint_type_id)
            finally:
                evaluating.discard(blueprint_type_id)
        return ctx.decryptor_choice_by_blueprint_type_id.setdefault(blueprint_type_id, choice)

    def _evaluate_decryptors(self, ctx: _ProductPlanningContext, blueprint_type_id: int) -> DecryptorChoice:
        """Build one invention cycle of the blueprint's own row per decryptor
        option and let select_best_decryptor pick (amendment A1).

        One cycle = the runs one invented BPC yields with that option (SDE
        runs + its run modifier), every run invented: the expected attempts
        are then exactly 1 / chance, so the row's materials, amortized
        invention inputs (decryptor included) and job fees are the cycle's.
        The builds are thrown away, so their WARNINGs are dropped
        (quiet_warnings); the row that is kept logs its own."""
        row = ctx.blueprint_row_by_type_id.get(blueprint_type_id) or {}
        manufacturing_job = row.get("manufacturing_job") or {}
        products = [
            p for p in (manufacturing_job.get("products") or [])
            if isinstance(p, dict) and int(p.get("type_id") or 0) > 0
        ] if isinstance(manufacturing_job, dict) else []
        if not products:
            return DecryptorChoice(NO_DECRYPTOR, "no decryptor: the blueprint makes no product to compare on")
        source = ctx.invention_row_by_blueprint_type_id.get(blueprint_type_id) or {}
        invention_job = source.get("invention_job") if isinstance(source.get("invention_job"), dict) else {}
        target_entry = self._invention_target_entry(invention_job, blueprint_type_id)
        max_production_limit = int(manufacturing_job.get("max_production_limit") or 0)
        evaluations: list[DecryptorEvaluation] = []
        with quiet_warnings():
            for option in ctx.decryptor_options:
                skip_reason = self._decryptor_price_skip_reason(ctx, option)
                if skip_reason is not None:
                    evaluations.append(DecryptorEvaluation(option, None, skip_reason))
                    continue
                candidate = self._build_single_product_row(
                    ctx, row=row, raw_product=products[0], row_index=0, product_index=0,
                    blueprint_copy_assets=[], blueprint_original_asset=None,
                    effective_runs=self._runs_per_invented_blueprint(
                        target_entry, max_production_limit=max_production_limit, decryptor=option,
                    ),
                    decryptor=option, fully_invented=True,
                )
                evaluations.append(self._cycle_evaluation(ctx, option, candidate))
        return select_best_decryptor(evaluations)

    @classmethod
    def _decryptor_price_skip_reason(cls, ctx: _ProductPlanningContext, option: DecryptorOption) -> str | None:
        """Why a decryptor cannot be a candidate: it has no hub market price.
        An adjusted-price fallback would make an unbuyable decryptor look cheap."""
        if option.type_id is None:
            return None
        unit_price = cls._as_float((ctx.material_price_map.get(int(option.type_id)) or {}).get("unit_price"))
        if unit_price is None or unit_price <= 0:
            return f"no market price for {option.type_name}"
        return None

    _MARKET_FEES_UNKNOWN_FALLBACK_REASON = (
        "sales tax unknown (no character to read Accounting from), so net proceeds are unknown: "
        "chose lowest cost per unit"
    )

    @classmethod
    def _net_sale_fraction(cls, fee_context: dict[str, Any]) -> float | None:
        """The share of the gross sale value the corp keeps, as
        _enrich_product_rows_with_sale_proceeds computes net_proceeds:
        1 - broker fee (when it applies) - sales tax. None when the sales tax
        is unknown (that step then leaves net_proceeds None too)."""
        rates = (fee_context or {}).get("rates") or {}
        tax = cls._as_float(rates.get("sales_tax_fraction"))
        if tax is None:
            return None
        broker = cls._as_float(rates.get("broker_fee_fraction"))
        applies = bool((fee_context or {}).get("broker_fee_applies"))
        return 1.0 - (float(broker) if applies and broker is not None else 0.0) - float(tax)

    @classmethod
    def _cycle_evaluation(
        cls, ctx: _ProductPlanningContext, option: DecryptorOption, row_out: dict[str, Any] | None,
    ) -> DecryptorEvaluation:
        """A candidate cycle row's cost per unit and profit per cycle.

        The profit is the cycle's net proceeds, the way the row's
        profit_amount is: units x the row's sell price (market_unit_price) x
        (1 - broker fee - sales tax), minus the cycle's cost. Unknown when the
        sell price is unknown or not positive, or the sales tax is unknown;
        select_best_decryptor then falls back to cost per unit with that
        reason. A candidate without a comparable cost gets a skip_reason."""
        quantity = int((row_out or {}).get("quantity") or 0)
        total_cost, why = cls._row_total_cost_estimate(ctx, row_out)
        if total_cost is None or quantity <= 0:
            return DecryptorEvaluation(
                option, None, why or "the candidate build made no units", units_per_cycle=quantity or None,
            )
        sell_price = cls._as_float((row_out or {}).get("market_unit_price"))
        net_fraction = cls._net_sale_fraction(ctx.market_fee_context)
        profit: float | None = None
        profit_unknown_reason: str | None = None
        if sell_price is None or sell_price <= 0:
            profit_unknown_reason = None  # the default SELL_PRICE_UNKNOWN_FALLBACK_REASON
        elif net_fraction is None:
            profit_unknown_reason = cls._MARKET_FEES_UNKNOWN_FALLBACK_REASON
        else:
            profit = float(sell_price) * quantity * net_fraction - total_cost
        return DecryptorEvaluation(
            option, total_cost / quantity, profit_per_cycle=profit, units_per_cycle=quantity,
            profit_unknown_reason=profit_unknown_reason,
        )

    @classmethod
    def _row_total_cost_estimate(
        cls, ctx: _ProductPlanningContext, row_out: dict[str, Any] | None,
    ) -> tuple[float | None, str | None]:
        """(a row's total cost before the pricing step, None) or (None, why).

        Priced the way _enrich_product_rows_with_material_prices prices it:
        each procurement and amortized invention line at its own unit price,
        else the hub price (ctx.material_price_map), plus the job fees (no fee
        priced at all reads as 0, the same for every candidate of one row).
        Unknown when any line is unpriced or the invention cost is unknown: a
        partial cost cannot be compared."""
        if not isinstance(row_out, dict):
            return None, "the candidate row could not be built"
        mj = row_out.get("manufacturing_job") or {}
        if mj.get("invention_cost_unknown_reason"):
            return None, f"invention cost unknown: {mj['invention_cost_unknown_reason']}"

        def unit_price(entry: dict[str, Any]) -> float | None:
            own = cls._as_float(entry.get("unit_price"))
            if own is not None and own > 0:
                return own
            hub = cls._as_float((ctx.material_price_map.get(int(entry.get("type_id") or 0)) or {}).get("unit_price"))
            return hub if hub is not None and hub > 0 else None

        total = 0.0
        unpriced: list[str] = []
        lines = [
            (entry, False) for entry in (mj.get("procurement_materials") or mj.get("materials") or {}).values()
        ] + [(entry, True) for entry in (mj.get("expected_invention_materials") or {}).values()]
        for entry, fractional in lines:
            if not isinstance(entry, dict):
                continue
            price = unit_price(entry)
            if price is None:
                unpriced.append(str(entry.get("type_name") or entry.get("type_id") or "?"))
                continue
            total += price * (float(entry.get("quantity") or 0.0) if fractional else int(entry.get("quantity") or 0))
        if unpriced:
            return None, f"an input has no price: {', '.join(dict.fromkeys(unpriced))}"
        return total + float(mj.get("total_job_cost") or 0.0), None

    def _build_single_product_row(
        self,
        ctx: _ProductPlanningContext,
        *,
        row: dict[str, Any],
        raw_product: dict[str, Any],
        row_index: int,
        product_index: int,
        blueprint_copy_assets: list,
        blueprint_original_asset: Any,
        effective_runs: int,
        decryptor: DecryptorOption | None = None,
        fully_invented: bool = False,
    ) -> dict[str, Any] | None:
        """Build a single product row from a blueprint variant (Step 3).

        `decryptor`: None (the default) takes the refresh's choice from
        ctx.invention_context.decryptor_for_blueprint, the same source the
        nested planner uses. An explicit option overrides it, e.g. to build
        one decryptor candidate.

        `fully_invented`: plan every run as invented, whatever BPC runs of
        this blueprint are owned (a decryptor candidate's invention cycle).
        """
        manufacturing_job = row.get("manufacturing_job") or {}
        if not isinstance(manufacturing_job, dict):
            return None
        blueprint_type_id = int(row.get("blueprint_type_id") or 0)
        matched_blueprint_originals = ctx.blueprint_original_assets_by_type_id.get(blueprint_type_id) or []
        blueprint_copy_asset = blueprint_copy_assets[0] if blueprint_copy_assets else None
        blueprint_copy_runs = sum(max(0, int(getattr(a, "blueprint_runs", 0) or 0)) for a in blueprint_copy_assets)
        product_type_id = int(raw_product.get("type_id") or 0)
        product_quantity_per_run = int(raw_product.get("quantity") or 0)
        max_production_limit = int(manufacturing_job.get("max_production_limit") or 0)

        # Per-variant mutable state (each variant gets its own copy). The BPC
        # run map also carries negative keys (_whole_source_runs_key): a T1
        # source's whole-run invention budget, not a type id.
        available_blueprint_copy_runs_by_type_id = dict(ctx.available_blueprint_copy_runs_by_type_id_base)
        available_owned_item_quantity_by_type_id = dict(ctx.available_owned_item_quantity_by_type_id_base)
        if bool(ctx.build_from_bpc):
            if blueprint_copy_assets:
                available_blueprint_copy_runs_by_type_id[blueprint_type_id] = max(0, blueprint_copy_runs)
            elif blueprint_original_asset is not None:
                available_blueprint_copy_runs_by_type_id[blueprint_type_id] = 0
            if fully_invented:
                available_blueprint_copy_runs_by_type_id[blueprint_type_id] = 0
        # Owned BPC runs before batch 1 spends any: a later batch has none of
        # what batch 1 spent (see the extra_batch_schedule below).
        copy_runs_before_batch_one = dict(available_blueprint_copy_runs_by_type_id)

        compact_product = dict(raw_product)
        compact_product["quantity"] = product_quantity_per_run * effective_runs

        # Blueprint SDE payload
        blueprint_sde_payload: dict[str, Any] = {
            **dict(row.get("blueprint") or {}),
            "blueprint_type_id": blueprint_type_id,
            "copying": {"time": int(((row.get("copying_job") or {}).get("time_seconds") or 0))},
            "research_material": {"time": int(((row.get("research_material_job") or {}).get("time_seconds") or 0))},
            "research_time": {"time": int(((row.get("research_time_job") or {}).get("time_seconds") or 0))},
        }
        inv_job_raw = row.get("invention_job") or {}
        if isinstance(inv_job_raw, dict) and inv_job_raw:
            blueprint_sde_payload["invention"] = {
                "materials": self._overview_keyed_entries([dict(e) for e in (inv_job_raw.get("materials") or [])], compactor=self._compact_material_entry),
                "products": self._compact_invention_product_entries([dict(e) for e in (inv_job_raw.get("products") or [])]),
                "skills": self._overview_keyed_entries([dict(e) for e in (inv_job_raw.get("skill_entries") or [])], compactor=self._compact_skill_entry),
                "time": int(inv_job_raw.get("time_seconds") or 0),
            }

        # Skills
        skill_compactor = lambda entry: self._compact_required_skill_entry(entry, ctx.character_skill_levels)
        manufacturing_skill_entries = self._overview_keyed_entries(
            [dict(e) for e in (manufacturing_job.get("skill_entries") or [])], compactor=skill_compactor,
        )
        manufacturing_skill_requirements_met = all(
            bool(se.get("skill_requirement_met", False)) for se in manufacturing_skill_entries.values() if isinstance(se, dict)
        )
        manufacturing_skills: dict[str, Any] = {**manufacturing_skill_entries, "skill_requirements_met": manufacturing_skill_requirements_met}
        manufacturing_skill_entry_list = [se for se in manufacturing_skill_entries.values() if isinstance(se, dict)]

        invention_source_row = ctx.invention_row_by_blueprint_type_id.get(int(blueprint_type_id))
        # The same predicate as the nested planner: the source row really has an invention job.
        has_top_level_invention_path = isinstance((invention_source_row or {}).get("invention_job"), dict)
        invention_job = (invention_source_row or {}).get("invention_job") or {}
        if decryptor is None:
            # One source for the decryptor, shared with the nested planner. It
            # is resolved only for a row that invents (the condition of the
            # invented baseline below), so a non-inventing row never asks.
            # Callers that evaluate a candidate pass `decryptor=` explicitly.
            decryptor = (
                ctx.invention_context.decryptor_for_blueprint(int(blueprint_type_id))
                if bool(ctx.build_from_bpc) and has_top_level_invention_path and not matched_blueprint_originals
                else NO_DECRYPTOR
            )
        # Blueprint source resolution (Step 3a)
        blueprint_material_efficiency = 0
        blueprint_time_efficiency = 0
        blueprint_source_kind = "unowned"
        include_copying_job = False
        include_sde_research_chain = False
        if bool(ctx.build_from_bpc):
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
                if has_top_level_invention_path and not matched_blueprint_originals:
                    # The BPC is invented: ME 2 / TE 4 plus the decryptor
                    # (EVE). A plain unowned T1 BPC stays at ME 0 / TE 0.
                    blueprint_material_efficiency, blueprint_time_efficiency = invented_blueprint_efficiency(decryptor)
        else:
            if blueprint_original_asset is not None:
                blueprint_material_efficiency = int(blueprint_original_asset.blueprint_material_efficiency or 0)
                blueprint_time_efficiency = int(blueprint_original_asset.blueprint_time_efficiency or 0)
                blueprint_source_kind = "owned_blueprint_original"
            else:
                blueprint_material_efficiency = int(self._adm("industry", "max_blueprint_material_efficiency", self._MAX_BLUEPRINT_MATERIAL_EFFICIENCY))
                blueprint_time_efficiency = int(self._adm("industry", "max_blueprint_time_efficiency", self._MAX_BLUEPRINT_TIME_EFFICIENCY))
                blueprint_source_kind = "blueprint_sde_fallback"
                include_sde_research_chain = True

        # Owned BPC runs this batch uses, and the runs it still lacks.
        available_target_copy_runs = int(available_blueprint_copy_runs_by_type_id.get(blueprint_type_id, 0)) if bool(ctx.build_from_bpc) else 0
        owned_target_copy_runs_used = min(max(0, available_target_copy_runs), effective_runs) if bool(ctx.build_from_bpc) else 0
        if bool(ctx.build_from_bpc):
            available_blueprint_copy_runs_by_type_id[blueprint_type_id] = max(0, available_target_copy_runs - owned_target_copy_runs_used)
        missing_top_level_copy_runs = max(0, effective_runs - owned_target_copy_runs_used) if bool(ctx.build_from_bpc) else 0

        # Manufacturing costs via unified calculator (Step 2)
        manufacturing_group = self._infer_manufacturing_group(compact_product)
        rig_groups = self._manufacturing_rig_groups(compact_product)
        # Where each run comes from (see _BatchRunLayout): one job per owned
        # BPC (D3), then the next source, one job per BPC when invented.
        batch_run_layout = self._top_level_batch_run_layout(
            ctx, blueprint_copy_assets=blueprint_copy_assets, owned_copy_runs=available_target_copy_runs,
            blueprint_source_kind=blueprint_source_kind, matched_blueprint_originals=matched_blueprint_originals,
            has_top_level_invention_path=has_top_level_invention_path, invention_job=invention_job,
            blueprint_type_id=blueprint_type_id, max_production_limit=max_production_limit, decryptor=decryptor,
            material_efficiency=blueprint_material_efficiency, time_efficiency=blueprint_time_efficiency,
        )
        job_segments = batch_run_layout.segments(0, effective_runs)
        sized_batch = self._size_manufacturing_job_segments(
            ctx, manufacturing_job=manufacturing_job, job_segments=job_segments,
            manufacturing_group=rig_groups, required_skill_entries=manufacturing_skill_entry_list,
        )
        mfg_costs = sized_batch["costs"]
        # Row-level reductions: runs-weighted over the jobs when they differ
        # (labelled reduction_basis); job_segments has each job's own values.
        manufacturing_material_reduction = sized_batch["material_reduction"]
        manufacturing_time_reduction = sized_batch["time_reduction"]
        manufacturing_cost_reduction = mfg_costs["cost_reduction"]
        adjusted_material_entries = cast(list[dict[str, Any]], sized_batch["adjusted_material_entries"])
        base_material_entries = cast(list[dict[str, Any]], sized_batch["base_material_entries"])

        # EIV calculations
        base_material_eiv_total, base_material_eiv_priced_count, base_material_eiv_line_count = self._sum_estimated_item_value_counted(base_material_entries, quantity_key="quantity", adjusted_price_map=ctx.adjusted_market_price_map)
        per_run_material_eiv, _ = self._sum_estimated_item_value(adjusted_material_entries, quantity_key="quantity_per_run", adjusted_price_map=ctx.adjusted_market_price_map)
        product_eiv_unit, product_eiv_source = self._resolve_eiv_pricing(type_id=product_type_id, type_payload=compact_product, adjusted_price_map=ctx.adjusted_market_price_map)
        product_eiv_total = float(product_eiv_unit) * int(compact_product.get("quantity") or 0) if product_eiv_unit is not None else None

        # Recompute manufacturing job cost with actual process value
        manufacturing_cost_index = self._system_cost_index(profile_payload=ctx.selected_industry_profile, activity="manufacturing")
        manufacturing_job_cost = self._job_install_cost(activity="manufacturing", process_value=base_material_eiv_total, system_index=manufacturing_cost_index, structure_cost_reduction=manufacturing_cost_reduction, surcharge_rates=ctx.surcharge_rates)
        direct_manufacturing_time_seconds = sum(int(segment["duration_seconds"]) for segment in job_segments)

        # Activity breakdown
        activity_breakdown: dict[str, Any] = {
            "manufacturing": {
                "activity": "manufacturing", "duration_seconds": direct_manufacturing_time_seconds,
                "base_duration_seconds": int(manufacturing_job.get("time_seconds") or 0) * effective_runs,
                "time_reduction": manufacturing_time_reduction, "material_reduction": manufacturing_material_reduction,
                "cost_reduction": manufacturing_cost_reduction, "cost_index": manufacturing_cost_index,
                "estimated_item_value": base_material_eiv_total,
                "estimated_item_value_priced_material_count": base_material_eiv_priced_count,
                "estimated_item_value_material_count": base_material_eiv_line_count,
                **manufacturing_job_cost,
                # The reductions above are runs-weighted over job_segments.
                **({"reduction_basis": "runs_weighted_job_segments"} if sized_batch["reductions_differ"] else {}),
            }
        }
        total_time_seconds = direct_manufacturing_time_seconds
        total_job_cost = float(manufacturing_job_cost.get("total_job_cost") or 0.0)
        priced_job_count = 1 if manufacturing_job_cost.get("total_job_cost") is not None else 0
        prerequisite_tree_nodes: list[dict[str, Any]] = []
        top_level_procurement_materials = cast(list[dict[str, Any]], [*adjusted_material_entries])

        # BPC allocation + prerequisite handling
        requires_copying_job = bool(include_copying_job) and missing_top_level_copy_runs > 0
        requires_invention_chain = missing_top_level_copy_runs > 0 and not matched_blueprint_originals
        owned_top_level_copy_allocations = self._allocate_owned_blueprint_copy_run_usage(blueprint_copy_assets, requested_runs=owned_target_copy_runs_used, available_total_runs=owned_target_copy_runs_used)

        if bool(ctx.build_from_bpc) and owned_top_level_copy_allocations:
            for consumed_asset, consumed_runs in owned_top_level_copy_allocations:
                fields = self._owned_blueprint_copy_node_fields(
                    consumed_asset, blueprint_name=str((row.get("blueprint") or {}).get("type_name") or blueprint_type_id),
                    recommendation_action="take", runs_required=consumed_runs,
                    category_name=str(compact_product.get("category_name") or "") or None,
                    meta_group_name=str(compact_product.get("meta_group_name") or "") or None,
                    use_invention_label=bool(has_top_level_invention_path),
                    character_name_by_id=ctx.character_name_by_id, corporation_name_by_id=ctx.corporation_name_by_id,
                )
                prerequisite_tree_nodes.append(self._job_tree_node(
                    label=(self._ACTIVITY_LABELS["invention"] if has_top_level_invention_path else self._ACTIVITY_LABELS["copying"]),
                    **fields,
                ))

        # Copying (Step 4)
        copying_costs, copying_tree = self._plan_copying_for_variant(
            ctx, row=row, blueprint_type_id=blueprint_type_id, per_run_material_eiv=per_run_material_eiv,
            missing_copy_runs=missing_top_level_copy_runs, requires_copying_job=requires_copying_job,
            owned_target_copy_runs_used=owned_target_copy_runs_used, compact_product=compact_product,
            has_invention_path=bool(has_top_level_invention_path), matched_blueprint_originals=matched_blueprint_originals,
        )
        if copying_costs is not None:
            activity_breakdown["copying"] = copying_costs
            if requires_copying_job:
                total_time_seconds += copying_costs["duration_seconds"]
                if copying_costs.get("total_job_cost") is not None:
                    total_job_cost += float(copying_costs.get("total_job_cost") or 0.0)
                    priced_job_count += 1
            else:
                activity_breakdown.pop("copying", None)
        prerequisite_tree_nodes.extend(copying_tree)

        # One valuation stock per row for every invention's amortized inputs.
        invention_valuation_stock = dict(available_owned_item_quantity_by_type_id)
        # Invention (Step 4)
        inv_costs, inv_tree, inv_procurement, source_copy_costs = self._plan_invention_chain_for_variant(
            ctx, row=row, blueprint_type_id=blueprint_type_id,
            missing_top_level_copy_runs=missing_top_level_copy_runs,
            requires_invention_chain=requires_invention_chain,
            owned_target_copy_runs_used=owned_target_copy_runs_used,
            max_production_limit=max_production_limit, compact_product=compact_product,
            available_blueprint_copy_runs_by_type_id=available_blueprint_copy_runs_by_type_id,
            available_owned_item_quantity_by_type_id=available_owned_item_quantity_by_type_id,
            invention_valuation_stock=invention_valuation_stock,
            decryptor=decryptor,
            per_run_material_eiv=per_run_material_eiv,
        )
        expected_invention_inputs: list[dict[str, Any]] = []
        # elapsed_time_seconds = time_seconds with the amortized invention and
        # source-copy slot time swapped for their whole-job elapsed time (see
        # _plan_invention_chain_for_variant). None when that time is unknown.
        elapsed_time_adjustment: float | None = 0.0
        invention_cost_unknown_reason: str | None = None
        if inv_costs is not None:
            activity_breakdown["invention"] = inv_costs
            if requires_invention_chain:
                expected_invention_inputs = self._expected_invention_inputs(
                    planned_materials=inv_procurement, invention_costs=inv_costs,
                )
                invention_cost_unknown_reason = inv_costs.get("cost_unknown_reason")
                elapsed_time_adjustment = self._invention_elapsed_adjustment(inv_costs, source_copy_costs)
                total_time_seconds += inv_costs["duration_seconds"]
                if inv_costs.get("total_job_cost") is not None:
                    total_job_cost += float(inv_costs.get("total_job_cost") or 0.0)
                    priced_job_count += 1
                # The invented runs exactly cover the missing runs in the
                # expected-value model, so no surplus BPC runs are left over.
            else:
                activity_breakdown.pop("invention", None)
        prerequisite_tree_nodes.extend(inv_tree)
        if source_copy_costs is not None and requires_invention_chain and (
            source_copy_costs.get("duration_seconds", 0) > 0
            or (source_copy_costs.get("elapsed_duration_seconds") or 0) > 0
        ):
            # Listed so activity_breakdown's fees add up to total_job_cost, and
            # so a copy that only the whole attempts need (owned runs cover the
            # expected attempts: 0 slot time, no fee) still shows its elapsed time.
            activity_breakdown["invention_source_copying"] = source_copy_costs
            total_time_seconds += source_copy_costs["duration_seconds"]
            if source_copy_costs.get("total_job_cost") is not None:
                total_job_cost += float(source_copy_costs.get("total_job_cost") or 0.0)
                priced_job_count += 1

        # Research (Step 4)
        if include_sde_research_chain:
            for act_name, res_costs, res_node in self._plan_research_chain_for_variant(ctx, row=row, per_run_material_eiv=per_run_material_eiv):
                activity_breakdown[act_name] = res_costs
                total_time_seconds += res_costs["duration_seconds"]
                if res_costs.get("total_job_cost") is not None:
                    total_job_cost += float(res_costs.get("total_job_cost") or 0.0)
                    priced_job_count += 1
                prerequisite_tree_nodes.append(res_node)

        # Blueprint metadata
        selected_bp_copy_asset = owned_top_level_copy_allocations[0][0] if owned_top_level_copy_allocations else blueprint_copy_asset
        blueprint_copy_payload = self._compact_owned_blueprint_assets(
            blueprint_copy_assets if len(blueprint_copy_assets) > 1 else ([selected_bp_copy_asset] if selected_bp_copy_asset is not None else []),
            character_name_by_id=ctx.character_name_by_id, corporation_name_by_id=ctx.corporation_name_by_id, top_location_name_by_id=ctx.top_location_name_by_id,
        )
        bp_copy_variant_key = "-".join(str(int(getattr(a, "item_id", 0) or 0)) for a in blueprint_copy_assets if int(getattr(a, "item_id", 0) or 0) > 0) or "none"
        uses_bp_original = blueprint_source_kind in {"copied_from_owned_blueprint_original", "owned_blueprint_original"} or requires_copying_job
        blueprint_original_payload = self._compact_owned_blueprint_asset(
            blueprint_original_asset if uses_bp_original else None,
            character_name_by_id=ctx.character_name_by_id, corporation_name_by_id=ctx.corporation_name_by_id, top_location_name_by_id=ctx.top_location_name_by_id,
        )
        bp_original_item_id = int(blueprint_original_asset.item_id) if uses_bp_original and blueprint_original_asset is not None else None

        # Recursive prerequisite plan
        recursive_prerequisite_plan = self._build_recursive_prerequisite_plan(
            adjusted_material_entries=adjusted_material_entries, blueprint_type_id=blueprint_type_id,
            build_from_bpc=ctx.build_from_bpc, include_reactions=ctx.include_reactions,
            selected_industry_profile=ctx.selected_industry_profile,
            selected_character_modifiers=ctx.selected_character_modifiers,
            character_skill_levels_by_name=ctx.character_skill_levels_by_name,
            adjusted_market_price_map=ctx.adjusted_market_price_map,
            sell_price_map=ctx.material_price_map,
            blueprint_copy_assets_by_type_id=ctx.blueprint_copy_assets_by_type_id,
            available_blueprint_copy_runs_by_type_id=available_blueprint_copy_runs_by_type_id,
            available_owned_item_quantity_by_type_id=available_owned_item_quantity_by_type_id,
            invention_valuation_stock=invention_valuation_stock,
            owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
            blueprint_original_assets_by_type_id=ctx.blueprint_original_assets_by_type_id,
            character_name_by_id=ctx.character_name_by_id, corporation_name_by_id=ctx.corporation_name_by_id,
            manufacturing_row_by_product_type_id=ctx.manufacturing_row_by_product_type_id,
            reaction_row_by_product_type_id=ctx.reaction_row_by_product_type_id,
            invention_row_by_blueprint_type_id=ctx.invention_row_by_blueprint_type_id,
            invention_context=ctx.invention_context,
        )
        top_level_procurement_materials = cast(list[dict[str, Any]], recursive_prerequisite_plan.get("procurement_materials") or top_level_procurement_materials)
        # Nested T2 sub-builds invent on the same expected-value model: their
        # amortized inputs join the row's expected invention inputs, their
        # whole-job elapsed time joins elapsed_time_seconds, and an unknown
        # nested invention cost marks the row's invention cost unknown.
        nested_expected_invention_inputs = [
            dict(e) for e in (recursive_prerequisite_plan.get("expected_invention_materials") or []) if isinstance(e, dict)
        ]
        nested_elapsed_adjustment = recursive_prerequisite_plan.get("elapsed_time_adjustment", 0.0)
        if elapsed_time_adjustment is not None:
            elapsed_time_adjustment = (
                None if nested_elapsed_adjustment is None else elapsed_time_adjustment + float(nested_elapsed_adjustment)
            )
        nested_unknown_reasons = [str(r) for r in (recursive_prerequisite_plan.get("invention_cost_unknown_reasons") or [])]
        if nested_unknown_reasons:
            invention_cost_unknown_reason = "; ".join(
                [*([invention_cost_unknown_reason] if invention_cost_unknown_reason else []), *nested_unknown_reasons]
            )
        # What to buy for the invention: the top-level and every built nested
        # invention's inputs for whole attempts (integer units), merged per type
        # and taken from owned stock first like any other procurement. Buy list
        # only: the cost is on the amortized expected_invention_materials.
        invention_requirements = [
            *(self._invention_whole_attempt_requirements(
                invention_job, int(inv_costs.get("whole_attempts") or 0), decryptor=decryptor,
            ) if requires_invention_chain and inv_costs is not None else []),
            *[dict(e) for e in (recursive_prerequisite_plan.get("invention_requirements") or []) if isinstance(e, dict)],
        ]
        invention_procurement_materials, _ = self._plan_take_or_buy_material_nodes(
            self._merge_invention_requirements(invention_requirements),
            available_owned_item_quantity_by_type_id=available_owned_item_quantity_by_type_id,
            owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
            sell_price_map=ctx.material_price_map,
            adjusted_price_map=ctx.adjusted_market_price_map,
        )
        # The list above is batch 1 only. The row's own invention for the
        # later batches: the owned T2 BPC runs cover the first batches, every
        # run past them is invented. Pure requirements: planned against no
        # stock (take 0), because the shopping list nets owned stock once over
        # all batches.
        top_level_extra_attempts = 0
        # Why the shopping list cannot size the datacores, and from which batch
        # on: 1 when batch 1 itself invents with unknown odds, 2 when only the
        # batches past the owned BPC runs do. Never replaced by a guessed count.
        datacore_need_unknown_reasons: list[str] = []
        datacore_need_unknown_from_batch = 2
        extra_chance: dict[str, Any] | None = None
        invents_top_level = bool(ctx.build_from_bpc) and has_top_level_invention_path and not matched_blueprint_originals
        if invents_top_level:
            if requires_invention_chain and owned_target_copy_runs_used <= 0 and inv_costs is not None:
                # Batch 1 already invented every run when no owned BPC ran.
                top_level_extra_attempts = int(inv_costs.get("whole_attempts") or 0)
                if inv_costs.get("cost_unknown_reason") is not None:
                    datacore_need_unknown_reasons.append(str(inv_costs["cost_unknown_reason"]))
                    datacore_need_unknown_from_batch = 1
            elif requires_invention_chain and inv_costs is not None and inv_costs.get("cost_unknown_reason") is not None:
                # Partly owned BPC and unknown odds: batch 1 is unknown already,
                # so a full batch is too (no second evaluation, no second WARNING).
                datacore_need_unknown_reasons.append(str(inv_costs["cost_unknown_reason"]))
                datacore_need_unknown_from_batch = 1
            else:
                top_level_extra_attempts, extra_unknown_reason, extra_chance = self._full_batch_invention_whole_attempts(
                    invention_job=invention_job, blueprint_type_id=blueprint_type_id,
                    runs=effective_runs, max_production_limit=max_production_limit,
                    adjusted_price_map=ctx.adjusted_market_price_map,
                    decryptor=decryptor, invention_context=ctx.invention_context,
                )
                if extra_unknown_reason is not None:
                    datacore_need_unknown_reasons.append(f"batches past the owned T2 BPC runs: {extra_unknown_reason}")
        # The nested sub-builds' invention for one later batch that repeats
        # batch 1 (no extra_batch_schedule below): batch 1's own.
        batch_one_nested_requirements = [
            dict(e) for e in (recursive_prerequisite_plan.get("invention_requirements") or []) if isinstance(e, dict)
        ]
        # I2: later batches share invented BPCs. One BPC's runs can outlast a
        # batch (a +run decryptor, or batches shorter than a BPC), so the
        # shopping list sizes the row's own invention over ALL batches from
        # this plan: BPCs needed = ceil(runs past the owned BPC runs / runs
        # per BPC), each bought as whole attempts. Written only when the
        # attempts are known (unknown odds are flagged instead, see above).
        invention_bpc_plan: dict[str, Any] | None = None
        if top_level_extra_attempts > 0 and inv_costs is not None:
            runs_per_bpc = max(1, int(inv_costs.get("runs_per_invented_blueprint") or 1))
            attempts_per_bpc, per_bpc_unknown, _chance = self._full_batch_invention_whole_attempts(
                invention_job=invention_job, blueprint_type_id=blueprint_type_id,
                runs=runs_per_bpc, max_production_limit=max_production_limit,
                adjusted_price_map=ctx.adjusted_market_price_map,
                decryptor=decryptor, invention_context=ctx.invention_context,
            )
            if per_bpc_unknown is None and attempts_per_bpc > 0:
                per_attempt_lines, _ = self._plan_take_or_buy_material_nodes(
                    self._invention_whole_attempt_requirements(invention_job, 1, decryptor=decryptor),
                    available_owned_item_quantity_by_type_id={},
                    owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
                    sell_price_map=ctx.material_price_map,
                    adjusted_price_map=ctx.adjusted_market_price_map,
                )
                batch_one_invents = requires_invention_chain and missing_top_level_copy_runs > 0
                invention_bpc_plan = {
                    "batch_runs": int(effective_runs),
                    "runs_per_invented_blueprint": runs_per_bpc,
                    "whole_attempts_per_blueprint": int(attempts_per_bpc),
                    # Every owned BPC run of this variant, batch 1's included.
                    "owned_blueprint_runs": max(0, int(available_target_copy_runs)),
                    "batch_one_invented_blueprints": (
                        int(math.ceil(missing_top_level_copy_runs / runs_per_bpc)) if batch_one_invents else 0
                    ),
                    # The whole attempts batch 1's own invention buys (fix round 3).
                    "batch_one_whole_attempts": (
                        int(inv_costs.get("whole_attempts") or 0) if requires_invention_chain else 0
                    ),
                    "per_attempt_materials": self._overview_keyed_entries(per_attempt_lines, compactor=lambda e: e),
                    # Install fees per attempt and per source-copy run (D4):
                    # the planner's invent / copy actions when batch 1 does
                    # not invent (activity_breakdown.invention is then absent).
                    "job_cost_per_attempt": inv_costs.get("job_cost_per_attempt"),
                    "job_cost_per_attempt_unknown_reason": inv_costs.get("job_cost_per_attempt_unknown_reason"),
                    "source_copy_job_cost_per_run": inv_costs.get("source_copy_job_cost_per_run"),
                    "source_copy_job_cost_per_run_unknown_reason": inv_costs.get("source_copy_job_cost_per_run_unknown_reason"),
                }
        # Batches 2..N (shopping list only). A later batch differs from batch
        # 1 when batch 1 spent owned BPC runs (top level or in a sub-build)
        # that it no longer has, or when its own job straddles two invented
        # BPCs. Each distinct later batch is then planned on its own
        # (_plan_extra_batch), quietly: this row logs one WARNING below.
        extra_batch_schedule: dict[str, Any] | None = None
        if bool(ctx.build_from_bpc) and not fully_invented:
            nested_copy_runs_spent = self._blueprint_copy_runs_spent(
                {t: r for t, r in copy_runs_before_batch_one.items() if int(t) != blueprint_type_id},
                {t: r for t, r in available_blueprint_copy_runs_by_type_id.items() if int(t) != blueprint_type_id},
            )
            sub_build_plan_cache: dict[tuple, dict[str, Any]] = {}
            with quiet_warnings():
                extra_batch_schedule = self._later_batch_schedule(
                    batch_run_layout, batch_runs=effective_runs, batch_one_segments=job_segments,
                    nested_copy_runs_spent=nested_copy_runs_spent,
                    plan_for=lambda segments: self._plan_extra_batch(
                        ctx, manufacturing_job=manufacturing_job, blueprint_type_id=blueprint_type_id,
                        job_segments=segments, manufacturing_group=rig_groups,
                        required_skill_entries=manufacturing_skill_entry_list,
                        # The row's own invention for that batch alone (the
                        # shopping list's list without an invention_bpc_plan).
                        own_invention_source=(
                            batch_run_layout.next_source
                            if invents_top_level and batch_run_layout.runs_per_blueprint else None
                        ),
                        invention_job=invention_job, decryptor=decryptor, max_production_limit=max_production_limit,
                        sub_build_plan_cache=sub_build_plan_cache,
                    ),
                )
        # Later batches are planned without any nested owned BPC run batch 1
        # used, even when that BPC has runs left (it may also be counted by
        # another selected row): over-buy only, and the row says so.
        later_batch_notes: list[str] = []
        if extra_batch_schedule is not None:
            for spare_type_id, spare_runs in sorted(available_blueprint_copy_runs_by_type_id.items()):
                before = int(copy_runs_before_batch_one.get(spare_type_id, 0) or 0)
                if int(spare_type_id) == blueprint_type_id or not (0 < int(spare_runs or 0) < before):
                    continue
                spare_blueprint = (ctx.blueprint_row_by_type_id.get(int(spare_type_id)) or {}).get("blueprint") or {}
                spare_name = str((spare_blueprint if isinstance(spare_blueprint, dict) else {}).get("type_name") or spare_type_id)
                later_batch_notes.append(
                    f"later batches assume nested BPC runs are spent: {spare_name} has {int(spare_runs)} runs "
                    f"left after batch 1; the shopping list counts them for its invention unless another "
                    f"selected row may use them, else batches 2+ may over-buy its sub-build"
                )
        invention_procurement_materials_per_extra_batch: list[dict[str, Any]] = []
        nested_extra_batch_lines: list[dict[str, Any]] = []
        if extra_batch_schedule is None:
            # Every later batch repeats batch 1: its full-batch own attempts
            # plus batch 1's nested sub-build invention.
            invention_procurement_materials_per_extra_batch, _ = self._plan_take_or_buy_material_nodes(
                self._merge_invention_requirements([
                    *self._invention_whole_attempt_requirements(invention_job, top_level_extra_attempts, decryptor=decryptor),
                    *batch_one_nested_requirements,
                ]),
                available_owned_item_quantity_by_type_id={},
                owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
                sell_price_map=ctx.material_price_map,
                adjusted_price_map=ctx.adjusted_market_price_map,
            )
            if invention_bpc_plan is not None or batch_one_nested_requirements:
                nested_extra_batch_lines, _ = self._plan_take_or_buy_material_nodes(
                    self._merge_invention_requirements(batch_one_nested_requirements),
                    available_owned_item_quantity_by_type_id={},
                    owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
                    sell_price_map=ctx.material_price_map,
                    adjusted_price_map=ctx.adjusted_market_price_map,
                )
        else:
            datacore_need_unknown_reasons.extend(
                r for r in extra_batch_schedule.pop("unknown_reasons") if r not in datacore_need_unknown_reasons
            )
        # A batch past the owned BPC runs that must invent with unknown odds
        # marks the row's invention cost unknown (Low confidence via the
        # pricing step) even when batch 1's own cost is known.
        extra_only_unknown_reasons = [
            r for r in datacore_need_unknown_reasons
            if r not in (invention_cost_unknown_reason or "")
        ]
        # Batch 1's cost is exact when every unknown is a later batch's.
        batch_one_cost_exact = invention_cost_unknown_reason is None
        if extra_only_unknown_reasons:
            invention_cost_unknown_reason = "; ".join(
                [*([invention_cost_unknown_reason] if invention_cost_unknown_reason else []), *extra_only_unknown_reasons]
            )
        if extra_only_unknown_reasons:
            # One WARNING per row: the full-batch evaluations log theirs at
            # DEBUG, the later-batch plans run quietly, and a batch-1 unknown
            # already logged "invention cost left unknown" for the row's own
            # invention.
            logger.warning(
                "Product %s: datacore need unknown from batch %s on (%s); "
                "the shopping list flags it instead of buying 0 datacores",
                product_type_id, datacore_need_unknown_from_batch, "; ".join(datacore_need_unknown_reasons),
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
        prerequisite_tree_nodes.extend([n for n in (recursive_prerequisite_plan.get("tree_children") or []) if isinstance(n, dict)])
        top_level_material_nodes = cast(list[dict[str, Any]], recursive_prerequisite_plan.get("material_nodes") or [])
        if not top_level_material_nodes:
            top_level_material_nodes = [
                self._job_tree_node(
                    label=str(m.get("type_name") or m.get("type_id") or "Material"), node_type="material", activity="material",
                    sourcing_strategy=("take_or_buy_reaction_available" if not (bool(ctx.include_reactions) and self._reactions_allowed_for_profile(ctx.selected_industry_profile)) and int(m.get("type_id") or 0) in ctx.reaction_row_by_product_type_id else "take_or_buy"),
                    category_name=str(m.get("category_name") or "") or None, meta_group_name=str(m.get("meta_group_name") or "") or None,
                    type_id=int(m.get("type_id") or 0), quantity=int(m.get("quantity") or 0),
                    runs=None, duration_seconds=None, job_cost=None, total_job_cost=None, children=[],
                ) for m in adjusted_material_entries if isinstance(m, dict)
            ]

        # Assemble job tree
        row_duration_is_amortized = bool(
            (requires_invention_chain and inv_costs is not None and inv_costs.get("duration_is_amortized"))
            or recursive_prerequisite_plan.get("duration_is_amortized")
        )
        elapsed_time_seconds = int(round(total_time_seconds + elapsed_time_adjustment)) if elapsed_time_adjustment is not None else None
        manufacturing_tree = self._job_tree_node(
            label=self._ACTIVITY_LABELS["manufacturing"], node_type="activity", activity="manufacturing",
            blueprint_type_id=blueprint_type_id, type_id=product_type_id,
            quantity=int(compact_product.get("quantity") or 0), runs=effective_runs,
            duration_seconds=total_time_seconds, direct_duration_seconds=direct_manufacturing_time_seconds,
            duration_is_amortized=row_duration_is_amortized, elapsed_duration_seconds=elapsed_time_seconds,
            job_cost=manufacturing_job_cost.get("total_job_cost"),
            total_job_cost=(total_job_cost if priced_job_count > 0 else None),
            blueprint_source_kind=blueprint_source_kind,
            children=[*prerequisite_tree_nodes, self._job_tree_node(label=self._ACTIVITY_LABELS["materials"], node_type="materials", activity="materials", children=top_level_material_nodes)],
        )
        product_tree = self._job_tree_node(
            label=str(compact_product.get("type_name") or compact_product.get("type_id") or "Product"),
            node_type="product", activity="product", type_id=product_type_id,
            quantity=int(compact_product.get("quantity") or 0), runs=effective_runs,
            duration_seconds=total_time_seconds,
            duration_is_amortized=row_duration_is_amortized, elapsed_duration_seconds=elapsed_time_seconds,
            job_cost=(total_job_cost if priced_job_count > 0 else None),
            total_job_cost=(total_job_cost if priced_job_count > 0 else None),
            material_cost=None, total_cost=None, children=[manufacturing_tree],
        )
        product_market_pricing = ctx.product_sell_price_map.get(product_type_id) or {}

        row_out = {
            **compact_product,
            "overview_row_id": f"product:{row_index}:{product_index}:bpc:{bp_copy_variant_key}:bpo:{bp_original_item_id or 'none'}",
            "manufacturing_group": manufacturing_group,
            "manufacturing_job": {
                "materials": self._overview_keyed_entries(
                    adjusted_material_entries, compactor=lambda e: self._with_component_job_fee(ctx, e),
                ),
                "skills": manufacturing_skills,
                "character_modifiers": ctx.selected_character_modifiers,
                "time_seconds": total_time_seconds,
                "manufacturing_time_seconds": direct_manufacturing_time_seconds,
                "preparation_time_seconds": max(0, total_time_seconds - direct_manufacturing_time_seconds),
                # Latency for one batch: whole invention jobs in sequence instead
                # of the amortized slot time in time_seconds (which ISK/h uses).
                "elapsed_time_seconds": elapsed_time_seconds,
                "max_production_limit": max_production_limit,
                "runs": effective_runs,
                "job_segments": job_segments,
                "product_quantity_per_run": product_quantity_per_run,
                "material_reduction": manufacturing_material_reduction,
                "time_reduction": manufacturing_time_reduction,
                "cost_reduction": manufacturing_cost_reduction,
                "job_cost": manufacturing_job_cost.get("total_job_cost"),
                "manufacturing_job_cost": manufacturing_job_cost.get("total_job_cost"),
                "manufacturing_job_cost_unknown_reason": manufacturing_job_cost.get("fee_unknown_reason"),
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
                "procurement_materials": self._overview_keyed_entries(top_level_procurement_materials, compactor=lambda e: e),
                "activity_cost_indices": {
                    "manufacturing": manufacturing_cost_index,
                    "copying": self._system_cost_index(profile_payload=ctx.selected_industry_profile, activity="copying"),
                    "research_material": self._system_cost_index(profile_payload=ctx.selected_industry_profile, activity="research_material"),
                    "research_time": self._system_cost_index(profile_payload=ctx.selected_industry_profile, activity="research_time"),
                    "invention": self._system_cost_index(profile_payload=ctx.selected_industry_profile, activity="invention"),
                    "reaction": self._system_cost_index(profile_payload=ctx.selected_industry_profile, activity="reaction"),
                },
                "blueprint_material_efficiency": blueprint_material_efficiency,
                "blueprint_time_efficiency": blueprint_time_efficiency,
                "blueprint_source_kind": blueprint_source_kind,
                "industry_profile": ctx.selected_industry_profile,
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
            "bpc_count": len(ctx.blueprint_copy_assets_by_type_id.get(blueprint_type_id, [])),
            "has_bpo": bool(ctx.blueprint_original_assets_by_type_id.get(blueprint_type_id)),
        }
        if invention_cost_unknown_reason is not None:
            row_out["manufacturing_job"]["invention_cost_unknown_reason"] = invention_cost_unknown_reason
        if matched_blueprint_originals:
            # An owned BPO, the only blueprint the daily planner researches:
            # its ME/TE research fee per level (A2, Capital Reserved). Also on
            # a row that builds from an owned BPC of it: the planner schedules
            # the research whenever the BPO is owned, and the fee depends on
            # the blueprint, not on the variant.
            (
                row_out["manufacturing_job"]["research_job_cost_by_level"],
                row_out["manufacturing_job"]["research_job_cost_by_level_unknown_reason"],
            ) = self._research_job_cost_by_level(ctx, row=row, per_run_material_eiv=per_run_material_eiv)
            row_out["manufacturing_job"]["research_duration_seconds_by_level"] = self._research_duration_by_level(ctx, row=row)
            row_out["manufacturing_job"]["research_duration_seconds_by_level_unskilled"] = self._research_duration_by_level(
                ctx, row=row, skilled=False,
            )
        top_level_chance = (
            inv_costs.get("invention_chance") if requires_invention_chain and inv_costs is not None else None
        ) or extra_chance
        if isinstance(top_level_chance, dict):
            # The chance this row's own invention assumes: batch 1's, or the
            # later batches' when an owned BPC covers batch 1.
            row_out["manufacturing_job"]["invention_chance"] = top_level_chance
        nested_chance_notes = list(dict.fromkeys(
            str(r) for r in (recursive_prerequisite_plan.get("invention_chance_notes") or [])
        ))
        if nested_chance_notes:
            row_out["manufacturing_job"]["invention_chance_notes"] = nested_chance_notes
        if datacore_need_unknown_reasons:
            # Read by the shopping list (invention_need_unknown_rows): these
            # batches' datacores are not in either invention list.
            row_out["manufacturing_job"]["invention_datacore_need_unknown"] = {
                "reason": "; ".join(datacore_need_unknown_reasons),
                "from_batch": datacore_need_unknown_from_batch,
                # Read by the pricing-confidence step for its wording.
                "batch_one_cost_exact": batch_one_cost_exact,
            }
        if not bool(ctx.build_from_bpc) and has_top_level_invention_path and not matched_blueprint_originals:
            # A T2 row that would be invented, costed with no invention because
            # build_from_bpc is off: it silently assumes a T2 BPO the corp does
            # not own. A label only; no cost number changes.
            row_out["manufacturing_job"]["assumes_owned_t2_bpo"] = True
        if invention_procurement_materials:
            # Read by the Portfolio / Industry Builder shopping list
            # (industry/shopping_list.py). Not priced by the pricing step and not
            # read by the daily planner, which buys datacores via invent actions.
            row_out["manufacturing_job"]["invention_procurement_materials"] = self._overview_keyed_entries(
                invention_procurement_materials, compactor=lambda e: e,
            )
        if invention_procurement_materials_per_extra_batch:
            # Shopping list only: batches 2..N of this row, see above.
            row_out["manufacturing_job"]["invention_procurement_materials_per_extra_batch"] = self._overview_keyed_entries(
                invention_procurement_materials_per_extra_batch, compactor=lambda e: e,
            )
        if invention_bpc_plan is not None:
            # Shopping list (industry/shopping_list.py): the row's own invention
            # over all batches.
            row_out["manufacturing_job"]["invention_bpc_plan"] = invention_bpc_plan
        if extra_batch_schedule is None and (invention_bpc_plan is not None or nested_extra_batch_lines):
            # The nested sub-builds' per-batch lines (the extra-batch list
            # above minus this row's own invention).
            row_out["manufacturing_job"]["invention_procurement_materials_per_extra_batch_nested"] = (
                self._overview_keyed_entries(nested_extra_batch_lines, compactor=lambda e: e)
            )
        if extra_batch_schedule is not None:
            # Shopping list only: batches 2..N, each from its own plan.
            row_out["manufacturing_job"]["extra_batch_schedule"] = extra_batch_schedule
        if bool(ctx.build_from_bpc):
            # Read by the shopping list: a row without it is from an older
            # overview whose later batches repeat batch 1.
            row_out["manufacturing_job"]["later_batch_plan_version"] = LATER_BATCH_PLAN_VERSION
            # I3: batch 1's nested inventions and the owned BPC runs left of
            # every nested blueprint after batch 1, so the shopping list
            # carries invented and owned spare runs across batches.
            batch_one_nested_bpc_plans = self._compact_nested_invention_bpc_plans(
                ctx, recursive_prerequisite_plan.get("nested_invention_bpc_plans"),
            )
            nested_blueprint_ids = {int(p["invented_blueprint_type_id"]) for p in batch_one_nested_bpc_plans}
            for later_plan in (extra_batch_schedule or {}).get("plans") or []:
                nested_blueprint_ids |= {
                    int(p["invented_blueprint_type_id"]) for p in later_plan.get("nested_invention_bpc_plans") or []
                }
            nested_blueprint_ids |= {
                int(t) for t, r in copy_runs_before_batch_one.items()
                if int(t) > 0 and float(available_blueprint_copy_runs_by_type_id.get(t, 0) or 0) < float(r or 0)
            }
            nested_blueprint_ids.discard(int(blueprint_type_id))
            row_out["manufacturing_job"]["nested_invention_bpc_plans"] = batch_one_nested_bpc_plans
            # Fix round 2, item 4: each row is planned on its own copy of the
            # owned BPC runs, so the shopping list nets the nested BPC runs
            # batch 1 spent once across the selected rows.
            used_nested_runs = {
                int(t): int(math.ceil(float(r or 0) - float(available_blueprint_copy_runs_by_type_id.get(t, 0) or 0) - 1e-9))
                for t, r in copy_runs_before_batch_one.items()
                if int(t) > 0 and int(t) != int(blueprint_type_id)
                and float(available_blueprint_copy_runs_by_type_id.get(t, 0) or 0) < float(r or 0)
            }
            owned_runs_before = {
                t: max(0, int(math.floor(float(copy_runs_before_batch_one.get(t, 0) or 0) + 1e-9)))
                for t in used_nested_runs
            }
            if owned_target_copy_runs_used > 0:
                # Fix round 3: the row's own top-level BPC runs too, so a
                # top-level user of a BPC another row nests counts as well.
                used_nested_runs[int(blueprint_type_id)] = int(owned_target_copy_runs_used)
                owned_runs_before[int(blueprint_type_id)] = max(0, int(available_target_copy_runs))
            row_out["manufacturing_job"]["nested_blueprint_runs_used"] = {
                str(t): used for t, used in sorted(used_nested_runs.items()) if used > 0
            }
            row_out["manufacturing_job"]["nested_blueprint_runs_owned"] = {
                str(t): owned_runs_before[t] for t in sorted(used_nested_runs) if used_nested_runs[t] > 0
            }
            row_out["manufacturing_job"]["nested_blueprint_runs_left"] = {
                str(t): max(0, int(math.floor(float(available_blueprint_copy_runs_by_type_id.get(t, 0) or 0) + 1e-9)))
                for t in sorted(nested_blueprint_ids)
            }
        if later_batch_notes:
            # Informational (pricing-confidence reasons): over-buy only.
            row_out["manufacturing_job"]["later_batch_notes"] = later_batch_notes
        if sized_batch["reductions_differ"]:
            # material_reduction / time_reduction above (and each material's)
            # are the runs-weighted mean over job_segments, which holds each
            # job's own reductions.
            row_out["manufacturing_job"]["reduction_basis"] = "runs_weighted_job_segments"
        if expected_invention_inputs or nested_expected_invention_inputs:
            # Priced into material_cost / total_cost by _enrich_product_rows_with_material_prices.
            # Kept out of `materials` and `procurement_materials`: those are the
            # manufacturing job's own buy list, and the daily planner buys the
            # datacores from its separate invent action.
            keyed_invention_inputs = self._overview_keyed_entries(
                expected_invention_inputs, compactor=lambda e: e,
            )
            # A nested sub-build's inputs are keyed "<type_id>@<invented blueprint>",
            # so they never overwrite the row's own (or another sub-build's) line.
            for entry in nested_expected_invention_inputs:
                base_key = f"{int(entry.get('type_id') or 0)}@{int(entry.get('invented_blueprint_type_id') or 0)}"
                key, n = base_key, 1
                while key in keyed_invention_inputs:  # same blueprint invented in two sub-builds
                    n += 1
                    key = f"{base_key}#{n}"
                keyed_invention_inputs[key] = entry
            row_out["manufacturing_job"]["expected_invention_materials"] = keyed_invention_inputs
        # The hangar stock this row was planned against, for every type on its
        # buy lists (batch 1 and every later batch). Every row of one overview
        # is planned against a copy of the same stock, so the shopping list
        # nets its aggregated need against it once (C8, C9).
        row_out["manufacturing_job"][_shopping_list.OWNED_STOCK_KEY] = {
            str(type_id): max(0, int(ctx.available_owned_item_quantity_by_type_id_base.get(type_id, 0) or 0))
            for type_id in self._buy_line_type_ids(row_out["manufacturing_job"])
        }
        row_out["manufacturing_job"][_shopping_list.OWNED_STOCK_POOL_KEY] = ctx.owned_stock_pool_id
        return row_out

    @staticmethod
    def _owned_stock_pool_id(stock: dict[int, Any]) -> str:
        """A short hash of one compute's owned stock (sorted type_id, quantity)."""
        payload = json.dumps(sorted((int(t), int(q or 0)) for t, q in (stock or {}).items()))
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    @staticmethod
    def _buy_line_type_ids(manufacturing_job: dict[str, Any]) -> list[int]:
        """Every type_id on a row's shopping-list lines, sorted: the lists
        under the keys industry/shopping_list.py reads (its ROW_BUY_LIST_KEYS,
        each extra_batch_schedule plan's LATER_BATCH_PLAN_BUY_LIST_KEYS, and
        the invention_bpc_plan's per-attempt inputs), shared so they cannot
        drift."""
        def entries(source: Any, key: str) -> list[Any]:
            value = source.get(key) if isinstance(source, dict) else None
            return list(value.values()) if isinstance(value, dict) else []

        sl = _shopping_list
        lines: list[Any] = [entry for key in sl.ROW_BUY_LIST_KEYS for entry in entries(manufacturing_job, key)]
        lines.extend(entries(manufacturing_job.get(sl.BPC_PLAN_KEY), sl.BPC_PLAN_LINES_KEY))
        schedule = manufacturing_job.get(sl.EXTRA_BATCH_SCHEDULE_KEY)
        for plan in (schedule.get("plans") or []) if isinstance(schedule, dict) else []:
            for key in sl.LATER_BATCH_PLAN_BUY_LIST_KEYS:
                lines.extend(entries(plan, key))
        type_ids: set[int] = set()
        for line in lines:
            if not isinstance(line, dict):
                continue
            try:
                type_id = int(line.get("type_id") or 0)
            except (TypeError, ValueError):
                continue
            if type_id > 0:
                type_ids.add(type_id)
        return sorted(type_ids)

    @classmethod
    def _expected_invention_inputs(
        cls,
        *,
        planned_materials: list[dict[str, Any]],
        invention_costs: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """The invention plan's inputs (datacores, plus any decryptor the SDE
        lists), tagged with the expected-value model they were sized by.

        _plan_invention_chain_for_variant sizes them as
        quantity_per_attempt x invented_runs / (success_probability x runs_per_invented_blueprint),
        a fractional amortized amount, not a buy quantity, and prices them with
        the same take-or-buy rule as every other material (owned FIFO cost when
        taken from stock, market price when bought). line_total is quantity x
        unit_price, set by the pricing step.
        """
        attempts = cls._as_float(invention_costs.get("runs")) or 0.0
        if attempts <= 0:
            return []
        out: list[dict[str, Any]] = []
        for entry in planned_materials:
            type_id = int(entry.get("type_id") or 0)
            quantity = cls._as_float(entry.get("quantity")) or 0.0
            if type_id <= 0 or quantity <= 0:
                continue
            out.append({
                **dict(entry),
                "quantity": quantity,
                "quantity_per_attempt": int(entry.get("quantity_per_attempt") or 0),
                "expected_attempts": attempts,
                "success_probability": invention_costs.get("success_probability"),
                "runs_per_invented_blueprint": invention_costs.get("runs_per_invented_blueprint"),
                "invented_runs": invention_costs.get("invented_runs"),
                "amortized_invention_input": True,
            })
        return out

    # ----------------------------------------------------------------
    # Rewritten orchestrator (Step 5 + 6)
    # ----------------------------------------------------------------

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
        overview_input_hash = hashlib.sha256(
            json.dumps({
                "maximize_bp_runs": maximize_bp_runs, "group_identical_bpcs": group_identical_bpcs,
                "build_from_bpc": build_from_bpc, "have_blueprint_source_only": have_blueprint_source_only,
                "include_reactions": include_reactions, "market_hub": market_hub,
                "material_price_side": material_price_side, "product_price_side": product_price_side,
                "industry_profile_id": industry_profile_id, "owned_blueprints_scope": owned_blueprints_scope,
                "character_id": character_id,
                # A cached overview planned before this version of the
                # later-batch plans misses and is rebuilt (shopping_list).
                "later_batch_plan_version": LATER_BATCH_PLAN_VERSION,
            }, sort_keys=True).encode()
        ).hexdigest()

        if not force_refresh:
            cached = getattr(self._state, "_overview_result_cache", None)
            if (
                isinstance(cached, dict) and cached.get("hash") == overview_input_hash
                and cached.get("later_batch_plan_version") == LATER_BATCH_PLAN_VERSION
            ):
                cached_rows = cached.get("rows")
                if isinstance(cached_rows, list):
                    if progress_callback is not None:
                        progress_callback(1.0, "Returned cached overview (no input changes)", {"stage": "cached"})
                    return [dict(row) for row in cached_rows]

        # Step 1: Build immutable planning context
        ctx = self._build_planning_context(
            force_refresh=force_refresh, build_from_bpc=build_from_bpc,
            include_reactions=include_reactions, maximize_bp_runs=maximize_bp_runs,
            group_identical_bpcs=group_identical_bpcs, have_blueprint_source_only=have_blueprint_source_only,
            market_hub=market_hub, material_price_side=material_price_side,
            product_price_side=product_price_side, industry_profile_id=industry_profile_id,
            owned_blueprints_scope=owned_blueprints_scope, character_id=character_id,
            progress_callback=progress_callback,
        )
        normalized_market_hub = MarketPricingService.normalize_market_hub(market_hub)
        normalized_material_price_side = MarketPricingService.normalize_order_side(material_price_side)
        normalized_product_price_side = MarketPricingService.normalize_order_side(product_price_side)

        # Step 6: Flatten iteration and build product rows (parallelized)
        variants = list(self._iter_product_variants(ctx))
        variant_count = len(variants)
        product_rows: list[dict[str, Any]] = [None] * variant_count  # type: ignore[list-item]
        completed_count = 0
        completed_lock = threading.Lock()

        def _build_variant(index: int, variant: tuple) -> tuple[int, dict[str, Any] | None]:
            row_idx, prod_idx, row, raw_product, copy_assets, original_asset, eff_runs = variant
            return index, self._build_product_row(
                ctx, row=row, raw_product=raw_product,
                row_index=row_idx, product_index=prod_idx,
                blueprint_copy_assets=copy_assets,
                blueprint_original_asset=original_asset,
                effective_runs=eff_runs,
            )

        _admin = getattr(self._state, "admin_settings", None)
        _cfg_max_workers = _admin.get("performance", "product_row_build_max_workers") if _admin else 8
        max_workers = min(_cfg_max_workers, max(1, variant_count))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_build_variant, i, v) for i, v in enumerate(variants)]
            for fut in as_completed(futures):
                idx, result = fut.result()
                product_rows[idx] = result  # type: ignore[assignment]
                if progress_callback is not None and variant_count > 0:
                    with completed_lock:
                        completed_count += 1
                        if completed_count % max(1, variant_count // 10) == 0:
                            progress_callback(
                                0.60 + 0.20 * (completed_count / variant_count),
                                f"Building product rows ({completed_count}/{variant_count})",
                                {"stage": "rows", "variant": completed_count, "variant_count": variant_count},
                            )

        product_rows = [r for r in product_rows if r is not None]  # type: ignore[misc]

        # Filter and enrich (inventory allocation is separate from planning)
        product_rows = [row for row in product_rows if isinstance(row, dict) and not self._exclude_from_product_overview(row)]

        if progress_callback is not None:
            progress_callback(0.82, "Built manufacturing product rows", {"stage": "rows_done", "rows": len(product_rows)})

        product_rows = self._enrich_product_rows_with_material_prices(product_rows, market_hub=normalized_market_hub, material_price_side=normalized_material_price_side, progress_callback=progress_callback)
        product_rows = self._enrich_product_rows_with_market_activity(product_rows, market_hub=normalized_market_hub)
        product_rows = self._enrich_product_rows_with_liquidity_metrics(product_rows)
        product_rows = self._enrich_product_rows_with_price_anomaly(product_rows, market_hub=normalized_market_hub)
        _mh_svc = MarketHistoryService(state=self._state, sessions=self._sessions)
        _ps = MarketPricingService(state=self._state, sessions=self._sessions)
        _region_id = int((_ps._market_hub_context(normalized_market_hub).get("region_id")) or 10000002)
        self._enrich_product_rows_with_market_trends(
            product_rows,
            market_history_svc=_mh_svc,
            region_id=_region_id,
        )
        if progress_callback is not None:
            progress_callback(0.93, "Calculating sale proceeds and profit metrics", {"stage": "profit"})
        product_rows = self._enrich_product_rows_with_sale_proceeds(product_rows, character_id=character_id, market_hub=normalized_market_hub, product_price_side=normalized_product_price_side, fee_context=ctx.market_fee_context)
        product_rows = self._enrich_product_rows_with_profit_metrics(product_rows)
        product_rows = self._score_inventory_allocation_priority(product_rows)
        product_rows = self._enrich_product_rows_with_manufacturing_signals(product_rows)
        if progress_callback is not None:
            progress_callback(0.97, "Scoring pricing confidence", {"stage": "finalize"})
        product_rows = self._enrich_product_rows_with_pricing_confidence(product_rows, product_price_side=normalized_product_price_side)

        product_rows.sort(key=lambda row: (str(row.get("type_name") or "").lower(), str(row.get("overview_row_id") or "")))
        if progress_callback is not None:
            progress_callback(1.0, "Product overview ready", {"stage": "completed", "rows": len(product_rows)})

        self._state._overview_result_cache = {
            "hash": overview_input_hash, "rows": product_rows, "later_batch_plan_version": LATER_BATCH_PLAN_VERSION,
        }
        return product_rows

    def get_cached_overview_rows(self) -> list[dict] | None:
        """Return the most recent product overview rows from in-memory cache, or None if not yet computed
        or computed by an older version of the later-batch plans (it must be rebuilt)."""
        cached = getattr(self._state, "_overview_result_cache", None)
        if isinstance(cached, dict) and cached.get("later_batch_plan_version") == LATER_BATCH_PLAN_VERSION:
            rows = cached.get("rows")
            if isinstance(rows, list):
                return [dict(r) for r in rows]
        return None

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
            | {
                int(material.get("type_id") or 0)
                for row in product_rows
                for material in list((((row.get("manufacturing_job") or {}).get("expected_invention_materials") or {})).values())
                if isinstance(material, dict) and int(material.get("type_id") or 0) > 0
            }
        )
        if progress_callback is not None:
            progress_callback(
                0.83,
                "Enriching material prices",
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
                    0.83 + (0.07 * progress_fraction),
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
            certain_material_cost = 0.0
            estimated_material_cost = 0.0
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
                    price_source = str(material.get("price_source") or "").lower()
                    is_certain = (
                        price_source.startswith("owned_asset_item_value")
                        or price_source.startswith("market_")
                        or price_source.startswith("explicit")
                    )
                    if is_certain:
                        certain_material_cost += float(line_total)
                    else:
                        estimated_material_cost += float(line_total)
                material["line_total"] = line_total
            # Invented T2: the expected invention inputs (fractional, amortized
            # per run; see _expected_invention_inputs) are part of the cost and
            # count as material lines for the priced/type counts.
            invention_materials = manufacturing_job.get("expected_invention_materials")
            invention_line_count = 0
            invention_priced_count = 0
            if isinstance(invention_materials, dict) and invention_materials:
                invention_material_cost = 0.0
                for material in invention_materials.values():
                    if not isinstance(material, dict):
                        continue
                    invention_line_count += 1
                    type_id = int(material.get("type_id") or 0)
                    unit_price = self._as_float(material.get("unit_price"))
                    if unit_price is None or unit_price <= 0:
                        pricing = price_by_type_id.get(type_id) or {}
                        unit_price = self._as_float(pricing.get("unit_price"))
                        material["unit_price"] = unit_price
                        material["price_source"] = pricing.get("price_source")
                    line_total = None
                    if unit_price is not None:
                        line_total = float(unit_price) * float(material.get("quantity") or 0.0)
                        invention_material_cost += line_total
                        invention_priced_count += 1
                        price_source = str(material.get("price_source") or "").lower()
                        if price_source.startswith(("owned_asset_item_value", "market_", "explicit")):
                            certain_material_cost += line_total
                        else:
                            estimated_material_cost += line_total
                    else:
                        logger.warning(
                            "Product overview %s: invention input %s (%s) has no price; "
                            "it is left out of material_cost",
                            row.get("type_id"), type_id, material.get("type_name"),
                        )
                    material["line_total"] = line_total
                manufacturing_job["invention_material_cost"] = invention_material_cost if invention_priced_count > 0 else None
                material_cost += invention_material_cost
            any_priced = (priced_material_count + invention_priced_count) > 0
            manufacturing_job["material_cost"] = material_cost if any_priced else None
            manufacturing_job["certain_material_cost"] = certain_material_cost if any_priced else None
            manufacturing_job["estimated_material_cost"] = estimated_material_cost if any_priced else None
            manufacturing_job["priced_material_count"] = priced_material_count + invention_priced_count
            manufacturing_job["material_type_count"] = len(procurement_materials) + invention_line_count
            total_job_cost = manufacturing_job.get("total_job_cost")
            if total_job_cost is not None or any_priced:
                manufacturing_job["total_cost"] = float(total_job_cost or 0.0) + float(material_cost or 0.0)
                manufacturing_job["certain_cost"] = float(total_job_cost or 0.0) + float(certain_material_cost or 0.0)
                manufacturing_job["estimated_cost"] = float(estimated_material_cost or 0.0)
            else:
                manufacturing_job["total_cost"] = None
                manufacturing_job["certain_cost"] = None
                manufacturing_job["estimated_cost"] = None

            job_tree = manufacturing_job.get("job_tree") or {}
            if isinstance(job_tree, dict) and job_tree:
                self._apply_material_pricing_to_job_tree(job_tree, price_by_type_id=price_by_type_id)

        if progress_callback is not None:
            progress_callback(0.90, "Applied material prices to manufacturing rows", {"rows": len(product_rows)})

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

    def structure_rigs(self) -> list[dict]:
        cache = getattr(self._state, "_structure_rigs_cache", None)
        now = time.time()
        if (
            cache
            and isinstance(cache, tuple)
            and len(cache) == 3
            and cache[1] == self._STRUCTURE_RIGS_CACHE_VERSION
            and (now - cache[0] < int(self._adm("cache_ttl", "structure_rig_cache_ttl_seconds", self._STRUCTURE_RIG_MFG_CACHE_TTL_SECONDS)))
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
                name_by_type_id[int(type_id)] = name_obj.get(language) or name_obj.get("en") or str(type_id)
            except Exception:
                name_by_type_id[int(type_id)] = str(type_id)

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

    _ACTIVITY_ID_TO_NAME = {
        1: "Manufacturing",
        3: "TE Research",
        4: "ME Research",
        5: "Copying",
        8: "Invention",
        9: "Reaction",
    }

    # EVE skill IDs that grant additional industry slots
    _SKILL_MASS_PRODUCTION = 3387          # +1 manufacturing slot per level
    _SKILL_ADV_MASS_PRODUCTION = 24625     # +1 manufacturing slot per level
    _SKILL_LABORATORY_OPERATION = 3406     # +1 research/copy/invention slot per level
    _SKILL_ADV_LABORATORY_OPERATION = 24624  # +1 research/copy/invention slot per level
    _SKILL_REACTIONS = 45746               # +1 reaction slot per level
    _SKILL_MASS_REACTIONS = 45748          # +1 reaction slot per level

    def _character_slot_capacities(self) -> dict[int, dict[str, int]]:
        """Return max industry slots per character based on skills.

        Slot formula per category:
        - Manufacturing: 1 (base) + Mass Production lvl + Advanced Mass Production lvl
        - Research/Copy/Invention: 1 (base) + Laboratory Operation lvl + Advanced Laboratory Operation lvl
        - Reactions: 1 (base) + Reactions lvl + Mass Reactions lvl
        """
        char_manager = getattr(self._state, "char_manager", None)
        if char_manager is None:
            return {}

        try:
            characters = char_manager.get_characters() or []
        except Exception:
            return {}

        slot_skill_ids = {
            self._SKILL_MASS_PRODUCTION,
            self._SKILL_ADV_MASS_PRODUCTION,
            self._SKILL_LABORATORY_OPERATION,
            self._SKILL_ADV_LABORATORY_OPERATION,
            self._SKILL_REACTIONS,
            self._SKILL_MASS_REACTIONS,
        }

        capacities: dict[int, dict[str, int]] = {}
        for char in characters:
            cid = int(char.get("character_id") or 0)
            if cid <= 0:
                continue

            skills_data = char.get("skills") or {}
            skill_list = skills_data.get("skills") or [] if isinstance(skills_data, dict) else []

            skill_levels: dict[int, int] = {}
            for s in skill_list:
                sid = s.get("skill_id")
                if sid in slot_skill_ids:
                    skill_levels[sid] = int(s.get("trained_skill_level") or 0)

            manufacturing_slots = (
                1
                + skill_levels.get(self._SKILL_MASS_PRODUCTION, 0)
                + skill_levels.get(self._SKILL_ADV_MASS_PRODUCTION, 0)
            )
            research_slots = (
                1
                + skill_levels.get(self._SKILL_LABORATORY_OPERATION, 0)
                + skill_levels.get(self._SKILL_ADV_LABORATORY_OPERATION, 0)
            )
            reaction_slots = (
                1
                + skill_levels.get(self._SKILL_REACTIONS, 0)
                + skill_levels.get(self._SKILL_MASS_REACTIONS, 0)
            )

            capacities[cid] = {
                "manufacturing_max": manufacturing_slots,
                "research_max": research_slots,
                "reaction_max": reaction_slots,
            }

        return capacities

    def industry_active_jobs(self, *, character_id: int | None = None) -> dict[str, Any]:
        """Return active industry jobs and slot capacities, optionally filtered by character.

        Includes both character-scope jobs (CharacterIndustryJobsModel) and
        corporation-scope jobs (CorporationIndustryJobsModel).  Corp jobs are
        attributed to the character who installed them via installer_id so that
        per-character slot counting works correctly.
        """
        from eve_online_industry_tracker.infrastructure.sde.types import get_type_data

        session: Any = self._sessions.app_session()

        # --- Character-scope jobs ---
        char_query = session.query(CharacterIndustryJobsModel).filter(
            CharacterIndustryJobsModel.status == "active"
        )
        if character_id is not None:
            char_query = char_query.filter(CharacterIndustryJobsModel.character_id == int(character_id))
        char_jobs = char_query.all()

        # --- Corporation-scope jobs ---
        # Fetch for all corporations known to the system; filter by installer_id
        # when character_id is given.
        corp_query = session.query(CorporationIndustryJobsModel).filter(
            CorporationIndustryJobsModel.status == "active"
        )
        if character_id is not None:
            corp_query = corp_query.filter(CorporationIndustryJobsModel.installer_id == int(character_id))
        corp_jobs = corp_query.all()

        # Slot capacities (always include even with no active jobs)
        all_capacities = self._character_slot_capacities()
        if character_id is not None:
            slot_capacities = {str(character_id): all_capacities.get(int(character_id), {})}
        else:
            slot_capacities = {str(k): v for k, v in all_capacities.items()}

        if not char_jobs and not corp_jobs:
            return {"jobs": [], "slot_capacities": slot_capacities}

        # Collect type IDs for name resolution across both job lists
        type_ids: set[int] = set()
        for job in list(char_jobs) + list(corp_jobs):
            bp_type_id = getattr(job, "blueprint_type_id", None)
            prod_type_id = getattr(job, "product_type_id", None)
            if bp_type_id:
                type_ids.add(int(bp_type_id))
            if prod_type_id:
                type_ids.add(int(prod_type_id))

        sde_session = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        type_data_map: dict[int, dict] = {}
        if type_ids:
            type_data_map = get_type_data(sde_session, language, sorted(type_ids))

        # Resolve character names from both job sets
        char_ids: set[int] = set()
        for j in char_jobs:
            cid = getattr(j, "character_id", None)
            if cid:
                char_ids.add(int(cid))
        for j in corp_jobs:
            iid = getattr(j, "installer_id", None)
            if iid:
                char_ids.add(int(iid))

        char_name_map: dict[int, str] = {}
        if char_ids:
            char_manager = getattr(self._state, "char_manager", None)
            if char_manager is not None:
                try:
                    characters = char_manager.get_characters() or []
                    for c in characters:
                        cid = c.get("character_id")
                        if cid and int(cid) in char_ids:
                            char_name_map[int(cid)] = c.get("character_name") or str(cid)
                except Exception:
                    pass

        rows: list[dict[str, Any]] = []

        # Build rows for character-scope jobs
        for job in char_jobs:
            raw = getattr(job, "raw", None) or {}
            activity_id = raw.get("activity_id") if isinstance(raw, dict) else None
            activity_name = self._ACTIVITY_ID_TO_NAME.get(activity_id, f"Unknown ({activity_id})")

            bp_type_id = int(getattr(job, "blueprint_type_id", 0) or 0)
            prod_type_id = int(getattr(job, "product_type_id", 0) or 0)

            bp_name = (type_data_map.get(bp_type_id) or {}).get("type_name", str(bp_type_id)) if bp_type_id else ""
            prod_name = (type_data_map.get(prod_type_id) or {}).get("type_name", str(prod_type_id)) if prod_type_id else ""

            cid = int(getattr(job, "character_id", 0) or 0)

            rows.append({
                "job_id": int(getattr(job, "job_id", 0) or 0),
                "character_id": cid,
                "character_name": char_name_map.get(cid, str(cid)),
                "activity_id": activity_id,
                "activity_name": activity_name,
                "blueprint_type_id": bp_type_id,
                "blueprint_name": bp_name,
                "product_type_id": prod_type_id,
                "product_name": prod_name,
                "runs": int(getattr(job, "runs", 0) or 0),
                "output_quantity": int(getattr(job, "output_quantity", 0) or 0),
                "cost": float(getattr(job, "cost", 0) or 0),
                "start_date": getattr(job, "start_date", None),
                "end_date": getattr(job, "end_date", None),
                "facility_id": int(getattr(job, "facility_id", 0) or 0),
                "station_id": int(raw.get("station_id", 0) or 0) if isinstance(raw, dict) else 0,
                "duration_seconds": int(raw.get("duration", 0) or 0) if isinstance(raw, dict) else 0,
                "blueprint_location_id": int(getattr(job, "location_id", 0) or 0),
                "output_location_id": int(getattr(job, "output_location_id", 0) or 0),
                "is_corp_job": False,
            })

        # Build rows for corporation-scope jobs — attribute to installer_id
        # Deduplicate: the character endpoint returns corp-structure jobs too,
        # so skip any corp job whose job_id is already in the character rows.
        char_job_ids = {r["job_id"] for r in rows}
        for job in corp_jobs:
            raw = getattr(job, "raw", None) or {}
            activity_id = raw.get("activity_id") if isinstance(raw, dict) else None
            activity_name = self._ACTIVITY_ID_TO_NAME.get(activity_id, f"Unknown ({activity_id})")

            bp_type_id = int(getattr(job, "blueprint_type_id", 0) or 0)
            prod_type_id = int(getattr(job, "product_type_id", 0) or 0)

            bp_name = (type_data_map.get(bp_type_id) or {}).get("type_name", str(bp_type_id)) if bp_type_id else ""
            prod_name = (type_data_map.get(prod_type_id) or {}).get("type_name", str(prod_type_id)) if prod_type_id else ""

            # Use installer_id as character_id so slot counting attributes correctly
            installer_id = int(getattr(job, "installer_id", 0) or 0)

            corp_job_id = int(getattr(job, "job_id", 0) or 0)
            if corp_job_id in char_job_ids:
                continue

            rows.append({
                "job_id": corp_job_id,
                "character_id": installer_id,
                "character_name": char_name_map.get(installer_id, str(installer_id)),
                "activity_id": activity_id,
                "activity_name": activity_name,
                "blueprint_type_id": bp_type_id,
                "blueprint_name": bp_name,
                "product_type_id": prod_type_id,
                "product_name": prod_name,
                "runs": int(getattr(job, "runs", 0) or 0),
                "output_quantity": int(getattr(job, "output_quantity", 0) or 0),
                "cost": float(getattr(job, "cost", 0) or 0),
                "start_date": getattr(job, "start_date", None),
                "end_date": getattr(job, "end_date", None),
                "facility_id": int(getattr(job, "facility_id", 0) or 0),
                "station_id": int(raw.get("station_id", 0) or 0) if isinstance(raw, dict) else 0,
                "duration_seconds": int(raw.get("duration", 0) or 0) if isinstance(raw, dict) else 0,
                "blueprint_location_id": int(getattr(job, "location_id", 0) or 0),
                "output_location_id": int(getattr(job, "output_location_id", 0) or 0),
                "is_corp_job": True,
            })

        rows.sort(key=lambda r: r.get("end_date") or "")
        return {"jobs": rows, "slot_capacities": slot_capacities}

    def get_blueprint_skill_qualification(
        self,
        *,
        blueprint_type_ids: list[int],
    ) -> dict[int, dict[int, bool]]:
        """
        For each blueprint_type_id, return whether each configured character
        meets the manufacturing skill requirements.

        Returns: {blueprint_type_id: {character_id: bool}}
        """
        if not blueprint_type_ids:
            return {}

        characters = self._state.char_manager.get_characters()
        character_ids: list[int] = [int(c["character_id"]) for c in characters if c.get("character_id")]

        # Fetch trained skill levels for each character
        char_skill_levels: dict[int, dict[int, int]] = {}
        for character_id in character_ids:
            try:
                char_skill_levels[character_id] = self._get_character_trained_skill_levels(
                    character_id=character_id
                )
            except Exception:
                char_skill_levels[character_id] = {}

        # Query blueprints from SDE
        sde_session: Any = self._sessions.sde_session()
        blueprint_rows = (
            sde_session.query(Blueprints)
            .filter(Blueprints.blueprintTypeID.in_(blueprint_type_ids))
            .all()
        )
        blueprint_skills_by_type_id: dict[int, list[dict[str, int]]] = {}
        for bp in blueprint_rows:
            activities = bp.activities or {}
            if not isinstance(activities, dict):
                activities = {}
            mfg_skills = activities.get("1", {}).get("skills", [])
            if not isinstance(mfg_skills, list):
                mfg_skills = []
            blueprint_skills_by_type_id[int(bp.blueprintTypeID)] = [
                s for s in mfg_skills if isinstance(s, dict)
            ]

        result: dict[int, dict[int, bool]] = {}
        for bp_type_id in blueprint_type_ids:
            required_skills = blueprint_skills_by_type_id.get(bp_type_id)
            if required_skills is None:
                # Blueprint not found in SDE — no requirements, all qualify
                result[bp_type_id] = {cid: True for cid in character_ids}
                continue

            char_qual: dict[int, bool] = {}
            for character_id in character_ids:
                trained = char_skill_levels.get(character_id) or {}
                qualifies = all(
                    trained.get(int(req.get("typeID") or 0), 0) >= int(req.get("level") or 0)
                    for req in required_skills
                )
                char_qual[character_id] = qualifies
            result[bp_type_id] = char_qual

        return result

    def get_industry_reorder_alerts(
        self,
        *,
        type_ids: list[int],
        lookback_days: int = 30,
        today_iso: str,
    ) -> dict[int, dict]:
        """
        For each type_id, compute reorder urgency based on current stock and sell velocity.

        Returns: {type_id: {"reorder_date": str|None, "days_until_reorder": float|None,
                             "urgency": "urgent"|"soon"|"ok"|"no_data",
                             "stock_qty": int, "velocity_per_day": float|None}}
        """
        from datetime import date, timedelta

        if not type_ids:
            return {}

        corps = self._state.corp_manager.get_corporations()
        corp_ids = [int(c["corporation_id"]) for c in corps if c.get("corporation_id")]

        # If no corporations are configured, return no_data for all type_ids
        if not corp_ids:
            return {
                tid: {
                    "reorder_date": None,
                    "days_until_reorder": None,
                    "urgency": "no_data",
                    "stock_qty": 0,
                    "velocity_per_day": None,
                }
                for tid in type_ids
            }

        # Parse today
        today = date.fromisoformat(today_iso)
        cutoff_str = (today - timedelta(days=lookback_days)).isoformat()

        app_session: Any = self._sessions.app_session()

        # --- Hangar stock from corporation assets ---
        asset_rows = (
            app_session.query(CorporationAssetsModel)
            .filter(
                CorporationAssetsModel.corporation_id.in_(corp_ids),
                CorporationAssetsModel.type_id.in_(type_ids),
                CorporationAssetsModel.is_blueprint_copy == False,  # noqa: E712
                CorporationAssetsModel.is_singleton == False,  # noqa: E712
            )
            .all()
        )
        hangar_qty: dict[int, int] = {}
        for row in asset_rows:
            tid = int(row.type_id)
            hangar_qty[tid] = hangar_qty.get(tid, 0) + int(row.quantity or 0)

        # --- In-progress quantity from active manufacturing jobs ---
        job_rows = (
            app_session.query(CorporationIndustryJobsModel)
            .filter(
                CorporationIndustryJobsModel.corporation_id.in_(corp_ids),
                CorporationIndustryJobsModel.status == "active",
                CorporationIndustryJobsModel.product_type_id.in_(type_ids),
            )
            .all()
        )
        job_qty: dict[int, int] = {}
        for row in job_rows:
            tid = int(row.product_type_id)
            job_qty[tid] = job_qty.get(tid, 0) + int(row.output_quantity or 0)

        # --- Sell velocity from wallet transactions ---
        tx_rows = (
            app_session.query(CorporationWalletTransactionsModel)
            .filter(
                CorporationWalletTransactionsModel.corporation_id.in_(corp_ids),
                CorporationWalletTransactionsModel.type_id.in_(type_ids),
                CorporationWalletTransactionsModel.is_buy == False,  # noqa: E712
                CorporationWalletTransactionsModel.date >= cutoff_str,
            )
            .all()
        )
        sold_qty: dict[int, int] = {}
        for row in tx_rows:
            tid = int(row.type_id)
            sold_qty[tid] = sold_qty.get(tid, 0) + int(row.quantity or 0)

        # --- Compute urgency for each type_id ---
        output: dict[int, dict] = {}
        for tid in type_ids:
            stock_qty = hangar_qty.get(tid, 0) + job_qty.get(tid, 0)
            total_sold = sold_qty.get(tid, 0)

            if total_sold == 0:
                output[tid] = {
                    "reorder_date": None,
                    "days_until_reorder": None,
                    "urgency": "no_data",
                    "stock_qty": stock_qty,
                    "velocity_per_day": None,
                }
                continue

            velocity_per_day = total_sold / lookback_days
            days_of_stock = stock_qty / velocity_per_day
            reorder_date = (today + timedelta(days=days_of_stock)).isoformat()

            if days_of_stock < 3:
                urgency = "urgent"
            elif days_of_stock < 7:
                urgency = "soon"
            else:
                urgency = "ok"

            output[tid] = {
                "reorder_date": reorder_date,
                "days_until_reorder": days_of_stock,
                "urgency": urgency,
                "stock_qty": stock_qty,
                "velocity_per_day": velocity_per_day,
            }

        return output

    def get_type_track_record(
        self,
        *,
        type_ids: list[int],
        lookback_days: int = 90,
        today_iso: str,
    ) -> dict[int, dict]:
        """
        Returns {type_id: {"status": "proven"|"unprofitable"|"marginal"|"untested",
                            "avg_margin_fraction": float|None,
                            "sale_count": int}}

        Status rules (based on last `lookback_days` days of CorporationRealizedSalesLedgerModel):
        - No rows for type_id in period -> "untested", avg_margin_fraction=None, sale_count=0
        - Has rows and avg realized_margin_fraction >= 0.10 -> "proven"
        - Has rows and avg realized_margin_fraction < 0.0 -> "unprofitable"
        - Has rows and 0.0 <= avg realized_margin_fraction < 0.10 -> "marginal"
        """
        from datetime import datetime, timedelta

        if not type_ids:
            return {}

        corps = self._state.corp_manager.get_corporations()
        corp_ids = [int(c["corporation_id"]) for c in corps if c.get("corporation_id")]

        if not corp_ids:
            return {
                tid: {
                    "status": "untested",
                    "avg_margin_fraction": None,
                    "sale_count": 0,
                }
                for tid in type_ids
            }

        cutoff_str = (datetime.fromisoformat(today_iso) - timedelta(days=lookback_days)).isoformat()[:10]

        session: Any = self._sessions.app_session()

        rows = (
            session.query(CorporationRealizedSalesLedgerModel)
            .filter(
                CorporationRealizedSalesLedgerModel.corporation_id.in_(corp_ids),
                CorporationRealizedSalesLedgerModel.type_id.in_(type_ids),
                CorporationRealizedSalesLedgerModel.date >= cutoff_str,
                CorporationRealizedSalesLedgerModel.realized_margin_fraction.isnot(None),
            )
            .all()
        )

        # Group by type_id
        margin_totals: dict[int, float] = {}
        sale_counts: dict[int, int] = {}
        for row in rows:
            tid = int(row.type_id)
            margin_totals[tid] = margin_totals.get(tid, 0.0) + float(row.realized_margin_fraction)
            sale_counts[tid] = sale_counts.get(tid, 0) + 1

        output: dict[int, dict] = {}
        for tid in type_ids:
            count = sale_counts.get(tid, 0)
            if count == 0:
                output[tid] = {
                    "status": "untested",
                    "avg_margin_fraction": None,
                    "sale_count": 0,
                }
            else:
                avg = margin_totals[tid] / count
                if avg >= 0.10:
                    status = "proven"
                elif avg < 0.0:
                    status = "unprofitable"
                else:
                    status = "marginal"
                output[tid] = {
                    "status": status,
                    "avg_margin_fraction": avg,
                    "sale_count": count,
                }

        return output
