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
from collections.abc import Collection, Mapping
from typing import Any, Callable, cast

from sqlalchemy.exc import SQLAlchemyError
from eve_online_industry_tracker.db_models import (
    CharacterAssetsModel,
    CharacterIndustryJobsModel,
    CorporationAssetsModel,
    CorporationIndustryJobsModel,
    CorporationRealizedSalesLedgerModel,
    CorporationWalletTransactionsModel,
)
from eve_online_industry_tracker.application.market_analysis.market_history_service import (
    MarketHistoryService,
)
from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA
from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider, StateSessionProvider
from eve_online_industry_tracker.infrastructure.industry_adapter import get_type_data
from eve_online_industry_tracker.application.industry.job_cost import (
    JobSurchargeRates,
    job_install_cost,
    RESEARCH_RANK1_LEVEL_SECONDS,
)
from eve_online_industry_tracker.application.industry import shopping_list as _shopping_list
from eve_online_industry_tracker.application.industry.shopping_list import LATER_BATCH_PLAN_VERSION
from eve_online_industry_tracker.application.industry.invention_model import (
    NO_CHARACTER_SKILLS,
    NO_DECRYPTOR,
    DecryptorChoice,
    DecryptorEvaluation,
    DecryptorOption,
    InventionContext,
    QuietWarningsFilter,
    invented_blueprint_efficiency,
    quiet_warnings,
)
from eve_online_industry_tracker.application.characters.asset_provenance import (
    RealizedInventionInputs,
    RealizedInventionRefresh,
    build_market_price_map,
    load_published_decryptor_choices,
    resnapshot_jobs_without_decryptor_choice,
    save_published_decryptor_choices,
)
from eve_online_industry_tracker.application.characters.character_manager import CharacterDataError
from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS
from eve_online_industry_tracker.application.industry.invention_planning import _segments_key
from eve_online_industry_tracker.application.industry.overview_jobs import OverviewJobsMixin
from eve_online_industry_tracker.application.industry.facilities import FacilitiesMixin
from eve_online_industry_tracker.application.industry.pilot_modifiers import PilotModifiersMixin
from eve_online_industry_tracker.application.industry.market_enrichment import MarketEnrichmentMixin
from eve_online_industry_tracker.application.industry.portfolio_candidates import PortfolioCandidatesMixin
from eve_online_industry_tracker.application.industry.owned_assets import OwnedAssetsMixin
from eve_online_industry_tracker.application.industry.invention_planning import InventionPlanningMixin


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


class IndustryService(
    OverviewJobsMixin,
    FacilitiesMixin,
    PilotModifiersMixin,
    MarketEnrichmentMixin,
    PortfolioCandidatesMixin,
    OwnedAssetsMixin,
    InventionPlanningMixin,
):
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

    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)

    @property
    def _admin(self):
        return getattr(self._state, "admin_settings", None)

    def _adm(self, category: str, key: str) -> Any:
        """Read an admin setting; its schema default when the manager is
        unavailable (no duplicated constant that can drift from it)."""
        admin = self._admin
        if admin is None:
            return ADMIN_SETTINGS_SCHEMA[category]["settings"][key]["default"]
        return admin.get(category, key)

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
        system_index: float | None,
        structure_cost_reduction: float,
        surcharge_rates: JobSurchargeRates,
        index_unknown_reason: str | None = None,
        eiv_unknown_reason: str | None = None,
    ) -> dict[str, Any]:
        """The one install-cost model of every activity (job_cost.py): job
        value x index x (1 - reduction) + job value x the activity's
        surcharge rate. `process_value` is the activity's job value; an
        unknown value or index names its reason in fee_unknown_reason."""
        return job_install_cost(
            activity=activity, process_value=process_value, system_index=system_index,
            structure_cost_reduction=structure_cost_reduction, surcharge_rates=surcharge_rates,
            index_unknown_reason=index_unknown_reason, eiv_unknown_reason=eiv_unknown_reason,
        )

    #: Row labels of the activities whose fee can be unknown (I1).
    _FEE_LABELS: dict[str, str] = {"research_material": "ME research", "research_time": "TE research"}

    @staticmethod
    def _job_fee_unknown_entries(*fees: tuple[str, dict[str, Any] | None, bool | None]) -> list[str]:
        """"<label> (<why>)" for each (label, fee payload, has work) whose
        fee is unknown (total_job_cost None). `has work` None reads it off the
        payload: a job with no base duration (0 runs or attempts) charges no
        fee, so its None is not unknown. An activity whose attempt count is
        unknown (cost_unknown_reason) is named by that reason elsewhere."""
        out: list[str] = []
        for label, fee, has_work in fees:
            if not isinstance(fee, dict) or fee.get("total_job_cost") is not None:
                continue
            if fee.get("cost_unknown_reason"):
                continue
            work = has_work if has_work is not None else float(fee.get("base_duration_seconds") or 0) > 0
            if work:
                out.append(f"{label} ({fee.get('fee_unknown_reason') or 'the fee could not be priced'})")
        return out

    @classmethod
    def _research_target_duration_seconds(cls, *, level_one_duration_seconds: int, target_level: int) -> int:
        if level_one_duration_seconds <= 0 or target_level <= 0:
            return 0
        # EVE's rank-1 level table, kept once in job_cost (it also sets the fee).
        rank = float(level_one_duration_seconds) / float(RESEARCH_RANK1_LEVEL_SECONDS[1])
        rank1_target_seconds = RESEARCH_RANK1_LEVEL_SECONDS.get(int(target_level), 0)
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
    def _aggregate_material_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        aggregated: dict[int, dict[str, Any]] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                type_id = int(entry.get("type_id") or 0)
            except (TypeError, ValueError):
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
                except (TypeError, ValueError):
                    product_type_id = 0
                if product_type_id > 0:
                    _keep_lowest(manufacturing_row_by_product_type_id, product_type_id, row)
            for product in (row.get("reaction_job") or {}).get("products") or []:
                if not isinstance(product, dict):
                    continue
                try:
                    product_type_id = int(product.get("type_id") or 0)
                except (TypeError, ValueError):
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
                except (TypeError, ValueError):
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
                    blueprint_material_efficiency = int(self._adm("industry", "max_blueprint_material_efficiency"))
                    blueprint_time_efficiency = int(self._adm("industry", "max_blueprint_time_efficiency"))
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
        segment_costs: list[dict[str, Any]] = []
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
            segment_costs.append(activity_by_efficiency[efficiency])
        cost_reduction = segment_costs[0]["cost_reduction"]
        sized = self._fill_job_segments(
            job_segments, segment_costs,
            base_time_seconds=int(job.get("time_seconds") or 0),
            materials=[dict(entry) for entry in (job.get("materials") or []) if isinstance(entry, dict)],
        )
        # Runs-weighted over the jobs when they differ; job_segments has each job's own.
        material_reduction = sized["material_reduction"]
        # Labels the runs-weighted material_reduction (see the top-level row).
        reduction_basis = "runs_weighted_job_segments" if sized["reductions_differ"] else None
        adjusted_material_entries: list[dict[str, Any]] = sized["adjusted_material_entries"]
        base_material_entries: list[dict[str, Any]] = sized["base_material_entries"]

        eiv_unknown_reasons: list[str] = []
        process_value, priced_material_count, eiv_material_count = self._sum_estimated_item_value_counted(
            base_material_entries,
            quantity_key="quantity",
            adjusted_price_map=adjusted_market_price_map,
            unknown_reasons=eiv_unknown_reasons,
        )
        cost_index, cost_index_unknown_reason = self._system_cost_index_with_reason(
            profile_payload=selected_industry_profile, activity=activity,
        )
        surcharge_rates = self._profile_job_surcharge_rates(selected_industry_profile)
        job_cost = self._job_install_cost(
            activity=activity,
            process_value=process_value,
            system_index=cost_index,
            structure_cost_reduction=cost_reduction,
            surcharge_rates=surcharge_rates,
            index_unknown_reason=cost_index_unknown_reason,
            eiv_unknown_reason="; ".join(eiv_unknown_reasons) or None,
        )
        direct_duration_seconds = sum(int(job_segment["duration_seconds"]) for job_segment in job_segments)

        total_time_seconds = direct_duration_seconds
        total_job_cost = float(job_cost.get("total_job_cost") or 0.0)
        priced_job_count = 1 if job_cost.get("total_job_cost") is not None else 0
        # Every fee of this plan (and its built children) that is unknown,
        # carried up to the product row (I1): never read as 0 silently.
        job_fee_unknown_reasons: list[str] = self._job_fee_unknown_entries(
            (activity, job_cost, runs > 0),
        )
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
            copy_process_value = self._copy_process_value(per_run_material_eiv, missing_blueprint_copy_runs)
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
            job_fee_unknown_reasons.extend(self._job_fee_unknown_entries(("copying", copying_job_cost, None)))
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
                job_fee_unknown_reasons.extend(self._job_fee_unknown_entries(
                    (self._FEE_LABELS.get(activity_name, activity_name), research_job_cost, None),
                ))
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
                    job_fee_unknown_reasons.extend(str(r) for r in (child_plan.get("job_fee_unknown_reasons") or []))
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
                            **self._build_cost_unknown_fields(child_plan, build_total_cost),
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
                job_fee_unknown_reasons.extend(self._job_fee_unknown_entries(("invention", invention_job_cost, None)))
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
                    job_fee_unknown_reasons.extend(self._job_fee_unknown_entries(
                        ("invention source copying", source_copy_job_cost, None),
                    ))
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
            "job_fee_unknown_reasons": list(dict.fromkeys(job_fee_unknown_reasons)),
            "estimated_item_value": process_value,
            "estimated_item_value_priced_material_count": priced_material_count,
            "estimated_item_value_material_count": eiv_material_count,
            "estimated_material_cost": estimated_material_cost,
            # Unknown (not a partial sum) when any job fee is unknown (Task 15
            # fix round 1, minor 1).
            "estimated_total_cost": (
                float(total_job_cost or 0.0) + float(estimated_material_cost or 0.0)
                if (total_job_cost is not None or estimated_material_cost is not None)
                and not job_fee_unknown_reasons
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
        job_fee_unknown_reasons: list[str] = []
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
                job_fee_unknown_reasons.extend(str(r) for r in (child_plan.get("job_fee_unknown_reasons") or []))
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
                        **self._build_cost_unknown_fields(child_plan, build_total_cost),
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
            "job_fee_unknown_reasons": list(dict.fromkeys(job_fee_unknown_reasons)),
            "procurement_materials": self._aggregate_material_entries(procurement_materials),
            "activity_breakdown": recursive_activity_breakdown,
            "tree_children": tree_children,
            "material_nodes": material_nodes,
        }

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
            except (TypeError, ValueError):
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
                except (TypeError, ValueError):
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
        cost_index, cost_index_unknown_reason = self._system_cost_index_with_reason(
            profile_payload=selected_industry_profile, activity=activity,
        )
        job_cost = self._job_install_cost(activity=activity, process_value=process_value, system_index=cost_index, structure_cost_reduction=cost_reduction, surcharge_rates=surcharge_rates, index_unknown_reason=cost_index_unknown_reason)
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
        sized = self._fill_job_segments(
            job_segments, segment_costs, base_time_seconds=base_time,
            materials=[
                self._compact_material_entry(dict(e))
                for e in (manufacturing_job.get("materials") or []) if isinstance(e, dict)
            ],
        )
        return {**sized, "costs": segment_costs[0]}

    def _fill_job_segments(
        self,
        job_segments: list[dict[str, Any]],
        segment_costs: list[dict[str, Any]],
        *,
        base_time_seconds: int,
        materials: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """The one sizing rule for a batch's jobs (top-level rows and nested
        sub-builds): each job at its own reductions, one ceiling per job and
        material, never per run (EVE). Fills each segment's reductions,
        duration_seconds and materials in place."""
        for segment, segment_cost in zip(job_segments, segment_costs):
            segment["material_reduction"] = float(segment_cost.get("material_reduction", 0.0))
            segment["time_reduction"] = float(segment_cost["time_reduction"])
            segment["duration_seconds"] = self._round_duration_seconds(
                base_time_seconds * int(segment["runs"]) * max(0.0, 1.0 - segment["time_reduction"])
            )
            segment["materials"] = {}
        material_reduction, material_differs = self._runs_weighted_reduction(job_segments, "material_reduction")
        time_reduction, time_differs = self._runs_weighted_reduction(job_segments, "time_reduction")
        total_runs = sum(int(segment["runs"]) for segment in job_segments)
        adjusted: list[dict[str, Any]] = []
        base: list[dict[str, Any]] = []
        for mat in materials:
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
            "material_reduction": material_reduction,
            "time_reduction": time_reduction,
            "reductions_differ": material_differs or time_differs,
        }

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
        pilot_industry_payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Unified activity cost calculator — delegates to _compute_activity_job with per-refresh caching.

        `pilot_industry_payload`: time the job for another corp pilot (a
        parallel invention or copy lane, Task 12) with that pilot's own
        industry payload, skills and implants (the one convention for
        "another pilot", _get_character_industry_modifier_payload)."""
        # The required skills' trained levels change the time reduction
        # (_manufacturing_required_skill_time_reduction), so they are part of the key.
        skill_fingerprint = tuple(sorted(
            (int(entry.get("type_id") or 0), str(entry.get("type_name") or ""), str(entry.get("trained_skill_level") or 0))
            for entry in (required_skill_entries or []) if isinstance(entry, dict)
        ))
        pilot_fingerprint = (
            self._pilot_time_fingerprint(activity, pilot_industry_payload)
            if pilot_industry_payload is not None else None
        )
        cache_key = (activity, base_time_seconds, runs, process_value, manufacturing_group, blueprint_material_efficiency, blueprint_time_efficiency, skill_fingerprint, pilot_fingerprint)
        cached = ctx.activity_job_cache.get(cache_key)
        if cached is not None:
            return dict(cached)
        result = self._compute_activity_job(
            activity=activity,
            base_time_seconds=base_time_seconds,
            runs=runs,
            process_value=process_value,
            selected_industry_profile=ctx.selected_industry_profile,
            selected_character_modifiers=(
                ctx.selected_character_modifiers if pilot_industry_payload is None else dict(pilot_industry_payload)
            ),
            character_skill_levels_by_name=(
                ctx.character_skill_levels_by_name if pilot_industry_payload is None
                else self._skill_levels_by_name(dict(pilot_industry_payload))
            ),
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
        decryptor_selection_enabled = bool(self._adm("industry", "invention_decryptor_selection_enabled"))
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
        _admin_hangar_flag = self._adm("industry", "industry_hangar_flag")
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
            except (TypeError, ValueError):
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
                # Parallel invention and copy lanes (Task 12).
                slot_capacity_by_character_id=self._get_corp_slot_capacity(),
                selected_character_id=int(character_id) if character_id else None,
                source_originals_by_type_id=self._source_originals_by_owner(blueprint_original_assets_by_type_id),
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

    @classmethod
    def _cycle_evaluation(
        cls, ctx: _ProductPlanningContext, option: DecryptorOption, row_out: dict[str, Any] | None,
        *, fees: str = "all",
    ) -> DecryptorEvaluation:
        """A candidate cycle row's cost per unit and profit per cycle.

        The profit is the cycle's net proceeds, the way the row's
        profit_amount is: units x the row's sell price (market_unit_price) x
        (1 - broker fee - sales tax), minus the cycle's cost. Unknown when the
        sell price is unknown or not positive, or the sales tax is unknown;
        select_best_decryptor then falls back to cost per unit with that
        reason. A candidate without a comparable cost gets a skip_reason."""
        quantity = int((row_out or {}).get("quantity") or 0)
        total_cost, why = cls._row_total_cost_estimate(ctx, row_out, fees=fees)
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
            why = cls._sale_fee_amounts(1.0, ctx.market_fee_context)[3] or cls._SALES_TAX_UNKNOWN_REASON
            profit_unknown_reason = f"{why}, so net proceeds are unknown: chose lowest cost per unit"
        else:
            profit = float(sell_price) * quantity * net_fraction - total_cost
        return DecryptorEvaluation(
            option, total_cost / quantity, profit_per_cycle=profit, units_per_cycle=quantity,
            profit_unknown_reason=profit_unknown_reason,
        )

    @classmethod
    def _row_total_cost_estimate(
        cls, ctx: _ProductPlanningContext, row_out: dict[str, Any] | None, *, fees: str = "all",
    ) -> tuple[float | None, str | None]:
        """(a row's total cost before the pricing step, None) or (None, why).

        Priced the way _enrich_product_rows_with_material_prices prices it:
        each procurement and amortized invention line at its own unit price,
        else the hub price (ctx.material_price_map), plus the job fees.
        Unknown when any line is unpriced, the invention cost is unknown, or
        (fees="all") any job fee is unknown: a partial cost is not a cost.
        fees="known" adds only the priced fees and fees="none" none (the
        decryptor comparison, _decryptor_fee_basis, which says so)."""
        if not isinstance(row_out, dict):
            return None, "the candidate row could not be built"
        mj = row_out.get("manufacturing_job") or {}
        if mj.get("invention_cost_unknown_reason"):
            return None, f"invention cost unknown: {mj['invention_cost_unknown_reason']}"
        if fees == "all" and mj.get("job_fee_unknown_reasons"):
            return None, f"job fees unknown for: {'; '.join(str(r) for r in mj['job_fee_unknown_reasons'])}"

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
        return total + (0.0 if fees == "none" else float(mj.get("total_job_cost") or 0.0)), None

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
                blueprint_material_efficiency = int(self._adm("industry", "max_blueprint_material_efficiency"))
                blueprint_time_efficiency = int(self._adm("industry", "max_blueprint_time_efficiency"))
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
        base_material_eiv_unknown_reasons: list[str] = []
        base_material_eiv_total, base_material_eiv_priced_count, base_material_eiv_line_count = self._sum_estimated_item_value_counted(base_material_entries, quantity_key="quantity", adjusted_price_map=ctx.adjusted_market_price_map, unknown_reasons=base_material_eiv_unknown_reasons)
        per_run_material_eiv, _ = self._sum_estimated_item_value(adjusted_material_entries, quantity_key="quantity_per_run", adjusted_price_map=ctx.adjusted_market_price_map)
        product_eiv_unit, product_eiv_source = self._resolve_eiv_pricing(type_id=product_type_id, type_payload=compact_product, adjusted_price_map=ctx.adjusted_market_price_map)
        product_eiv_total = float(product_eiv_unit) * int(compact_product.get("quantity") or 0) if product_eiv_unit is not None else None

        # Recompute manufacturing job cost with actual process value
        manufacturing_cost_index, manufacturing_cost_index_unknown_reason = self._system_cost_index_with_reason(profile_payload=ctx.selected_industry_profile, activity="manufacturing")
        manufacturing_job_cost = self._job_install_cost(activity="manufacturing", process_value=base_material_eiv_total, system_index=manufacturing_cost_index, structure_cost_reduction=manufacturing_cost_reduction, surcharge_rates=ctx.surcharge_rates, index_unknown_reason=manufacturing_cost_index_unknown_reason, eiv_unknown_reason="; ".join(base_material_eiv_unknown_reasons) or None)
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
        # I1: every fee of the row (its own activities and the built
        # sub-components) that could not be priced; never counted as 0 silently.
        job_fee_unknown_reasons: list[str] = self._job_fee_unknown_entries(
            ("manufacturing", manufacturing_job_cost, effective_runs > 0),
        )
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
                job_fee_unknown_reasons.extend(self._job_fee_unknown_entries(("copying", copying_costs, None)))
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
                job_fee_unknown_reasons.extend(self._job_fee_unknown_entries(("invention", inv_costs, None)))
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
            job_fee_unknown_reasons.extend(self._job_fee_unknown_entries(
                ("invention source copying", source_copy_costs, None),
            ))

        # Research (Step 4)
        if include_sde_research_chain:
            for act_name, res_costs, res_node in self._plan_research_chain_for_variant(ctx, row=row, per_run_material_eiv=per_run_material_eiv):
                activity_breakdown[act_name] = res_costs
                total_time_seconds += res_costs["duration_seconds"]
                if res_costs.get("total_job_cost") is not None:
                    total_job_cost += float(res_costs.get("total_job_cost") or 0.0)
                    priced_job_count += 1
                job_fee_unknown_reasons.extend(self._job_fee_unknown_entries(
                    (self._FEE_LABELS.get(act_name, act_name), res_costs, None),
                ))
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
            attempts_per_bpc, per_bpc_unknown = self._whole_attempts_per_blueprint(
                invention_job=invention_job, blueprint_type_id=blueprint_type_id,
                runs_per_bpc=runs_per_bpc, batch_runs=int(effective_runs),
                max_production_limit=max_production_limit,
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
        job_fee_unknown_reasons.extend(
            str(r) for r in (recursive_prerequisite_plan.get("job_fee_unknown_reasons") or [])
        )
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
                "product_market_price_unavailable_reason": product_market_pricing.get("price_unavailable_reason"),
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
            # Why the product price is unknown: its hub order-book fetch failed.
            "price_unavailable_reason": product_market_pricing.get("price_unavailable_reason"),
            "bpc_count": len(ctx.blueprint_copy_assets_by_type_id.get(blueprint_type_id, [])),
            "has_bpo": bool(ctx.blueprint_original_assets_by_type_id.get(blueprint_type_id)),
        }
        if invention_cost_unknown_reason is not None:
            row_out["manufacturing_job"]["invention_cost_unknown_reason"] = invention_cost_unknown_reason
        if job_fee_unknown_reasons:
            # total_job_cost (and total_cost) then hold the priced fees only:
            # a lower bound, flagged by the pricing step and the UI.
            row_out["manufacturing_job"]["job_fee_unknown_reasons"] = list(dict.fromkeys(job_fee_unknown_reasons))
        rig_reason = self._aggregate_only_rig_reason(ctx.selected_industry_profile)
        if rig_reason is not None:
            # A product row is a manufacturing row: the aggregate was never a
            # reaction bonus, so its reaction sub-builds lose nothing.
            row_out["manufacturing_job"]["rig_bonus_unknown_reason"] = rig_reason
            row_out["manufacturing_job"]["rig_bonus_unknown_metrics"] = list(
                self._aggregate_only_rig_metrics(ctx.selected_industry_profile)
            )
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
            # Task 12 fix round 1: one convention for "another pilot". The
            # planner times a research action with the ASSIGNED pilot's own
            # skills and implants: this table has neither (structure and rigs
            # only), and each corp pilot's implant multiplier is beside it.
            row_out["manufacturing_job"]["research_duration_seconds_by_level_base"] = self._research_duration_by_level(
                ctx, row=row, skilled=False, implants=False,
            )
            row_out["manufacturing_job"]["research_implant_time_multiplier_by_character_id"] = (
                self._research_implant_multipliers(ctx)
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
        lane_notes = self._elapsed_lane_notes(row_out["manufacturing_job"])
        if lane_notes:
            # Task 12 fix round 1: the build time's parallel lanes (capacity),
            # or why an invention is timed in one slot. Informational.
            row_out["manufacturing_job"]["elapsed_lane_notes"] = lane_notes
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

        # One refresh = one scope for the once-only WARNINGs (I2): a missing
        # input still missing is logged again by the next refresh.
        self._reset_once_warnings()
        self.__dict__.pop("_facility_tax_warned", None)
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
        # The realized invention estimate uses the same decryptor (realized_invention_inputs).
        self.publish_decryptor_choices(ctx.decryptor_choice_by_blueprint_type_id)
        return product_rows

    @staticmethod
    def _published_decryptor_choices(choices: Mapping[int, DecryptorChoice]) -> dict[int, DecryptorOption]:
        """{invented blueprint type_id: the decryptor this overview invents it
        with}. A "no decryptor data" fallback (NO_DECRYPTOR_DATA_REASON) is
        published as NO_DECRYPTOR: the producer plans and prices that
        invention without a decryptor, so the realized side does too."""
        return {int(bp_type_id): choice.option for bp_type_id, choice in choices.items()}

    def publish_decryptor_choices(self, choices: Mapping[int, DecryptorChoice]) -> dict[int, DecryptorOption]:
        """Publish this overview's decryptor choices for the realized side:
        in memory (the overview cache), persisted in the app DB (read after a
        restart, before the first overview), and, after the first publish of
        the process or one that plans a blueprint the earlier ones did not,
        the stored jobs that had no choice are re-snapshotted.
        A database or ESI failure here is a WARNING: the overview stands."""
        published = self._published_decryptor_choices(choices)
        cached = getattr(self._state, "_overview_result_cache", None)
        if not isinstance(cached, dict):
            cached = {}
            self._state._overview_result_cache = cached
        cached["decryptor_by_blueprint_type_id"] = published
        if self._sessions is None:  # no databases in this process (tests)
            return published
        try:
            session = self._sessions.app_session()
        except RuntimeError as exc:  # no app DB in this process
            logger.warning("Invention: the decryptor choices are not persisted (%s); a restart forgets them", exc)
            return published
        try:
            save_published_decryptor_choices(
                session,
                {bp: (published[bp], choices[bp].reason) for bp in published},
                now=datetime.now(timezone.utc).replace(tzinfo=None),
            )
        except SQLAlchemyError as exc:
            session.rollback()
            logger.warning("Invention: the decryptor choices could not be persisted (%s); a restart forgets them", exc)
        finally:
            session.close()
        self._resnapshot_jobs_after_publish(frozenset(int(bp) for bp in published))
        return published

    def _resnapshot_jobs_after_publish(self, planned: frozenset[int]) -> None:
        """After the first publish of the process, and after any later one
        that plans a blueprint no earlier re-snapshot had a choice for:
        re-snapshot the stored jobs whose invention cost was unknown for want
        of a decryptor choice (the startup job refresh runs before any
        overview; a blueprint the overview starts planning later leaves its
        jobs so too). A failure is a WARNING and the next publish tries again."""
        covered = getattr(self._state, "_realized_invention_resnapshot_blueprints", None)
        if covered is not None and planned <= covered:
            return
        esi_service = getattr(self._state, "esi_service", None)
        if esi_service is None:
            logger.warning("Invention: no ESI service, so no re-snapshot of the jobs without a decryptor choice")
            return
        try:
            market_price_map = build_market_price_map(esi_service.get_market_prices())
        except ESI_REQUEST_ERRORS as exc:
            logger.warning(
                "Invention: market prices unavailable (%s); the jobs without a decryptor choice keep their "
                "reason, the re-snapshot is retried at the next overview", exc,
            )
            return
        app_session = sde_session = None
        try:
            app_session = self._sessions.app_session()
            sde_session = self._sessions.sde_session()
            resnapshot_jobs_without_decryptor_choice(
                app_session=app_session, sde_session=sde_session, market_price_map=market_price_map,
                invention_refresh=RealizedInventionRefresh(inputs=self.realized_invention_inputs()),
            )
            self._state._realized_invention_resnapshot_blueprints = frozenset(covered or ()) | planned
        except (SQLAlchemyError, RuntimeError) as exc:  # RuntimeError: a database is not initialized
            if app_session is not None:
                app_session.rollback()
            logger.warning(
                "Invention: the re-snapshot of the jobs without a decryptor choice failed (%s); it is "
                "retried at the next overview", exc,
            )
        finally:
            if sde_session is not None:
                sde_session.close()
            if app_session is not None:
                app_session.close()

    def realized_invention_inputs(self) -> RealizedInventionInputs:
        """The inputs the realized side sizes an invention with, the producer's
        own: the corp's inventors (_get_inventor_skill_levels), the SDE
        encryption skills, the admin floor, and per invented blueprint the
        decryptor the last overview chose. Decryptor selection off: every
        blueprint is invented without one, as in the producer. On and no
        overview choice: unknown (the estimate then gives a reason)."""
        levels = self._get_inventor_skill_levels()
        context = InventionContext(
            inventor_skill_levels_by_character_id=levels,
            encryption_skill_type_ids=self._load_encryption_skill_type_ids(),
            no_inventor_reason=NO_CHARACTER_SKILLS if levels else self._no_inventor_reason(),
        )
        selection_enabled = bool(self._adm("industry", "invention_decryptor_selection_enabled"))
        published = self._last_published_decryptor_choices() if selection_enabled else None
        return RealizedInventionInputs(
            invention_context=context,
            decryptor_by_blueprint_type_id=dict(published or {}) if selection_enabled else {},
            default_decryptor=None if selection_enabled else NO_DECRYPTOR,
            probability_floor=float(self._adm("industry", "invention_probability_floor")),
            decryptor_choices_published=published is not None,
        )

    def _last_published_decryptor_choices(self) -> dict[int, DecryptorOption] | None:
        """The in-memory overview's decryptor choices, else the persisted
        ones (after a restart), else None. A database error propagates (the
        provider caller turns it into a reason)."""
        cached = getattr(self._state, "_overview_result_cache", None)
        if isinstance(cached, dict) and isinstance(cached.get("decryptor_by_blueprint_type_id"), dict):
            return dict(cached["decryptor_by_blueprint_type_id"])
        if self._sessions is None:  # no databases in this process (tests)
            return None
        try:
            session = self._sessions.app_session()
        except RuntimeError:  # no app DB in this process: nothing was persisted
            return None
        try:
            return load_published_decryptor_choices(session)
        finally:
            session.close()

    def get_cached_overview_rows(self) -> list[dict] | None:
        """Return the most recent product overview rows from in-memory cache, or None if not yet computed
        or computed by an older version of the later-batch plans (it must be rebuilt)."""
        cached = getattr(self._state, "_overview_result_cache", None)
        if isinstance(cached, dict) and cached.get("later_batch_plan_version") == LATER_BATCH_PLAN_VERSION:
            rows = cached.get("rows")
            if isinstance(rows, list):
                return [dict(r) for r in rows]
        return None

    def industry_job_manager_status(self) -> dict:
        mgr = self._ensure_industry_job_manager()
        return mgr.get_status()

    @classmethod
    def _active_job_build_cost_fields(cls, job: Any) -> dict[str, Any]:
        """The stored job's build cost (total and per unit) and why it is
        partly unknown; each None when the row has none."""
        return {
            "total_build_cost": cls._as_float(getattr(job, "total_build_cost", None)),
            "unit_build_cost": cls._as_float(getattr(job, "unit_build_cost", None)),
            "invention_cost_unknown_reason": getattr(job, "invention_cost_unknown_reason", None) or None,
            "materials_cost_lower_bound_reason": getattr(job, "materials_cost_lower_bound_reason", None) or None,
        }

    def industry_active_jobs(self, *, character_id: int | None = None) -> dict[str, Any]:
        """Return active industry jobs and slot capacities, optionally filtered by character.

        Includes both character-scope jobs (CharacterIndustryJobsModel) and
        corporation-scope jobs (CorporationIndustryJobsModel).  Corp jobs are
        attributed to the character who installed them via installer_id so that
        per-character slot counting works correctly.

        An unknown character_id is UnknownCharacterError (a 404,
        unknown_character), not an empty job list.
        """
        from eve_online_industry_tracker.application.unknown_owner import require_known_character
        from eve_online_industry_tracker.infrastructure.sde.activities import activity_label
        from eve_online_industry_tracker.infrastructure.sde.types import get_type_data

        require_known_character(self._state, character_id, "industry_active_jobs")
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
                except (TypeError, ValueError):
                    pass  # cosmetic: no characters (ValueError) or an odd ID leaves the job with its ID
                except CharacterDataError as exc:
                    logger.warning("Active jobs: the character names cannot be read (%s); the jobs keep their "
                                   "character IDs", exc)

        rows: list[dict[str, Any]] = []

        # Build rows for character-scope jobs
        for job in char_jobs:
            raw = getattr(job, "raw", None) or {}
            activity_id = raw.get("activity_id") if isinstance(raw, dict) else None
            activity_name = activity_label(activity_id)

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
                "location_id": int(getattr(job, "location_id", 0) or 0),
                "output_location_id": int(getattr(job, "output_location_id", 0) or 0),
                "is_corp_job": False,
                **self._active_job_build_cost_fields(job),
            })

        # Build rows for corporation-scope jobs — attribute to installer_id
        # Deduplicate: the character endpoint returns corp-structure jobs too,
        # so skip any corp job whose job_id is already in the character rows.
        char_job_ids = {r["job_id"] for r in rows}
        for job in corp_jobs:
            raw = getattr(job, "raw", None) or {}
            activity_id = raw.get("activity_id") if isinstance(raw, dict) else None
            activity_name = activity_label(activity_id)

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
                "location_id": int(getattr(job, "location_id", 0) or 0),
                "output_location_id": int(getattr(job, "output_location_id", 0) or 0),
                "is_corp_job": True,
                **self._active_job_build_cost_fields(job),
            })

        rows.sort(key=lambda r: r.get("end_date") or "")
        return {"jobs": rows, "slot_capacities": slot_capacities}

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
