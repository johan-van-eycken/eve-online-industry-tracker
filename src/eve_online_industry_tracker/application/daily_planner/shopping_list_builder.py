"""ShoppingListBuilder — Phase 7: build material shopping list net of corp stock.

Uses vwap_5d from market_depth_cache as the primary price estimate.
Falls back to spot_sell_price if vwap_5d unavailable.

already_allocated tracking prevents double-buying when two jobs share a material.
"""
from __future__ import annotations

import logging
import math
from typing import Any

from eve_online_industry_tracker.application.daily_planner.models import AssignedAction, ShoppingItem
from eve_online_industry_tracker.application.industry import overview_row as orow

logger = logging.getLogger(__name__)


class ShoppingListBuilder:
    """Phase 7: compute net material requirements."""

    def build(
        self,
        assigned_actions: list[AssignedAction],
        corp_assets: list[Any],
        market_depth_cache: dict[int, Any],
        admin_settings: Any,
        blueprint_data: dict[int, dict[str, Any]],
        meta_resolver: Any,
    ) -> list[ShoppingItem]:
        """Return shopping list of net required materials.

        assigned_actions: from Phase 6 (includes manufacture, sub_manufacture, invent, copy, me_research, te_research)
        corp_assets: all corp asset objects
        market_depth_cache: keyed by type_id → MarketDepthCacheModel or dict
        admin_settings: for future config (currently unused)
        blueprint_data: type_id → blueprint manufacturing data (for material lookups)
        meta_resolver: TypeMetadataResolver, used to tell blueprints apart from
            material stock in corp_assets
        """
        # Build corp stock map: type_id → available quantity
        corp_stock: dict[int, int] = self._build_corp_stock_map(corp_assets, meta_resolver)

        # Track allocated quantities to prevent double-buying
        already_allocated: dict[int, int] = {}

        shopping: list[ShoppingItem] = []

        # Process actions in priority order (current_job first, then future_stock)
        # Split by category
        current_job_actions = [a for a in assigned_actions if a.action_type in ("manufacture", "sub_manufacture")]
        invention_actions = [a for a in assigned_actions if a.action_type == "invent"]
        # buy_materials and buy_bpo are generated here, not from Phase 6

        for action in current_job_actions:
            # Look up materials from blueprint_data by matching product type_id → blueprint type_id
            mats, per_run_output = self._get_materials_and_output_for_action(action, blueprint_data)
            runs = self._effective_runs(action, per_run_output)
            for mat in mats:
                mat_type_id = int(mat.get("type_id") or 0)
                if mat_type_id <= 0:
                    continue
                per_run_qty = int(mat.get("quantity") or 0)
                if per_run_qty <= 0:
                    continue

                # Blueprint material quantities are per run.
                qty_needed = per_run_qty * runs

                mat_name = str(mat.get("type_name") or f"type_{mat_type_id}")
                net_required = self._compute_net_required(
                    mat_type_id=mat_type_id,
                    qty_needed=qty_needed,
                    corp_stock=corp_stock,
                    already_allocated=already_allocated,
                )

                # Claim the stock this job consumes before any early exit, or the
                # next job is told the same units are still free.
                already_allocated[mat_type_id] = (
                    already_allocated.get(mat_type_id, 0) + qty_needed
                )

                if net_required <= 0:
                    continue

                estimated_unit_price, is_vwap = self._get_price(mat_type_id, market_depth_cache)
                if estimated_unit_price is None or estimated_unit_price <= 0:
                    logger.debug("ShoppingListBuilder: no price for type_id=%s", mat_type_id)
                    continue

                shopping.append(ShoppingItem(
                    type_id=mat_type_id,
                    type_name=mat_name,
                    quantity=net_required,
                    shopping_category="current_job",
                    estimated_unit_price=estimated_unit_price,
                    estimated_total=estimated_unit_price * net_required,
                    notes=None if is_vwap else "price: spot (no VWAP data)",
                ))

        # Invention inputs (datacores, decryptors)
        for action in invention_actions:
            inv_mats = action.overview_row.get("invention_materials") or [] if hasattr(action, "overview_row") else []
            # inv_mats may not be directly on AssignedAction; check notes or skip for now
            # (full invention input resolution requires SDE data — simplified here)
            pass

        return shopping

    def _get_materials_and_output_for_action(
        self,
        action: AssignedAction,
        blueprint_data: dict[int, dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], int]:
        """Materials and per-run product output for a manufacture/sub_manufacture action.

        Looks up blueprint_data by the action's type_id (product type_id) and
        returns (materials, per_run_output) from the same matched blueprint, so
        callers needing both (e.g. to convert a target quantity into a run
        count) do not look the blueprint up twice.

        per_run_output is the quantity of action.type_id produced by one run of
        the matched blueprint, or 0 when no matching product entry is found
        (materials may still be non-empty via the bp_type_id path even then).
        """
        # blueprint_data is keyed by blueprint_type_id; we need product_type_id → blueprint_type_id mapping.
        # Read the nested overview-row path (with top-level fallback) through the
        # shared accessor, since no producer writes blueprint_type_id at the row's
        # top level.
        overview = getattr(action, "overview_row", {}) or {}
        bp_type_id = orow.get_blueprint_type_id(overview) or 0

        mfg: dict[str, Any] | None = None
        if bp_type_id and bp_type_id in blueprint_data:
            mfg = blueprint_data[bp_type_id].get("manufacturing", {})
        else:
            # Fallback: search blueprint_data for a blueprint whose product matches action.type_id
            for bpd in blueprint_data.values():
                products = bpd.get("manufacturing", {}).get("products", []) or []
                if any(int(p.get("type_id") or 0) == action.type_id for p in products):
                    mfg = bpd.get("manufacturing", {})
                    break

        if mfg is None:
            return [], 0

        materials = mfg.get("materials", []) or []
        per_run_output = 0
        for prod in mfg.get("products", []) or []:
            if int(prod.get("type_id") or 0) == action.type_id:
                per_run_output = int(prod.get("quantity") or 0)
                break

        return materials, per_run_output

    def _effective_runs(self, action: AssignedAction, per_run_output: int) -> int:
        """Runs to buy materials for.

        `manufacture` actions carry a real run count in `action.runs`; use it
        directly when positive. `sub_manufacture` actions instead carry the
        target component quantity in `action.quantity` with `action.runs` left
        None (character_assigner.py), so derive runs as
        ceil(quantity / per_run_output). Guards the division: a missing, zero,
        or non-numeric per_run_output falls back to 1 run (never divides by
        zero), logged at debug since it means the blueprint data is
        incomplete rather than that only 1 unit was actually needed.
        """
        runs = _positive_int(action.runs)
        if runs is not None:
            return runs

        quantity = _positive_int(action.quantity)
        if quantity is not None:
            if per_run_output > 0:
                return math.ceil(quantity / per_run_output)
            logger.debug(
                "ShoppingListBuilder: type_id=%s has quantity=%s but no usable "
                "per-run output (blueprint data incomplete) — falling back to 1 run",
                action.type_id, quantity,
            )
            return 1

        return 1

    def _compute_net_required(
        self,
        mat_type_id: int,
        qty_needed: int,
        corp_stock: dict[int, int],
        already_allocated: dict[int, int],
    ) -> int:
        """Net required = needed - stock - already allocated to other planned jobs."""
        available = corp_stock.get(mat_type_id, 0)
        allocated = already_allocated.get(mat_type_id, 0)
        net = qty_needed - max(0, available - allocated)
        return max(0, net)

    def _get_price(
        self, type_id: int, market_depth_cache: dict[int, Any]
    ) -> tuple[float | None, bool]:
        """Return (price, is_vwap). Prefers vwap_5d over spot_sell_price."""
        entry = market_depth_cache.get(type_id)
        if entry is None:
            return None, False

        vwap = _get_attr(entry, "vwap_5d")
        if vwap is not None:
            return float(vwap), True

        spot = _get_attr(entry, "spot_sell_price")
        if spot is not None:
            return float(spot), False

        return None, False

    def _build_corp_stock_map(
        self, corp_assets: list[Any], meta_resolver: Any
    ) -> dict[int, int]:
        """{type_id: quantity} of material stock, excluding blueprints.

        Pre-warms meta_resolver's cache with every distinct asset type_id in one
        batched SDE query. Without this, TypeMetadataResolver._entry() self-heals
        a cache miss by calling prefetch() for a single id, so is_blueprint()
        inside the per-asset loop below would otherwise open one SDE session
        (with its metaGroups table reflection) per distinct type_id. prefetch()
        is idempotent (skips ids already cached or already marked missing), so
        this is safe even if a caller already warmed it. Same shape as
        index_blueprint_assets in daily_planner/service.py.
        """
        type_ids = {int(_asset_attr(a, "type_id") or 0) for a in corp_assets}
        meta_resolver.prefetch({t for t in type_ids if t > 0})

        stock: dict[int, int] = {}
        for asset in corp_assets:
            type_id = int(_asset_attr(asset, "type_id") or 0)
            if type_id <= 0:
                continue
            if meta_resolver.is_blueprint(type_id):
                continue  # blueprints are not material stock
            qty = int(_asset_attr(asset, "quantity") or 0)
            if qty <= 0:
                continue
            stock[type_id] = stock.get(type_id, 0) + qty
        return stock


def _positive_int(value: Any) -> int | None:
    """int(value) if it converts to a positive integer, else None."""
    if value is None:
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _get_attr(obj: Any, attr: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(attr)
    return getattr(obj, attr, None)


def _asset_attr(asset: Any, attr: str) -> Any:
    if isinstance(asset, dict):
        return asset.get(attr)
    return getattr(asset, attr, None)
