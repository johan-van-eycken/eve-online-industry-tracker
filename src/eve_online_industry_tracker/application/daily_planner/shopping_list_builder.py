"""ShoppingListBuilder — Phase 7: build material shopping list net of corp stock.

Uses vwap_5d from market_depth_cache as the primary price estimate.
Falls back to spot_sell_price if vwap_5d unavailable.

already_allocated tracking prevents double-buying when two jobs share a material.
"""
from __future__ import annotations

import logging
from typing import Any

from eve_online_industry_tracker.application.daily_planner.models import AssignedAction, ShoppingItem

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
    ) -> list[ShoppingItem]:
        """Return shopping list of net required materials.

        assigned_actions: from Phase 6 (includes manufacture, sub_manufacture, invent, copy, me_research, te_research)
        corp_assets: all corp asset objects
        market_depth_cache: keyed by type_id → MarketDepthCacheModel or dict
        admin_settings: for future_stock_days and other config
        blueprint_data: type_id → blueprint manufacturing data (for material lookups)
        """
        future_stock_days = float(_adm(admin_settings, "planner_shopping_future_stock_days", 7))

        # Build corp stock map: type_id → available quantity
        corp_stock: dict[int, int] = self._build_corp_stock_map(corp_assets)

        # Track allocated quantities to prevent double-buying
        already_allocated: dict[int, int] = {}

        shopping: list[ShoppingItem] = []

        # Process actions in priority order (current_job first, then future_stock)
        # Split by category
        current_job_actions = [a for a in assigned_actions if a.action_type in ("manufacture", "sub_manufacture")]
        invention_actions = [a for a in assigned_actions if a.action_type == "invent"]
        # buy_materials and buy_bpo are generated here, not from Phase 6

        for action in current_job_actions:
            bp_type_id = action.type_id  # for mfg/sub_mfg, type_id is the product type_id
            # Look up materials from blueprint_data by matching product type_id → blueprint type_id
            mats = self._get_materials_for_action(action, blueprint_data)
            for mat in mats:
                mat_type_id = int(mat.get("type_id") or 0)
                if mat_type_id <= 0:
                    continue
                qty_needed = int(mat.get("quantity") or 0)
                if qty_needed <= 0:
                    continue

                mat_name = str(mat.get("type_name") or f"type_{mat_type_id}")
                net_required = self._compute_net_required(
                    mat_type_id=mat_type_id,
                    qty_needed=qty_needed,
                    corp_stock=corp_stock,
                    already_allocated=already_allocated,
                )
                if net_required <= 0:
                    continue

                # Mark as allocated
                already_allocated[mat_type_id] = already_allocated.get(mat_type_id, 0) + qty_needed

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

    def _get_materials_for_action(
        self,
        action: AssignedAction,
        blueprint_data: dict[int, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Get required materials for a manufacture/sub_manufacture action.

        Looks up blueprint_data by the action's type_id (product type_id).
        """
        # blueprint_data is keyed by blueprint_type_id; we need product_type_id → blueprint_type_id mapping
        # Check if the overview_row on the action has blueprint_type_id
        overview = getattr(action, "overview_row", {}) or {}
        bp_type_id = int(overview.get("blueprint_type_id") or 0)

        if bp_type_id and bp_type_id in blueprint_data:
            mfg = blueprint_data[bp_type_id].get("manufacturing", {})
            return mfg.get("materials", []) or []

        # Fallback: search blueprint_data for a blueprint whose product matches action.type_id
        for bpd in blueprint_data.values():
            products = bpd.get("manufacturing", {}).get("products", []) or []
            for prod in products:
                if int(prod.get("type_id") or 0) == action.type_id:
                    return bpd.get("manufacturing", {}).get("materials", []) or []

        return []

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

    def _build_corp_stock_map(self, corp_assets: list[Any]) -> dict[int, int]:
        """Build {type_id: quantity} map from corp assets."""
        stock: dict[int, int] = {}
        for asset in corp_assets:
            # Skip blueprints — they're not material stock
            if _asset_attr_bool(asset, "is_blueprint"):
                continue
            type_id = int(_asset_attr(asset, "type_id") or 0)
            if type_id <= 0:
                continue
            qty = int(_asset_attr(asset, "quantity") or 0)
            if qty <= 0:
                continue
            stock[type_id] = stock.get(type_id, 0) + qty
        return stock


def _adm(admin_settings: Any, key: str, fallback: Any) -> Any:
    try:
        return admin_settings.get("daily_planner", key)
    except Exception:
        return fallback


def _get_attr(obj: Any, attr: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(attr)
    return getattr(obj, attr, None)


def _asset_attr(asset: Any, attr: str) -> Any:
    if isinstance(asset, dict):
        return asset.get(attr)
    return getattr(asset, attr, None)


def _asset_attr_bool(asset: Any, attr: str) -> bool:
    v = _asset_attr(asset, attr)
    return bool(v) if v is not None else False
