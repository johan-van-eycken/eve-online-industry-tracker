from __future__ import annotations

from typing import Any

from sqlalchemy import and_
from eve_online_industry_tracker.application.industry.sales_history_service import SalesHistoryService
from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService
from eve_online_industry_tracker.infrastructure.models import CharacterAssetsModel
from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider, StateSessionProvider


class PricingSuggestionService:
    """Suggest optimal sell prices based on market data and sales history."""

    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)
        self._sales_history = SalesHistoryService(state=state, sessions=sessions)
        self._market_history = MarketHistoryService(state=state, sessions=sessions)

    def suggest_price(
        self,
        *,
        character_id: int,
        type_id: int,
        current_price: float,
        hub: str = "jita",
        region_id: int = 10000002,
    ) -> dict[str, Any]:
        """Suggest a sell price with detailed breakdown.

        Returns dict with:
        - advised_price: recommended price
        - confidence: high/medium/low
        - breakdown: detailed component analysis for UI
        - reasoning: human-readable explanation
        - cost_basis_source: 'asset' or 'market_order_fallback' or None
        """
        # Get all inputs
        my_sales = self._sales_history.suggest_sell_price(character_id=character_id, type_id=type_id)
        market_data = self._market_history.get_price_stats(type_id=type_id, region_id=region_id)
        volume_data = self._market_history.get_volume_stats(type_id=type_id, region_id=region_id)
        cost_basis, acquisition_source, cost_source = self._get_cost_basis(
            character_id=character_id,
            type_id=type_id,
            fallback_price=current_price
        )

        # Fetch current market hub price
        hub_price = self._get_hub_price(type_id=type_id, hub=hub)

        # Calculate weighted price
        breakdown = self._calculate_breakdown(
            my_sales=my_sales,
            market_data=market_data,
            volume_data=volume_data,
            hub_price=hub_price,
            cost_basis=cost_basis,
            current_price=current_price,
        )

        advised_price = breakdown["weighted_price"]
        confidence = self._assess_confidence(my_sales, market_data, volume_data, cost_basis)

        return {
            "advised_price": advised_price,
            "confidence": confidence,
            "current_price": current_price,
            "price_difference": advised_price - current_price,
            "price_difference_pct": ((advised_price - current_price) / current_price * 100) if current_price > 0 else 0,
            "cost_basis": cost_basis,
            "acquisition_source": acquisition_source,
            "cost_basis_source": cost_source,
            "breakdown": breakdown,
            "reasoning": self._build_reasoning(breakdown, confidence),
        }

    def _get_hub_price(self, *, type_id: int, hub: str = "jita") -> float | None:
        """Get current hub price from market pricing service."""
        import logging
        try:
            pricing_service = getattr(self._state, "market_pricing_service", None)
            if pricing_service is None:
                logging.debug(f"Hub price: market_pricing_service not available (type_id={type_id})")
                return None

            price_map = pricing_service.get_type_price_map(
                type_ids=[type_id],
                hub=hub,
                side="sell",
            )
            if type_id in price_map:
                hub_price = float(price_map[type_id].get("unit_price") or 0)
                logging.debug(f"Hub price (type_id={type_id}, {hub}): {hub_price}")
                return hub_price
            else:
                logging.debug(f"Hub price: type_id {type_id} not in price_map")
        except Exception as e:
            logging.debug(f"Hub price error (type_id={type_id}): {e}")
        return None

    def _get_cost_basis(self, *, character_id: int, type_id: int, fallback_price: float | None = None) -> tuple[float | None, str | None, str | None]:
        """Get average acquisition cost and source for items in inventory.

        Returns: (cost_basis, acquisition_source, cost_source)
        where cost_source is 'asset', 'market_order_fallback', or None
        """
        import logging
        app_session = self._sessions.app_session()
        try:
            assets = app_session.query(CharacterAssetsModel).filter(
                and_(
                    CharacterAssetsModel.character_id == character_id,
                    CharacterAssetsModel.type_id == type_id,
                )
            ).all()

            if not assets:
                logging.debug(f"Cost basis: no assets found (char_id={character_id}, type_id={type_id})")
                # Fallback to market order price if available (ESI data inconsistency workaround)
                if fallback_price and fallback_price > 0:
                    logging.debug(f"Cost basis: using market order price as fallback ({fallback_price})")
                    return fallback_price, "market_order_fallback", "market_order_fallback"
                return None, None, None

            total_cost = 0.0
            total_quantity = 0
            sources = set()

            for asset in assets:
                cost = asset.acquisition_unit_cost
                qty = asset.quantity
                if cost and cost > 0 and qty and qty > 0:
                    total_cost += cost * qty
                    total_quantity += qty
                if asset.acquisition_source:
                    sources.add(asset.acquisition_source)

            if total_quantity > 0:
                avg_cost = total_cost / total_quantity
                # Prefer more specific sources
                source = None
                if "manufactured" in sources:
                    source = "manufactured"
                elif "bought" in sources or "market" in sources:
                    source = "bought"
                elif sources:
                    source = list(sources)[0]

                logging.debug(f"Cost basis found: {avg_cost} ({source}), char_id={character_id}, type_id={type_id}")
                return avg_cost, source, "asset"

            logging.debug(f"Cost basis: found {len(assets)} asset(s) but none with costs, char_id={character_id}, type_id={type_id}")
            # Fallback to market order price if no cost data on assets
            if fallback_price and fallback_price > 0:
                logging.debug(f"Cost basis: using market order price as fallback for assetless item ({fallback_price})")
                return fallback_price, "market_order_fallback", "market_order_fallback"
            return None, None, None
        finally:
            try:
                app_session.close()
            except Exception:
                pass

    def _calculate_breakdown(
        self,
        *,
        my_sales: dict[str, Any],
        market_data: dict[str, Any],
        volume_data: dict[str, Any],
        hub_price: float | None,
        cost_basis: float | None,
        current_price: float,
    ) -> dict[str, Any]:
        """Calculate components and weighted price."""
        components = {}

        # Component 0: Your cost basis (15% weight) - floor price
        if cost_basis and cost_basis > 0:
            # Add 5% margin on top of cost basis as the floor
            cost_with_margin = cost_basis * 1.05
            components["your_cost_basis"] = {
                "value": float(cost_with_margin),
                "weight": 0.15,
                "raw_cost": float(cost_basis),
                "margin_pct": 5,
            }

        # Component 1: Your recent sales median (20% weight)
        my_sales_price = my_sales.get("suggested_price")
        if my_sales_price and my_sales_price > 0:
            components["your_recent_sales"] = {
                "value": float(my_sales_price),
                "weight": 0.20,
                "sample_size": my_sales.get("sample_size", 0),
                "confidence": my_sales.get("confidence", "none"),
            }

        # Component 2: Current market hub price (35% weight)
        if hub_price and hub_price > 0:
            components["market_hub_price"] = {
                "value": float(hub_price),
                "weight": 0.35,
            }

        # Component 3: Market trend (15% weight)
        market_trend_price = None
        if market_data.get("has_data"):
            avg_42w = market_data.get("avg_42w", 0)
            avg_7d = market_data.get("avg_7d", 0)
            trend_pct = market_data.get("trend_pct", 0)

            # If trending down, be more conservative; if trending up, can price higher
            adjustment_factor = 1.0 + (trend_pct / 100 * 0.5)  # 50% of trend impact
            market_trend_price = avg_42w * adjustment_factor if avg_42w > 0 else None

            if market_trend_price:
                components["market_trend"] = {
                    "value": float(market_trend_price),
                    "weight": 0.15,
                    "trend_pct": trend_pct,
                    "avg_42w": float(avg_42w),
                    "avg_7d": float(avg_7d),
                }

        # Component 4: Liquidity/Undercut adjustment (15% weight)
        liquidity_adjustment = self._calc_liquidity_adjustment(volume_data, current_price)
        if liquidity_adjustment:
            components["liquidity_adjustment"] = {
                "value": float(liquidity_adjustment),
                "weight": 0.15,
                "reasoning": "Based on market volume and order activity",
            }

        # Calculate weighted price
        total_weight = sum(c.get("weight", 0) for c in components.values())
        if total_weight > 0:
            weighted_price = sum(
                c.get("value", 0) * c.get("weight", 0)
                for c in components.values()
                if c.get("value", 0) > 0
            ) / total_weight
        else:
            weighted_price = current_price

        # Ensure we never go below cost basis (with margin)
        if cost_basis and cost_basis > 0:
            min_price = cost_basis * 1.05
            if weighted_price < min_price:
                weighted_price = min_price

        return {
            "components": components,
            "weighted_price": float(weighted_price),
            "total_weight": total_weight,
        }

    def _calc_liquidity_adjustment(
        self,
        volume_data: dict[str, Any],
        current_price: float,
    ) -> float | None:
        """Calculate price adjustment based on liquidity."""
        if not volume_data.get("has_data"):
            return None

        avg_vol = volume_data.get("avg_daily_volume", 0)
        if avg_vol < 100:
            # Low liquidity - be more cautious
            return current_price * 0.95
        elif avg_vol > 10000:
            # High liquidity - can be more aggressive
            return current_price * 1.02
        else:
            # Medium liquidity - neutral
            return current_price

    def _assess_confidence(
        self,
        my_sales: dict[str, Any],
        market_data: dict[str, Any],
        volume_data: dict[str, Any],
        cost_basis: float | None = None,
    ) -> str:
        """Assess confidence in the recommendation."""
        score = 0

        # Cost basis availability
        if cost_basis and cost_basis > 0:
            score += 2

        # Sales history confidence
        sales_confidence = my_sales.get("confidence", "none")
        if sales_confidence == "high":
            score += 3
        elif sales_confidence == "medium":
            score += 2
        elif sales_confidence == "low":
            score += 1

        # Market data completeness
        if market_data.get("has_data"):
            records = market_data.get("record_count", 0)
            if records > 100:
                score += 3
            elif records > 30:
                score += 2
            elif records > 7:
                score += 1

        # Volume data
        if volume_data.get("has_data") and volume_data.get("avg_daily_volume", 0) > 100:
            score += 2

        if score >= 8:
            return "high"
        elif score >= 4:
            return "medium"
        else:
            return "low"

    def _build_reasoning(self, breakdown: dict[str, Any], confidence: str) -> str:
        """Build human-readable explanation."""
        components = breakdown.get("components", {})
        parts = []

        if "your_cost_basis" in components:
            parts.append("Your cost basis (floor)")

        if "your_recent_sales" in components:
            comp = components["your_recent_sales"]
            sample = comp.get("sample_size", 0)
            parts.append(f"Your recent sales ({sample} samples)")

        if "market_hub_price" in components:
            parts.append("Current market hub price")

        if "market_trend" in components:
            comp = components["market_trend"]
            trend = comp.get("trend_pct", 0)
            direction = "rising" if trend > 0 else "falling"
            parts.append(f"Market {direction} ({trend:+.1f}%)")

        if "liquidity_adjustment" in components:
            parts.append("Market liquidity adjustment")

        reasoning = f"Based on: {', '.join(parts)}. Confidence: {confidence}."
        return reasoning
