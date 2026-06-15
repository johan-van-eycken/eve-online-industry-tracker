from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import and_
from eve_online_industry_tracker.application.industry.sales_history_service import SalesHistoryService
from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService
from eve_online_industry_tracker.infrastructure.models import CharacterAssetsModel, CharacterAssetHistoryModel, CharacterModel
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
        quantity: int = 0,
        order_duration_days: int = 90,
    ) -> dict[str, Any]:
        """Suggest a sell price with detailed breakdown.

        Returns dict with:
        - advised_price: recommended price
        - confidence: high/medium/low
        - breakdown: detailed component analysis for UI
        - reasoning: human-readable explanation
        - cost_basis_source: 'asset' or 'market_order_fallback' or None
        - break_even_price: minimum list price for profit after fees
        - net_margin_pct_advised / net_margin_pct_current: margin % after fees
        - estimated_sell_days_advised / estimated_sell_days_current
        - isk_per_day_advised / isk_per_day_current
        - hold_signal: dict with hold recommendation and reasoning
        - relist_risk: dict flagging if order may expire before selling
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

        # Fetch character fees and orderbook queue for profitability metrics
        sales_tax, broker_fee = self._get_fees(character_id=character_id)
        orderbook_levels = self._get_orderbook_levels(type_id=type_id, region_id=region_id)

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

        # Profitability and velocity metrics
        avg_daily_vol = float(volume_data.get("avg_daily_volume", 0)) if volume_data.get("has_data") else 0.0
        break_even = self._calc_break_even(cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)

        # Clamp advised price to fee-aware break-even: the 5% margin floor in
        # _calculate_breakdown() ignores fees, so the weighted price can still
        # be a net loss after sales tax + broker fee.
        if break_even and advised_price < break_even:
            advised_price = break_even
            breakdown["weighted_price"] = advised_price
            breakdown["floored_to_break_even"] = True
        net_margin_advised = self._calc_net_margin(price=advised_price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)
        net_margin_current = self._calc_net_margin(price=current_price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)
        est_days_advised = self._estimate_sell_days(target_price=advised_price, quantity=quantity, orderbook_levels=orderbook_levels, avg_daily_volume=avg_daily_vol)
        est_days_current = self._estimate_sell_days(target_price=current_price, quantity=quantity, orderbook_levels=orderbook_levels, avg_daily_volume=avg_daily_vol)
        isk_per_day_advised = self._calc_isk_per_day(price=advised_price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee, quantity=quantity, est_days=est_days_advised)
        isk_per_day_current = self._calc_isk_per_day(price=current_price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee, quantity=quantity, est_days=est_days_current)
        hold_signal = self._calc_hold_signal(market_data=market_data, hub_price=hub_price, quantity=quantity, isk_per_day_advised=isk_per_day_advised)
        relist_risk = self._calc_relist_risk(est_days=est_days_advised, order_duration_days=order_duration_days, broker_fee=broker_fee, price=advised_price, quantity=quantity)

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
            # Fee metadata
            "sales_tax_fraction": sales_tax,
            "broker_fee_fraction": broker_fee,
            # Profitability
            "break_even_price": break_even,
            "net_margin_pct_advised": net_margin_advised,
            "net_margin_pct_current": net_margin_current,
            # Sell velocity
            "estimated_sell_days_advised": est_days_advised,
            "estimated_sell_days_current": est_days_current,
            # Combined metric
            "isk_per_day_advised": isk_per_day_advised,
            "isk_per_day_current": isk_per_day_current,
            # Signals
            "hold_signal": hold_signal,
            "relist_risk": relist_risk,
        }

    def _get_hub_price(self, *, type_id: int, hub: str = "jita") -> float | None:
        """Get current hub price from market pricing service."""
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

        cost_source values:
          'asset'                — weighted avg from current CharacterAssetsModel rows
          'asset_history'        — most recent CharacterAssetHistoryModel record
                                   (used when item is listed on market and absent
                                   from the current assets snapshot)
          'market_order_fallback' — neither assets nor history found; order price used
        """
        app_session = self._sessions.app_session()
        try:
            assets = app_session.query(CharacterAssetsModel).filter(
                and_(
                    CharacterAssetsModel.character_id == character_id,
                    CharacterAssetsModel.type_id == type_id,
                )
            ).all()

            if assets:
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
                    source = None
                    if "manufactured" in sources:
                        source = "manufactured"
                    elif "bought" in sources or "market" in sources:
                        source = "bought"
                    elif sources:
                        source = list(sources)[0]

                    logging.debug(f"Cost basis found: {avg_cost} ({source}), char_id={character_id}, type_id={type_id}")
                    return avg_cost, source, "asset"

            # Item is likely listed on market — ESI removes active sell orders from
            # the assets endpoint. Fall back to the most recent asset history record.
            logging.debug(f"Cost basis: no current assets with cost (char_id={character_id}, type_id={type_id}), checking history")
            history_row = (
                app_session.query(CharacterAssetHistoryModel)
                .filter(
                    and_(
                        CharacterAssetHistoryModel.character_id == character_id,
                        CharacterAssetHistoryModel.type_id == type_id,
                        CharacterAssetHistoryModel.acquisition_unit_cost.isnot(None),
                    )
                )
                .order_by(CharacterAssetHistoryModel.observed_at.desc())
                .first()
            )
            if history_row and history_row.acquisition_unit_cost and history_row.acquisition_unit_cost > 0:
                source = history_row.acquisition_source or "unknown"
                logging.debug(f"Cost basis from history: {history_row.acquisition_unit_cost} ({source})")
                return float(history_row.acquisition_unit_cost), source, "asset_history"

            # Last resort: use the order's own price as a rough proxy
            if fallback_price and fallback_price > 0:
                logging.debug(f"Cost basis: using market order price as fallback ({fallback_price})")
                return fallback_price, "market_order_fallback", "market_order_fallback"
            return None, None, None
        finally:
            try:
                app_session.close()
            except Exception:
                pass

    def _get_fees(self, *, character_id: int) -> tuple[float, float]:
        """Return (sales_tax_fraction, broker_fee_fraction) for the character.

        Falls back to EVE defaults (7.5% tax, 3% broker) if not found.
        """
        try:
            app_session = self._sessions.app_session()
            try:
                char = app_session.query(CharacterModel).filter(
                    CharacterModel.character_id == character_id
                ).first()
                if char and char.market_fees:
                    fees = json.loads(char.market_fees) if isinstance(char.market_fees, str) else char.market_fees
                    if isinstance(fees, dict):
                        return float(fees.get("sales_tax_fraction", 0.075)), float(fees.get("broker_fee_fraction", 0.03))
            finally:
                app_session.close()
        except Exception as exc:
            logging.debug(f"_get_fees error (char_id={character_id}): {exc}")
        return 0.075, 0.03

    def _get_orderbook_levels(self, *, type_id: int, region_id: int = 10000002) -> list[list[float]]:
        """Return cached Jita sell orderbook levels [[price, volume], ...] sorted ascending by price."""
        try:
            from eve_online_industry_tracker.infrastructure.persistence.market_orderbook_cache_repo import get_cached_orderbook_levels
            app_session = self._sessions.app_session()
            try:
                result = get_cached_orderbook_levels(
                    app_session,
                    hub="jita",
                    region_id=region_id,
                    station_id=60003760,  # Jita 4-4
                    side="sell",
                    type_id=type_id,
                    at_hub=True,
                    ttl_seconds=3600,
                )
                if result is not None:
                    levels, _ = result
                    return sorted(levels, key=lambda lv: lv[0])
            finally:
                app_session.close()
        except Exception as exc:
            logging.debug(f"_get_orderbook_levels error (type_id={type_id}): {exc}")
        return []

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

        # Calculate weighted price (renormalise weights for missing components)
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

    def _estimate_sell_days(
        self,
        *,
        target_price: float,
        quantity: int,
        orderbook_levels: list[list[float]],
        avg_daily_volume: float,
    ) -> float | None:
        """Estimate trading days to sell `quantity` units listed at `target_price`.

        Uses the cached Jita sell orderbook to compute queue depth ahead of our
        price, then estimates our proportional share of remaining daily volume.
        """
        if not quantity or quantity <= 0 or not avg_daily_volume or avg_daily_volume <= 0:
            return None

        # Volume from sell orders priced strictly below our target (must clear before ours)
        volume_ahead = sum(int(vol) for price, vol in orderbook_levels if price < target_price)

        # Volume listed at or just above our price (we compete with them for daily demand)
        volume_at_level = sum(
            int(vol) for price, vol in orderbook_levels
            if target_price <= price <= target_price * 1.001
        )

        # Days until our order reaches the effective front of the queue
        days_until_front = volume_ahead / avg_daily_volume

        # Proportional share of daily demand: our units vs all units at our level
        total_at_level = quantity + volume_at_level
        capture_rate = min(0.50, max(0.05, quantity / total_at_level))

        days_for_our_units = quantity / (avg_daily_volume * capture_rate)
        return float(days_until_front + days_for_our_units)

    def _calc_break_even(
        self, *, cost_basis: float | None, sales_tax: float, broker_fee: float
    ) -> float | None:
        """Minimum list price to recover cost after sales tax and broker fee."""
        if not cost_basis or cost_basis <= 0:
            return None
        net_rate = 1.0 - sales_tax - broker_fee
        return float(cost_basis / net_rate) if net_rate > 0 else None

    def _calc_net_margin(
        self,
        *,
        price: float,
        cost_basis: float | None,
        sales_tax: float,
        broker_fee: float,
    ) -> float | None:
        """Net profit margin (%) at `price` after sales tax and broker fee."""
        if not cost_basis or cost_basis <= 0 or not price or price <= 0:
            return None
        net_proceeds = price * (1.0 - sales_tax - broker_fee)
        return float((net_proceeds - cost_basis) / price * 100)

    def _calc_isk_per_day(
        self,
        *,
        price: float,
        cost_basis: float | None,
        sales_tax: float,
        broker_fee: float,
        quantity: int,
        est_days: float | None,
    ) -> float | None:
        """Total profit ISK per day for this order batch at the given price and sell velocity."""
        if not cost_basis or cost_basis <= 0 or not quantity or quantity <= 0:
            return None
        if not est_days or est_days <= 0:
            return None
        profit_per_unit = price * (1.0 - sales_tax - broker_fee) - cost_basis
        return float(profit_per_unit * quantity / est_days)

    def _calc_hold_signal(
        self,
        *,
        market_data: dict[str, Any],
        hub_price: float | None,
        quantity: int,
        isk_per_day_advised: float | None,
    ) -> dict[str, Any] | None:
        """Suggest whether to delay listing when the market trend strongly favours holding."""
        if not market_data.get("has_data") or not hub_price or hub_price <= 0:
            return None

        trend_pct = float(market_data.get("trend_pct") or 0)
        if trend_pct <= 5:
            return {"suggested": False, "reason": "Market not trending up significantly"}

        # Linear extrapolation: spread the observed 7d-vs-42w trend over 7 calendar days
        # trend_pct = ((avg_7d - avg_42w) / avg_42w) * 100 — represents drift over 42 weeks
        daily_rate = trend_pct / (42 * 7)
        price_in_7d = hub_price * (1.0 + daily_rate * 7)
        gain_per_unit = price_in_7d - hub_price
        total_gain = gain_per_unit * max(0, quantity)
        opportunity_cost_7d = float(isk_per_day_advised or 0) * 7

        if total_gain > opportunity_cost_7d and total_gain > 0:
            return {
                "suggested": True,
                "reason": f"Market +{trend_pct:.1f}% trend; est. +{gain_per_unit:,.0f} ISK/unit in 7d outweighs selling now",
                "estimated_price_7d": float(price_in_7d),
                "estimated_gain_total": float(total_gain),
                "opportunity_cost_7d": float(opportunity_cost_7d),
            }

        reason = (
            f"Selling now ({isk_per_day_advised:,.0f} ISK/day) beats waiting for +{trend_pct:.1f}% trend"
            if isk_per_day_advised
            else f"Trend +{trend_pct:.1f}% not significant enough to delay"
        )
        return {
            "suggested": False,
            "reason": reason,
            "estimated_gain_total": float(total_gain),
            "opportunity_cost_7d": float(opportunity_cost_7d),
        }

    def _calc_relist_risk(
        self,
        *,
        est_days: float | None,
        order_duration_days: int,
        broker_fee: float,
        price: float,
        quantity: int,
    ) -> dict[str, Any] | None:
        """Flag if the order is likely to expire before fully selling."""
        if est_days is None:
            return None
        if est_days <= order_duration_days:
            return {"at_risk": False}

        unsold_fraction = max(0.0, 1.0 - (order_duration_days / est_days))
        unsold_quantity = max(0, int(quantity * unsold_fraction))
        relist_cost_estimate = float(price * broker_fee * unsold_quantity)

        return {
            "at_risk": True,
            "estimated_sell_days": float(est_days),
            "order_duration_days": order_duration_days,
            "estimated_unsold_quantity": unsold_quantity,
            "estimated_relist_cost": relist_cost_estimate,
            "reason": f"Est. {est_days:.0f}d to sell exceeds {order_duration_days}d order duration",
        }

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
        if breakdown.get("floored_to_break_even"):
            reasoning += " (Advised price raised to break-even — weighted average did not cover fees.)"
        return reasoning
