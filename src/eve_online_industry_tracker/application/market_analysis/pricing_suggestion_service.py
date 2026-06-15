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
        """Suggest a sell price with detailed breakdown and three-tier price band."""
        my_sales = self._sales_history.suggest_sell_price(character_id=character_id, type_id=type_id)
        market_data = self._market_history.get_price_stats(type_id=type_id, region_id=region_id)
        volume_data = self._market_history.get_volume_stats(type_id=type_id, region_id=region_id)
        cost_basis, acquisition_source, cost_source = self._get_cost_basis(
            character_id=character_id, type_id=type_id, fallback_price=current_price
        )
        hub_price = self._get_hub_price(type_id=type_id, hub=hub)
        hub_buy_price = self._get_hub_buy_price(type_id=type_id, hub=hub)
        sales_tax, broker_fee = self._get_fees(character_id=character_id)
        orderbook_levels = self._get_orderbook_levels(type_id=type_id, region_id=region_id)
        min_target_margin = self._get_min_target_margin()

        breakdown = self._calculate_breakdown(
            my_sales=my_sales,
            market_data=market_data,
            volume_data=volume_data,
            hub_price=hub_price,
            hub_buy_price=hub_buy_price,
            cost_basis=cost_basis,
            current_price=current_price,
            orderbook_levels=orderbook_levels,
        )

        advised_price = breakdown["weighted_price"]
        confidence = self._assess_confidence(my_sales, market_data, volume_data, cost_basis)

        avg_daily_vol = float(volume_data.get("avg_daily_volume", 0)) if volume_data.get("has_data") else 0.0
        break_even = self._calc_break_even(cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)
        min_target_price = self._calc_min_target_price(
            cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee,
            min_target_margin=min_target_margin,
        )

        # Floor 1: break-even — never advise a loss after fees
        if break_even and advised_price < break_even:
            advised_price = break_even
            breakdown["weighted_price"] = advised_price
            breakdown["floored_to_break_even"] = True

        # Floor 2: minimum target margin — respect configured profit target
        if min_target_price and advised_price < min_target_price:
            advised_price = min_target_price
            breakdown["weighted_price"] = advised_price
            breakdown["floored_to_target_margin"] = True
            breakdown["min_target_margin_pct"] = round(min_target_margin * 100, 1)

        net_margin_advised = self._calc_net_margin(price=advised_price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)
        net_margin_current = self._calc_net_margin(price=current_price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)
        est_days_advised = self._estimate_sell_days(target_price=advised_price, quantity=quantity, orderbook_levels=orderbook_levels, avg_daily_volume=avg_daily_vol)
        est_days_current = self._estimate_sell_days(target_price=current_price, quantity=quantity, orderbook_levels=orderbook_levels, avg_daily_volume=avg_daily_vol)
        isk_per_day_advised = self._calc_isk_per_day(price=advised_price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee, quantity=quantity, est_days=est_days_advised)
        isk_per_day_current = self._calc_isk_per_day(price=current_price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee, quantity=quantity, est_days=est_days_current)
        hold_signal = self._calc_hold_signal(market_data=market_data, hub_price=hub_price, quantity=quantity, isk_per_day_advised=isk_per_day_advised)
        relist_risk = self._calc_relist_risk(est_days=est_days_advised, order_duration_days=order_duration_days, broker_fee=broker_fee, price=advised_price, quantity=quantity)
        price_band = self._calc_price_band(
            target_price=advised_price,
            min_target_price=min_target_price,
            break_even_price=break_even,
            hub_price=hub_price,
            orderbook_levels=orderbook_levels,
            current_price=current_price,
            market_data=market_data,
            cost_basis=cost_basis,
            sales_tax=sales_tax,
            broker_fee=broker_fee,
            quantity=quantity,
            avg_daily_vol=avg_daily_vol,
        )

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
            "sales_tax_fraction": sales_tax,
            "broker_fee_fraction": broker_fee,
            "min_target_margin_pct": round(min_target_margin * 100, 1),
            "break_even_price": break_even,
            "net_margin_pct_advised": net_margin_advised,
            "net_margin_pct_current": net_margin_current,
            "estimated_sell_days_advised": est_days_advised,
            "estimated_sell_days_current": est_days_current,
            "isk_per_day_advised": isk_per_day_advised,
            "isk_per_day_current": isk_per_day_current,
            "hold_signal": hold_signal,
            "relist_risk": relist_risk,
            "price_band": price_band,
        }

    # ── Data retrieval ────────────────────────────────────────────────────────

    def _get_hub_price(self, *, type_id: int, hub: str = "jita") -> float | None:
        """Current best Jita sell price from market pricing service."""
        try:
            svc = getattr(self._state, "market_pricing_service", None)
            if svc is None:
                return None
            price_map = svc.get_type_price_map(type_ids=[type_id], hub=hub, side="sell")
            if type_id in price_map:
                return float(price_map[type_id].get("unit_price") or 0) or None
        except Exception as exc:
            logging.debug(f"_get_hub_price error (type_id={type_id}): {exc}")
        return None

    def _get_hub_buy_price(self, *, type_id: int, hub: str = "jita") -> float | None:
        """Best Jita buy order price — used to compute the buy-sell spread."""
        try:
            svc = getattr(self._state, "market_pricing_service", None)
            if svc is None:
                return None
            price_map = svc.get_type_price_map(type_ids=[type_id], hub=hub, side="buy")
            if type_id in price_map:
                return float(price_map[type_id].get("unit_price") or 0) or None
        except Exception as exc:
            logging.debug(f"_get_hub_buy_price error (type_id={type_id}): {exc}")
        return None

    def _get_cost_basis(
        self, *, character_id: int, type_id: int, fallback_price: float | None = None
    ) -> tuple[float | None, str | None, str | None]:
        """Weighted-average acquisition cost for an item.

        Priority:
          1. Current CharacterAssetsModel rows (item in hangar)
          2. Most recent CharacterAssetHistoryModel row (item on market — ESI removes
             active sell orders from the assets snapshot)
          3. Current order price as last resort (unreliable; triggers warning)

        Returns (cost_basis, acquisition_source, cost_source).
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
                sources: set[str] = set()
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
                    source = (
                        "manufactured" if "manufactured" in sources
                        else "bought" if "bought" in sources or "market" in sources
                        else list(sources)[0] if sources else None
                    )
                    return avg_cost, source, "asset"

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
                return float(history_row.acquisition_unit_cost), history_row.acquisition_source or "unknown", "asset_history"

            if fallback_price and fallback_price > 0:
                return fallback_price, "market_order_fallback", "market_order_fallback"
            return None, None, None
        finally:
            try:
                app_session.close()
            except Exception:
                pass

    def _get_fees(self, *, character_id: int) -> tuple[float, float]:
        """(sales_tax_fraction, broker_fee_fraction) — falls back to EVE defaults."""
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
        """Cached Jita 4-4 sell orderbook [[price, volume], ...] sorted ascending."""
        try:
            from eve_online_industry_tracker.infrastructure.persistence.market_orderbook_cache_repo import get_cached_orderbook_levels
            app_session = self._sessions.app_session()
            try:
                result = get_cached_orderbook_levels(
                    app_session,
                    hub="jita", region_id=region_id, station_id=60003760,
                    side="sell", type_id=type_id, at_hub=True, ttl_seconds=3600,
                )
                if result is not None:
                    levels, _ = result
                    return sorted(levels, key=lambda lv: lv[0])
            finally:
                app_session.close()
        except Exception as exc:
            logging.debug(f"_get_orderbook_levels error (type_id={type_id}): {exc}")
        return []

    def _get_min_target_margin(self) -> float:
        """Minimum acceptable net profit margin (fraction). Configurable via cfg_manager."""
        try:
            cfg_manager = getattr(self._state, "cfg_manager", None)
            if cfg_manager:
                cfg = cfg_manager.all() or {}
                advisory_cfg = ((cfg.get("defaults") or {}).get("market_advisory") or {})
                if "min_target_margin_pct" in advisory_cfg:
                    return max(0.0, min(0.50, float(advisory_cfg["min_target_margin_pct"]) / 100))
        except Exception:
            pass
        return 0.08  # 8 % default

    # ── Core price calculation ────────────────────────────────────────────────

    def _calculate_breakdown(
        self,
        *,
        my_sales: dict[str, Any],
        market_data: dict[str, Any],
        volume_data: dict[str, Any],
        hub_price: float | None,
        hub_buy_price: float | None,
        cost_basis: float | None,
        current_price: float,
        orderbook_levels: list[list[float]],
    ) -> dict[str, Any]:
        """Build weighted-price components.

        Improvements vs original:
          B — Hub price weight dampened by volatility (high vol = less hub anchoring)
          A — Days-of-supply replaces the old three-bucket liquidity adjustment
          C — Buy-sell spread added as a demand-pressure signal
        """
        components: dict[str, Any] = {}
        volatility_pct = float(market_data.get("volatility_pct") or 0) if market_data.get("has_data") else 0.0

        # Component 0: Your cost basis (15% weight) — soft floor
        if cost_basis and cost_basis > 0:
            components["your_cost_basis"] = {
                "value": float(cost_basis * 1.05),
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

        # Component 2: Hub price — B: weight dampened by volatility
        # High volatility → hub snapshot is less reliable → redistribute to other signals
        if hub_price and hub_price > 0:
            hub_weight = max(0.15, 0.35 / (1.0 + volatility_pct / 100.0))
            components["market_hub_price"] = {
                "value": float(hub_price),
                "weight": hub_weight,
                "volatility_pct": volatility_pct,
                "volatility_dampened": volatility_pct > 10,
            }

        # Component 3: Market trend (15% weight)
        if market_data.get("has_data"):
            avg_42w = market_data.get("avg_42w") or 0
            avg_7d = market_data.get("avg_7d") or 0
            trend_pct = market_data.get("trend_pct") or 0
            adjustment_factor = 1.0 + (trend_pct / 100.0 * 0.5)
            trend_price = avg_42w * adjustment_factor if avg_42w > 0 else None
            if trend_price:
                components["market_trend"] = {
                    "value": float(trend_price),
                    "weight": 0.15,
                    "trend_pct": trend_pct,
                    "avg_42w": float(avg_42w),
                    "avg_7d": float(avg_7d),
                }

        # Component 4: A — Days-of-supply (replaces three-bucket liquidity adjustment)
        supply_signal = self._calc_supply_demand_signal(
            orderbook_levels=orderbook_levels,
            volume_data=volume_data,
            hub_price=hub_price,
            current_price=current_price,
        )
        if supply_signal:
            components["supply_demand"] = supply_signal

        # Component 5: C — Buy-sell spread demand pressure (if buy price available)
        spread_signal = self._calc_buy_sell_spread_signal(
            hub_sell_price=hub_price,
            hub_buy_price=hub_buy_price,
        )
        if spread_signal:
            components["buy_sell_spread"] = spread_signal

        # Weighted price — renormalise across present components
        total_weight = sum(c.get("weight", 0) for c in components.values())
        if total_weight > 0:
            weighted_price = sum(
                c["value"] * c["weight"] for c in components.values() if c.get("value", 0) > 0
            ) / total_weight
        else:
            weighted_price = current_price

        # Soft floor: cost + 5% (fee-aware hard floors applied in suggest_price)
        if cost_basis and cost_basis > 0:
            weighted_price = max(weighted_price, cost_basis * 1.05)

        return {
            "components": components,
            "weighted_price": float(weighted_price),
            "total_weight": total_weight,
        }

    def _calc_supply_demand_signal(
        self,
        *,
        orderbook_levels: list[list[float]],
        volume_data: dict[str, Any],
        hub_price: float | None,
        current_price: float,
    ) -> dict[str, Any] | None:
        """A — Days-of-supply signal replacing the old liquidity bucket.

        days_of_supply = total sell volume in orderbook / avg daily volume
          > 30 days → oversupplied  → price conservatively
          7–30 days → balanced      → price at reference
          < 7 days  → undersupplied → can hold above reference
        """
        if not volume_data.get("has_data"):
            return None
        avg_daily_volume = float(volume_data.get("avg_daily_volume") or 0)
        if avg_daily_volume <= 0 or not orderbook_levels:
            return None

        total_sell_volume = sum(int(vol) for _, vol in orderbook_levels)
        if total_sell_volume <= 0:
            return None

        days_of_supply = total_sell_volume / avg_daily_volume
        reference = hub_price or current_price

        if days_of_supply > 30:
            signal_price = reference * 0.97
            label = f"oversupplied ({days_of_supply:.0f}d supply) — price conservatively"
        elif days_of_supply < 7:
            signal_price = reference * 1.03
            label = f"undersupplied ({days_of_supply:.1f}d supply) — can hold premium"
        else:
            signal_price = reference
            label = f"balanced ({days_of_supply:.0f}d supply)"

        return {
            "value": float(signal_price),
            "weight": 0.15,
            "days_of_supply": round(days_of_supply, 1),
            "total_sell_volume": int(total_sell_volume),
            "avg_daily_volume": round(avg_daily_volume, 0),
            "label": label,
        }

    def _calc_buy_sell_spread_signal(
        self,
        *,
        hub_sell_price: float | None,
        hub_buy_price: float | None,
    ) -> dict[str, Any] | None:
        """C — Buy-sell spread as a demand pressure signal.

        Tight spread (<5%)  → buyers are close to the ask → demand is strong
        Normal spread (5-25%) → neutral
        Wide spread (>25%)  → thin demand → price more conservatively
        """
        if not hub_sell_price or hub_sell_price <= 0 or not hub_buy_price or hub_buy_price <= 0:
            return None
        if hub_buy_price >= hub_sell_price:
            return None

        spread_pct = (hub_sell_price - hub_buy_price) / hub_sell_price * 100

        if spread_pct < 5:
            # Tight: buyers close to ask — slight premium possible
            signal_price = hub_sell_price * 1.01
            label = f"tight spread ({spread_pct:.1f}%) — strong demand"
        elif spread_pct > 25:
            # Wide: thin demand — approach midpoint pricing
            midpoint = (hub_sell_price + hub_buy_price) / 2
            signal_price = midpoint
            label = f"wide spread ({spread_pct:.1f}%) — thin demand, price conservatively"
        else:
            signal_price = hub_sell_price
            label = f"normal spread ({spread_pct:.1f}%)"

        return {
            "value": float(signal_price),
            "weight": 0.10,
            "spread_pct": round(spread_pct, 1),
            "hub_sell_price": float(hub_sell_price),
            "hub_buy_price": float(hub_buy_price),
            "label": label,
        }

    # ── Profitability helpers ─────────────────────────────────────────────────

    def _calc_break_even(
        self, *, cost_basis: float | None, sales_tax: float, broker_fee: float
    ) -> float | None:
        if not cost_basis or cost_basis <= 0:
            return None
        net_rate = 1.0 - sales_tax - broker_fee
        return float(cost_basis / net_rate) if net_rate > 0 else None

    def _calc_min_target_price(
        self,
        *,
        cost_basis: float | None,
        sales_tax: float,
        broker_fee: float,
        min_target_margin: float,
    ) -> float | None:
        """Minimum list price to achieve the configured profit margin after fees."""
        if not cost_basis or cost_basis <= 0 or min_target_margin <= 0:
            return None
        net_rate = 1.0 - min_target_margin - sales_tax - broker_fee
        return float(cost_basis / net_rate) if net_rate > 0 else None

    def _calc_net_margin(
        self, *, price: float, cost_basis: float | None, sales_tax: float, broker_fee: float
    ) -> float | None:
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
        if not cost_basis or cost_basis <= 0 or not quantity or quantity <= 0:
            return None
        if not est_days or est_days <= 0:
            return None
        profit_per_unit = price * (1.0 - sales_tax - broker_fee) - cost_basis
        return float(profit_per_unit * quantity / est_days)

    # ── Velocity ──────────────────────────────────────────────────────────────

    def _estimate_sell_days(
        self,
        *,
        target_price: float,
        quantity: int,
        orderbook_levels: list[list[float]],
        avg_daily_volume: float,
    ) -> float | None:
        if not quantity or quantity <= 0 or not avg_daily_volume or avg_daily_volume <= 0:
            return None
        volume_ahead = sum(int(vol) for price, vol in orderbook_levels if price < target_price)
        volume_at_level = sum(
            int(vol) for price, vol in orderbook_levels
            if target_price <= price <= target_price * 1.001
        )
        days_until_front = volume_ahead / avg_daily_volume
        total_at_level = quantity + volume_at_level
        capture_rate = min(0.50, max(0.05, quantity / total_at_level))
        days_for_our_units = quantity / (avg_daily_volume * capture_rate)
        return float(days_until_front + days_for_our_units)

    # ── Price band ────────────────────────────────────────────────────────────

    def _calc_price_band(
        self,
        *,
        target_price: float,
        min_target_price: float | None,
        break_even_price: float | None,
        hub_price: float | None,
        orderbook_levels: list[list[float]],
        current_price: float,
        market_data: dict[str, Any],
        cost_basis: float | None,
        sales_tax: float,
        broker_fee: float,
        quantity: int,
        avg_daily_vol: float,
    ) -> dict[str, Any]:
        """F — Three-tier price band: aggressive / target / premium.

        Aggressive — fastest sell, at or above minimum target margin floor.
          • If our current price is already at or below the cheapest queue entry
            (we're at the front), aggressive = target (no need to undercut further).
          • Otherwise, undercut the cheapest competing sell by 0.01 ISK, but never
            below the minimum target margin price.

        Target — optimal ISK/day; the current advised price after all floors.

        Premium — best margin, slower sell.
          • Projects the market trend forward 14 days.
          • Capped at hub_price × 1.25 to avoid absurd suggestions.
          • Only offered when trend_pct > 2 %; otherwise equals target.
        """
        floor = min_target_price or break_even_price or target_price

        # Aggressive
        if orderbook_levels:
            cheapest_competing = orderbook_levels[0][0]
            if current_price <= cheapest_competing:
                # We're already at the front — don't go lower
                price_aggressive = max(floor, current_price)
            else:
                price_aggressive = max(floor, cheapest_competing - 0.01)
        else:
            price_aggressive = floor

        # Aggressive is always ≤ target (it's the fast-sell option, not a premium)
        price_aggressive = min(price_aggressive, target_price)
        price_aggressive = max(price_aggressive, floor)

        # Target (= advised price)
        price_target = target_price

        # Premium
        trend_pct = float(market_data.get("trend_pct") or 0) if market_data.get("has_data") else 0.0
        if hub_price and hub_price > 0 and trend_pct > 2:
            daily_rate = trend_pct / (42 * 7)
            price_premium = hub_price * (1.0 + daily_rate * 14)
            price_premium = min(price_premium, hub_price * 1.25)
            price_premium = max(price_premium, price_target)
            premium_label = f"Hold for trend (+{trend_pct:.1f}% 42w, 14d projection)"
        else:
            price_premium = price_target
            premium_label = "Market not trending up — same as target"

        # Compute sell days and ISK/day for aggressive and premium for UI context
        est_days_aggressive = self._estimate_sell_days(
            target_price=price_aggressive, quantity=quantity,
            orderbook_levels=orderbook_levels, avg_daily_volume=avg_daily_vol,
        )
        est_days_premium = self._estimate_sell_days(
            target_price=price_premium, quantity=quantity,
            orderbook_levels=orderbook_levels, avg_daily_volume=avg_daily_vol,
        )
        isk_day_aggressive = self._calc_isk_per_day(
            price=price_aggressive, cost_basis=cost_basis, sales_tax=sales_tax,
            broker_fee=broker_fee, quantity=quantity, est_days=est_days_aggressive,
        )
        isk_day_premium = self._calc_isk_per_day(
            price=price_premium, cost_basis=cost_basis, sales_tax=sales_tax,
            broker_fee=broker_fee, quantity=quantity, est_days=est_days_premium,
        )
        margin_aggressive = self._calc_net_margin(price=price_aggressive, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)
        margin_premium = self._calc_net_margin(price=price_premium, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)

        return {
            "aggressive": {
                "price": float(price_aggressive),
                "label": "Aggressive — fastest sell",
                "estimated_sell_days": est_days_aggressive,
                "isk_per_day": isk_day_aggressive,
                "net_margin_pct": margin_aggressive,
            },
            "target": {
                "price": float(price_target),
                "label": "Target — optimal ISK/day",
                "estimated_sell_days": None,  # shown in main metrics already
                "isk_per_day": None,
                "net_margin_pct": None,
            },
            "premium": {
                "price": float(price_premium),
                "label": premium_label,
                "estimated_sell_days": est_days_premium,
                "isk_per_day": isk_day_premium,
                "net_margin_pct": margin_premium,
            },
        }

    # ── Signals ───────────────────────────────────────────────────────────────

    def _calc_hold_signal(
        self,
        *,
        market_data: dict[str, Any],
        hub_price: float | None,
        quantity: int,
        isk_per_day_advised: float | None,
    ) -> dict[str, Any] | None:
        if not market_data.get("has_data") or not hub_price or hub_price <= 0:
            return None
        trend_pct = float(market_data.get("trend_pct") or 0)
        if trend_pct <= 5:
            return {"suggested": False, "reason": "Market not trending up significantly"}

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
        if est_days is None:
            return None
        if est_days <= order_duration_days:
            return {"at_risk": False}
        unsold_fraction = max(0.0, 1.0 - (order_duration_days / est_days))
        unsold_quantity = max(0, int(quantity * unsold_fraction))
        return {
            "at_risk": True,
            "estimated_sell_days": float(est_days),
            "order_duration_days": order_duration_days,
            "estimated_unsold_quantity": unsold_quantity,
            "estimated_relist_cost": float(price * broker_fee * unsold_quantity),
            "reason": f"Est. {est_days:.0f}d to sell exceeds {order_duration_days}d order duration",
        }

    # ── Confidence & reasoning ────────────────────────────────────────────────

    def _assess_confidence(
        self,
        my_sales: dict[str, Any],
        market_data: dict[str, Any],
        volume_data: dict[str, Any],
        cost_basis: float | None = None,
    ) -> str:
        score = 0
        if cost_basis and cost_basis > 0:
            score += 2
        sales_confidence = my_sales.get("confidence", "none")
        score += {"high": 3, "medium": 2, "low": 1}.get(sales_confidence, 0)
        if market_data.get("has_data"):
            records = market_data.get("record_count", 0)
            score += 3 if records > 100 else 2 if records > 30 else 1 if records > 7 else 0
        if volume_data.get("has_data") and volume_data.get("avg_daily_volume", 0) > 100:
            score += 2
        return "high" if score >= 8 else "medium" if score >= 4 else "low"

    def _build_reasoning(self, breakdown: dict[str, Any], confidence: str) -> str:
        components = breakdown.get("components", {})
        parts = []
        if "your_cost_basis" in components:
            parts.append("Your cost basis (floor)")
        if "your_recent_sales" in components:
            n = components["your_recent_sales"].get("sample_size", 0)
            parts.append(f"Your recent sales ({n} samples)")
        if "market_hub_price" in components:
            comp = components["market_hub_price"]
            dampened = comp.get("volatility_dampened", False)
            parts.append(f"Hub price{'  (volatility-dampened)' if dampened else ''}")
        if "market_trend" in components:
            t = components["market_trend"].get("trend_pct", 0)
            parts.append(f"Market {'rising' if t > 0 else 'falling'} ({t:+.1f}%)")
        if "supply_demand" in components:
            parts.append(components["supply_demand"].get("label", "supply/demand signal"))
        if "buy_sell_spread" in components:
            parts.append(components["buy_sell_spread"].get("label", "spread signal"))

        reasoning = f"Based on: {', '.join(parts)}. Confidence: {confidence}."
        if breakdown.get("floored_to_break_even"):
            reasoning += " (Price raised to break-even — weighted average did not cover fees.)"
        if breakdown.get("floored_to_target_margin"):
            m = breakdown.get("min_target_margin_pct", 8)
            reasoning += f" (Price raised to meet {m}% target margin.)"
        return reasoning
