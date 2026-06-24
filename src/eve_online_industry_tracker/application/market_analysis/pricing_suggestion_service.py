from __future__ import annotations

import json
import logging
import math
import statistics
from datetime import datetime
from typing import Any

from sqlalchemy import and_
from eve_online_industry_tracker.application.industry.sales_history_service import SalesHistoryService
from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService
from eve_online_industry_tracker.application.market_pricing import MarketPricingService
from eve_online_industry_tracker.infrastructure.models import CharacterAssetsModel, CharacterAssetHistoryModel, CharacterModel
from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider, StateSessionProvider


class PricingSuggestionService:
    """Suggest optimal sell prices based on market data and sales history."""

    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)
        self._sales_history = SalesHistoryService(state=state, sessions=sessions)
        self._market_history = MarketHistoryService(state=state, sessions=sessions)
        self._market_pricing = MarketPricingService(state=state, sessions=self._sessions)

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
        days_remaining: int | None = None,
    ) -> dict[str, Any]:
        """Suggest a sell price with detailed breakdown, three-tier price band, and urgency signals."""
        my_sales = self._sales_history.suggest_sell_price(character_id=character_id, type_id=type_id)
        market_data = self._market_history.get_price_stats(type_id=type_id, region_id=region_id)
        volume_data = self._market_history.get_volume_stats(type_id=type_id, region_id=region_id)
        cost_basis, acquisition_source, cost_source = self._get_cost_basis(
            character_id=character_id, type_id=type_id, fallback_price=current_price
        )
        hub_price = self._get_hub_price(type_id=type_id, hub=hub)
        hub_buy_price = self._get_hub_buy_price(type_id=type_id, hub=hub)
        sales_tax, broker_fee = self._get_fees(character_id=character_id)
        raw_orderbook = self._get_orderbook_levels(type_id=type_id, region_id=region_id)
        orderbook_levels = self._filter_orderbook_outliers(raw_orderbook)
        min_target_margin = self._get_min_target_margin()

        fill_rate = self._get_fill_rate_velocity(
            character_id=character_id, type_id=type_id,
            current_price=current_price, lookback_days=90,
        )
        seller_concentration = self._get_seller_concentration(orderbook_levels=orderbook_levels)

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

        # Floor 1: break-even
        if break_even and advised_price < break_even:
            advised_price = break_even
            breakdown["weighted_price"] = advised_price
            breakdown["floored_to_break_even"] = True

        # Floor 2: minimum target margin
        if min_target_price and advised_price < min_target_price:
            advised_price = min_target_price
            breakdown["weighted_price"] = advised_price
            breakdown["floored_to_target_margin"] = True
            breakdown["min_target_margin_pct"] = round(min_target_margin * 100, 1)

        # Snap to nearest valid EVE price tick (4 significant figures)
        advised_price = self._snap_to_eve_tick(advised_price)
        breakdown["weighted_price"] = advised_price

        # Gap positioning: if being undercut, prefer the highest gap price over the
        # weighted average — no reason to go lower than the nearest undercutter - 1 tick.
        _gap_floor = min_target_price or break_even or advised_price
        gap_price = self._find_gap_position(
            current_price=current_price,
            orderbook_levels=orderbook_levels,
            floor_price=_gap_floor,
        )
        if gap_price is not None and gap_price > advised_price:
            advised_price = gap_price
            breakdown["weighted_price"] = advised_price
            breakdown["gap_positioned"] = True
            breakdown["gap_price"] = gap_price

        # Front-of-queue guard: if already the cheapest seller, never advise going lower.
        # Reducing price when front of queue only hurts margin — no one is buying from a
        # cheaper competitor because there isn't one.
        if orderbook_levels and current_price > 0:
            front_price = orderbook_levels[0][0]
            if current_price <= front_price and advised_price < current_price:
                advised_price = self._snap_to_eve_tick(current_price)
                breakdown["weighted_price"] = advised_price
                breakdown["already_front_of_queue"] = True

        floor = min_target_price or break_even or advised_price

        net_margin_advised = self._calc_net_margin(price=advised_price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)
        net_margin_current = self._calc_net_margin(price=current_price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)
        est_days_advised = self._estimate_sell_days(target_price=advised_price, quantity=quantity, orderbook_levels=orderbook_levels, avg_daily_volume=avg_daily_vol, fill_rate=fill_rate)
        est_days_current = self._estimate_sell_days(target_price=current_price, quantity=quantity, orderbook_levels=orderbook_levels, avg_daily_volume=avg_daily_vol, fill_rate=fill_rate)
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
            fill_rate=fill_rate,
        )

        # Expiry urgency: if the order will likely expire before selling, blend toward aggressive
        expiry_urgency = False
        expiry_urgency_detail: dict[str, Any] = {}
        if days_remaining is not None and days_remaining >= 0 and est_days_advised is not None and est_days_advised > 0:
            if days_remaining < est_days_advised * 0.5:
                expiry_urgency = True
                # urgency_ratio 0.0 = just entered danger zone, 1.0 = expiry tomorrow
                urgency_ratio = max(0.0, 1.0 - (days_remaining / max(1.0, est_days_advised * 0.5)))
                blend = min(0.5, urgency_ratio)  # cap at 50% blend toward aggressive
                aggressive_price = (price_band or {}).get("aggressive", {}).get("price") or advised_price
                urgency_price = self._snap_to_eve_tick(
                    max(floor, advised_price * (1.0 - blend) + aggressive_price * blend)
                )
                expiry_urgency_detail = {
                    "days_remaining": int(days_remaining),
                    "est_days_advised": round(est_days_advised, 1),
                    "urgency_blend": round(blend, 3),
                    "urgency_ratio": round(urgency_ratio, 3),
                    "original_advised": float(advised_price),
                    "urgency_adjusted_price": float(urgency_price),
                    "reason": (
                        f"Order expires in {days_remaining}d but est. {est_days_advised:.0f}d to sell — "
                        f"advised price moved {blend*100:.0f}% toward aggressive tier."
                    ),
                }
                advised_price = urgency_price
                breakdown["weighted_price"] = advised_price
                breakdown["urgency_adjusted"] = True
                breakdown["urgency_blend"] = round(blend, 3)

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
            "fill_rate_velocity": fill_rate,
            "seller_concentration": seller_concentration,
            "expiry_urgency": expiry_urgency,
            "expiry_urgency_detail": expiry_urgency_detail if expiry_urgency else None,
            "outlier_levels_removed": len(raw_orderbook) - len(orderbook_levels),
        }

    # ── Data retrieval ────────────────────────────────────────────────────────

    def _get_hub_price(self, *, type_id: int, hub: str = "jita") -> float | None:
        try:
            price_map = self._market_pricing.get_type_price_map(type_ids=[type_id], hub=hub, side="sell")
            if type_id in price_map:
                return float(price_map[type_id].get("unit_price") or 0) or None
        except Exception as exc:
            logging.debug(f"_get_hub_price error (type_id={type_id}): {exc}")
        return None

    def _get_hub_buy_price(self, *, type_id: int, hub: str = "jita") -> float | None:
        """Best Jita buy order price — used to compute the buy-sell spread."""
        try:
            price_map = self._market_pricing.get_type_price_map(type_ids=[type_id], hub=hub, side="buy")
            if type_id in price_map:
                return float(price_map[type_id].get("unit_price") or 0) or None
        except Exception as exc:
            logging.debug(f"_get_hub_buy_price error (type_id={type_id}): {exc}")
        return None

    def _get_cost_basis(
        self, *, character_id: int, type_id: int, fallback_price: float | None = None
    ) -> tuple[float | None, str | None, str | None]:
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
        try:
            cfg_manager = getattr(self._state, "cfg_manager", None)
            if cfg_manager:
                cfg = cfg_manager.all() or {}
                advisory_cfg = ((cfg.get("defaults") or {}).get("market_advisory") or {})
                if "min_target_margin_pct" in advisory_cfg:
                    return max(0.0, min(0.50, float(advisory_cfg["min_target_margin_pct"]) / 100))
        except Exception:
            pass
        return 0.08

    # ── Orderbook preprocessing ───────────────────────────────────────────────

    def _filter_orderbook_outliers(self, levels: list[list[float]]) -> list[list[float]]:
        """Remove manipulation walls using IQR fencing on the price axis.

        A single large-volume order at an extreme price can inflate days_of_supply
        and make the market appear oversupplied when it isn't. Only upper outliers
        are removed — lower prices are legitimate undercuts.
        """
        if len(levels) < 4:
            return levels
        prices = [float(lv[0]) for lv in levels]
        qs = statistics.quantiles(prices, n=4)  # [Q1, median, Q3]
        q1, q3 = qs[0], qs[2]
        iqr = q3 - q1
        if iqr <= 0:
            return levels
        upper_fence = q3 + 2.0 * iqr
        filtered = [lv for lv in levels if float(lv[0]) <= upper_fence]
        return filtered if len(filtered) >= 2 else levels[:2]

    # ── EVE price tick helpers ────────────────────────────────────────────────

    @staticmethod
    def _eve_price_tick(price: float) -> float:
        """Minimum ISK increment for a price in EVE Online.

        EVE allows only 4 significant figures when modifying an order price.
        Digits below the 4th most-significant figure are zeroed out, so the
        effective tick is 10^(floor(log10(price)) - 3), capped at 0.01 ISK.
        Examples:
          1,500,000 ISK → tick 1,000 ISK
            150,000 ISK → tick   100 ISK
             15,000 ISK → tick    10 ISK
              1,500 ISK → tick     1 ISK
                150 ISK → tick   0.1 ISK
                 15 ISK → tick  0.01 ISK
        """
        if price <= 0:
            return 0.01
        magnitude = math.floor(math.log10(price))
        return max(0.01, 10.0 ** (magnitude - 3))

    @staticmethod
    def _snap_to_eve_tick(price: float) -> float:
        """Floor price DOWN to the nearest valid EVE market tick.

        Snapping down (not rounding) ensures the output never exceeds the
        intended price — important when the caller uses the result as an
        undercut target or a margin floor.
        """
        if price <= 0:
            return round(price, 2)
        tick = PricingSuggestionService._eve_price_tick(price)
        return round(math.floor(price / tick) * tick, 2)

    def _find_gap_position(
        self,
        *,
        current_price: float,
        orderbook_levels: list[list[float]],
        floor_price: float,
    ) -> float | None:
        """Find the highest valid price that still beats the nearest undercutting order.

        When being undercut, the optimal strategy is not to chase the absolute
        cheapest price but to position 1 EVE tick below the nearest competitor
        that is cheaper than the current price — capturing the gap between
        adjacent price clusters.

        Example (Bustard):
          Orderbook: ...170,000,000 | 171,000,000 | 171,100,000 (your order)
          Nearest undercutter: 171,000,000
          Tick at 171M: 100,000
          Gap position: 170,900,000  (beats the 171M sellers by 1 tick, no need to go lower)
        """
        if not orderbook_levels or current_price <= 0:
            return None
        prices_below = [float(lv[0]) for lv in orderbook_levels if float(lv[0]) < current_price]
        if not prices_below:
            return None  # already front of queue — handled by front-of-queue guard
        nearest_undercut = max(prices_below)
        tick = self._eve_price_tick(nearest_undercut)
        gap_top = self._snap_to_eve_tick(nearest_undercut) - tick
        return gap_top if gap_top >= floor_price else None

    # ── Market microstructure signals ─────────────────────────────────────────

    def _get_seller_concentration(self, *, orderbook_levels: list[list[float]]) -> dict[str, Any]:
        """Detect sniper risk by measuring how concentrated the front of the queue is.

        In EVE a single seller can instantly reset the queue by repricing. We use
        front-level volume fraction as a proxy: if one or two levels own >60-80%
        of visible supply, the advised aggressive undercut may not last.
        """
        if not orderbook_levels:
            return {"sniper_risk": False, "risk_label": "No orderbook data", "note": ""}

        total_vol = sum(int(lv[1]) for lv in orderbook_levels)
        if total_vol == 0:
            return {"sniper_risk": False, "risk_label": "Empty orderbook", "note": ""}

        front_1_vol = int(orderbook_levels[0][1])
        front_2_vol = sum(int(lv[1]) for lv in orderbook_levels[:2])
        front_1_fraction = front_1_vol / total_vol
        front_2_fraction = front_2_vol / total_vol
        num_levels = len(orderbook_levels)

        sniper_risk = (front_1_fraction > 0.60 and num_levels >= 2) or \
                      (front_2_fraction > 0.80 and num_levels >= 3)

        if sniper_risk:
            risk_label = "Sniper risk"
            note = "Dominant seller controls front of queue — undercutting by 0.01 ISK may not stick"
        elif front_2_fraction > 0.60:
            risk_label = "Moderate concentration"
            note = "A few large sell walls dominate; check how often they reprice"
        else:
            risk_label = "Distributed"
            note = "Multiple sellers at varied levels — normal market"

        return {
            "sniper_risk": sniper_risk,
            "front_1_volume_pct": round(front_1_fraction * 100, 1),
            "front_2_volume_pct": round(front_2_fraction * 100, 1),
            "num_price_levels": num_levels,
            "total_visible_volume": int(total_vol),
            "risk_label": risk_label,
            "note": note,
        }

    # ── Wallet fill-rate velocity ─────────────────────────────────────────────

    def _get_fill_rate_velocity(
        self,
        *,
        character_id: int,
        type_id: int,
        current_price: float,
        lookback_days: int = 90,
    ) -> dict[str, Any] | None:
        """Estimate actual daily sell velocity from wallet transaction history.

        Uses your own completed sales rather than the market-wide orderbook
        heuristic, reflecting your real historical throughput at this station.
        A simple price-elasticity factor adjusts for the difference between your
        median historical sell price and the current target price.
        """
        history = self._sales_history.get_sold_history(
            character_id=character_id, type_id=type_id, days=lookback_days
        )
        if len(history) < 2:
            return None

        total_qty = sum(tx["quantity"] for tx in history)
        if total_qty <= 0:
            return None

        # Infer actual date span (not just the lookback window)
        dates = [tx["date"] for tx in history if tx["date"]]
        try:
            oldest = datetime.fromisoformat(str(dates[-1]).replace("Z", "+00:00"))
            newest = datetime.fromisoformat(str(dates[0]).replace("Z", "+00:00"))
            day_span = max(1.0, float((newest - oldest).days + 1))
        except Exception:
            day_span = float(lookback_days)

        daily_velocity_raw = total_qty / day_span

        # Price-elasticity: each 1% above median historical sell price
        # reduces expected velocity by ~0.5% (conservative linear approximation)
        prices = [tx["unit_price"] for tx in history if tx["unit_price"] > 0]
        median_price = float(statistics.median(prices)) if prices else current_price
        if median_price > 0 and current_price > 0:
            premium_pct = (current_price - median_price) / median_price * 100
            elasticity_factor = max(0.20, 1.0 - premium_pct * 0.005)
        else:
            elasticity_factor = 1.0

        adjusted_velocity = max(0.001, daily_velocity_raw * elasticity_factor)
        transaction_count = len(history)
        confidence = "high" if transaction_count >= 10 else "medium" if transaction_count >= 4 else "low"

        return {
            "daily_velocity_raw": round(daily_velocity_raw, 3),
            "daily_velocity_adjusted": round(adjusted_velocity, 4),
            "median_historical_price": float(median_price),
            "transaction_count": transaction_count,
            "day_span": int(day_span),
            "confidence": confidence,
            "elasticity_factor": round(elasticity_factor, 3),
        }

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
        components: dict[str, Any] = {}
        volatility_pct = float(market_data.get("volatility_pct") or 0) if market_data.get("has_data") else 0.0

        if cost_basis and cost_basis > 0:
            components["your_cost_basis"] = {
                "value": float(cost_basis * 1.05),
                "weight": 0.15,
                "raw_cost": float(cost_basis),
                "margin_pct": 5,
            }

        my_sales_price = my_sales.get("suggested_price")
        if my_sales_price and my_sales_price > 0:
            components["your_recent_sales"] = {
                "value": float(my_sales_price),
                "weight": 0.20,
                "sample_size": my_sales.get("sample_size", 0),
                "confidence": my_sales.get("confidence", "none"),
            }

        if hub_price and hub_price > 0:
            hub_weight = max(0.15, 0.35 / (1.0 + volatility_pct / 100.0))
            components["market_hub_price"] = {
                "value": float(hub_price),
                "weight": hub_weight,
                "volatility_pct": volatility_pct,
                "volatility_dampened": volatility_pct > 10,
            }

        if market_data.get("has_data"):
            avg_42w = market_data.get("avg_42w") or 0
            trend_pct = market_data.get("trend_pct") or 0
            adjustment_factor = 1.0 + (trend_pct / 100.0 * 0.5)
            trend_price = avg_42w * adjustment_factor if avg_42w > 0 else None
            if trend_price:
                components["market_trend"] = {
                    "value": float(trend_price),
                    "weight": 0.15,
                    "trend_pct": trend_pct,
                    "avg_42w": float(avg_42w),
                    "avg_7d": float(market_data.get("avg_7d") or 0),
                }

        supply_signal = self._calc_supply_demand_signal(
            orderbook_levels=orderbook_levels,
            volume_data=volume_data,
            hub_price=hub_price,
            current_price=current_price,
        )
        if supply_signal:
            components["supply_demand"] = supply_signal

        spread_signal = self._calc_buy_sell_spread_signal(
            hub_sell_price=hub_price,
            hub_buy_price=hub_buy_price,
        )
        if spread_signal:
            components["buy_sell_spread"] = spread_signal

        total_weight = sum(c.get("weight", 0) for c in components.values())
        if total_weight > 0:
            weighted_price = sum(
                c["value"] * c["weight"] for c in components.values() if c.get("value", 0) > 0
            ) / total_weight
        else:
            weighted_price = current_price

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
        if not hub_sell_price or hub_sell_price <= 0 or not hub_buy_price or hub_buy_price <= 0:
            return None
        if hub_buy_price >= hub_sell_price:
            return None

        spread_pct = (hub_sell_price - hub_buy_price) / hub_sell_price * 100

        if spread_pct < 5:
            signal_price = hub_sell_price * 1.01
            label = f"tight spread ({spread_pct:.1f}%) — strong demand"
        elif spread_pct > 25:
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
        fill_rate: dict[str, Any] | None = None,
    ) -> float | None:
        """Blend orderbook queue heuristic with wallet fill-rate history.

        Fill-rate weight by confidence:
          high (≥10 txns)   → 65% fill-rate, 35% heuristic
          medium (4-9 txns) → 45% fill-rate, 55% heuristic
          low (2-3 txns)    → 25% fill-rate, 75% heuristic
        """
        heuristic_days: float | None = None
        if quantity > 0 and avg_daily_volume > 0:
            volume_ahead = sum(int(vol) for price, vol in orderbook_levels if price < target_price)
            volume_at_level = sum(
                int(vol) for price, vol in orderbook_levels
                if target_price <= price <= target_price * 1.001
            )
            days_until_front = volume_ahead / avg_daily_volume
            total_at_level = quantity + volume_at_level
            capture_rate = min(0.50, max(0.05, quantity / total_at_level)) if total_at_level > 0 else 0.35
            days_for_our_units = quantity / (avg_daily_volume * capture_rate)
            heuristic_days = float(days_until_front + days_for_our_units)

        if fill_rate and fill_rate.get("daily_velocity_adjusted", 0) > 0 and quantity > 0:
            fill_days = float(quantity / fill_rate["daily_velocity_adjusted"])
            confidence = fill_rate.get("confidence", "low")
            fill_weight = {"high": 0.65, "medium": 0.45, "low": 0.25}.get(confidence, 0.25)
            if heuristic_days is not None:
                return float(heuristic_days * (1.0 - fill_weight) + fill_days * fill_weight)
            return fill_days

        return heuristic_days

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
        fill_rate: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        floor = min_target_price or break_even_price or target_price

        if orderbook_levels:
            cheapest_competing = orderbook_levels[0][0]
            # Undercut by exactly 1 EVE tick (the minimum price change EVE allows)
            eve_tick = self._eve_price_tick(cheapest_competing)
            undercut_price = self._snap_to_eve_tick(cheapest_competing) - eve_tick
            if current_price <= cheapest_competing:
                # Already front of queue — hold current price (snapped to valid tick)
                price_aggressive = max(floor, self._snap_to_eve_tick(current_price))
            else:
                price_aggressive = max(floor, undercut_price)
        else:
            price_aggressive = floor

        price_aggressive = min(price_aggressive, target_price)
        price_aggressive = max(price_aggressive, floor)
        price_aggressive = self._snap_to_eve_tick(price_aggressive)

        price_target = self._snap_to_eve_tick(target_price)

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
        price_premium = self._snap_to_eve_tick(price_premium)

        def _tier_metrics(price: float) -> dict[str, Any]:
            est_d = self._estimate_sell_days(
                target_price=price, quantity=quantity,
                orderbook_levels=orderbook_levels, avg_daily_volume=avg_daily_vol,
                fill_rate=fill_rate,
            )
            isk_d = self._calc_isk_per_day(
                price=price, cost_basis=cost_basis, sales_tax=sales_tax,
                broker_fee=broker_fee, quantity=quantity, est_days=est_d,
            )
            margin = self._calc_net_margin(price=price, cost_basis=cost_basis, sales_tax=sales_tax, broker_fee=broker_fee)
            return {"estimated_sell_days": est_d, "isk_per_day": isk_d, "net_margin_pct": margin}

        return {
            "aggressive": {"price": float(price_aggressive), "label": "Aggressive — fastest sell", **_tier_metrics(price_aggressive)},
            "target": {"price": float(price_target), "label": "Target — optimal ISK/day", "estimated_sell_days": None, "isk_per_day": None, "net_margin_pct": None},
            "premium": {"price": float(price_premium), "label": premium_label, **_tier_metrics(price_premium)},
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
            dampened = components["market_hub_price"].get("volatility_dampened", False)
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
        if breakdown.get("gap_positioned"):
            gap = breakdown.get("gap_price", 0)
            reasoning += f" (Gap positioning: raised to {gap:,.0f} ISK — 1 tick below nearest undercutting order.)"
        if breakdown.get("already_front_of_queue"):
            reasoning += " (Already cheapest at hub — held at current price, no undercut needed.)"
        if breakdown.get("urgency_adjusted"):
            blend_pct = round((breakdown.get("urgency_blend", 0)) * 100)
            reasoning += f" (Expiry urgency: price moved {blend_pct}% toward aggressive tier.)"
        return reasoning
