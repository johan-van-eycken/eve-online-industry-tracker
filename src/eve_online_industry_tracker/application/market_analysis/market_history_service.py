from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError
from eve_online_industry_tracker.application.market_pricing.order_values import readable_int, readable_number
from eve_online_industry_tracker.infrastructure.models import MarketHistoryModel
from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider, StateSessionProvider


def _history_values(item: dict[str, Any]) -> dict[str, Any] | None:
    """The stored values of one ESI market history day; None when a field is
    missing or not a number (the day is skipped, never stored as 0)."""
    close = readable_number(item.get("average", item.get("close")))
    high = readable_number(item.get("highest"))
    low = readable_number(item.get("lowest"))
    volume = readable_int(item.get("volume"))
    order_count = readable_int(item.get("order_count"))
    if None in (close, high, low, volume, order_count):
        return None
    return {"close": close, "high": high, "low": low, "volume": volume, "order_count": order_count}


class MarketHistoryService:
    """Fetch, store, and analyze historical market price data."""

    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)

    def fetch_and_store_history(
        self,
        *,
        type_id: int,
        region_id: int = 10000002,  # Jita region by default
    ) -> None:
        """Fetch market history from ESI and store in database."""
        app_session = self._sessions.app_session()
        try:
            esi_service = getattr(self._state, "esi_service", None)
            if esi_service is None:
                return

            history_rows = esi_service.get_market_history([type_id], region_id=region_id)
            items = history_rows.get(int(type_id), []) if isinstance(history_rows, dict) else []

            skipped = 0
            for item in items:
                if not isinstance(item, dict):
                    skipped += 1
                    continue

                date_str = str(item.get("date") or "").strip()
                values = _history_values(item)
                if not date_str or values is None:
                    skipped += 1
                    continue

                existing = app_session.query(MarketHistoryModel).filter(
                    and_(
                        MarketHistoryModel.type_id == type_id,
                        MarketHistoryModel.region_id == region_id,
                        MarketHistoryModel.date == date_str,
                    )
                ).first()

                if existing:
                    for field, value in values.items():
                        setattr(existing, field, value)
                else:
                    app_session.add(MarketHistoryModel(type_id=type_id, region_id=region_id, date=date_str, **values))

            app_session.commit()
            if skipped:
                logging.warning(
                    "Market history of type %s (region %s): skipped %d unreadable history row(s) "
                    "(no date, or a price, volume or order count that is not a number)",
                    type_id,
                    region_id,
                    skipped,
                )
        finally:
            try:
                app_session.close()
            except SQLAlchemyError:
                pass

    def get_price_stats(
        self,
        *,
        type_id: int,
        region_id: int = 10000002,
        days: int = 42 * 7,  # 42 weeks
    ) -> dict[str, Any]:
        """Get historical price statistics (42-week avg, 7d avg, etc)."""
        app_session = self._sessions.app_session()
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            cutoff_str = cutoff_date.date().isoformat()

            records = app_session.query(MarketHistoryModel).filter(
                and_(
                    MarketHistoryModel.type_id == type_id,
                    MarketHistoryModel.region_id == region_id,
                    MarketHistoryModel.date >= cutoff_str,
                )
            ).order_by(MarketHistoryModel.date).all()

            if not records:
                return {
                    "has_data": False,
                    "avg_42w": None,
                    "avg_7d": None,
                    "avg_30d": None,
                    "avg_1d": None,
                    "volatility": None,
                    "price_range": None,
                    "trend": None,
                    "trend_30d_pct": None,
                }

            prices = [float(r.close) for r in records if r.close and r.close > 0]
            if not prices:
                return {
                    "has_data": False,
                    "avg_42w": None,
                    "avg_7d": None,
                    "avg_30d": None,
                    "avg_1d": None,
                    "volatility": None,
                    "price_range": None,
                    "trend": None,
                    "trend_30d_pct": None,
                }

            # 42-week average
            avg_42w = float(statistics.mean(prices))

            # 7-day average (last 7 records)
            prices_7d = prices[-7:] if len(prices) >= 7 else prices
            avg_7d = float(statistics.mean(prices_7d))

            # 30-day average (last 30 records)
            avg_30d = float(statistics.mean(prices[-30:])) if len(prices) >= 30 else None

            # 1-day (last price)
            avg_1d = prices[-1] if prices else None

            # Volatility (std dev)
            volatility = float(statistics.stdev(prices)) if len(prices) > 1 else 0.0

            # Price trend (7d vs 42w)
            trend_pct = ((avg_7d - avg_42w) / avg_42w * 100) if avg_42w > 0 else 0

            # Price trend (7d vs 30d)
            trend_30d_pct = ((avg_7d - avg_30d) / avg_30d * 100) if (avg_30d is not None and avg_30d > 0) else None

            return {
                "has_data": True,
                "avg_42w": avg_42w,
                "avg_7d": avg_7d,
                "avg_30d": avg_30d,
                "avg_1d": avg_1d,
                "volatility": volatility,
                "volatility_pct": (volatility / avg_42w * 100) if avg_42w > 0 else 0,
                "price_range": {
                    "min": float(min(prices)),
                    "max": float(max(prices)),
                },
                "trend_pct": trend_pct,  # positive = trending up
                "trend_30d_pct": trend_30d_pct,
                "record_count": len(records),
            }
        finally:
            try:
                app_session.close()
            except SQLAlchemyError:
                pass

    def get_volume_stats(
        self,
        *,
        type_id: int,
        region_id: int = 10000002,
        days: int = 42 * 7,
    ) -> dict[str, Any]:
        """Get volume statistics."""
        app_session = self._sessions.app_session()
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            cutoff_str = cutoff_date.date().isoformat()

            records = app_session.query(MarketHistoryModel).filter(
                and_(
                    MarketHistoryModel.type_id == type_id,
                    MarketHistoryModel.region_id == region_id,
                    MarketHistoryModel.date >= cutoff_str,
                )
            ).order_by(MarketHistoryModel.date).all()

            if not records:
                return {
                    "has_data": False,
                    "total_volume": 0,
                    "avg_daily_volume": 0,
                    "peak_daily_volume": 0,
                }

            volumes = [int(r.volume) for r in records if r.volume is not None]
            if not volumes:
                return {
                    "has_data": False,
                    "total_volume": 0,
                    "avg_daily_volume": 0,
                    "peak_daily_volume": 0,
                }

            return {
                "has_data": True,
                "total_volume": int(sum(volumes)),
                "avg_daily_volume": float(statistics.mean(volumes)),
                "peak_daily_volume": int(max(volumes)),
                "record_count": len(records),
            }
        finally:
            try:
                app_session.close()
            except SQLAlchemyError:
                pass
