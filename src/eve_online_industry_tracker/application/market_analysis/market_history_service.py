from __future__ import annotations

import statistics
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_
from eve_online_industry_tracker.infrastructure.models import MarketHistoryModel
from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider, StateSessionProvider


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

            for item in items:
                if not isinstance(item, dict):
                    continue

                date_str = str(item.get("date", "")).strip()
                if not date_str:
                    continue

                existing = app_session.query(MarketHistoryModel).filter(
                    and_(
                        MarketHistoryModel.type_id == type_id,
                        MarketHistoryModel.region_id == region_id,
                        MarketHistoryModel.date == date_str,
                    )
                ).first()

                if existing:
                    existing.close = float(item.get("average", item.get("close", 0)))
                    existing.high = float(item.get("highest", 0))
                    existing.low = float(item.get("lowest", 0))
                    existing.volume = int(item.get("volume", 0))
                    existing.order_count = int(item.get("order_count", 0))
                else:
                    record = MarketHistoryModel(
                        type_id=type_id,
                        region_id=region_id,
                        date=date_str,
                        close=float(item.get("average", item.get("close", 0))),
                        high=float(item.get("highest", 0)),
                        low=float(item.get("lowest", 0)),
                        volume=int(item.get("volume", 0)),
                        order_count=int(item.get("order_count", 0)),
                    )
                    app_session.add(record)

            app_session.commit()
        finally:
            try:
                app_session.close()
            except Exception:
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
                    "avg_1d": None,
                    "volatility": None,
                    "price_range": None,
                    "trend": None,
                }

            prices = [float(r.close) for r in records if r.close and r.close > 0]
            if not prices:
                return {
                    "has_data": False,
                    "avg_42w": None,
                    "avg_7d": None,
                    "avg_1d": None,
                    "volatility": None,
                    "price_range": None,
                    "trend": None,
                }

            # 42-week average
            avg_42w = float(statistics.mean(prices))

            # 7-day average (last 7 records)
            prices_7d = prices[-7:] if len(prices) >= 7 else prices
            avg_7d = float(statistics.mean(prices_7d))

            # 1-day (last price)
            avg_1d = prices[-1] if prices else None

            # Volatility (std dev)
            volatility = float(statistics.stdev(prices)) if len(prices) > 1 else 0.0

            # Price trend (7d vs 42w)
            trend_pct = ((avg_7d - avg_42w) / avg_42w * 100) if avg_42w > 0 else 0

            return {
                "has_data": True,
                "avg_42w": avg_42w,
                "avg_7d": avg_7d,
                "avg_1d": avg_1d,
                "volatility": volatility,
                "volatility_pct": (volatility / avg_42w * 100) if avg_42w > 0 else 0,
                "price_range": {
                    "min": float(min(prices)),
                    "max": float(max(prices)),
                },
                "trend_pct": trend_pct,  # positive = trending up
                "record_count": len(records),
            }
        finally:
            try:
                app_session.close()
            except Exception:
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
            except Exception:
                pass
