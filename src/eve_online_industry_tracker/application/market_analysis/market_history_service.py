from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from eve_online_industry_tracker.application.market_pricing.order_values import readable_int, readable_number
from eve_online_industry_tracker.infrastructure.models import MarketHistoryModel
from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider, StateSessionProvider


def _history_row_values(item: Any) -> dict[str, Any] | None:
    """The stored values of one ESI market history row, or None when the
    row is malformed (not a dict, no date, a missing/None or non-numeric
    price, volume or order count): the caller skips and counts it, never
    storing it as 0."""
    if not isinstance(item, dict):
        return None
    date_str = str(item.get("date") or "").strip()
    if not date_str:
        return None
    close = readable_number(item.get("average", item.get("close")))
    high = readable_number(item.get("highest"))
    low = readable_number(item.get("lowest"))
    volume = readable_int(item.get("volume"))
    order_count = readable_int(item.get("order_count"))
    if None in (close, high, low, volume, order_count):
        return None
    return {"date": date_str, "close": close, "high": high, "low": low, "volume": volume,
            "order_count": order_count}


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
    ) -> str | None:
        """Fetch market history from ESI and store in database. A malformed
        row (None, no date, a None or non-numeric field) is skipped, never
        stored as 0, with one WARNING per call counting the skipped rows.

        Returns None when the history was fetched (an empty history included),
        or why it was not (no ESI service, or the ESI fetch failed and the type
        was left out), with a WARNING: the stored history then stays as it was,
        and a caller can show the reason."""
        esi_service = getattr(self._state, "esi_service", None)
        if esi_service is None:
            reason = "no ESI service: market history not refreshed"
            logging.warning("Market history: type_id=%s region_id=%s not refreshed (%s)", type_id, region_id, reason)
            return reason

        history_rows = esi_service.get_market_history([type_id], region_id=region_id)
        if not isinstance(history_rows, dict) or int(type_id) not in history_rows:
            # ESIService leaves a type whose fetch failed out of the result.
            reader = getattr(esi_service, "market_history_unavailable_reason", None)
            reason = (reader(int(type_id), region_id=region_id) if reader is not None else None) or (
                f"ESI returned no market history for type_id={type_id}")
            logging.warning("Market history: type_id=%s region_id=%s not refreshed, the stored history is kept (%s)",
                            type_id, region_id, reason)
            return reason

        app_session = self._sessions.app_session()
        try:
            items = history_rows.get(int(type_id), [])
            if not isinstance(items, list):
                logging.warning("Market history: the ESI history of type_id=%s region_id=%s is not a list (%r); "
                                "nothing stored", type_id, region_id, type(items).__name__)
                items = []

            skipped = 0
            rows = []
            for item in items:
                values = _history_row_values(item)
                if values is None:
                    skipped += 1
                    continue
                rows.append({"type_id": int(type_id), "region_id": int(region_id), **values})

            if rows:
                # One upsert: no SELECT per day, and a day another writer stored meanwhile is updated.
                stmt = sqlite_insert(MarketHistoryModel)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["type_id", "region_id", "date"],
                    set_={**{c: stmt.excluded[c] for c in ("close", "high", "low", "volume", "order_count")},
                          "updated_at": func.now()},
                )
                app_session.execute(stmt, rows)
            app_session.commit()
            if skipped:
                logging.warning(
                    "Market history: skipped %d unreadable history row(s) (%d of %d) for type_id=%s region_id=%s "
                    "(None, no date, or a price, volume or order count that is not a number)",
                    skipped, skipped, len(items), type_id, region_id,
                )
            return None
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
