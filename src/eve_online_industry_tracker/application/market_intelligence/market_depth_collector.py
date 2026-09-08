from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text
from eve_online_industry_tracker.infrastructure.models import MarketHistoryModel
from eve_online_industry_tracker.infrastructure.session_provider import StateSessionProvider

HUB_REGION: dict[str, int] = {
    "jita": 10000002,
}


class MarketDepthCollector:
    def __init__(self, *, state: Any, admin_settings: Any = None):
        self._state = state
        self._admin = admin_settings
        self._sessions = StateSessionProvider(state=state)

    def _get_hub_and_region(self) -> tuple[str, int]:
        hub = "jita"
        if self._admin:
            try:
                hub = str(self._admin.get("daily_planner", "planner_market_hub")).lower()
            except Exception:
                pass
        region_id = HUB_REGION.get(hub, 10000002)
        return hub, region_id

    def _get_outlier_sigma(self) -> float:
        if self._admin:
            try:
                return float(self._admin.get("daily_planner", "planner_price_outlier_sigma"))
            except Exception:
                pass
        return 3.0

    def _get_type_ids_from_active_plan(self) -> list[int]:
        """Return type_ids from the most recent active build_plan."""
        app_session = self._sessions.app_session()
        try:
            result = app_session.execute(text(
                "SELECT DISTINCT bpi.type_id FROM build_plan_item bpi "
                "JOIN build_plan bp ON bp.id = bpi.plan_id "
                "WHERE bp.status = 'active' "
                "ORDER BY bp.id DESC"
            )).fetchall()
            return [int(row[0]) for row in result]
        except Exception:
            return []
        finally:
            try:
                app_session.close()
            except Exception:
                pass

    def _get_effective_velocity(self, type_id: int, corp_id: int | None) -> float | None:
        """Read effective_velocity from most recent build_plan_item, or fall back to SalesHistory."""
        app_session = self._sessions.app_session()
        try:
            row = app_session.execute(text(
                "SELECT bpi.effective_velocity FROM build_plan_item bpi "
                "JOIN build_plan bp ON bp.id = bpi.plan_id "
                "WHERE bpi.type_id = :type_id AND bp.status = 'active' "
                "ORDER BY bpi.id DESC LIMIT 1"
            ), {"type_id": type_id}).fetchone()
            if row and row[0] is not None:
                return float(row[0])
        except Exception:
            pass
        finally:
            try:
                app_session.close()
            except Exception:
                pass

        # Fallback: compute from SalesHistoryService
        if corp_id is None:
            return None
        try:
            from eve_online_industry_tracker.application.industry.sales_history_service import SalesHistoryService
            svc = SalesHistoryService(state=self._state)
            history = svc.get_sold_history(character_id=0, corporation_id=corp_id, type_id=type_id, days=30)
            total_qty = sum(int(tx.get("quantity", 0)) for tx in history)
            return max(0.01, total_qty / 30.0) if total_qty > 0 else None
        except Exception:
            return None

    def _compute_vwap_5d(self, type_id: int, region_id: int) -> float | None:
        """Compute 5-day VWAP from MarketHistoryModel. Returns None if < 3 days of data."""
        app_session = self._sessions.app_session()
        try:
            cutoff = (datetime.utcnow() - timedelta(days=5)).date().isoformat()
            rows = app_session.query(MarketHistoryModel).filter(
                MarketHistoryModel.type_id == type_id,
                MarketHistoryModel.region_id == region_id,
                MarketHistoryModel.date >= cutoff,
            ).order_by(MarketHistoryModel.date.desc()).limit(5).all()

            if len(rows) < 3:
                return None

            # Only include rows where both close price and volume are present
            valid_rows = [r for r in rows if r.volume and r.close]
            total_vol = sum(r.volume for r in valid_rows)
            if total_vol <= 0:
                return None
            return sum(r.close * r.volume for r in valid_rows) / total_vol
        finally:
            try:
                app_session.close()
            except Exception:
                pass

    def _get_avg_30d_stats(self, type_id: int, region_id: int) -> tuple[float | None, float | None]:
        """Returns (avg_30d, std_30d) for outlier filtering."""
        app_session = self._sessions.app_session()
        try:
            cutoff = (datetime.utcnow() - timedelta(days=30)).date().isoformat()
            rows = app_session.query(MarketHistoryModel).filter(
                MarketHistoryModel.type_id == type_id,
                MarketHistoryModel.region_id == region_id,
                MarketHistoryModel.date >= cutoff,
            ).all()
            prices = [float(r.close) for r in rows if r.close and r.close > 0]
            if not prices:
                return None, None
            avg = statistics.mean(prices)
            std = statistics.stdev(prices) if len(prices) > 1 else 0.0
            return avg, std
        finally:
            try:
                app_session.close()
            except Exception:
                pass

    def _get_own_volume_for_type(self, corp_id: int | None, type_id: int) -> int:
        """Total volume_remain of corp's own sell orders for this type_id."""
        if corp_id is None:
            return 0
        app_session = self._sessions.app_session()
        try:
            from eve_online_industry_tracker.infrastructure.models import CorporationMarketOrdersModel
            rows = app_session.query(CorporationMarketOrdersModel).filter(
                CorporationMarketOrdersModel.corporation_id == corp_id,
                CorporationMarketOrdersModel.type_id == type_id,
                CorporationMarketOrdersModel.is_buy_order == False,
            ).all()
            return sum(int(getattr(r, "volume_remain", 0) or 0) for r in rows)
        except Exception:
            return 0
        finally:
            try:
                app_session.close()
            except Exception:
                pass

    def _write_market_depth(
        self,
        type_id: int,
        hub: str,
        competitor_units: int,
        vwap_5d: float | None,
        spot_sell_price: float | None,
        competition_index: float | None,
    ) -> None:
        app_session = self._sessions.app_session()
        try:
            app_session.execute(text(
                "INSERT OR REPLACE INTO market_depth_cache "
                "(type_id, hub, competitor_units, vwap_5d, spot_sell_price, competition_index, snapshot_at) "
                "VALUES (:type_id, :hub, :competitor_units, :vwap_5d, :spot_sell_price, :competition_index, :snapshot_at)"
            ), {
                "type_id": type_id,
                "hub": hub,
                "competitor_units": competitor_units,
                "vwap_5d": vwap_5d,
                "spot_sell_price": spot_sell_price,
                "competition_index": competition_index,
                "snapshot_at": datetime.utcnow().isoformat(),
            })
            app_session.commit()
        finally:
            try:
                app_session.close()
            except Exception:
                pass

    def run(self) -> None:
        hub, region_id = self._get_hub_and_region()
        sigma = self._get_outlier_sigma()
        type_ids = self._get_type_ids_from_active_plan()

        if not type_ids:
            logging.info("MarketDepthCollector: no active plan type_ids, skipping cycle")
            return

        # Get primary corp ID
        corp_id: int | None = None
        try:
            corp_manager = getattr(self._state, "corp_manager", None)
            if corp_manager:
                ids = getattr(corp_manager, "_corporation_ids", None) or []
                if ids:
                    corp_id = int(ids[0])
        except Exception:
            pass

        # Step 1: Refresh market history for all type_ids
        esi_service = getattr(self._state, "esi_service", None)
        if esi_service is None:
            logging.warning("MarketDepthCollector: no esi_service on state, cannot refresh history")
            return

        from eve_online_industry_tracker.application.market_analysis.market_history_service import MarketHistoryService
        history_svc = MarketHistoryService(state=self._state)

        for type_id in type_ids:
            try:
                history_svc.fetch_and_store_history(type_id=type_id, region_id=region_id)
            except Exception as e:
                logging.warning("MarketDepthCollector: history refresh failed for type_id=%s: %s", type_id, e)

        # Step 2: Fetch sell orders for all type_ids
        try:
            sell_orders: dict[int, list[dict]] = esi_service.get_sell_order_book(type_ids, region_id=region_id)
        except Exception as e:
            logging.warning("MarketDepthCollector: sell order fetch failed: %s", e)
            sell_orders = {}

        # Step 3: Process each type_id
        for type_id in type_ids:
            try:
                orders = sell_orders.get(type_id) or []
                avg_30d, std_30d = self._get_avg_30d_stats(type_id, region_id)

                # Outlier filter — two-sided: removes both high-price and low-price anomalies
                if avg_30d is not None and std_30d is not None:
                    upper = avg_30d + sigma * std_30d
                    lower = max(1.0, avg_30d - sigma * std_30d)
                    filtered = [o for o in orders if lower <= float(o.get("price", 0)) <= upper]
                    if len(filtered) < len(orders) * 0.5:
                        logging.warning(
                            "MarketDepthCollector: >50%% outliers for type_id=%s, skipping filter",
                            type_id,
                        )
                        filtered = orders
                    orders = filtered

                # Spot price = lowest price
                spot_sell_price: float | None = None
                if orders:
                    spot_sell_price = min(float(o.get("price", 0)) for o in orders if o.get("price"))

                # competitor_units = total volume_remain minus own corp volume
                total_units = sum(int(o.get("volume_remain", 0)) for o in orders)
                own_units = self._get_own_volume_for_type(corp_id, type_id)
                competitor_units = max(0, total_units - own_units)

                # VWAP
                vwap_5d = self._compute_vwap_5d(type_id, region_id)
                if vwap_5d is None:
                    vwap_5d = spot_sell_price  # fallback

                # effective_velocity for competition_index
                ev = self._get_effective_velocity(type_id, corp_id)
                competition_index: float | None = None
                if ev is not None and ev > 0:
                    competition_index = competitor_units / (ev * 30)

                self._write_market_depth(
                    type_id=type_id,
                    hub=hub,
                    competitor_units=competitor_units,
                    vwap_5d=vwap_5d,
                    spot_sell_price=spot_sell_price,
                    competition_index=competition_index,
                )
            except Exception as e:
                logging.warning("MarketDepthCollector: failed for type_id=%s: %s", type_id, e)
