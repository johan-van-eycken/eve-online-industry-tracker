from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA, AdminSettingInvalid
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
        """(daily_planner/planner_market_hub, its region). The setting is a
        strict select of the HUB_REGION hubs: a stored invalid value raises
        AdminSettingInvalid naming it, so the cycle fails loudly (logged and
        kept as the job's last_error) instead of collecting Jita silently.
        Jita without a settings store."""
        hub = "jita" if self._admin is None else self._admin.get("daily_planner", "planner_market_hub")
        if hub not in HUB_REGION:
            raise ValueError(f"daily_planner/planner_market_hub={hub!r} is not a hub with a known region "
                             f"({', '.join(sorted(HUB_REGION))})")
        return hub, HUB_REGION[hub]

    def _get_outlier_sigma(self) -> float:
        if self._admin:
            try:
                return float(self._admin.get("daily_planner", "planner_price_outlier_sigma"))
            except AdminSettingInvalid:
                raise  # a stored invalid value fails loudly, naming it
            except (TypeError, ValueError) as exc:
                logging.warning("MarketDepthCollector: daily_planner/planner_price_outlier_sigma is not a "
                                "number (%s); using 3.0", exc)
        return 3.0

    def _get_type_ids_from_active_plan(self) -> list[int]:
        """Return type_ids from the most recent active build_plan. A failed
        read raises, so the cycle fails loudly (the job logs it and names it
        in last_error) instead of reporting a refresh that collected nothing."""
        app_session = self._sessions.app_session()
        try:
            result = app_session.execute(text(
                "SELECT DISTINCT bpi.type_id FROM build_plan_item bpi "
                "JOIN build_plan bp ON bp.id = bpi.plan_id "
                "WHERE bp.status = 'active' "
                "ORDER BY bp.id DESC"
            )).fetchall()
            return [int(row[0]) for row in result]
        finally:
            try:
                app_session.close()
            except SQLAlchemyError:
                pass

    def _get_effective_velocity(self, type_id: int, corp_ids: Any = (), character_ids: Any = ()) -> float | None:
        """Read effective_velocity from the most recent active build_plan_item,
        or fall back to the 30-day sales history of every tracked corporation
        and character (the same owners whose sell orders count as own volume)."""
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
        except SQLAlchemyError as exc:
            logging.warning("MarketDepthCollector: could not read the plan effective_velocity for type_id=%s "
                            "(%s); falling back to the sales history", type_id, exc)
        finally:
            try:
                app_session.close()
            except SQLAlchemyError:
                pass

        # Fallback: the sales history of every tracked owner
        corp_ids = sorted(int(c) for c in (corp_ids or ()))
        character_ids = sorted(int(c) for c in (character_ids or ()))
        if not corp_ids and not character_ids:
            return None
        try:
            from eve_online_industry_tracker.application.industry.sales_history_service import SalesHistoryService
            svc = SalesHistoryService(state=self._state)
            total_qty = 0
            for corp_id in corp_ids:
                history = svc.get_sold_history(character_id=0, corporation_id=corp_id, type_id=type_id, days=30)
                total_qty += sum(int(tx.get("quantity", 0)) for tx in history)
            for character_id in character_ids:
                history = svc.get_sold_history(character_id=character_id, type_id=type_id, days=30)
                total_qty += sum(int(tx.get("quantity", 0)) for tx in history)
            return max(0.01, total_qty / 30.0) if total_qty > 0 else None
        except SQLAlchemyError as exc:
            logging.warning("MarketDepthCollector: could not read the sales history for type_id=%s (%s); "
                            "no competition_index", type_id, exc)
            return None

    def _vwap_days(self) -> int:
        """daily_planner/planner_vwap_days (the schema default without settings)."""
        if self._admin:
            try:
                return int(self._admin.get("daily_planner", "planner_vwap_days"))
            except AdminSettingInvalid:
                raise  # a stored invalid value fails loudly, naming it
        return int(ADMIN_SETTINGS_SCHEMA["daily_planner"]["settings"]["planner_vwap_days"]["default"])

    def _compute_vwap_5d(self, type_id: int, region_id: int, days: int | None = None) -> float | None:
        """The VWAP over the last planner_vwap_days trading days (5 by
        default; the cache column keeps its vwap_5d name) from
        MarketHistoryModel. None (the caller falls back to spot) with fewer
        than min(3, days) days of data."""
        days = self._vwap_days() if days is None else int(days)   # run() reads it once per cycle
        app_session = self._sessions.app_session()
        try:
            cutoff = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
            rows = app_session.query(MarketHistoryModel).filter(
                MarketHistoryModel.type_id == type_id,
                MarketHistoryModel.region_id == region_id,
                MarketHistoryModel.date >= cutoff,
            ).order_by(MarketHistoryModel.date.desc()).limit(days).all()

            if len(rows) < min(3, days):
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
            except SQLAlchemyError:
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
            except SQLAlchemyError:
                pass

    def _tracked_owner_ids(self) -> tuple[set[int], set[int]]:
        """(corporation ids, character ids) of every owner we track: their
        sell orders are our own volume, not competition. corp_manager keeps
        its ids in a set, so there is no "primary" corporation to pick."""
        corp_manager = getattr(self._state, "corp_manager", None)
        corp_ids = {int(c) for c in (getattr(corp_manager, "_corporation_ids", None) or ()) if c is not None}
        char_manager = getattr(self._state, "char_manager", None)
        character_ids = {
            int(ch.character_id)
            for ch in (getattr(char_manager, "_character_list", None) or ())
            if getattr(ch, "character_id", None) is not None
        }
        return corp_ids, character_ids

    def _query_own_sell_volume(self, type_id: int, corp_ids: set[int], character_ids: set[int]) -> int:
        """Total volume_remain of the open sell orders of every tracked
        corporation and character for this type_id. A failed read raises
        (SQLAlchemyError): the caller keeps the previous row, marked stale."""
        if not corp_ids and not character_ids:
            return 0
        from eve_online_industry_tracker.infrastructure.models import (
            CharacterMarketOrdersModel,
            CorporationMarketOrdersModel,
        )
        app_session = self._sessions.app_session()
        try:
            total = 0
            if corp_ids:
                rows = app_session.query(CorporationMarketOrdersModel).filter(
                    CorporationMarketOrdersModel.corporation_id.in_(sorted(corp_ids)),
                    CorporationMarketOrdersModel.type_id == type_id,
                    CorporationMarketOrdersModel.is_buy_order == False,  # noqa: E712
                ).all()
                total += sum(int(getattr(r, "volume_remain", 0) or 0) for r in rows)
            if character_ids:
                rows = app_session.query(CharacterMarketOrdersModel).filter(
                    CharacterMarketOrdersModel.character_id.in_(sorted(character_ids)),
                    CharacterMarketOrdersModel.type_id == type_id,
                    CharacterMarketOrdersModel.is_buy_order == False,  # noqa: E712
                ).all()
                total += sum(int(getattr(r, "volume_remain", 0) or 0) for r in rows)
            return total
        finally:
            try:
                app_session.close()
            except SQLAlchemyError:
                pass

    def _ensure_stale_reason_column(self) -> None:
        """Add market_depth_cache.stale_reason when it is missing (additive;
        NULL on a fresh snapshot, the reason on a kept, stale one)."""
        app_session = self._sessions.app_session()
        try:
            cols = {str(r[1]) for r in app_session.execute(text("PRAGMA table_info(market_depth_cache)")).fetchall()}
            if cols and "stale_reason" not in cols:
                app_session.execute(text("ALTER TABLE market_depth_cache ADD COLUMN stale_reason TEXT"))
                app_session.commit()
        finally:
            try:
                app_session.close()
            except SQLAlchemyError:
                pass

    def _mark_stale(self, type_id: int, hub: str, reason: str) -> None:
        """Keep the previous market_depth_cache row for (type_id, hub) and
        mark it stale with the reason, with a WARNING. Without a previous row
        nothing is written."""
        app_session = self._sessions.app_session()
        try:
            result = app_session.execute(text(
                "UPDATE market_depth_cache SET stale_reason = :reason WHERE type_id = :type_id AND hub = :hub"
            ), {"reason": reason, "type_id": type_id, "hub": hub})
            app_session.commit()
            kept = bool(result.rowcount)
        finally:
            try:
                app_session.close()
            except SQLAlchemyError:
                pass
        logging.warning("MarketDepthCollector: type_id=%s not refreshed, %s (%s)", type_id,
                        "the previous market depth row is kept and marked stale" if kept
                        else "there is no previous market depth row (stale, nothing written)", reason)

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
                "(type_id, hub, competitor_units, vwap_5d, spot_sell_price, competition_index, snapshot_at, "
                "stale_reason) "
                "VALUES (:type_id, :hub, :competitor_units, :vwap_5d, :spot_sell_price, :competition_index, "
                ":snapshot_at, NULL)"
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
            except SQLAlchemyError:
                pass

    def run(self) -> None:
        hub, region_id = self._get_hub_and_region()
        sigma = self._get_outlier_sigma()
        vwap_days = self._vwap_days()   # once per cycle: an invalid value fails the cycle, not each type
        type_ids = self._get_type_ids_from_active_plan()

        if not type_ids:
            logging.info("MarketDepthCollector: no active plan type_ids, skipping cycle")
            return

        # Own volume: the sell orders of every tracked corporation and character
        corp_ids, character_ids = self._tracked_owner_ids()
        self._ensure_stale_reason_column()

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
            except AdminSettingInvalid:
                raise  # a stored invalid setting fails loudly, naming it
            except Exception as e:
                logging.warning("MarketDepthCollector: history refresh failed for type_id=%s: %s", type_id, e)

        # Step 2: Fetch sell orders for all type_ids
        try:
            sell_orders: dict[int, list[dict]] = esi_service.get_sell_order_book(type_ids, region_id=region_id)
        except AdminSettingInvalid:
            raise  # a stored invalid setting fails loudly, naming it
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

                # competitor_units = total volume_remain minus our own (every tracked corp and character)
                total_units = sum(int(o.get("volume_remain", 0)) for o in orders)
                try:
                    own_units = self._query_own_sell_volume(type_id, corp_ids, character_ids)
                except SQLAlchemyError as exc:
                    # competitor_units would silently include our own orders: keep the previous row instead
                    self._mark_stale(type_id, hub, f"own sell orders unreadable: {exc}")
                    continue
                competitor_units = max(0, total_units - own_units)

                # VWAP
                vwap_5d = self._compute_vwap_5d(type_id, region_id, days=vwap_days)
                if vwap_5d is None:
                    vwap_5d = spot_sell_price  # fallback

                # effective_velocity for competition_index
                ev = self._get_effective_velocity(type_id, corp_ids, character_ids)
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
