from __future__ import annotations

import statistics
import time
import threading
from typing import Any, Callable

from eve_online_industry_tracker.infrastructure.persistence import market_orderbook_view_cache_repo as market_orderbook_cache_repo
from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider, StateSessionProvider


ProgressCallback = Callable[[float, str, dict[str, Any] | None], None]


class MarketPricingService:
    _MARKET_HUBS: dict[str, dict[str, Any]] = {
        "jita": {"label": "Jita 4-4", "region_id": 10000002, "station_id": 60003760},
        "amarr": {"label": "Amarr VIII (Oris)", "region_id": 10000043, "station_id": 60008494},
        "dodixie": {"label": "Dodixie IX - Moon 20", "region_id": 10000032, "station_id": 60011866},
        "rens": {"label": "Rens VI - Moon 8", "region_id": 10000030, "station_id": 60004588},
        "hek": {"label": "Hek VIII - Moon 12", "region_id": 10000042, "station_id": 60005686},
    }

    _REGION_VOLUME_CACHE_TTL_SECONDS = 6 * 3600
    _region_volume_cache_lock = threading.Lock()
    _region_volume_cache: dict[str, tuple[float, dict[int, dict[str, Any]]]] = {}

    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)

    @property
    def _region_volume_ttl(self) -> int:
        admin = getattr(self._state, "admin_settings", None)
        return int(admin.get("cache_ttl", "region_volume_cache_ttl_seconds")) if admin else self._REGION_VOLUME_CACHE_TTL_SECONDS

    @classmethod
    def normalize_market_hub(cls, hub: str | None) -> str:
        normalized = str(hub or "").strip().lower()
        return normalized if normalized in cls._MARKET_HUBS else "jita"

    @staticmethod
    def normalize_order_side(side: str | None) -> str:
        normalized = str(side or "").strip().lower()
        return normalized if normalized in {"buy", "sell"} else "sell"

    def _market_hub_context(self, hub: str | None) -> dict[str, Any]:
        normalized_hub = self.normalize_market_hub(hub)
        configured = dict(self._MARKET_HUBS.get(normalized_hub) or {})

        esi_service = getattr(self._state, "esi_service", None)
        admin = getattr(self._state, "admin_settings", None)
        fallback_region = int(admin.get("market_defaults", "default_region_id")) if admin else 10000002
        fallback_station = int(admin.get("market_defaults", "default_station_id")) if admin else 60003760
        default_region_id = int(getattr(esi_service, "_region_id", fallback_region) or fallback_region)
        default_station_id = int(getattr(esi_service, "_station_id", fallback_station) or fallback_station)

        return {
            "hub": normalized_hub,
            "label": str(configured.get("label") or normalized_hub.title()),
            "region_id": int(configured.get("region_id") or default_region_id),
            "station_id": int(configured.get("station_id") or default_station_id),
        }

    def _market_pricing_cfg(self) -> dict[str, Any]:
        cfg_manager = getattr(self._state, "cfg_manager", None)
        if cfg_manager is None:
            return {}
        try:
            cfg = cfg_manager.all() or {}
        except Exception:
            return {}
        market_pricing_cfg = ((cfg.get("defaults") or {}).get("market_pricing") or {})
        return market_pricing_cfg if isinstance(market_pricing_cfg, dict) else {}

    def orderbook_depth(self) -> int:
        raw_value = self._market_pricing_cfg().get("orderbook_depth", 5)
        try:
            return max(1, int(raw_value))
        except Exception:
            return 5

    def orderbook_smoothing(self) -> str:
        raw_value = str(self._market_pricing_cfg().get("orderbook_smoothing") or "volume_weighted_mean_best_n").strip().lower()
        if raw_value not in {"mean_best_n", "median_best_n", "volume_weighted_mean_best_n"}:
            return "volume_weighted_mean_best_n"
        return raw_value

    def material_price_cache_ttl_seconds(self) -> int:
        raw_value = self._market_pricing_cfg().get("material_price_cache_ttl_seconds", 3600)
        try:
            return max(60, int(raw_value))
        except Exception:
            return 3600

    def _fetch_order_book(
        self,
        *,
        type_ids: list[int],
        region_id: int,
        side: str,
    ) -> dict[int, list[dict[str, Any]]]:
        esi_service = getattr(self._state, "esi_service", None)
        if esi_service is None:
            return {}

        normalized_side = self.normalize_order_side(side)
        if normalized_side == "buy":
            return esi_service.get_buy_order_book(type_ids, region_id=region_id)
        return esi_service.get_sell_order_book(type_ids, region_id=region_id)

    def _build_cached_views(
        self,
        *,
        order_books: dict[int, list[dict[str, Any]]],
        station_id: int,
        side: str,
    ) -> dict[int, list[tuple[float, int]]]:
        normalized_side = self.normalize_order_side(side)
        out: dict[int, list[tuple[float, int]]] = {}
        for type_id, rows in (order_books or {}).items():
            filtered_rows = [
                row for row in (rows or []) if isinstance(row, dict) and int(row.get("location_id") or 0) == int(station_id)
            ]
            filtered_rows.sort(
                key=lambda row: float(row.get("price") or 0.0),
                reverse=normalized_side == "buy",
            )

            levels: list[tuple[float, int]] = []
            for row in filtered_rows[: self.orderbook_depth()]:
                try:
                    price = float(row.get("price") or 0.0)
                    volume = int(row.get("volume_remain") or 0)
                except Exception:
                    continue
                if price <= 0 or volume <= 0:
                    continue
                levels.append((price, volume))
            out[int(type_id)] = levels
        return out

    def _build_liquidity_summaries(
        self,
        *,
        order_books: dict[int, list[dict[str, Any]]],
        station_id: int,
    ) -> dict[int, dict[str, int]]:
        out: dict[int, dict[str, int]] = {}
        for type_id, rows in (order_books or {}).items():
            total_volume = 0
            order_count = 0
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                if int(row.get("location_id") or 0) != int(station_id):
                    continue
                try:
                    volume = int(row.get("volume_remain") or 0)
                except Exception:
                    continue
                if volume <= 0:
                    continue
                total_volume += volume
                order_count += 1
            out[int(type_id)] = {
                "total_volume": int(total_volume),
                "order_count": int(order_count),
            }
        return out

    def _summarize_levels(self, levels: list[list[float | int]]) -> dict[str, Any]:
        prices: list[float] = []
        volumes: list[int] = []
        for level in levels[: self.orderbook_depth()]:
            if not isinstance(level, list) or len(level) < 2:
                continue
            try:
                prices.append(float(level[0]))
                volumes.append(int(level[1]))
            except Exception:
                continue

        if not prices:
            return {
                "unit_price": None,
                "sample_size": 0,
                "price_source": f"{self.orderbook_smoothing()}:{self.orderbook_depth()}",
                "levels": [],
            }

        if self.orderbook_smoothing() == "median_best_n":
            unit_price = float(statistics.median(prices))
        elif self.orderbook_smoothing() == "volume_weighted_mean_best_n":
            weighted_volume_total = sum(max(0, volume) for volume in volumes)
            if weighted_volume_total > 0:
                unit_price = float(
                    sum(price * max(0, volume) for price, volume in zip(prices, volumes)) / weighted_volume_total
                )
            else:
                unit_price = float(sum(prices) / len(prices))
        else:
            unit_price = float(sum(prices) / len(prices))

        return {
            "unit_price": unit_price,
            "sample_size": len(prices),
            "price_source": f"{self.orderbook_smoothing()}:{self.orderbook_depth()}",
            "levels": levels[: self.orderbook_depth()],
            "volume_total": sum(volumes),
        }

    def get_type_price_map(
        self,
        *,
        type_ids: list[int],
        hub: str = "jita",
        side: str = "sell",
        progress_callback: ProgressCallback | None = None,
    ) -> dict[int, dict[str, Any]]:
        normalized_type_ids = sorted({int(type_id) for type_id in (type_ids or []) if int(type_id) > 0})
        if not normalized_type_ids:
            return {}

        if getattr(self._state, "esi_service", None) is None:
            return {}

        hub_context = self._market_hub_context(hub)
        normalized_hub = str(hub_context.get("hub") or "jita")
        normalized_side = self.normalize_order_side(side)
        region_id = int(hub_context.get("region_id") or 10000002)
        station_id = int(hub_context.get("station_id") or 60003760)
        ttl_seconds = self.material_price_cache_ttl_seconds()
        depth = self.orderbook_depth()
        result: dict[int, dict[str, Any]] = {}

        app_session = self._sessions.app_session()
        try:
            cached_views = market_orderbook_cache_repo.get_views(
                app_session,
                hub=normalized_hub,
                region_id=region_id,
                station_id=station_id,
                side=normalized_side,
                at_hub=True,
                type_ids=normalized_type_ids,
                ttl_seconds=ttl_seconds,
            )
            missing_ids = [type_id for type_id in normalized_type_ids if type_id not in cached_views]
            total_ids = len(normalized_type_ids)

            for index, type_id in enumerate(normalized_type_ids, start=1):
                cached_view = cached_views.get(int(type_id)) or {}
                cached_levels = cached_view.get("levels") if isinstance(cached_view, dict) else None
                if cached_levels is None:
                    continue
                result[int(type_id)] = {
                    **self._summarize_levels([[float(price), int(volume)] for price, volume in cached_levels[:depth]]),
                    "cached": True,
                    "fetched_at": cached_view.get("fetched_at") if isinstance(cached_view, dict) else None,
                    "hub": normalized_hub,
                    "hub_label": str(hub_context.get("label") or normalized_hub.title()),
                    "side": normalized_side,
                    "region_id": region_id,
                    "station_id": station_id,
                }
                result[int(type_id)]["price_source"] = (
                    f"market_{normalized_hub}_{normalized_side}:{self.orderbook_smoothing()}:{self.orderbook_depth()}"
                )
                if progress_callback is not None:
                    progress_callback(
                        min(0.25, (index / max(1, total_ids)) * 0.25),
                        f"Reading cached {normalized_hub.title()} {normalized_side} prices",
                        {"completed": index, "total": total_ids},
                    )

            if missing_ids:
                fetched_books = self._fetch_order_book(
                    type_ids=missing_ids,
                    region_id=region_id,
                    side=normalized_side,
                )
                views_by_type_id = self._build_cached_views(
                    order_books=fetched_books,
                    station_id=station_id,
                    side=normalized_side,
                )
                liquidity_by_type_id = self._build_liquidity_summaries(
                    order_books=fetched_books,
                    station_id=station_id,
                )
                now = time.time()
                market_orderbook_cache_repo.upsert_views(
                    app_session,
                    hub=normalized_hub,
                    region_id=region_id,
                    station_id=station_id,
                    side=normalized_side,
                    at_hub=True,
                    views_by_type_id=views_by_type_id,
                    depth=depth,
                    liquidity_by_type_id=liquidity_by_type_id,
                )

                total_missing = len(missing_ids)
                for index, type_id in enumerate(missing_ids, start=1):
                    fetched_levels = views_by_type_id.get(int(type_id), [])
                    result[int(type_id)] = {
                        **self._summarize_levels([[float(price), int(volume)] for price, volume in fetched_levels[:depth]]),
                        "cached": False,
                        "fetched_at": now,
                        "hub": normalized_hub,
                        "hub_label": str(hub_context.get("label") or normalized_hub.title()),
                        "side": normalized_side,
                        "region_id": region_id,
                        "station_id": station_id,
                    }
                    result[int(type_id)]["price_source"] = (
                        f"market_{normalized_hub}_{normalized_side}:{self.orderbook_smoothing()}:{self.orderbook_depth()}"
                    )
                    if progress_callback is not None:
                        progress_callback(
                            0.25 + ((index / max(1, total_missing)) * 0.75),
                            f"Fetching {normalized_hub.title()} {normalized_side} prices from ESI",
                            {"completed": index, "total": total_missing},
                        )

                app_session.commit()
        finally:
            try:
                app_session.close()
            except Exception:
                pass

        return result

    def get_hub_liquidity_map(
        self,
        *,
        type_ids: list[int],
        hub: str = "jita",
        progress_callback: ProgressCallback | None = None,
    ) -> dict[int, dict[str, Any]]:
        normalized_type_ids = sorted({int(type_id) for type_id in (type_ids or []) if int(type_id) > 0})
        if not normalized_type_ids:
            return {}

        if getattr(self._state, "esi_service", None) is None:
            return {}

        hub_context = self._market_hub_context(hub)
        normalized_hub = str(hub_context.get("hub") or "jita")
        region_id = int(hub_context.get("region_id") or 10000002)
        station_id = int(hub_context.get("station_id") or 60003760)
        ttl_seconds = self.material_price_cache_ttl_seconds()

        app_session = self._sessions.app_session()
        try:
            cached_sell_summary = market_orderbook_cache_repo.get_liquidity_summaries(
                app_session,
                hub=normalized_hub,
                region_id=region_id,
                station_id=station_id,
                side="sell",
                at_hub=True,
                type_ids=normalized_type_ids,
                ttl_seconds=ttl_seconds,
            )
            cached_buy_summary = market_orderbook_cache_repo.get_liquidity_summaries(
                app_session,
                hub=normalized_hub,
                region_id=region_id,
                station_id=station_id,
                side="buy",
                at_hub=True,
                type_ids=normalized_type_ids,
                ttl_seconds=ttl_seconds,
            )

            missing_sell_ids = [type_id for type_id in normalized_type_ids if int(type_id) not in cached_sell_summary]
            missing_buy_ids = [type_id for type_id in normalized_type_ids if int(type_id) not in cached_buy_summary]

            if missing_sell_ids:
                sell_books = self._fetch_order_book(type_ids=missing_sell_ids, region_id=region_id, side="sell")
                if progress_callback is not None:
                    progress_callback(0.5, f"Fetched {normalized_hub.title()} sell liquidity", {"type_count": len(missing_sell_ids)})
                sell_views = self._build_cached_views(order_books=sell_books, station_id=station_id, side="sell")
                sell_summary = self._build_liquidity_summaries(order_books=sell_books, station_id=station_id)
                market_orderbook_cache_repo.upsert_views(
                    app_session,
                    hub=normalized_hub,
                    region_id=region_id,
                    station_id=station_id,
                    side="sell",
                    at_hub=True,
                    views_by_type_id=sell_views,
                    depth=self.orderbook_depth(),
                    liquidity_by_type_id=sell_summary,
                )
                cached_sell_summary.update(sell_summary)

            if missing_buy_ids:
                buy_books = self._fetch_order_book(type_ids=missing_buy_ids, region_id=region_id, side="buy")
                if progress_callback is not None:
                    progress_callback(1.0, f"Fetched {normalized_hub.title()} buy liquidity", {"type_count": len(missing_buy_ids)})
                buy_views = self._build_cached_views(order_books=buy_books, station_id=station_id, side="buy")
                buy_summary = self._build_liquidity_summaries(order_books=buy_books, station_id=station_id)
                market_orderbook_cache_repo.upsert_views(
                    app_session,
                    hub=normalized_hub,
                    region_id=region_id,
                    station_id=station_id,
                    side="buy",
                    at_hub=True,
                    views_by_type_id=buy_views,
                    depth=self.orderbook_depth(),
                    liquidity_by_type_id=buy_summary,
                )
                cached_buy_summary.update(buy_summary)

            app_session.commit()
        finally:
            try:
                app_session.close()
            except Exception:
                pass

        result: dict[int, dict[str, Any]] = {}
        for type_id in normalized_type_ids:
            sell_metrics = cached_sell_summary.get(int(type_id)) or {}
            buy_metrics = cached_buy_summary.get(int(type_id)) or {}
            result[int(type_id)] = {
                "hub": normalized_hub,
                "hub_label": str(hub_context.get("label") or normalized_hub.title()),
                "region_id": region_id,
                "station_id": station_id,
                "sell_volume_total": int(sell_metrics.get("total_volume") or 0),
                "sell_order_count": int(sell_metrics.get("order_count") or 0),
                "buy_volume_total": int(buy_metrics.get("total_volume") or 0),
                "buy_order_count": int(buy_metrics.get("order_count") or 0),
            }

        return result

    def get_region_daily_volume_map(
        self,
        *,
        type_ids: list[int],
        hub: str = "jita",
        progress_callback: ProgressCallback | None = None,
    ) -> dict[int, dict[str, Any]]:
        normalized_type_ids = sorted({int(type_id) for type_id in (type_ids or []) if int(type_id) > 0})
        if not normalized_type_ids:
            return {}

        esi_service = getattr(self._state, "esi_service", None)
        if esi_service is None:
            return {}

        hub_context = self._market_hub_context(hub)
        normalized_hub = str(hub_context.get("hub") or "jita")
        region_id = int(hub_context.get("region_id") or 10000002)

        now = time.time()
        cache_key = f"{normalized_hub}:{region_id}"
        result: dict[int, dict[str, Any]] = {}
        missing_type_ids: list[int] = []

        with self._region_volume_cache_lock:
            cached_entry = self._region_volume_cache.get(cache_key)
            if cached_entry and (now - cached_entry[0]) < self._region_volume_ttl:
                cached_map = cached_entry[1]
                for type_id in normalized_type_ids:
                    if type_id in cached_map:
                        result[type_id] = cached_map[type_id]
                    else:
                        missing_type_ids.append(type_id)
            else:
                missing_type_ids = list(normalized_type_ids)

        if not missing_type_ids:
            return result

        history_by_type_id = esi_service.get_market_history(missing_type_ids, region_id=region_id)
        total_ids = len(missing_type_ids)
        fetched_results: dict[int, dict[str, Any]] = {}

        for index, type_id in enumerate(missing_type_ids, start=1):
            history_rows = history_by_type_id.get(int(type_id)) or []
            latest_row = history_rows[-1] if history_rows else {}
            trailing_rows = history_rows[-7:] if history_rows else []
            trailing_volumes = [
                int(row.get("volume") or 0)
                for row in trailing_rows
                if isinstance(row, dict)
            ]
            trailing_average_volume = (
                float(sum(trailing_volumes)) / float(len(trailing_volumes))
                if trailing_volumes
                else 0.0
            )
            fetched_results[int(type_id)] = {
                "hub": normalized_hub,
                "hub_label": str(hub_context.get("label") or normalized_hub.title()),
                "region_id": region_id,
                "daily_volume": int(latest_row.get("volume") or 0) if latest_row else 0,
                "daily_volume_7d_avg": trailing_average_volume,
                "daily_volume_7d_sample_size": len(trailing_volumes),
                "daily_order_count": int(latest_row.get("order_count") or 0) if latest_row else 0,
                "daily_volume_date": latest_row.get("date") if latest_row else None,
            }
            if progress_callback is not None:
                progress_callback(
                    index / max(1, total_ids),
                    f"Fetched {normalized_hub.title()} region history",
                    {"completed": index, "total": total_ids},
                )

        with self._region_volume_cache_lock:
            cached_entry = self._region_volume_cache.get(cache_key)
            if cached_entry and (now - cached_entry[0]) < self._region_volume_ttl:
                cached_entry[1].update(fetched_results)
            else:
                self._region_volume_cache[cache_key] = (now, dict(fetched_results))

        result.update(fetched_results)
        return result

    def get_material_sell_price_map(
        self,
        *,
        material_type_ids: list[int],
        progress_callback: ProgressCallback | None = None,
    ) -> dict[int, dict[str, Any]]:
        return self.get_type_price_map(
            type_ids=material_type_ids,
            hub="jita",
            side="sell",
            progress_callback=progress_callback,
        )
