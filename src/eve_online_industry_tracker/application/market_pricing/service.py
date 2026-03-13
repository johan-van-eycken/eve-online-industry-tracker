from __future__ import annotations

import statistics
import time
from typing import Any, Callable

from eve_online_industry_tracker.infrastructure.persistence import market_orderbook_cache_repo
from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider, StateSessionProvider


ProgressCallback = Callable[[float, str, dict[str, Any] | None], None]


class MarketPricingService:
    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)

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

    def get_material_sell_price_map(
        self,
        *,
        material_type_ids: list[int],
        progress_callback: ProgressCallback | None = None,
    ) -> dict[int, dict[str, Any]]:
        material_ids = sorted({int(type_id) for type_id in (material_type_ids or []) if int(type_id) > 0})
        if not material_ids:
            return {}

        if getattr(self._state, "esi_service", None) is None:
            return {}

        region_id = int(getattr(self._state.esi_service, "_region_id", 10000002) or 10000002)
        station_id = int(getattr(self._state.esi_service, "_station_id", 60003760) or 60003760)
        ttl_seconds = self.material_price_cache_ttl_seconds()
        depth = self.orderbook_depth()
        result: dict[int, dict[str, Any]] = {}

        app_session = self._sessions.app_session()
        try:
            missing_ids: list[int] = []
            total_ids = len(material_ids)
            for index, material_type_id in enumerate(material_ids, start=1):
                cached = market_orderbook_cache_repo.get_cached_orderbook_levels(
                    app_session,
                    hub="jita",
                    region_id=region_id,
                    station_id=station_id,
                    side="sell",
                    type_id=int(material_type_id),
                    at_hub=True,
                    ttl_seconds=ttl_seconds,
                )
                if cached is None:
                    missing_ids.append(int(material_type_id))
                    continue
                levels, fetched_at = cached
                result[int(material_type_id)] = {
                    **self._summarize_levels(levels[:depth]),
                    "cached": True,
                    "fetched_at": float(fetched_at),
                }
                if progress_callback is not None:
                    progress_callback(
                        min(0.25, (index / max(1, total_ids)) * 0.25),
                        "Reading cached material prices",
                        {"completed": index, "total": total_ids},
                    )

            if missing_ids:
                fetched_books = self._state.esi_service.get_material_prices(missing_ids)
                now = time.time()
                total_missing = len(missing_ids)
                for index, material_type_id in enumerate(missing_ids, start=1):
                    raw_levels = fetched_books.get(int(material_type_id)) or []
                    levels: list[list[float | int]] = []
                    for row in raw_levels[:depth]:
                        if not isinstance(row, dict):
                            continue
                        try:
                            levels.append([float(row.get("price") or 0.0), int(row.get("volume_remain") or 0)])
                        except Exception:
                            continue
                    market_orderbook_cache_repo.upsert_orderbook_levels(
                        app_session,
                        hub="jita",
                        region_id=region_id,
                        station_id=station_id,
                        side="sell",
                        type_id=int(material_type_id),
                        at_hub=True,
                        depth=depth,
                        levels=levels,
                        fetched_at=now,
                    )
                    result[int(material_type_id)] = {
                        **self._summarize_levels(levels),
                        "cached": False,
                        "fetched_at": now,
                    }
                    if progress_callback is not None:
                        progress_callback(
                            0.25 + ((index / max(1, total_missing)) * 0.75),
                            "Fetching missing material prices from ESI",
                            {"completed": index, "total": total_missing},
                        )

                app_session.commit()
        finally:
            try:
                app_session.close()
            except Exception:
                pass

        return result
