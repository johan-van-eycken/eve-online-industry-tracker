from __future__ import annotations

import logging
import statistics
import time
import threading
from typing import Any, Callable

from sqlalchemy.exc import SQLAlchemyError

from config.schemas import CONFIG_SCHEMA
from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA
from eve_online_industry_tracker.application.market_pricing.order_values import readable_int, readable_number
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

    _region_volume_cache_lock = threading.Lock()
    _region_volume_cache: dict[str, tuple[float, dict[int, dict[str, Any]]]] = {}

    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)

    @property
    def _region_volume_ttl(self) -> int:
        return int(self._admin_value("cache_ttl", "region_volume_cache_ttl_seconds"))

    @classmethod
    def normalize_market_hub(cls, hub: str | None) -> str:
        """The hub key; jita when none is given. An unknown hub is refused (a
        400 naming it), never priced as Jita."""
        normalized = str(hub or "").strip().lower()
        if not normalized:
            return "jita"
        if normalized not in cls._MARKET_HUBS:
            raise ServiceError(
                f"unknown market hub {normalized!r} (known: {', '.join(sorted(cls._MARKET_HUBS))})", status_code=400,
            )
        return normalized

    @staticmethod
    def normalize_order_side(side: str | None) -> str:
        """buy or sell; sell when none is given. An unknown side is refused (a
        400 naming it), never priced as sell."""
        normalized = str(side or "").strip().lower()
        if not normalized:
            return "sell"
        if normalized not in {"buy", "sell"}:
            raise ServiceError(f"unknown order side {normalized!r} (known: buy, sell)", status_code=400)
        return normalized

    def _market_hub_context(self, hub: str | None) -> dict[str, Any]:
        normalized_hub = self.normalize_market_hub(hub)
        # Every hub has its region and station (the removed market_defaults/default_region_id and
        # default_station_id never applied).
        configured = self._MARKET_HUBS[normalized_hub]

        return {
            "hub": normalized_hub,
            "label": str(configured.get("label") or normalized_hub.title()),
            "region_id": int(configured["region_id"]),
            "station_id": int(configured["station_id"]),
        }

    def _market_pricing_cfg(self) -> dict[str, Any]:
        cfg_manager = getattr(self._state, "cfg_manager", None)
        if cfg_manager is None:
            return {}
        cfg = cfg_manager.all() or {}
        market_pricing_cfg = ((cfg.get("defaults") or {}).get("market_pricing") or {})
        return market_pricing_cfg if isinstance(market_pricing_cfg, dict) else {}

    def _admin_value(self, category: str, key: str) -> Any:
        """An admin setting at each use; the schema default without a settings store."""
        admin = getattr(self._state, "admin_settings", None)
        if admin is None:
            return ADMIN_SETTINGS_SCHEMA[category]["settings"][key]["default"]
        return admin.get(category, key)

    def orderbook_depth(self) -> int:
        """market_defaults/orderbook_depth (formerly config.json market_pricing.orderbook_depth)."""
        return int(self._admin_value("market_defaults", "orderbook_depth"))

    _ORDERBOOK_SMOOTHINGS = frozenset({"mean_best_n", "median_best_n", "volume_weighted_mean_best_n"})

    def orderbook_smoothing(self) -> str:
        """config defaults.market_pricing.orderbook_smoothing; the config
        schema's default when it is not set. An invalid value is refused
        (ValueError naming it), never silently replaced."""
        raw = self._market_pricing_cfg().get("orderbook_smoothing")
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return str(CONFIG_SCHEMA["defaults"]["market_pricing"]["orderbook_smoothing"])
        raw_value = str(raw).strip().lower()
        if raw_value not in self._ORDERBOOK_SMOOTHINGS:
            raise ValueError(
                f"config defaults.market_pricing.orderbook_smoothing={raw!r} is not one of "
                f"{', '.join(sorted(self._ORDERBOOK_SMOOTHINGS))}"
            )
        return raw_value

    def material_price_cache_ttl_seconds(self) -> int:
        """cache_ttl/material_price_cache_ttl_seconds (formerly config.json
        market_pricing.material_price_cache_ttl_seconds)."""
        return int(self._admin_value("cache_ttl", "material_price_cache_ttl_seconds"))

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

    def _order_book_unavailable_reasons(
        self, *, type_ids: list[int], region_id: int, side: str, books: dict[int, Any] | None,
    ) -> dict[int, str]:
        """{type: why its `side` order-book fetch failed} for the requested
        types left out of `books` with a reason (ESIService leaves a failed
        type out; a type with no orders is left out without one). A failed
        type's price and liquidity are unknown, not an empty book."""
        reader = getattr(getattr(self._state, "esi_service", None), "order_book_unavailable_reason", None)
        if reader is None:
            return {}
        out: dict[int, str] = {}
        for type_id in type_ids:
            if int(type_id) in (books or {}):
                continue
            reason = reader(int(type_id), side=side, region_id=region_id)
            if reason:
                out[int(type_id)] = str(reason)
        if out:
            logging.warning(
                "Market order book (%s, region %s): the fetch of %d type(s) failed, their price and liquidity are "
                "unknown: %s", side, region_id, len(out), sorted(out)[:20],
            )
        return out

    def _build_cached_views(
        self,
        *,
        order_books: dict[int, list[dict[str, Any]]],
        station_id: int,
        side: str,
    ) -> dict[int, list[tuple[float, int]]]:
        normalized_side = self.normalize_order_side(side)
        depth = self.orderbook_depth()
        out: dict[int, list[tuple[float, int]]] = {}
        skipped = 0
        skipped_type_ids: set[int] = set()
        for type_id, rows in (order_books or {}).items():
            # Read every row at the station first: an unreadable one is skipped (counted),
            # so it neither breaks the sort nor takes one of the `depth` levels.
            readable: list[tuple[float, int]] = []
            for row in rows or []:
                location_id = readable_int(row.get("location_id")) if isinstance(row, dict) else None
                if location_id is None:
                    skipped += 1
                    skipped_type_ids.add(int(type_id))
                    continue
                if location_id != int(station_id):
                    continue
                price = readable_number(row.get("price"))
                volume = readable_int(row.get("volume_remain"))
                if price is None or volume is None or price <= 0:
                    skipped += 1
                    skipped_type_ids.add(int(type_id))
                    continue
                if volume <= 0:
                    continue  # a filled order: nothing left to buy or sell
                readable.append((price, volume))
            readable.sort(key=lambda level: level[0], reverse=normalized_side == "buy")
            out[int(type_id)] = readable[:depth]
        if skipped:
            logging.warning(
                "Market order book (%s, station %s): skipped %d unreadable row(s) (no readable location, "
                "price or volume) of %d type(s): %s",
                normalized_side,
                station_id,
                skipped,
                len(skipped_type_ids),
                sorted(skipped_type_ids)[:20],
            )
        return out

    def _build_liquidity_summaries(
        self,
        *,
        order_books: dict[int, list[dict[str, Any]]],
        station_id: int,
    ) -> dict[int, dict[str, int]]:
        out: dict[int, dict[str, int]] = {}
        skipped = 0
        skipped_type_ids: set[int] = set()
        for type_id, rows in (order_books or {}).items():
            total_volume = 0
            order_count = 0
            for row in rows or []:
                location_id = readable_int(row.get("location_id")) if isinstance(row, dict) else None
                if location_id is None:
                    skipped += 1
                    skipped_type_ids.add(int(type_id))
                    continue
                if location_id != int(station_id):
                    continue
                volume = readable_int(row.get("volume_remain"))
                if volume is None:
                    skipped += 1
                    skipped_type_ids.add(int(type_id))
                    continue
                if volume <= 0:
                    continue
                total_volume += volume
                order_count += 1
            out[int(type_id)] = {
                "total_volume": int(total_volume),
                "order_count": int(order_count),
            }
        if skipped:
            logging.warning(
                "Market liquidity (station %s): skipped %d unreadable order book row(s) (no readable "
                "location or volume) of %d type(s): %s",
                station_id,
                skipped,
                len(skipped_type_ids),
                sorted(skipped_type_ids)[:20],
            )
        return out

    def _summarize_levels(self, levels: list[list[float | int]]) -> dict[str, Any]:
        prices: list[float] = []
        volumes: list[int] = []
        skipped = 0
        for level in levels[: self.orderbook_depth()]:
            price = readable_number(level[0]) if isinstance(level, (list, tuple)) and len(level) >= 2 else None
            volume = readable_int(level[1]) if price is not None else None
            if price is None or volume is None:
                skipped += 1
                continue
            prices.append(price)
            volumes.append(volume)
        if skipped:
            logging.warning(
                "Market price levels: skipped %d unreadable price level(s) of %d (no readable price or volume)",
                skipped,
                len(levels[: self.orderbook_depth()]),
            )

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
        normalized_hub = str(hub_context["hub"])
        normalized_side = self.normalize_order_side(side)
        region_id = int(hub_context["region_id"])
        station_id = int(hub_context["station_id"])
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
                    **self._summarize_levels([list(level) if isinstance(level, (list, tuple)) else level
                                              for level in cached_levels[:depth]]),
                    "cached": True,
                    "fetched_at": cached_view.get("fetched_at") if isinstance(cached_view, dict) else None,
                    "hub": normalized_hub,
                    "hub_label": str(hub_context.get("label") or normalized_hub.title()),
                    "side": normalized_side,
                    "region_id": region_id,
                    "station_id": station_id,
                    "price_unavailable_reason": None,
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
                # A failed fetch is not in the book, so it is not cached either.
                unavailable = self._order_book_unavailable_reasons(
                    type_ids=missing_ids, region_id=region_id, side=normalized_side, books=fetched_books,
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
                        **self._summarize_levels([list(level) for level in fetched_levels[:depth]]),
                        "cached": False,
                        "fetched_at": now,
                        "hub": normalized_hub,
                        "hub_label": str(hub_context.get("label") or normalized_hub.title()),
                        "side": normalized_side,
                        "region_id": region_id,
                        "station_id": station_id,
                        "price_unavailable_reason": unavailable.get(int(type_id)),
                    }
                    if int(type_id) in unavailable:
                        # The fetch failed: the price is unknown, not an empty book's.
                        result[int(type_id)]["unit_price"] = None
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
            except SQLAlchemyError:
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
        normalized_hub = str(hub_context["hub"])
        region_id = int(hub_context["region_id"])
        station_id = int(hub_context["station_id"])
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
            #: {side: {type: why its fetch failed}}: unknown liquidity, not 0 units.
            unavailable: dict[str, dict[int, str]] = {"sell": {}, "buy": {}}

            if missing_sell_ids:
                sell_books = self._fetch_order_book(type_ids=missing_sell_ids, region_id=region_id, side="sell")
                if progress_callback is not None:
                    progress_callback(0.5, f"Fetched {normalized_hub.title()} sell liquidity", {"type_count": len(missing_sell_ids)})
                sell_views = self._build_cached_views(order_books=sell_books, station_id=station_id, side="sell")
                sell_summary = self._build_liquidity_summaries(order_books=sell_books, station_id=station_id)
                unavailable["sell"] = self._order_book_unavailable_reasons(
                    type_ids=missing_sell_ids, region_id=region_id, side="sell", books=sell_books,
                )
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
                unavailable["buy"] = self._order_book_unavailable_reasons(
                    type_ids=missing_buy_ids, region_id=region_id, side="buy", books=buy_books,
                )
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
            except SQLAlchemyError:
                pass

        result: dict[int, dict[str, Any]] = {}
        for type_id in normalized_type_ids:
            sell_metrics = cached_sell_summary.get(int(type_id)) or {}
            buy_metrics = cached_buy_summary.get(int(type_id)) or {}
            sell_failed = unavailable["sell"].get(int(type_id))
            buy_failed = unavailable["buy"].get(int(type_id))
            reasons = [f"{side}: {why}" for side, why in (("sell", sell_failed), ("buy", buy_failed)) if why]
            result[int(type_id)] = {
                "hub": normalized_hub,
                "hub_label": str(hub_context.get("label") or normalized_hub.title()),
                "region_id": region_id,
                "station_id": station_id,
                # A failed side is unknown (None), not an empty book's 0.
                "sell_volume_total": None if sell_failed else int(sell_metrics.get("total_volume") or 0),
                "sell_order_count": None if sell_failed else int(sell_metrics.get("order_count") or 0),
                "buy_volume_total": None if buy_failed else int(buy_metrics.get("total_volume") or 0),
                "buy_order_count": None if buy_failed else int(buy_metrics.get("order_count") or 0),
                "liquidity_unavailable_reason": "; ".join(reasons) or None,
            }

        return result

    def get_region_daily_volume_map(
        self,
        *,
        type_ids: list[int],
        hub: str = "jita",
        progress_callback: ProgressCallback | None = None,
        unavailable: dict[int, str] | None = None,
    ) -> dict[int, dict[str, Any]]:
        """The latest and 7-day average daily volume of each type in the hub's
        region, from the ESI market history. A type whose history could not
        be fetched is unknown: absent from the result (and not cached), its
        reason put in `unavailable` when given, never a volume of 0. A type
        with a real, empty history is present with no volume."""
        normalized_type_ids = sorted({int(type_id) for type_id in (type_ids or []) if int(type_id) > 0})
        if not normalized_type_ids:
            return {}
        if unavailable is None:
            unavailable = {}

        esi_service = getattr(self._state, "esi_service", None)
        if esi_service is None:
            for type_id in normalized_type_ids:
                unavailable[type_id] = "no ESI service: region daily volume unknown"
            logging.warning("Region daily volume (%s): no ESI service, the volume of %d type(s) is unknown",
                            hub, len(normalized_type_ids))
            return {}

        hub_context = self._market_hub_context(hub)
        normalized_hub = str(hub_context["hub"])
        region_id = int(hub_context["region_id"])

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
        skipped_rows = 0
        skipped_type_ids: set[int] = set()

        history_reason = getattr(esi_service, "market_history_unavailable_reason", None)
        failed_type_ids: list[int] = []
        for index, type_id in enumerate(missing_type_ids, start=1):
            if not isinstance(history_by_type_id, dict) or int(type_id) not in history_by_type_id:
                # The fetch failed (ESIService leaves the type out): unknown, not a volume of 0.
                reason = history_reason(int(type_id), region_id=region_id) if history_reason is not None else None
                unavailable[int(type_id)] = reason or "ESI market history not returned: region daily volume unknown"
                failed_type_ids.append(int(type_id))
                if progress_callback is not None:
                    progress_callback(
                        index / max(1, total_ids),
                        f"Fetched {normalized_hub.title()} region history",
                        {"completed": index, "total": total_ids},
                    )
                continue
            # A day without a readable volume is skipped (counted), not read as 0.
            history_rows: list[dict[str, Any]] = []
            for row in history_by_type_id.get(int(type_id)) or []:
                if isinstance(row, dict) and readable_int(row.get("volume")) is not None:
                    history_rows.append(row)
                else:
                    skipped_rows += 1
                    skipped_type_ids.add(int(type_id))
            latest_row = history_rows[-1] if history_rows else {}
            trailing_rows = history_rows[-7:] if history_rows else []
            trailing_volumes = [int(readable_int(row.get("volume")) or 0) for row in trailing_rows]
            trailing_average_volume = (
                float(sum(trailing_volumes)) / float(len(trailing_volumes))
                if trailing_volumes
                else 0.0
            )
            fetched_results[int(type_id)] = {
                "hub": normalized_hub,
                "hub_label": str(hub_context.get("label") or normalized_hub.title()),
                "region_id": region_id,
                "daily_volume": int(readable_int(latest_row.get("volume")) or 0) if latest_row else 0,
                "daily_volume_7d_avg": trailing_average_volume,
                "daily_volume_7d_sample_size": len(trailing_volumes),
                "daily_order_count": int(readable_int(latest_row.get("order_count")) or 0) if latest_row else 0,
                "daily_volume_date": latest_row.get("date") if latest_row else None,
            }
            if progress_callback is not None:
                progress_callback(
                    index / max(1, total_ids),
                    f"Fetched {normalized_hub.title()} region history",
                    {"completed": index, "total": total_ids},
                )

        if failed_type_ids:
            logging.warning(
                "Region daily volume (%s): the market history of %d type(s) could not be fetched, their volume is "
                "unknown: %s",
                normalized_hub,
                len(failed_type_ids),
                sorted(failed_type_ids)[:20],
            )
        if skipped_rows:
            logging.warning(
                "Region daily volume (%s): skipped %d unreadable history row(s) (no readable volume) of %d type(s): %s",
                normalized_hub,
                skipped_rows,
                len(skipped_type_ids),
                sorted(skipped_type_ids)[:20],
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
