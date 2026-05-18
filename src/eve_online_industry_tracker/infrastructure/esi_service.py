from __future__ import annotations

import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import requests
from typing import Any, Dict, Iterable, List, Optional, Union

from eve_online_industry_tracker.infrastructure.esi_client import ESIClient
from utils.requests_ssl import get_requests_ssl_kwargs

try:
    from utils.esi_monitor import get_esi_monitor
except Exception:  # pragma: no cover
    get_esi_monitor = None  # type: ignore


class ESIService:
    """Higher-level ESI operations with lightweight caching.

    This class replaces the former module-level ESI adapter functions.
    """

    DEFAULT_REGION_ID = 10000002
    DEFAULT_STATION_ID = 60003760
    _PUBLIC_ESI_RETRYABLE_STATUS_CODES = {420, 429, 500, 502, 503, 504}
    _PUBLIC_MARKET_ORDER_MAX_WORKERS = 10
    _PUBLIC_MARKET_HISTORY_MAX_WORKERS = 10

    def __init__(
        self,
        esi_client: ESIClient,
        region_id: int | None = None,
        station_id: int | None = None,
        type_orders_cache_ttl_seconds: int = 300,
        market_prices_cache_ttl_seconds: int = 3600,
        public_structures_cache_ttl_seconds: int = 600,
        industry_facilities_cache_ttl_seconds: int = 6 * 3600,
        market_history_cache_ttl_seconds: int = 6 * 3600,
    ):
        self._esi_client = esi_client
        self._region_id = region_id if region_id is not None else self.DEFAULT_REGION_ID
        self._station_id = station_id if station_id is not None else self.DEFAULT_STATION_ID

        self._type_orders_cache_ttl_seconds = type_orders_cache_ttl_seconds
        self._market_prices_cache_ttl_seconds = market_prices_cache_ttl_seconds
        self._public_structures_cache_ttl_seconds = public_structures_cache_ttl_seconds
        self._industry_facilities_cache_ttl_seconds = industry_facilities_cache_ttl_seconds
        self._market_history_cache_ttl_seconds = market_history_cache_ttl_seconds

        # Persistent HTTP session for connection pooling (TCP + TLS reuse).
        self._http_session = requests.Session()
        self._http_session.headers.update(self._public_headers())
        ssl_kwargs = get_requests_ssl_kwargs()
        if "verify" in ssl_kwargs:
            self._http_session.verify = ssl_kwargs["verify"]

        # Optional admin settings manager (set after construction).
        self._admin_settings = None

        # { (order_type, region_id, type_id): (timestamp, [orders]) }
        self._type_orders_cache: Dict[tuple, tuple] = {}
        # Legacy: kept for backwards compatibility; we no longer fetch full region order books.
        self._region_orders_cache: Dict[tuple, tuple] = {}
        # { (region_id, type_id): (timestamp, [history_rows]) }
        self._market_history_cache: Dict[tuple, tuple] = {}
        # (timestamp, [prices])
        self._market_prices_cache: Optional[tuple] = None
        # { (system_id, filter): (timestamp, [structures]) }
        self._public_structures_cache: Dict[tuple, tuple] = {}
        # (timestamp, [facilities])
        self._industry_facilities_cache: Optional[tuple] = None

    def set_market_context(self, *, region_id: Optional[int] = None, station_id: Optional[int] = None) -> None:
        if region_id is not None:
            self._region_id = region_id
        if station_id is not None:
            self._station_id = station_id

    def _validate_type_ids(self, type_ids: Iterable[int]) -> List[int]:
        if type_ids is None:
            return []
        normalized = list(type_ids)
        for type_id in normalized:
            if not isinstance(type_id, int) or type_id <= 0:
                raise ValueError(f"Invalid type_id: {type_id}")
        return normalized

    def _public_headers(self) -> Dict[str, str]:
        return {
            "Accept": getattr(self._esi_client, "esi_header_accept", "application/json"),
            "Accept-Language": getattr(self._esi_client, "esi_header_acceptlanguage", "en"),
            "User-Agent": getattr(self._esi_client, "user_agent", "eve-online-industry-tracker"),
            "X-Compatibility-Date": getattr(self._esi_client, "esi_header_xcompatibilitydate", "2025-08-26"),
            "X-Tenant": getattr(self._esi_client, "esi_header_xtenant", "tranquility"),
        }

    @staticmethod
    def _retry_after_seconds(response: requests.Response | None) -> float:
        if response is None:
            return 0.0
        try:
            raw_value = response.headers.get("Retry-After")
        except Exception:
            return 0.0
        if raw_value in {None, ""}:
            return 0.0
        try:
            return max(0.0, float(raw_value))
        except Exception:
            return 0.0

    def _public_esi_get(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        paginate: bool = False,
        timeout_seconds: float = 15.0,
        max_retries: int = 4,
    ) -> Any:
        base_uri = str(getattr(self._esi_client, "esi_base_uri", "https://esi.evetech.net")).rstrip("/")

        def issue_request(*, request_params: Optional[Dict[str, Any]] = None) -> requests.Response:
            attempts = 0
            url = f"{base_uri}{endpoint}"
            page = None
            if isinstance(request_params, dict):
                try:
                    raw_page = request_params.get("page")
                    page = int(raw_page) if raw_page is not None else None
                except Exception:
                    page = None
            while True:
                request_started_at = time.time()
                try:
                    response = self._http_session.get(
                        url,
                        params=request_params,
                        timeout=float(timeout_seconds),
                    )
                    try:
                        if get_esi_monitor is not None:
                            get_esi_monitor().record_http_attempt(
                                method="GET",
                                endpoint=str(endpoint),
                                url=str(getattr(response, "url", url)),
                                status_code=int(response.status_code) if response is not None else None,
                                elapsed_ms=(time.time() - request_started_at) * 1000.0,
                                headers=getattr(response, "headers", None),
                                exception=None,
                                cache_mode="off",
                                page=page,
                            )
                    except Exception:
                        pass
                except requests.RequestException:
                    exc = None
                    try:
                        raise
                    except requests.RequestException as caught:
                        exc = caught
                        try:
                            if get_esi_monitor is not None:
                                get_esi_monitor().record_http_attempt(
                                    method="GET",
                                    endpoint=str(endpoint),
                                    url=str(url),
                                    status_code=None,
                                    elapsed_ms=(time.time() - request_started_at) * 1000.0,
                                    headers=None,
                                    exception=caught,
                                    cache_mode="off",
                                    page=page,
                                )
                        except Exception:
                            pass
                    if exc is None:
                        raise
                    attempts += 1
                    if attempts > max_retries:
                        raise exc
                    wait_seconds = min(8.0, float(2 ** attempts) + random.uniform(0.0, 0.5))
                    try:
                        if get_esi_monitor is not None:
                            get_esi_monitor().record_retry_event(
                                reason=type(exc).__name__,
                                sleep_seconds=float(wait_seconds),
                                method="GET",
                                endpoint=str(endpoint),
                                url=str(url),
                            )
                    except Exception:
                        pass
                    time.sleep(wait_seconds)
                    continue

                if response.status_code not in self._PUBLIC_ESI_RETRYABLE_STATUS_CODES:
                    response.raise_for_status()
                    return response

                attempts += 1
                if attempts > max_retries:
                    response.raise_for_status()
                retry_after = self._retry_after_seconds(response)
                backoff = min(12.0, float(2 ** attempts) + random.uniform(0.0, 0.5))
                wait_seconds = max(retry_after, backoff)
                try:
                    if get_esi_monitor is not None:
                        get_esi_monitor().record_retry_event(
                            reason=str(response.status_code),
                            sleep_seconds=float(wait_seconds),
                            method="GET",
                            endpoint=str(endpoint),
                            url=str(getattr(response, "url", url)),
                        )
                except Exception:
                    pass
                time.sleep(wait_seconds)

        if not paginate:
            response = issue_request(request_params=params)
            return response.json()

        all_data: List[Dict[str, Any]] = []
        page = 1
        while True:
            paged_params = dict(params or {})
            paged_params["page"] = page
            response = issue_request(request_params=paged_params)

            payload = response.json()
            if isinstance(payload, list):
                all_data.extend(payload)
            else:
                all_data.append(payload)

            total_pages = int(response.headers.get("X-Pages", "1"))
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.1 if self._admin_settings is None else self._admin_settings.get("performance", "esi_pagination_sleep_seconds"))

        return all_data

    def _fetch_region_orders(self, type_ids: Iterable[int], *, region_id: int, order_type: str) -> List[Dict[str, Any]]:
        type_ids_list = self._validate_type_ids(type_ids)
        if not type_ids_list:
            return []

        now = time.time()
        all_orders: List[Dict[str, Any]] = []

        # Collect cached results first and fetch missing type_ids.
        to_fetch: List[int] = []
        for type_id in type_ids_list:
            cache_key = (order_type, region_id, type_id)
            cached = self._type_orders_cache.get(cache_key)
            if cached and (now - cached[0] < self._type_orders_cache_ttl_seconds):
                all_orders.extend(cached[1])
            else:
                to_fetch.append(int(type_id))

        if not to_fetch:
            return all_orders

        # ESI supports filtering region orders by type_id; use that to avoid
        # downloading the entire region order book (which can be huge).
        to_fetch = sorted(set(to_fetch))

        def _fetch_one(type_id: int) -> tuple[int, List[Dict[str, Any]]]:
            # Small jitter to avoid spiky bursts against ESI.
            try:
                time.sleep(random.uniform(0.0, 0.05))
            except Exception:
                pass

            try:
                orders = self._public_esi_get(
                    f"/markets/{region_id}/orders/",
                    params={"order_type": order_type, "type_id": int(type_id)},
                    paginate=True,
                    timeout_seconds=15.0,
                )
            except Exception as e:
                logging.warning(
                    "ESI market orders fetch failed (order_type=%s, region_id=%s, type_id=%s): %s",
                    order_type,
                    region_id,
                    type_id,
                    e,
                )
                orders = []

            if not isinstance(orders, list):
                return int(type_id), []

            # Defensive filter by is_buy_order.
            if order_type == "sell":
                orders = [
                    o for o in orders if isinstance(o, dict) and o.get("is_buy_order") is False
                ]
            elif order_type == "buy":
                orders = [
                    o for o in orders if isinstance(o, dict) and o.get("is_buy_order") is True
                ]

            return int(type_id), orders

        max_workers = min(self._PUBLIC_MARKET_ORDER_MAX_WORKERS if self._admin_settings is None else self._admin_settings.get("performance", "market_order_max_workers"), max(1, len(to_fetch)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_fetch_one, int(type_id)) for type_id in to_fetch]
            for fut in as_completed(futures):
                try:
                    type_id, orders = fut.result()
                except Exception as e:
                    logging.warning(
                        "ESI market orders fetch task failed (order_type=%s, region_id=%s): %s",
                        order_type,
                        region_id,
                        e,
                    )
                    continue
                cache_key = (order_type, region_id, int(type_id))
                self._type_orders_cache[cache_key] = (time.time(), orders)
                all_orders.extend(orders)

        return all_orders

    def get_sell_order_book(self, type_ids: Iterable[int], region_id: Optional[int] = None) -> Dict[int, List[Dict[str, Any]]]:
        region_id = region_id or self._region_id
        orders = self._fetch_region_orders(type_ids, region_id=region_id, order_type="sell")

        type_ids_list = self._validate_type_ids(type_ids)
        target = set(type_ids_list)
        book: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

        for order in orders:
            tid = order.get("type_id")
            if tid not in target:
                continue
            book[tid].append(
                {
                    "price": order.get("price"),
                    "volume_remain": order.get("volume_remain", 0),
                    "min_volume": order.get("min_volume", 1),
                    "order_id": order.get("order_id"),
                    "location_id": order.get("location_id"),
                }
            )

        for tid in book:
            book[tid].sort(key=lambda r: r["price"])
        return dict(book)

    def get_buy_order_book(self, type_ids: Iterable[int], region_id: Optional[int] = None) -> Dict[int, List[Dict[str, Any]]]:
        region_id = region_id or self._region_id
        orders = self._fetch_region_orders(type_ids, region_id=region_id, order_type="buy")

        type_ids_list = self._validate_type_ids(type_ids)
        target = set(type_ids_list)
        book: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

        for order in orders:
            tid = order.get("type_id")
            if tid not in target:
                continue
            book[tid].append(
                {
                    "price": order.get("price"),
                    "volume_remain": order.get("volume_remain", 0),
                    "min_volume": order.get("min_volume", 1),
                    "order_id": order.get("order_id"),
                    "location_id": order.get("location_id"),
                }
            )

        for tid in book:
            book[tid].sort(key=lambda r: r["price"])
        return dict(book)

    def get_ore_prices(self, ore_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        if not isinstance(ore_ids, list) or not ore_ids:
            return {}
        return self.get_sell_order_book(ore_ids)

    def get_material_prices(self, material_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        if not isinstance(material_ids, list) or not material_ids:
            return {}
        return self.get_sell_order_book(material_ids)

    def get_sell_order_book_metadata(self, type_ids: Iterable[int], region_id: Optional[int] = None) -> Dict[str, Any]:
        region_id = region_id or self._region_id
        type_ids_list = self._validate_type_ids(type_ids)
        timestamps: List[float] = []
        cached_type_count = 0
        total_orders = 0

        for type_id in type_ids_list:
            cached = self._type_orders_cache.get(("sell", region_id, int(type_id)))
            if not cached:
                continue
            cached_type_count += 1
            timestamps.append(float(cached[0]))
            orders = cached[1] if isinstance(cached[1], list) else []
            total_orders += len(orders)

        return {
            "region_id": int(region_id),
            "order_type": "sell",
            "type_count": len(type_ids_list),
            "cached_type_count": cached_type_count,
            "total_orders": total_orders,
            "cache_ttl_seconds": int(self._type_orders_cache_ttl_seconds),
            "oldest_fetched_at": min(timestamps) if timestamps else None,
            "newest_fetched_at": max(timestamps) if timestamps else None,
        }

    def get_type_sellprices(self, type_ids: List[int], region_id: Optional[int] = None) -> Dict[int, List[Dict[str, Any]]]:
        if not isinstance(type_ids, list) or not type_ids:
            return {}
        return self.get_sell_order_book(type_ids, region_id=region_id)

    def get_type_buyprices(self, type_ids: List[int], region_id: Optional[int] = None) -> Dict[int, List[Dict[str, Any]]]:
        if not isinstance(type_ids, list) or not type_ids:
            return {}
        return self.get_buy_order_book(type_ids, region_id=region_id)

    def get_market_history(self, type_ids: Iterable[int], region_id: Optional[int] = None) -> Dict[int, List[Dict[str, Any]]]:
        region_id = region_id or self._region_id
        type_ids_list = self._validate_type_ids(type_ids)
        if not type_ids_list:
            return {}

        now = time.time()
        result: Dict[int, List[Dict[str, Any]]] = {}
        to_fetch: List[int] = []

        for type_id in type_ids_list:
            cache_key = (int(region_id), int(type_id))
            cached = self._market_history_cache.get(cache_key)
            if cached and (now - cached[0] < self._market_history_cache_ttl_seconds):
                result[int(type_id)] = cached[1] if isinstance(cached[1], list) else []
            else:
                to_fetch.append(int(type_id))

        if not to_fetch:
            return result

        def _fetch_one(type_id: int) -> tuple[int, List[Dict[str, Any]]]:
            try:
                payload = self._public_esi_get(
                    f"/markets/{int(region_id)}/history/",
                    params={"type_id": int(type_id)},
                    paginate=False,
                    timeout_seconds=15.0,
                )
            except Exception as e:
                logging.warning(
                    "ESI market history fetch failed (region_id=%s, type_id=%s): %s",
                    region_id,
                    type_id,
                    e,
                )
                payload = []

            if not isinstance(payload, list):
                return int(type_id), []

            rows = [row for row in payload if isinstance(row, dict)]
            rows.sort(key=lambda row: str(row.get("date") or ""))
            return int(type_id), rows

        max_workers = min(self._PUBLIC_MARKET_HISTORY_MAX_WORKERS if self._admin_settings is None else self._admin_settings.get("performance", "market_history_max_workers"), max(1, len(to_fetch)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_fetch_one, int(type_id)) for type_id in to_fetch]
            for fut in as_completed(futures):
                try:
                    type_id, rows = fut.result()
                except Exception as e:
                    logging.warning(
                        "ESI market history fetch task failed (region_id=%s): %s",
                        region_id,
                        e,
                    )
                    continue
                self._market_history_cache[(int(region_id), int(type_id))] = (time.time(), rows)
                result[int(type_id)] = rows

        return result

    def get_location_info(
        self,
        location_id: Union[int, List[int]],
        *,
        suppress_forbidden_log: bool = False,
        suppress_not_found_log: bool = False,
    ) -> Union[Dict[str, Any], Dict[int, Dict[str, Any]]]:
        if isinstance(location_id, list):
            return {
                loc: self.get_location_info(
                    loc,
                    suppress_forbidden_log=suppress_forbidden_log,
                    suppress_not_found_log=suppress_not_found_log,
                )
                for loc in location_id
            }

        if not location_id or not isinstance(location_id, int):
            return {}

        id_type = self._esi_client.get_id_type(location_id)
        try:
            if id_type == "station":
                return self._public_esi_get(f"/universe/stations/{location_id}/")
            if id_type == "structure":
                return self._esi_client.esi_get(
                    f"/universe/structures/{location_id}/",
                    suppress_forbidden_log=suppress_forbidden_log,
                    suppress_not_found_log=suppress_not_found_log,
                )
            if id_type == "region":
                return self._public_esi_get(f"/universe/regions/{location_id}/")
            if id_type == "constellation":
                return self._public_esi_get(f"/universe/constellations/{location_id}/")
            if id_type == "solar_system":
                return self._public_esi_get(f"/universe/systems/{location_id}/")
            return {}
        except Exception as e:
            raise RuntimeError(f"ESI request failed for location {location_id}: {e}")

    def get_universe_structure(
        self,
        structure_id: int,
        *,
        timeout_seconds: float = 15.0,
        suppress_forbidden_log: bool = False,
        suppress_not_found_log: bool = False,
    ) -> Dict[str, Any]:
        """Return /universe/structures/{structure_id}/.

        This endpoint is gated by ACLs (requires auth). Callers should use this
        helper instead of reaching into `_esi_client`.
        """

        if not isinstance(structure_id, int) or structure_id <= 0:
            raise ValueError(f"Invalid structure_id: {structure_id}")

        try:
            return self._esi_client.esi_get(
                f"/universe/structures/{structure_id}/",
                use_cache=False,
                timeout_seconds=float(timeout_seconds),
                suppress_forbidden_log=suppress_forbidden_log,
                suppress_not_found_log=suppress_not_found_log,
            )
        except Exception as e:
            raise RuntimeError(f"ESI request failed for universe structure {structure_id}: {e}")

    def get_market_prices(self) -> List[Dict[str, Any]]:
        """Return market prices for all items (cached)."""
        now = time.time()

        if self._market_prices_cache and (now - self._market_prices_cache[0] < self._market_prices_cache_ttl_seconds):
            return self._market_prices_cache[1]

        try:
            market_prices = self._esi_client.esi_get("/markets/prices/", paginate=True)
        except Exception as e:
            raise RuntimeError(f"ESI request failed: {e}")

        self._market_prices_cache = (now, market_prices)
        return market_prices

    def get_industry_facilities(self) -> List[Dict[str, Any]]:
        """Return public industry facilities.

        This endpoint is much more suitable for quickly locating manufacturing
        structures per solar system than scanning /universe/structures.
        """
        now = time.time()
        if self._industry_facilities_cache and (now - self._industry_facilities_cache[0] < self._industry_facilities_cache_ttl_seconds):
            return self._industry_facilities_cache[1]

        try:
            data = self._esi_client.esi_get("/industry/facilities/")
        except Exception as e:
            raise RuntimeError(f"ESI request failed: {e}")

        out = list(data) if isinstance(data, list) else []
        self._industry_facilities_cache = (now, out)
        return out

    def get_industry_systems(self) -> List[Dict[str, Any]]:
        """Return industry system cost indices.

        Cached in-memory because the payload can be sizable.
        """
        now = time.time()
        cache = getattr(self, "_industry_systems_cache", None)
        ttl = getattr(self, "_industry_systems_cache_ttl_seconds", 3600)
        if cache and (now - cache[0] < ttl):
            return cache[1]

        try:
            data = self._esi_client.esi_get("/industry/systems/", paginate=True)
        except Exception as e:
            raise RuntimeError(f"ESI request failed: {e}")

        out = list(data) if isinstance(data, list) else []
        self._industry_systems_cache = (now, out)
        self._industry_systems_cache_ttl_seconds = ttl
        return out

    def get_universe_type(self, type_id: int) -> Dict[str, Any]:
        """Return /universe/types/{type_id}/ (cached).

        This is a public endpoint but is useful for retrieving dogma attributes
        for items such as structure rigs.
        """
        if not isinstance(type_id, int) or type_id <= 0:
            raise ValueError(f"Invalid type_id: {type_id}")

        now = time.time()
        cache: Dict[int, tuple] = getattr(self, "_universe_type_cache", {})
        ttl = getattr(self, "_universe_type_cache_ttl_seconds", 24 * 3600)

        cached = cache.get(type_id)
        if cached and (now - cached[0] < ttl):
            return cached[1]

        try:
            data = self._esi_client.esi_get(f"/universe/types/{type_id}/")
        except Exception as e:
            raise RuntimeError(f"ESI request failed for /universe/types/{type_id}/: {e}")

        out = dict(data) if isinstance(data, dict) else {}
        cache[type_id] = (now, out)
        self._universe_type_cache = cache
        self._universe_type_cache_ttl_seconds = ttl
        return out

    def get_universe_names(self, ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
        """Resolve a list of IDs to names via POST /universe/names/.

        Returns {id: {"name": str, "category": str}} for resolved IDs.
        Cached in-memory because this is often called for the same corp IDs.
        """
        id_list = [int(x) for x in (ids or []) if x is not None and int(x) > 0]
        if not id_list:
            return {}

        now = time.time()
        cache: Dict[int, tuple] = getattr(self, "_universe_names_cache", {})
        ttl = getattr(self, "_universe_names_cache_ttl_seconds", 24 * 3600)

        missing: list[int] = []
        result: Dict[int, Dict[str, Any]] = {}
        for _id in id_list:
            cached = cache.get(int(_id))
            if cached and (now - cached[0] < ttl):
                result[int(_id)] = cached[1]
            else:
                missing.append(int(_id))

        # ESI limit is reasonably high, but we chunk to be safe.
        chunk_size = 500
        for i in range(0, len(missing), chunk_size):
            chunk = missing[i : i + chunk_size]
            try:
                data = self._esi_client.esi_post("/universe/names/", json=chunk, use_cache=False)
            except Exception:
                data = None

            rows = list(data) if isinstance(data, list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                rid = row.get("id")
                name = row.get("name")
                category = row.get("category")
                if rid is None or not name:
                    continue
                payload = {"name": str(name), "category": str(category) if category is not None else ""}
                cache[int(rid)] = (now, payload)
                result[int(rid)] = payload

        self._universe_names_cache = cache
        self._universe_names_cache_ttl_seconds = ttl
        return result

    def resolve_universe_names(self, ids: List[int]) -> Dict[int, str]:
        """Resolve universe IDs to names using /universe/names/ (batched)."""
        if not ids:
            return {}

        out: Dict[int, str] = {}
        # Keep payload sizes reasonable.
        chunk_size = 500
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i : i + chunk_size]
            try:
                resp = self._esi_client.esi_post("/universe/names/", json=chunk, timeout=15)
            except Exception:
                continue

            if not isinstance(resp, list):
                continue
            for item in resp:
                if not isinstance(item, dict):
                    continue
                item_id = item.get("id")
                name = item.get("name")
                if isinstance(item_id, int) and isinstance(name, str):
                    out[item_id] = name

        return out


    def list_universe_structure_ids(self, *, filter: Optional[str] = None) -> List[int]:
        """List structure IDs from /universe/structures.

        Note: ESI returns only structures your character has access to.
        """
        if filter is not None and filter not in ("manufacturing_basic", "market"):
            raise ValueError("Invalid filter value. Must be one of: None, manufacturing_basic, market.")

        params = {"filter": filter} if filter is not None else None
        data = self._esi_client.esi_get("/universe/structures/", params=params, use_cache=False)
        if data is None:
            raise RuntimeError(
                "ESI returned no data for /universe/structures (likely 403 Forbidden). "
                "Your character may be missing the scope 'esi-universe.read_structures.v1' or has no access."
            )
        if not isinstance(data, list):
            return []
        out = []
        for x in data:
            try:
                out.append(int(x))
            except Exception:
                continue
        return out

    def get_public_structures(
        self,
        system_id: int,
        filter: Optional[str] = "manufacturing_basic",
        *,
        max_structure_ids_to_scan: int = 250,
        max_workers: int = 20,
        time_budget_seconds: float = 8.0,
        max_results: int = 50,
    ) -> List[Dict[str, Any]]:
        if not system_id or not isinstance(system_id, int):
            raise ValueError("System ID is required to fetch public structures.")
        if filter is not None and filter not in ("manufacturing_basic", "market"):
            raise ValueError("Invalid filter value. Must be one of: None, manufacturing_basic, market.")

        cache_key = (system_id, filter)
        now = time.time()
        cached = self._public_structures_cache.get(cache_key)
        if cached and (now - cached[0] < self._public_structures_cache_ttl_seconds):
            return cached[1]

        started_at = time.time()

        try:
            params = {"filter": filter} if filter is not None else None
            public_structure_ids = self._esi_client.esi_get("/universe/structures/", params=params)

            # ESIClient returns None on 403/404; distinguish that from an empty list.
            if public_structure_ids is None:
                raise RuntimeError(
                    "ESI returned no data for /universe/structures (likely 403 Forbidden). "
                    "Your character may be missing the scope 'esi-universe.read_structures.v1' or has no access."
                )

            if not public_structure_ids:
                self._public_structures_cache[cache_key] = (now, [])
                return []

            # This endpoint can return a *lot* of IDs. Scanning all of them is not viable
            # for an interactive UI; instead we scan a bounded subset concurrently and
            # enforce a time budget so the request returns promptly.
            #
            # Important: don't only take the first N IDs; that biases results and can
            # systematically miss systems. Use a sample across the full list.
            all_ids = list(public_structure_ids)
            scan_cap = max(0, int(max_structure_ids_to_scan))
            if scan_cap <= 0:
                self._public_structures_cache[cache_key] = (now, [])
                return []
            if len(all_ids) <= scan_cap:
                ids_to_scan = all_ids
            else:
                # Seed by system_id so repeated refreshes for the same system are stable-ish,
                # but still provide coverage across the global ID list.
                rng = random.Random(system_id)
                ids_to_scan = rng.sample(all_ids, scan_cap)

            def fetch_one(structure_id: int) -> Optional[Dict[str, Any]]:
                try:
                    # Disable DB-backed caching here; ESIClient cache uses a shared SQLAlchemy
                    # session which is not thread-safe under ThreadPoolExecutor.
                    structure_data = self.get_universe_structure(
                        int(structure_id),
                        suppress_forbidden_log=True,
                        suppress_not_found_log=True,
                    )
                    if isinstance(structure_data, dict) and structure_data.get("solar_system_id") == system_id:
                        # Normalize keys to match NPC stations output shape.
                        return {
                            "station_id": structure_id,
                            "station_name": structure_data.get("name"),
                            "system_id": structure_data.get("solar_system_id"),
                            "owner_id": structure_data.get("owner_id"),
                            "type_id": structure_data.get("type_id"),
                            "services": structure_data.get("services"),
                        }
                except Exception:
                    return None
                return None

            results: List[Dict[str, Any]] = []
            max_workers = max(1, int(max_workers))
            max_results = max(1, int(max_results))

            # Submit progressively so large scan caps don't create thousands of in-flight requests.
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                it = iter(ids_to_scan)
                in_flight = set()

                # Fill initial workers.
                for _ in range(max_workers):
                    try:
                        sid = next(it)
                    except StopIteration:
                        break
                    in_flight.add(executor.submit(fetch_one, sid))

                while in_flight:
                    remaining = time_budget_seconds - (time.time() - started_at)
                    if remaining <= 0:
                        break

                    done, in_flight = wait(in_flight, timeout=min(0.25, remaining), return_when=FIRST_COMPLETED)
                    for fut in done:
                        try:
                            item = fut.result()
                        except Exception:
                            item = None
                        if item:
                            results.append(item)
                            if len(results) >= max_results:
                                in_flight.clear()
                                break

                        # Keep the pool full.
                        if len(results) < max_results and (time.time() - started_at) <= time_budget_seconds:
                            try:
                                sid = next(it)
                            except StopIteration:
                                continue
                            in_flight.add(executor.submit(fetch_one, sid))

            self._public_structures_cache[cache_key] = (now, results)
            return results
        except Exception as e:
            raise RuntimeError(f"ESI request failed for system {system_id}: {e}")