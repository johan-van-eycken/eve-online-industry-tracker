from __future__ import annotations

import time
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import random
from typing import Any, Dict, Iterable, List, Optional, Union

from classes.esi import ESIClient


class ESIService:
    """Higher-level ESI operations with lightweight caching.

    This class replaces the former module-level ESI adapter functions.
    """

    DEFAULT_REGION_ID = 10000002
    DEFAULT_STATION_ID = 60003760

    def __init__(
        self,
        esi_client: ESIClient,
        region_id: int = DEFAULT_REGION_ID,
        station_id: int = DEFAULT_STATION_ID,
        type_orders_cache_ttl_seconds: int = 300,
        market_prices_cache_ttl_seconds: int = 3600,
        public_structures_cache_ttl_seconds: int = 600,
        industry_facilities_cache_ttl_seconds: int = 6 * 3600,
    ):
        self._esi_client = esi_client
        self._region_id = region_id
        self._station_id = station_id

        self._type_orders_cache_ttl_seconds = type_orders_cache_ttl_seconds
        self._market_prices_cache_ttl_seconds = market_prices_cache_ttl_seconds
        self._public_structures_cache_ttl_seconds = public_structures_cache_ttl_seconds
        self._industry_facilities_cache_ttl_seconds = industry_facilities_cache_ttl_seconds

        # { (order_type, region_id, type_id): (timestamp, [orders]) }
        self._type_orders_cache: Dict[tuple, tuple] = {}
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

    def _fetch_region_orders(self, type_ids: Iterable[int], *, region_id: int, order_type: str) -> List[Dict[str, Any]]:
        type_ids_list = self._validate_type_ids(type_ids)
        if not type_ids_list:
            return []

        now = time.time()
        all_orders: List[Dict[str, Any]] = []

        for type_id in type_ids_list:
            cache_key = (order_type, region_id, type_id)
            cached = self._type_orders_cache.get(cache_key)
            if cached and (now - cached[0] < self._type_orders_cache_ttl_seconds):
                all_orders.extend(cached[1])
                continue

            try:
                data = self._esi_client.esi_get(
                    f"/markets/{region_id}/orders",
                    params={"order_type": order_type, "type_id": type_id},
                    paginate=True,
                )
            except Exception as e:
                raise RuntimeError(f"ESI request failed (type_id={type_id}, order_type={order_type}): {e}")

            # Defensive filter by is_buy_order.
            if order_type == "sell":
                collected = [order for order in data if order.get("is_buy_order") is False]
            elif order_type == "buy":
                collected = [order for order in data if order.get("is_buy_order") is True]
            else:
                collected = list(data) if isinstance(data, list) else []

            self._type_orders_cache[cache_key] = (now, collected)
            all_orders.extend(collected)

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

    def get_type_sellprices(self, type_ids: List[int], region_id: Optional[int] = None) -> Dict[int, List[Dict[str, Any]]]:
        if not isinstance(type_ids, list) or not type_ids:
            return {}
        return self.get_sell_order_book(type_ids, region_id=region_id)

    def get_type_buyprices(self, type_ids: List[int], region_id: Optional[int] = None) -> Dict[int, List[Dict[str, Any]]]:
        if not isinstance(type_ids, list) or not type_ids:
            return {}
        return self.get_buy_order_book(type_ids, region_id=region_id)

    def get_location_info(self, location_id: Union[int, List[int]]) -> Union[Dict[str, Any], Dict[int, Dict[str, Any]]]:
        if isinstance(location_id, list):
            return {loc: self.get_location_info(loc) for loc in location_id}

        if not location_id or not isinstance(location_id, int):
            return {}

        id_type = self._esi_client.get_id_type(location_id)
        try:
            if id_type == "station":
                return self._esi_client.esi_get(f"/universe/stations/{location_id}/")
            if id_type == "structure":
                return self._esi_client.esi_get(f"/universe/structures/{location_id}/")
            if id_type == "region":
                return self._esi_client.esi_get(f"/universe/regions/{location_id}/")
            if id_type == "constellation":
                return self._esi_client.esi_get(f"/universe/constellations/{location_id}/")
            if id_type == "solar_system":
                return self._esi_client.esi_get(f"/universe/systems/{location_id}/")
            return {}
        except Exception as e:
            raise RuntimeError(f"ESI request failed for location {location_id}: {e}")

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
                    structure_data = self._esi_client.esi_get(
                        f"/universe/structures/{structure_id}/",
                        use_cache=False,
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