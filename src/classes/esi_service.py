from __future__ import annotations

import time
from collections import defaultdict
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
    ):
        self._esi_client = esi_client
        self._region_id = region_id
        self._station_id = station_id

        self._type_orders_cache_ttl_seconds = type_orders_cache_ttl_seconds
        self._market_prices_cache_ttl_seconds = market_prices_cache_ttl_seconds

        # { (order_type, region_id, type_id): (timestamp, [orders]) }
        self._type_orders_cache: Dict[tuple, tuple] = {}
        # (timestamp, [prices])
        self._market_prices_cache: Optional[tuple] = None

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

    def get_public_structures(self, system_id: int, filter: str = "manufacturing_basic") -> List[Dict[str, Any]]:
        if not system_id or not isinstance(system_id, int):
            raise ValueError("System ID is required to fetch public structures.")
        if filter is not None and filter not in ("manufacturing_basic", "market"):
            raise ValueError("Invalid filter value. Must be one of: None, manufacturing_basic, market.")

        try:
            public_structure_ids = self._esi_client.esi_get("/universe/structures/", params={"filter": filter})
            public_structures_in_system: List[Dict[str, Any]] = []
            for structure_id in public_structure_ids:
                structure_data = self._esi_client.esi_get(f"/universe/structures/{structure_id}/")
                if isinstance(structure_data, dict) and structure_data.get("solar_system_id") == system_id:
                    public_structures_in_system.append(structure_data)
            return public_structures_in_system
        except Exception as e:
            raise RuntimeError(f"ESI request failed for system {system_id}: {e}")