"""
Adapter for retrieving ESI data from API calls.
"""
import logging
import time
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

_esi_client = None

# Default The Forge: Jita IV - Moon 4 - Caldari Navy Assembly Plant
_region_id = 10000002
_station_id = 60003760

# Cache structures
_TYPE_SELLORDERS_CACHE = {}          # { type_id: (timestamp, [orders]) }
_TYPE_SELLORDERS_CACHE_TTL = 300     # seconds (5 minutes)
_TYPE_BUYORDERS_CACHE = {}           # { type_id: (timestamp, [orders]) }
_TYPE_BUYORDERS_CACHE_TTL = 300      # seconds (5 minutes)
_MARKET_PRICES_CACHE = None          # (timestamp, [prices])
_MARKET_PRICES_CACHE_TTL = 3600      # seconds (1 hour)

# Default The Forge: Jita IV - Moon 4 - Caldari Navy Assembly Plant
def esi_adapter(character, region_id=_region_id, station_id=_station_id):
    global _esi_client, _station_id, _region_id
    _esi_client = character.esi_client
    _station_id = station_id
    _region_id = region_id

def _ensure():
    if not _esi_client or _esi_client is None:
        raise RuntimeError("ESI client not initialized. Call esi_adapter(character) first.")
    if not _region_id or _region_id is None:
        raise RuntimeError("Region ID not set.")
    if not _station_id or _station_id is None:
        raise RuntimeError("Station ID not set.")

def _fetch_region_sell_orders(type_ids, region_id=_region_id):
    _ensure()

    if not type_ids:
        return []

    now = time.time()
    all_orders = []

    for type_id in type_ids:
        if not isinstance(type_id, int) or type_id <= 0:
            raise ValueError(f"Invalid type_id: {type_id}")

        # Cache hit?
        cached = _TYPE_SELLORDERS_CACHE.get(type_id)
        if cached and (now - cached[0] < _TYPE_SELLORDERS_CACHE_TTL):
            all_orders.extend(cached[1])
            continue

        # Fetch all pages for this type_id using built-in pagination
        try:
            data = _esi_client.esi_get(
                f"/markets/{region_id}/orders",
                params={"order_type": "sell", "type_id": type_id},
                paginate=True
            )
        except Exception as e:
            raise RuntimeError(f"ESI request failed (type_id={type_id}): {e}")

        # Filter: sell orders only (is_buy_order == False)
        collected = [order for order in data if order.get("is_buy_order") is False]

        # Store in cache
        _TYPE_SELLORDERS_CACHE[type_id] = (now, collected)
        all_orders.extend(collected)

    return all_orders

def _fetch_region_buy_orders(type_ids, region_id=_region_id):
    _ensure()
    if not type_ids:
        return []

    now = time.time()
    all_orders = []

    for type_id in type_ids:
        if not isinstance(type_id, int) or type_id <= 0:
            raise ValueError(f"Invalid type_id: {type_id}")

        # Cache hit?
        cached = _TYPE_BUYORDERS_CACHE.get(type_id)
        if cached and (now - cached[0] < _TYPE_BUYORDERS_CACHE_TTL):
            all_orders.extend(cached[1])
            continue

        # Fetch all pages for this type_id using built-in pagination
        try:
            data = _esi_client.esi_get(
                f"/markets/{region_id}/orders",
                params={"order_type": "buy", "type_id": type_id},
                paginate=True
            )
        except Exception as e:
            raise RuntimeError(f"ESI request failed (type_id={type_id}): {e}")

        # Filter: buy orders only (is_buy_order == True)
        collected = [order for order in data if order.get("is_buy_order") is True]

        # Store in cache
        _TYPE_BUYORDERS_CACHE[type_id] = (now, collected)
        all_orders.extend(collected)

    return all_orders

def get_sell_order_book(type_ids, region_id=_region_id):
    """
    Returns: { type_id: [ {price, volume_remain, min_volume, order_id}, ... ] } sorted by ascending price.
    """
    _ensure()
    if not type_ids:
        return {}

    orders = _fetch_region_sell_orders(type_ids, region_id)
    book = defaultdict(list)
    target = set(type_ids)

    for order in orders:
        tid = order.get("type_id")
        if tid not in target:
            continue
        book[tid].append({
            "price": order.get("price"),
            "volume_remain": order.get("volume_remain", 0),
            "min_volume": order.get("min_volume", 1),
            "order_id": order.get("order_id")
        })

    # Sort each list by price
    for tid in book:
        book[tid].sort(key=lambda r: r["price"])
    return dict(book)

def get_buy_order_book(type_ids, region_id=_region_id):
    """
    Returns: { type_id: [ {price, volume_remain, min_volume, order_id}, ... ] } sorted by ascending price.
    """
    _ensure()
    if not type_ids:
        return {}

    orders = _fetch_region_buy_orders(type_ids, region_id)
    book = defaultdict(list)
    target = set(type_ids)

    for order in orders:
        tid = order.get("type_id")
        if tid not in target:
            continue
        book[tid].append({
            "price": order.get("price"),
            "volume_remain": order.get("volume_remain", 0),
            "min_volume": order.get("min_volume", 1),
            "order_id": order.get("order_id")
        })

    # Sort each list by price
    for tid in book:
        book[tid].sort(key=lambda r: r["price"])
    return dict(book)

def get_ore_prices(ore_ids):
    if not isinstance(ore_ids, list) or not ore_ids:
        return {}
    return get_sell_order_book(ore_ids)

def get_material_prices(material_ids):
    if not isinstance(material_ids, list) or not material_ids:
        return {}
    return get_sell_order_book(material_ids)

def get_type_sellprices(type_ids, region_id=_region_id):
    if not isinstance(type_ids, list) or not type_ids:
        return {}
    return get_sell_order_book(type_ids, region_id)

def get_type_buyprices(type_ids, region_id=_region_id):
    if not isinstance(type_ids, list) or not type_ids:
        return {}
    return get_buy_order_book(type_ids, region_id)

def get_location_type(location_id: int):
    """
    Returns the type of the given location_id.
    """
    if not location_id or not isinstance(location_id, int):
        return None
    
    _ensure()
    return _esi_client.get_id_type(location_id)

def get_location_info(location_id: int):
    """
    Returns metadata about the given location_id.
    """
    if not location_id or not isinstance(location_id, int):
        return {}
    
    _ensure()
    id_type = get_location_type(location_id)
    try:
        if id_type == "station":
            station = _esi_client.esi_get(f"/universe/stations/{location_id}/")
            return station
        elif id_type == "structure":
            structure = _esi_client.esi_get(f"/universe/structures/{location_id}/")
            return structure
        elif id_type == "region":
            region = _esi_client.esi_get(f"/universe/regions/{location_id}/")
            return region
        elif id_type == "solar_system":
            solar_system = _esi_client.esi_get(f"/universe/systems/{location_id}/")
            return solar_system
        else:
            return {}
    except Exception as e:
        raise RuntimeError(f"ESI request failed for location {location_id}: {e}")
    
def get_market_prices():
    """
    Returns market prices for all items (cached for 1 hour).
    - adjusted_price: used for calculating manufacturing costs.
    - average_price: used for calculating sell prices.
    """
    global _MARKET_PRICES_CACHE

    _ensure()
    now = time.time()
    
    # Cache hit?
    if _MARKET_PRICES_CACHE and (now - _MARKET_PRICES_CACHE[0] < _MARKET_PRICES_CACHE_TTL):
        return _MARKET_PRICES_CACHE[1]

    # Fetch fresh data
    try:
        market_prices = _esi_client.esi_get("/markets/prices/", paginate=True)
    except Exception as e:
        raise RuntimeError(f"ESI request failed: {e}")

    # Store in cache
    _MARKET_PRICES_CACHE = (now, market_prices)

    return market_prices

def get_public_structures(system_id: int, filter: str="manufacturing_basic"):
    """
    Returns a list of public structures in the given system.
    """
    if not system_id or not isinstance(system_id, int):
        raise ValueError("System ID is required to fetch public structures.")
    if filter is not None and filter not in ("manufacturing_basic", "market"):
        raise ValueError("Invalid filter value. Must be one of: None, manufacturing_basic, market.")
    
    _ensure()
    try:
        public_structure_ids = _esi_client.esi_get(f"/universe/structures/", params={"filter": filter})
        public_structures_in_system = []
        for structure_id in public_structure_ids:
            structure_data = _esi_client.esi_get(f"/universe/structures/{structure_id}/")
            if isinstance(structure_data, dict) and structure_data.get("solar_system_id") == system_id:
                public_structures_in_system.append(structure_data)
        return public_structures_in_system
    except Exception as e:
        raise RuntimeError(f"ESI request failed for system {system_id}: {e}")
