import logging
import time
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

_esi_client = None
_region_id = 10000002
_station_id = 60003760

# Default The Forge: Jita IV - Moon 4 - Caldari Navy Assembly Plant
# Cache structures
_TYPE_SELLORDERS_CACHE = {}          # { type_id: (timestamp, [orders]) }
_TYPE_SELLORDERS_CACHE_TTL = 300     # seconds (5 minutes)
_TYPE_BUYORDERS_CACHE = {}           # { type_id: (timestamp, [orders]) }
_TYPE_BUYORDERS_CACHE_TTL = 300      # seconds (5 minutes)

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
    logging.info(f"_fetch_region_sell_orders({type_ids}, region_id={region_id})")
    _ensure()
    logging.info(f"after _ensure(): region_id={region_id}, station_id={_station_id}")

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

        # Filter: sell orders only (is_buy_order == False)
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
    Possible return values: "station", "structure", or None if unknown.
    """
    if not location_id or not isinstance(location_id, int):
        return None
    
    _ensure()
    id_type = _esi_client.get_id_type(location_id)
    if id_type == "station":
        return "station"
    elif id_type == "structure":
        return "structure"
    elif id_type == "region":
        return "region"
    else:
        return None

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
        else:
            return {}
    except Exception as e:
        raise RuntimeError(f"ESI request failed for location {location_id}: {e}")