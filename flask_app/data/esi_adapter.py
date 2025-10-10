import time
from collections import defaultdict

_esi_client = None
_region_id = None
_station_id = None

# Cache structures
_TYPE_ORDERS_CACHE = {}          # { type_id: (timestamp, [orders]) }
_TYPE_ORDERS_CACHE_TTL = 120     # seconds
_RATE_LIMIT_SLEEP = 0.2          # polite delay between page fetches

# Default The Forge: Jita IV - Moon 4 - Caldari Navy Assembly Plant
def esi_adapter(character, region_id=10000002, station_id=60003760):
    global _esi_client, _station_id, _region_id
    _esi_client = character.esi_client
    _station_id = station_id
    _region_id = region_id

def _ensure():
    if not _esi_client:
        raise RuntimeError("ESI client not initialized. Call esi_adapter(character) first.")
    if not _region_id:
        raise RuntimeError("Region ID not set.")
    if not _station_id:
        raise RuntimeError("Station ID not set.")

def _fetch_region_sell_orders(type_ids):
    """
    Fetch sell orders for the given type_ids (list of ints) in the configured region,
    filtered to the configured station. Aggregates paginated results per type_id.
    Uses a simple per-type cache to reduce ESI calls.
    Returns: list of order dicts (already filtered to station & sell side).
    """
    _ensure()
    if not type_ids:
        return []

    now = time.time()
    all_orders = []

    for type_id in type_ids:
        if not isinstance(type_id, int) or type_id <= 0:
            raise ValueError(f"Invalid type_id: {type_id}")

        # Cache hit?
        cached = _TYPE_ORDERS_CACHE.get(type_id)
        if cached and (now - cached[0] < _TYPE_ORDERS_CACHE_TTL):
            all_orders.extend(cached[1])
            continue

        # Fetch all pages for this type_id
        page = 1
        collected = []
        while True:
            try:
                # Expect esi_get to optionally return (data, headers) if return_headers=True is supported.
                # Fallback: if only list returned, assume single page.
                result = _esi_client.esi_get(
                    f"/markets/{_region_id}/orders",
                    params={"order_type": "sell", "type_id": type_id, "page": page},
                    return_headers=True
                )
            except TypeError:
                # Client does not support return_headers -> single page only
                result = (_esi_client.esi_get(
                    f"/markets/{_region_id}/orders?order_type=sell&type_id={type_id}&page={page}"
                ), {"X-Pages": "1"})
            except Exception as e:
                raise RuntimeError(f"ESI request failed (type_id={type_id}, page={page}): {e}")

            data, headers = result
            if not isinstance(data, list):
                break

            # Filter: same station & sell orders only (is_buy_order == False)
            for order in data:
                if (order.get("is_buy_order") is False and
                        order.get("location_id") == _station_id):
                    collected.append(order)

            total_pages = int(headers.get("X-Pages", "1"))
            if page >= total_pages:
                break
            page += 1
            time.sleep(_RATE_LIMIT_SLEEP)  # polite pacing

        # Store in cache
        _TYPE_ORDERS_CACHE[type_id] = (now, collected)
        all_orders.extend(collected)

    return all_orders

def get_order_book(type_ids):
    """
    Returns: { type_id: [ {price, volume_remain, min_volume, order_id}, ... ] } sorted by ascending price.
    """
    _ensure()
    if not type_ids:
        return {}

    orders = _fetch_region_sell_orders(type_ids)
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
    return get_order_book(ore_ids)

def get_material_prices(material_ids):
    if not isinstance(material_ids, list) or not material_ids:
        return {}
    return get_order_book(material_ids)