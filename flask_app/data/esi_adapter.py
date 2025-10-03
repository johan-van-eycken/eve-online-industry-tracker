_esi_client = None
_region_id = None # set via esi_adapter()
_station_id = None  # set via esi_adapter()

# Default The Forge: Jita IV - Moon 4 - Caldari Navy Assembly Plant
def esi_adapter(character, region_id=10000002, station_id=60003760):
    global _esi_client, _station_id, _region_id
    _esi_client = character.esi_client
    _station_id = station_id
    _region_id = region_id

def _ensure():
    if not _esi_client:
        raise RuntimeError("ESI client not initialized. Call esi_adapter(character) first.")
    if not _station_id:
        raise RuntimeError("Station ID not set.")

def _fetch_region_sell_order(type_id):
    _ensure()
    # Simple (no pagination). Add paging later if needed.
    return _esi_client.esi_get(f"/markets/{_region_id}/orders/", params={
        "order_type": "sell",
        "type_id": type_id
    })

def get_ore_order_book(type_ids):
    """
    Returns: { type_id: [ {price, volume_remain, min_volume, order_id}, ... ] } (sell orders only, ascending price)
    """
    if not type_ids:
        return {}
    target = set(type_ids)
    book = {}
    for type_id in target:
        for order in _fetch_region_sell_order(type_id):
            if order.get("is_buy_order"):
                continue
            if order.get("location_id") != _station_id:
                continue
            tid = order.get("type_id")
            book.setdefault(tid, []).append({
                "price": order["price"],
                "volume_remain": order.get("volume_remain", 0),
                "min_volume": order.get("min_volume", 1),
                "order_id": order.get("order_id")
            })
    # Sort each list by ascending price
    for tid, lst in book.items():
        lst.sort(key=lambda r: r["price"])
    return book

# Backwards compatibility (still returns full list not just best)
def get_ore_prices(ores):
    if not ores:
        return {}
    ids = [o["id"] for o in ores] if isinstance(ores[0], dict) else ores
    return get_ore_order_book(ids)

def get_mineral_prices(minerals):
    if not minerals:
        return {}
    ids = [m["id"] for m in minerals] if isinstance(minerals[0], dict) else minerals
    return get_ore_order_book(ids)