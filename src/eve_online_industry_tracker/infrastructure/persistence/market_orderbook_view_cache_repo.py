from __future__ import annotations

import json
import time
from typing import Any

from sqlalchemy import bindparam, text


_CACHE_VERSION = 1


def _normalize_hub(hub: str | None) -> str:
    h = str(hub or "").strip().lower()
    return h or "jita"


def get_views(
    session,
    *,
    hub: str,
    region_id: int,
    station_id: int,
    side: str,
    at_hub: bool,
    type_ids: list[int],
    ttl_seconds: int,
) -> dict[int, dict[str, Any]]:
    """Return cached orderbook price levels for the given keys.

    The stored payload is a JSON list of [price, volume] pairs (already sorted).
    """

    if session is None:
        return {}

    ids = sorted({int(t) for t in (type_ids or []) if t is not None and int(t) > 0})
    if not ids:
        return {}

    hub_n = _normalize_hub(hub)
    side_n = str(side or "").strip().lower()
    if side_n not in {"buy", "sell"}:
        return {}

    now = time.time()
    min_fetched_at = float(now) - float(max(0, int(ttl_seconds or 0)))

    rows = session.execute(
        text(
            "SELECT type_id, levels, total_volume, order_count, fetched_at, version "
            "FROM market_orderbook_view_cache "
            "WHERE hub = :hub AND region_id = :region_id AND station_id = :station_id "
            "AND side = :side AND at_hub = :at_hub AND type_id IN :type_ids "
            "AND fetched_at >= :min_fetched_at"
        ).bindparams(bindparam("type_ids", expanding=True)),
        {
            "hub": hub_n,
            "region_id": int(region_id),
            "station_id": int(station_id),
            "side": side_n,
            "at_hub": 1 if bool(at_hub) else 0,
            "type_ids": ids,
            "min_fetched_at": float(min_fetched_at),
        },
    ).fetchall()

    out: dict[int, dict[str, Any]] = {}
    for type_id, levels_raw, total_volume, order_count, fetched_at, version in rows or []:
        try:
            if int(version or 0) != int(_CACHE_VERSION):
                continue
        except Exception:
            continue

        levels_obj: Any = levels_raw
        if isinstance(levels_obj, str):
            try:
                levels_obj = json.loads(levels_obj)
            except Exception:
                levels_obj = None

        if not isinstance(levels_obj, list):
            continue

        levels: list[tuple[float, int]] = []
        ok = True
        for pair in levels_obj:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                ok = False
                break
            try:
                price_f = float(pair[0])
                vol_i = int(pair[1])
            except Exception:
                ok = False
                break
            if price_f <= 0 or vol_i <= 0:
                continue
            levels.append((price_f, vol_i))

        if not ok:
            continue

        try:
            out[int(type_id)] = {
                "levels": levels,
                "fetched_at": float(fetched_at or 0.0) if fetched_at is not None else None,
                "total_volume": int(total_volume or 0),
                "order_count": int(order_count or 0),
            }
        except Exception:
            continue

    return out


def get_liquidity_summaries(
    session,
    *,
    hub: str,
    region_id: int,
    station_id: int,
    side: str,
    at_hub: bool,
    type_ids: list[int],
    ttl_seconds: int,
) -> dict[int, dict[str, int]]:
    if session is None:
        return {}

    ids = sorted({int(t) for t in (type_ids or []) if t is not None and int(t) > 0})
    if not ids:
        return {}

    hub_n = _normalize_hub(hub)
    side_n = str(side or "").strip().lower()
    if side_n not in {"buy", "sell"}:
        return {}

    now = time.time()
    min_fetched_at = float(now) - float(max(0, int(ttl_seconds or 0)))

    rows = session.execute(
        text(
            "SELECT type_id, levels, total_volume, order_count, version "
            "FROM market_orderbook_view_cache "
            "WHERE hub = :hub AND region_id = :region_id AND station_id = :station_id "
            "AND side = :side AND at_hub = :at_hub AND type_id IN :type_ids "
            "AND fetched_at >= :min_fetched_at"
        ).bindparams(bindparam("type_ids", expanding=True)),
        {
            "hub": hub_n,
            "region_id": int(region_id),
            "station_id": int(station_id),
            "side": side_n,
            "at_hub": 1 if bool(at_hub) else 0,
            "type_ids": ids,
            "min_fetched_at": float(min_fetched_at),
        },
    ).fetchall()

    out: dict[int, dict[str, int]] = {}
    for type_id, levels_raw, total_volume, order_count, version in rows or []:
        try:
            if int(version or 0) != int(_CACHE_VERSION):
                continue

            levels_obj: Any = levels_raw
            if isinstance(levels_obj, str):
                try:
                    levels_obj = json.loads(levels_obj)
                except Exception:
                    levels_obj = None

            has_cached_levels = False
            if isinstance(levels_obj, list):
                for pair in levels_obj:
                    if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                        continue
                    try:
                        price_f = float(pair[0])
                        vol_i = int(pair[1])
                    except Exception:
                        continue
                    if price_f > 0 and vol_i > 0:
                        has_cached_levels = True
                        break

            # Legacy cache rows may still have valid price levels but zeroed liquidity fields.
            # Treat those rows as missing so callers refetch and repopulate the summaries.
            if has_cached_levels and int(total_volume or 0) <= 0 and int(order_count or 0) <= 0:
                continue

            out[int(type_id)] = {
                "total_volume": int(total_volume or 0),
                "order_count": int(order_count or 0),
            }
        except Exception:
            continue

    return out


def upsert_views(
    session,
    *,
    hub: str,
    region_id: int,
    station_id: int,
    side: str,
    at_hub: bool,
    views_by_type_id: dict[int, list[tuple[float, int]]],
    depth: int,
    liquidity_by_type_id: dict[int, dict[str, int]] | None = None,
) -> None:
    """Upsert cached orderbook views.

    Uses a UNIQUE constraint on (hub, region_id, station_id, side, type_id, at_hub).
    """

    if session is None:
        return

    if not views_by_type_id:
        return

    hub_n = _normalize_hub(hub)
    side_n = str(side or "").strip().lower()
    if side_n not in {"buy", "sell"}:
        return

    now = time.time()

    # Write via the underlying bind/engine so we don't commit unrelated ORM state.
    bind = None
    try:
        bind = session.get_bind()
    except Exception:
        bind = getattr(session, "bind", None)

    if bind is None:
        return

    stmt = text(
        "INSERT INTO market_orderbook_view_cache "
        "(hub, region_id, station_id, side, type_id, at_hub, depth, levels, total_volume, order_count, fetched_at, version) "
        "VALUES (:hub, :region_id, :station_id, :side, :type_id, :at_hub, :depth, :levels, :total_volume, :order_count, :fetched_at, :version) "
        "ON CONFLICT(hub, region_id, station_id, side, type_id, at_hub) "
        "DO UPDATE SET "
        "depth=excluded.depth, "
        "levels=excluded.levels, "
        "total_volume=excluded.total_volume, "
        "order_count=excluded.order_count, "
        "fetched_at=excluded.fetched_at, "
        "version=excluded.version"
    )

    with bind.begin() as conn:
        for type_id, levels in views_by_type_id.items():
            try:
                tid = int(type_id)
            except Exception:
                continue
            if tid <= 0:
                continue

            payload = json.dumps([[float(p), int(v)] for (p, v) in (levels or [])])
            liquidity = (liquidity_by_type_id or {}).get(int(tid)) or {}

            conn.execute(
                stmt,
                {
                    "hub": hub_n,
                    "region_id": int(region_id),
                    "station_id": int(station_id),
                    "side": side_n,
                    "type_id": int(tid),
                    "at_hub": 1 if bool(at_hub) else 0,
                    "depth": int(depth),
                    "levels": payload,
                    "total_volume": int(liquidity.get("total_volume") or 0),
                    "order_count": int(liquidity.get("order_count") or 0),
                    "fetched_at": float(now),
                    "version": int(_CACHE_VERSION),
                },
            )
