from __future__ import annotations

from typing import Any

from eve_online_industry_tracker.db_models import MarketOrderbookViewCacheModel


def get_cached_orderbook_levels(
    session: Any,
    *,
    hub: str,
    region_id: int,
    station_id: int,
    side: str,
    type_id: int,
    at_hub: bool,
    ttl_seconds: int,
) -> tuple[list[list[float | int]], float] | None:
    row = (
        session.query(MarketOrderbookViewCacheModel)
        .filter(
            MarketOrderbookViewCacheModel.hub == str(hub),
            MarketOrderbookViewCacheModel.region_id == int(region_id),
            MarketOrderbookViewCacheModel.station_id == int(station_id),
            MarketOrderbookViewCacheModel.side == str(side),
            MarketOrderbookViewCacheModel.type_id == int(type_id),
            MarketOrderbookViewCacheModel.at_hub == bool(at_hub),
        )
        .order_by(MarketOrderbookViewCacheModel.fetched_at.desc())
        .first()
    )
    if row is None:
        return None

    try:
        fetched_at = float(row.fetched_at)
    except Exception:
        return None
    import time

    if fetched_at + max(0, int(ttl_seconds)) < time.time():
        return None

    levels = row.levels if isinstance(row.levels, list) else []
    normalized_levels: list[list[float | int]] = []
    for level in levels:
        if not isinstance(level, list) or len(level) < 2:
            continue
        try:
            normalized_levels.append([float(level[0]), int(level[1])])
        except Exception:
            continue
    return normalized_levels, fetched_at


def upsert_orderbook_levels(
    session: Any,
    *,
    hub: str,
    region_id: int,
    station_id: int,
    side: str,
    type_id: int,
    at_hub: bool,
    depth: int,
    levels: list[list[float | int]],
    fetched_at: float,
) -> None:
    row = (
        session.query(MarketOrderbookViewCacheModel)
        .filter(
            MarketOrderbookViewCacheModel.hub == str(hub),
            MarketOrderbookViewCacheModel.region_id == int(region_id),
            MarketOrderbookViewCacheModel.station_id == int(station_id),
            MarketOrderbookViewCacheModel.side == str(side),
            MarketOrderbookViewCacheModel.type_id == int(type_id),
            MarketOrderbookViewCacheModel.at_hub == bool(at_hub),
        )
        .order_by(MarketOrderbookViewCacheModel.fetched_at.desc())
        .first()
    )
    if row is None:
        row = MarketOrderbookViewCacheModel(
            hub=str(hub),
            region_id=int(region_id),
            station_id=int(station_id),
            side=str(side),
            type_id=int(type_id),
            at_hub=bool(at_hub),
            depth=int(depth),
            levels=levels,
            fetched_at=float(fetched_at),
            version=1,
        )
        session.add(row)
    else:
        row.depth = int(depth)
        row.levels = levels
        row.fetched_at = float(fetched_at)
        row.version = 1
