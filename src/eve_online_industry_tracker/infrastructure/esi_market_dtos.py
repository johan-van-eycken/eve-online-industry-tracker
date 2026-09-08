from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class EsiMarketOrder(BaseModel):
    """DTO for a single order from GET /v1/markets/{region_id}/orders/.

    Validates ESI response at the boundary; a ValidationError here means CCP
    renamed or dropped a field — catch it in logs before it corrupts MarketDepthCache.
    Fields not listed below (e.g. future additions) are silently ignored.
    """

    model_config = ConfigDict(extra="ignore")

    order_id: int
    type_id: int
    price: float
    volume_remain: int
    volume_total: int
    is_buy_order: bool
    location_id: int
    system_id: int
    duration: int
    issued: str
    min_volume: int
    range: str


class EsiMarketHistoryEntry(BaseModel):
    """DTO for one day of history from GET /v1/markets/{region_id}/history/.

    Used by MarketDepthCollector when computing VWAP and price trends.
    """

    model_config = ConfigDict(extra="ignore")

    date: str
    average: float
    highest: float
    lowest: float
    order_count: int
    volume: int
