"""H1.6: a non-numeric ESI adjusted_price is left out with a WARNING (not a
crash of the whole optimizer), so the reprocessing fee is unknown with its
reason. Fake values."""
from __future__ import annotations

import logging
from types import SimpleNamespace

from eve_online_industry_tracker.infrastructure.static_data.optimizer_service import (
    _adjusted_prices,
    _reprocessing_fee,
)


def test_a_non_numeric_adjusted_price_is_left_out_with_a_warning(caplog):
    esi = SimpleNamespace(get_market_prices=lambda: [
        {"type_id": 34, "adjusted_price": 4.5},
        {"type_id": 35, "adjusted_price": "n/a"},
        {"type_id": 36, "adjusted_price": float("nan")},
    ])
    with caplog.at_level(logging.WARNING):
        prices = _adjusted_prices(esi, [34, 35, 36])
    assert prices == {34: 4.5}
    assert any("35" in r.getMessage() and "36" in r.getMessage() for r in caplog.records)
    fee, reason, _ = _reprocessing_fee(
        tax=0.05, demand_coverage={"Tritanium": {"yielded": 10.0}, "Pyerite": {"yielded": 5.0}},
        material_ids={"Tritanium": 34, "Pyerite": 35}, adjusted_prices=prices,
    )
    assert fee is None and "Pyerite" in reason
