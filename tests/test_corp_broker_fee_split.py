"""A corp order's broker fee is matched and split by the value of the units
each fill sold, not by the number of fills. Fake ids."""
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.characters.realized_profit import (
    CorporationRealizedProfitLedgerService,
)

_split = CorporationRealizedProfitLedgerService._precompute_per_unit_broker_fees


def _sell(tx_id, minute, quantity, unit_price=1_000_000.0):
    return SimpleNamespace(transaction_id=tx_id, type_id=34, unit_price=unit_price, quantity=quantity,
                           is_buy=False, date=f"2026-10-01T00:{minute:02d}:00Z")


def _fee(amount):
    return SimpleNamespace(wallet_journal_id=1, amount=-amount, date="2026-10-01T00:00:00Z")


def test_a_100_unit_order_filled_twice_gets_its_broker_fee():
    # 3% of 100 x 1M: the old count-based rate (3M / 2 x 1M = 1.5) failed the 25% cap.
    fees, consumed = _split([_sell(1, 10, 50), _sell(2, 20, 50)], [_fee(3_000_000.0)])

    assert fees == {1: pytest.approx(1_500_000.0), 2: pytest.approx(1_500_000.0)}
    assert consumed == {1}


def test_the_fee_is_split_by_each_fills_share_of_the_value():
    fees, _ = _split([_sell(1, 10, 30), _sell(2, 20, 70)], [_fee(3_000_000.0)])

    assert fees == {1: pytest.approx(900_000.0), 2: pytest.approx(2_100_000.0)}
