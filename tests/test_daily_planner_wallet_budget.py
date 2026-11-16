from eve_online_industry_tracker.application.daily_planner.wallet_budget import (
    StockLedger,
    WalletBudget,
    max_affordable_runs,
)


def test_budget_applies_reserve():
    b = WalletBudget(balance=1_000.0, reserve_pct=10)
    assert b.available == 900.0
    assert b.charge(900.0) and not b.charge(0.01)


def test_budget_zero_after_reserve_blocks_everything():
    b = WalletBudget(balance=0.0, reserve_pct=10)
    assert b.blocked_reason == "no budget: corp wallet 0 ISK, 10% reserve"
    assert not b.charge(1.0) and not b.charge_science(1.0)


def test_unknown_amount_is_never_affordable():
    assert not WalletBudget(balance=1e9, reserve_pct=0).charge(None)


def test_blocked_reason_from_caller():
    b = WalletBudget(balance=1e9, reserve_pct=10, blocked_reason="corp wallet last read 7.0 h ago (limit 6 h)")
    assert not b.charge(1.0) and b.blocked_reason.startswith("corp wallet last read")


def test_unknown_balance_is_blocked_and_caller_reason_wins():
    assert WalletBudget(balance=None, reserve_pct=10).blocked_reason == "corp wallet unknown"
    assert WalletBudget(balance=None, reserve_pct=10, blocked_reason="stale").blocked_reason == "stale"
    assert WalletBudget(balance=0.0, reserve_pct=10, blocked_reason="stale").blocked_reason == "stale"


def test_negative_amount_fails():
    b = WalletBudget(balance=100.0, reserve_pct=0)
    assert not b.charge(-1.0) and not b.charge_science(-1.0) and b.spent == 0.0


def test_science_pot_is_spent_first():
    b = WalletBudget(balance=1_000.0, reserve_pct=0)
    assert b.set_aside(100.0) == 100.0 and b.available == 900.0
    assert b.charge_science(150.0) and b.available == 850.0 and b.spent == 150.0


def test_set_aside_is_capped_and_zero_when_blocked():
    assert WalletBudget(balance=50.0, reserve_pct=0).set_aside(100.0) == 50.0
    assert WalletBudget(balance=0.0, reserve_pct=0).set_aside(10.0) == 0.0


def test_charge_science_fails_without_charging_when_short():
    b = WalletBudget(balance=100.0, reserve_pct=0)
    b.set_aside(30.0)
    assert not b.charge_science(101.0) and b.spent == 0.0 and b.available == 70.0


def test_stock_ledger_consumes_in_order():
    s = StockLedger({34: 100})
    price = {34: 5.0}.get
    assert s.purchase_cost({34: 60}, price, commit=True) == 0.0
    assert s.purchase_cost({34: 60}, price, commit=True) == 100.0   # 40 left in stock, 20 bought


def test_stock_ledger_unpriced_gap_is_unknown():
    assert StockLedger({}).purchase_cost({99: 1}, {}.get, commit=False) is None


def test_stock_ledger_commit_false_and_copy():
    src = {34: 10}
    s = StockLedger(src)
    assert s.purchase_cost({34: 10}, {}.get, commit=False) == 0.0
    assert s.purchase_cost({34: 10}, {}.get, commit=True) == 0.0
    assert src == {34: 10}


def test_max_affordable_runs():
    assert max_affordable_runs(lambda r: 10.0 * r, 30, 95.0) == 9
    assert max_affordable_runs(lambda r: 10.0 * r, 30, 5.0) == 0
    assert max_affordable_runs(lambda r: None, 30, 1e9) == 0
