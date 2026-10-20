#!/usr/bin/env python
"""Backfill acquisition costs for corp asset history records from character industry jobs
and character market buy transactions.

When characters transfer manufactured items to the corporation, the corp asset history
has no cost basis (acquisition_unit_cost IS NULL, acquisition_source IS NULL or
acquisition_source = 'untracked_inventory').
This script:
1. Matches those records (acquisition_source IS NULL, "untracked_inventory", or "unknown")
   to character industry jobs using FIFO chronological matching, or falls back to a
   quantity-weighted average per type_id when FIFO cannot be resolved.
2. For items with no matching industry job, attempts a second pass using character market
   buy transactions as the cost source.

The matching lives in eve_online_industry_tracker.application.characters.corp_transfer_costs,
which also re-stamps the blended lots of a type when one of its jobs is re-costed.
"""

import logging
import sys
from pathlib import Path

# Add parent directory to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from config.paths import app_config_path, app_secret_path
from config.schemas import CONFIG_SCHEMA
from eve_online_industry_tracker.application.characters.corp_transfer_costs import backfill_corp_transfer_costs
from eve_online_industry_tracker.application.characters.realized_profit import CorporationRealizedProfitLedgerService
from eve_online_industry_tracker.config.config_manager import ConfigManager
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.models import CorporationAssetHistoryModel


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """Run corp transfer cost backfill."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg_manager = ConfigManager(
        base_path=app_config_path(),
        secret_path=app_secret_path(),
        schema=CONFIG_SCHEMA,
    )
    cfg = cfg_manager.all()
    db_app = DatabaseManager(cfg["app"]["database_app_uri"], cfg["app"]["language"])
    session = db_app.session
    db_sde = DatabaseManager(cfg["app"]["database_sde_uri"], cfg["app"]["language"])
    sde_session = db_sde.session

    print("Starting corp transfer cost backfill...")
    summary = backfill_corp_transfer_costs(session)

    print("\n--- Summary ---")
    print(f"  Total uncosted corp history rows:  {summary['total_uncosted']}")
    print(f"  FIFO-matched (exact job):          {summary['fifo_matched']}")
    print(f"  Average-matched (weighted avg):    {summary['avg_matched']}")
    print(f"  Buy-matched (character market buy):{summary['buy_matched']}")
    print(f"  Buy-avg-matched (buy weighted avg):{summary['buy_avg_matched']}")
    print(f"  Unmatched (no source found):       {summary['unmatched']}")

    any_matched = sum(summary[k] for k in ["fifo_matched", "avg_matched", "buy_matched", "buy_avg_matched"])
    if any_matched > 0:
        print("\nRebuilding realized profit ledger for affected corporations...")
        corp_ids = [
            row[0] for row in session.query(CorporationAssetHistoryModel.corporation_id)
            .filter(CorporationAssetHistoryModel.acquisition_source.in_([
                "industry_build_transferred",
                "industry_build_transferred_avg",
                "character_market_buy_transferred",
                "character_market_buy_transferred_avg",
            ]))
            .distinct()
            .all()
        ]
        service = CorporationRealizedProfitLedgerService(
            app_session=session,
            sde_session=sde_session,
            market_prices=[],
        )
        for corp_id in corp_ids:
            service.rebuild(corporation_id=corp_id)
            print(f"  Rebuilt realized profit ledger for corporation_id={corp_id}")

    print("\nBackfill complete!")


if __name__ == "__main__":
    main()
