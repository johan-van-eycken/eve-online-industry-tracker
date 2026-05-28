#!/usr/bin/env python
"""Backfill acquisition costs for existing assets from historical records."""

import sys
from pathlib import Path

# Add parent directory to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from config.paths import app_config_path, app_secret_path
from config.schemas import CONFIG_SCHEMA
from eve_online_industry_tracker.config.config_manager import ConfigManager
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.models import CharacterAssetsModel
from eve_online_industry_tracker.application.characters.asset_history import build_historical_input_cost_lookup


def backfill_character_assets(character_id: int, db_app: DatabaseManager) -> None:
    """Backfill acquisition costs for a character's assets."""
    session = db_app.session

    # Get all assets for this character
    assets = session.query(CharacterAssetsModel).filter_by(character_id=character_id).all()
    if not assets:
        print(f"No assets found for character_id={character_id}")
        return

    # Get unique type IDs from current assets
    type_ids = {asset.type_id for asset in assets if asset.type_id}

    # Build cost lookup from historical records
    cost_lookup = build_historical_input_cost_lookup(
        app_session=session,
        owner_kind="character",
        owner_id=character_id,
        as_of=None,
        type_ids=type_ids,
    )

    # Update assets with costs
    updated_count = 0
    for asset in assets:
        if asset.type_id in cost_lookup:
            cost_data = cost_lookup[asset.type_id]
            if not asset.acquisition_unit_cost or asset.acquisition_unit_cost == 0:
                asset.acquisition_unit_cost = cost_data.get("unit_cost")
                asset.acquisition_source = cost_data.get("source")
                asset.acquisition_reference_type = cost_data.get("reference_type")
                asset.acquisition_reference_id = cost_data.get("reference_id")
                asset.acquisition_date = cost_data.get("observed_at")

                if asset.acquisition_unit_cost and asset.quantity:
                    asset.acquisition_total_cost = float(asset.acquisition_unit_cost * asset.quantity)

                updated_count += 1
                print(
                    f"  Updated asset: type_id={asset.type_id}, qty={asset.quantity}, "
                    f"unit_cost={asset.acquisition_unit_cost:.2f}, source={asset.acquisition_source}"
                )

    if updated_count > 0:
        session.commit()
        print(f"Backfilled {updated_count} assets for character_id={character_id}")
    else:
        print(f"No assets needed backfilling for character_id={character_id}")


def main():
    """Run backfill for all characters or specific character."""
    cfg_manager = ConfigManager(
        base_path=app_config_path(),
        secret_path=app_secret_path(),
        schema=CONFIG_SCHEMA,
    )
    cfg = cfg_manager.all()
    db_app = DatabaseManager(cfg["app"]["database_app_uri"], cfg["app"]["language"])

    session = db_app.session

    # Get all unique character IDs
    character_ids = session.query(CharacterAssetsModel.character_id).distinct().all()
    character_ids = [cid[0] for cid in character_ids]

    if not character_ids:
        print("No characters found with assets")
        return

    print(f"Found {len(character_ids)} characters with assets")

    for character_id in character_ids:
        print(f"\nBackfilling character_id={character_id}...")
        backfill_character_assets(character_id, db_app)

    print("\nBackfill complete!")


if __name__ == "__main__":
    main()
