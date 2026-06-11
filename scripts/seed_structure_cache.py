"""Seed the structure_name_cache by trying each character's ESI client.

Usage: python scripts/seed_structure_cache.py
"""
import sys
import os
import logging
import sqlite3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

from utils.app_init import load_config, init_db_oauth, init_db_app
from eve_online_industry_tracker.infrastructure.esi_client import ESIClient


def main():
    cfg = load_config()
    db_oauth = init_db_oauth(cfg)
    db_app = init_db_app(cfg)

    # Get all unique structure IDs from character assets
    app_path = cfg.all()["app"]["database_app_uri"].replace("sqlite:///", "")
    conn = sqlite3.connect(app_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT DISTINCT top_location_id FROM character_assets WHERE top_location_id >= 1020000000000"
    )
    structure_ids = [r[0] for r in cursor.fetchall()]
    print(f"Found {len(structure_ids)} unique structure IDs in assets")

    # Also check corporation_assets
    cursor.execute(
        "SELECT DISTINCT top_location_id FROM corporation_assets WHERE top_location_id >= 1020000000000"
    )
    corp_ids = [r[0] for r in cursor.fetchall()]
    structure_ids = list(set(structure_ids + corp_ids))
    print(f"Total unique structure IDs (incl. corp): {len(structure_ids)}")

    # Check which are already cached
    cursor.execute("SELECT structure_id FROM structure_name_cache")
    cached = {r[0] for r in cursor.fetchall()}
    uncached = [sid for sid in structure_ids if sid not in cached]
    print(f"Already cached: {len(cached)}, need resolution: {len(uncached)}")

    if not uncached:
        print("All structures already cached!")
        conn.close()
        return

    # Build ESI clients for all characters
    characters = cfg.all()["characters"]
    clients = []
    for char_cfg in characters:
        name = char_cfg["character_name"]
        try:
            client = ESIClient(
                cfg, db_oauth, name,
                char_cfg.get("is_main", False),
                char_cfg.get("is_corp_director", False),
            )
            if client.character_id:
                clients.append((name, client))
                print(f"  Loaded ESI client for {name} (id: {client.character_id})")
        except Exception as e:
            print(f"  Failed to load {name}: {e}")

    # Try each structure with each character
    resolved = 0
    for sid in sorted(uncached):
        for char_name, client in clients:
            try:
                data = client.esi_get(
                    f"/universe/structures/{sid}/",
                    suppress_forbidden_log=True,
                    suppress_not_found_log=True,
                )
                if data and isinstance(data, dict) and data.get("name"):
                    name = data["name"]
                    solar_system_id = data.get("solar_system_id")
                    owner_id = data.get("owner_id")
                    type_id = data.get("type_id")
                    cursor.execute(
                        "INSERT OR REPLACE INTO structure_name_cache (structure_id, name, solar_system_id, owner_id, type_id) VALUES (?, ?, ?, ?, ?)",
                        (sid, name, solar_system_id, owner_id, type_id),
                    )
                    conn.commit()
                    print(f"  {sid}: {name} (via {char_name})")
                    resolved += 1
                    break
            except Exception:
                continue
        else:
            print(f"  {sid}: UNRESOLVED (no character has access)")

    print(f"\nResolved {resolved}/{len(uncached)} structures")
    conn.close()


if __name__ == "__main__":
    main()
