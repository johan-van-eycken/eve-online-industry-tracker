#!/usr/bin/env python3
import os
import sys
import zipfile
import shutil
import requests # pyright: ignore[reportMissingModuleSource]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
import yaml # pyright: ignore[reportMissingModuleSource]
import argparse
import json

# Add project root to sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

from classes.config_manager import ConfigManager
from config.schemas import IMPORT_SDE_SCHEMA
from classes.database_manager import DatabaseManager

# ----------------------------
# Helpers
# ----------------------------
def flatten_row(d):
    """Flatten row shallowly. Dicts/lists → JSON strings, scalars unchanged."""
    flat = {}
    for k, v in d.items():
        if isinstance(v, (dict, list)):
            # keep full dict/list as JSON
            flat[k] = json.dumps(v, ensure_ascii=False)
        else:
            flat[k] = v
    return flat

def sanitize_column_name(name: str) -> str:
    """Sanitize SQL column name to avoid conflicts with quotes."""
    return name.replace("-", "_").replace(" ", "_")

def load_repackaged_volumes(json_path):
    """Load repackaged volumes from JSON file."""
    if not os.path.exists(json_path):
        return {}, {}
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    repackaged = data.get("repackaged_volumes", {})
    groups = repackaged.get("groups", {})
    items = repackaged.get("items", {})
    groups = {int(k): v.get("repackaged_volume") for k, v in groups.items()}
    items = {int(k): v.get("repackaged_volume") for k, v in items.items()}
    return groups, items

# ----------------------------
# SDE download & extraction
# ----------------------------
def download_sde(url: str, dest_dir: str) -> str:
    os.makedirs(dest_dir, exist_ok=True)
    zip_path = os.path.join(dest_dir, "sde.zip")

    if not os.path.exists(zip_path):
        print("Downloading SDE...")
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(1024*1024):
                f.write(chunk)
    else:
        print("SDE already downloaded.")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dest_dir)

    print(f"SDE extracted to {dest_dir}")
    return dest_dir

# ----------------------------
# Import YAML SDE tables to SQLite
# ----------------------------
def import_sde_to_sqlite(sde_dir: str, db_uri: str, tables_to_import: list, repackaged_json_path: str):
    db = DatabaseManager(db_uri)

    # Load repackaged volumes once
    repackaged_groups, repackaged_items = load_repackaged_volumes(repackaged_json_path)

    for root, _, files in os.walk(sde_dir):
        for file in sorted(files):
            table_name = os.path.splitext(file)[0]

            if table_name not in tables_to_import:
                continue

            if file.endswith((".yaml", ".yml")):
                yaml_path = os.path.join(root, file)
                print(f"Importing table '{table_name}' ...")
                try:
                    with open(yaml_path, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f)

                    if isinstance(data, dict):
                        new_data = []
                        for k, v in data.items():
                            if isinstance(v, dict):
                                v = {"id": int(k), **v}
                            else:
                                v = {"id": int(k), "value": v}
                            new_data.append(v)
                        data = new_data
                    elif not isinstance(data, list):
                        data = []

                    data = [flatten_row(row) for row in data]
                    df = pd.DataFrame(data)
                    df = df.loc[:, ~df.columns.duplicated()]
                    df.columns = [sanitize_column_name(c) for c in df.columns]

                    # Add repackaged_volume column if items or groups table
                    if table_name == "types" and "id" in df.columns:
                        df["repackaged_volume"] = df["id"].map(repackaged_items)
                    elif table_name == "groups" and "id" in df.columns:
                        df["repackaged_volume"] = df["id"].map(repackaged_groups)

                    db.save_df(df, table_name)
                    print(f" -> Imported {len(df)} rows into '{table_name}'")
                except Exception as e:
                    print(f" !!! Failed to import '{table_name}': {e}")

    print(f"All selected SDE tables imported to {db_uri}")

# ----------------------------
# Cleanup
# ----------------------------
def cleanup_temp(dest_dir: str):
    if os.path.exists(dest_dir):
        print(f"Cleaning up temporary folder {dest_dir} ...")
        try:
            shutil.rmtree(dest_dir)
        except Exception as e:
            print(f" !!! Failed to cleanup {dest_dir}: {e}")
        print("Cleanup done.")
    else:
        print(f"No temporary folder found at {dest_dir}")

# ----------------------------
# CLI
# ----------------------------
def main():
    default_config_path = "config/import_sde.json"
    try:
        cfg = ConfigManager(base_path=default_config_path, schema=IMPORT_SDE_SCHEMA)
    except Exception as e:
        print(f" !!! Failed to initialize ConfigManager ({default_config_path}): {e}")

    # Load tables from config
    if not os.path.exists(default_config_path):
        raise FileNotFoundError(f"{default_config_path} not found.")

    parser = argparse.ArgumentParser(
        description=f"EVE Online SDE Importer {cfg.get('APP_VERSION')} (YAML -> SQLite)",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--version", action="version", version=f"EVE Online SDE Importer {cfg.get('APP_VERSION')}")
    parser.add_argument("--all", action="store_true", help="Download, import, and cleanup in one go")
    parser.add_argument("--download", action="store_true", help="Download and extract the SDE")
    parser.add_argument("--import", dest="do_import", action="store_true", help="Import selected YAML tables into SQLite")
    parser.add_argument("--cleanup", action="store_true", help="Cleanup temporary SDE folder")
    parser.add_argument("--db", default=cfg.get("DEFAULT_DB_URI"), help="SQLite database file path")
    parser.add_argument("--tmp", default=cfg.get("DEFAULT_TMP_DIR"), help="Temporary folder for SDE extraction")
    parser.add_argument("--tables", nargs="*", help="Tables to import (default: TABLES_TO_IMPORT from config)")

    args = parser.parse_args()

    # Show help if no arguments
    if not any(vars(args).values()):
        parser.print_help()
        return

    if args.all:
        args.download = True
        args.do_import = True
        args.cleanup = True

    sde_path = args.tmp

    if args.download:
        sde_path = download_sde(cfg.get("SDE_URL"), sde_path)

    if args.do_import:
        if not os.path.exists(sde_path):
            print(f"Temporary SDE folder '{sde_path}' not found. Running --download first.")
            sde_path = download_sde(cfg.get("SDE_URL"), sde_path)

        repackaged_json_path = cfg.get("REPACKAGED_JSON_PATH")
        if not os.path.exists(repackaged_json_path):
            raise FileNotFoundError(f"Data file '{repackaged_json_path}' not found.")
        
        # Either CLI tables or config tables
        if args.tables:
            tables_to_import = args.tables
        else:
            tables_to_import = cfg.get("TABLES_TO_IMPORT", [])

        if not tables_to_import:
            raise ValueError("No tables specified for import (check config or CLI args).")

        import_sde_to_sqlite(sde_path, db_uri=args.db, tables_to_import=tables_to_import, repackaged_json_path=repackaged_json_path)

    if args.cleanup:
        cleanup_temp(sde_path)

if __name__ == "__main__":
    main()