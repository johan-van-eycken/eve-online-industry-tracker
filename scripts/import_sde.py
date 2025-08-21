#!/usr/bin/env python3
import os
import sys
import zipfile
import shutil
import requests
import pandas as pd
import yaml
import argparse
import json

# Add project root to sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager

# ----------------------------
# Default values
# ----------------------------
APP_VERSION = "v1.0"
SDE_URL = "https://eve-static-data-export.s3-eu-west-1.amazonaws.com/tranquility/sde.zip"
DEFAULT_DB_FILE = "database/eve_sde.db"
DEFAULT_TMP_DIR = "database/data/tmp_sde"
DEFAULT_CONFIG_TABLES = "config/sde_tables.json"

# ----------------------------
# Helpers
# ----------------------------
def flatten_row(d):
    """Flatten dicts recursively; lists/dicts to JSON strings."""
    flat = {}
    for k, v in d.items():
        if isinstance(v, dict):
            nested = flatten_row(v)
            for nk, nv in nested.items():
                flat[f"{k}_{nk}"] = nv
        elif isinstance(v, list):
            flat[k] = json.dumps(v)
        else:
            flat[k] = v
    return flat

def sanitize_column_name(name: str) -> str:
    """Sanitize SQL column name to avoid conflicts with quotes."""
    return name.replace("-", "_").replace(" ", "_")

# ----------------------------
# SDE download & extraction
# ----------------------------
def download_sde(url=SDE_URL, dest_dir=DEFAULT_TMP_DIR) -> str:
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
def import_sde_to_sqlite(sde_dir: str, db_file: str, tables_to_import: list):
    db = DatabaseManager(db_file)

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
                        data = [v for k, v in data.items()]
                    elif not isinstance(data, list):
                        data = []

                    data = [flatten_row(row) for row in data]
                    df = pd.DataFrame(data)
                    df = df.loc[:, ~df.columns.duplicated()]
                    df.columns = [sanitize_column_name(c) for c in df.columns]

                    db.save_df(df, table_name)
                    print(f" -> Imported {len(df)} rows into '{table_name}'")
                except Exception as e:
                    print(f" !!! Failed to import '{table_name}': {e}")

    print(f"All selected SDE tables imported to {db_file}")

# ----------------------------
# Cleanup
# ----------------------------
def cleanup_temp(dest_dir=DEFAULT_TMP_DIR):
    if os.path.exists(dest_dir):
        print(f"Cleaning up temporary folder {dest_dir} ...")
        shutil.rmtree(dest_dir)
        print("Cleanup done.")
    else:
        print(f"No temporary folder found at {dest_dir}")

# ----------------------------
# CLI
# ----------------------------
def main():
    parser = argparse.ArgumentParser(
        description=f"EVE Online SDE Importer {APP_VERSION} (YAML -> SQLite)",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--version", action="version", version=f"EVE Online SDE Importer {APP_VERSION}")
    parser.add_argument("--download", action="store_true", help="Download and extract the SDE")
    parser.add_argument("--import", dest="do_import", action="store_true", help="Import selected YAML tables into SQLite")
    parser.add_argument("--cleanup", action="store_true", help="Cleanup temporary SDE folder")
    parser.add_argument("--db", default=DEFAULT_DB_FILE, help="SQLite database file path")
    parser.add_argument("--tmp", default=DEFAULT_TMP_DIR, help="Temporary folder for SDE extraction")
    parser.add_argument("--tables", default=DEFAULT_CONFIG_TABLES, help="JSON file listing tables to import")
    args = parser.parse_args()

    # Show help if no arguments
    if not any(vars(args).values()):
        parser.print_help()
        return

    # Load tables from config
    if not os.path.exists(args.tables_config):
        raise FileNotFoundError(f"{args.tables_config} not found.")
    
    tables_to_import = json.loads(open(args.tables_config, "r", encoding="utf-8").read())

    sde_path = args.tmp

    if args.download:
        sde_path = download_sde(dest_dir=args.tmp)

    if args.do_import:
        if not os.path.exists(sde_path):
            print(f"Temporary SDE folder '{sde_path}' not found. Run --download first.")
        else:
            import_sde_to_sqlite(sde_path, db_file=args.db, tables_to_import=tables_to_import)

    if args.cleanup:
        cleanup_temp(dest_dir=args.tmp)


if __name__ == "__main__":
    main()