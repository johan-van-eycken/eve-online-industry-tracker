#!/usr/bin/env python3
import os
import sys
import zipfile
import shutil
import requests  # pyright: ignore[reportMissingModuleSource]
import pandas as pd  # pyright: ignore[reportMissingModuleSource]
import yaml  # pyright: ignore[reportMissingModuleSource]
import argparse
import json
from datetime import datetime
from tqdm import tqdm

# Add project root to sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

from classes.config_manager import ConfigManager
from config.schemas import IMPORT_SDE_SCHEMA, SDE_VERSION_SCHEMA
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
# SDE Version Management
# ----------------------------
def init_sde_version_table(db: DatabaseManager):
    """Initialize the sde_version table if it doesn't exist."""
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {SDE_VERSION_SCHEMA['table']} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        build_number INTEGER NOT NULL UNIQUE,
        release_date TEXT NOT NULL,
        imported_at TEXT NOT NULL,
        is_current INTEGER DEFAULT 0
    )
    """
    db.execute(create_query)
    print(f"Initialized table '{SDE_VERSION_SCHEMA['table']}'")


def get_latest_sde_version(version_url: str) -> dict:
    """Fetch the latest SDE version from CCP's API."""
    try:
        print(f"Checking latest SDE version from {version_url}...")
        response = requests.get(version_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"Latest SDE version: {data['buildNumber']} (released: {data['releaseDate']})")
        return data
    except Exception as e:
        print(f"!!! Failed to fetch SDE version: {e}")
        return None


def get_current_sde_version(db: DatabaseManager) -> dict:
    """Get the current (most recent) SDE version from the database."""
    query = f"""
    SELECT build_number, release_date, imported_at
    FROM {SDE_VERSION_SCHEMA['table']}
    WHERE is_current = 1
    ORDER BY imported_at DESC
    LIMIT 1
    """
    result = db.query(query)
    if result:
        return {
            "build_number": result[0][0],
            "release_date": result[0][1],
            "imported_at": result[0][2],
        }
    return None


def is_newer_version(latest_build: int, current_build: int) -> bool:
    """Check if the latest build is newer than current."""
    return latest_build > current_build if current_build else True


def record_sde_version(db: DatabaseManager, build_number: int, release_date: str):
    """Record a new SDE version and mark it as current."""
    # Mark all previous versions as not current
    db.execute(f"UPDATE {SDE_VERSION_SCHEMA['table']} SET is_current = 0")

    # Check if this build already exists
    check_query = f"""
    SELECT id FROM {SDE_VERSION_SCHEMA['table']}
    WHERE build_number = :build_number
    """
    existing = db.query(check_query, {"build_number": build_number})

    imported_at = datetime.now().isoformat()

    if existing:
        # Update existing record
        update_query = f"""
        UPDATE {SDE_VERSION_SCHEMA['table']}
        SET release_date = :release_date,
            imported_at = :imported_at,
            is_current = 1
        WHERE build_number = :build_number
        """
        params = {
            "build_number": build_number,
            "release_date": release_date,
            "imported_at": imported_at,
        }
        db.execute(update_query, params)
        print(
            f"Updated SDE version {build_number} as current (released: {release_date})"
        )
    else:
        # Insert new version as current
        insert_query = f"""
        INSERT INTO {SDE_VERSION_SCHEMA['table']} (build_number, release_date, imported_at, is_current)
        VALUES (:build_number, :release_date, :imported_at, 1)
        """
        params = {
            "build_number": build_number,
            "release_date": release_date,
            "imported_at": imported_at,
        }
        db.execute(insert_query, params)
        print(
            f"Recorded SDE version {build_number} as current (released: {release_date})"
        )


def get_version_history(db: DatabaseManager) -> list:
    """Get all SDE version history."""
    query = f"""
    SELECT id, build_number, release_date, imported_at, is_current
    FROM {SDE_VERSION_SCHEMA['table']}
    ORDER BY imported_at DESC
    """
    return db.query(query)


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
        total_size = int(r.headers.get('content-length', 0))
        with open(zip_path, "wb") as f, tqdm(
            total=total_size, unit='B', unit_scale=True, desc="Download SDE"
        ) as pbar:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)
                pbar.update(len(chunk))
    else:
        print("SDE already downloaded.")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dest_dir)

    print(f"SDE extracted to {dest_dir}")
    return dest_dir


# ----------------------------
# Import YAML SDE tables to SQLite
# ----------------------------
def import_sde_to_sqlite(
    sde_dir: str, db_uri: str, tables_to_import: list, repackaged_json_path: str
):
    db = DatabaseManager(db_uri)
    repackaged_groups, repackaged_items = load_repackaged_volumes(repackaged_json_path)

    table_files = []
    for root, _, files in os.walk(sde_dir):
        for file in sorted(files):
            table_name = os.path.splitext(file)[0]
            if table_name in tables_to_import and file.endswith((".yaml", ".yml")):
                table_files.append((table_name, os.path.join(root, file)))

    tqdm.write("\n=== SDE Import ===")
    tqdm.write(f"Importing {len(table_files)} tables...\n")

    import_summary = []

    with tqdm(total=len(table_files), desc="Overall Progress", unit="table") as overall_pbar:
        for table_name, yaml_path in table_files:
            data = None
            try:
                with open(yaml_path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)

                if isinstance(data, dict):
                    items = list(data.items())
                    new_data = []
                    for k, v in items:
                        if isinstance(v, dict):
                            v = {"id": int(k), **v}
                        else:
                            v = {"id": int(k), "value": v}
                        new_data.append(v)
                    data = new_data
                elif not isinstance(data, list):
                    data = []

                data_flat = [flatten_row(row) for row in data]
                df = pd.DataFrame(data_flat)
                df = df.loc[:, ~df.columns.duplicated()]
                df.columns = [sanitize_column_name(c) for c in df.columns]

                if table_name == "types" and "id" in df.columns:
                    df["repackaged_volume"] = df["id"].map(repackaged_items)
                elif table_name == "groups" and "id" in df.columns:
                    df["repackaged_volume"] = df["id"].map(repackaged_groups)

                db.save_df(df, table_name)
                tqdm.write(f"{table_name:<25} | {len(df):>8} rows imported")
                import_summary.append((table_name, len(df)))
            except Exception as e:
                tqdm.write(f"{table_name:<25} | ERROR: {e}")
                import_summary.append((table_name, "ERROR"))
            overall_pbar.update(1)

    tqdm.write("\n=== Import Summary ===")
    tqdm.write(f"{'Table':<25} | {'Rows Imported':>12}")
    tqdm.write("-" * 40)
    for name, count in import_summary:
        tqdm.write(f"{name:<25} | {str(count):>12}")

    tqdm.write(f"\nAll selected SDE tables imported to {db_uri}\n")


# ----------------------------
# Cleanup
# ----------------------------
def cleanup_temp(dest_dir: str):
    if os.path.exists(dest_dir):
        print(f"Cleaning up temporary folder {dest_dir} ...")
        # Count files for progress bar
        files_to_delete = []
        for root, dirs, files in os.walk(dest_dir):
            for file in files:
                files_to_delete.append(os.path.join(root, file))
            for d in dirs:
                files_to_delete.append(os.path.join(root, d))
        with tqdm(total=len(files_to_delete), desc="Cleanup", unit="file") as pbar:
            for path in files_to_delete:
                try:
                    if os.path.isfile(path):
                        os.remove(path)
                    elif os.path.isdir(path):
                        shutil.rmtree(path)
                except Exception as e:
                    tqdm.write(f"Failed to delete {path}: {e}")
                pbar.update(1)
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
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"EVE Online SDE Importer {cfg.get('APP_VERSION')}",
    )
    parser.add_argument(
        "--all", action="store_true", help="Download, import, and cleanup in one go"
    )
    parser.add_argument(
        "--download", action="store_true", help="Download and extract the SDE"
    )
    parser.add_argument(
        "--import",
        dest="do_import",
        action="store_true",
        help="Import selected YAML tables into SQLite",
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Cleanup temporary SDE folder"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force download and import even if version is current",
    )
    parser.add_argument(
        "--check-version",
        action="store_true",
        help="Check SDE version without downloading",
    )
    parser.add_argument(
        "--version-history",
        action="store_true",
        help="Show SDE version import history",
    )
    parser.add_argument(
        "--db", default=cfg.get("DEFAULT_DB_URI"), help="SQLite database file path"
    )
    parser.add_argument(
        "--tmp",
        default=cfg.get("DEFAULT_TMP_DIR"),
        help="Temporary folder for SDE extraction",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Tables to import (default: TABLES_TO_IMPORT from config)",
    )

    args = parser.parse_args()

    # Show help if no arguments
    if not any(vars(args).values()):
        parser.print_help()
        return

    # Initialize database and version table
    db = DatabaseManager(args.db)
    init_sde_version_table(db)

    # Check version history
    if args.version_history:
        history = get_version_history(db)
        if history:
            print("\n=== SDE Version History ===")
            print(
                f"{'ID':<5} {'Build':<10} {'Release Date':<20} {'Imported At':<20} {'Current'}"
            )
            print("-" * 75)
            for row in history:
                current_marker = "✓" if row[4] == 1 else ""
                print(
                    f"{row[0]:<5} {row[1]:<10} {row[2]:<20} {row[3]:<20} {current_marker}"
                )
        else:
            print("No SDE version history found.")
        return

    # Check version
    version_url = cfg.get("SDE_VERSION")
    latest_version = get_latest_sde_version(version_url)

    if not latest_version:
        print("!!! Could not fetch latest SDE version. Exiting.")
        return

    current_version = get_current_sde_version(db)

    if args.check_version:
        print("\n=== SDE Version Status ===")
        if current_version:
            print(
                f"Current version: {current_version['build_number']} (imported: {current_version['imported_at']})"
            )
        else:
            print("Current version: None (no SDE imported yet)")
        print(
            f"Latest version:  {latest_version['buildNumber']} (released: {latest_version['releaseDate']})"
        )

        if current_version and not is_newer_version(
            latest_version["buildNumber"], current_version["build_number"]
        ):
            print("\n✓ You are using the latest SDE version.")
        else:
            print("\n⚠ A newer SDE version is available.")
        return

    # Check if update is needed
    needs_update = True
    if current_version and not args.force:
        if not is_newer_version(
            latest_version["buildNumber"], current_version["build_number"]
        ):
            print(
                f"\n✓ Current SDE version {current_version['build_number']} is up to date."
            )
            print(
                "   Use --force to re-download and import anyway, or --check-version to see details."
            )
            needs_update = False

    if not needs_update and not args.force:
        return

    if args.force:
        print("\n⚠ Force flag enabled - will download and import regardless of version.")

    if args.all:
        args.download = True
        args.do_import = True
        args.cleanup = True

    sde_path = args.tmp

    if args.download:
        # Clean up old SDE files before downloading new version
        if os.path.exists(sde_path):
            print(f"Removing old SDE files from {sde_path}...")
            cleanup_temp(sde_path)
        sde_path = download_sde(cfg.get("SDE_URL"), sde_path)

    if args.do_import:
        if not os.path.exists(sde_path):
            print(
                f"Temporary SDE folder '{sde_path}' not found. Running --download first."
            )
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

        import_sde_to_sqlite(
            sde_path,
            db_uri=args.db,
            tables_to_import=tables_to_import,
            repackaged_json_path=repackaged_json_path,
        )

        # Record the new version
        record_sde_version(
            db, latest_version["buildNumber"], latest_version["releaseDate"]
        )

    if args.cleanup:
        cleanup_temp(sde_path)


if __name__ == "__main__":
    main()