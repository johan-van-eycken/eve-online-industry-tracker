# EVE Online Industry Tracker

A Python-based project to track EVE Online characters, corporations, and industry data.  
It uses **EVE Static Data Export (SDE)**, **ESI API** calls, provides a **Streamlit dashboard** for viewing character and database information, and integrates with **Flask** for backend operations.

---

## Features

### Database Management
- Centralized SQLite database management via `DatabaseManager` class.
- Supports multiple databases.
- Tables can be listed, loaded into pandas DataFrames, and saved back.
- ESI cache table included for storing API responses with etags.

### Character Management
- Add, update, and retrieve characters using `CharacterManager`.
- Supports main character designation.
- Stores refresh tokens and ESI scopes.
- Streamlit dashboard displays character portraits, wallet balance, birthday, security status, and other metadata.
- **New:** Flask integration allows server-side logic (e.g., wallet balance refreshes) to be managed in an independent API backend.

### EVE SDE Import
- Download and extract EVE Static Data Export (SDE) package.
- Import selected YAML tables into SQLite.
- Stores original nested dictionaries as JSON strings instead of flattening.
- CLI options:
  - `--download`: download and extract SDE.
  - `--import`: import YAML tables into SQLite.
  - `--cleanup`: remove temporary SDE folder.
  - `--db`: specify SQLite database path (default: `database/eve_sde.db`).
  - `--tmp`: specify temporary folder for extraction (default: `database/data/tmp_sde`).
  - `--tables`: override tables to import (space-separated list).  
    If omitted, defaults to `config/import_sde.json`.
  - `--all`: download, import, and cleanup in one go.
  - `--help`: show CLI usage instructions.

Selected SDE tables imported by default from `config/import_sde.json`:
> More tables can be added by editing `config/import_sde.json`.

### Flask Backend
- **API endpoint**: `/refresh_wallet_balances` for refreshing wallet balances.
- Accepts optional `character_name` to refresh specific character balances.
- Returns response with updated wallet balances.

### Streamlit Dashboard
- Characters tab: shows cards for each character with images, wallet balance (formatted in ISK), birthday (with age), gender, corporation ID, bloodline ID, race ID, and security status.
- Database Maintenance tab:
  - Select database and table to view.
  - Refresh database and table lists dynamically.
  - Filter tables using SQL `WHERE` clauses.
  - Drop tables from the database.

### Utils
- Formatters for ISK, dates with age, and security status rounding.
- Can be reused across different scripts and the dashboard.

---

## Installation

1. Clone this repository:

```bash
git clone https://github.com/jveyc/eve-online-industry-tracker.git
cd eve-online-industry-tracker
```

2. Create a virtual environment and install dependencies:

Recommended: use a local `.venv` (isolated from global Python).

Windows (PowerShell):

```powershell
./scripts/setup_venv.ps1
./.venv/Scripts/Activate.ps1
```

Manual setup (cross-platform):

```bash
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate

python -m pip install -r requirements.txt

# Needed for editable installs (provides the 'bdist_wheel' command):
python -m pip install wheel

# This repository uses a `src/` layout, so install in editable mode:
python -m pip install -e . --no-build-isolation
```

If pip fails with `SSLCertVerificationError`, you likely need to configure your
corporate proxy / trust store (or use an internal PyPI mirror). As a pragmatic
fallback (less isolated, but works without downloading), you can create the venv
with system site-packages:

```bash
python -m venv --system-site-packages .venv
```

3. Ensure config files exist:

```txt
config/config.json
config/secret.json (fill in your EVE client secret; you can also put your character list here)
config/import_sde.json (list of SDE tables to import)
```

## Usage

### Import EVE SDE

Download, import, and cleanup with one command:

```bash
python ./scripts/import_sde.py --all
```

Or run steps individually:

```bash
# Download and extract
python ./scripts/import_sde.py --download

# Import selected tables from config/import_sde.json
python ./scripts/import_sde.py --import

# Import custom tables (overrides config)
python ./scripts/import_sde.py --import --tables invTypes invGroups

# Cleanup temporary files
python ./scripts/import_sde.py --cleanup
```

### Run Streamlit App (Frontend)

Serve the Streamlit dashboard:

```bash
streamlit run streamlit_app.py
```

### Run Flask App (Backend)

Serve the Flask API:

```bash
python -m flask_app
```

### Runtime settings (env vars)

The backend behavior is controlled via environment variables:

- `FLASK_HOST` (default: `localhost`)
- `FLASK_PORT` (default: `5000`)
- `FLASK_DEBUG` (default: `false`)
- `FLASK_REFRESH_METADATA` (default: `true`) – controls DB metadata creation on startup
- `FLASK_API_REQUEST_TIMEOUT` (default: `10`) – seconds for UI → API requests
- `FLASK_HEALTH_POLL_TIMEOUT` (default: `120`) – seconds to wait for backend readiness in `main.py`
- `FLASK_HEALTH_REQUEST_TIMEOUT` (default: `2`) – per-request timeout for `/health`
- `LOG_LEVEL` (default: `INFO` for `python -m flask_app`, `DEBUG` for `python main.py`)

The app configuration file locations are also configurable:

- `APP_CONFIG_PATH` (default: `config/config.json`)
- `APP_SECRET_PATH` (default: `config/secret.json`)

Features:

* Refresh wallet balances via /refresh_wallet_balances endpoint.
* Fully integrated with CharacterManager logic for server-side operations.

## Project Structure

```plaintext
.
├── src/
│   ├── classes/
│   ├── config/
│   ├── flask_app/
│   ├── utils/
│   └── webpages/
├── config/
│   ├── config.json
│   ├── secret.json          # gitignored (secrets + optional character list)
│   └── import_sde.json      # list of SDE tables to import
├── database/
│   ├── eve_oauth.db
│   ├── eve_app.db
│   └── eve_sde.db
├── scripts/
│   └── import_sde.py
├── main.py
├── README.md
├── requirements.txt
└── streamlit_app.py
```

## Notes

* Only a subset of SDE tables are imported initially. Add more via config/import_sde.json.
* The project uses SQLite for simplicity; for large-scale use, consider a more robust DBMS.
* Changes to Streamlit source files require a manual refresh.
* Flask and Streamlit apps can run in parallel on different ports.

## License

MIT License
