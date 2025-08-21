# EVE Online Industry Tracker

A Python-based project to track EVE Online characters, corporations, and industry data.  
It uses **EVE Static Data Export (SDE)**, **ESI API** calls, and provides a **Streamlit dashboard** for viewing character and database information.

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

### EVE SDE Import
- Download and extract EVE Static Data Export (SDE) package.
- Import selected YAML tables into SQLite.
- Flatten nested dictionaries and convert lists to JSON strings.
- CLI options:
  - `--download`: download and extract SDE.
  - `--import`: import YAML tables into SQLite.
  - `--cleanup`: remove temporary SDE folder.
  - `--db`: specify SQLite database path (default: `database/eve_sde.db`).
  - `--tmp`: specify temporary folder for extraction (default: `database/data/tmp_sde`).
  - `--help`: show CLI usage instructions.

Selected SDE tables imported by default:

- `mapRegions`
- `staStations`
- `chrRaces`
- `chrFactions`
- `industryBlueprints`
- `industryActivity`
- `industryActivitySkills`
- `industryActivityMaterials`
- `industryActivityProducts`
- `industryActivityProbabilities`
- `invTypes`
- `invGroups`
- `invCategories`

> More tables can be added as the project evolves.

### Streamlit Dashboard
- Characters tab: shows cards for each character with images, wallet balance (formatted in ISK), birthday (with age), gender, corporation ID, bloodline ID, race ID, and security status.
- Database Maintenance tab:
  - Select database and table to view.
  - Refresh database and table lists.
  - Filter tables using SQL `WHERE` clauses.

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

```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

pip install -r requirements.txt
```

3. Ensure config files exist:

config/config.json

config/secret.json (fill in your EVE client secret)

Usage
Import EVE SDE
Download, import, and cleanup:

```bash
# Download and extract
python ./scripts/import_sde.py --download

# Import selected tables into SQLite
python ./scripts/import_sde.py --import

# Cleanup temporary files
python ./scripts/import_sde.py --cleanup
```

Run Streamlit Dashboard
```bash
streamlit run dashboard.py
```

Navigate between Characters and Database Maintenance tabs.

Refresh database and table lists dynamically.

Search database tables using SQL WHERE clauses.

Project Structure
.

├── config/

│   ├── config.json

│   ├── secret.json

│   └── sde_tables.json      # list of SDE tables to import

├── database/

│   ├── eve_data.db          # main project database

│   └── eve_sde.db           # imported SDE database

├── scripts/

│   └── import_sde.py

├── classes/

│   ├── database_manager.py

│   └── config_manager.py

├── utils/

│   └── formatters.py

├── dashboard.py

├── requirements.txt

└── README.md


Notes

Only a subset of SDE tables are imported initially. You can add more by updating config/sde_tables.json.

The project uses sqlite for simplicity; for large-scale use, consider a more robust DBMS.

The dashboard relies on Streamlit; changes to source files require a manual refresh.

License

MIT License
