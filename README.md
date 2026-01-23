# EVE Online Industry Tracker

Local-first EVE Online helper app that combines:

- A Flask API backend (data refresh + background jobs)
- A Streamlit UI frontend
- Local SQLite databases for app data, OAuth tokens, and EVE SDE

The main “batteries included” entrypoint starts both backend and UI and keeps them running.

## Quickstart (Windows)

Requirements:

- Python 3.10–3.12 (Python 3.13 is currently not supported due to pinned deps/wheels).

```powershell
# From repo root
py -3.10 -m venv .venv
./.venv/Scripts/Activate.ps1

python -m pip install -U pip setuptools wheel

# This repo uses a src/ layout; editable install is the simplest way to run it
python -m pip install -e . --no-build-isolation

# First run will initialize databases and may take a bit
python -m eve_online_industry_tracker
```

## Installation

1) Clone the repository:

```bash
git clone https://github.com/jveyc/eve-online-industry-tracker.git
cd eve-online-industry-tracker
```

2) Create a virtualenv and install (recommended approach):

```bash
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate

python -m pip install -U pip setuptools wheel
python -m pip install -e . --no-build-isolation
```

If pip fails with `SSLCertVerificationError`, you likely need to configure your corporate proxy/trust store (or use an internal PyPI mirror).

As a pragmatic fallback (less isolated, but often works without downloads), you can create the venv with system site-packages:

```bash
python -m venv --system-site-packages .venv
```

## Configuration

This project expects a few JSON config files under `config/`.

- `config/config.json` (tracked)
- `config/secret.json` (should be gitignored; contains EVE client secret + optional character list)
- `config/import_sde.json` (tables + SDE download settings for the importer)

By default, the app reads the main config from `config/config.json` and secrets from `config/secret.json`.
You can override these paths via environment variables:

- `APP_CONFIG_PATH` (default: `config/config.json`)
- `APP_SECRET_PATH` (default: `config/secret.json`)

If `config/secret.json` is missing, the app will create a placeholder and ask you to fill in the EVE `client_secret`.

## Usage

### Run the full app (recommended)

Runs Flask + Streamlit and restarts either process if it crashes:

```bash
python -m eve_online_industry_tracker
```

After an editable install, you can also run the console script:

```bash
eve-online-industry-tracker
```

CLI flags:

```bash
python -m eve_online_industry_tracker --help
python -m eve_online_industry_tracker --no-streamlit
python -m eve_online_industry_tracker --log-level INFO
```

### Run backend only

Option A (via the supervisor, no UI):

```bash
python -m eve_online_industry_tracker --no-streamlit
```

Option B (run Flask directly):

```bash
python -m flask_app
```

### Run UI only

```bash
streamlit run streamlit_app.py
```

Note: the Streamlit UI expects the Flask API to be reachable (defaults to `http://localhost:5000`).

### Import / update the EVE SDE

The importer downloads the EVE Static Data Export and imports selected YAML tables into a local SQLite DB.

```bash
python ./scripts/import_sde.py --help

# Download + import + cleanup
python ./scripts/import_sde.py --all
```

The default list of tables comes from `config/import_sde.json`.

## Runtime settings (environment variables)

General:

- `LOG_LEVEL` (overrides default logging level; e.g. `DEBUG`, `INFO`)
- `LOG_FORCE` (set to `0`/`false` to avoid reconfiguring logging)

Flask server:

- `FLASK_HOST` (default: `localhost`)
- `FLASK_PORT` (default: `5000`)
- `FLASK_DEBUG` (default: `false`)
- `FLASK_REFRESH_METADATA` (default: `true`) – best-effort DB metadata initialization during startup

Supervisor / health checks:

- `FLASK_HEALTH_POLL_TIMEOUT` (default: `120`) – max time to wait for backend readiness
- `FLASK_HEALTH_REQUEST_TIMEOUT` (default: `2`) – per-request timeout for `/health`
- `FLASK_API_REQUEST_TIMEOUT` (default: `10`) – UI → API request timeout

Public structures caching & global scan:

- `FLASK_PUBLIC_STRUCTURES_TTL` (default: `3600`) – cache freshness window (seconds)
- `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN` (default: `true`) – enable a bounded background global scan at startup
- `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_WORKERS` (default: `10`)
- `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_CAP` (default: `5000`)
- `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_TIME_BUDGET` (default: `60`)
- `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_BATCH_SIZE` (default: `100`)
- `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_PAUSE` (default: `5`)
- `FLASK_PUBLIC_STRUCTURES_ESI_TIMEOUT` (default: `5`) – per-ESI-request timeout during scans

## Project layout

```text
.
├── src/
│   ├── eve_online_industry_tracker/   # main package + CLI entrypoint
│   ├── flask_app/                    # Flask API app
│   ├── webpages/                     # Streamlit pages
│   ├── classes/                      # legacy domain/services (gradually being wrapped)
│   ├── config/                       # config schema + path helpers
│   └── utils/
├── config/
│   ├── config.json
│   ├── secret.json                   # expected locally; should not be committed
│   └── import_sde.json
├── database/                         # local SQLite DBs
├── scripts/
│   └── import_sde.py
├── streamlit_app.py
├── main.py                           # legacy runner (kept for compatibility)
└── README.md
```

## Troubleshooting

- `No module named eve_online_industry_tracker`: you likely didn’t install the project. Use `python -m pip install -e . --no-build-isolation`.
- `SSLCertVerificationError`: configure your proxy/trust store or use a trusted/internal package index. As a fallback you can create the venv with `--system-site-packages`.
- Startup shows `/health` 503 for a while: initialization is intentionally done in the background; wait until it reports `status=OK`.
