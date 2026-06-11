# EVE Online Industry Tracker

A local-first EVE Online helper app for industry and market analysis. Combines a Flask API backend with a Streamlit UI frontend, all backed by local SQLite databases.

## Features

- **EVE SSO OAuth** — multi-character authentication via the official EVE login flow
- **Industry Builder** — full manufacturing cost calculator with invention support, FIFO inventory cost basis, submanufacturing chains, and live market order pricing
- **Realised Profit** — ledger view of wallet journal and transaction history per character/corporation
- **Market Analysis** — liquidity metrics, price anomaly detection, and manufacturing signals
- **Corporation Support** — director-level access to corp wallet, assets, and industry jobs
- **SDE Integration** — local import of the EVE Static Data Export for offline game data lookups
- **Public Structures Cache** — background scan to resolve player-owned structure names
- **ESI Monitoring** — admin panel for tracking ESI request rates and error budgets

---

## Prerequisites

- Python 3.10–3.12 (3.13+ not yet supported due to pinned dependencies)
- An [EVE Online developer application](https://developers.eveonline.com/applications) with the following scopes:

```
esi-industry.read_character_jobs.v1
esi-industry.read_corporation_jobs.v1
esi-assets.read_assets.v1
esi-assets.read_corporation_assets.v1
esi-wallet.read_character_wallet.v1
esi-wallet.read_corporation_wallets.v1
esi-markets.read_character_orders.v1
esi-corporations.read_structures.v1
esi-universe.read_structures.v1
```

Set the callback URL to: `http://localhost:8765/callback`

---

## Quickstart

### macOS / Linux

```bash
git clone https://github.com/johan-van-eycken/eve-online-industry-tracker.git
cd eve-online-industry-tracker

python3 -m venv .venv
source .venv/bin/activate

pip install -U pip setuptools wheel
pip install -e . --no-build-isolation

# Import the EVE Static Data Export (required on first run)
python3 scripts/import_sde.py --download --import

# Start the app
python3 -m eve_online_industry_tracker
```

### Windows

```powershell
git clone https://github.com/johan-van-eycken/eve-online-industry-tracker.git
cd eve-online-industry-tracker

py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install -U pip setuptools wheel
python -m pip install -e . --no-build-isolation

# Import the EVE Static Data Export (required on first run)
python scripts/import_sde.py --download --import

# Start the app
python -m eve_online_industry_tracker
```

Once running, open **http://localhost:8501** in your browser.

---

## Configuration

### `config/secret.json`

Create this file before first launch (it is gitignored):

```json
{
    "client_id": "your_eve_app_client_id",
    "client_secret": "your_eve_app_client_secret",
    "characters": [
        {
            "character_name": "Your Main",
            "is_main": true,
            "is_corp_director": true
        },
        {
            "character_name": "Your Alt",
            "is_main": false,
            "is_corp_director": false
        }
    ]
}
```

### `config/config.json`

Tracked in the repo. Contains app settings, database paths, and ESI configuration. Most defaults work out of the box.

### `config/import_sde.json`

Controls which SDE tables are imported and where the SQLite file is stored.

### Environment variable overrides

| Variable | Default | Description |
|---|---|---|
| `APP_CONFIG_PATH` | `config/config.json` | Path to main config |
| `APP_SECRET_PATH` | `config/secret.json` | Path to secrets file |
| `LOG_LEVEL` | `DEBUG` | Logging verbosity |
| `FLASK_HOST` | `localhost` | Flask bind host |
| `FLASK_PORT` | `5000` | Flask bind port |
| `FLASK_DEBUG` | `false` | Flask debug mode |
| `FLASK_HEALTH_POLL_TIMEOUT` | `300` | Max seconds to wait for backend readiness |
| `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN` | `true` | Scan for public structures at startup |
| `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_CAP` | `5000` | Max structures to scan |
| `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_TIME_BUDGET` | `60` | Max seconds for startup scan |

---

## Usage

### Full app (recommended)

Starts Flask + Streamlit and automatically restarts either process if it crashes:

```bash
python -m eve_online_industry_tracker
```

CLI options:

```bash
python -m eve_online_industry_tracker --no-streamlit       # API only, no UI
python -m eve_online_industry_tracker --log-level INFO     # reduce log noise
python -m eve_online_industry_tracker --help
```

### SDE import

```bash
# Download, import, and clean up in one step
python scripts/import_sde.py --all

# Or step by step
python scripts/import_sde.py --download
python scripts/import_sde.py --import

# Check if a newer SDE version is available
python scripts/import_sde.py --check-version

# Force re-import even if version is current
python scripts/import_sde.py --download --import --force
```

---

## Project layout

```
.
├── src/
│   ├── eve_online_industry_tracker/   # main package + CLI entrypoint
│   ├── flask_app/                     # Flask API
│   ├── webpages/                      # Streamlit pages
│   ├── classes/                       # domain classes (ESI client, DB manager, etc.)
│   ├── config/                        # config schemas and path helpers
│   └── utils/
├── config/
│   ├── config.json
│   ├── secret.json                    # gitignored — create locally
│   └── import_sde.json
├── database/                          # local SQLite databases (gitignored)
├── scripts/
│   └── import_sde.py
├── streamlit_app.py
└── main.py                            # alternative entrypoint
```

---

## Troubleshooting

**`No module named eve_online_industry_tracker`**
Run `pip install -e . --no-build-isolation` from the repo root with your venv active.

**`SDE database is missing table "races"`**
The SDE hasn't been imported yet. Run:
```bash
python scripts/import_sde.py --download --import --force
```

**`SSL certificate verification failed`**
You are likely behind a corporate proxy (e.g. Zscaler). Install truststore so Python uses your system certificate store:
```bash
pip install truststore
```

**Startup shows `/health` 503 for a while**
Normal — initialization runs in the background. Wait until the log shows `Flask is ready!` or open http://localhost:8501 and it will load once ready.

**`UNIQUE constraint failed: oauth_characters.character_id`**
A character is already registered in the OAuth database. Safe to ignore on startup.
