# EVE Online Industry Tracker

A local-first EVE Online helper app for industry and market analysis. Combines a Flask API backend with a Streamlit UI frontend, all backed by local SQLite databases.

## Features

- **EVE SSO OAuth** — multi-character authentication via the official EVE login flow
- **Characters / Corporations** — wallet journal and transactions, assets and market orders per pilot; director-level access to the corp wallets, assets and industry jobs
- **Industry Builder** — manufacturing cost calculator with invention (decryptors, copies), sub-build chains, FIFO inventory cost basis, job fees, and market prices, liquidity, trends and price-anomaly checks per product
- **Industry Slots** — the pilots' manufacturing, research and reaction slots and their running jobs
- **Daily Planner** — a daily action plan: what to build, research, copy, invent, sell or deliver, assigned to the pilot with the skills and a free slot
- **Realized Profit** — FIFO ledger of the sales per character and corporation: net revenue, fees and allocated cost
- **Market Orders** — the open orders with pricing advice
- **Ore Calculator** — the cheapest ore mix (MILP, via PuLP) for a mineral need, with compression and the configured reprocessing facility
- **Settings** — industry profiles (structure, rigs, system) used by the calculators
- **Admin** — application settings, database maintenance, ESI monitoring (request rates and error budget) and a public-structures scan that resolves player-owned structure names
- **SDE Integration** — local import of the EVE Static Data Export for offline game data lookups

---

## Prerequisites

- Python 3.10–3.12 (3.13+ not yet supported due to pinned dependencies)
- An [EVE Online developer application](https://developers.eveonline.com/applications). Put its client id in `config/config.json` (`oauth.client_id`) and its secret in `secret.json` (see [Configuration](#configuration)). Give it the scopes the app requests (`defaults.scopes` and `defaults.scopes_corp_director` in `config/config.json`):

```
# Every character
esi-assets.read_assets.v1
esi-characters.read_blueprints.v1
esi-characters.read_corporation_roles.v1
esi-characters.read_standings.v1
esi-characters.read_titles.v1
esi-clones.read_implants.v1
esi-industry.read_character_jobs.v1
esi-markets.read_character_orders.v1
esi-markets.structure_markets.v1
esi-skills.read_skillqueue.v1
esi-skills.read_skills.v1
esi-universe.read_structures.v1
esi-wallet.read_character_wallet.v1

# Characters with is_corp_director: true, in addition
esi-assets.read_corporation_assets.v1
esi-corporations.read_blueprints.v1
esi-corporations.read_corporation_membership.v1
esi-corporations.read_divisions.v1
esi-corporations.read_facilities.v1
esi-corporations.read_standings.v1
esi-corporations.read_starbases.v1
esi-corporations.read_structures.v1
esi-corporations.read_titles.v1
esi-corporations.track_members.v1
esi-industry.read_corporation_jobs.v1
esi-markets.read_corporation_orders.v1
esi-wallet.read_corporation_wallets.v1
```

Set the callback URL to `http://localhost:8080/callback`. The login listener takes its port from `app.redirect_uri` in `config/config.json`, so change both together.

---

## Quickstart

### macOS / Linux

```bash
git clone --recurse-submodules https://github.com/johan-van-eycken/eve-online-industry-tracker.git
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
git clone --recurse-submodules https://github.com/johan-van-eycken/eve-online-industry-tracker.git
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

### `config/private/secret.json`

Stored in a private git submodule (`eve-online-industry-tracker-config`); `--recurse-submodules` pulls it only if you have access to it. Otherwise create the file at `config/private/secret.json` yourself. On the first start, each listed character goes through the EVE login once in the browser:

```json
{
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

Tracked in the repo. Contains app settings, database paths, the OAuth client id, the requested scopes and the ESI configuration. Set `oauth.client_id` to your own application's.

### `config/import_sde.json`

Controls which SDE tables are imported and where the SQLite file is stored.

### Application settings

Market defaults (such as the default broker fee and order book depth), industry and daily-planner thresholds, cache TTLs, performance, ESI resilience and the reprocessing facility are set in the UI (**Admin → Application Settings**) and stored in `database/admin_settings.json`.

### Environment variable overrides

| Variable | Default | Description |
|---|---|---|
| `APP_CONFIG_PATH` | `config/config.json` | Path to main config |
| `APP_SECRET_PATH` | `config/private/secret.json` | Path to secrets file |
| `ADMIN_SETTINGS_PATH` | `database/admin_settings.json` | Path to the application settings |
| `STREAMLIT_PREFERENCES_PATH` | `database/streamlit_preferences.json` | Path to the UI preferences |
| `LOG_LEVEL` | `DEBUG` | Logging verbosity |
| `FLASK_HOST` | `localhost` | Flask bind host |
| `FLASK_PORT` | `5000` | Flask bind port |
| `FLASK_DEBUG` | `false` | Flask debug mode |
| `FLASK_HEALTH_POLL_TIMEOUT` | `300` | Max seconds to wait for backend readiness |
| `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN` | `true` | Scan for public structures at startup |
| `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_CAP` | `5000` | Max structures to scan |
| `FLASK_PUBLIC_STRUCTURES_STARTUP_SCAN_TIME_BUDGET` | `60` | Max seconds for startup scan |
| `EIT_SSL_CA_BUNDLE` | — | CA bundle file for HTTPS (e.g. a corporate proxy's) |
| `EIT_SSL_VERIFY` | — | `0` turns HTTPS certificate checks off (last resort) |

More `FLASK_*` tuning variables (timeouts, scan workers, batch size) are listed in `src/flask_app/settings.py`.

---

## Usage

### Full app (recommended)

Starts Flask + Streamlit and automatically restarts either process if it crashes:

```bash
python -m eve_online_industry_tracker
```

CLI options:

```bash
python -m eve_online_industry_tracker --no-streamlit              # API only, no UI
python -m eve_online_industry_tracker --log-level INFO            # reduce log noise
python -m eve_online_industry_tracker --health-timeout-seconds 600  # wait longer for Flask
python -m eve_online_industry_tracker --help
```

`eve-online-industry-tracker` (installed by `pip install -e .`) and `python main.py` (no install needed) start the same launcher.

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

# Show the import history, remove the temporary download folder
python scripts/import_sde.py --version-history
python scripts/import_sde.py --cleanup
```

Stop the app before a re-import.

### Tests

```bash
pip install pytest
python -m pytest -q
```

About 400 tests read the local SDE (`database/eve_sde.db`), and a few read a git-ignored fixture of real overview rows (`tests/fixtures/overview_rows_real.json`). Without those files these tests are skipped, and the summary lists each skip with the missing file.

---

## Project layout

```
.
├── src/
│   ├── eve_online_industry_tracker/   # main package + CLI launcher (cli.py)
│   │   ├── application/               # services: industry, daily planner, characters, market, ...
│   │   ├── infrastructure/            # ESI client, SDE readers, persistence, DB models
│   │   └── config/                    # config manager, admin settings schema
│   ├── flask_app/                     # Flask API (routes, background jobs)
│   ├── streamlit_ui/                  # Streamlit pages, components and API client
│   ├── config/                        # config path helpers
│   └── utils/                         # logging, SSL, ESI monitor, startup checks
├── config/
│   ├── config.json
│   ├── import_sde.json
│   └── private/                       # private submodule
│       └── secret.json
├── database/                          # local SQLite databases and settings (gitignored)
├── scripts/
│   └── import_sde.py
├── tests/
├── docs/
├── streamlit_app.py                   # Streamlit entry (started by the launcher)
└── main.py                            # launcher without an install
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

**`SDE industry tables missing`** (an ERROR at startup, a `/health` warning, industry pages answering 503 `sde_reimport_required`)
The SDE was imported before build 3607705 or with an older importer, so it lacks CCP's industry tables (`industryTargetFilters`, `industryModifierSources`, `industryAssemblyLines`, `industryInstallationTypes`) that the rig groups and the shipyard gate are read from. Stop the app, re-import, then restart the app:
```bash
python scripts/import_sde.py --download --import --force
```
**`SDE tables missing: compressibleTypes`** or **`industryActivities`** (the same ERROR, `/health` warning and 503)
The SDE was imported with an older `config/import_sde.json`, so it lacks the ore compression table (the ore calculator reads it, with each ore's reprocessing skill from `typeDogma`) or the industry activity names (the industry jobs page). Re-import as above; the rig groups and the shipyard gate keep working meanwhile.

`--force` is needed when the recorded build is already current. The importer records the build of the files it imported (their `_sde.yaml`) and warns when they are older than the latest online build.

**`SSL certificate verification failed`**
You are likely behind a corporate proxy (e.g. Zscaler). The app already uses your system certificate store through `truststore` (installed with the requirements). If the proxy's certificate isn't in that store, point `EIT_SSL_CA_BUNDLE` at a CA bundle file that contains it.

**Startup shows `/health` 503 for a while**
Normal — initialization runs in the background. Wait until the log shows `Flask is ready!` or open http://localhost:8501 and it will load once ready.

**`UNIQUE constraint failed: oauth_characters.character_id`**
A character is already registered in the OAuth database. Safe to ignore on startup.

---

## Credits

- **CCP Games**: EVE Online and all EVE-related materials, including the Static Data Export, are the intellectual property of CCP hf. and are used under the [EVE Online Developer License Agreement](https://developers.eveonline.com/license-agreement). From the SDE come, among others, the types' packaged volumes and the industry tables (`industryTargetFilters`, `industryModifierSources`, `industryAssemblyLines`, `industryInstallationTypes`) behind the engineering- and reactor-rig groups and the shipyard gate. EVE Online and the EVE logo are trademarks of CCP hf. This project is not affiliated with or endorsed by CCP hf.
