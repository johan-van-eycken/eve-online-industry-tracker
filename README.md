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

## Getting started

Five steps, about 15 minutes. Run every command from the repository root (the folder with `README.md`): the config and database paths are relative to it.

### What you need

- **Python 3.11 or 3.12.** 3.10 has no wheels for the pinned numpy, and 3.13+ isn't supported yet. On Windows, install it from [python.org](https://www.python.org/downloads/) with the *py launcher* option on; `py -0` lists the versions it can start.
- **Git.**
- **About 500 MB of free disk.** The EVE Static Data Export (SDE) download is about 100 MB, and the imported database is about 220 MB.
- **An EVE Online account** for each character you want to track.

### 1. Create an EVE developer application

1. Sign in at [developers.eveonline.com/applications](https://developers.eveonline.com/applications) and create an application with *Authentication & API Access*.
2. Set the **callback URL** to `http://localhost:8080/callback`.
3. Give it the [scopes listed below](#eve-scopes). If none of your characters is a corporation director, the first list is enough.
4. Keep the page open: you need its **Client ID** and **Secret Key** in step 3.

### 2. Clone and install

macOS / Linux:

```bash
git clone https://github.com/johan-van-eycken/eve-online-industry-tracker.git
cd eve-online-industry-tracker

python3.12 -m venv .venv          # or python3.11
source .venv/bin/activate

python -m pip install -U pip setuptools wheel
python -m pip install -e . --no-build-isolation
```

Windows (PowerShell):

```powershell
git clone https://github.com/johan-van-eycken/eve-online-industry-tracker.git
cd eve-online-industry-tracker

py -3.12 -m venv .venv
# Once per user, if PowerShell says "running scripts is disabled on this system":
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
.\.venv\Scripts\Activate.ps1

python -m pip install -U pip setuptools wheel
python -m pip install -e . --no-build-isolation
```

Don't skip the `pip install -U pip setuptools wheel` line. A new venv has no setuptools, and `--no-build-isolation` (which keeps the install working behind a corporate proxy) needs it. Without it, the install fails with `Cannot import 'setuptools.build_meta'`.

### 3. Configure

All your own values go in **`config/private/secret.json`**. It isn't in the repo, so create it (UTF-8). If you start the app without it, the app writes this template for you and stops:

```json
{
    "client_id": "the Client ID from step 1",
    "client_secret": "the Secret Key from step 1",
    "user_agent": "eve-online-industry-tracker/1.0 (you@example.com)",
    "characters": [
        { "character_name": "Your Main", "is_main": true,  "is_corp_director": true },
        { "character_name": "Your Alt",  "is_main": false, "is_corp_director": false }
    ]
}
```

- `client_id` and `client_secret`: from your developer application.
- `user_agent`: your own contact. CCP asks every ESI client to identify itself.
- `character_name`: the exact in-game name.
- `is_main`: the character the UI picks by default. Mark one; without one, the first in the list becomes main.
- `is_corp_director`: this character also reads the corporation's wallets, assets, blueprints and industry jobs. It requests the corp scopes, and needs the in-game Director role. Without one marked, the app uses the first character, so the developer application needs the corp scopes too.
- `industry_enabled` (optional, default `true`): `false` keeps a character out of the industry planning.

(The owner keeps this file in a private git submodule. `git clone --recurse-submodules` only works with access to it; everyone else creates the file.)

### 4. Import the EVE Static Data Export

```bash
python scripts/import_sde.py --all
```

This downloads CCP's static game data (items, blueprints, map), imports it into `database/eve_sde.db` and removes the download. It takes a few minutes. The app won't start without it.

### 5. Start the app

```bash
python -m eve_online_industry_tracker
```

On the first start, your browser opens the EVE login once for each character in `secret.json`, one after another. Log in and authorize the scopes. You have 5 minutes per character. The tokens are stored in `database/eve_oauth.db`, so later starts skip the login.

When the log shows `Flask is ready!`, open **http://localhost:8501**. The first start also loads assets, wallets and jobs from ESI, which can take a few minutes.

To stop the app, press `Ctrl+C` in the terminal. Next time, activate the venv and run step 5 again.

### EVE scopes

The app requests these scopes (`defaults.scopes` and `defaults.scopes_corp_director` in `config/config.json`). Give your developer application all the scopes it will request:

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

The login listener takes its port from `app.redirect_uri` in `config/config.json`. If port 8080 is taken, change that and the callback URL in your developer application together.

---

## Configuration

### `config/config.json`

App settings, database paths, the requested scopes and the ESI configuration. The same for every user; your own values go in `secret.json`.

### `config/private/secret.json`

Your application's client id and secret, your ESI contact (`user_agent`) and your characters. See [step 3](#3-configure).

### `config/import_sde.json`

Controls which SDE tables are imported and where the SQLite file is stored.

### Application settings

Market defaults (such as the default broker fee and order book depth), industry and daily-planner thresholds, cache TTLs, performance, ESI resilience and the reprocessing facility are set in the UI (**Admin → Application Settings**) and stored in `database/admin_settings.json`.

### Environment variable overrides

| Variable | Default | Description |
|---|---|---|
| `APP_CONFIG_PATH` | `config/config.json` | Path to main config |
| `APP_SECRET_PATH` | `config/private/secret.json` | Path to the file with your own values |
| `ADMIN_SETTINGS_PATH` | `database/admin_settings.json` | Path to the application settings |
| `STREAMLIT_PREFERENCES_PATH` | `database/streamlit_preferences.json` | Path to the UI preferences |
| `LOG_LEVEL` | `DEBUG` | Logging verbosity |
| `FLASK_HOST` | `127.0.0.1` | Flask bind host |
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
python -m pip install pytest hypothesis
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
Run `python -m pip install -e . --no-build-isolation` from the repo root with your venv active.

**`requires a different Python: 3.x.y not in '<3.13,>=3.11'`**
The venv was made with an unsupported Python (often a bare `python3` that is newer than 3.12). Delete `.venv` and recreate it with `python3.12 -m venv .venv` (Windows: `py -3.12 -m venv .venv`).

**`SDE not imported yet (database/eve_sde.db does not exist)`**
Run step 4 of [Getting started](#4-import-the-eve-static-data-export), then start the app again.

**`secret.json created with placeholders` / `contains placeholder client_secret`**
Fill in `config/private/secret.json` as in [step 3](#3-configure), then start the app again.

**The EVE login times out, or the browser doesn't open**
Each character's login waits 5 minutes. If no browser opens, copy the login URL from the log into a browser. If the redirect to `localhost:8080` fails, another program holds port 8080: change `app.redirect_uri` and your application's callback URL together. On Windows, Hyper-V, WSL or Docker can reserve ports. `netsh interface ipv4 show excludedportrange protocol=tcp` lists them.

**`Expecting value: line 1 column 1 (char 0)` when reading secret.json**
The file isn't UTF-8. This happens on Windows with `echo ... > secret.json` in PowerShell 5, which writes UTF-16. Save it as UTF-8 from an editor.

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
