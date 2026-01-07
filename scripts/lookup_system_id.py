from __future__ import annotations

import os
import sys


def main() -> int:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src = os.path.join(repo_root, "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    from utils.app_init import load_config, init_db_managers  # noqa: WPS433
    from flask_app.services.sde_context import get_language  # noqa: WPS433
    from flask_app.state import state  # noqa: WPS433
    from classes.database_models import MapSolarSystems  # noqa: WPS433
    from flask_app.services.sde_localization import parse_localized  # noqa: WPS433

    name = " ".join(sys.argv[1:]).strip() if len(sys.argv) > 1 else ""
    if not name:
        print("Usage: python scripts/lookup_system_id.py <solar system name>")
        return 2

    state.cfg_manager = load_config()
    state.db_oauth, state.db_app, state.db_sde = init_db_managers(state.cfg_manager, refresh_metadata=False)

    session = state.db_sde.Session()
    try:
        lang = get_language()
        # SDE names are localized blobs; do a brute-force scan but keep it tight.
        rows = session.query(MapSolarSystems).all()
        matches = []
        needle = name.casefold()
        for r in rows:
            n = parse_localized(r.name, lang) or ""
            if n.casefold() == needle:
                matches.append((r.id, n, r.securityStatus, r.regionID, r.constellationID))

        if not matches:
            # fall back to contains
            for r in rows:
                n = parse_localized(r.name, lang) or ""
                if needle in n.casefold():
                    matches.append((r.id, n, r.securityStatus, r.regionID, r.constellationID))

        if not matches:
            print(f"No solar system found matching '{name}'.")
            return 1

        for (system_id, system_name, sec, region_id, constellation_id) in matches[:25]:
            print(
                f"{system_name}: system_id={system_id} sec={sec} region_id={region_id} constellation_id={constellation_id}"
            )
        if len(matches) > 25:
            print(f"... and {len(matches) - 25} more")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
