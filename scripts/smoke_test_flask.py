from __future__ import annotations

import os
import sys


def main() -> int:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src = os.path.join(repo_root, "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    from flask_app.app import create_app  # noqa: WPS433
    from flask_app.state import state  # noqa: WPS433

    app = create_app()
    client = app.test_client()

    # 1) Health should always respond.
    resp = client.get("/health")
    print("GET /health (not ready):", resp.status_code, resp.get_json(silent=True))

    # 2) Try a DB-only init (no ESI calls) so SDE-backed endpoints can work.
    try:
        from utils.app_init import load_config, init_db_managers  # noqa: WPS433

        state.cfg_manager = load_config()
        state.db_oauth, state.db_app, state.db_sde = init_db_managers(state.cfg_manager, refresh_metadata=False)
        state.init_state = "Ready"
        print("Initialized DB managers from config; setting init_state=Ready")
    except Exception as exc:
        state.init_state = "Ready"  # still exercise require_ready() paths
        print("DB-only init failed (continuing with stubs):", str(exc))

    for path in [
        "/materials",
        "/ores",
        "/solar_systems",
        "/npc_stations/30000142",
        "/industry_profiles/1",
        "/public_structures/30000142",
    ]:
        r = client.get(path)
        payload = r.get_json(silent=True)
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            data = payload.get("data")
            preview = data[0] if data else None
            print(f"GET {path}:", r.status_code, f"items={len(data)}", f"first={preview}")
        else:
            if payload is None:
                payload = r.data[:200].decode("utf-8", errors="replace")
            print(f"GET {path}:", r.status_code, payload)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
