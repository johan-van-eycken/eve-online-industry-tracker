"""Register characters via EVE SSO one at a time.

Usage: python scripts/register_characters.py [character_name ...]
If no names given, registers all characters missing OAuth tokens.
"""
import sys
import os
import logging
import time
import webbrowser
import re
from urllib.parse import urlencode

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import requests
from utils.app_init import load_config, init_db_oauth
from eve_online_industry_tracker.infrastructure.models import OAuthCharacter
from eve_online_industry_tracker.infrastructure.oauth import OAuthServer, OAuthHandler
from eve_online_industry_tracker.infrastructure.esi_client import get_requests_ssl_kwargs

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

TIMEOUT_SECONDS = 120


def _extract_character_info_from_jwt(access_token):
    """Extract character_id and character_name from EVE SSO JWT access token."""
    try:
        import jwt as pyjwt
        claims = pyjwt.decode(access_token, options={"verify_signature": False})
        sub = claims.get("sub", "")
        char_name = claims.get("name", "")
        match = re.search(r":(\d+)$", sub)
        character_id = int(match.group(1)) if match else None
        return character_id, char_name
    except Exception:
        pass

    # Fallback: verify via ESI endpoint
    try:
        verify_url = "https://login.eveonline.com/oauth/verify"
        response = requests.get(
            verify_url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
            **get_requests_ssl_kwargs(),
        )
        response.raise_for_status()
        data = response.json()
        return data.get("CharacterID"), data.get("CharacterName", "")
    except Exception as e:
        logging.error(f"Failed to extract character info: {e}")
        return None, ""


def register_character(cfg, db_path, char_cfg):
    name = char_cfg["character_name"]

    # Check if already registered
    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT refresh_token FROM oauth_characters WHERE character_name = ?", (name,))
    row = cursor.fetchone()
    conn.close()
    if row and row[0]:
        print(f"  {name} already has tokens, skipping.")
        return True

    all_scopes = set(cfg.get("defaults")["scopes"])
    if char_cfg.get("is_corp_director", False):
        all_scopes.update(cfg.get("defaults")["scopes_corp_director"])
    scopes = " ".join(sorted(all_scopes))

    auth_params = {
        "response_type": "code",
        "client_id": cfg.get("oauth")["client_id"],
        "scope": scopes,
        "redirect_uri": cfg.get("app")["redirect_uri"],
        "state": "eve_auth",
    }
    auth_url = f"{cfg.get('esi')['auth_url']}?{urlencode(auth_params)}"

    print(f"\n{'='*60}")
    print(f"  Registering: {name}")
    print(f"  Log in with the correct EVE account for this character.")
    print(f"{'='*60}")
    print(f"\n  If the browser does not open, copy this URL:\n  {auth_url}\n")
    print(f"  Waiting up to {TIMEOUT_SECONDS}s for SSO callback on port 8080...")

    webbrowser.open(auth_url)

    streamlit_url = (cfg.get("app") or {}).get("streamlit_url", "http://localhost:8501")
    with OAuthServer(("localhost", 8080), OAuthHandler, return_to_url=streamlit_url) as httpd:
        httpd.timeout = TIMEOUT_SECONDS
        start_time = time.time()
        while httpd.code is None:
            httpd.handle_request()
            if time.time() - start_time > TIMEOUT_SECONDS:
                print(f"  ERROR: Timeout waiting for {name} SSO callback.")
                return False

        # Exchange code for tokens
        response = requests.post(
            cfg.get("esi")["token_url"],
            auth=(cfg.get("oauth")["client_id"], cfg.get("client_secret")),
            data={
                "grant_type": "authorization_code",
                "code": httpd.code,
                "redirect_uri": cfg.get("app")["redirect_uri"],
            },
            timeout=10,
            **get_requests_ssl_kwargs(),
        )
        response.raise_for_status()
        token_data = response.json()

        character_id, jwt_name = _extract_character_info_from_jwt(token_data["access_token"])
        if not character_id:
            print(f"  ERROR: Could not determine character_id for {name}")
            return False

        if jwt_name and jwt_name != name:
            print(f"  ERROR: You logged in as '{jwt_name}' but expected '{name}'!")
            print(f"         Please re-run and log in with the correct EVE account.")
            return False

        # Use raw SQL to avoid SQLAlchemy session/autoflush issues
        import sqlite3
        db_path = cfg.all()["app"]["database_oauth_uri"].replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Delete any existing record for this character_name or character_id
        cursor.execute("DELETE FROM oauth_characters WHERE character_name = ? OR character_id = ?", (name, character_id))
        cursor.execute(
            "INSERT INTO oauth_characters (character_name, character_id, refresh_token, access_token, token_expiry, scopes, is_main) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (name, character_id, token_data["refresh_token"], token_data["access_token"],
             int(time.time()) + token_data["expires_in"] - 30, scopes, 0),
        )
        conn.commit()
        conn.close()
        print(f"  {name} registered successfully! (character_id: {character_id})")
        return True


def main():
    cfg = load_config()
    db_path = cfg.all()["app"]["database_oauth_uri"].replace("sqlite:///", "")
    all_chars = cfg.all()["characters"]

    # Filter to requested names, or all if none specified
    requested = sys.argv[1:]
    if requested:
        chars = [c for c in all_chars if c["character_name"] in requested]
        missing = set(requested) - {c["character_name"] for c in chars}
        for m in missing:
            print(f"WARNING: Character '{m}' not found in config.")
    else:
        chars = all_chars

    if not chars:
        print("No characters to register.")
        return

    print(f"Characters to check: {', '.join(c['character_name'] for c in chars)}")

    for char_cfg in chars:
        if not register_character(cfg, db_path, char_cfg):
            print(f"\nFailed to register {char_cfg['character_name']}. Stopping.")
            sys.exit(1)

    print("\nAll characters registered!")


if __name__ == "__main__":
    main()
