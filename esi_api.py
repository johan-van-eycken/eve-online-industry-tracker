import requests
import json
import socketserver
import webbrowser
from urllib.parse import urlencode
from pathlib import Path
from typing import Any, Dict

CONFIG_FILE = Path("config.json")
PORT = 8080
REDIRECT_URI = f"http://localhost:{PORT}/callback"
ESI_BASE = "https://esi.evetech.net/latest"

def load_config() -> Dict[str, Any]:
    """Load configuration from config.json."""
    if CONFIG_FILE.exists():
        return json.loads(CONFIG_FILE.read_text())
    else:
        raise FileNotFoundError("config.json not found")

def save_config(cfg: Dict[str, Any]) -> None:
    """Save configuration to config.json."""
    CONFIG_FILE.write_text(json.dumps(cfg, indent=4))

def get_authorization_code(client_id: str, scope: str) -> str:
    """Start a local server and open browser for OAuth authorization code."""
    from oauth import OAuthHandler  # Import here to avoid circular import
    with socketserver.TCPServer(("localhost", PORT), OAuthHandler) as httpd:
        params = {
            "response_type": "code",
            "redirect_uri": REDIRECT_URI,
            "client_id": client_id,
            "scope": scope,
            "state": "eve_auth"
        }
        auth_url = "https://login.eveonline.com/v2/oauth/authorize/?" + urlencode(params)
        print("Opening browser for OAuth login...")
        webbrowser.open(auth_url)
        httpd.handle_request()  # blocks here until code received
        return httpd.code

def get_access_token() -> str:
    """Obtain a valid ESI access token, refreshing or authorizing as needed."""
    cfg = load_config()
    client_id = cfg["client_id"]
    client_secret = cfg["client_secret"]
    scope = "esi-assets.read_assets.v1 esi-wallet.read_character_wallet.v1"

    try:
        if not cfg.get("refresh_token"):
            # Get authorization code via local server
            code = get_authorization_code(client_id, scope)

            # Exchange authorization code for tokens
            token_resp = requests.post(
                "https://login.eveonline.com/v2/oauth/token",
                auth=(client_id, client_secret),
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": REDIRECT_URI,
                },
                timeout=10
            )
            token_resp.raise_for_status()
            token_data = token_resp.json()
            cfg["refresh_token"] = token_data["refresh_token"]
            save_config(cfg)
        else:
            code = None

        # Use refresh token to get access token
        token_resp = requests.post(
            "https://login.eveonline.com/v2/oauth/token",
            auth=(client_id, client_secret),
            data={
                "grant_type": "refresh_token",
                "refresh_token": cfg["refresh_token"],
            },
            timeout=10
        )
        token_resp.raise_for_status()
        token_data = token_resp.json()

        # Save refresh token if it changed
        if token_data.get("refresh_token") and token_data["refresh_token"] != cfg.get("refresh_token"):
            cfg["refresh_token"] = token_data["refresh_token"]
            save_config(cfg)

        return token_data["access_token"]
    except requests.RequestException as e:
        print(f"Error obtaining access token: {e}")
        raise

def verify_token(token: str) -> Dict[str, Any]:
    """Verify an ESI access token and return its payload."""
    url = "https://login.eveonline.com/oauth/verify"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"Error verifying token: {e}")
        raise

def esi_get(endpoint: str, token: str) -> Any:
    """Perform an authenticated GET request to the ESI API."""
    url = ESI_BASE + endpoint  # build full URL
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "eve-online-industry-tracker/1.0"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"Error fetching ESI data: {e}")
        raise