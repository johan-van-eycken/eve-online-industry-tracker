import requests
import json
import webbrowser
from urllib.parse import urlencode
from pathlib import Path

CONFIG_FILE = Path("config.json")
ESI_BASE = "https://esi.evetech.net/latest"

def load_config():
    return json.loads(CONFIG_FILE.read_text())

def save_config(cfg):
    CONFIG_FILE.write_text(json.dumps(cfg, indent=4))

def get_access_token():
    cfg = load_config()
    if not cfg["refresh_token"]:
        # First time: ask user to login
        auth_url = (
            "https://login.eveonline.com/v2/oauth/authorize/?" +
            urlencode({
                "response_type": "code",
                "redirect_uri": cfg["callback_url"],
                "client_id": cfg["client_id"],
                "scope": "esi-assets.read_assets.v1 esi-wallet.read_character_wallet.v1",
                "state": "eve_auth"
            })
        )
        print("Open this URL in your browser and paste the 'code' parameter from the redirect:")
        print(auth_url)
        webbrowser.open(auth_url)
        code = input("Paste code: ").strip()
        
        token_resp = requests.post(
            "https://login.eveonline.com/v2/oauth/token",
            auth=(cfg["client_id"], cfg["client_secret"]),
            json={"grant_type": "authorization_code", "code": code}
        ).json()
        
        cfg["refresh_token"] = token_resp["refresh_token"]
        save_config(cfg)

    # Always refresh token before call
    token_resp = requests.post(
        "https://login.eveonline.com/v2/oauth/token",
        auth=(cfg["client_id"], cfg["client_secret"]),
        json={"grant_type": "refresh_token", "refresh_token": cfg["refresh_token"]}
    ).json()
    return token_resp["access_token"]

def esi_get(endpoint, token):
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{ESI_BASE}{endpoint}", headers=headers)
    r.raise_for_status()
    return r.json()
