import logging
import requests
import webbrowser
from urllib.parse import urlencode
from typing import Optional, Any, Dict, Tuple

from classes.oauth import OAuthHandler, OAuthServer
from classes.config_manager import ConfigManager
from classes.database_manager import CharacterManager


class ESIClient:
    def __init__(self, character_name: str, cfg: ConfigManager, db: CharacterManager, is_main: bool = False):
        self.cfg = cfg
        self.db = db

        # App configuratie
        self.port = cfg.get("app")["port"]
        self.redirect_uri = cfg.get("app")["redirect_uri"]
        self.user_agent = cfg.get("app")["user_agent"]

        # ESI configuratie
        self.esi_base = cfg.get("esi")["base"]
        self.auth_url = cfg.get("esi")["auth_url"]
        self.token_url = cfg.get("esi")["token_url"]
        self.verify_url = cfg.get("esi")["verify_url"]

        # OAuth configuratie
        self.client_id = cfg.get("oauth")["client_id"]
        self.scope = " ".join(cfg.get("defaults")["scopes"])
        self.client_secret = cfg.get("client_secret")

        # Character info
        self.character_name = character_name
        self.character_id: Optional[int] = None
        self.character_info: Optional[Dict[str, Any]] = None
        self.token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.is_main = is_main

        # Init flow: login of register indien character nog niet gekend
        self._init_or_register()

    # ----------------------------
    # Init helpers
    # ----------------------------
    def _init_or_register(self) -> None:
        existing = self.db.get_character(self.character_name)

        if existing:
            # Character gekend -> login met refresh token
            logging.info(f"Found {self.character_name} in DB, logging in...")
            self.refresh_token = existing["refresh_token"]
            self._login_with_refresh()
        else:
            # Character nieuw -> registreren via OAuth
            logging.info(f"{self.character_name} not in DB, starting OAuth registration...")
            self.register_new_character()

    # ----------------------------
    # Login helpers
    # ----------------------------
    def _login_with_refresh(self) -> None:
        access_token, new_refresh_token = self._get_access_token(self.refresh_token)
        self.token = access_token
        self.refresh_token = new_refresh_token
        self.character_info = self._verify_token()
        self.character_id = self.character_info["CharacterID"]

        # Refresh token updaten in DB indien gewijzigd
        existing = self.db.get_character(self.character_name)
        if existing and new_refresh_token != existing["refresh_token"]:
            logging.info(f"Updating refresh token for {self.character_name} in database.")
            self.db.update_character_refresh_token(self.character_name, new_refresh_token)

        logging.info(f"Logged in as {self.character_name}")

    # ---------------------------
    # Nieuw character registreren
    # ---------------------------
    def register_new_character(self) -> None:
        access_token, refresh_token = self._get_access_token()
        self.token = access_token
        self.refresh_token = refresh_token
        self.character_info = self._verify_token()
        self.character_id = self.character_info["CharacterID"]

        # Schrijf character in DB
        self.db.add_or_update_character(
            name=self.character_name,
            char_id=self.character_id,
            refresh_token=refresh_token,
            scopes=self.scope.split(" "),
            is_main=self.is_main
        )

        if self.is_main:
            logging.info(f"Character {self.character_name} registered and set as MAIN.")
        else:
            logging.info(f"Character {self.character_name} registered as alt.")

    # ----------------------------
    # Public ESI requests
    # ----------------------------
    def esi_get(self, endpoint: str) -> Any:
        if not self.token:
            raise RuntimeError("No access token available. Please login first.")
        url = self.esi_base + endpoint
        headers = {
            "Authorization": f"Bearer {self.token}",
            "User-Agent": self.user_agent
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------
    # OAuth helpers
    # ----------------------------
    def _get_access_token(self, refresh_token: Optional[str] = None) -> Tuple[str, str]:
        if refresh_token:
            token_resp = requests.post(
                self.token_url,
                auth=(self.client_id, self.client_secret),
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token
                }, timeout=10
            )
        else:
            code = self._get_authorization_code()
            token_resp = requests.post(
                self.token_url,
                auth=(self.client_id, self.client_secret),
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": self.redirect_uri,
                }, timeout=10
            )
        
        token_resp.raise_for_status()
        token_data = token_resp.json()
        access_token: str = token_data.get("access_token")
        new_refresh_token: str = token_data.get("refresh_token", refresh_token)
        if not access_token:
            raise RuntimeError("Failed to retrieve access token from ESI.")

        logging.info("Access token retrieved successfully.")
        return access_token, new_refresh_token

    def _verify_token(self) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.token}"}
        resp = requests.get(self.verify_url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _get_authorization_code(self, state: str = "eve_auth") -> str:
        """
        Start a local server and open browser for OAuth authorization code.
        Waits for a maximum timeout, then raises an error if no code is received.
        """
        import time

        timeout_seconds = 60  # max wachten
        with OAuthServer(("localhost", self.port), OAuthHandler) as httpd:
            httpd.timeout = 1  # interne timeout voor handle_request (loopt per seconde)

            # Bouw de OAuth URL
            params = {
                "response_type": "code",
                "redirect_uri": self.redirect_uri,
                "client_id": self.client_id,
                "scope": self.scope,
                "state": state
            }
            auth_url = self.auth_url + "?" + urlencode(params)
            logging.info("Opening browser for OAuth login...")
            webbrowser.open(auth_url)

            # Wacht totdat code ontvangen wordt of timeout bereikt is
            start_time = time.time()
            while httpd.code is None:
                httpd.handle_request()  # blokkeert max 1 seconde
                if time.time() - start_time > timeout_seconds:
                    raise TimeoutError(
                        f"No OAuth code received within {timeout_seconds} seconds. "
                        f"Make sure you authorize in the browser."
                    )

            return httpd.code
