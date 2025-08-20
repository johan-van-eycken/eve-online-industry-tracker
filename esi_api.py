import logging
import requests
import webbrowser
from urllib.parse import urlencode
from typing import Any, Dict
from oauth import OAuthHandler, OAuthServer
from config import ConfigManager

class ESIClient:
    def __init__(self):
        self.cfg = ConfigManager()
        self.port = self.cfg.get("port")
        self.redirect_uri = self.cfg.get("redirect_uri")
        self.esi_base = self.cfg.get("esi_base")
        self.auth_url = self.cfg.get("auth_url")
        self.token_url = self.cfg.get("token_url")
        self.verify_url = self.cfg.get("verify_url")
        self.user_agent = self.cfg.get("user_agent")
        self.scope = self.cfg.get("scope")
        self.client_id = self.cfg.get("client_id")
        self.client_secret = self.cfg.get("client_secret")
        self.token = None
        self.character_info = None
        self.character_id = None
        self.character_name = None

    def login(self) -> Dict[str, Any]:
        """
        Authenticate and verify, returns character info dict.
        """
        self.token = self._get_access_token()
        self.character_info = self._verify_token()
        self.character_id = self.character_info["CharacterID"]
        self.character_name = self.character_info["CharacterName"]
        logging.info(f"Logged in as {self.character_name}")
        return self.character_info

    def esi_get(self, endpoint: str) -> Any:
        """Perform an authenticated GET request to the ESI API."""
        if not self.token:
            logging.error("No access token available. Please login first.")
            raise RuntimeError("No access token available. Please login first.")
        url = self.esi_base + endpoint
        headers = {
            "Authorization": f"Bearer {self.token}",
            "User-Agent": self.user_agent
        }
        resp = None
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            resp_text = resp.text if resp is not None else ''
            logging.error(f"Error fetching ESI data from {endpoint}: {e} ({resp_text})")
            raise

    def _get_access_token(self) -> str:
        """Obtain a valid ESI access token, refreshing or authorizing as needed."""
        try:
            refresh_token = self.cfg.get("refresh_token")
            if not refresh_token:
                code = self._get_authorization_code()
                token_resp = requests.post(
                    self.token_url,
                    auth=(self.client_id, self.client_secret),
                    data={
                        "grant_type": "authorization_code",
                        "code": code,
                        "redirect_uri": self.redirect_uri,
                    },
                    timeout=10
                )
                token_resp.raise_for_status()
                token_data = token_resp.json()
                self.cfg.set("refresh_token", token_data["refresh_token"])
                refresh_token = token_data["refresh_token"]
            token_resp = requests.post(
                self.token_url,
                auth=(self.client_id, self.client_secret),
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                timeout=10
            )
            token_resp.raise_for_status()
            token_data = token_resp.json()
            if token_data.get("refresh_token") and token_data["refresh_token"] != refresh_token:
                self.cfg.set("refresh_token", token_data["refresh_token"])
            return token_data["access_token"]
        except requests.RequestException as e:
            logging.error(f"Error obtaining access token: {e}")
            raise

    def _verify_token(self) -> Dict[str, Any]:
        """Verify an ESI access token and return its payload."""
        headers = {"Authorization": f"Bearer {self.token}"}
        resp = None
        try:
            resp = requests.get(self.verify_url, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logging.error(f"Error verifying token: {e} ({resp.text if resp is not None else ''})")
            raise

    def _get_authorization_code(self, state: str = "eve_auth") -> str:
        """Start a local server and open browser for OAuth authorization code."""
        with OAuthServer(("localhost", self.port), OAuthHandler) as httpd:
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
            httpd.handle_request()  # blocks here until code received
            if httpd.code is None:
                raise RuntimeError("Authorization code was not received.")
            return httpd.code