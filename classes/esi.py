import logging
import requests
import webbrowser
import time
import random
import json
from urllib.parse import urlencode
from typing import Optional, Any, Dict, Tuple, Union

from classes.database_manager import DatabaseManager
from classes.config_manager import ConfigManager
from classes.database_models import EsiCache, OAuthCharacter
from classes.oauth import OAuthHandler, OAuthServer


class ESIClient:
    def __init__(self, cfg: ConfigManager, db_oauth: DatabaseManager, character_name: str, is_main: bool, refresh_token: Optional[str] = None):
        self.cfg = cfg
        self.character_name = character_name
        self.is_main = is_main
        self.db_oauth = db_oauth

        # Verified Character
        self.character_id: Optional[int] = None

        # Tokens
        self.refresh_token = refresh_token
        self.access_token: Optional[str] = None
        self.token_expiry: Optional[int] = None

        # ESI Config
        self.esi_base_uri = self.cfg.get("esi")["base"]
        self.redirect_uri = self.cfg.get("app")["redirect_uri"]
        self.token_url = self.cfg.get("esi")["token_url"]
        self.verify_url = self.cfg.get("esi")["verify_url"]
        self.auth_url = self.cfg.get("esi")["auth_url"]
        self.esi_header_accept = self.cfg.get("esi").get("headers")["Accept"]
        self.esi_header_acceptlanguage = self.cfg.get("esi").get("headers")["Accept-Language"]
        self.esi_header_xcompatibilitydate = self.cfg.get("esi").get("headers")["X-Compatibility-Date"]
        self.esi_header_xtenant = self.cfg.get("esi").get("headers")["X-Tenant"]
        self.client_id = self.cfg.get("oauth")["client_id"]
        self.client_secret = self.cfg.get("client_secret")
        self.user_agent = self.cfg.get("app")["user_agent"]
        self.scopes = " ".join(self.cfg.get("defaults")["scopes"])

        # Load tokens from DB if character exists
        self._load_tokens_from_db()
    
    # ----------------------------
    # Internal Helpers
    # ----------------------------
    def _load_tokens_from_db(self) -> None:
        """Load tokens from DB if available. If missing, run registration flow."""
        record = (self.db_oauth.session.query(OAuthCharacter).filter_by(character_name=self.character_name).first())
        if record and record.refresh_token:
            self.refresh_token = record.refresh_token
            self.access_token = record.access_token
            self.token_expiry = record.token_expiry

            # Make sure character_id is populated
            if not record.character_id:
                self.verify_access_token()
            else:
                self.character_id = record.character_id

            logging.info(f"Loaded existing tokens for {self.character_name} ({self.character_id}).")
        else:
            logging.info(f"No token found for {self.character_name}. Registering new character.")
            self.register_new_character()

    # ------------------------------------------------------------------
    # Redirect User to Get Authorization Code
    # ------------------------------------------------------------------
    def _get_authorization_code(self, state: str = "eve_auth") -> str:
        """Open browser for user login and capture authorization code."""
        timeout_seconds = 60
        auth_params = {
            "response_type": "code",
            "client_id": self.client_id,
            "scope": self.scopes,
            "redirect_uri": self.redirect_uri,
            "state": state,
        }
        auth_url = f"{self.auth_url}?{urlencode(auth_params)}"
        logging.info(f"Opening URL for EVE Online login: {auth_url}")
        webbrowser.open(auth_url)

        with OAuthServer(("localhost", 8080), OAuthHandler) as httpd:
            httpd.timeout = timeout_seconds
            logging.info("Waiting for authorization code...")
            start_time = time.time()
            while httpd.code is None:
                httpd.handle_request()
                if time.time() - start_time > timeout_seconds:
                    raise TimeoutError("Authorization code retrieval timed out.")
            logging.info("Authorization code received.")
            return httpd.code
    
    # ------------------------------------------------------------------
    # Exchange Authorization Code for Tokens
    # ------------------------------------------------------------------
    def exchange_code_for_tokens(self, authorization_code: str) -> Tuple[str, str, int]:
        """Exchange code for access + refresh token."""
        response = requests.post(
            self.token_url,
            auth=(self.client_id, self.client_secret),
            data={
                "grant_type": "authorization_code",
                "code": authorization_code,
                "redirect_uri": self.redirect_uri,
            },
            timeout=10,
        )
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data["access_token"]
        refresh_token = token_data["refresh_token"]
        expires_in = token_data["expires_in"]
        logging.info("Access token and refresh token successfully retrieved.")
        return access_token, refresh_token, expires_in

    # ----------------------------
    # Token Refresh
    # ----------------------------
    def refresh_access_token(self) -> None:
        """Refresh access token using stored refresh token."""
        if not self.refresh_token:
            raise RuntimeError("No refresh token provided for token refresh.")

        response = requests.post(
            self.token_url,
            auth=(self.client_id, self.client_secret),
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        self.token_expiry = int(time.time()) + data["expires_in"] - 30

        # Update DB
        record = (self.db_oauth.session.query(OAuthCharacter).filter_by(character_name=self.character_name).first())
        if record:
            record.access_token = self.access_token
            record.refresh_token = self.refresh_token
            record.token_expiry = self.token_expiry
            self.db_oauth.session.commit()

        logging.info(f"Access token refreshed for {self.character_name}.")
    
    # ----------------------------
    # Verify Access Token
    # ----------------------------
    def verify_access_token(self) -> Optional[int]:
        """Verify access token and capture character_id. Returns CharacterID if successful, None otherwise."""
        if not self.access_token:
            logging.error("No access token provided for token verification.")
            return None

        try:
            response = requests.get(
                self.verify_url,
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            self.character_id = data.get("CharacterID")
            logging.debug(f"Access token verified for {self.character_name} ({self.character_id}).")
            return self.character_id
        
        except requests.RequestException as e:
            logging.error(f"Failed to verify access token for {self.character_name}: {e}")
            return None
    
    # ------------------------------------------------------------------
    # Public Method: Register a New Character and Save Tokens
    # ------------------------------------------------------------------
    def register_new_character(self) -> None:
        """Register a new character and persist tokens to DB."""
        logging.info("Starting character registration flow...")
        authorization_code = self._get_authorization_code()
        access_token, refresh_token, expires_in = self.exchange_code_for_tokens(authorization_code)

        # Update instance attributes
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_expiry = int(time.time()) + expires_in - 30

        # Verify access token an get character_id
        if access_token:
            self.verify_access_token()

        # Update DB record
        record = (self.db_oauth.session.query(OAuthCharacter).filter_by(character_name=self.character_name).first())
        if not record:
            record = OAuthCharacter(character_name=self.character_name)
            self.db_oauth.session.add(record)

        record.character_id = self.character_id
        record.access_token = access_token
        record.refresh_token = refresh_token
        record.token_expiry = int(time.time()) + expires_in - 30  # refresh slightly early
        record.scopes = self.scopes

        self.db_oauth.session.commit()

        logging.info(f"Character {self.character_name} ({self.character_id}) registered and tokens saved.")
    
    # ----------------------------
    # ESI API + Cache Helpers
    # ----------------------------
    def get_cached_data(self, endpoint: str) -> Optional[Dict[str, Any]]:
        cache_entry = self.db_oauth.session.query(EsiCache).filter(EsiCache.endpoint == endpoint).first()
        if cache_entry:
            try:
                data = cache_entry.data
                return json.loads(data) if isinstance(data, str) else data
            except Exception as e:
                logging.error(f"Failed to decode cached data for {endpoint}: {e}")
        return None

    def save_to_cache(self, endpoint: str, etag: Optional[str], data: Dict[str, Any]) -> None:
        cache_entry = self.db_oauth.session.query(EsiCache).filter(EsiCache.endpoint == endpoint).first()
        serialized_data = json.dumps(data)
        if cache_entry:
            cache_entry.etag = etag
            cache_entry.data = serialized_data
            cache_entry.last_updated = int(time.time())
        else:
            cache_entry = EsiCache(endpoint=endpoint, etag=etag, data=serialized_data, last_updated=int(time.time()))
            self.db_oauth.session.add(cache_entry)
        self.db_oauth.session.commit()
        logging.info(f"Cache updated for {endpoint}.")
    
    # ------------------------------
    # ESI API Calls (with Caching)
    # -----------------------------
    def esi_get(self, endpoint: str, use_cache: bool = True) -> Any:
        """
        Issue a GET request to the ESI API with caching support.

        Args:
            endpoint (str): The ESI endpoint to access.
            use_cache (bool): Whether to use cached data if available.

        Returns:
            JSON response data or cached data.
        """
        if not self.access_token or (self.token_expiry and time.time() > self.token_expiry):
            self.refresh_access_token()  # Refresh token if no valid access token
        
        headers = {
            "Accept": self.esi_header_accept,
            "Accept-Language": self.esi_header_acceptlanguage,
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": self.user_agent,
            "X-Compatibility-Date": self.esi_header_xcompatibilitydate,
            "X-Tenant": self.esi_header_xtenant
        }

        # Check cache before making API calls
        etag: Optional[str] = None
        cached_data: Optional[Dict[str, Any]] = None
        if use_cache:
            cached_data = self.get_cached_data(endpoint)
            cache_entry = self.db_oauth.session.query(EsiCache).filter(EsiCache.endpoint == endpoint).first()
            if cache_entry and cache_entry.etag:  # Use ETag for conditional requests
                headers["If-None-Match"] = cache_entry.etag

        retries = 0

        url = f"{self.esi_base_uri}{endpoint}"

        while retries < 3:
            try:
                url = f"{self.esi_base_uri}{endpoint}"
                response = requests.get(url, headers=headers, timeout=15)

                if response.status_code == 200:
                    # Response updated; save new data to the cache
                    etag = response.headers.get("ETag", None)
                    self.save_to_cache(endpoint, etag, response.json())
                    if hasattr(response, "json"):
                        return response.json()
                    else:
                        logging.error(f"Unexpected response type: {type(response)}")
                        raise RuntimeError("Expected HTTP response with .json() method.")

                elif response.status_code == 304:
                    # Not modified, return cached data
                    logging.info(f"Using cached data for endpoint {endpoint}.")
                    return json.loads(cached_data) if isinstance(cached_data, str) else cached_data

                elif response.status_code in (420, 429, 500, 502, 503, 504):
                    wait = (2 ** retries) + random.uniform(0, 1)
                    logging.warning(f"ESI error {response.status_code} on {endpoint}, retrying in {wait:.1f}s...")
                    time.sleep(wait)
                    retries += 1
                    continue

                else:
                    response.raise_for_status()

            except requests.RequestException as e:
                logging.error(f"Failed ESI request to {endpoint}: {e}")
                retries += 1
                time.sleep(2 ** retries)

        raise RuntimeError(f"ESI GET request failed for endpoint {endpoint} after retries.")