import logging
import requests
import webbrowser
import time
import random
from urllib.parse import urlencode
from typing import Optional, Any, Dict, Tuple

from classes.database_manager import DatabaseManager
from classes.config_manager import ConfigManager
from classes.database_models import EsiCache
from classes.oauth import OAuthHandler, OAuthServer


class ESIClient:
    def __init__(self, cfg: ConfigManager, db_oauth: DatabaseManager, character_name: str, is_main: bool, refresh_token: Optional[str]):
        self.cfg = cfg
        self.character_name = character_name
        self.is_main = is_main
        self.db_oauth = db_oauth
        self.character_name = character_name
        self.is_main = is_main
        self.refresh_token = refresh_token
        self.access_token: Optional[str] = None
        self.token_expiry: Optional[int] = None

        self.esi_base_uri = self.cfg.get("esi")["base"]
        self.redirect_uri = self.cfg.get("app")["redirect_uri"]
        self.token_url = self.cfg.get("esi")["token_url"]
        self.verify_url = self.cfg.get("esi")["verify_url"]
        self.auth_url = self.cfg.get("esi")["auth_url"]
        self.client_id = self.cfg.get("oauth")["client_id"]
        self.client_secret = self.cfg.get("client_secret")
        self.user_agent = self.cfg.get("app")["user_agent"]
        self.scope = " ".join(self.cfg.get("defaults")["scopes"])
    
    # ------------------------------------------------------------------
    # Redirect User to Get Authorization Code
    # ------------------------------------------------------------------
    def _get_authorization_code(self, state: str = "eve_auth") -> str:
        """
        Open a browser to let the user log in and authorize the application.
        Listen on a local callback server to retrieve the authorization code.
        """
        timeout_seconds = 60  # Maximum time to wait for the authorization code
        auth_params = {
            "response_type": "code",
            "client_id": self.client_id,
            "scope": self.scope,                # Scopes defined in your config
            "redirect_uri": self.redirect_uri,  # Must match ESI settings
            "state": state,                     # Optional unique identifier for this session
        }

        # Build the authorization URL
        auth_url = f"{self.auth_url}?{urlencode(auth_params)}"
        logging.info(f"Opening URL for EVE Online login: {auth_url}")
        webbrowser.open(auth_url)               # Open the authorization URL in the browser

        # Start a local HTTP server to capture the authorization code
        with OAuthServer(("localhost", 8080), OAuthHandler) as httpd:
            httpd.timeout = timeout_seconds
            logging.info("Waiting for authorization code...")
            start_time = time.time()

            while httpd.code is None:  # Wait until the user logs in and the code is received
                httpd.handle_request()
                if time.time() - start_time > timeout_seconds:
                    raise TimeoutError("Authorization code retrieval timed out.")

            logging.info("Authorization code received.")
            return httpd.code
    
    # ------------------------------------------------------------------
    # Exchange Authorization Code for Tokens
    # ------------------------------------------------------------------
    def exchange_code_for_tokens(self, authorization_code: str) -> Tuple[str, str, int]:
        """
        Exchange the authorization code for an access token and refresh token.
        
        Args:
            authorization_code: The authorization code received from EVE login.

        Returns:
            A tuple containing the access_token, refresh_token, and expires_in.
        """
        try:
            # Make a POST request to the ESI `/token` endpoint
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
            response.raise_for_status()  # Raise error for non-200 responses

            token_data = response.json()
            access_token = token_data["access_token"]
            refresh_token = token_data.get("refresh_token")
            expires_in = token_data["expires_in"]  # e.g., 1200 seconds (20 mins)

            logging.info("Access token and refresh token successfully retrieved.")
            return access_token, refresh_token, expires_in

        except requests.RequestException as e:
            logging.error(f"Failed to exchange authorization code for token: {e}")
            raise RuntimeError("Token exchange failed.")

    # ------------------------------
    # Token Refresh
    # ------------------------------
    def refresh_access_token(self) -> None:
        """Refresh the access token using the provided refresh token."""
        if not self.refresh_token:
            raise RuntimeError("No refresh token provided for token refresh.")

        try:
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
            if not hasattr(response, "json"):  # Validate the correct type
                logging.error(f"Unexpected response type: {type(response)}")
                raise RuntimeError("Expected response object with .json() method.")
        
            data = response.json()
            self.access_token = data["access_token"]
            self.refresh_token = data["refresh_token"]
            self.token_expiry = time.time() + data["expires_in"] - 30  # Refresh slightly before expiry
            logging.info(f"Access token refreshed for {self.character_name}.")
        except requests.RequestException as e:
            logging.error(f"Failed to refresh token for {self.character_name}: {e}")
            raise RuntimeError("Token refresh failed.")
    
    # ------------------------------------------------------------------
    # Public Method: Register a New Character and Save Tokens
    # ------------------------------------------------------------------
    def register_new_character(self) -> Tuple[str, str, int]:
        """
        Walk through the full authorization flow to register a new character.
        
        Returns:
            A tuple containing the access_token, refresh_token, and expires_in.
        """
        logging.info("Starting character registration flow...")
        authorization_code = self._get_authorization_code()  # Redirect user and get code
        access_token, refresh_token, expires_in = self.exchange_code_for_tokens(authorization_code)

        logging.info(f"Registration complete. Tokens retrieved for character: {self.character_name}")
        # Save tokens to a database or configuration file as needed
        return access_token, refresh_token, expires_in
    
    # ------------------------------
    # Caching Helpers
    # ------------------------------
    def get_cached_data(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached data for an endpoint from the database."""
        cache_entry = self.db_oauth.session.query(EsiCache).filter(EsiCache.endpoint == endpoint).first()
        if cache_entry:
            try:
                return requests.utils.json.loads(cache_entry.data)  # Deserialize JSON data
            except Exception as e:
                logging.error(f"Failed to decode cached data for {endpoint}: {e}")
        return None

    def save_to_cache(self, endpoint: str, etag: str, data: Dict[str, Any]) -> None:
        """Save data and ETag from ESI API to the cache."""
        cache_entry = self.db_oauth.session.query(EsiCache).filter(EsiCache.endpoint == endpoint).first()
        if cache_entry:
            # Update existing cache entry
            cache_entry.etag = etag
            cache_entry.data = requests.utils.json.dumps(data)  # Serialize JSON data
            cache_entry.last_updated = time.time()
        else:
            # Create a new cache entry
            cache_entry = EsiCache(
                endpoint=endpoint,
                etag=etag,
                data=requests.utils.json.dumps(data),
                last_updated=time.time(),
            )
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
        if not self.access_token:
            self.refresh_access_token()  # Refresh token if no valid access token
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": self.user_agent,
        }

        # Check cache before making API calls
        etag = None
        if use_cache:
            cached_data = self.get_cached_data(endpoint)
            cache_entry = self.db_oauth.query(EsiCache).filter(EsiCache.endpoint == endpoint).first()
            if cache_entry and cache_entry.etag:  # Use ETag for conditional requests
                headers["If-None-Match"] = cache_entry.etag

        retries = 0
        while retries < 5:
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
                    return cached_data

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