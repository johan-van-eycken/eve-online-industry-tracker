import logging
import requests # pyright: ignore[reportMissingModuleSource]
import webbrowser
import time
import random
import json as jsonlib
import threading
import re
from urllib.parse import urlencode
from typing import Callable, Optional, Any, Dict, Tuple, Union

try:
    from utils.esi_monitor import get_esi_monitor
except ImportError:  # pragma: no cover
    get_esi_monitor = None  # type: ignore

try:
    import jwt  # type: ignore
    try:
        from jwt.algorithms import RSAAlgorithm  # type: ignore
    except ImportError:  # pragma: no cover
        RSAAlgorithm = None
except ImportError:  # pragma: no cover
    jwt = None
    RSAAlgorithm = None

from sqlalchemy.dialects.sqlite import insert as sqlite_insert  # pyright: ignore[reportMissingModuleSource]
from sqlalchemy.exc import SQLAlchemyError  # pyright: ignore[reportMissingModuleSource]

from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.esi_versions import ESI_COMPATIBILITY_DATE
from eve_online_industry_tracker.config.config_manager import ConfigManager
from eve_online_industry_tracker.infrastructure.models import EsiCache, OAuthCharacter
from eve_online_industry_tracker.infrastructure.oauth import OAuthHandler, OAuthServer
from utils.requests_ssl import get_requests_ssl_kwargs

class EsiRequestError(RuntimeError):
    """An ESI request failed for a reason that is not a bug in our code: "failed
    after retries", a token problem (no refresh token, an unexpected SSO token
    response, storing a refreshed token), the ESI response cache that cannot be
    read, or an ESI response the caller cannot use (ESIService). A RuntimeError,
    so a caller that catches RuntimeError still sees it; the catch sites use
    ESI_REQUEST_ERRORS, which no longer includes plain RuntimeError/ValueError."""


# The contract of ESIClient.esi_get/esi_post (and of the ESIService calls built on them):
# every failure that is not a bug in our code is one of these.
#   - requests.RequestException: HTTP and connection errors, a body that is not JSON
#     (requests' JSONDecodeError; ESIClient retries it, see below);
#   - EsiRequestError: "failed after retries", token problems (no refresh token, an
#     unexpected SSO token response), and the app DB problems of ESIClient itself
#     (storing a refreshed token, reading the ESI response cache): ESIClient wraps their
#     SQLAlchemyError in EsiRequestError, so callers need not add SQLAlchemyError for them.
# Callers that skip one item on failure catch exactly these, so a NameError/
# AttributeError/TypeError/KeyError/ValueError/RuntimeError of our own code still
# surfaces. A caller that also touches the app DB itself adds SQLAlchemyError for its
# own queries.
ESI_REQUEST_ERRORS: Tuple[type, ...] = (EsiRequestError, requests.RequestException)


def esi_total_pages(headers: Any, url: str) -> int:
    """The X-Pages header of a paginated ESI response (1 without it);
    EsiRequestError when ESI sent one that is not a positive whole number."""
    raw = headers.get("X-Pages", "1") if headers is not None else "1"
    try:
        pages = int(raw)
    except (TypeError, ValueError):
        raise EsiRequestError(f"ESI sent an unreadable X-Pages header {raw!r}: {url}") from None
    if pages < 1:
        raise EsiRequestError(f"ESI sent an unreadable X-Pages header {raw!r}: {url}")
    return pages


class _EsiErrorRateLimiter:
    """Cooperative rate limiter for ESI's error budget.

    ESI provides a floating-window error budget via response headers:
      - X-Esi-Error-Limit-Remain
      - X-Esi-Error-Limit-Reset

    This limiter uses those headers as the source of truth and applies
    cooperative sleeping when the budget is depleted (or low) to avoid
    hitting hard rate limits.
    """

    def __init__(self, *, low_watermark: int = 5, max_sleep_seconds: int = 60):
        self._lock = threading.Lock()
        self._remain: Optional[int] = None
        self._reset_seconds: Optional[int] = None
        self._updated_at: float = 0.0
        self._low_watermark = int(low_watermark)
        self._max_sleep_seconds = int(max_sleep_seconds)
        self._pacing_sleep_seconds = 0.2
        self._admin_settings = None

    def update_from_headers(self, headers: Any) -> None:
        if not headers:
            return
        try:
            remain_raw = headers.get("X-Esi-Error-Limit-Remain")
            reset_raw = headers.get("X-Esi-Error-Limit-Reset")
        except AttributeError:
            return

        remain: Optional[int]
        reset_seconds: Optional[int]
        try:
            remain = int(remain_raw) if remain_raw is not None else None
        except (TypeError, ValueError):
            remain = None
        try:
            reset_seconds = int(reset_raw) if reset_raw is not None else None
        except (TypeError, ValueError):
            reset_seconds = None

        with self._lock:
            if remain is not None:
                self._remain = max(0, remain)
            if reset_seconds is not None:
                self._reset_seconds = max(0, reset_seconds)
            self._updated_at = time.time()

    def suggested_sleep_seconds(self) -> float:
        """Return how long we should sleep before issuing the next request."""
        # Read tuning knobs from admin settings if wired.
        admin = self._admin_settings
        low_watermark = int(admin.get("esi_resilience", "esi_error_budget_low_watermark")) if admin else self._low_watermark
        max_sleep = int(admin.get("esi_resilience", "esi_error_budget_max_sleep_seconds")) if admin else self._max_sleep_seconds
        pacing = float(admin.get("esi_resilience", "esi_error_budget_pacing_sleep_seconds")) if admin else self._pacing_sleep_seconds

        with self._lock:
            remain = self._remain
            reset_seconds = self._reset_seconds

        # If we haven't seen headers yet, don't gate.
        if remain is None or reset_seconds is None:
            return 0.0

        # If budget is exhausted, sleep until reset (plus jitter).
        if remain <= 0:
            base = float(reset_seconds)
            jitter = random.uniform(0.05, 0.35)
            return min(base + jitter, float(max_sleep))

        # When low, apply a gentle pacing to avoid draining to 0.
        if remain < low_watermark:
            return pacing

        return 0.0

    def snapshot(self) -> tuple[Optional[int], Optional[int]]:
        with self._lock:
            return self._remain, self._reset_seconds


_ESI_ERROR_LIMITER = _EsiErrorRateLimiter()

# The configured X-Compatibility-Date values whose "ignored" WARNING was logged (once per process).
_COMPATIBILITY_DATE_WARNED: set[str] = set()
_COMPATIBILITY_DATE_WARNED_LOCK = threading.Lock()


def esi_cache_key(endpoint_and_query: str) -> str:
    """The esi_cache row key of an ESI request: the request prefixed with the
    compatibility date ("[2025-08-26]/characters/1/skills"), so a later date
    never reuses an entry, or its ETag (an old-shape 304), of another date."""
    return f"[{ESI_COMPATIBILITY_DATE}]{endpoint_and_query}"


def prune_stale_esi_cache(session: Any) -> int:
    """Delete the esi_cache rows that are not keyed for the current
    compatibility date: the /latest/ and /vN/ routes of before the
    unversioned endpoints, the unprefixed keys of before esi_cache_key, and
    the keys of an earlier date. No request reads them any more. Idempotent
    (a second run deletes nothing); the count is logged. Returns it (0
    without an esi_cache table yet)."""
    from sqlalchemy import inspect as sqlalchemy_inspect  # pyright: ignore[reportMissingImports]

    if not sqlalchemy_inspect(session.get_bind()).has_table(EsiCache.__tablename__):
        return 0
    prefix = esi_cache_key("")
    stale = session.query(EsiCache).filter(~EsiCache.endpoint.startswith(prefix, autoescape=True))
    count = stale.count()
    if count:
        stale.delete(synchronize_session=False)
        session.commit()
        logging.info("ESI cache: pruned %d esi_cache row(s) not keyed for compatibility date %s "
                     "(/latest/, /vN/ and older keys no request reads)", count, ESI_COMPATIBILITY_DATE)
    return count


def _esi_setting(category: str, key: str) -> Any:
    """An ESI admin setting, read at each request: through the admin settings
    the bootstrap wires into _ESI_ERROR_LIMITER, or the schema default before
    that (the startup character refresh) and without a settings store."""
    admin = _ESI_ERROR_LIMITER._admin_settings
    if admin is None:
        from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA
        return ADMIN_SETTINGS_SCHEMA[category]["settings"][key]["default"]
    return admin.get(category, key)


def esi_request_timeout_seconds() -> float:
    """esi_resilience/esi_request_timeout_seconds."""
    return float(_esi_setting("esi_resilience", "esi_request_timeout_seconds"))


def esi_max_retries() -> int:
    """esi_resilience/esi_max_retries: retries after the first attempt."""
    return int(_esi_setting("esi_resilience", "esi_max_retries"))


def _auth_pagination_sleep_seconds() -> float:
    return float(_esi_setting("performance", "esi_auth_pagination_sleep_seconds"))


class _EsiRequestErrorLogLimiter:
    """Rate-limit noisy per-request exception logs.

    The public structures startup scan can fan out thousands of requests; when
    ESI is slow or unreachable, logging every timeout at ERROR floods the console.
    """

    def __init__(self, *, window_seconds: float = 60.0, max_logs_per_key: int = 5):
        self._lock = threading.Lock()
        self._window_seconds = float(window_seconds)
        self._max_logs_per_key = int(max_logs_per_key)
        self._state: dict[str, tuple[float, int]] = {}

    def allow(self, key: str) -> bool:
        if not key:
            return True
        now = time.time()
        with self._lock:
            window_start, count = self._state.get(key, (now, 0))
            if (now - window_start) >= self._window_seconds:
                window_start, count = now, 0
            if count >= self._max_logs_per_key:
                self._state[key] = (window_start, count)
                return False
            self._state[key] = (window_start, count + 1)
            return True


_ESI_REQUEST_ERROR_LOG_LIMITER = _EsiRequestErrorLogLimiter()

# One WARNING per monitor call kind per 5 minutes: a broken monitor is hit on every request.
_ESI_MONITOR_WARNING_LIMITER = _EsiRequestErrorLogLimiter(window_seconds=300.0, max_logs_per_key=1)


def call_esi_monitor(get_monitor: Any, fn: str, build_kwargs: Callable[[], Dict[str, Any]]) -> None:
    """Call get_monitor().<fn>(**build_kwargs()), the ESI telemetry. The
    arguments are built inside the guard (build_kwargs is a callable, e.g. a
    lambda over the response), so a response field that cannot be read fails
    the telemetry, not the request. Telemetry never fails a fetch: any
    exception of the monitor or of build_kwargs (a bug included) is a WARNING,
    rate-limited per `fn`, and the request goes on. get_monitor None
    (utils.esi_monitor not importable): nothing is recorded."""
    if get_monitor is None:
        return
    try:
        getattr(get_monitor(), fn)(**build_kwargs())
    except Exception as exc:  # noqa: BLE001 - telemetry only, see the docstring
        if _ESI_MONITOR_WARNING_LIMITER.allow(fn):
            logging.warning(
                "ESI monitor %s failed (telemetry skipped, the request goes on; "
                "repeats are not logged for 5 minutes): %s: %s",
                fn,
                type(exc).__name__,
                exc,
            )


def _monitor(fn: str, build_kwargs: Callable[[], Dict[str, Any]]) -> None:
    """This module's ESI monitor call (see call_esi_monitor)."""
    call_esi_monitor(get_esi_monitor, fn, build_kwargs)


def _esi_error_log_key(*, method: str, endpoint: Optional[str], url: str) -> str:
    ep = (endpoint or "").strip()
    # Collapse the noisiest endpoint into a single key.
    if ep.startswith("/universe/structures/"):
        return f"{method} /universe/structures/*"
    if ep:
        return f"{method} {ep}"
    return f"{method} {url}"


_SSO_ISSUER = "https://login.eveonline.com"
_SSO_JWKS_URL = "https://login.eveonline.com/oauth/jwks"
_SSO_JWKS_TTL_SECONDS = 24 * 3600
_SSO_JWKS_LOCK = threading.Lock()
_SSO_JWKS_CACHE: dict[str, Any] = {
    "fetched_at": 0.0,
    "keys_by_kid": {},
}


def _get_sso_jwk_for_kid(kid: str, *, timeout_seconds: int = 10) -> Optional[dict]:
    if not kid:
        return None

    now = time.time()
    with _SSO_JWKS_LOCK:
        fetched_at = float(_SSO_JWKS_CACHE.get("fetched_at") or 0.0)
        keys_by_kid = _SSO_JWKS_CACHE.get("keys_by_kid") or {}
        cached = keys_by_kid.get(kid)
        cache_fresh = (now - fetched_at) < _SSO_JWKS_TTL_SECONDS
        if cached is not None and cache_fresh:
            return cached

    # (Re)fetch JWKS outside the lock.
    try:
        resp = requests.get(_SSO_JWKS_URL, timeout=timeout_seconds, **get_requests_ssl_kwargs())
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logging.warning("Failed to fetch SSO JWKS: %s", str(e))
        return None

    keys = payload.get("keys") if isinstance(payload, dict) else None
    if not isinstance(keys, list):
        return None

    new_map: dict[str, dict] = {}
    for k in keys:
        if not isinstance(k, dict):
            continue
        k_kid = k.get("kid")
        if not k_kid:
            continue
        new_map[str(k_kid)] = k

    with _SSO_JWKS_LOCK:
        _SSO_JWKS_CACHE["fetched_at"] = now
        _SSO_JWKS_CACHE["keys_by_kid"] = new_map

    return new_map.get(kid)


def _parse_retry_after_seconds(headers: Any) -> float:
    if not headers:
        return 0.0
    try:
        ra = headers.get("Retry-After")
    except AttributeError:
        return 0.0
    if ra is None:
        return 0.0
    try:
        # Retry-After is seconds for ESI throttles.
        return max(0.0, float(ra))
    except (TypeError, ValueError):
        return 0.0


def _sleep_seconds(seconds: float) -> None:
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return
    if seconds <= 0:
        return
    time.sleep(seconds)


def _esi_gate(context: str) -> None:
    """Sleep if ESI error-budget suggests waiting.

    Logs at debug level only when we actually sleep.
    """
    wait = _ESI_ERROR_LIMITER.suggested_sleep_seconds()
    if wait <= 0:
        return
    remain, reset_seconds = _ESI_ERROR_LIMITER.snapshot()
    logging.debug(
        "ESI limiter sleeping %.2fs before %s (remain=%s reset=%s)",
        float(wait),
        str(context),
        str(remain) if remain is not None else "?",
        str(reset_seconds) if reset_seconds is not None else "?",
    )
    _monitor("record_sleep", lambda: dict(kind="gate", seconds=float(wait), context=str(context)))
    _sleep_seconds(wait)


class ESIClient:
    def __init__(self, cfg: ConfigManager, db_oauth: DatabaseManager, character_name: str, is_main: bool, is_corp_director: bool, refresh_token: Optional[str] = None):
        self.cfg = cfg
        self.character_name = character_name
        self.is_main = is_main
        self.is_corp_director = is_corp_director
        self.db_oauth = db_oauth

        # Verified Character
        self.character_id: Optional[int] = None

        # Tokens
        self.refresh_token = refresh_token
        self.access_token: Optional[str] = None
        self.token_expiry: Optional[int] = None

        # ESI Config
        # Strip any trailing version segment left over from pre-refactor configs
        # (old default was "https://esi.evetech.net/latest"; new is "https://esi.evetech.net")
        _raw_base = self.cfg.get("esi")["base"].rstrip("/")
        if _raw_base.endswith("/latest"):
            _raw_base = _raw_base[: -len("/latest")]
        self.esi_base_uri = _raw_base
        self.redirect_uri = self.cfg.get("app")["redirect_uri"]
        self.token_url = self.cfg.get("esi")["token_url"]
        self.verify_url = self.cfg.get("esi")["verify_url"]
        self.auth_url = self.cfg.get("esi")["auth_url"]
        self.esi_header_accept = self.cfg.get("esi").get("headers")["Accept"]
        self.esi_header_acceptlanguage = self.cfg.get("esi").get("headers")["Accept-Language"]
        # The compatibility date pins every ESI response shape the code parses, so it is
        # the code's (esi_versions.ESI_COMPATIBILITY_DATE), not a config choice.
        _configured_date = (self.cfg.get("esi").get("headers") or {}).get("X-Compatibility-Date")
        if _configured_date and _configured_date != ESI_COMPATIBILITY_DATE:
            with _COMPATIBILITY_DATE_WARNED_LOCK:
                first = _configured_date not in _COMPATIBILITY_DATE_WARNED
                _COMPATIBILITY_DATE_WARNED.add(_configured_date)
            if first:   # once per process, not once per character's client
                logging.warning(
                    "config esi.headers.X-Compatibility-Date=%s ignored; ESI requests use the pinned %s",
                    _configured_date,
                    ESI_COMPATIBILITY_DATE,
                )
        self.esi_header_xcompatibilitydate = ESI_COMPATIBILITY_DATE
        self.esi_header_xtenant = self.cfg.get("esi").get("headers")["X-Tenant"]
        self.client_id = self.cfg.get("oauth")["client_id"]
        self.client_secret = self.cfg.get("client_secret")
        self.user_agent = self.cfg.get("app")["user_agent"]

        # Persistent HTTP session for connection pooling (TCP + TLS reuse).
        self._http_session = requests.Session()
        self._http_session.headers.update({
            "Accept": self.esi_header_accept,
            "Accept-Language": self.esi_header_acceptlanguage,
            "X-Compatibility-Date": self.esi_header_xcompatibilitydate,
            "X-Tenant": self.esi_header_xtenant,
            "User-Agent": self.user_agent,
        })
        ssl_kwargs = get_requests_ssl_kwargs()
        if "verify" in ssl_kwargs:
            self._http_session.verify = ssl_kwargs["verify"]

        # Assign correct scopes
        all_scopes = set(self.cfg.get("defaults")["scopes"])
        if is_corp_director:
            all_scopes.update(self.cfg.get("defaults")["scopes_corp_director"])
        self.scopes = " ".join(sorted(all_scopes))

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

            logging.debug(f"Loaded existing tokens for {self.character_name} ({self.character_id}).")
        else:
            logging.debug(f"No token found for {self.character_name}. Registering new character.")
            self.register_new_character()

    # ------------------------------------------------------------------
    # Redirect User to Get Authorization Code
    # ------------------------------------------------------------------
    def _get_authorization_code(self, state: str = "eve_auth") -> str:
        """Open browser for user login and capture authorization code."""
        timeout_seconds = 60
        # Where to send the user after SSO completes (best-effort).
        streamlit_url = None
        try:
            streamlit_url = (self.cfg.get("app") or {}).get("streamlit_url")
        except AttributeError:
            streamlit_url = None
        if not streamlit_url:
            streamlit_url = "http://localhost:8501"
        auth_params = {
            "response_type": "code",
            "client_id": self.client_id,
            "scope": self.scopes,
            "redirect_uri": self.redirect_uri,
            "state": state,
        }
        auth_url = f"{self.auth_url}?{urlencode(auth_params)}"
        logging.debug(f"Opening URL for EVE Online login: {auth_url}")
        webbrowser.open(auth_url)

        with OAuthServer(("localhost", 8080), OAuthHandler, return_to_url=streamlit_url) as httpd:
            httpd.timeout = timeout_seconds
            logging.debug("Waiting for authorization code...")
            start_time = time.time()
            while httpd.code is None:
                httpd.handle_request()
                if time.time() - start_time > timeout_seconds:
                    raise TimeoutError("Authorization code retrieval timed out.")
            logging.debug("Authorization code received.")
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
            **get_requests_ssl_kwargs(),
        )
        response.raise_for_status()
        access_token, refresh_token, expires_in = self._parse_token_payload(response.json())
        logging.debug("Access token and refresh token successfully retrieved.")
        return access_token, refresh_token, expires_in

    # ----------------------------
    # Token Refresh
    # ----------------------------
    def refresh_access_token(self) -> None:
        """Refresh access token using stored refresh token."""
        if not self.refresh_token:
            raise EsiRequestError("No refresh token provided for token refresh.")

        response = requests.post(
            self.token_url,
            auth=(self.client_id, self.client_secret),
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            },
            timeout=10,
            **get_requests_ssl_kwargs(),
        )
        response.raise_for_status()
        access_token, refresh_token, expires_in = self._parse_token_payload(response.json())
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_expiry = int(time.time()) + expires_in - 30

        # Update DB
        try:
            record = (self.db_oauth.session.query(OAuthCharacter).filter_by(character_name=self.character_name).first())
            if record:
                record.access_token = self.access_token
                record.refresh_token = self.refresh_token
                record.token_expiry = self.token_expiry
                self.db_oauth.session.commit()
        except SQLAlchemyError as exc:
            self._rollback_quietly()
            # The new token is in memory for this process; the stored one is the old one.
            raise EsiRequestError(
                f"could not store the refreshed ESI token of {self.character_name}: {exc}"
            ) from exc

        logging.debug(f"Access token refreshed for {self.character_name}.")

    @staticmethod
    def _parse_token_payload(data: Any) -> Tuple[str, str, int]:
        """(access_token, refresh_token, expires_in) of an SSO token response;
        EsiRequestError("unexpected SSO token response: ...") for any other shape."""
        if not isinstance(data, dict):
            raise EsiRequestError(f"unexpected SSO token response: not an object ({type(data).__name__})")
        try:
            access_token = data["access_token"]
            refresh_token = data["refresh_token"]
            expires_in = data["expires_in"]
        except KeyError as exc:
            raise EsiRequestError(f"unexpected SSO token response: no {exc.args[0]!r}") from exc
        if not isinstance(access_token, str) or not access_token:
            raise EsiRequestError("unexpected SSO token response: access_token is not a string")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise EsiRequestError("unexpected SSO token response: refresh_token is not a string")
        if isinstance(expires_in, bool) or not isinstance(expires_in, (int, float)):
            raise EsiRequestError(f"unexpected SSO token response: expires_in is {expires_in!r}, not a number")
        return access_token, refresh_token, int(expires_in)

    def _rollback_quietly(self) -> None:
        """Roll the OAuth session back after a failed statement, so the next one can run;
        a failing rollback is logged (the original error is what the caller raises)."""
        try:
            self.db_oauth.session.rollback()
        except SQLAlchemyError as exc:
            logging.warning("ESI client: rollback of the OAuth session failed: %s", exc)

    def _cache_lookup(self, cache_key: str) -> Tuple[Optional[Any], Optional[EsiCache]]:
        """(cached data, cache row) of an ESI request; EsiRequestError when the
        esi_cache table cannot be read (see ESI_REQUEST_ERRORS)."""
        try:
            cached_data = self.get_cached_data(cache_key)
            cache_entry = self.db_oauth.session.query(EsiCache).filter(EsiCache.endpoint == cache_key).first()
        except SQLAlchemyError as exc:
            self._rollback_quietly()
            raise EsiRequestError(f"ESI response cache unreadable for {cache_key}: {exc}") from exc
        return cached_data, cache_entry
    
    # ----------------------------
    # Verify Access Token
    # ----------------------------
    def verify_access_token(self) -> Optional[int]:
        """Verify access token and capture character_id. Returns CharacterID if successful, None otherwise."""
        if not self.access_token:
            logging.error("No access token provided for token verification.")
            return None

        # Prefer local JWT verification (SSO v2 issues JWT access tokens).
        try:
            claims = self._validate_access_token_jwt(self.access_token)
            if claims is not None:
                character_id = self._extract_character_id_from_claims(claims)
                if character_id is not None:
                    self.character_id = character_id
                    try:
                        self._sync_jwt_claims_to_db(claims)
                    except Exception as e:
                        logging.debug("Failed to persist JWT claims to DB: %s", str(e))
                    logging.debug(
                        "Access token JWT validated for %s (%s).",
                        self.character_name,
                        str(self.character_id),
                    )
                    return self.character_id
        except Exception as e:
            # Fallback to /oauth/verify for non-JWT tokens or if JWKS fetch fails.
            logging.debug("JWT validation failed; falling back to /oauth/verify: %s", str(e))

        try:
            response = requests.get(
                self.verify_url,
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=10,
                **get_requests_ssl_kwargs(),
            )
            response.raise_for_status()
            data = response.json()
            self.character_id = data.get("CharacterID")
            logging.debug(f"Access token verified for {self.character_name} ({self.character_id}).")
            return self.character_id
        
        except requests.RequestException as e:
            logging.error(f"Failed to verify access token for {self.character_name}: {e}")
            return None

    def _validate_access_token_jwt(self, token: str) -> Optional[Dict[str, Any]]:
        """Validate an EVE SSO JWT access token locally.

        Uses SSO JWKS to verify signature, and checks issuer/audience/expiry.
        Returns decoded claims when valid; returns None if PyJWT isn't available.
        """
        if jwt is None:
            return None
        if not token or not isinstance(token, str):
            raise ValueError("Missing token")

        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        alg = header.get("alg") or "RS256"
        if alg != "RS256":
            raise ValueError(f"Unexpected JWT alg: {alg}")
        if not kid:
            raise ValueError("JWT missing kid")

        jwk = _get_sso_jwk_for_kid(str(kid))
        if jwk is None:
            raise ValueError("Unable to resolve JWKS key for kid")

        if RSAAlgorithm is None:
            raise RuntimeError("PyJWT RSAAlgorithm not available")
        public_key = RSAAlgorithm.from_jwk(jsonlib.dumps(jwk))

        # EVE SSO uses issuer https://login.eveonline.com and audience == client_id.
        claims = jwt.decode(
            token,
            key=public_key,  # type: ignore[arg-type]
            algorithms=["RS256"],
            audience=self.client_id,
            issuer=_SSO_ISSUER,
            options={
                "require": ["exp", "iat", "iss", "aud", "sub"],
            },
        )
        return claims

    def _sync_jwt_claims_to_db(self, claims: Dict[str, Any]) -> None:
        """Persist useful JWT claims (name/scopes/character_id) to OAuth DB."""
        if not isinstance(claims, dict):
            return

        name = claims.get("name")
        scp = claims.get("scp")

        scopes_str: Optional[str]
        if isinstance(scp, list):
            scopes_str = " ".join([str(s).strip() for s in scp if str(s).strip()])
        elif isinstance(scp, str):
            scopes_str = scp.strip()
        else:
            scopes_str = None

        # This record should exist already, but create defensively.
        record = self.db_oauth.session.query(OAuthCharacter).filter_by(character_name=self.character_name).first()
        if not record:
            record = OAuthCharacter(character_name=self.character_name)
            self.db_oauth.session.add(record)

        # Keep the DB authoritative for the name key, but log if the token disagrees.
        if isinstance(name, str) and name and name != self.character_name:
            logging.debug(
                "JWT name claim differs from configured character_name: claim=%s local=%s",
                name,
                self.character_name,
            )

        if self.character_id is not None:
            record.character_id = int(self.character_id)
        if self.access_token is not None:
            record.access_token = self.access_token
        if self.refresh_token is not None:
            record.refresh_token = self.refresh_token
        if self.token_expiry is not None:
            record.token_expiry = int(self.token_expiry)
        if scopes_str:
            record.scopes = scopes_str

        self.db_oauth.session.commit()

    @staticmethod
    def _extract_character_id_from_claims(claims: Dict[str, Any]) -> Optional[int]:
        """Extract CharacterID from JWT claims.

        EVE SSO typically encodes it in `sub` like: CHARACTER:EVE:123456789
        """
        sub = claims.get("sub")
        if not sub:
            return None
        m = re.match(r"^CHARACTER:EVE:(\d+)$", str(sub))
        if not m:
            return None
        try:
            return int(m.group(1))
        except ValueError:
            return None
    
    # ------------------------------------------------------------------
    # Public Method: Register a New Character and Save Tokens
    # ------------------------------------------------------------------
    def register_new_character(self) -> None:
        """Register a new character and persist tokens to DB."""
        logging.debug("Starting character registration flow...")
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

        logging.debug(f"Character {self.character_name} ({self.character_id}) registered and tokens saved.")
    
    # ----------------------------
    # ESI API + Cache Helpers
    # ----------------------------
    def get_cached_data(self, endpoint: str) -> Optional[Dict[str, Any]]:
        cache_entry = self.db_oauth.session.query(EsiCache).filter(EsiCache.endpoint == endpoint).first()
        if cache_entry:
            try:
                data = cache_entry.data
                return jsonlib.loads(data) if isinstance(data, str) else data
            except Exception as e:
                logging.error(f"Failed to decode cached data for {endpoint}: {e}")
        return None

    def save_to_cache(self, endpoint: str, etag: Optional[str], data: Dict[str, Any]) -> None:
        try:
            serialized_data = jsonlib.dumps(data)
            last_updated = int(time.time())
            stmt = (
                sqlite_insert(EsiCache)
                .values(endpoint=endpoint, etag=etag, data=serialized_data, last_updated=last_updated)
                .on_conflict_do_update(
                    index_elements=["endpoint"],
                    set_=dict(etag=etag, data=serialized_data, last_updated=last_updated),
                )
            )
            self.db_oauth.session.execute(stmt)
            self.db_oauth.session.commit()
            logging.debug(f"Cache updated for {endpoint}.")
        except SQLAlchemyError as e:   # the request's data stands; a bug propagates
            logging.error(f"Error saving cache for {endpoint}: {e}")
            self._rollback_quietly()
    
    # ------------------------------
    # ESI API Calls (with Caching)
    # -----------------------------
    def esi_get(
        self,
        endpoint: str,
        params: dict | None = None,
        use_cache: bool = True,
        paginate: bool = False,
        return_headers: bool = False,
        timeout_seconds: float | None = None,
        suppress_forbidden_log: bool = False,
        suppress_not_found_log: bool = False,
    ) -> Any:
        """
        Issue a GET request to the ESI API with optional query params and caching.
        If paginate=True, will fetch all pages and return a combined list.
        If return_headers=True, returns (data, headers) for the last page.
        timeout_seconds None: esi_resilience/esi_request_timeout_seconds; the
        attempts are 1 + esi_resilience/esi_max_retries.
        """
        if not self.access_token or (self.token_expiry and time.time() > self.token_expiry):
            self.refresh_access_token()
        if timeout_seconds is None:
            timeout_seconds = esi_request_timeout_seconds()
        max_attempts = esi_max_retries() + 1

        # Build URL + cache key
        query = ""
        if params:
            query = "?" + urlencode(sorted(params.items()), doseq=True)
        cache_key = esi_cache_key(f"{endpoint}{query}")

        headers = {
            "Accept": self.esi_header_accept,
            "Accept-Language": self.esi_header_acceptlanguage,
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": self.user_agent,
            "X-Compatibility-Date": self.esi_header_xcompatibilitydate,
            "X-Tenant": self.esi_header_xtenant
        }

        etag: Optional[str] = None
        cached_data: Optional[Dict[str, Any]] = None
        cache_mode = "off"
        if use_cache:
            cached_data, cache_entry = self._cache_lookup(cache_key)
            if cache_entry and cache_entry.etag:
                headers["If-None-Match"] = cache_entry.etag
            cache_mode = "enabled_with_entry" if (cached_data is not None or cache_entry is not None) else "enabled_no_entry"

        retries = 0
        url = f"{self.esi_base_uri}{endpoint}{query}"

        # Pagination logic
        if paginate:
            all_data = []
            page = 1
            last_headers = {}
            while True:
                paged_params = dict(params) if params else {}
                paged_params["page"] = page
                paged_query = "?" + urlencode(sorted(paged_params.items()), doseq=True)
                paged_url = f"{self.esi_base_uri}{endpoint}{paged_query}"
                _t0: Optional[float] = None
                try:
                    _esi_gate(f"GET {endpoint} page={page}")
                    _t0 = time.time()
                    response = self._http_session.get(
                        paged_url,
                        headers=headers,
                        timeout=float(timeout_seconds),
                    )
                    _monitor(
                        "record_http_attempt",
                        lambda: dict(
                            method="GET",
                            endpoint=str(endpoint),
                            url=str(paged_url),
                            status_code=int(response.status_code) if response is not None else None,
                            elapsed_ms=(time.time() - _t0) * 1000.0,
                            headers=getattr(response, "headers", None),
                            exception=None,
                            cache_mode=cache_mode,
                            page=int(page),
                        ),
                    )
                    _ESI_ERROR_LIMITER.update_from_headers(response.headers)
                    if response.status_code == 200:
                        etag = response.headers.get("ETag")
                        data_json = response.json()
                        all_data.extend(data_json if isinstance(data_json, list) else [data_json])
                        last_headers = response.headers
                        total_pages = esi_total_pages(response.headers, paged_url)
                        if page >= total_pages:
                            break
                        page += 1
                        retries = 0  # esi_max_retries applies per page
                        pacing = _auth_pagination_sleep_seconds()
                        _monitor(
                            "record_sleep",
                            lambda: dict(
                                kind="pagination",
                                seconds=pacing,
                                context="polite pacing",
                                method="GET",
                                endpoint=str(endpoint),
                                url=str(paged_url),
                            ),
                        )
                        time.sleep(pacing)  # polite pacing
                    elif response.status_code == 304:
                        # Still update limiter from headers (done above).
                        return jsonlib.loads(cached_data) if isinstance(cached_data, str) else cached_data
                    elif response.status_code == 403:
                        if not suppress_forbidden_log:
                            logging.warning(f"ESI GET 403 Forbidden: {paged_url}")
                        return None
                    elif response.status_code == 404:
                        if not suppress_not_found_log:
                            logging.warning(f"ESI GET 404 Not Found: {paged_url}")
                        return None
                    elif response.status_code in (420, 429, 500, 502, 503, 504):
                        retry_after = _parse_retry_after_seconds(response.headers)
                        limiter_wait = _ESI_ERROR_LIMITER.suggested_sleep_seconds()
                        backoff = (2 ** retries) + random.uniform(0, 1)
                        wait = max(backoff, retry_after, limiter_wait)
                        if retries + 1 >= max_attempts:
                            raise EsiRequestError(f"ESI GET failed after retries ({response.status_code}): {paged_url}")
                        logging.warning(f"ESI GET {response.status_code} on {paged_url}, retrying in {wait:.1f}s...")
                        _monitor(
                            "record_retry_event",
                            lambda: dict(
                                reason=str(response.status_code),
                                sleep_seconds=float(wait),
                                method="GET",
                                endpoint=str(endpoint),
                                url=str(paged_url),
                            ),
                        )
                        time.sleep(wait)
                        retries += 1
                        continue
                    else:
                        response.raise_for_status()
                except requests.RequestException as e:
                    _monitor(
                        "record_http_attempt",
                        lambda: dict(
                            method="GET",
                            endpoint=str(endpoint),
                            url=str(paged_url),
                            status_code=None,
                            elapsed_ms=((time.time() - _t0) * 1000.0) if _t0 is not None else None,
                            headers=None,
                            exception=e,
                            cache_mode=cache_mode,
                            page=int(page),
                        ),
                    )
                    next_retries = retries + 1
                    wait = 2 ** next_retries
                    will_retry = next_retries < max_attempts
                    log_key = _esi_error_log_key(method="GET", endpoint=str(endpoint) if endpoint is not None else None, url=str(paged_url))
                    if will_retry:
                        if _ESI_REQUEST_ERROR_LOG_LIMITER.allow(log_key):
                            logging.warning(
                                "ESI GET request error %s: %s (retry %s/%s in %.1fs)",
                                str(paged_url),
                                str(e),
                                next_retries,
                                max_attempts,
                                float(wait),
                            )
                    else:
                        logging.error(
                            "ESI GET request failed %s: %s (attempt %s/%s)",
                            str(paged_url),
                            str(e),
                            next_retries,
                            max_attempts,
                        )

                    retries = next_retries
                    _monitor(
                        "record_retry_event",
                        lambda: dict(
                            reason=f"exception:{type(e).__name__}",
                            sleep_seconds=float(wait),
                            method="GET",
                            endpoint=str(endpoint),
                            url=str(paged_url),
                        ),
                    )
                    if retries < max_attempts:  # no wait after the last attempt
                        time.sleep(wait)
                    if retries >= max_attempts:
                        raise EsiRequestError(f"ESI GET failed after retries: {paged_url}")
            if return_headers:
                return all_data, last_headers
            return all_data

        # Non-paginated (original logic)
        while retries < max_attempts:
            _t0: Optional[float] = None
            try:
                _esi_gate(f"GET {endpoint}")
                _t0 = time.time()
                response = self._http_session.get(
                    url,
                    headers=headers,
                    timeout=float(timeout_seconds),
                )
                _monitor(
                    "record_http_attempt",
                    lambda: dict(
                        method="GET",
                        endpoint=str(endpoint),
                        url=str(url),
                        status_code=int(response.status_code) if response is not None else None,
                        elapsed_ms=(time.time() - _t0) * 1000.0,
                        headers=getattr(response, "headers", None),
                        exception=None,
                        cache_mode=cache_mode,
                        page=None,
                    ),
                )
                _ESI_ERROR_LIMITER.update_from_headers(response.headers)
                if response.status_code == 200:
                    etag = response.headers.get("ETag")
                    data_json = response.json()
                    self.save_to_cache(cache_key, etag, data_json)
                    if return_headers:
                        return data_json, response.headers
                    return data_json  # <-- Add this!
                elif response.status_code == 304:
                    return jsonlib.loads(cached_data) if isinstance(cached_data, str) else cached_data
                elif response.status_code == 403:
                    if not suppress_forbidden_log:
                        logging.warning(f"ESI 403 GET Forbidden: {url}")
                    return None
                elif response.status_code == 404:
                    if not suppress_not_found_log:
                        logging.warning(f"ESI 404 GET Not Found: {url}")
                    return None
                elif response.status_code in (420, 429, 500, 502, 503, 504):
                    retry_after = _parse_retry_after_seconds(response.headers)
                    limiter_wait = _ESI_ERROR_LIMITER.suggested_sleep_seconds()
                    backoff = (2 ** retries) + random.uniform(0, 1)
                    wait = max(backoff, retry_after, limiter_wait)
                    logging.warning(f"ESI GET{response.status_code} on {url}, retrying in {wait:.1f}s...")
                    _monitor(
                        "record_retry_event",
                        lambda: dict(
                            reason=str(response.status_code),
                            sleep_seconds=float(wait),
                            method="GET",
                            endpoint=str(endpoint),
                            url=str(url),
                        ),
                    )
                    if retries + 1 < max_attempts:  # no wait after the last attempt
                        time.sleep(wait)
                    retries += 1
                    continue
                else:
                    response.raise_for_status()
            except requests.RequestException as e:
                _monitor(
                    "record_http_attempt",
                    lambda: dict(
                        method="GET",
                        endpoint=str(endpoint),
                        url=str(url),
                        status_code=None,
                        elapsed_ms=((time.time() - _t0) * 1000.0) if _t0 is not None else None,
                        headers=None,
                        exception=e,
                        cache_mode=cache_mode,
                        page=None,
                    ),
                )
                next_retries = retries + 1
                wait = 2 ** next_retries
                will_retry = next_retries < max_attempts
                log_key = _esi_error_log_key(method="GET", endpoint=str(endpoint) if endpoint is not None else None, url=str(url))
                if will_retry:
                    if _ESI_REQUEST_ERROR_LOG_LIMITER.allow(log_key):
                        logging.warning(
                            "ESI GET request error %s: %s (retry %s/%s in %.1fs)",
                            str(url),
                            str(e),
                            next_retries,
                            max_attempts,
                            float(wait),
                        )
                else:
                    logging.error(
                        "ESI GET request failed %s: %s (attempt %s/%s)",
                        str(url),
                        str(e),
                        next_retries,
                        max_attempts,
                    )

                retries = next_retries
                _monitor(
                    "record_retry_event",
                    lambda: dict(
                        reason=f"exception:{type(e).__name__}",
                        sleep_seconds=float(wait),
                        method="GET",
                        endpoint=str(endpoint),
                        url=str(url),
                    ),
                )
                if retries < max_attempts:  # no wait after the last attempt
                    time.sleep(wait)

        raise EsiRequestError(f"ESI GET failed after retries: {url}")
    
    def esi_post(self, endpoint: str, json: Optional[dict] = None, headers: Optional[dict] = None, use_cache: bool = False, paginate: bool = False, return_headers: bool = False, timeout: float | None = None) -> Any:
        """
        Issue a POST request to the ESI API with caching.
        If paginate=True, will fetch all pages and return a combined list.
        If return_headers=True, returns (data, headers) for the last page.
        timeout None: esi_resilience/esi_request_timeout_seconds; the attempts
        are 1 + esi_resilience/esi_max_retries.
        """
        if not self.access_token or (self.token_expiry and time.time() > self.token_expiry):
            self.refresh_access_token()
        if timeout is None:
            timeout = esi_request_timeout_seconds()
        max_attempts = esi_max_retries() + 1
        if headers is None:
            headers = {
                "Accept": self.esi_header_accept,
                "Accept-Language": self.esi_header_acceptlanguage,
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": self.user_agent,
                "X-Compatibility-Date": self.esi_header_xcompatibilitydate,
                "X-Tenant": self.esi_header_xtenant
            }
        url = f"{self.esi_base_uri}{endpoint}"

        # Build cache key
        cache_key = esi_cache_key(f"{endpoint}:{json}" if json else endpoint)
        etag: Optional[str] = None
        cached_data: Optional[Dict[str, Any]] = None
        cache_mode = "off"
        if use_cache:
            cached_data, cache_entry = self._cache_lookup(cache_key)
            if cache_entry and cache_entry.etag:
                headers["If-None-Match"] = cache_entry.etag
            cache_mode = "enabled_with_entry" if (cached_data is not None or cache_entry is not None) else "enabled_no_entry"

        retries = 0

        # Pagination logic
        if paginate:
            all_data = []
            page = 1
            last_headers = {}
            while True:
                paged_json = dict(json) if json else {}
                paged_json["page"] = page
                _t0: Optional[float] = None
                try:
                    _esi_gate(f"POST {endpoint} page={page}")
                    _t0 = time.time()
                    response = requests.post(
                        url,
                        headers=headers,
                        json=paged_json,
                        timeout=timeout,
                        **get_requests_ssl_kwargs(),
                    )
                    _monitor(
                        "record_http_attempt",
                        lambda: dict(
                            method="POST",
                            endpoint=str(endpoint),
                            url=str(url),
                            status_code=int(response.status_code) if response is not None else None,
                            elapsed_ms=(time.time() - _t0) * 1000.0,
                            headers=getattr(response, "headers", None),
                            exception=None,
                            cache_mode=cache_mode,
                            page=int(page),
                        ),
                    )
                    _ESI_ERROR_LIMITER.update_from_headers(response.headers)
                    if response.status_code == 200:
                        etag = response.headers.get("ETag")
                        data_json = response.json()
                        all_data.extend(data_json if isinstance(data_json, list) else [data_json])
                        last_headers = response.headers
                        total_pages = esi_total_pages(response.headers, url)
                        if page >= total_pages:
                            break
                        page += 1
                        retries = 0  # esi_max_retries applies per page
                        pacing = _auth_pagination_sleep_seconds()
                        _monitor(
                            "record_sleep",
                            lambda: dict(
                                kind="pagination",
                                seconds=pacing,
                                context="polite pacing",
                                method="POST",
                                endpoint=str(endpoint),
                                url=str(url),
                            ),
                        )
                        time.sleep(pacing)
                    elif response.status_code == 304:
                        return jsonlib.loads(cached_data) if isinstance(cached_data, str) else cached_data
                    elif response.status_code == 403:
                        logging.warning(f"ESI POST 403 Forbidden: {url}")
                        return None
                    elif response.status_code == 404:
                        logging.warning(f"ESI POST 404 Not Found: {url}")
                        return None
                    elif response.status_code in (420, 429, 500, 502, 503, 504):
                        retry_after = _parse_retry_after_seconds(response.headers)
                        limiter_wait = _ESI_ERROR_LIMITER.suggested_sleep_seconds()
                        backoff = (2 ** retries) + random.uniform(0, 1)
                        wait = max(backoff, retry_after, limiter_wait)
                        if retries + 1 >= max_attempts:
                            raise EsiRequestError(f"ESI POST failed after retries ({response.status_code}): {url}")
                        logging.warning(f"ESI POST {response.status_code} on {url}, retrying in {wait:.1f}s...")
                        _monitor(
                            "record_retry_event",
                            lambda: dict(
                                reason=str(response.status_code),
                                sleep_seconds=float(wait),
                                method="POST",
                                endpoint=str(endpoint),
                                url=str(url),
                            ),
                        )
                        time.sleep(wait)
                        retries += 1
                        continue
                    else:
                        response.raise_for_status()
                except requests.RequestException as e:
                    _monitor(
                        "record_http_attempt",
                        lambda: dict(
                            method="POST",
                            endpoint=str(endpoint),
                            url=str(url),
                            status_code=None,
                            elapsed_ms=((time.time() - _t0) * 1000.0) if _t0 is not None else None,
                            headers=None,
                            exception=e,
                            cache_mode=cache_mode,
                            page=int(page),
                        ),
                    )
                    logging.error(f"ESI POST request error {url}: {e}")
                    retries += 1
                    wait = 2 ** retries
                    _monitor(
                        "record_retry_event",
                        lambda: dict(
                            reason=f"exception:{type(e).__name__}",
                            sleep_seconds=float(wait),
                            method="POST",
                            endpoint=str(endpoint),
                            url=str(url),
                        ),
                    )
                    if retries < max_attempts:  # no wait after the last attempt
                        time.sleep(wait)
                    if retries >= max_attempts:
                        raise EsiRequestError(f"ESI POST failed after retries: {url}")
       
        while retries < max_attempts:
            _t0: Optional[float] = None
            try:
                _esi_gate(f"POST {endpoint}")
                _t0 = time.time()
                response = requests.post(
                    url,
                    headers=headers,
                    json=json,
                    timeout=timeout,
                    **get_requests_ssl_kwargs(),
                )
                _monitor(
                    "record_http_attempt",
                    lambda: dict(
                        method="POST",
                        endpoint=str(endpoint),
                        url=str(url),
                        status_code=int(response.status_code) if response is not None else None,
                        elapsed_ms=(time.time() - _t0) * 1000.0,
                        headers=getattr(response, "headers", None),
                        exception=None,
                        cache_mode=cache_mode,
                        page=None,
                    ),
                )
                _ESI_ERROR_LIMITER.update_from_headers(response.headers)
                logging.debug(f"ESI POST {url} responded with status {response.status_code}")
                logging.debug(f"ESI POST Request Headers: {response.request.headers}")
                logging.debug(f"ESI POST Request Body: {response.request.body}")
                logging.debug(f"ESI POST Response Headers: {response.headers}")
                logging.debug(f"ESI POST Response Body: {response.text}")
                if response.status_code == 200:
                    etag = response.headers.get("ETag")
                    data_json = response.json()
                    self.save_to_cache(cache_key, etag, data_json)
                    if return_headers:
                        return data_json, response.headers
                    return data_json
                elif response.status_code == 304:
                    return jsonlib.loads(cached_data) if isinstance(cached_data, str) else cached_data
                elif response.status_code == 403:
                    logging.warning(f"ESI POST 403 Forbidden: {url}")
                    return None
                elif response.status_code == 404:
                    logging.warning(f"ESI POST 404 Not Found: {url}")
                    return None
                elif response.status_code in (420, 429, 500, 502, 503, 504):
                    retry_after = _parse_retry_after_seconds(response.headers)
                    limiter_wait = _ESI_ERROR_LIMITER.suggested_sleep_seconds()
                    backoff = (2 ** retries) + random.uniform(0, 1)
                    wait = max(backoff, retry_after, limiter_wait)
                    logging.warning(f"ESI POST {response.status_code} on {url}, retrying in {wait:.1f}s...")
                    _monitor(
                        "record_retry_event",
                        lambda: dict(
                            reason=str(response.status_code),
                            sleep_seconds=float(wait),
                            method="POST",
                            endpoint=str(endpoint),
                            url=str(url),
                        ),
                    )
                    if retries + 1 < max_attempts:  # no wait after the last attempt
                        time.sleep(wait)
                    retries += 1
                    continue
                else:
                    response.raise_for_status()
            except requests.RequestException as e:
                _monitor(
                    "record_http_attempt",
                    lambda: dict(
                        method="POST",
                        endpoint=str(endpoint),
                        url=str(url),
                        status_code=None,
                        elapsed_ms=((time.time() - _t0) * 1000.0) if _t0 is not None else None,
                        headers=None,
                        exception=e,
                        cache_mode=cache_mode,
                        page=None,
                    ),
                )
                logging.error(f"ESI POST request error {url}: {e}")
                retries += 1
                wait = 2 ** retries
                _monitor(
                    "record_retry_event",
                    lambda: dict(
                        reason=f"exception:{type(e).__name__}",
                        sleep_seconds=float(wait),
                        method="POST",
                        endpoint=str(endpoint),
                        url=str(url),
                    ),
                )
                if retries < max_attempts:  # no wait after the last attempt
                    time.sleep(wait)

        raise EsiRequestError(f"ESI POST failed after retries: {url}")
    
    def get_id_type(self, entity_id: int) -> Optional[str]:
        """Get the type of an entity by its ID."""
        if 500000 <= entity_id <= 599999:
            return "faction"
        elif 1000000 <= entity_id <= 1999999:
            return "npc_corporation"
        elif 3000000 <= entity_id <= 3999999:
            return "npc_character"
        elif 900000 <= entity_id <= 999999:
            return "universe"
        # EVE Universe IDs (ESI):
        # - Regions:         10000000-19999999
        # - Constellations:  20000000-29999999
        # - Solar systems:   30000000-39999999
        elif 10000000 <= entity_id <= 19999999:
            return "region"
        elif 20000000 <= entity_id <= 29999999:
            return "constellation"
        elif 30000000 <= entity_id <= 39999999:
            return "solar_system"
        elif 40000000 <= entity_id <= 49999999:
            return "celestial"
        elif 50000000 <= entity_id <= 59999999:
            return "stargate"
        elif 60000000 <= entity_id <= 69999999:
            return "station"
        elif 70000000 <= entity_id <= 79999999:
            return "asteroid"
        elif 80000000 <= entity_id <= 80099999:
            return "control_bunker"
        elif 81000000 <= entity_id <= 81999999:
            return "wis_promenade"
        elif 82000000 <= entity_id <= 84999999:
            return "planetary_district"
        elif 90000000 <= entity_id <= 97999999:
            return "character" # 2010-2016
        elif 98000000 <= entity_id <= 98999999:
            return "corporation" # post 2010
        elif 99000000 <= entity_id <= 99999999:
            return "alliance" # post 2010
        elif 100000000 <= entity_id <= 2099999999:
            return "character_corp_alliance" # pre 2010
        elif 2100000000 <= entity_id <= 2111999999:
            return "character" # dust post 2016
        elif 2112000000 <= entity_id <= 2129999999:
            return "character" # post 2016
        elif 1000000000000 <= entity_id < 1020000000000:
            return "spawned_item"
        elif entity_id >= 1020000000000: # Upwell structures (player owned)
            return "structure"
