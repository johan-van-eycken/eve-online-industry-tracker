import logging
from typing import Any, Optional, List, Dict

from sqlalchemy.exc import SQLAlchemyError

from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.config.config_manager import ConfigManager
from eve_online_industry_tracker.application.characters.character import Character
from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS
from eve_online_industry_tracker.infrastructure.models import OAuthCharacter


class CharacterDataError(Exception):
    """A character's data could not be read (get_characters). Not a
    RuntimeError, so the ESI catch sites do not take it for an ESI failure;
    the cause is chained."""


class CharacterRefreshError(Exception):
    """A batch refresh in which one or more characters failed. Every
    character was tried; `errors` is {character name: error message}, each
    named in the message. Not a RuntimeError, so the ESI catch sites do not
    take it for an ESI failure."""

    def __init__(self, method_name: str, errors: Dict[str, str]):
        self.method_name = method_name
        self.errors = dict(errors)
        details = "; ".join(f"{name}: {message}" for name, message in self.errors.items())
        super().__init__(f"{method_name} failed for {len(self.errors)} character(s): {details}")


class UnknownCharacterError(CharacterRefreshError):
    """A refresh asked for a character_id or name the manager does not have:
    nothing was refreshed. `errors` names the unknown selector; the routes
    answer 404."""

    def __init__(self, method_name: str, selector: str):
        self.selector = selector
        super().__init__(method_name, {selector: "no such character in this manager"})


class CharacterManager:
    """Manages multiple Character objects and provides batch utilities."""

    def __init__(
        self,
        cfgManager: ConfigManager,
        db_oauth: DatabaseManager,
        db_app: DatabaseManager,
        db_sde: DatabaseManager,
        corporation_id: Optional[int] = None,
        char_manager: Optional["CharacterManager"] = None,
        admin_settings: Any = None,
    ) -> None:
        # The admin settings for the characters' market fees (industry/*); None: the schema defaults.
        self.admin_settings = admin_settings
        # Private arguments
        self._cfgManager = cfgManager
        self._cfg = cfgManager.all()
        self._cfg_characters = self._cfg["characters"]
        self._db_oauth = db_oauth
        self._db_app = db_app
        self._db_sde = db_sde
        self._character_list: List[Character] = []
        self._char_by_id: Dict[int, Character] = {}
        self._char_by_name: Dict[str, Character] = {}
        # One line per configured character that could not be initialised (bootstrap: init_warnings).
        self.init_errors: List[str] = []

        if corporation_id is None and char_manager is None:
            self._validate_cfg_characters()
            self._initialize_characters()
        elif corporation_id is not None and char_manager is not None:
            self._character_list = [
                char for char in char_manager._character_list
                if char.corporation_id == corporation_id
            ]

        self._rebuild_indexes()

    def _rebuild_indexes(self) -> None:
        self._char_by_id = {}
        self._char_by_name = {}
        for char in self._character_list:
            if char.character_id is not None:
                self._char_by_id[char.character_id] = char
            self._char_by_name[char.character_name.lower()] = char

    def _validate_cfg_characters(self) -> None:
        if not isinstance(self._cfg_characters, list) or len(self._cfg_characters) == 0:
            raise ValueError("Character configuration must be a non-empty list.")
        for char_cfg in self._cfg_characters:
            if not isinstance(char_cfg, dict) or "character_name" not in char_cfg:
                raise ValueError(f"Invalid character configuration: {char_cfg}")

    def _get_existing_token(self, character_name: str) -> Optional[str]:
        character_row = self._db_oauth.session.query(OAuthCharacter).filter_by(character_name=character_name).first()
        if character_row and character_row.refresh_token:
            logging.debug(f"Found existing refresh token for character: {character_name}")
            if isinstance(character_row.refresh_token, str):
                return character_row.refresh_token
            else:
                logging.error(f"Invalid token type for character: {character_name}")
        return None

    def _initialize_characters(self) -> None:
        cfg_main_char = next((c for c in self._cfg_characters if c.get("is_main", False)), None)
        if not cfg_main_char:
            logging.warning("No main character found in the configuration. Using the first character in the list as main.")
            self._cfg_characters[0]["is_main"] = True

        cfg_corp_director = next((c for c in self._cfg_characters if c.get("is_corp_director", False)), None)
        if not cfg_corp_director:
            logging.warning("No Corporation Director found in the configuration. Using the first character in the list as Corporation Director.")
            self._cfg_characters[0]["is_corp_director"] = True

        for char_cfg in self._cfg_characters:
            character_name = char_cfg["character_name"]
            character_is_main = char_cfg.get("is_main", False)
            character_is_corp_director = char_cfg.get("is_corp_director", False)
            character_industry_enabled = char_cfg.get("industry_enabled", True)
            try:
                existing_token = self._get_existing_token(character_name)
                char = Character(
                    self._cfgManager,
                    self._db_oauth,
                    self._db_app,
                    self._db_sde,
                    character_name,
                    character_is_main,
                    character_is_corp_director,
                    existing_token,
                    industry_enabled=character_industry_enabled,
                    admin_settings=self.admin_settings,
                )
                self._character_list.append(char)
            except KeyError as e:
                message = f"Failed to initialize character {character_name}. Missing key: {e}"
                logging.error(message)
                self.init_errors.append(message)
            except Exception as e:   # one character's failure does not stop the others; named in init_errors
                message = f"Error initializing character {character_name}: {e}"
                logging.error(message)
                self.init_errors.append(message)
        return None

    def get_main_character(self) -> Optional[Character]:
        for char in self._character_list:
            if char.is_main:
                return char
        return None

    def get_corp_director(self) -> Optional[Character]:
        for char in self._character_list:
            if char.is_corp_director:
                return char
        return None

    def get_character_by_name(self, character_name: str) -> Optional[Character]:
        return self._char_by_name.get(character_name.lower())

    def get_character_by_id(self, character_id: int) -> Optional[Character]:
        return self._char_by_id.get(character_id)

    def get_characters(self) -> Optional[List[Dict[str, object]]]:
        """Every character, serialized. ValueError when there are none yet;
        CharacterDataError when a character's data cannot be read (a DB
        failure). A programming error propagates as is."""
        if not self._character_list:
            raise ValueError("No characters available in the manager.")
        try:
            characters_data = [c.get_character() for c in self._character_list]
            return characters_data
        except SQLAlchemyError as e:
            error_message = f"Error retrieving characters: {str(e)}"
            logging.error(error_message)
            raise CharacterDataError(error_message) from e

    def get_wallet_balances(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> Optional[List[Dict[str, float]]]:
        return self._get_each("refresh_wallet_balance", "get_wallet_balance", "wallet balance", character_name, character_id)

    def get_assets(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> Optional[List[Dict[str, object]]]:
        return self._get_each("refresh_assets", "get_assets", "assets", character_name, character_id)

    def get_market_orders(
        self,
        character_name: Optional[str] = None,
        character_id: Optional[int] = None,
        *,
        refresh: bool = True,
    ) -> Optional[List[Dict[str, object]]]:
        return self._get_each("refresh_market_orders", "get_market_orders", "market orders", character_name, character_id,
                              refresh=refresh)

    def _get_each(
        self,
        refresh_method: str,
        getter: str,
        what: str,
        character_name: Optional[str],
        character_id: Optional[int],
        *,
        refresh: bool = True,
    ) -> List[Any]:
        """`getter` of each character, after `refresh_method`. With a
        character_id or name: that character only, refreshed once
        (UnknownCharacterError when it does not exist, CharacterRefreshError
        naming it when its refresh fails). Without one, every character, and
        the whole batch is refreshed before each read as before."""
        selected = character_id is not None or bool(character_name)
        if selected:
            chars = self._select_characters(refresh_method, character_name, character_id)
            if refresh:
                self._refresh_batch(refresh_method, character_name, character_id)
        else:
            chars = self._character_list

        results = []
        for char in chars:
            try:
                if refresh and not selected:
                    self._refresh_batch(refresh_method)
                results.append(getattr(char, getter)())
            except Exception as e:
                error_message = f"Error retrieving {what} for {char.character_name}: {str(e)}"
                logging.error(error_message)
                raise Exception(error_message)
        return results

    def _select_characters(self, method_name: str, character_name: Optional[str] = None,
                           character_id: Optional[int] = None) -> List[Character]:
        """The character with this id, else with this name, else every
        character. UnknownCharacterError when the id or name matches none."""
        if character_id is not None:
            char = self.get_character_by_id(character_id)
            if char is None:
                raise UnknownCharacterError(method_name, f"character_id {character_id}")
            return [char]
        if character_name:
            char = self.get_character_by_name(character_name)
            if char is None:
                raise UnknownCharacterError(method_name, f"character_name {character_name!r}")
            return [char]
        return list(self._character_list)

    def _refresh_batch(self, method_name: str, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        """Run `method_name` on each selected character. Every character is
        tried: one that fails is logged as an ERROR and the others go on;
        then CharacterRefreshError names each failed character. An unknown
        character_id or name raises UnknownCharacterError before any refresh."""
        chars = self._select_characters(method_name, character_name, character_id)

        errors: Dict[str, str] = {}
        for char in chars:
            try:
                if method_name.startswith("refresh") and hasattr(char, "ensure_esi"):
                    char.ensure_esi()
                getattr(char, method_name)()
            except Exception as e:   # the batch boundary: one character must not stop the others
                logging.error("Error during %s for %s: %s", method_name, char.character_name, e,
                              exc_info=not isinstance(e, ESI_REQUEST_ERRORS))   # a traceback unless ESI failed
                errors[char.character_name] = f"{type(e).__name__}: {e}"
        if errors:
            raise CharacterRefreshError(method_name, errors)

    def refresh_all(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        """refresh_all of each character (see _refresh_batch); the indexes are
        rebuilt either way, so the characters that refreshed are found by id."""
        try:
            self._refresh_batch("refresh_all", character_name, character_id)
        finally:
            self._rebuild_indexes()

    def refresh_profile(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        self._refresh_batch("refresh_profile", character_name, character_id)

    def refresh_skills(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        self._refresh_batch("refresh_skills", character_name, character_id)

    def refresh_wallet_journal(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        self._refresh_batch("refresh_wallet_journal", character_name, character_id)

    def refresh_wallet_transactions(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        self._refresh_batch("refresh_wallet_transactions", character_name, character_id)

    def refresh_industry_jobs(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        self._refresh_batch("refresh_industry_jobs", character_name, character_id)

    def refresh_realized_profit_inputs(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        """The wallet journal, transactions and industry jobs, each for every
        character; then one CharacterRefreshError naming each failure."""
        errors: Dict[str, str] = {}
        for refresh in (self.refresh_wallet_journal, self.refresh_wallet_transactions, self.refresh_industry_jobs):
            try:
                refresh(character_name=character_name, character_id=character_id)
            except UnknownCharacterError:
                raise   # nothing was refreshed: the same unknown character for every kind
            except CharacterRefreshError as exc:
                for name, message in exc.errors.items():
                    errors[f"{name} ({exc.method_name})"] = message
        if errors:
            raise CharacterRefreshError("refresh_realized_profit_inputs", errors)

