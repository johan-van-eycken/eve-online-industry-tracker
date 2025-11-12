import logging
import json
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from classes.database_manager import DatabaseManager
from classes.config_manager import ConfigManager
from classes.character import Character
from classes.database_models import OAuthCharacter

class CharacterManager:
    """
    Manages multiple Character objects and provides batch refresh and lookup utilities.
    """

    def __init__(
        self,
        cfgManager: ConfigManager,
        db_oauth: DatabaseManager,
        db_app: DatabaseManager,
        db_sde: DatabaseManager,
        corporation_id: Optional[int] = None,
        char_manager_all: Optional["CharacterManager"] = None
    ):
        self.cfgManager = cfgManager
        self.cfg = cfgManager.all()
        self.cfg_characters = self.cfg["characters"]
        self.db_oauth = db_oauth
        self.db_app = db_app
        self.db_sde = db_sde

        # Initialize Characters
        self.character_list: List[Character] = []
        self.char_by_id: Dict[int, Character] = {}
        self.char_by_name: Dict[str, Character] = {}

        if corporation_id is None and char_manager_all is None:
            self._validate_cfg_characters()
            self._initialize_characters()
        elif corporation_id is not None and char_manager_all is not None:
            self.character_list = [
                char for char in char_manager_all.character_list
                if char.corporation_id == corporation_id
            ]

        for char in self.character_list:
            if char.character_id is not None:
                self.char_by_id[char.character_id] = char
            self.char_by_name[char.character_name.lower()] = char

    def _validate_cfg_characters(self) -> None:
        if not isinstance(self.cfg_characters, list) or len(self.cfg_characters) == 0:
            raise ValueError("Character configuration must be a non-empty list.")
        for char_cfg in self.cfg_characters:
            if not isinstance(char_cfg, dict) or "character_name" not in char_cfg:
                raise ValueError(f"Invalid character configuration: {char_cfg}")

    def _get_existing_token(self, character_name: str) -> Optional[str]:
        character_row = self.db_oauth.session.query(OAuthCharacter).filter_by(character_name=character_name).first()
        if character_row and character_row.refresh_token:
            logging.debug(f"Found existing refresh token for character: {character_name}")
            if isinstance(character_row.refresh_token, str):
                return character_row.refresh_token
            else:
                logging.error(f"Invalid token type for character: {character_name}")
        return None

    def _initialize_characters(self) -> None:
        cfg_main_char = next((c for c in self.cfg_characters if c.get("is_main", False)), None)
        if not cfg_main_char:
            logging.warning("No main character found in the configuration. Using the first character in the list as main.")
            self.cfg_characters[0]["is_main"] = True

        cfg_corp_director = next((c for c in self.cfg_characters if c.get("is_corp_director", False)), None)
        if not cfg_corp_director:
            raise ValueError("No Corporation Director found in the configuration. Unable to continue the application.")

        for char_cfg in self.cfg_characters:
            character_name = char_cfg["character_name"]
            character_is_main = char_cfg.get("is_main", False)
            character_is_corp_director = char_cfg.get("is_corp_director", False)
            try:
                existing_token = self._get_existing_token(character_name)
                char = Character(
                    self.cfgManager,
                    self.db_oauth,
                    self.db_app,
                    self.db_sde,
                    character_name,
                    character_is_main,
                    character_is_corp_director,
                    existing_token
                )
                self.character_list.append(char)
            except KeyError as e:
                logging.error(f"Failed to initialize character {char_cfg}. Missing key: {e}")
            except Exception as e:
                logging.error(f"Error initializing character {char_cfg['character_name']}: {e}")

    def get_main_character(self) -> Optional[Character]:
        for char in self.character_list:
            if char.is_main:
                return char
        return None

    def get_corp_director(self) -> Optional[Character]:
        for char in self.character_list:
            if char.is_corp_director:
                return char
        return None

    def get_character_by_name(self, character_name: str) -> Optional[Character]:
        return self.char_by_name.get(character_name.lower())

    def get_character_by_id(self, character_id: int) -> Optional[Character]:
        return self.char_by_id.get(character_id)

    def get_all_characters(self) -> List[Character]:
        return self.character_list

    def _refresh_batch(self, method_name: str, character_name: Optional[str] = None, character_id: Optional[int] = None) -> List[str]:
        results = []
        # Filter characters
        if character_id is not None:
            chars = [self.get_character_by_id(character_id)]
        elif character_name:
            chars = [self.get_character_by_name(character_name)]
        else:
            chars = self.character_list

        chars = [c for c in chars if c is not None]

        for char in chars:
            try:
                result = getattr(char, method_name)()
            except Exception as e:
                logging.error(f"Failed to refresh {method_name} for {char.character_name}: {e}")
                result = json.dumps({'character_name': char.character_name, 'error': str(e)}, indent=4)
            results.append(result)
        return results

    def refresh_all(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> List[str]:
        return self._refresh_batch("refresh_all", character_name, character_id)

    def refresh_wallet_balance(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> List[str]:
        return self._refresh_batch("refresh_wallet_balance", character_name, character_id)

    def refresh_profile(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> List[str]:
        return self._refresh_batch("refresh_profile", character_name, character_id)

    def refresh_skills(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> List[str]:
        return self._refresh_batch("refresh_skills", character_name, character_id)

    def refresh_wallet_journal(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> List[str]:
        return self._refresh_batch("refresh_wallet_journal", character_name, character_id)

    def refresh_wallet_transactions(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> List[str]:
        return self._refresh_batch("refresh_wallet_transactions", character_name, character_id)

    def refresh_market_orders(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> List[str]:
        return self._refresh_batch("refresh_market_orders", character_name, character_id)

    def refresh_assets(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> List[str]:
        return self._refresh_batch("refresh_assets", character_name, character_id)