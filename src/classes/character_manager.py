import logging
from typing import Optional, List, Dict

from classes.database_manager import DatabaseManager
from classes.config_manager import ConfigManager
from classes.character import Character
from classes.database_models import OAuthCharacter

class CharacterManager:
    """Manages multiple Character objects and provides batch utilities."""

    def __init__(
        self,
        cfgManager: ConfigManager,
        db_oauth: DatabaseManager,
        db_app: DatabaseManager,
        db_sde: DatabaseManager,
        corporation_id: Optional[int] = None,
        char_manager: Optional["CharacterManager"] = None
    ) -> None:
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
                    existing_token
                )
                self._character_list.append(char)
            except KeyError as e:
                logging.error(f"Failed to initialize character {char_cfg}. Missing key: {e}")
            except Exception as e:
                logging.error(f"Error initializing character {char_cfg['character_name']}: {e}")
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
        if not self._character_list:
            raise ValueError("No characters available in the manager.")
        try:
            characters_data = [c.get_character() for c in self._character_list]
            return characters_data
        except Exception as e:
            error_message = f"Error retrieving characters: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def get_wallet_balances(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> Optional[List[Dict[str, float]]]:
        balances = []
        for char in self._character_list:
            try:
                self._refresh_batch("refresh_wallet_balance", character_name, character_id)
                balance = char.get_wallet_balance()
                balances.append(balance)
            except Exception as e:
                error_message = f"Error retrieving wallet balance for {char.character_name}: {str(e)}"
                logging.error(error_message)
                raise Exception(error_message)
        return balances
    
    def get_assets(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> Optional[List[Dict[str, object]]]:
        assets_list = []
        for char in self._character_list:
            try:
                self._refresh_batch("refresh_assets", character_name, character_id)
                assets = char.get_assets()
                assets_list.append(assets)
            except Exception as e:
                error_message = f"Error retrieving assets for {char.character_name}: {str(e)}"
                logging.error(error_message)
                raise Exception(error_message)
        return assets_list
    
    def get_market_orders(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> Optional[List[Dict[str, object]]]:
        orders_list = []
        for char in self._character_list:
            try:
                self._refresh_batch("refresh_market_orders", character_name, character_id)
                orders = char.get_market_orders()
                orders_list.append(orders)
            except Exception as e:
                error_message = f"Error retrieving market orders for {char.character_name}: {str(e)}"
                logging.error(error_message)
                raise Exception(error_message)
        return orders_list

    def _refresh_batch(self, method_name: str, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        try:
            # Filter characters
            if character_id is not None:
                chars = [self.get_character_by_id(character_id)]
            elif character_name:
                chars = [self.get_character_by_name(character_name)]
            else:
                chars = self._character_list

            chars = [c for c in chars if c is not None]

            for char in chars:
                try:
                    if method_name.startswith("refresh") and hasattr(char, "ensure_esi"):
                        char.ensure_esi()
                    getattr(char, method_name)()
                except Exception as e:
                    error_message = f"Error during {method_name} for {char.character_name}: {e}"
                    logging.error(error_message)
                    raise Exception(error_message)
        except Exception as e:
            error_message = f"Batch refresh error in {method_name}: {e}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_all(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        try:
            self._refresh_batch("refresh_all", character_name, character_id)
            self._rebuild_indexes()
        except Exception as e:
            error_message = f"Failed to refresh all data: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_profile(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        try:
            self._refresh_batch("refresh_profile", character_name, character_id)
        except Exception as e:
            error_message = f"Failed to refresh profile: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_skills(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        try:
            self._refresh_batch("refresh_skills", character_name, character_id)
        except Exception as e:
            error_message = f"Failed to refresh skills: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_wallet_journal(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        try:
            self._refresh_batch("refresh_wallet_journal", character_name, character_id)
        except Exception as e:
            error_message = f"Failed to refresh wallet journal: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_wallet_transactions(self, character_name: Optional[str] = None, character_id: Optional[int] = None) -> None:
        try:
            self._refresh_batch("refresh_wallet_transactions", character_name, character_id)
        except Exception as e:
            error_message = f"Failed to refresh wallet transactions: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

