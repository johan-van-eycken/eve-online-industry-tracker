
import logging
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from classes.database_manager import DatabaseManager
from classes.config_manager import ConfigManager
from classes.character import Character
from classes.database_models import OAuthCharacter

# ----------------------------
# Characters Manager
# ----------------------------
class CharacterManager():
    def __init__(self, cfg: ConfigManager, db_oauth: DatabaseManager, db_app: DatabaseManager, db_sde: DatabaseManager, cfg_characters: List[Dict[str, Any]]):
        """
        Initialize the CharacterManager with database managers and character configurations.

        :param cfg: Configuration manager
        :param db_oauth: DatabaseManager for OAuth operations
        :param db_app: DatabaseManager for app-specific tables
        :param db_sde: DatabaseManager for static data export (SDE) assets
        :param cfg_characters: List of character configurations (name, is_main, tokens, etc.)
        """
        self.cfg = cfg
        self.db_oauth = db_oauth
        self.db_app = db_app
        self.db_sde = db_sde
        self.cfg_characters = cfg_characters
        self.character_list: List[Character] = []

        # Initialize Characters
        self._validate_cfg_characters()
        self._initialize_characters()

    # ----------------------------
    # Manage Authenticated Characters
    # ----------------------------
    def _validate_cfg_characters(self) -> None:
        """Validate that cfg_characters is a list with valid properties."""
        if not isinstance(self.cfg_characters, list) or len(self.cfg_characters) == 0:
            raise ValueError("Character configuration must be a non-empty list.")
        
        for char_cfg in self.cfg_characters:
            if not isinstance(char_cfg, dict) or "character_name" not in char_cfg:
                raise ValueError(f"Invalid character configuration: {char_cfg}")

    def _get_existing_token(self, character_name: str) -> Optional[str]:
        """Check db_oauth for an existing refresh token for the given character."""
        character_row = self.db_oauth.session.query(OAuthCharacter).filter_by(character_name=character_name).first()
        if character_row and character_row.refresh_token:
            logging.debug(f"Found existing refresh token for character: {character_name}")
            if isinstance(character_row.refresh_token, str):  # Ensure it's a valid string
                return character_row.refresh_token
            else:
                logging.error(f"Invalid token type for character: {character_name}")
        return None

    def _initialize_characters(self) -> None:
        """Initialize a list of authenticated characters."""
        # Determine main character
        cfg_main_char = next((c for c in self.cfg_characters if c.get("is_main", False)), None)
        if not cfg_main_char:
            logging.warning("No main character found in the configuration. Using the first character in the list as main.")
            self.cfg_characters[0]["is_main"] = True # Set first character as main, if none specified.

        # Initialize characters
        for char_cfg in self.cfg_characters:
            character_name = char_cfg["character_name"]
            character_is_main = char_cfg.get("is_main", False)
            try:
                # Check for existing refresh token in the database
                logging.debug(f"Starting initialization for {character_name}{' as main' if character_is_main else ' '}...")
                existing_token = self._get_existing_token(character_name)
                if not existing_token:
                    logging.debug(f"Found no existing_token for {character_name}.")
                else:
                    logging.debug(f"Found existing_token for {character_name} : {existing_token}.")
                
                char = Character(
                    self.cfg,
                    self.db_oauth,
                    self.db_app,
                    self.db_sde,
                    character_name,
                    character_is_main,
                    existing_token
                )
                self.character_list.append(char)
                logging.debug(f"Initialized character: {char_cfg['character_name']} ({'main' if char_cfg.get('is_main', False) else 'alt'}).")
            except KeyError as e:
                logging.error(f"Failed to initialize character {char_cfg}. Missing key: {e}")
            except Exception as e:
                logging.error(f"Error initializing character {char_cfg['character_name']}: {e}")
        
    def refresh_wallet_balance(self, character_name: Optional[str] = None) -> List[Dict]:
        """
        Refresh wallet balances for one or all characters.

        :param character_name: Optionally specify a single character to refresh.
        :return: JSON response containing wallet balances.
        """
        if character_name:
            # Refresh wallet balance for a single character
            result = [
                char.refresh_wallet_balance()
                for char in self.character_list
                if char.character_name == character_name
            ]
            return result

        # Refresh wallet balances for all characters
        result = [char.refresh_wallet_balance() for char in self.character_list]
        return result
    
    def refresh_profile(self, character_name: Optional[str] = None) -> List[Dict]:
        """
        Refresh profiles for one or all characters.

        :param character_name: Optionally specify a single character to refresh.
        :return: JSON response containing profile data.
        """
        if character_name:
            # refresh profile for a single character
            result = [
                char.refresh_profile()
                for char in self.character_list
                if char.character_name == character_name
            ]
            return result

        # Refresh profile data for all characters
        result = [char.refresh_profile() for char in self.character_list]
        return result

    def refresh_skills(self, character_name: Optional[str] = None) -> List[Dict]:
        if character_name:
            # Refresh skills for a single character
            result = [
                char.refresh_skills()
                for char in self.character_list
                if char.character_name == character_name
            ]
            return result
        
        # Refresh skills for all characters
        result = [char.refresh_skills() for char in self.character_list]
        return result