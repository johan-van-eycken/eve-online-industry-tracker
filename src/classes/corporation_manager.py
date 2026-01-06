import logging
import json
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.character_manager import CharacterManager
from classes.corporation import Corporation

class CorporationManager:
    """Manages multiple Character objects and provides batch utilities."""

    def __init__(
        self,
        cfgManager: ConfigManager,
        db_oauth: DatabaseManager,
        db_app: DatabaseManager,
        db_sde: DatabaseManager,
        char_manager: CharacterManager
    ) -> None:
        try:
            # Private arguments
            self._cfgManager = cfgManager
            self._cfg = cfgManager.all()
            self._db_oauth = db_oauth
            self._db_app = db_app
            self._db_sde = db_sde
            self._char_manager = char_manager
            self._corporations: List[Corporation] = []
            self._corp_by_id: Dict[int, Corporation] = {}
            self._corp_by_name: Dict[str, Corporation] = {}

            # Determine unique corporation IDs of player owned corporations
            self._corporation_ids = {
                char.corporation_id
                for char in self._char_manager._character_list
                if char.corporation_id is not None and char.is_corp_director is True
            }

            self._initialize_corporations()
        except Exception as e:
            error_message = f"Failed to initialize CorporationManager: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)
    
    def _initialize_corporations(self) -> None:
        for corp_id in self._corporation_ids:
            try:
                char_manager_corp = CharacterManager(self._cfgManager, self._db_oauth, self._db_app, self._db_sde, corp_id, self._char_manager)
            except KeyError as e:
                logging.error(f"Failed to initialize character manager for corporation ID {corp_id}. Missing key: {e}")
            except Exception as e:
                logging.error(f"Error initializing character manager for corporation ID {corp_id}: {e}")

            try:
                corp = Corporation(self._cfgManager, self._db_app, self._db_sde, corp_id, char_manager_corp)
            except KeyError as e:
                logging.error(f"Failed to initialize corporation {corp_id}. Missing key: {e}")
            except Exception as e:
                logging.error(f"Error initializing corporation {corp_id}: {e}")

            if corp is None:
                continue

            # Constructor is cheap now; refresh is handled explicitly (e.g., by Flask bootstrap).
            self._corporations.append(corp)
            self._corp_by_id[corp_id] = corp
            if corp.corporation_name:
                self._corp_by_name[corp.corporation_name.lower()] = corp

        return None

    def get_corporation(self, corporation_id: Optional[int] = None, corporation_name: Optional[str] = None) -> Optional[Corporation]:
        """Return the Corporation object for a given ID or name, or None if not found."""
        if corporation_id is not None:
            return self._corp_by_id.get(corporation_id)
        if corporation_name:
            return self._corp_by_name.get(corporation_name.lower())
        return None

    def get_corporation_name_by_character_id(self, character_id: int) -> Optional[str]:
        """Retrieve the corporation name for a given character_id."""
        character = next(
            (char for char in self._char_manager._character_list if char.character_id == character_id),
            None
        )
        if not character or character.corporation_id is None:
            return None
        corp = self.get_corporation(corporation_id=character.corporation_id)
        if corp and corp.corporation_name:
            return corp.corporation_name
        return None

    def get_corporations(self) -> List[Dict[str, object]]:
        corporations_data = [c.get_corporation() for c in self._corporations]
        return corporations_data
    
    def get_assets(self) -> List[Dict[str, object]]:
        all_assets = []
        for corp in self._corporations:
            try:
                self._refresh_batch("refresh_assets", corporation_id=corp.corporation_id)
                corp_assets = corp.get_assets()
                all_assets.extend(corp_assets)
            except Exception as e:
                logging.error(f"Error retrieving assets for corporation {corp.corporation_name}: {e}")
        return all_assets
    
    def get_members(self) -> List[Dict[str, object]]:
        all_members = []
        for corp in self._corporations:
            try:
                self._refresh_batch("refresh_members", corporation_id=corp.corporation_id)
                corp_members = corp.get_members()
                all_members.extend(corp_members)
            except Exception as e:
                logging.error(f"Error retrieving members for corporation {corp.corporation_name}: {e}")
        return all_members
    
    def get_structures(self) -> List[Dict[str, object]]:
        all_structures = []
        for corp in self._corporations:
            try:
                self._refresh_batch("refresh_structures", corporation_id=corp.corporation_id)
                corp_structures = corp.get_structures()
                all_structures.extend(corp_structures)
            except Exception as e:
                logging.error(f"Error retrieving structures for corporation {corp.corporation_name}: {e}")
        return all_structures

    def _refresh_batch(self, method_name: str, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        """Internal helper to refresh data for one or all corporations."""
        try:
            # Filter corporations
            if corporation_id is not None:
                corps = [self.get_corporation(corporation_id=corporation_id)]
            elif corporation_name:
                corps = [self.get_corporation(corporation_name=corporation_name)]
            else:
                corps = self._corporations

            corps = [c for c in corps if c is not None]

            for corp in corps:
                try:
                    getattr(corp, method_name)()
                except Exception as e:
                    error_message = f"Error during {method_name} for {corp.corporation_name}: {e}"
                    logging.error(error_message)
                    raise Exception(error_message)
            logging.debug(f"Batch refresh '{method_name}' completed for {len(corps)} corporations.")
        except Exception as e:
            error_message = f"Failed to refresh batch for method '{method_name}': {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)

    def refresh_all(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> None:
        """Refresh all corporation data for one or all corporations."""
        try:
            self._refresh_batch("refresh_all", corporation_name, corporation_id)
        except Exception as e:
            error_message = f"Failed to refresh all corporation data: {str(e)}"
            logging.error(error_message)
            raise Exception(error_message)
