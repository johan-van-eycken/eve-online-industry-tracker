import logging
import json
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.character_manager import CharacterManager
from classes.corporation import Corporation

class CorporationManager:
    """
    Manages multiple Corporation objects and provides batch refresh and lookup utilities.
    """

    def __init__(
        self,
        cfgManager: ConfigManager,
        db_oauth: DatabaseManager,
        db_app: DatabaseManager,
        db_sde: DatabaseManager,
        char_manager_all: CharacterManager
    ):
        self.cfgManager = cfgManager
        self.cfg = cfgManager.all()
        self.db_oauth = db_oauth
        self.db_app = db_app
        self.db_sde = db_sde
        self.char_manager_all = char_manager_all

        # Determine unique corporation IDs (skip None) of player owned corporations
        self.corporation_ids = {
            char.corporation_id
            for char in char_manager_all.character_list
            if char.corporation_id is not None and char.is_corp_director is True
        }

        # Lookup dictionaries for fast access
        self.corporations: List[Corporation] = []
        self.corp_by_id: Dict[int, Corporation] = {}
        self.corp_by_name: Dict[str, Corporation] = {}

        for corp_id in self.corporation_ids:
            char_manager_corp = CharacterManager(
                self.cfgManager,
                self.db_oauth,
                self.db_app,
                self.db_sde,
                corp_id,
                char_manager_all
            )
            corp = Corporation(
                self.cfgManager,
                self.db_oauth,
                self.db_app,
                self.db_sde,
                corp_id,
                char_manager_corp
            )
            self.corporations.append(corp)
            self.corp_by_id[corp_id] = corp
            if corp.corporation_name:
                self.corp_by_name[corp.corporation_name.lower()] = corp

    def get_corporation(self, corporation_id: Optional[int] = None, corporation_name: Optional[str] = None) -> Optional[Corporation]:
        """
        Return the Corporation object for a given ID or name, or None if not found.
        """
        if corporation_id is not None:
            return self.corp_by_id.get(corporation_id)
        if corporation_name:
            return self.corp_by_name.get(corporation_name.lower())
        return None

    def get_corporation_name_by_character_id(self, character_id: int) -> Optional[str]:
        """
        Retrieve the corporation name for a given character_id.
        Returns None if not found.
        """
        character = next(
            (char for char in self.char_manager_all.character_list if char.character_id == character_id),
            None
        )
        if not character or character.corporation_id is None:
            return None
        corp = self.get_corporation(corporation_id=character.corporation_id)
        if corp and corp.corporation_name:
            return corp.corporation_name
        return None

    def _refresh_batch(self, method_name: str, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> List[str]:
        """
        Internal helper to refresh data for one or all corporations using parallel threads.
        Returns a list of JSON strings.
        """
        results = []
        # Filter corporations
        if corporation_id is not None:
            corps = [self.get_corporation(corporation_id=corporation_id)]
        elif corporation_name:
            corps = [self.get_corporation(corporation_name=corporation_name)]
        else:
            corps = self.corporations

        corps = [c for c in corps if c is not None]

        # Use ThreadPoolExecutor for parallel refresh
        with ThreadPoolExecutor(max_workers=min(8, len(corps))) as executor:
            future_to_corp = {
                executor.submit(getattr(corp, method_name)): corp for corp in corps
            }
            for future in as_completed(future_to_corp):
                corp = future_to_corp[future]
                try:
                    result = future.result()
                except Exception as e:
                    logging.error(f"Failed to refresh {method_name} for {corp.corporation_name}: {e}")
                    result = json.dumps({'corporation_name': corp.corporation_name, 'error': str(e)}, indent=4)
                results.append(result)
        return results

    def refresh_all(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> List[str]:
        """
        Refresh all corporation data for one or all corporations.
        :param corporation_name: Optionally specify a single corporation by name.
        :param corporation_id: Optionally specify a single corporation by ID.
        :return: List of JSON responses containing all corporation data.
        """
        return self._refresh_batch("refresh_all", corporation_name, corporation_id)

    def refresh_members(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> List[str]:
        """
        Refresh corporation members for one or all corporations.
        :param corporation_name: Optionally specify a single corporation by name.
        :param corporation_id: Optionally specify a single corporation by ID.
        :return: List of JSON responses containing all corporation members data.
        """
        return self._refresh_batch("refresh_members", corporation_name, corporation_id)

    def refresh_structures(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> List[str]:
        """
        Refresh corporation structures for one or all corporations.
        :param corporation_name: Optionally specify a single corporation by name.
        :param corporation_id: Optionally specify a single corporation by ID.
        :return: List of JSON responses containing all corporation structures data.
        """
        return self._refresh_batch("refresh_structures", corporation_name, corporation_id)

    def refresh_assets(self, corporation_name: Optional[str] = None, corporation_id: Optional[int] = None) -> List[str]:
        """
        Refresh corporation assets for one or all corporations.
        :param corporation_name: Optionally specify a single corporation by name.
        :param corporation_id: Optionally specify a single corporation by ID.
        :return: List of JSON responses containing all corporation assets data.
        """
        return self._refresh_batch("refresh_assets", corporation_name, corporation_id)