import logging
from typing import Optional, List

from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.character_manager import CharacterManager
from classes.corporation import Corporation

class CorporationManager:
    def __init__(self,
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

        # Initialize Corporation objects
        self.corporations: List[Corporation] = []
        for corp_id in self.corporation_ids:
            # Create a corp-specific CharacterManager filtered by corporation_id
            char_manager_corp = CharacterManager(
                self.cfgManager,
                self.db_oauth,
                self.db_app,
                self.db_sde,
                corp_id,
                char_manager_all
            )

            # Create Corporation instance
            corp = Corporation(
                self.cfgManager,
                self.db_oauth,
                self.db_app,
                self.db_sde,
                corp_id,
                char_manager_corp
            )
            self.corporations.append(corp)

    def get_corporation(self, corporation_id: int) -> Optional["Corporation"]:
        """Return the Corporation object for a given ID, or None if not found."""
        for corp in self.corporations:
            if corp.corporation_id == corporation_id:
                return corp
        return None

    def refresh_all_corporations(self, corporation_data_fl: bool = True, members_fl: bool = True, structures_fl: bool = True) -> None:
        """Refresh data for all corporations managed by this CorporationManager."""
        try:
            for corp in self.corporations:
                if corporation_data_fl:
                    corp.refresh_corporation_data()
                if members_fl:
                    corp.refresh_members()
                if structures_fl:
                    corp.refresh_structures()
        except Exception as e:
            logging.error(f"Error refreshing corporations: {e}")
            raise