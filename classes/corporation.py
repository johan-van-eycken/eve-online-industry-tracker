from typing import Optional, List

from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.character_manager import CharacterManager

class Corporation:
    """
    Ingame entity of a corporation.
    """
    def __init__(self, 
                 cfgManager: ConfigManager,
                 db_oauth: DatabaseManager,
                 db_app: DatabaseManager,
                 db_sde: DatabaseManager,
                 corporation_id: int,
                 char_manager: CharacterManager
        ):
        self.cfgManager = cfgManager
        self.cfg = self.cfgManager.all()
        self.db_oauth = db_oauth
        self.db_app = db_app
        self.db_sde = db_sde

        self.corporation_id = corporation_id
        self.char_manager = char_manager

        # Runtime attributes
        self.corporation_name: Optional[str] = None
        self.ticker: Optional[str] = None
        self.description: Optional[str] = None
        self.member_count: Optional[int] = None
