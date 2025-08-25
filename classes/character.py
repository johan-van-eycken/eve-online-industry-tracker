import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any

from classes.config_manager import ConfigManagerSingleton
from classes.database_manager import DatabaseManager
from classes.esi import ESIClient

class Character:
    """Class representing a character in EVE Online."""

    def __init__(self, character_name: str, is_main: bool):
        self.cfg = ConfigManagerSingleton()
        self.db_eve = DatabaseManager(self.cfg.get("app").get("db_app"))
        self.db_sde = DatabaseManager(self.cfg.get("app").get("db_sde"))

        self.character_name = character_name
        self.is_main = is_main
        self.esi = ESIClient(character_name, is_main)
        
        # Runtime properties
        self.character_id: Optional[int] = None
        self.image_url: Optional[str] = None
        self.birthday: Optional[str] = None
        self.bloodline_id: Optional[int] = None
        self.bloodline: Optional[str] = None
        self.race_id: Optional[int] = None
        self.race: Optional[str] = None
        self.gender: Optional[str] = None
        self.corporation_id: Optional[int] = None
        self.description: Optional[str] = None
        self.security_status: Optional[float] = None
        
        # Wallet balance
        self.wallet_balance: Optional[float] = None
        
    # Refresh methods
    def refresh_profile(self) -> Dict[str, Any]:
        """Fetch and update character profile data from ESI."""
        df_sde_races = self.db_sde.load_df("races")
        df_sde_bloodlines = self.db_sde.load_df("bloodlines")

        data = self.esi.esi_get(f"/characters/{self.character_id}/")
        self.character_id = data.get("character_id")
        self.image_url = f"https://images.evetech.net/characters/{self.character_id}/portrait?size=128"
        self.birthday = data.get("birthday")
        self.bloodLine_id = data.get("bloodline_id")
        self.bloodline = df_sde_bloodlines.set_index('id')['nameID'].to_dict()
        self.race_id = data.get("race_id")
        self.race = df_sde_races.set_index('id')['nameID'].to_dict()
        self.gender = data.get("gender")
        self.corporation_id = data.get("corporation_id")
        self.description = data.get("description")
        self.security_status = data.get("security_status")

        df = pd.DataFrame([{
            "character_id": [self.character_id],
            "character_name": [self.character_name],
            "birthday": [self.birthday],
            "bloodline_id": [self.bloodLine_id],
            "bloodline": [self.bloodline],
            "race_id": [self.race_id],
            "race": [self.race],
            "gender": [self.gender],
            "corporation_id": [self.corporation_id],
            "description": [self.description],
            "security_status": [self.security_status],
            "is_main": [self.is_main],
            "updated_at": [datetime.utcnow().isoformat()]
        }])
        self.db_eve.upsert_df(df, "characters", key_columns=["character_id"])
        
        return data
    
    def refresh_wallet(self) -> float:
        """Fetch and store wallet balance."""
        balance = self.esi.esi_get(f"/characters/{self.character_id}/wallet/")
        self.wallet_balance = balance

        df = pd.DataFrame([{
            "character_id": self.character_id,
            "balance": self.wallet_balance,
            "updated_at": [datetime.utcnow().isoformat()]
        }])
        self.db_eve.upsert_df(df, "characters", key_columns=["character_id"])

        return balance