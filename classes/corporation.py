import logging
import json
from datetime import datetime
from typing import Optional, List, Dict

from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.database_models import CorporationModel, StructureModel, MemberModel
from classes.database_models import Factions, NpcCorporations
from classes.database_models import Types, Groups, Categories
from classes.character import Character
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

        # Default ESI character
        self.default_esi_character: Character = self.char_manager.get_corp_director()
        if not self.default_esi_character:
            logging.warning("No CEO character found. Defaulting to main character.")
            self.default_esi_character = self.char_manager.get_main_character()
            if not self.default_esi_character:
                logging.warning("No main character found. Defaulting to first character in list.")
                self.default_esi_character = self.char_manager.character_list[0]
        
        if not self.default_esi_character:
            raise ValueError("No valid character available for corporation operations.")

        # Runtime attributes
        self.corporation_name: Optional[str] = None
        self.creator_id: Optional[int] = None
        self.ceo_id: Optional[int] = None
        self.date_founded: Optional[datetime] = None
        self.description: Optional[str] = None
        self.home_station_id: Optional[int] = None
        self.member_count: Optional[int] = None
        self.shares: Optional[int] = None
        self.tax_rate: Optional[float] = None
        self.ticker: Optional[str] = None
        self.url: Optional[str] = None
        self.war_eligible: Optional[bool] = None
        self.image_url: Optional[str] = None
        self.wallets: List[Dict[str, str]] = []
        self.standings: List[Dict[str, str]] = []

        self.structures: List[StructureModel] = []
        self.members: List[MemberModel] = []

        # Refresh all Corporation data on init
        self.refresh_corporation_data()
        self.refresh_structures()
        self.refresh_members()

    # -------------------
    # Safe Corporation
    # -------------------
    def save_corporation(self) -> None:
        """Save the current runtime properties of the corporation to the database."""

        corporation_record = (self.db_app.session.query(CorporationModel).filter_by(corporation_id=self.corporation_id).first())

        if not corporation_record:
            # Create new record if it doesn't exist
            corporation_record = CorporationModel(
                corporation_id=self.corporation_id
            )
            self.db_app.session.add(corporation_record)

        # Dynamically update based on CorporationModel's columns
        for column in CorporationModel.__table__.columns.keys():
            if column == "id":
                continue
            if hasattr(self, column):
                value = getattr(self, column)
                setattr(corporation_record, column, value)

        if hasattr(self, "wallets"):
            corporation_record.wallets = json.dumps(self.wallets)
        if hasattr(self, "standings"):
            corporation_record.standings = json.dumps(self.standings)

        corporation_record.updated_at = datetime.utcnow()

        self.db_app.session.commit()
        logging.debug(f"Corporation '{self.corporation_name}' saved to database.")

    def save_corporation_structures(self, corporation_structures: List[StructureModel]) -> None:
        """Safe the corporation structures to the database."""
        for structure in corporation_structures:
            existing_structure = self.db_app.session.query(StructureModel).filter_by(structure_id=structure.structure_id).first()

            if existing_structure:
                # Update existing structure
                for column in StructureModel.__table__.columns.keys():
                    if column == "id":
                        continue
                    if hasattr(structure, column):
                        value = getattr(structure, column)
                        setattr(existing_structure, column, value)
                
                existing_structure.updated_at = datetime.utcnow()
            else:
                # Add new structure
                self.db_app.session.add(structure)

        self.db_app.session.commit()
        logging.debug(f"Corporation structures saved to database.")

    def save_corporation_members(self, corporation_members: List[MemberModel]) -> None:
        """Safe the corporation members to the database."""
        for member in corporation_members:
            existing_member = self.db_app.session.query(MemberModel).filter_by(character_id=member.character_id).first()

            if existing_member:
                # Update existing member
                for column in MemberModel.__table__.columns.keys():
                    if column == "id":
                        continue
                    if hasattr(member, column):
                        value = getattr(member, column)
                        setattr(existing_member, column, value)
                
                existing_member.updated_at = datetime.utcnow()
            else:
                # Add new member
                self.db_app.session.add(member)
        
        self.db_app.session.commit()
        logging.debug(f"Corporation members saved to database.")

    # -------------------
    # Load Corporation
    # -------------------
    def load_corporation(self) -> bool:
        """Load corporation data from the database into the instance. Returns True if found."""

        corporation_record = (self.db_app.session.query(CorporationModel).filter_by(corporation_id=self.corporation_id).first())

        if not corporation_record:
            logging.debug(f"No database record found for corporation '{self.corporation_name}'.")
            return False

        # Dynamically use attributes from CorporationModel
        for column in CorporationModel.__table__.columns.keys():
            if hasattr(self, column):
                setattr(self, column, getattr(corporation_record, column))

        logging.debug(f"Corporation '{self.corporation_name}' loaded from database.")
        return True
    
    def load_corporation_structures(self) -> bool:
        """Load corporation structures from the database into the instance. Returns True if found."""
        
        structures = self.db_app.session.query(StructureModel).filter_by(corporation_id=self.corporation_id).all()
        if not structures:
            logging.debug(f"No structures found for corporation '{self.corporation_name}' in database.")
            return False
        
        self.structures = structures
        
        logging.debug(f"Loaded {len(self.structures)} structures for corporation '{self.corporation_name}' from database.")
        return True
    
    def load_corporation_members(self) -> bool:
        """Load corporation members from the database into the instance. Returns True if found."""
        
        members = self.db_app.session.query(MemberModel).filter_by(corporation_id=self.corporation_id).all()
        if not members:
            logging.debug(f"No members found for corporation '{self.corporation_name}' in database.")
            return False
        
        self.members = members
        
        logging.debug(f"Loaded {len(self.members)} members for corporation '{self.corporation_name}' from database.")
        return True
    
    # -------------------
    # Refresh Corporation runtime data
    # -------------------
    def refresh_corporation_data(self, safe_corporation_fl: bool = True) -> str:
        """Refresh the runtime data of the corporation from the SDE."""
        try:
            logging.debug(f"Refreshing profile for {self.corporation_name}...")
            corp_data = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/")
            corp_divisions = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/divisions/")
            corp_wallets = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/wallets/")
            corp_standings = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/standings/")

            # Convert String responses to JSON if necessary
            if isinstance(corp_wallets, str):
                corp_wallets = json.loads(corp_wallets)
            if isinstance(corp_divisions, str):
                corp_divisions = json.loads(corp_divisions)
            if isinstance(corp_standings, str):
                corp_standings = json.loads(corp_standings)

            # Load additional details from the SDE database
            faction_data = self.db_sde.load_df("factions")
            npccorp_data = self.db_sde.load_df("npcCorporations")

            # Lookup tables
            def get_name(nameID, language):
                if isinstance(nameID, dict):
                    return nameID.get(language, next(iter(nameID.values()), "Unknown"))
                return nameID

            faction_lookup = {row['id']: get_name(row['nameID'], self.cfg["app"]["language"]) for _, row in faction_data.iterrows()}
            npccorp_lookup = {row['id']: get_name(row['nameID'], self.cfg["app"]["language"]) for _, row in npccorp_data.iterrows()}
            divisions_lookup = {
                d["division"]: (
                    "Master Wallet" if d["division"] == 1 else d.get("name", f"Division {d['division']}")
                )
                for d in corp_divisions.get("wallet", [])
            }


            # Update runtime properties
            logging.debug
            self.image_url = f"https://images.evetech.net/corporations/{self.corporation_id}/portrait?size=128"
            self.corporation_name = corp_data.get("name")
            self.creator_id = corp_data.get("creator_id")
            self.ceo_id = corp_data.get("ceo_id")
            self.date_founded = corp_data.get("date_founded")
            self.description = corp_data.get("description")
            self.home_station_id = corp_data.get("home_station_id")
            self.member_count = corp_data.get("member_count")
            self.shares = corp_data.get("shares")
            self.tax_rate = corp_data.get("tax_rate")
            self.ticker = corp_data.get("ticker")
            self.url = corp_data.get("url")
            self.war_eligible = corp_data.get("war_eligible")
            self.wallets = [
                {
                    "division": str(w.get("division")),
                    "division_name": divisions_lookup.get(w.get("division"), f"Division {w.get('division')}"),
                    "balance": str(w.get("balance"))
                }
                for w in corp_wallets
            ]
            self.standings = []
            for s in corp_standings:
                entry = {
                    "from_id": str(s.get("from_id")),
                    "from_type": s.get("from_type"),
                    "standing": str(s.get("standing"))
                }
                # Add name if available
                if s.get("from_type") == "faction":
                    entry["name"] = faction_lookup.get(s.get("from_id"), "Unknown Faction")
                elif s.get("from_type") == "npc_corp":
                    entry["name"] = npccorp_lookup.get(s.get("from_id"), "Unknown Corporation")
                else:
                    entry["name"] = ""
                self.standings.append(entry)

            # Save to database
            if safe_corporation_fl == True:
                self.save_corporation()

            logging.debug(f"Corporation data successfully updated for {self.corporation_name}.")
            return json.dumps({'corporation_name': self.corporation_name, 'corporation_data': corp_data}, indent=4)

        except Exception as e:
            logging.error(f"Failed to refresh corporation data for {self.corporation_name}. Error: {e}")
            return json.dumps({'corporation_name': self.corporation_name, 'error': str(e)}, indent=4)
        
    # -------------------
    # Refresh Corporation structures
    # -------------------
    def refresh_structures(self, safe_structures_fl: bool = True) -> str:
        """Refresh the structures of the corporation from the SDE."""
        try:
            logging.debug(f"Refreshing structures for {self.corporation_name}...")
            structures_data = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/structures/")
            
            self.structures = []
            for structure in structures_data:
                # Load additional details from ESI
                system_data = self.default_esi_character.esi_client.esi_get(f"/universe/systems/{structure.get('system_id')}/")
                constellation_data = self.default_esi_character.esi_client.esi_get(f"/universe/constellations/{system_data.get('constellation_id')}/")
                region_data = self.default_esi_character.esi_client.esi_get(f"/universe/regions/{constellation_data.get('region_id')}/")

                # Load additional details from the SDE database
                type_data = self.db_sde.session.query(Types).filter_by(id=structure.get("type_id")).first()
                group_data = self.db_sde.session.query(Groups).filter_by(id=type_data.groupID).first()
                category_data = self.db_sde.session.query(Categories).filter_by(id=group_data.categoryID).first()

                self.structures.append(StructureModel(
                    corporation_id = structure.get("corporation_id"),
                    structure_id = structure.get("structure_id"),
                    structure_name = structure.get("name", "Unknown"),
                    system_id = structure.get("system_id"),
                    system_name = system_data.get("name", "Unkown"),
                    system_security = system_data.get("security_status"),
                    constellation_id = system_data.get("constellation_id"),
                    constellation_name = constellation_data.get("name", "Unknown"),
                    region_id = constellation_data.get("region_id"),
                    region_name = region_data.get("name", "Unknown"),
                    type_id = structure.get("type_id"),
                    type_name = type_data.name[self.db_sde.language],
                    type_description = type_data.description[self.db_sde.language],
                    group_id = type_data.groupID,
                    group_name = group_data.name[self.db_sde.language],
                    category_id = group_data.categoryID,
                    category_name = category_data.name[self.db_sde.language],
                    state = structure.get("state"),
                    state_timer_end = structure.get("state_timer_end"),
                    state_timer_start = structure.get("state_timer_start"),
                    unachors_at = structure.get("unanchors_at"),
                    fuel_expires = structure.get("fuel_expires"),
                    reinforce_hour = structure.get("reinforce_hour"),
                    next_reinforce_apply = structure.get("next_reinforce_apply"),
                    next_reinforce_hour = structure.get("next_reinforce_hour"),
                    acl_profile_id = structure.get("profile_id"),
                    services = structure.get("services", {})
                ))

            if safe_structures_fl == True:
                self.save_corporation_structures(self.structures)

            structure_list_summary = [s.structure_name for s in self.structures]
            logging.debug(f"Corporation structures successfully updated for {self.corporation_name}.")
            return json.dumps({'corporation_name': self.corporation_name, 'structures': structure_list_summary}, indent=4)
        except Exception as e:
            logging.error(f"Failed to refresh corporation structures for {self.corporation_name}. Error: {e}")
            return json.dumps({'corporation_name': self.corporation_name, 'structures': {}}, indent=4)

    # -------------------
    # Refresh Corporation members and member roles
    # -------------------
    def refresh_members(self, save_members_fl: bool = True) -> str:
        """Refresh the member list of the corporation from ESI."""
        try:
            logging.debug(f"Refreshing members for {self.corporation_name}...")
            members = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/members/")
            member_roles = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/members/titles/")
            corporation_titles = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/titles/")

            self.members = []    
            for character_id in members:
                character = self.char_manager.get_character_by_id(character_id)
                if not character:
                    logging.debug(f"Character ID {character_id} not found in character manager. Skipping.")
                    continue
                
                titles: List[Dict[str, str]] = []

                # Zoek de juiste entry in member_roles gebaseerd op character_id
                member_role_entry = next(
                    (role for role in member_roles if role["character_id"] == character_id), 
                    None
                )

                if member_role_entry and "titles" in member_role_entry:
                    for title_id in member_role_entry["titles"]:
                        title = {
                            "title_id": title_id,
                            "title_name": next(
                                (corp_title["name"] for corp_title in corporation_titles if corp_title["title_id"] == title_id),
                                "Unknown"
                            )
                        }
                        titles.append(title)

                self.members.append(MemberModel(
                    corporation_id = self.corporation_id,
                    character_id = character_id,
                    character_name = character.character_name,
                    titles = titles
                ))
            
            if save_members_fl == True:
                self.save_corporation_members(self.members)

            members_summary = [m.character_name for m in self.members]
            logging.debug(f"Corporation members successfully updated for {self.corporation_name}. Total members: {len(members_summary)}")
            return json.dumps({'corporation_name': self.corporation_name, 'members': members_summary}, indent=4)

        except Exception as e:
            logging.error(f"Failed to refresh corporation members for {self.corporation_name}. Error: {e}")
            return json.dumps({'corporation_name': self.corporation_name, 'members': []}, indent=4)