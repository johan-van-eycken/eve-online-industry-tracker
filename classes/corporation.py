import logging
import json
import traceback
from datetime import datetime
from typing import Optional, List, Dict

from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.database_models import CorporationModel, StructureModel, MemberModel, CorporationAssetsModel
from classes.database_models import Types, Groups, Categories, Factions, Races
from classes.character import Character
from classes.character_manager import CharacterManager

class Corporation:
    """
    Ingame entity of a corporation.
    """
    def __init__(
        self,
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
        self.assets: List[CorporationAssetsModel] = []

        # Load all Corporation data on init (consider lazy loading for large corp lists)
        self.load_corporation()
        self.load_corporation_structures()
        self.load_corporation_members()
        self.load_corporation_assets()

    def save_corporation(self) -> None:
        """Save the current runtime properties of the corporation to the database."""
        corporation_record = self.db_app.session.query(CorporationModel).filter_by(corporation_id=self.corporation_id).first()
        if not corporation_record:
            corporation_record = CorporationModel(corporation_id=self.corporation_id)
            self.db_app.session.add(corporation_record)
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
        """Save the corporation structures to the database."""
        for structure in corporation_structures:
            existing_structure = self.db_app.session.query(StructureModel).filter_by(structure_id=structure.structure_id).first()
            if existing_structure:
                for column in StructureModel.__table__.columns.keys():
                    if column == "id":
                        continue
                    if hasattr(structure, column):
                        value = getattr(structure, column)
                        setattr(existing_structure, column, value)
                existing_structure.updated_at = datetime.utcnow()
            else:
                self.db_app.session.add(structure)
        self.db_app.session.commit()
        logging.debug(f"Corporation structures saved to database.")

    def save_corporation_members(self, corporation_members: List[MemberModel]) -> None:
        """Save the corporation members to the database."""
        for member in corporation_members:
            existing_member = self.db_app.session.query(MemberModel).filter_by(character_id=member.character_id).first()
            if existing_member:
                for column in MemberModel.__table__.columns.keys():
                    if column == "id":
                        continue
                    if hasattr(member, column):
                        value = getattr(member, column)
                        setattr(existing_member, column, value)
                existing_member.updated_at = datetime.utcnow()
            else:
                self.db_app.session.add(member)
        self.db_app.session.commit()
        logging.debug(f"Corporation members saved to database.")

    def save_corporation_assets(self, corporation_assets: List[Dict]) -> None:
        """Save the corporation assets to the database."""
        if not corporation_assets:
            logging.debug(f"No corporation assets to save for {self.corporation_name}.")
            return
        self.assets = []
        for asset in corporation_assets:
            new_asset = CorporationAssetsModel(**asset)
            self.assets.append(new_asset)
        if self.assets:
            self.db_app.session.query(CorporationAssetsModel).filter_by(corporation_id=self.corporation_id).delete()
            self.db_app.session.bulk_save_objects(self.assets)
            self.db_app.session.commit()
        else:
            logging.debug(f"No new corporation assets to save for {self.corporation_name}.")
        logging.debug(f"Corporation assets saved ({len(self.assets)}) for {self.corporation_name}.")

    def load_corporation(self) -> bool:
        """Load corporation data from the database into the instance. Returns True if found."""
        corporation_record = self.db_app.session.query(CorporationModel).filter_by(corporation_id=self.corporation_id).first()
        if not corporation_record:
            logging.debug(f"No database record found for corporation '{self.corporation_name}'.")
            return False
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

    def load_corporation_assets(self) -> bool:
        """Load corporation assets from the database into the instance. Returns True if found."""
        assets = self.db_app.session.query(CorporationAssetsModel).filter_by(corporation_id=self.corporation_id).all()
        if not assets:
            logging.debug(f"No assets found for corporation '{self.corporation_name}' in database.")
            return False
        self.assets = assets
        logging.debug(f"Loaded {len(self.assets)} assets for corporation '{self.corporation_name}' from database.")
        return True

    def refresh_all(self) -> str:
        """
        Refresh all data for the current corporation and return a JSON string.
        Returns:
            str: JSON string with all refreshed corporation data.
        """
        try:
            corp_data = json.loads(self.refresh_corporation())
            corp_members = json.loads(self.refresh_members())
            corp_structures = json.loads(self.refresh_structures())
            corp_assets = json.loads(self.refresh_assets())
            self.save_corporation()
            combined_data = {
                "corporation_name": self.corporation_name,
                **corp_data,
                **corp_members,
                **corp_structures,
                **corp_assets
            }
            combined_json = json.dumps(combined_data, indent=4)
            return combined_json
        except Exception as e:
            logging.error(f"Failed to refresh all corporation data for {self.corporation_name}. Error: {e}\n{traceback.format_exc()}")
            return json.dumps({'corporation_name': self.corporation_name, 'error': str(e)}, indent=4)

    def refresh_corporation(self, safe_corporation_fl: bool = True) -> str:
        """
        Refresh the runtime data of the corporation from the SDE.
        Returns:
            str: JSON string with refreshed corporation data.
        """
        try:
            logging.debug(f"Refreshing profile for {self.corporation_name}...")
            corp_data = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/")
            corp_divisions = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/divisions/")
            corp_wallets = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/wallets/")
            corp_standings = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/standings/")
            if isinstance(corp_wallets, str):
                corp_wallets = json.loads(corp_wallets)
            if isinstance(corp_divisions, str):
                corp_divisions = json.loads(corp_divisions)
            if isinstance(corp_standings, str):
                corp_standings = json.loads(corp_standings)
            faction_data = self.db_sde.load_df("factions")
            npccorp_data = self.db_sde.load_df("npcCorporations")
            def get_name(name, language):
                if isinstance(name, dict):
                    return name.get(language, next(iter(name.values()), "Unknown"))
                return name
            faction_lookup = {row["id"]: get_name(row["name"], self.cfg["app"]["language"]) for _, row in faction_data.iterrows()}
            npccorp_lookup = {row["id"]: get_name(row["name"], self.cfg["app"]["language"]) for _, row in npccorp_data.iterrows()}
            divisions_lookup = {
                d["division"]: (
                    "Master Wallet" if d["division"] == 1 else d.get("name", f"Division {d['division']}")
                )
                for d in corp_divisions.get("wallet", [])
            }
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
                if s.get("from_type") == "faction":
                    entry["name"] = faction_lookup.get(s.get("from_id"), "Unknown Faction")
                elif s.get("from_type") == "npc_corp":
                    entry["name"] = npccorp_lookup.get(s.get("from_id"), "Unknown Corporation")
                else:
                    entry["name"] = ""
                self.standings.append(entry)
            if safe_corporation_fl:
                self.save_corporation()
            logging.debug(f"Corporation data successfully updated for {self.corporation_name}.")
            return json.dumps({'corporation_name': self.corporation_name, 'corporation_data': corp_data}, indent=4)
        except Exception as e:
            logging.error(f"Failed to refresh corporation data for {self.corporation_name}. Error: {e}\n{traceback.format_exc()}")
            return json.dumps({'corporation_name': self.corporation_name, 'error': str(e)}, indent=4)

    def refresh_structures(self, safe_structures_fl: bool = True) -> str:
        """
        Refresh the structures of the corporation from the SDE.
        Returns:
            str: JSON string with refreshed structure names.
        """
        try:
            logging.debug(f"Refreshing structures for {self.corporation_name}...")
            structures_data = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/structures/")
            self.structures = []
            for structure in structures_data:
                system_data = self.default_esi_character.esi_client.esi_get(f"/universe/systems/{structure.get('system_id')}/")
                constellation_data = self.default_esi_character.esi_client.esi_get(f"/universe/constellations/{system_data.get('constellation_id')}/")
                region_data = self.default_esi_character.esi_client.esi_get(f"/universe/regions/{constellation_data.get('region_id')}/")
                type_data = self.db_sde.session.query(Types).filter_by(id=structure.get("type_id")).first()
                group_data = self.db_sde.session.query(Groups).filter_by(id=type_data.groupID).first()
                category_data = self.db_sde.session.query(Categories).filter_by(id=group_data.categoryID).first()
                self.structures.append(StructureModel(
                    corporation_id=structure.get("corporation_id"),
                    structure_id=structure.get("structure_id"),
                    structure_name=structure.get("name", "Unknown"),
                    system_id=structure.get("system_id"),
                    system_name=system_data.get("name", "Unknown"),
                    system_security=system_data.get("security_status"),
                    constellation_id=system_data.get("constellation_id"),
                    constellation_name=constellation_data.get("name", "Unknown"),
                    region_id=constellation_data.get("region_id"),
                    region_name=region_data.get("name", "Unknown"),
                    type_id=structure.get("type_id"),
                    type_name=type_data.name[self.db_sde.language],
                    type_description=type_data.description[self.db_sde.language],
                    group_id=type_data.groupID,
                    group_name=group_data.name[self.db_sde.language],
                    category_id=group_data.categoryID,
                    category_name=category_data.name[self.db_sde.language],
                    state=structure.get("state"),
                    state_timer_end=structure.get("state_timer_end"),
                    state_timer_start=structure.get("state_timer_start"),
                    unachors_at=structure.get("unanchors_at"),
                    fuel_expires=structure.get("fuel_expires"),
                    reinforce_hour=structure.get("reinforce_hour"),
                    next_reinforce_apply=structure.get("next_reinforce_apply"),
                    next_reinforce_hour=structure.get("next_reinforce_hour"),
                    acl_profile_id=structure.get("profile_id"),
                    services=structure.get("services", {})
                ))
            if safe_structures_fl:
                self.save_corporation_structures(self.structures)
            structure_list_summary = [s.structure_name for s in self.structures]
            logging.debug(f"Corporation structures successfully updated for {self.corporation_name}.")
            return json.dumps({'corporation_name': self.corporation_name, 'structures': structure_list_summary}, indent=4)
        except Exception as e:
            logging.error(f"Failed to refresh corporation structures for {self.corporation_name}. Error: {e}\n{traceback.format_exc()}")
            return json.dumps({'corporation_name': self.corporation_name, 'structures': {}, 'error': str(e)}, indent=4)

    def refresh_members(self, save_members_fl: bool = True) -> str:
        """
        Refresh the member list of the corporation from ESI.
        Returns:
            str: JSON string with refreshed member names.
        """
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
                member_role_entry = next((role for role in member_roles if role["character_id"] == character_id), None)
                if member_role_entry and "titles" in member_role_entry:
                    for title_id in member_role_entry["titles"]:
                        title = {
                            "title_id": title_id,
                            "title_name": next((corp_title["name"] for corp_title in corporation_titles if corp_title["title_id"] == title_id), "Unknown")
                        }
                        titles.append(title)
                self.members.append(MemberModel(
                    corporation_id=self.corporation_id,
                    character_id=character_id,
                    character_name=character.character_name,
                    titles=titles
                ))
            if save_members_fl:
                self.save_corporation_members(self.members)
            members_summary = [m.character_name for m in self.members]
            logging.debug(f"Corporation members successfully updated for {self.corporation_name}. Total members: {len(members_summary)}")
            return json.dumps({'corporation_name': self.corporation_name, 'members': members_summary}, indent=4)
        except Exception as e:
            logging.error(f"Failed to refresh corporation members for {self.corporation_name}. Error: {e}\n{traceback.format_exc()}")
            return json.dumps({'corporation_name': self.corporation_name, 'members': [], 'error': str(e)}, indent=4)

    def refresh_assets(self, save_assets_fl: bool = True) -> str:
        """
        Refresh the asset list of the corporation from ESI and enrich with SDE and container custom names.
        Returns:
            str: JSON string with refreshed asset list.
        """
        try:
            logging.debug(f"Refreshing assets for {self.corporation_name}...")
            assets = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/assets/")
            blueprints = self.default_esi_character.esi_client.esi_get(f"/corporations/{self.corporation_id}/blueprints/", paginate=True)
            market_prices = self.default_esi_character.esi_client.esi_get(f"/markets/prices/")

            asset_list = []
            type_ids = set(asset.get("type_id") for asset in assets)
            type_data_map = {t.id: t for t in self.db_sde.session.query(Types).filter(Types.id.in_(type_ids)).all()}
            group_ids = set(t.groupID for t in type_data_map.values())
            group_data_map = {g.id: g for g in self.db_sde.session.query(Groups).filter(Groups.id.in_(group_ids)).all()}
            category_ids = set(g.categoryID for g in group_data_map.values())
            category_data_map = {c.id: c for c in self.db_sde.session.query(Categories).filter(Categories.id.in_(category_ids)).all()}
            race_ids = set(t.raceID for t in type_data_map.values() if hasattr(t, "raceID") and t.raceID is not None)
            race_data_map = {r.id: r for r in self.db_sde.session.query(Races).filter(Races.id.in_(race_ids)).all()}
            faction_ids = set(t.factionID for t in type_data_map.values() if hasattr(t, "factionID") and t.factionID is not None)
            faction_data_map = {f.id: f for f in self.db_sde.session.query(Factions).filter(Factions.id.in_(faction_ids)).all()}
            blueprint_data_map = {bp["item_id"]: bp for bp in blueprints}
            for asset in assets:
                type_id = asset.get("type_id")
                type_data = type_data_map.get(type_id)
                type_adjusted_price = next((item.get("adjusted_price", 0.0) for item in market_prices if item.get("type_id") == type_id), 0.0)
                type_average_price = next((item.get("average_price", 0.0) for item in market_prices if item.get("type_id") == type_id), 0.0)
                group_data = group_data_map.get(type_data.groupID) if type_data else None
                category_data = category_data_map.get(group_data.categoryID) if group_data else None
                race_data = race_data_map.get(type_data.raceID) if type_data and hasattr(type_data, "raceID") else None
                faction_data = faction_data_map.get(type_data.factionID) if type_data and hasattr(type_data, "factionID") else None
                blueprint_data = blueprint_data_map.get(asset.get("item_id"))

                # --- Calculate actual volume ---
                sde_volume = getattr(type_data, "volume", 0.0) if type_data else 0.0
                repackaged_volume = None

                # Check type repackaged_volume
                if type_data and hasattr(type_data, "repackaged_volume") and type_data.repackaged_volume:
                    repackaged_volume = type_data.repackaged_volume
                # If not, check group repackaged_volume
                elif group_data and hasattr(group_data, "repackaged_volume") and group_data.repackaged_volume:
                    repackaged_volume = group_data.repackaged_volume

                # Use repackaged_volume if repackaged, else normal volume
                if asset.get("is_singleton", False) == False and repackaged_volume is not None:
                    actual_volume = repackaged_volume
                else:
                    actual_volume = getattr(type_data, "volume", 0.0) if type_data else 0.0

                asset_entry = {
                    "corporation_id": self.corporation_id,
                    "item_id": asset.get("item_id"),
                    "type_id": type_id,
                    "type_name": getattr(type_data, "name", {}).get(self.db_sde.language, "") if type_data else "",
                    "type_default_volume": sde_volume,
                    "type_repackaged_volume": repackaged_volume,
                    "type_volume": actual_volume,
                    "type_capacity": getattr(type_data, "capacity", None) if type_data else None,
                    "type_description": getattr(type_data, "description", {}).get(self.db_sde.language, "") if type_data and getattr(type_data, "description", None) else "",
                    "container_name": None,
                    "ship_name": None,
                    "type_group_id": getattr(type_data, "groupID", None) if type_data else None,
                    "type_group_name": getattr(group_data, "name", {}).get(self.db_sde.language, "") if group_data else "",
                    "type_category_id": getattr(group_data, "categoryID", None) if group_data else None,
                    "type_category_name": getattr(category_data, "name", {}).get(self.db_sde.language, "") if category_data else "",
                    "type_meta_group_id": getattr(type_data, "metaGroupID", None) if type_data else None,
                    "type_race_id": getattr(type_data, "raceID", None) if type_data else None,
                    "type_race_name": getattr(race_data, "name", {}).get(self.db_sde.language, "") if race_data and getattr(race_data, "name", None) else "",
                    "type_race_description": getattr(race_data, "description", {}).get(self.db_sde.language, "") if race_data and getattr(race_data, "description", None) else "",
                    "type_faction_id": getattr(type_data, "factionID", None) if type_data else None,
                    "type_faction_name": getattr(faction_data, "name", {}).get(self.db_sde.language, "") if faction_data and getattr(faction_data, "name", None) else "",
                    "type_faction_description": getattr(faction_data, "description", {}).get(self.db_sde.language, "") if faction_data and getattr(faction_data, "description", None) else "",
                    "type_faction_short_description": getattr(faction_data, "shortDescriptionID", {}).get(self.db_sde.language, "") if faction_data and getattr(faction_data, "shortDescriptionID", None) else "",
                    "location_id": asset.get("location_id"),
                    "location_type": asset.get("location_type"),
                    "location_flag": asset.get("location_flag"),
                    "is_singleton": asset.get("is_singleton"),
                    "is_blueprint_copy": asset.get("is_blueprint_copy", False),
                    "blueprint_runs": blueprint_data.get("runs") if blueprint_data else None,
                    "blueprint_time_efficiency": blueprint_data.get("time_efficiency") if blueprint_data else None,
                    "blueprint_material_efficiency": blueprint_data.get("material_efficiency") if blueprint_data else None,
                    "quantity": asset.get("quantity", 0),
                    "type_adjusted_price": type_adjusted_price,
                    "type_average_price": type_average_price
                }
                asset_list.append(asset_entry)
            
            # Fetch custom names for containers (type_id == 17366 and is_singleton == True)
            container_ids = [a["item_id"] for a in asset_list if a.get("type_id") == 17366 and a.get("is_singleton") == True]
            container_names = {}
            if container_ids:
                names_response = self.default_esi_character.esi_client.esi_post(
                    f"/corporations/{self.corporation_id}/assets/names",
                    json=container_ids
                )
                if names_response and isinstance(names_response, list):
                    container_names = {c["item_id"]: c["name"] for c in names_response}
            
            for asset in asset_list:
                if asset.get("item_id") in container_names:
                    asset["container_name"] = container_names[asset["item_id"]]
            
            # Fetch custom names for ships (type_category_id == 6 and is_singleton == True)
            ship_ids = [a["item_id"] for a in asset_list if a.get("type_category_id") == 6 and a.get("is_singleton") == True]
            ship_names = {}
            if ship_ids:
                names_response = self.default_esi_character.esi_client.esi_post(
                    f"/corporations/{self.corporation_id}/assets/names",
                    json=ship_ids
                )
                if names_response and isinstance(names_response, list):
                    ship_names = {s["item_id"]: s["name"] for s in names_response}
            
            for asset in asset_list:
                if asset.get("item_id") in ship_names:
                    asset["ship_name"] = ship_names[asset["item_id"]]
            
            # Lookup for all containers and ships by item_id
            container_lookup = {a["item_id"]: a for a in asset_list if a.get("type_id") == 17366 and a.get("is_singleton")}
            ship_lookup = {a["item_id"]: a for a in asset_list if a.get("type_category_id") == 6 and a.get("is_singleton")}

            for asset in asset_list:
                # Assign container_name for non-containers
                if not (asset.get("type_id") == 17366 and asset.get("is_singleton")):
                    container = container_lookup.get(asset.get("location_id"))
                    if container:
                        asset["container_name"] = container.get("container_name")
                # Assign ship_name for non-ships (modules, etc.)
                if not (asset.get("type_category_id") == 6 and asset.get("is_singleton")):
                    ship = ship_lookup.get(asset.get("location_id"))
                    if ship:
                        asset["ship_name"] = ship.get("ship_name")

            if save_assets_fl:
                self.save_corporation_assets(asset_list)
            
            logging.debug(f"Corporation assets successfully updated for {self.corporation_name}. Total assets: {len(asset_list)}")
            return json.dumps({'corporation_name': self.corporation_name, 'assets': asset_list}, indent=4)
        except Exception as e:
            logging.error(f"Failed to refresh corporation assets for {self.corporation_name}. Error: {e}\n{traceback.format_exc()}")
            return json.dumps({'corporation_name': self.corporation_name, 'assets': [], 'error': str(e)}, indent=4)