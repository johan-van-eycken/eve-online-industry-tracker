import logging
import pandas as pd

from classes.config_manager import ConfigManagerSingleton
from classes.database_manager import DatabaseManager, CharacterManager
from classes.esi import ESIClient


def main():
    logging.basicConfig(level=logging.INFO)

    # ---------------------------
    # Configuraties laden
    # ---------------------------
    try:
        cfg = ConfigManagerSingleton()
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        return
    
    # ---------------------------
    # Characters ophalen uit config
    # ---------------------------
    characters = cfg.get("characters")
    main_char_cfg = next((c for c in characters if c.get("is_main")), None)
    if not main_char_cfg:
        logging.warning("No main character defined, using first character in list.")
        main_char_cfg = characters[0]

    # ---------------------------
    # ESI clients initialiseren
    # ---------------------------
    db_characters = CharacterManager(cfg.get("app").get("db_characters"))

    # Main character
    esi_main = ESIClient(main_char_cfg["character_name"], db_characters, is_main=True)
    logging.info(f"Main character set: {esi_main.character_name} (ID: {esi_main.character_id})")
    esi_clients.append(esi_main)

    # Andere characters
    for c in characters:
        if c["character_name"] != esi_main.character_name:
            client = ESIClient(c["character_name"], is_main=False)
            esi_clients.append(client)
            logging.info(f"Alt character loaded: {client.character_name} (ID: {client.character_id})")

    # ---------------------------
    # Character data
    # ---------------------------

    df_sde_races = db_sde.load_df("races")
    df_sde_bloodlines = db_sde.load_df("bloodlines")

    all_data = []
    for c in esi_clients:
        data = c.esi_get(f"/characters/{c.character_id}/")
        data["character_id"] = c.character_id
        data["image_url"] = f"https://images.evetech.net/characters/{c.character_id}/portrait?size=128"
        data["wallet_balance"] = c.esi_get(f"/characters/{c.character_id}/wallet/")
        # Lookup eve_sde names
        race_name_lookup = df_sde_races.set_index('id')['nameID'].to_dict()
        bloodline_name_lookup = df_sde_bloodlines.set_index('id')['nameID'].to_dict()
        data["race"] = race_name_lookup.get(data['race_id'], "unknown")
        data["bloodline"] = bloodline_name_lookup.get(data['bloodline_id'], "unknown")

        all_data.append(data)

    df = pd.DataFrame(all_data)
    db_eve.save_df(df, "characters") 

    logging.info(f"Character data saved.")

if __name__ == "__main__":
    main()
