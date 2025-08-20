import logging

from classes.config_manager import ConfigManager
from classes.database_manager import CharacterManager
from classes.esi import ESIClient


def main():
    logging.basicConfig(level=logging.INFO)

    # Config en DB laden
    cfg = ConfigManager("config/config.json")
    db = CharacterManager("database/eve_characters.db")

    # ---------------------------
    # Characters ophalen uit config
    # ---------------------------
    characters = cfg.get("characters", [])
    if not characters:
        logging.error("No characters defined in config!")
        return
    
    main_char_cfg = next((c for c in characters if c.get("is_main")), None)
    if not main_char_cfg:
        logging.warning("No main character defined, using first character in list.")
        main_char_cfg = characters[0]

    # ---------------------------
    # ESI clients initialiseren
    # ---------------------------
    esi_clients = []

    # Main character
    esi_main = ESIClient(main_char_cfg["character_name"], cfg, db, is_main=True)
    logging.info(f"Main character set: {esi_main.character_name} (ID: {esi_main.character_id})")
    esi_clients.append(esi_main)

    # Andere characters
    for c in characters:
        if c["character_name"] != esi_main.character_name:
            client = ESIClient(c["character_name"], cfg, db, is_main=False)
            esi_clients.append(client)
            logging.info(f"Alt character loaded: {client.character_name} (ID: {client.character_id})")


    # ---------------------------
    # Test ESI call
    # ---------------------------
    test_data = {
        c.character_name: c.esi_get(f"/characters/{c.character_id}/")
        for c in esi_clients
    }
    logging.info(f"Test ESI call results: {test_data}")


if __name__ == "__main__":
    main()
