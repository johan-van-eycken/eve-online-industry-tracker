import logging
from esi_api import ESIClient
from database import save_df
import pandas as pd

def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        esi = ESIClient()
        char_info = esi.login()
        char_id = esi.character_id

        assets = esi.esi_get(f"/characters/{char_id}/assets/")
        assets_df = pd.DataFrame(assets)
        save_df(assets_df, "assets")

        wallet = esi.esi_get(f"/characters/{char_id}/wallet/transactions/")
        wallet_df = pd.DataFrame(wallet)
        save_df(wallet_df, "wallet_transactions")

        logging.info("Data saved to SQLite.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
