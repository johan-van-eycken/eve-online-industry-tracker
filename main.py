import logging
from esi_api import get_access_token, esi_get, verify_token
from database import save_df
import pandas as pd

def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        token = get_access_token()
        verify = verify_token(token)
        char_id = verify["CharacterID"]
        logging.info(f"Logged in as {verify['CharacterName']}")

        # Get assets
        assets = esi_get(f"/characters/{char_id}/assets/", token)
        assets_df = pd.DataFrame(assets)
        save_df(assets_df, "assets")

        # Get wallet transactions
        wallet = esi_get(f"/characters/{char_id}/wallet/transactions/", token)
        wallet_df = pd.DataFrame(wallet)
        save_df(wallet_df, "wallet_transactions")

        logging.info("Data saved to SQLite.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
