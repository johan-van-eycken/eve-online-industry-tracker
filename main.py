from esi_api import get_access_token, esi_get
from database import save_df
import pandas as pd

def main():
    token = get_access_token()
    
    # Example: character ID from /verify endpoint
    verify = esi_get("/verify/", token)
    char_id = verify["CharacterID"]
    print(f"Logged in as {verify['CharacterName']}")

    # Get assets
    assets = esi_get(f"/characters/{char_id}/assets/", token)
    assets_df = pd.DataFrame(assets)
    save_df(assets_df, "assets")

    # Get wallet transactions
    wallet = esi_get(f"/characters/{char_id}/wallet/transactions/", token)
    wallet_df = pd.DataFrame(wallet)
    save_df(wallet_df, "wallet_transactions")

    print("Data saved to SQLite.")

if __name__ == "__main__":
    main()
