import asyncio
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def check_poly_allowance():
    load_dotenv()
    pm_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    
    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON
    
    client = ClobClient(host="https://clob.polymarket.com", key=pm_key, chain_id=POLYGON)
    
    # Deriving API keys
    try:
        creds = client.derive_api_key()
        client.set_api_creds(creds)
        print("API Credentials set successfully.")
    except Exception as e:
        print(f"Failed to derive API keys: {e}")
        return

    address = client.get_address()
    print(f"Address: {address}")
    
    try:
        print("Checking Balance/Allowance...")
        try:
            res = client.get_balance_allowance()
            print(f"Balance/Allowance Result: {res}")
        except Exception as e:
            print(f"Call Failed: {e}")
            
        print("\nAttempting to Update Allowance to max...")
        try:
            approve_res = client.update_balance_allowance()
            print(f"Approval Transaction Result: {approve_res}")
        except Exception as e:
            print(f"Approval Failed (likely missing MATIC for gas): {e}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(check_poly_allowance())
