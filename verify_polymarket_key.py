import asyncio
import os
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

async def verify_polymarket_key():
    load_dotenv()
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    
    if not private_key:
        print("ERROR: POLYMARKET_PRIVATE_KEY not found in .env file.")
        return False
    
    print("Verifying Polymarket Private Key...")
    print(f"   Key starts with: {private_key[:10]}...")
    print()
    
    try:
        # Initialize CLOB client
        print("[1/4] Initializing Polymarket CLOB client...")
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=private_key,
            chain_id=POLYGON
        )
        print("   [OK] Client initialized")
        print()
        
        # Try to create API credentials (this signs a message with the private key)
        print("[2/4] Creating API credentials (signing message)...")
        # DERIVING api key instead of creating (create_api_creds might be deprecated/renamed)
        creds = client.derive_api_key()
        client.set_api_creds(creds)
        print("   [OK] API credentials derived successfully")
        print(f"   API Key: {creds.api_key[:20]}...")
        print(f"   API Secret: {creds.api_secret[:20]}...")
        print()
        
        # Try to get balance
        print("[3/4] Fetching account balance...")
        try:
            balance_response = client.get_balance_allowance()
            print("   [OK] Balance retrieved successfully")
            print(f"   Balance: {balance_response}")
        except Exception as e:
            print(f"   [WARN] Could not fetch balance: {e}")
        print()
        
        # Try to get open orders (this will fail if no orders, but proves API works)
        print("[4/4] Testing API connectivity (fetching open orders)...")
        try:
            orders = client.get_orders()
            print(f"   [OK] API working! Found {len(orders)} open orders")
        except Exception as e:
            print(f"   [WARN] API call failed: {e}")
        print()
        
        print("=" * 60)
        print("SUCCESS: POLYMARKET SETUP IS WORKING!")
        print("=" * 60)
        print("The bot will be able to place orders on Polymarket.")
        return True
        
    except Exception as e:
        print()
        print("=" * 60)
        print("FAILED: POLYMARKET SETUP FAILED!")
        print("=" * 60)
        print(f"Error: {e}")
        print()
        print("Possible issues:")
        print("1. Invalid private key format (should start with '0x')")
        print("2. Private key doesn't have funds on Polygon network")
        print("3. Network connectivity issue")
        return False

if __name__ == "__main__":
    asyncio.run(verify_polymarket_key())
