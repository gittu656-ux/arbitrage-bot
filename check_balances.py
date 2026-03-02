import asyncio
import os
import sys
import httpx
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def check_balances():
    load_dotenv()
    
    pm_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    cb_key = os.getenv("CLOUDBET_API_KEY")
    cb_proxy = os.getenv("CLOUDBET_PROXY")
    
    print("--- Searching Cloudbet Balance Endpoint ---")
    # We know /pub/v3/bets/place works, so let's try paths near there
    base = "https://sports-api.cloudbet.com/pub"
    endpoints = [
        "/v3/accounts/balance",
        "/v2/accounts/balance",
        "/v1/accounts/balance",
        "/v3/account/balance",
        "/v1/account/balance",
        "/v1/profile",
        "/v2/profile"
    ]
    
    async with httpx.AsyncClient(headers={"X-API-Key": cb_key}) as client:
        for ep in endpoints:
            try:
                resp = await client.get(f"{base}{ep}")
                if resp.status_code == 200:
                    print(f"SUCCESS {ep}: {resp.json()}")
                    break
                else:
                    print(f"Failed {ep}: {resp.status_code}")
            except Exception:
                pass

    print("\n--- Checking Polymarket Balances ---")
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.constants import POLYGON
        
        client = ClobClient(host="https://clob.polymarket.com", key=pm_key, chain_id=POLYGON)
        
        # Try to get balance/allowance with possible params
        params_to_try = [
            {},
            {"asset_id": "USDC"},
            {"funder": client.get_address()}
        ]
        
        for p in params_to_try:
            try:
                print(f"Trying get_balance_allowance with {p}")
                ba = client.get_balance_allowance(**p)
                print(f"Result: {ba}")
                break
            except Exception as e:
                print(f"Error: {e}")
            
    except Exception as e:
        print(f"Polymarket error: {e}")

if __name__ == "__main__":
    asyncio.run(check_balances())
