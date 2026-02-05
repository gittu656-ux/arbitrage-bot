import asyncio
import httpx
import os
from dotenv import load_dotenv

async def test_cloudbet_trading_access():
    load_dotenv()
    api_key = os.getenv("CLOUDBET_API_KEY")
    
    headers = {
        "X-API-Key": api_key,
        "Accept": "application/json"
    }
    
    # Test 1: Check available currencies/wallets
    print("=" * 60)
    print("Test 1: Checking Cloudbet Account Access...")
    print("=" * 60)
    
    async with httpx.AsyncClient(timeout=10) as client:
        # Try to get account info (if available in v3)
        url = "https://sports-api.cloudbet.com/pub/v3/account"
        try:
            resp = await client.get(url, headers=headers)
            print(f"Account API Status: {resp.status_code}")
            if resp.status_code == 200:
                print(f"Response: {resp.text[:500]}")
            else:
                print(f"Error: {resp.text[:500]}")
        except Exception as e:
            print(f"Account API not available: {e}")
        
        print("\n" + "=" * 60)
        print("Test 2: Checking Recent Bets (to verify trading access)...")
        print("=" * 60)
        
        # Try to get recent bets
        url = "https://sports-api.cloudbet.com/pub/v3/bets"
        try:
            resp = await client.get(url, headers=headers)
            print(f"Bets API Status: {resp.status_code}")
            if resp.status_code == 200:
                print(f"Response: {resp.text[:500]}")
                print("\nSUCCESS: Trading API access confirmed!")
            else:
                print(f"Error: {resp.text[:500]}")
        except Exception as e:
            print(f"Bets API Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_cloudbet_trading_access())
