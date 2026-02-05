import asyncio
import httpx
import os
from dotenv import load_dotenv

async def verify_cloudbet_trading_key():
    load_dotenv()
    api_key = os.getenv("CLOUDBET_API_KEY")
    if not api_key:
        print("ERROR: CLOUDBET_API_KEY not found in .env file.")
        return

    # Using the V3 bets endpoint to check if we have permission to access trading functions
    url = "https://sports-api.cloudbet.com/pub/v3/bets"
    
    headers = {
        "X-API-Key": api_key,
        "Accept": "application/json"
    }

    print(f"Checking if API Key is a 'Trading API' key...")
    print(f"Querying: {url}\n")

    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        try:
            response = await client.get(url, headers=headers)
            
            if response.status_code == 200:
                print("SUCCESS: This is a TRADING API KEY.")
                print("Your balances:")
                print(response.text)
            elif response.status_code == 403:
                print("FORBIDDEN (403): This is NOT a Trading API Key.")
                print("Reason: You likely have an 'Affiliate' key. You need to deposit 10 EUR and generate a 'Trading API Key' from the Account -> API section.")
            elif response.status_code == 401:
                print("UNAUTHORIZED (401): The API Key is invalid or expired.")
            else:
                print(f"UNKNOWN STATUS ({response.status_code}): {response.text}")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(verify_cloudbet_trading_key())
