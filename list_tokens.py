import asyncio
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.fetchers.polymarket_fetcher import PolymarketFetcher

async def list_tokens():
    load_dotenv()
    pm_fetcher = PolymarketFetcher()
    try:
        print("Fetching PM markets...")
        pm_markets = await pm_fetcher.fetch_all_markets(limit=200) # Smaller limit for speed
        print(f"Found {len(pm_markets)} parsed markets")
        
        count = 0
        for m in pm_markets:
            title = m.get('title', '')
            tokens = m.get('metadata', {}).get('token_ids', {})
            if tokens:
                print(f"Match with Tokens: {title} -> {tokens}")
                count += 1
                if count >= 20:
                    break
        
        if count == 0:
            print("❌ NO MARKETS WITH TOKENS FOUND!")
            # Debug: Print first market structure
            if pm_markets:
                print(f"Sample market structure: {pm_markets[0]}")
    finally:
        await pm_fetcher.close()

if __name__ == "__main__":
    asyncio.run(list_tokens())
