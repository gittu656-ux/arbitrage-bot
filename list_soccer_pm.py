import asyncio
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.fetchers.polymarket_fetcher import PolymarketFetcher

async def list_soccer():
    load_dotenv()
    pm_fetcher = PolymarketFetcher()
    try:
        print("Fetching PM markets...")
        pm_markets = await pm_fetcher.fetch_all_markets()
        print(f"Found {len(pm_markets)} PM markets")
        
        for m in pm_markets:
            title = m.title if hasattr(m, 'title') else m.get('title', '')
            if 'soccer' in title.lower():
                print(f"PM Soccer: {title}")
    finally:
        await pm_fetcher.close()

if __name__ == "__main__":
    asyncio.run(list_soccer())
