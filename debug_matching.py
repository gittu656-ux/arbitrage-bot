import asyncio
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.fetchers.polymarket_fetcher import PolymarketFetcher
from src.fetchers.cloudbet_fetcher import CloudbetFetcher
from src.event_matcher import EventMatcher

async def debug_matching():
    load_dotenv()
    cb_key = os.getenv("CLOUDBET_API_KEY")
    
    pm_fetcher = PolymarketFetcher()
    cb_fetcher = CloudbetFetcher(api_key=cb_key)
    matcher = EventMatcher()
    
    try:
        print("Fetching PM markets...")
        pm_markets = await pm_fetcher.fetch_all_markets()
        print(f"Found {len(pm_markets)} PM markets")
        
        print("\nFetching CB markets...")
        cb_outcomes = await cb_fetcher.fetch_all_markets()
        print(f"Found {len(cb_outcomes)} CB outcomes")
        
        cb_events = {}
        for outcome in cb_outcomes:
            event_name = outcome.get('event_name', '')
            if event_name not in cb_events:
                cb_events[event_name] = outcome
        
        print("\nCB Soccer Team Extraction Samples:")
        soccer_events = [name for name, data in cb_events.items() if 'soccer' in str(data.get('sport_key', '')).lower()]
        for name in soccer_events[:50]:
            cb_teams = matcher._extract_teams(name)
            print(f"CB Name: {name} -> Teams: {cb_teams}")

        print("\nPM Soccer Team Extraction Samples:")
        # Check both attributes and dictionary access
        soccer_pm = []
        for m in pm_markets:
            title = m.title if hasattr(m, 'title') else m.get('title', '')
            if 'soccer' in title.lower():
                soccer_pm.append(m)
                
        for m in soccer_pm[:50]:
            title = m.title if hasattr(m, 'title') else m.get('title', '')
            pm_teams = matcher._extract_teams(title)
            print(f"PM Title: {title} -> Teams: {pm_teams}")

    finally:
        await pm_fetcher.close()
        await cb_fetcher.close()

if __name__ == "__main__":
    asyncio.run(debug_matching())
