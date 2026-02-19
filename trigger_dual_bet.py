import asyncio
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.execution.polymarket_executor import PolymarketExecutor
from src.execution.cloudbet_executor import CloudbetExecutor
from src.fetchers.polymarket_fetcher import PolymarketFetcher
from src.fetchers.cloudbet_fetcher import CloudbetFetcher
from src.event_matcher import EventMatcher

async def trigger_dual_test_bets():
    load_dotenv()
    
    # Load keys
    pm_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    cb_key = os.getenv("CLOUDBET_API_KEY")
    cb_proxy = os.getenv("CLOUDBET_PROXY")
    
    print("--- 🚀 Initializing Target Soccer Test Bet ($1 each) ---")
    
    pm_executor = PolymarketExecutor(pm_key)
    cb_executor = CloudbetExecutor(api_key=cb_key, proxy=cb_proxy)
    pm_fetcher = PolymarketFetcher()
    cb_fetcher = CloudbetFetcher(api_key=cb_key)
    matcher = EventMatcher()
    
    try:
        # Step 1: Find a specific match on PM
        print("🔍 Searching for a major Soccer match on Polymarket...")
        pm_markets = await pm_fetcher.fetch_all_markets(limit=300)
        
        target_pm = None
        for m in pm_markets:
            title = m.get('title', '')
            tokens = m.get('metadata', {}).get('token_ids', {})
            # Target Chelsea vs Burnley or any other found in list_tokens
            if 'Chelsea' in title and 'Burnley' in title and tokens:
                target_pm = m
                break
        
        if not target_pm:
            # Fallback 1: Any soccer with tokens
            for m in pm_markets:
                title = m.get('title', '')
                tokens = m.get('metadata', {}).get('token_ids', {})
                if 'vs.' in title and tokens and any(x in title.lower() for x in ['fc', 'united', 'city']):
                    target_pm = m
                    break
                    
        if not target_pm:
            # Fallback 2: Any market with tokens
            for m in pm_markets:
                if m.get('metadata', {}).get('token_ids'):
                    target_pm = m
                    break
        
        if not target_pm:
            print("❌ No suitable match with tokens found on Polymarket.")
            return

        pm_title = target_pm.get('title', 'Unknown')
        print(f"✅ Selected PM Match: {pm_title}")
        
        # Step 2: Extract PM Token
        metadata = target_pm.get('metadata', {})
        token_ids_dict = metadata.get('token_ids', {})
        pm_token = list(token_ids_dict.values())[0] if token_ids_dict else None
            
        if not pm_token:
            print(f"❌ PM Token not found for {pm_title}.")
            return

        # Step 3: Find match on Cloudbet
        print("🔍 Fetching Cloudbet markets to match...")
        cb_outcomes = await cb_fetcher.fetch_all_markets()
        
        target_cb_outcome = None
        pm_teams = matcher._extract_teams(pm_title)
        
        for outcome in cb_outcomes:
            cb_event_name = outcome.get('event_name', '')
            cb_teams = matcher._extract_teams(cb_event_name)
            
            if pm_teams[0] and pm_teams[1] and cb_teams[0] and cb_teams[1]:
                match, _ = matcher._teams_match(pm_teams[0], pm_teams[1], cb_teams[0], cb_teams[1])
                if match:
                    target_cb_outcome = outcome
                    break
                    
        if not target_cb_outcome:
            print(f"❌ Could not find a matching event on Cloudbet for {pm_title}")
            # Log some CB soccer events to debug
            print("CB Soccer Samples:")
            soccer_ev = [o['event_name'] for o in cb_outcomes if 'soccer' in str(o.get('sport_key')).lower()][:5]
            print(soccer_ev)
            return
            
        print(f"✅ Found Cloudbet Match: {target_cb_outcome['event_name']}")

        # Step 4: Execute
        print(f"\n💰 Placing $1 bet on Polymarket ({pm_title})...")
        pm_res = await pm_executor.place_order(token_id=pm_token, price=0.95, side="BUY", amount=1.0)
        
        print(f"💰 Placing $1 bet on Cloudbet ({target_cb_outcome['event_name']} - {target_cb_outcome['outcome']})...")
        cb_res = await cb_executor.place_bet(
            event_id=str(target_cb_outcome['event_id']),
            market_url=target_cb_outcome['market_url'],
            odds=target_cb_outcome['odds'],
            stake=1.0
        )
        
        if pm_res and cb_res:
            print("\n🎉 SUCCESS! Both bets placed.")
        else:
            print("\n⚠️ One or both bets failed. PM success: " + str(bool(pm_res)) + ", CB success: " + str(bool(cb_res)))

    finally:
        await pm_fetcher.close()
        await cb_fetcher.close()
        await cb_executor.close()

if __name__ == "__main__":
    asyncio.run(trigger_dual_test_bets())
