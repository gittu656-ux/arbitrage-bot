
import asyncio
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.fetchers.polymarket_fetcher import PolymarketFetcher
from src.execution.polymarket_executor import PolymarketExecutor
from src.logger import setup_logger
from dotenv import load_dotenv

async def main():
    load_dotenv()
    logger = setup_logger("test_bet")
    
    pm_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    if not pm_key:
        print("[-] Error: POLYMARKET_PRIVATE_KEY not set in .env")
        return

    fetcher = PolymarketFetcher()
    executor = PolymarketExecutor(pm_key)
    
    print("[-] Fetching Polymarket markets to find an active one...")
    markets = await fetcher.fetch_all_markets(limit=20)
    
    target_market = None
    target_token_id = None
    target_outcome = None
    
    for m in markets:
        token_ids = m.get('metadata', {}).get('token_ids', {})
        if token_ids:
            for outcome, tid in token_ids.items():
                if tid:
                    target_market = m
                    target_token_id = tid
                    target_outcome = outcome
                    break
        if target_market:
            break
            
    if not target_market:
        print("[-] Error: Could not find any active market with a Polymarket Token ID.")
        await fetcher.close()
        return

    print(f"[*] Found Market: {target_market['title']}")
    print(f"[*] Outcome: {target_outcome}")
    print(f"[*] Token ID: {target_token_id}")
    
    # Place a $5 bet at current price (or slightly higher to ensure fill)
    outcomes = target_market.get('outcomes', {})
    current_odds = outcomes.get(target_outcome, 2.0)
    current_price = 1.0 / current_odds
    
    # Increase price by 0.05 to ensure fill
    limit_price = min(0.98, current_price + 0.05)
    amount_usdc = 5.0
    
    print(f"[*] Placing $5 BUY order on '{target_outcome}' at limit price {limit_price:.2f}...")
    
    try:
        resp = await executor.place_order(
            token_id=target_token_id,
            price=limit_price,
            side="BUY",
            amount=amount_usdc
        )
        
        if resp:
            print("\n[+] SUCCESS! Polymarket Autobet logic is WORKING perfectly.")
            print(f"Order Response: {resp}")
        else:
            print("\n[-] FAILED: Order was not placed successfully. Check the logs.")
            
    except Exception as e:
        print(f"\n[-] CRITICAL ERROR: {e}")
    finally:
        await fetcher.close()

if __name__ == "__main__":
    asyncio.run(main())
