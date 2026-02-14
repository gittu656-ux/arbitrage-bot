
import asyncio
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.execution.cloudbet_executor import CloudbetExecutor
from src.execution.polymarket_executor import PolymarketExecutor
from src.logger import setup_logger

async def test_insufficient_funds():
    logger = setup_logger("test_bet")
    
    # Get keys from .env
    from dotenv import load_dotenv
    load_dotenv()
    
    pm_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    cb_key = os.getenv("CLOUDBET_API_KEY")
    
    if not pm_key or not cb_key:
        print("❌ Error: POLYMARKET_PRIVATE_KEY and CLOUDBET_API_KEY must be set in .env")
        return

    print("--- Testing Cloudbet Insufficient Funds ---")
    cb_executor = CloudbetExecutor(cb_key)
    # Using a fake but valid-format event_id and market_url
    try:
        await cb_executor.place_bet(
            event_id="999999", 
            market_url="soccer.winner/home", 
            odds=2.0, 
            stake=1000000.0, # Huge stake to trigger insufficient funds
            currency="USDT"
        )
    finally:
        await cb_executor.close()

    print("\n--- Testing Polymarket Insufficient Funds ---")
    pm_executor = PolymarketExecutor(pm_key)
    # Using a fake but valid-format token_id
    # Note: Polymarket might fail with 'invalid token' before 'insufficient funds' if token is totally fake.
    # We should try to use a real token ID if possible, or just observe the error.
    try:
        await pm_executor.place_order(
            token_id="21742461017342628469333535940027584112447959955734222046467389476723225884877", # Random token ID
            price=0.5, 
            side="BUY", 
            amount=1000000.0 # Huge amount
        )
    except Exception as e:
        print(f"Caught expected error: {e}")

if __name__ == "__main__":
    asyncio.run(test_insufficient_funds())
