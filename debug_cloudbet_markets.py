"""
Debug script to check what market_type Cloudbet actually returns for NBA games.
This will help us identify why we're getting wrong odds (1.75 instead of 1.31).
"""
import asyncio
import json
from src.fetchers.cloudbet_fetcher import CloudbetFetcher
from src.logger import setup_logger

logger = setup_logger("debug_cloudbet")

async def main():
    fetcher = CloudbetFetcher()
    
    try:
        # Fetch all Cloudbet outcomes
        logger.info("Fetching Cloudbet outcomes...")
        outcomes = await fetcher.fetch_all_markets()
        
        # Find OKC Thunder vs SA Spurs game
        target_event = None
        for outcome in outcomes:
            event_name = outcome.get('event_name', '')
            if 'thunder' in event_name.lower() and 'spurs' in event_name.lower():
                target_event = event_name
                break
        
        if not target_event:
            logger.warning("Could not find OKC Thunder vs SA Spurs game")
            # Try to find any NBA game
            for outcome in outcomes:
                if outcome.get('sport_key') == 'basketball':
                    target_event = outcome.get('event_name')
                    logger.info(f"Using NBA game as example: {target_event}")
                    break
        
        if not target_event:
            logger.error("No NBA games found")
            return
        
        logger.info(f"Analyzing event: {target_event}")
        
        # Group all outcomes for this event by market_type
        market_types = {}
        for outcome in outcomes:
            if outcome.get('event_name') == target_event:
                market_type = outcome.get('market_type', 'UNKNOWN')
                outcome_name = outcome.get('outcome', 'Unknown')
                odds = outcome.get('odds', 0)
                
                if market_type not in market_types:
                    market_types[market_type] = []
                
                market_types[market_type].append({
                    'outcome': outcome_name,
                    'odds': odds
                })
        
        # Print summary
        logger.info(f"\n{'='*80}")
        logger.info(f"Market types found for '{target_event}':")
        logger.info(f"{'='*80}")
        
        for market_type, outcomes_list in sorted(market_types.items()):
            logger.info(f"\nMarket Type: '{market_type}' ({len(outcomes_list)} outcomes)")
            # Show first 5 outcomes
            for item in outcomes_list[:5]:
                logger.info(f"  - {item['outcome']}: {item['odds']:.2f}")
            if len(outcomes_list) > 5:
                logger.info(f"  ... and {len(outcomes_list) - 5} more")
        
        # Check which market type has moneyline-like odds
        logger.info(f"\n{'='*80}")
        logger.info("Looking for main moneyline (should be around 1.31 for OKC, 3.68 for SA):")
        logger.info(f"{'='*80}")
        
        for market_type, outcomes_list in sorted(market_types.items()):
            # Check if this looks like moneyline (2 outcomes, reasonable odds)
            if len(outcomes_list) == 2:
                odds_values = [item['odds'] for item in outcomes_list]
                odds_values.sort()
                # Moneyline typically has one favorite (< 2.0) and one underdog (> 2.0)
                if odds_values[0] < 2.0 and odds_values[1] > 2.0:
                    logger.info(f"\n✓ Potential moneyline in '{market_type}':")
                    for item in outcomes_list:
                        logger.info(f"  {item['outcome']}: {item['odds']:.2f}")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        await fetcher.close()

if __name__ == "__main__":
    asyncio.run(main())

