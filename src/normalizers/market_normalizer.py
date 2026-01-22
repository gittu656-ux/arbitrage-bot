"""
Normalizes market data from different platforms into unified schema.
"""
from typing import List, Dict
from ..models import NormalizedMarket
from ..logger import setup_logger


class MarketNormalizer:
    """Normalizes markets from different platforms."""
    
    def __init__(self):
        self.logger = setup_logger("market_normalizer")
    
    def normalize_polymarket(self, raw_markets: List[Dict]) -> List[NormalizedMarket]:
        """Normalize Polymarket markets."""
        normalized = []
        
        for market in raw_markets:
            try:
                norm = NormalizedMarket(
                    platform='polymarket',
                    market_id=market.get('market_id', ''),
                    title=market.get('title', ''),
                    outcomes=market.get('outcomes', {}),
                    url=market.get('url', ''),
                    start_time=market.get('start_time'),
                    metadata=market.get('metadata', {})
                )
                normalized.append(norm)
            except Exception as e:
                self.logger.warning(f"Failed to normalize Polymarket market: {e}")
                continue
        
        return normalized
    
    def normalize_cloudbet(self, raw_outcomes: List[Dict]) -> List[NormalizedMarket]:
        """Normalize Cloudbet outcomes into markets."""
        # Group outcomes by event_name + market_name
        markets_dict = {}
        
        for outcome in raw_outcomes:
            event_name = outcome.get('event_name', 'Unknown')
            market_name = outcome.get('market_name', 'Unknown')
            key = f"{event_name}::{market_name}"
            
            if key not in markets_dict:
                markets_dict[key] = {
                    'platform': 'cloudbet',
                    'market_id': key,
                    'title': f"{event_name} - {market_name}",
                    'outcomes': {},
                    'url': outcome.get('url', ''),
                    'start_time': outcome.get('start_time'),
                    'metadata': {
                        'selection_ids': {},
                        'event_id': outcome.get('market_id'), # market_id in outcome is the event_id
                        'sport_key': outcome.get('sport_key')
                    }
                }
            
            outcome_name = outcome.get('outcome', 'Unknown')
            odds = outcome.get('odds', 0.0)
            markets_dict[key]['outcomes'][outcome_name] = odds
            # Store selection ID in metadata
            markets_dict[key]['metadata']['selection_ids'][outcome_name] = outcome.get('selection_id')
        
        # Convert to NormalizedMarket objects
        normalized = []
        for market_data in markets_dict.values():
            try:
                # Only include markets with at least 2 outcomes
                if len(market_data['outcomes']) >= 2:
                    norm = NormalizedMarket(**market_data)
                    normalized.append(norm)
            except Exception as e:
                self.logger.warning(f"Failed to normalize Cloudbet market: {e}")
                continue
        
        return normalized

