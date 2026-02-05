"""
Probability-based value detection engine.

Converts all outcomes to implied probabilities and detects value edges
between Polymarket (prediction markets) and Cloudbet (sportsbook).
"""
from typing import List, Dict, Optional, Tuple
from .logger import setup_logger
from .sports_matcher import SportsMarketDetector


class ProbabilityEngine:
    """
    Converts odds/probabilities to implied probabilities and detects value edges.
    
    Handles:
    - Polymarket: YES/NO outcomes with probability-based pricing
    - Cloudbet: Moneyline odds (decimal format)
    
    Detects:
    - Arbitrage opportunities (sum of probabilities < 1.0)
    - Value edges (one platform has significantly better odds)
    """
    
    def __init__(
        self,
        min_value_edge: float = 0.05,  # 5% minimum edge
        min_arbitrage_profit: float = 0.5  # 0.5% minimum for arbitrage
    ):
        """
        Initialize probability engine.
        
        Args:
            min_value_edge: Minimum probability edge to consider (0.05 = 5%)
            min_arbitrage_profit: Minimum profit % for arbitrage alerts
        """
        self.min_value_edge = min_value_edge
        self.min_arbitrage_profit = min_arbitrage_profit
        self.logger = setup_logger("probability_engine")
        self.detector = SportsMarketDetector()
    
    def _normalize_team_name(self, name: str) -> str:
        """Normalize team name for matching."""
        name = name.lower().strip()
        name = name.replace('s-', '').replace('h-', '').replace('a-', '')
        name = name.replace('-', ' ').replace('_', ' ')
        name = ' '.join(name.split())
        return name
    
    def _odds_to_probability(self, odds: float) -> float:
        """Convert decimal odds to implied probability."""
        if odds <= 1.0:
            return 0.0
        return 1.0 / odds
    
    def _probability_to_odds(self, prob: float) -> float:
        """Convert probability to decimal odds."""
        if prob <= 0 or prob >= 1:
            return 0.0
        return 1.0 / prob
    
    def _map_polymarket_to_teams(
        self,
        pm_outcomes: Dict[str, float],
        pm_title: str,
        cb_teams: Tuple[str, str],
        is_futures: bool = False
    ) -> Dict[str, float]:
        """
        Map Polymarket YES/NO outcomes to team probabilities.
        
        Example:
        PM: "Will Lakers beat Warriors?" (YES: 0.6, NO: 0.4)
        CB Teams: ("Lakers", "Warriors")
        Returns: {"Lakers": 0.6, "Warriors": 0.4}
        """
        team_probs = {}
        
        # Extract teams from Polymarket title
        pm_teams = self.detector.extract_teams_from_title(pm_title)
        
        # Check if Polymarket has YES/NO outcomes
        yes_prob = None
        no_prob = None
        
        for outcome_name, odds in pm_outcomes.items():
            outcome_upper = outcome_name.upper()
            if outcome_upper in ['YES', 'Y']:
                yes_prob = self._odds_to_probability(odds)
            elif outcome_upper in ['NO', 'N']:
                no_prob = self._odds_to_probability(odds)
        
        # Handle futures markets (single team)
        if is_futures and pm_teams[0] and not pm_teams[1]:
            # "Will Ravens win Super Bowl?" -> YES = Ravens wins
            # Map YES to the team that matches Cloudbet teams
            if yes_prob is not None:
                pm_team_norm = self._normalize_team_name(pm_teams[0])
                cb1_norm = self._normalize_team_name(cb_teams[0])
                cb2_norm = self._normalize_team_name(cb_teams[1])
                
                from rapidfuzz import fuzz
                sim1 = fuzz.ratio(pm_team_norm, cb1_norm)
                sim2 = fuzz.ratio(pm_team_norm, cb2_norm)
                
                # Map to the Cloudbet team that matches better
                if sim1 > sim2 and sim1 > 60:
                    team_probs[cb_teams[0]] = yes_prob
                    # For the other team, use NO probability or complement
                    if no_prob is not None:
                        team_probs[cb_teams[1]] = no_prob
                    else:
                        team_probs[cb_teams[1]] = 1.0 - yes_prob
                elif sim2 > sim1 and sim2 > 60:
                    team_probs[cb_teams[1]] = yes_prob
                    if no_prob is not None:
                        team_probs[cb_teams[0]] = no_prob
                    else:
                        team_probs[cb_teams[0]] = 1.0 - yes_prob
        
        # If we have YES/NO, map to teams based on title (game markets)
        elif yes_prob is not None and no_prob is not None and pm_teams[0] and pm_teams[1]:
            # Determine which team YES refers to
            # "Will Lakers beat Warriors?" -> YES = Lakers wins
            # "Will Warriors beat Lakers?" -> YES = Warriors wins
            
            # Normalize team names for matching
            pm_team1_norm = self._normalize_team_name(pm_teams[0])
            pm_team2_norm = self._normalize_team_name(pm_teams[1])
            cb_team1_norm = self._normalize_team_name(cb_teams[0])
            cb_team2_norm = self._normalize_team_name(cb_teams[1])
            
            # Match PM team1 to CB team1 or CB team2
            from rapidfuzz import fuzz
            
            match_team1_to_cb1 = fuzz.ratio(pm_team1_norm, cb_team1_norm)
            match_team1_to_cb2 = fuzz.ratio(pm_team1_norm, cb_team2_norm)
            
            if match_team1_to_cb1 > match_team1_to_cb2:
                # PM team1 = CB team1, so YES = CB team1 wins
                team_probs[cb_teams[0]] = yes_prob
                team_probs[cb_teams[1]] = no_prob
            else:
                # PM team1 = CB team2, so YES = CB team2 wins
                team_probs[cb_teams[1]] = yes_prob
                team_probs[cb_teams[0]] = no_prob
        
        # If Polymarket already has team names, use them directly
        else:
            for outcome_name, odds in pm_outcomes.items():
                prob = self._odds_to_probability(odds)
                # Try to match outcome name to Cloudbet team names
                outcome_norm = self._normalize_team_name(outcome_name)
                
                # Match to CB team1 or team2
                cb_team1_norm = self._normalize_team_name(cb_teams[0])
                cb_team2_norm = self._normalize_team_name(cb_teams[1])
                
                from rapidfuzz import fuzz
                match1 = fuzz.ratio(outcome_norm, cb_team1_norm)
                match2 = fuzz.ratio(outcome_norm, cb_team2_norm)
                
                if match1 > match2 and match1 > 70:
                    team_probs[cb_teams[0]] = prob
                elif match2 > match1 and match2 > 70:
                    team_probs[cb_teams[1]] = prob
        
        return team_probs
    
    def _convert_cloudbet_to_probabilities(
        self,
        cb_outcomes: Dict[str, float],
        cb_teams: Tuple[str, str]
    ) -> Dict[str, float]:
        """
        Convert Cloudbet moneyline odds to team probabilities.
        
        Handles formats like:
        - {"s-lakers": 2.0, "s-warriors": 1.8} -> {"Lakers": 0.5, "Warriors": 0.556}
        - {"home": 2.0, "away": 1.8} -> Maps to teams based on event structure
        - {"Lakers": 2.0, "Warriors": 1.8} -> Direct mapping
        """
        team_probs = {}
        
        # First, try to match outcomes directly to team names
        for outcome_name, outcome_data in cb_outcomes.items():
            # Handle both old float format and new dict format
            if isinstance(outcome_data, dict):
                odds = outcome_data.get('odds', 0.0)
            else:
                odds = outcome_data
                
            if odds <= 1.0:
                continue
                
            prob = self._odds_to_probability(odds)
            
            # Normalize outcome name and match to teams
            outcome_norm = self._normalize_team_name(outcome_name)
            cb_team1_norm = self._normalize_team_name(cb_teams[0])
            cb_team2_norm = self._normalize_team_name(cb_teams[1])
            
            from rapidfuzz import fuzz
            
            # Try direct name matching
            match1 = fuzz.ratio(outcome_norm, cb_team1_norm)
            match2 = fuzz.ratio(outcome_norm, cb_team2_norm)
            
            # Also try token-based matching (handles "s-lakers" vs "lakers")
            token_match1 = fuzz.token_sort_ratio(outcome_norm, cb_team1_norm)
            token_match2 = fuzz.token_sort_ratio(outcome_norm, cb_team2_norm)
            
            # Use best match
            best_match1 = max(match1, token_match1)
            best_match2 = max(match2, token_match2)
            
            # Lower threshold to 60% for more matches
            if best_match1 > best_match2 and best_match1 > 60:
                team_probs[cb_teams[0]] = {
                    'prob': prob,
                    'data': outcome_data if isinstance(outcome_data, dict) else {'odds': odds}
                }
            elif best_match2 > best_match1 and best_match2 > 60:
                team_probs[cb_teams[1]] = {
                    'prob': prob,
                    'data': outcome_data if isinstance(outcome_data, dict) else {'odds': odds}
                }
            # If neither matches well, try "home"/"away"/"team1"/"team2" mapping
            elif outcome_norm in ['home', 'h', 'team1', '1'] and cb_teams[0]:
                # Assume first team is home/team1
                team_probs[cb_teams[0]] = {
                    'prob': prob,
                    'data': outcome_data if isinstance(outcome_data, dict) else {'odds': odds}
                }
            elif outcome_norm in ['away', 'a', 'team2', '2'] and cb_teams[1]:
                # Assume second team is away/team2
                team_probs[cb_teams[1]] = {
                    'prob': prob,
                    'data': outcome_data if isinstance(outcome_data, dict) else {'odds': odds}
                }
        
        return team_probs
    
    def detect_value_opportunities(self, matched_events: List[Dict]) -> List[Dict]:
        """
        Detect value opportunities from event-level matches.
        
        Compares probabilities between platforms to find:
        1. Arbitrage (sum < 1.0)
        2. Value edges (one platform has significantly better odds)
        
        Args:
            matched_events: List of event-level matches from EventMatcher
        
        Returns:
            List of value opportunities
        """
        opportunities = []
        
        for match in matched_events:
            pm_title = match.get('market_name', 'Unknown')
            cb_event_name = match.get('event_name', 'Unknown')
            pm_outcomes = match.get('pm_outcomes', {})
            cb_outcomes = match.get('cb_outcomes', {})
            pm_teams = match.get('pm_teams', (None, None))
            cb_teams = match.get('cb_teams', (None, None))
            is_futures = match.get('is_futures_market', False)
            
            # For futures, pm_teams[1] will be None, which is OK
            if not pm_teams[0] or not cb_teams[0] or not cb_teams[1]:
                continue
            
            # Convert Polymarket outcomes to team probabilities
            pm_team_probs = self._map_polymarket_to_teams(
                pm_outcomes, pm_title, cb_teams, is_futures=is_futures
            )
            
            # Convert Cloudbet outcomes to team probabilities
            cb_team_probs = self._convert_cloudbet_to_probabilities(
                cb_outcomes, cb_teams
            )
            
            # Need probabilities for both teams on both platforms
            if len(pm_team_probs) < 2 or len(cb_team_probs) < 2:
                self.logger.debug(
                    f"Skipping {pm_title}: PM probs={len(pm_team_probs)}, CB probs={len(cb_team_probs)}. "
                    f"PM outcomes: {list(pm_outcomes.keys())}, CB outcomes: {list(cb_outcomes.keys())[:5]}"
                )
                continue
            
            # Get probabilities for each team
            team1 = cb_teams[0]
            team2 = cb_teams[1]
            
            pm_prob_team1 = pm_team_probs.get(team1, 0)
            pm_prob_team2 = pm_team_probs.get(team2, 0)
            
            # Cloudbet probabilities are now in dicts
            cb_data_team1 = cb_team_probs.get(team1, {})
            cb_data_team2 = cb_team_probs.get(team2, {})
            
            cb_prob_team1 = cb_data_team1.get('prob', 0)
            cb_prob_team2 = cb_data_team2.get('prob', 0)
            
            if not pm_prob_team1 or not pm_prob_team2 or not cb_prob_team1 or not cb_prob_team2:
                self.logger.debug(
                    f"Skipping {pm_title}: Missing probabilities. "
                    f"PM: team1={pm_prob_team1:.2%}, team2={pm_prob_team2:.2%}, "
                    f"CB: team1={cb_prob_team1:.2%}, team2={cb_prob_team2:.2%}"
                )
                continue
            
            # Check for arbitrage (sum of probabilities < 1.0)
            # Team1: PM prob + CB prob (opposite outcome)
            total_prob_team1 = pm_prob_team1 + cb_prob_team2
            total_prob_team2 = pm_prob_team2 + cb_prob_team1
            
            # Use the better arbitrage opportunity
            arbitrage_found = False
            if total_prob_team1 < total_prob_team2:
                total_prob = total_prob_team1
                arb_team = team1
                pm_team = team1  # Bet on team1 on Polymarket
                cb_team = team2  # Bet on team2 on Cloudbet (opposite)
                pm_odds = self._probability_to_odds(pm_prob_team1)
                cb_odds = self._probability_to_odds(cb_prob_team2)
                cb_metadata = cb_data_team2.get('data', {})
                pm_outcome_name = "YES" if pm_prob_team1 == max(pm_prob_team1, pm_prob_team2) else "NO"
            else:
                total_prob = total_prob_team2
                arb_team = team2
                pm_team = team2  # Bet on team2 on Polymarket
                cb_team = team1  # Bet on team1 on Cloudbet (opposite)
                pm_odds = self._probability_to_odds(pm_prob_team2)
                cb_odds = self._probability_to_odds(cb_prob_team1)
                cb_metadata = cb_data_team1.get('data', {})
                pm_outcome_name = "NO" if pm_prob_team1 == max(pm_prob_team1, pm_prob_team2) else "YES"
            
            # Check for arbitrage
            if total_prob < 1.0:
                profit_pct = ((1.0 - total_prob) / total_prob) * 100
                
                if profit_pct >= self.min_arbitrage_profit:
                    arbitrage_found = True
                    opportunity = {
                        'market_name': pm_title,
                        'event_name': cb_event_name,
                        'type': 'arbitrage',
                        'platform_a': 'polymarket',
                        'platform_b': 'cloudbet',
                        # Keep team tuples so downstream formatters can show real names
                        'pm_teams': pm_teams,
                        'cb_teams': cb_teams,
                        'team': arb_team,
                        'pm_outcome': pm_outcome_name,
                        'pm_probability': pm_prob_team1 if arb_team == team1 else pm_prob_team2,
                        'cb_probability': cb_prob_team2 if arb_team == team1 else cb_prob_team1,
                        'pm_odds': pm_odds,
                        'cb_odds': cb_odds,
                        'odds_a': pm_odds,  # For bet sizing calculator (Polymarket)
                        'odds_b': cb_odds,  # For bet sizing calculator (Cloudbet - opposite)
                        'total_probability': total_prob,
                        'profit_percentage': profit_pct,
                        'market_a': match['market_a'],
                        'market_b': match['market_b'],
                        'sport_key': match.get('sport', 'unknown'),
                        'start_time': match.get('cb_time'),
                        # Add team names for each platform (opposite outcomes for arbitrage)
                        'outcome_a': {'name': pm_team, 'odds': pm_odds},  # Polymarket team
                        'outcome_b': {
                            'name': cb_team, 
                            'odds': cb_odds,
                            'event_id': cb_metadata.get('event_id'),
                            'market_url': cb_metadata.get('market_url'),
                            'selection_id': cb_metadata.get('selection_id')
                        }   # Cloudbet team (opposite)
                    }
                    
                    opportunities.append(opportunity)
                    
                    # Log with actual odds for debugging
                    self.logger.info(
                        f"ARBITRAGE: {pm_title} - {arb_team} - "
                        f"PM: {pm_team} @ {pm_odds:.2f} ({pm_prob_team1:.2%}) vs "
                        f"CB: {cb_team} @ {cb_odds:.2f} ({cb_prob_team2:.2%}) - "
                        f"Profit: {profit_pct:.2f}%"
                    )
            
            # Only check for value edges if NO arbitrage was found
            # This prioritizes arbitrage over value edges
            if not arbitrage_found:
                # Also check for value edges (one platform has better odds)
                # Value edge: PM says 60% chance, CB says 50% chance = 10% edge
                edge_team1 = pm_prob_team1 - cb_prob_team1
                edge_team2 = pm_prob_team2 - cb_prob_team2
                
                # Check if there's a significant edge
                if abs(edge_team1) >= self.min_value_edge:
                    opportunity = {
                        'market_name': pm_title,
                        'event_name': cb_event_name,
                        'type': 'value_edge',
                        'platform_a': 'polymarket',
                        'platform_b': 'cloudbet',
                        'team': team1,
                        'edge_percentage': edge_team1 * 100,
                        'pm_probability': pm_prob_team1,
                        'cb_probability': cb_prob_team1,
                        'pm_odds': self._probability_to_odds(pm_prob_team1),
                        'cb_odds': self._probability_to_odds(cb_prob_team1),
                        'better_platform': 'polymarket' if edge_team1 > 0 else 'cloudbet',
                        'pm_teams': pm_teams,
                        'cb_teams': cb_teams,
                        'market_a': match['market_a'],
                        'market_b': match['market_b'],
                        'sport_key': match.get('sport', 'unknown'),
                        'start_time': match.get('cb_time')
                    }
                    
                    opportunities.append(opportunity)
                    
                    self.logger.info(
                        f"VALUE EDGE: {pm_title} - {team1} - "
                        f"PM: {pm_prob_team1:.2%} vs CB: {cb_prob_team1:.2%} - "
                        f"Edge: {edge_team1*100:.2f}%"
                    )
                
                if abs(edge_team2) >= self.min_value_edge:
                    opportunity = {
                        'market_name': pm_title,
                        'event_name': cb_event_name,
                        'type': 'value_edge',
                        'platform_a': 'polymarket',
                        'platform_b': 'cloudbet',
                        'team': team2,
                        'edge_percentage': edge_team2 * 100,
                        'pm_probability': pm_prob_team2,
                        'cb_probability': cb_prob_team2,
                        'pm_odds': self._probability_to_odds(pm_prob_team2),
                        'cb_odds': self._probability_to_odds(cb_prob_team2),
                        'better_platform': 'polymarket' if edge_team2 > 0 else 'cloudbet',
                        'pm_teams': pm_teams,
                        'cb_teams': cb_teams,
                        'market_a': match['market_a'],
                        'market_b': match['market_b'],
                        'sport_key': match.get('sport', 'unknown'),
                        'start_time': match.get('cb_time')
                    }
                    
                    opportunities.append(opportunity)
                    
                    self.logger.info(
                        f"VALUE EDGE: {pm_title} - {team2} - "
                        f"PM: {pm_prob_team2:.2%} vs CB: {cb_prob_team2:.2%} - "
                        f"Edge: {edge_team2*100:.2f}%"
                    )
        
        arbitrage_count = sum(1 for o in opportunities if o['type'] == 'arbitrage')
        value_edge_count = sum(1 for o in opportunities if o['type'] == 'value_edge')
        
        self.logger.info(
            f"Detected {len(opportunities)} value opportunities "
            f"({arbitrage_count} arbitrage, {value_edge_count} value edges) "
            f"from {len(matched_events)} matched events"
        )
        
        # Debug: Log why opportunities weren't found
        if len(opportunities) == 0 and len(matched_events) > 0:
            self.logger.info(
                f"No opportunities found from {len(matched_events)} matches. "
                f"Thresholds: min_arbitrage={self.min_arbitrage_profit}%, "
                f"min_value_edge={self.min_value_edge*100}%. "
                f"Check DEBUG logs for probability conversion details."
            )
        
        return opportunities

