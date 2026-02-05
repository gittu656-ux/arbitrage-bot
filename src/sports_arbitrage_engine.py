"""
Sports-specific arbitrage detection engine.

Handles arbitrage detection for sports markets with outcome translation.
Now uses probability-based value detection for event-level matches.
"""
from typing import List, Dict, Optional
from .logger import setup_logger
from .probability_engine import ProbabilityEngine


class SportsArbitrageEngine:
    """Detects arbitrage opportunities in sports markets with outcome mapping."""

    def __init__(self, min_profit_threshold: float = 0.5, min_value_edge: float = 0.05):
        """
        Initialize sports arbitrage engine.

        Args:
            min_profit_threshold: Minimum profit percentage for arbitrage (e.g., 0.5 = 0.5%)
            min_value_edge: Minimum probability edge for value bets (e.g., 0.05 = 5%)
        """
        self.min_profit_threshold = min_profit_threshold
        self.min_value_edge = min_value_edge
        self.logger = setup_logger("sports_arbitrage_engine")
        self.probability_engine = ProbabilityEngine(
            min_value_edge=min_value_edge,
            min_arbitrage_profit=min_profit_threshold
        )

    def _calculate_arbitrage(
        self,
        odds_a: float,
        odds_b: float
    ) -> Optional[Dict]:
        """
        Calculate arbitrage opportunity between two odds.

        Args:
            odds_a: Decimal odds on platform A
            odds_b: Decimal odds on platform B

        Returns:
            Arbitrage data dictionary or None if no arbitrage
        """
        if odds_a <= 1.0 or odds_b <= 1.0:
            return None

        # Calculate implied probabilities
        prob_a = 1.0 / odds_a
        prob_b = 1.0 / odds_b

        # Check for arbitrage (sum of probabilities < 1)
        total_prob = prob_a + prob_b

        if total_prob >= 1.0:
            return None

        # Calculate profit percentage
        profit_percentage = ((1.0 - total_prob) / total_prob) * 100

        if profit_percentage < self.min_profit_threshold:
            return None

        return {
            'odds_a': odds_a,
            'odds_b': odds_b,
            'prob_a': prob_a,
            'prob_b': prob_b,
            'total_prob': total_prob,
            'profit_percentage': profit_percentage
        }

    def detect_sports_arbitrage(self, matched_events: List[Dict]) -> List[Dict]:
        """
        Detect arbitrage and value opportunities from event-level matches.
        
        Uses probability-based comparison for accurate detection.

        Args:
            matched_events: List of event-level matches (from EventMatcher)

        Returns:
            List of arbitrage and value opportunities
        """
        # Use the probability engine for detection
        opportunities = self.probability_engine.detect_value_opportunities(matched_events)
        
        # Convert to format expected by the rest of the system
        formatted_opportunities = []
        
        for opp in opportunities:
            if opp['type'] == 'arbitrage':
                # For arbitrage, we need to determine the OPPOSITE outcome for Cloudbet
                team = opp['team']
                cb_teams = opp.get('cb_teams')
                
                # Use outcome_b if already set (from probability_engine), otherwise calculate
                existing_outcome_b = opp.get('outcome_b', {})
                if isinstance(existing_outcome_b, dict) and 'name' in existing_outcome_b:
                    # Already has team name from probability_engine
                    opposite_team = existing_outcome_b.get('name')
                elif cb_teams and isinstance(cb_teams, (tuple, list)) and len(cb_teams) >= 2:
                    # Calculate opposite team from cb_teams tuple
                    if cb_teams[0] == team:
                        opposite_team = cb_teams[1]
                    elif cb_teams[1] == team:
                        opposite_team = cb_teams[0]
                    else:
                        # Team doesn't match either, use the one that's not arb_team
                        opposite_team = cb_teams[1] if cb_teams[0] == team else cb_teams[0]
                else:
                    # Fallback: try to get from market_b or use generic
                    opposite_team = "Opposite Team"
                    self.logger.warning(
                        f"Could not determine opposite team for {opp['market_name']}. "
                        f"cb_teams={cb_teams}, team={team}"
                    )
                
                # Format for arbitrage opportunities
                formatted = {
                    'market_name': opp['market_name'],
                    'outcome_name': f"{team} (PM) vs {opposite_team} (CB)",
                    'platform_a': opp['platform_a'],
                    'platform_b': opp['platform_b'],
                    'market_a': opp['market_a'],
                    'market_b': opp['market_b'],
                    'cb_teams': cb_teams or (),
                    'outcome_a': {
                        'name': opp.get('outcome_a', {}).get('name', f"{team} {opp.get('pm_outcome', 'YES')}"),
                        'odds': opp['pm_odds']
                    },
                    'outcome_b': {
                        'name': opposite_team,  # Use calculated/extracted opposite team
                        'odds': opp['cb_odds'],
                        # Preserve metadata from probability_engine
                        'event_id': existing_outcome_b.get('event_id'),
                        'market_url': existing_outcome_b.get('market_url'),
                        'selection_id': existing_outcome_b.get('selection_id')
                    },
                    'odds_a': opp['pm_odds'],
                    'odds_b': opp['cb_odds'],
                    'profit_percentage': opp['profit_percentage'],
                    'sport_key': opp.get('sport_key', 'unknown'),
                    'competition_key': opp['market_b'].get('competition_key', 'unknown'),
                    'start_time': opp.get('start_time'),
                    'type': 'arbitrage'
                }
            else:
                # Format for value edge opportunities
                # For value edge, we're betting the SAME TEAM on both platforms
                # but one platform has better odds
                
                # Determine which team from cb_teams matches for value edge
                team_for_value = opp['team']  # This is the team with the value edge
                cb_teams = opp.get('cb_teams') or None
                
                formatted = {
                    'market_name': opp['market_name'],
                    'outcome_name': f"{team_for_value} (Value Edge)",
                    'platform_a': opp['platform_a'],
                    'platform_b': opp['platform_b'],
                    'market_a': opp['market_a'],
                    'market_b': opp['market_b'],
                    'cb_teams': cb_teams,
                    'outcome_a': {
                        'name': team_for_value,  # Same team on both platforms
                        'odds': opp['pm_odds']
                    },
                    'outcome_b': {
                        'name': team_for_value,  # Same team (correct for value edge)
                        'odds': opp['cb_odds']
                    },
                    'odds_a': opp['pm_odds'],
                    'odds_b': opp['cb_odds'],
                    'profit_percentage': abs(opp['edge_percentage']),  # Use edge as "profit"
                    'edge_percentage': opp['edge_percentage'],
                    'better_platform': opp['better_platform'],
                    'sport_key': opp.get('sport_key', 'unknown'),
                    'competition_key': opp['market_b'].get('competition_key', 'unknown'),
                    'start_time': opp.get('start_time'),
                    'type': 'value_edge'
                }
            
            formatted_opportunities.append(formatted)
        
        return formatted_opportunities
