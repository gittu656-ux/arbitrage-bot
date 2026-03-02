"""
Kelly Criterion bet sizing calculator.
"""
from typing import Dict, Optional
from .logger import setup_logger


class BetSizing:
    """Calculates optimal bet sizes using Kelly Criterion."""
    
    def __init__(self, bankroll: float, kelly_fraction: float = 0.5):
        """
        Initialize bet sizing calculator.
        
        Args:
            bankroll: Total available capital
            kelly_fraction: Kelly multiplier (1.0 = full Kelly, 0.5 = half Kelly, etc.)
        """
        self.bankroll = bankroll
        self.kelly_fraction = kelly_fraction
        self.logger = setup_logger("bet_sizing")
    
    def calculate_kelly(
        self,
        odds_a: float,
        odds_b: float,
        profit_percentage: float,
        odds_c: Optional[float] = None
    ) -> Dict:
        """
        Calculate optimal bet sizes using Kelly Criterion for arbitrage.
        
        For arbitrage, we want to maximize guaranteed profit while ensuring
        we win regardless of outcome.
        
        Args:
            odds_a: Decimal odds on platform A
            odds_b: Decimal odds on platform B
            profit_percentage: Expected profit percentage
            odds_c: Decimal odds on platform C (optional for 3-way arbitrage)
        
        Returns:
            Dictionary with bet sizing information
        """
        # Calculate implied probabilities
        prob_a = 1.0 / odds_a
        prob_b = 1.0 / odds_b
        prob_c = 1.0 / odds_c if odds_c and odds_c > 1.0 else 0.0
        
        total_prob = prob_a + prob_b + prob_c
        
        # Kelly fraction of bankroll
        kelly_bankroll = self.bankroll * self.kelly_fraction
        
        # For equal profit regardless of outcome, bet amounts should be:
        # bet_i = (Total_Capital * prob_i) / Total_Prob
        
        bet_amount_a = (kelly_bankroll * prob_a) / total_prob
        bet_amount_b = (kelly_bankroll * prob_b) / total_prob
        
        result = {
            'bet_amount_a': round(bet_amount_a, 2),
            'bet_amount_b': round(bet_amount_b, 2),
            'total_capital': round(bet_amount_a + bet_amount_b, 2),
            'kelly_fraction_used': self.kelly_fraction,
            'bankroll_used': round(kelly_bankroll, 2)
        }
        
        if odds_c:
            bet_amount_c = (kelly_bankroll * prob_c) / total_prob
            result['bet_amount_c'] = round(bet_amount_c, 2)
            result['total_capital'] = round(bet_amount_a + bet_amount_b + bet_amount_c, 2)
        
        # Calculate guaranteed profit
        # profit = (Total_Capital / Total_Prob) - Total_Capital
        total_bet = result['total_capital']
        guaranteed_profit = (total_bet / total_prob) - total_bet
        
        result['guaranteed_profit'] = round(guaranteed_profit, 2)
        result['profit_percentage'] = round((guaranteed_profit / total_bet) * 100, 2)
        
        return result
    
    def calculate_for_opportunity(self, opportunity: Dict) -> Dict:
        """
        Calculate bet sizing for an arbitrage opportunity.
        
        Args:
            opportunity: Arbitrage opportunity dictionary
        
        Returns:
            Opportunity dictionary with bet sizing added
        """
        odds_a = opportunity['odds_a']
        odds_b = opportunity['odds_b']
        odds_c = opportunity.get('odds_c')
        profit_percentage = opportunity['profit_percentage']
        
        bet_sizing = self.calculate_kelly(odds_a, odds_b, profit_percentage, odds_c=odds_c)
        
        # Add bet sizing to opportunity
        opportunity.update(bet_sizing)
        
        msg = f"Bet A: ${bet_sizing['bet_amount_a']}, Bet B: ${bet_sizing['bet_amount_b']}"
        if 'bet_amount_c' in bet_sizing:
            msg += f", Bet C: ${bet_sizing['bet_amount_c']}"
        
        self.logger.debug(
            f"Bet sizing calculated: {msg}, Profit: ${bet_sizing['guaranteed_profit']}"
        )
        
        return opportunity

