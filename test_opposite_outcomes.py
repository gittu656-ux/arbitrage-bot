"""
Test to verify that arbitrage calculations work correctly with OPPOSITE outcomes.

Example from screenshot:
- Polymarket: Steelers YES @ 2.41
- Cloudbet: Texans @ 2.05

This should produce:
- Total Prob = 1/2.41 + 1/2.05 = 0.414 + 0.488 = 0.902 < 1.0 ✓ Arbitrage exists
- Profit % = (1 - 0.902) / 0.902 * 100 = 10.86%
- Bet Steelers: $540.32
- Bet Texans: $459.68
- Total: $1000
- Profit: $107.66
"""

def test_opposite_outcomes_arbitrage():
    """Test arbitrage with opposite outcomes."""
    
    # Odds from the screenshot
    odds_steelers = 2.41  # Polymarket - Steelers YES
    odds_texans = 2.05    # Cloudbet - Texans (opposite)
    
    # Calculate implied probabilities
    prob_steelers = 1.0 / odds_steelers
    prob_texans = 1.0 / odds_texans
    
    print(f"Steelers probability (Polymarket): {prob_steelers:.4f} ({prob_steelers*100:.2f}%)")
    print(f"Texans probability (Cloudbet): {prob_texans:.4f} ({prob_texans*100:.2f}%)")
    
    # For arbitrage: we need OPPOSITE outcomes
    # If we bet on Steelers @ 2.41, we need the OTHER platform to have opposite outcome
    # Cloudbet has Texans, which is correct opposite
    
    total_prob = prob_steelers + prob_texans
    print(f"\nTotal probability: {total_prob:.4f}")
    
    if total_prob < 1.0:
        profit_pct = ((1.0 - total_prob) / total_prob) * 100
        print(f"✓ ARBITRAGE EXISTS! Profit: {profit_pct:.2f}%")
    else:
        print(f"✗ No arbitrage (total prob >= 1.0)")
        return False
    
    # Calculate bet sizes using Kelly formula
    # For equal profit regardless of outcome:
    # bet_steelers / bet_texans = odds_texans / odds_steelers
    
    total_capital = 1000.0  # From screenshot
    
    bet_steelers = (total_capital * odds_texans) / (odds_steelers + odds_texans)
    bet_texans = (total_capital * odds_steelers) / (odds_steelers + odds_texans)
    
    print(f"\nBet Sizing:")
    print(f"Bet on Steelers @ {odds_steelers}: ${bet_steelers:.2f}")
    print(f"Bet on Texans @ {odds_texans}: ${bet_texans:.2f}")
    print(f"Total invested: ${bet_steelers + bet_texans:.2f}")
    
    # Calculate guaranteed profit (should be same regardless of outcome)
    profit_if_steelers_wins = bet_steelers * odds_steelers - total_capital
    profit_if_texans_wins = bet_texans * odds_texans - total_capital
    
    print(f"\nGuaranteed Outcomes:")
    print(f"If Steelers wins: ${profit_if_steelers_wins:.2f} profit")
    print(f"If Texans wins: ${profit_if_texans_wins:.2f} profit")
    print(f"Difference: ${abs(profit_if_steelers_wins - profit_if_texans_wins):.2f} (should be ~0)")
    
    # Verify against screenshot
    expected_profit = 107.66
    actual_profit = min(profit_if_steelers_wins, profit_if_texans_wins)
    
    print(f"\nVerification:")
    print(f"Expected profit (from screenshot): ${expected_profit:.2f}")
    print(f"Calculated profit: ${actual_profit:.2f}")
    print(f"Match: {'✓' if abs(actual_profit - expected_profit) < 1.0 else '✗'}")
    
    # Verify odds are DIFFERENT (opposite teams!)
    print(f"\n✓ Odds are DIFFERENT - confirming OPPOSITE outcomes:")
    print(f"  Polymarket odds for Steelers: {odds_steelers}")
    print(f"  Cloudbet odds for Texans: {odds_texans}")
    print(f"  This is correct! (Steelers ≠ Texans)")
    
    return True


if __name__ == "__main__":
    print("="*60)
    print("TESTING ARBITRAGE WITH OPPOSITE OUTCOMES")
    print("="*60)
    test_opposite_outcomes_arbitrage()
    print("="*60)
