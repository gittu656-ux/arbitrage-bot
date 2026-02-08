from src.database import ArbitrageDatabase

db = ArbitrageDatabase('data/arbitrage_events.db')
opps = db.get_recent_opportunities(10)

print(f"\nTotal recent opportunities: {len(opps)}")
print("\nRecent 10 opportunities:")
print("-" * 80)

for i, opp in enumerate(opps, 1):
    print(f"{i}. Market: {opp['market_name']}")
    print(f"   Profit: {opp['profit_percentage']:.2f}%")
    print(f"   Platforms: {opp['platform_a']} / {opp['platform_b']}")
    print(f"   Bet placed: {'Yes' if opp.get('bet_placed', 0) else 'No'}")
    print(f"   Timestamp: {opp['timestamp']}")
    print()
