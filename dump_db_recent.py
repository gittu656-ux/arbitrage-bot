import sqlite3
import json

db_path = "data/arbitrage_events.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.execute("SELECT * FROM arbitrage_events ORDER BY id DESC LIMIT 20")
rows = [dict(row) for row in cursor.fetchall()]
for row in rows:
    print(f"ID: {row['id']} | {row['timestamp']} | {row['market_name']}")
    print(f"  A: {row['platform_a']} @ {row['odds_a']} | B: {row['platform_b']} @ {row['odds_b']} | Profit: {row['profit_percentage']}%")
    print("-" * 40)
conn.close()
