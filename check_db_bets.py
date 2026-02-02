import sqlite3
import os

db_path = "data/arbitrage_events.db"
if not os.path.exists(db_path):
    print(f"Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

try:
    cursor.execute("SELECT * FROM arbitrage_events WHERE bet_placed = 1 ORDER BY timestamp DESC")
    rows = cursor.fetchall()
    print(f"Found {len(rows)} bets taken in database.")
    for row in rows:
        print(f"ID: {row['id']} | Time: {row['timestamp']} | Market: {row['market_name']} | Profit: {row['profit_percentage']}% | PnL: {row['realized_pnl']}")
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
