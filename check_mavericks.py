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
    cursor.execute("SELECT * FROM arbitrage_events WHERE market_name LIKE '%Mavericks%' ORDER BY timestamp DESC LIMIT 5")
    rows = cursor.fetchall()
    print(f"Found {len(rows)} instances of Mavericks in database.")
    for row in rows:
        print(f"ID: {row['id']} | Time: {row['timestamp']} | Placed: {row['bet_placed']} | Profit: {row['profit_percentage']}%")
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
