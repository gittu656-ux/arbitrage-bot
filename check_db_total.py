
import sqlite3
from pathlib import Path

db_path = Path("data/arbitrage_events.db")
if not db_path.exists():
    print("Database file does not exist.")
else:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM arbitrage_events")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM arbitrage_events WHERE bet_placed = 1")
    placed = cursor.fetchone()[0]
    
    print(f"Total opportunities found (all time): {total}")
    print(f"Total bets placed/taken: {placed}")
    
    if total > 0:
        print("\nLast 5 opportunities:")
        cursor.execute("SELECT timestamp, market_name, profit_percentage FROM arbitrage_events ORDER BY timestamp DESC LIMIT 5")
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]} ({row[2]}%)")
            
    conn.close()
