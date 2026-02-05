"""
SQLite database for storing arbitrage events.
"""
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from contextlib import contextmanager
import hashlib
import json


class ArbitrageDatabase:
    """Database manager for arbitrage events."""
    
    def __init__(self, db_path: str):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Create database tables if they don't exist."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS arbitrage_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    platform_a TEXT NOT NULL,
                    platform_b TEXT NOT NULL,
                    odds_a REAL NOT NULL,
                    odds_b REAL NOT NULL,
                    profit_percentage REAL NOT NULL,
                    bet_amount_a REAL NOT NULL,
                    bet_amount_b REAL NOT NULL,
                    total_capital REAL NOT NULL,
                    guaranteed_profit REAL NOT NULL,
                    opportunity_hash TEXT UNIQUE NOT NULL,
                    alert_sent INTEGER DEFAULT 0,
                    bet_placed INTEGER DEFAULT 0,
                    realized_pnl REAL DEFAULT 0.0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_opportunity_hash 
                ON arbitrage_events(opportunity_hash)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON arbitrage_events(timestamp)
            """)

            # Backwards-compatible schema upgrade: add bet_placed/realized_pnl if missing
            try:
                conn.execute("ALTER TABLE arbitrage_events ADD COLUMN bet_placed INTEGER DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE arbitrage_events ADD COLUMN realized_pnl REAL DEFAULT 0.0")
            except Exception:
                pass

            conn.commit()
    
    @contextmanager
    def _get_connection(self):
        """Get database connection with proper cleanup."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def _generate_opportunity_hash(
        self,
        market_name: str,
        platform_a: str,
        platform_b: str,
        odds_a: float,
        odds_b: float
    ) -> str:
        """
        Generate a unique hash for an arbitrage opportunity.
        
        Args:
            market_name: Name of the market
            platform_a: First platform name
            platform_b: Second platform name
            odds_a: Odds on platform A
            odds_b: Odds on platform B
        
        Returns:
            SHA256 hash string
        """
        data = f"{market_name}|{platform_a}|{platform_b}|{odds_a:.6f}|{odds_b:.6f}"
        return hashlib.sha256(data.encode()).hexdigest()
    
    def is_duplicate(
        self,
        market_name: str,
        platform_a: str,
        platform_b: str,
        odds_a: float,
        odds_b: float
    ) -> bool:
        """
        Check if an arbitrage opportunity has already been recorded.
        Now allows re-processing if odds have changed significantly (>5%).
        
        Args:
            market_name: Name of the market
            platform_a: First platform name
            platform_b: Second platform name
            odds_a: Odds on platform A
            odds_b: Odds on platform B
        
        Returns:
            True if duplicate (same odds within 5%), False otherwise
        """
        # First check exact hash match (fastest path)
        opportunity_hash = self._generate_opportunity_hash(
            market_name, platform_a, platform_b, odds_a, odds_b
        )
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT bet_placed FROM arbitrage_events WHERE opportunity_hash = ?",
                (opportunity_hash,)
            )
            row = cursor.fetchone()
            if row is not None:
                # If bet was already placed, it's a true duplicate
                if row[0] == 1:
                    return True
                # If bet was NOT placed (bet_placed=0), allow re-processing 
                # to give autobet another chance (e.g. if API was down or keys missing)
                return False
            
            # Check if similar opportunity exists (same market + platforms, different odds)
            # Allow re-processing if odds changed by more than 5%
            cursor = conn.execute(
                """SELECT odds_a, odds_b FROM arbitrage_events 
                   WHERE market_name = ? AND platform_a = ? AND platform_b = ?
                   ORDER BY timestamp DESC LIMIT 1""",
                (market_name, platform_a, platform_b)
            )
            result = cursor.fetchone()
            
            if result is None:
                return False  # No previous record for this market
            
            prev_odds_a, prev_odds_b = result
            
            # Calculate percentage change in odds
            odds_a_change = abs(odds_a - prev_odds_a) / prev_odds_a if prev_odds_a > 0 else 1.0
            odds_b_change = abs(odds_b - prev_odds_b) / prev_odds_b if prev_odds_b > 0 else 1.0
            
            # If either odds changed by more than 5%, treat as new opportunity
            if odds_a_change > 0.05 or odds_b_change > 0.05:
                return False  # Significant change - allow re-processing
            
            return True  # Odds too similar - skip as duplicate
    
    def insert_opportunity(
        self,
        market_name: str,
        platform_a: str,
        platform_b: str,
        odds_a: float,
        odds_b: float,
        profit_percentage: float,
        bet_amount_a: float,
        bet_amount_b: float,
        total_capital: float,
        guaranteed_profit: float,
        alert_sent: bool = False
    ) -> Optional[int]:
        """
        Insert a new arbitrage opportunity into the database.
        
        Args:
            market_name: Name of the market
            platform_a: First platform name
            platform_b: Second platform name
            odds_a: Odds on platform A
            odds_b: Odds on platform B
            profit_percentage: Profit percentage
            bet_amount_a: Bet amount on platform A
            bet_amount_b: Bet amount on platform B
            total_capital: Total capital required
            guaranteed_profit: Guaranteed profit amount
            alert_sent: Whether alert was sent
        
        Returns:
            Inserted row ID, or None if duplicate
        """
        opportunity_hash = self._generate_opportunity_hash(
            market_name, platform_a, platform_b, odds_a, odds_b
        )
        
        # Check if this exact opportunity exists and its status
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT id, bet_placed FROM arbitrage_events WHERE opportunity_hash = ?",
                (opportunity_hash,)
            )
            row = cursor.fetchone()
            if row is not None:
                existing_id, bet_placed = row
                if bet_placed == 1:
                    return None  # Truly a duplicate of a completed bet
                else:
                    return existing_id  # Return existing ID to allow retry of unplaced bet
        
        # If it doesn't exist, check for similar (odds change) logic in is_duplicate
        if self.is_duplicate(market_name, platform_a, platform_b, odds_a, odds_b):
            return None
        
        timestamp = datetime.utcnow().isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO arbitrage_events (
                    timestamp, market_name, platform_a, platform_b,
                    odds_a, odds_b, profit_percentage,
                    bet_amount_a, bet_amount_b, total_capital,
                    guaranteed_profit, opportunity_hash, alert_sent
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp, market_name, platform_a, platform_b,
                odds_a, odds_b, profit_percentage,
                bet_amount_a, bet_amount_b, total_capital,
                guaranteed_profit, opportunity_hash, 1 if alert_sent else 0
            ))
            conn.commit()
            return cursor.lastrowid
    
    def mark_alert_sent(self, opportunity_id: int):
        """
        Mark an opportunity as having an alert sent.
        
        Args:
            opportunity_id: Database row ID
        """
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE arbitrage_events SET alert_sent = 1 WHERE id = ?",
                (opportunity_id,)
            )
            conn.commit()
    
    def get_recent_opportunities(self, limit: int = 10) -> list:
        """
        Get recent arbitrage opportunities.
        
        Args:
            limit: Maximum number of records to return
        
        Returns:
            List of opportunity dictionaries
        """
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM arbitrage_events
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_opportunities(self, limit: int = 100) -> list:
        """
        Get all arbitrage opportunities.
        
        Args:
            limit: Maximum number of records to return
        
        Returns:
            List of opportunity dictionaries
        """
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM arbitrage_events
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_statistics(self) -> Dict:
        """
        Get database statistics for dashboard.
        
        Returns:
            Dictionary with statistics
        """
        with self._get_connection() as conn:
            # Total opportunities
            total = conn.execute("SELECT COUNT(*) FROM arbitrage_events").fetchone()[0]
            
            # Opportunities with alerts sent
            alerted = conn.execute(
                "SELECT COUNT(*) FROM arbitrage_events WHERE alert_sent = 1"
            ).fetchone()[0]
            
            # Total profit (sum of all guaranteed profits)
            profit_cursor = conn.execute(
                "SELECT SUM(guaranteed_profit) FROM arbitrage_events"
            )
            total_profit = profit_cursor.fetchone()[0] or 0.0

            # Average profit percentage
            avg_profit = conn.execute(
                "SELECT AVG(profit_percentage) FROM arbitrage_events"
            ).fetchone()[0] or 0.0

            # Bets taken & realized P&L (based on bet_placed flag)
            bets_taken = conn.execute(
                "SELECT COUNT(*) FROM arbitrage_events WHERE bet_placed = 1"
            ).fetchone()[0]
            realized_pnl = conn.execute(
                "SELECT SUM(realized_pnl) FROM arbitrage_events WHERE bet_placed = 1"
            ).fetchone()[0] or 0.0

            # Recent opportunities (last 24 hours)
            from datetime import datetime, timedelta
            yesterday = (datetime.utcnow() - timedelta(days=1)).isoformat()
            recent = conn.execute(
                "SELECT COUNT(*) FROM arbitrage_events WHERE timestamp > ?",
                (yesterday,)
            ).fetchone()[0]
            
            return {
                'total_opportunities': total,
                'alerted_opportunities': alerted,
                'total_profit': round(total_profit, 2),
                'average_profit_percentage': round(avg_profit, 2),
                'recent_opportunities_24h': recent,
                'bets_taken': bets_taken,
                'realized_pnl': round(realized_pnl, 2),
            }

    def mark_bet_placed(self, opportunity_id: int, realized_pnl: float = 0.0):
        """Mark an opportunity as having a bet placed and store realized P&L."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE arbitrage_events "
                "SET bet_placed = 1, realized_pnl = ? "
                "WHERE id = ?",
                (realized_pnl, opportunity_id)
            )
            conn.commit()

