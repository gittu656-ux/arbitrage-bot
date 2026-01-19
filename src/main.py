"""
Main entry point for the arbitrage detection bot.
Production-ready with mock data fallback.
"""
import asyncio
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict

# Add src to path
src_path = Path(__file__).parent
sys.path.insert(0, str(src_path.parent))
sys.path.insert(0, str(src_path))

try:
    from src.config_loader import load_config
    from src.logger import setup_logger
    from src.fetchers.polymarket_fetcher import PolymarketFetcher
    from src.fetchers.cloudbet_fetcher import CloudbetFetcher
    from src.normalizers.market_normalizer import MarketNormalizer
    from src.market_matcher import MarketMatcher
    from src.arbitrage_engine import ArbitrageEngine
    from src.sports_matcher import SportEventMatcher
    from src.sports_arbitrage_engine import SportsArbitrageEngine
    from src.bet_sizing import BetSizing
    from src.telegram_notifier import TelegramNotifier
    from src.database import ArbitrageDatabase
    from src.autobet import AutobetEngine
    from src.mock_data.loader import MockDataLoader
except ImportError:
    from config_loader import load_config
    from logger import setup_logger
    from fetchers.polymarket_fetcher import PolymarketFetcher
    from fetchers.cloudbet_fetcher import CloudbetFetcher
    from normalizers.market_normalizer import MarketNormalizer
    from market_matcher import MarketMatcher
    from arbitrage_engine import ArbitrageEngine
    from sports_matcher import SportEventMatcher
    from sports_arbitrage_engine import SportsArbitrageEngine
    from bet_sizing import BetSizing
    from telegram_notifier import TelegramNotifier
    from database import ArbitrageDatabase
    from autobet import AutobetEngine
    from mock_data.loader import MockDataLoader


class ArbitrageBot:
    """Main arbitrage detection bot with mock data fallback."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the arbitrage bot."""
        self.config = load_config(config_path)
        
        # Setup logger
        self.logger = setup_logger(
            "arbitrage_bot",
            self.config.logging.level,
            self.config.logging.file,
            self.config.logging.max_bytes,
            self.config.logging.backup_count
        )
        
        # Initialize fetchers
        self.polymarket_fetcher = PolymarketFetcher(
            base_url=self.config.apis.polymarket.base_url,
            timeout=self.config.apis.polymarket.timeout,
            retry_attempts=self.config.apis.polymarket.retry_attempts,
            retry_delay=self.config.apis.polymarket.retry_delay,
            debug_api=self.config.debug_api
        )
        
        self.cloudbet_fetcher = CloudbetFetcher(
            api_key=self.config.apis.cloudbet.api_key,
            base_url=self.config.apis.cloudbet.base_url,
            timeout=self.config.apis.cloudbet.timeout,
            retry_attempts=self.config.apis.cloudbet.retry_attempts,
            retry_delay=self.config.apis.cloudbet.retry_delay,
            debug_api=self.config.debug_api
        )
        
        # Initialize normalizer
        self.normalizer = MarketNormalizer()
        
        # Initialize matching and detection
        self.market_matcher = MarketMatcher(
            similarity_threshold=self.config.arbitrage.similarity_threshold
        )

        self.arbitrage_engine = ArbitrageEngine(
            min_profit_threshold=self.config.arbitrage.min_profit_threshold
        )

        # Initialize sports-specific matching and detection
        # Keep old matcher for backward compatibility
        self.sports_matcher = SportEventMatcher(
            similarity_threshold=55.0  # Lower threshold for award/future markets (more flexible)
        )
        
        # NEW: Event-level matcher for proper team + sport + time matching
        try:
            from src.event_matcher import EventMatcher
        except ImportError:
            from event_matcher import EventMatcher
        self.event_matcher = EventMatcher(
            team_similarity_threshold=70.0,  # Lowered to 70% for more matches
            time_window_hours=168  # Increased to 7 days for futures markets
        )

        self.sports_arbitrage_engine = SportsArbitrageEngine(
            min_profit_threshold=self.config.arbitrage.min_profit_threshold,
            min_value_edge=0.05  # 5% minimum edge for value bets
        )
        
        self.bet_sizing = BetSizing(
            bankroll=self.config.bankroll.amount,
            kelly_fraction=self.config.bankroll.kelly_fraction
        )
        
        # Initialize notification and storage
        self.telegram_notifier = TelegramNotifier(
            bot_token=self.config.telegram.bot_token,
            chat_id=self.config.telegram.chat_id,
            channel_id=self.config.telegram.channel_id
        )
        
        self.database = ArbitrageDatabase(self.config.database.path)
        # Autobet engine (can be disabled via config)
        self.autobet_engine = AutobetEngine(
            db=self.database,
            bankroll_cfg=self.config.bankroll,
            autobet_cfg=self.config.autobet,
        )
        
        # Mock data loader
        self.mock_loader = MockDataLoader()
        
        # Check if mock data should be used
        self.use_mock_data = getattr(self.config, 'use_mock_data', False)
        
        self.running = False
        self.logger.info("Arbitrage bot initialized")
    
    def _is_quiet_hours(self) -> bool:
        """Check if current time is within quiet hours."""
        if not self.config.quiet_hours.enabled:
            return False
        
        now = datetime.now()
        current_hour = now.hour
        
        start = self.config.quiet_hours.start_hour
        end = self.config.quiet_hours.end_hour
        
        if start <= end:
            return start <= current_hour < end
        else:
            # Quiet hours span midnight
            return current_hour >= start or current_hour < end
    
    async def _fetch_markets(self) -> tuple:
        """Fetch markets from both platforms with mock fallback."""
        polymarket_markets = []
        cloudbet_markets = []
        cloudbet_raw = []  # Keep raw outcomes for sports matching

        # Try real APIs first
        try:
            self.logger.info("Fetching markets from Polymarket...")
            polymarket_raw = await self.polymarket_fetcher.fetch_all_markets()
            polymarket_markets = self.normalizer.normalize_polymarket(polymarket_raw)
            self.logger.info(f"Fetched {len(polymarket_markets)} Polymarket markets")
        except Exception as e:
            self.logger.warning(f"Error fetching Polymarket: {e}")

        try:
            self.logger.info("Fetching markets from Cloudbet...")
            cloudbet_raw = await self.cloudbet_fetcher.fetch_all_markets()
            cloudbet_markets = self.normalizer.normalize_cloudbet(cloudbet_raw)
            self.logger.info(f"Fetched {len(cloudbet_markets)} Cloudbet markets")
        except Exception as e:
            self.logger.warning(f"Error fetching Cloudbet: {e}")

        # Use mock data if APIs returned empty or use_mock_data is True
        if self.use_mock_data or len(polymarket_markets) == 0 or len(cloudbet_markets) == 0:
            if len(polymarket_markets) == 0:
                self.logger.info("No Polymarket data - using mock data")
                polymarket_raw = self.mock_loader.load_polymarket_mock()
                polymarket_markets = self.normalizer.normalize_polymarket(polymarket_raw)

            if len(cloudbet_markets) == 0:
                self.logger.info("No Cloudbet data - using mock data")
                cloudbet_raw = self.mock_loader.load_cloudbet_mock()
                cloudbet_markets = self.normalizer.normalize_cloudbet(cloudbet_raw)

        return polymarket_markets, cloudbet_markets, cloudbet_raw
    
    async def _run_cycle(self):
        """Execute one detection cycle."""
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting detection cycle...")

            # Fetch markets
            polymarket_markets, cloudbet_markets, cloudbet_raw = await self._fetch_markets()

            if not polymarket_markets or not cloudbet_markets:
                self.logger.warning("Insufficient market data - skipping cycle")
                return

            all_opportunities = []

            # ============================================================
            # METHOD 1: Regular market matching (title-based fuzzy matching)
            # NOTE: This typically finds 0 matches because Polymarket uses futures markets
            # (e.g., "Will Packers win NFC Championship?") while Cloudbet uses game events
            # (e.g., "MIN Vikings v GB Packers"). Event-level matching (METHOD 2) handles this.
            # ============================================================
            matched = self.market_matcher.find_matches(
                polymarket_markets,
                cloudbet_markets,
                platform_a="polymarket",
                platform_b="cloudbet"
            )

            if matched:
                self.logger.info(f"Found {len(matched)} regular market matches (title-based)")
                regular_opportunities = self.arbitrage_engine.detect_arbitrage(matched)
                all_opportunities.extend(regular_opportunities)
            # Don't log "No regular market matches" - this is expected for futures vs games

            # ============================================================
            # METHOD 2: Event-level matching with probability comparison (NEW)
            # ============================================================
            self.logger.info("=" * 60)
            self.logger.info("Event-level matching: Matching by teams, sport, and time...")
            
            # Group Cloudbet outcomes by event (for event matcher)
            cloudbet_events = self.sports_matcher._group_cloudbet_by_event(cloudbet_raw)
            
            # Use event-level matcher (teams + sport + time)
            event_matched = self.event_matcher.match_events(
                polymarket_markets=polymarket_markets,
                cloudbet_events=cloudbet_events
            )

            if event_matched:
                self.logger.info(f"Analyzing {len(event_matched)} event-level matches for value opportunities...")
                sports_opportunities = self.sports_arbitrage_engine.detect_sports_arbitrage(event_matched)
                all_opportunities.extend(sports_opportunities)
            else:
                self.logger.info("No event-level matches found (teams + sport + time)")
            
                # ============================================================
                # METHOD 3: Legacy sports matching (for backward compatibility)
                # NOTE: This is now redundant since event-level matching handles all cases.
                # Keeping it for backward compatibility but with reduced logging.
                # ============================================================
                # Skip legacy matching since event-level matching is working
                # Uncomment below if you want to run legacy matching as fallback
                # self.logger.debug("Legacy sports matching: Skipped (event-level matching is active)")
                pass

            # ============================================================
            # Process all opportunities
            # ============================================================
            self.logger.info("=" * 60)
            total_opps = len(all_opportunities)

            if total_opps == 0:
                self.logger.info("No arbitrage opportunities found (regular or sports)")
                return

            self.logger.info(f"Total opportunities found: {total_opps}")

            # Calculate bet sizing
            self.logger.info(f"Calculating bet sizes for {total_opps} opportunities...")
            sized_opportunities = []
            for opp in all_opportunities:
                sized = self.bet_sizing.calculate_for_opportunity(opp)
                sized_opportunities.append(sized)
                
                # Print opportunity details to console (after bet sizing is calculated)
                self._print_opportunity(sized)

            # Process opportunities
            self.logger.info(f"Processing {len(sized_opportunities)} opportunities for alerts...")
            await self._process_opportunities(sized_opportunities)
            self.logger.info("Finished processing opportunities")

        except Exception as e:
            self.logger.error(f"Error in detection cycle: {e}", exc_info=True)
    
    def _print_opportunity(self, opportunity: Dict):
        """Print opportunity details to console and logs in a formatted way."""
        try:
            market_name = opportunity.get('market_name', 'Unknown')
            profit_pct = opportunity.get('profit_percentage', 0)
            odds_a = opportunity.get('odds_a', 0)
            odds_b = opportunity.get('odds_b', 0)
            outcome_a = opportunity.get('outcome_a', {}).get('name', 'N/A')
            outcome_b = opportunity.get('outcome_b', {}).get('name', 'N/A')
            bet_a = opportunity.get('bet_amount_a', 0)
            bet_b = opportunity.get('bet_amount_b', 0)
            total = opportunity.get('total_capital', 0)
            profit = opportunity.get('guaranteed_profit', 0)
            opp_type = opportunity.get('type', 'arbitrage')
            
            # Build formatted message
            lines = []
            lines.append("="*70)
            if opp_type == 'arbitrage':
                lines.append(f"[ARBITRAGE] {profit_pct:.2f}% Profit Opportunity")
            else:
                edge = opportunity.get('edge_percentage', 0)
                lines.append(f"[VALUE EDGE] {abs(edge):.2f}% Edge")
            lines.append("="*70)
            lines.append(f"Market: {market_name}")
            lines.append(f"Platform A ({opportunity.get('platform_a', 'N/A')}):")
            lines.append(f"  Outcome: {outcome_a}")
            lines.append(f"  Odds: {odds_a:.2f}")
            lines.append(f"  Bet Amount: ${bet_a:.2f}")
            lines.append(f"Platform B ({opportunity.get('platform_b', 'N/A')}):")
            lines.append(f"  Outcome: {outcome_b}")
            lines.append(f"  Odds: {odds_b:.2f}")
            lines.append(f"  Bet Amount: ${bet_b:.2f}")
            lines.append(f"Total Investment: ${total:.2f}")
            lines.append(f"Guaranteed Profit: ${profit:.2f}")
            if opportunity.get('market_a', {}).get('url'):
                lines.append(f"Polymarket URL: {opportunity['market_a']['url']}")
            if opportunity.get('market_b', {}).get('url'):
                lines.append(f"Cloudbet URL: {opportunity['market_b']['url']}")
            lines.append("="*70)
            
            # Log each line so it appears in console and logs
            for line in lines:
                self.logger.info(line)
        except Exception as e:
            self.logger.error(f"Error printing opportunity: {e}")
    
    async def _process_opportunities(self, opportunities: list):
        """Process and alert on arbitrage opportunities."""
        self.logger.info(f"_process_opportunities called with {len(opportunities)} opportunities")
        processed_count = 0
        for opportunity in opportunities:
            try:
                processed_count += 1
                # Check for duplicates
                is_duplicate = self.database.is_duplicate(
                    opportunity['market_name'],
                    opportunity['platform_a'],
                    opportunity['platform_b'],
                    opportunity['odds_a'],
                    opportunity['odds_b']
                )
                
                if is_duplicate:
                    self.logger.debug(f"Skipping duplicate opportunity: {opportunity['market_name']}")
                    continue
                
                self.logger.info(f"Processing new opportunity #{processed_count}: {opportunity['market_name']} ({opportunity.get('profit_percentage', 0):.2f}% profit)")
                
                # Store in database
                self.logger.debug(f"Storing opportunity in database...")
                db_id = self.database.insert_opportunity(
                    market_name=opportunity['market_name'],
                    platform_a=opportunity['platform_a'],
                    platform_b=opportunity['platform_b'],
                    odds_a=opportunity['odds_a'],
                    odds_b=opportunity['odds_b'],
                    profit_percentage=opportunity['profit_percentage'],
                    bet_amount_a=opportunity.get('bet_amount_a', 0),
                    bet_amount_b=opportunity.get('bet_amount_b', 0),
                    total_capital=opportunity.get('total_capital', 0),
                    guaranteed_profit=opportunity.get('guaranteed_profit', 0),
                    alert_sent=False
                )
                
                if db_id is None:
                    self.logger.warning(f"Database insert returned None for: {opportunity['market_name']}")
                    continue
                
                self.logger.info(f"Opportunity stored in database with ID: {db_id}")
                
                # Only send Telegram alert if profit >= 0.5%
                profit_pct = opportunity.get('profit_percentage', 0)
                if profit_pct < 0.5:
                    self.logger.debug(f"Skipping Telegram alert - profit {profit_pct:.2f}% is < 0.5% threshold")
                    continue

                # Autobet (simulation / bookkeeping only) if enabled
                try:
                    self.autobet_engine.autobet_opportunity(opportunity, db_id)
                except Exception as e:
                    self.logger.error(f"Error in autobet engine: {e}", exc_info=True)

                # Send Telegram alert (if not quiet hours)
                if not self._is_quiet_hours():
                    self.logger.info(f"Attempting to send Telegram alert for: {opportunity['market_name']}")
                    try:
                        # Use asyncio.wait_for with timeout
                        alert_sent = await asyncio.wait_for(
                            self.telegram_notifier.send_alert(opportunity, timeout=5),
                            timeout=6  # Slightly longer than send_alert timeout
                        )
                        if alert_sent:
                            self.database.mark_alert_sent(db_id)
                            self.logger.info(f"Telegram alert sent for: {opportunity['market_name']}")
                        else:
                            self.logger.warning(f"Failed to send Telegram alert for: {opportunity['market_name']}")
                    except asyncio.TimeoutError:
                        self.logger.error(f"Telegram alert timed out for: {opportunity['market_name']}")
                    except Exception as e:
                        self.logger.error(f"Error sending Telegram alert: {e}")
                else:
                    self.logger.info("Quiet hours - skipping Telegram alert")
                
            except Exception as e:
                self.logger.error(f"Error processing opportunity: {e}", exc_info=True)
        
        self.logger.info(f"_process_opportunities completed. Processed {processed_count} opportunities.")
    
    async def run(self):
        """Run the bot continuously."""
        self.running = True
        self.logger.info("Starting arbitrage bot...")
        
        # Setup signal handlers
        def signal_handler(sig, frame):
            self.logger.info("Received shutdown signal")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Main loop
        while self.running:
            try:
                await self._run_cycle()
                
                if self.running:
                    self.logger.info(f"Waiting {self.config.arbitrage.polling_interval} seconds until next cycle...")
                    await asyncio.sleep(self.config.arbitrage.polling_interval)
            
            except KeyboardInterrupt:
                self.logger.info("Keyboard interrupt received")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
                if self.running:
                    await asyncio.sleep(10)  # Wait before retrying
        
        # Cleanup
        await self._cleanup()
        self.logger.info("Arbitrage bot stopped")
    
    async def _cleanup(self):
        """Cleanup resources."""
        try:
            await self.polymarket_fetcher.close()
            await self.cloudbet_fetcher.close()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


async def main():
    """Main entry point."""
    bot = ArbitrageBot()
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
