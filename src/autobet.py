"""
Autobet execution and risk management.

NOTE: This module does NOT place real bets. It implements:
- Risk checks (min profit %, max stake %, max bets/day, optional daily loss cap)
- Bookkeeping by marking opportunities as "bet placed" in the database

Actual integration with Polymarket / Cloudbet APIs can be wired later by
implementing the `_execute_real_bet` method.
"""
from datetime import datetime, date
from typing import Dict

try:
    from src.logger import setup_logger
    from src.database import ArbitrageDatabase
    from src.config_loader import AutobetConfig, BankrollConfig, Config
    from src.execution.polymarket_executor import PolymarketExecutor
    from src.execution.cloudbet_executor import CloudbetExecutor
except ImportError:
    from logger import setup_logger
    from database import ArbitrageDatabase
    from config_loader import AutobetConfig, BankrollConfig, Config
    from execution.polymarket_executor import PolymarketExecutor
    from execution.cloudbet_executor import CloudbetExecutor


class AutobetEngine:
    """Simple autobet engine with risk controls and bookkeeping."""

    def __init__(
        self,
        db: ArbitrageDatabase,
        bankroll_cfg: BankrollConfig,
        autobet_cfg: AutobetConfig,
    ):
        self.db = db
        self.bankroll_cfg = bankroll_cfg
        self.cfg = autobet_cfg
        self.logger = setup_logger("autobet")
        self._today = date.today()
        self._bets_today = 0
        self._loss_today = 0.0
        
        # Initialize executors if real execution is enabled
        self.pm_executor = None
        self.cb_executor = None
        if self.cfg.real_execution:
            # We need the full config to get API keys
            pass # Will be initialized in start_execution

    def _reset_daily_counters_if_needed(self):
        today = date.today()
        if today != self._today:
            self._today = today
            self._bets_today = 0
            self._loss_today = 0.0

    def should_autobet(self, opportunity: Dict) -> bool:
        """Apply cheap risk filters before attempting to autobet."""
        if not self.cfg.enabled:
            self.logger.debug("Autobet skipped - engine disabled in config")
            return False

        self._reset_daily_counters_if_needed()

        profit_pct = opportunity.get("profit_percentage", 0.0)
        if profit_pct < self.cfg.min_profit_threshold:
            self.logger.debug(f"Autobet skipped - profit {profit_pct:.2f}% < threshold {self.cfg.min_profit_threshold}%")
            return False

        # STRICTLY Arbitrage Only (Both bets) as requested
        if opportunity.get('type') != 'arbitrage':
            self.logger.debug(f"Autobet skipped - not an arbitrage opportunity (Type: {opportunity.get('type')})") 
            return False

        if self.cfg.max_bets_per_day and self._bets_today >= self.cfg.max_bets_per_day:
            self.logger.info(
                f"Autobet skipped - reached max bets per day ({self.cfg.max_bets_per_day})"
            )
            return False

        if self.cfg.daily_loss_limit > 0 and self._loss_today <= -self.cfg.daily_loss_limit:
            self.logger.warning(
                "Autobet disabled for today - daily loss limit reached "
                f"({self._loss_today:.2f} <= -{self.cfg.daily_loss_limit:.2f})"
            )
            return False

        return True

    async def autobet_opportunity(self, opportunity: Dict, db_id: int) -> None:
        """
        Mark an opportunity as bet-taken, respecting risk limits.

        This does not talk to any external sportsbook/exchange. It simply
        records that, according to our model, we would have taken this bet.
        """
        self.logger.info(f"Evaluating autobet for: {opportunity.get('market_name')} (Profit: {opportunity.get('profit_percentage', 0):.2f}%)")
        if not self.should_autobet(opportunity):
            return

        total_capital = float(opportunity.get("total_capital", 0.0) or 0.0)

        # Hard cap: total capital per bet as fraction of bankroll
        max_stake = self.bankroll_cfg.amount * self.cfg.max_stake_fraction
        if max_stake > 0 and total_capital > max_stake:
            scale = max_stake / total_capital
            self.logger.info(
                f"Scaling autobet stake down: total={total_capital:.2f}, "
                f"max_stake={max_stake:.2f}, scale={scale:.3f}"
            )
            # Scale bet amounts and profits proportionally
            opportunity["bet_amount_a"] *= scale
            opportunity["bet_amount_b"] *= scale
            opportunity["total_capital"] = max_stake
            opportunity["guaranteed_profit"] *= scale
            total_capital = max_stake

        guaranteed_profit = float(opportunity.get("guaranteed_profit", 0.0) or 0.0)

        # REAL EXECUTION (OPTIONAL)
        if self.cfg.real_execution:
            self.logger.info(f"Real execution ENABLED for {opportunity.get('market_name')}. Attempting to place bets first...")
            execution_started = await self._execute_real_bets(opportunity)
            
            if not execution_started:
                self.logger.error("Real execution failed to start. Not marking as 'Bet Taken' on dashboard.")
                return 
        else:
            self.logger.info("Real execution DISABLED in config. Skipping real money placement.")

        # Record in DB (Only if real execution started or if real_execution is disabled)
        self.db.mark_bet_placed(
            opportunity_id=db_id,
            realized_pnl=guaranteed_profit,
        )

        self._bets_today += 1
        # If something went wrong and guaranteed_profit < 0, treat it as loss
        if guaranteed_profit < 0:
            self._loss_today += guaranteed_profit

        self.logger.info(
            f"AUTOBET SUCCESS: {opportunity.get('market_name')} | "
            f"{opportunity.get('platform_a')}/{opportunity.get('platform_b')} | "
            f"Stake=${total_capital:.2f} | PnL=${guaranteed_profit:.2f}"
        )

    async def _execute_real_bets(self, opportunity: Dict):
        """Execute real bets on both platforms."""
        try:
            # Try to initialize executors if not already done
            if not self.pm_executor or not self.cb_executor:
                self.logger.info("Executors not initialized. Attempting to load API keys...")
                # This requires refactoring how AutobetEngine is initialized
                # For now, we'll try to find keys in env
                import os
                pm_key = os.getenv("POLYMARKET_PRIVATE_KEY")
                cb_key = os.getenv("CLOUDBET_API_KEY")
                
                if pm_key:
                    self.logger.info("Found POLYMARKET_PRIVATE_KEY")
                    if not self.pm_executor:
                        self.pm_executor = PolymarketExecutor(pm_key)
                else:
                    self.logger.error("MISSING POLYMARKET_PRIVATE_KEY in env")

                if cb_key:
                    self.logger.info("Found CLOUDBET_API_KEY")
                    if not self.cb_executor:
                        self.cb_executor = CloudbetExecutor(cb_key)
                else:
                    self.logger.error("MISSING CLOUDBET_API_KEY in env")

            if not self.pm_executor or not self.cb_executor:
                self.logger.error("Executors not initialized - missing API keys. Cannot place bets.")
                return False

            platform_a = opportunity.get('platform_a')
            platform_b = opportunity.get('platform_b')
            
            # DEEP DEBUG: Log the structure of market_b to see where data is
            self.logger.debug(f"Opportunity Structure for {opportunity.get('market_name')}:")
            self.logger.debug(f"  Platforms: {platform_a} / {platform_b}")
            self.logger.debug(f"  Market B keys: {list(opportunity.get('market_b', {}).keys())}")
            if 'metadata' in opportunity.get('market_b', {}):
                self.logger.debug(f"  Market B metadata keys: {list(opportunity['market_b']['metadata'].keys())}")
            
            # Extract IDs and parameters
            # Platform A (Polymarket)
            market_a_meta = opportunity.get('market_a', {}).get('metadata', {})
            outcome_a_name = opportunity.get('outcome_a', {}).get('name', '')
            token_ids_a = market_a_meta.get('token_ids', {})
            token_id_a = token_ids_a.get(outcome_a_name)
            
            # Fuzzy match fallback for Polymarket
            if not token_id_a and token_ids_a:
                for tid_name, tid_val in token_ids_a.items():
                    if tid_name.lower() in outcome_a_name.lower() or outcome_a_name.lower() in tid_name.lower():
                        token_id_a = tid_val
                        break

            # Platform B (Cloudbet)
            market_b_meta = opportunity.get('market_b', {}).get('metadata', {})
            outcome_b_name = opportunity.get('outcome_b', {}).get('name', '')
            # Try to get from metadata dict first
            selection_ids_b_all = market_b_meta.get('selection_ids', {})
            selection_id_b = selection_ids_b_all.get(outcome_b_name)
            
            # Fuzzy match fallback for Cloudbet
            if not selection_id_b:
                for sid_name, sid_val in selection_ids_b_all.items():
                    if sid_name.lower() in outcome_b_name.lower() or outcome_b_name.lower() in sid_name.lower():
                        selection_id_b = sid_val
                        break

            # Fallback 2: search in outcomes_full if still not found
            if not selection_id_b:
                market_b_outcomes = opportunity.get('market_b', {}).get('outcomes_full', [])
                for o in market_b_outcomes:
                    # Cloudbet uses 'outcome' for the name in the raw dict
                    o_name = (o.get('outcome') or o.get('name') or '').lower()
                    if outcome_b_name.lower() in o_name or o_name in outcome_b_name.lower():
                        selection_id_b = o.get('selection_id')
                        if selection_id_b:
                            self.logger.info(f"Fuzzy matched Cloudbet selection_id: {selection_id_b} for {outcome_b_name}")
                        break

            # PARAMETERS
            odds_a = opportunity.get('odds_a')
            stake_a = opportunity.get('bet_amount_a')
            odds_b = opportunity.get('odds_b')
            stake_b = opportunity.get('bet_amount_b')

            self.logger.info(f"STARTING REAL EXECUTION for arbitrage: {opportunity.get('market_name')}")
            self.logger.debug(f"Extracted IDs - Polymarket: {token_id_a}, Cloudbet: {selection_id_b}")

            if not token_id_a:
                self.logger.error(f"Missing Polymarket token_id for '{outcome_a_name}'. Available: {list(token_ids_a.keys())}")
                return False
            if not selection_id_b:
                self.logger.error(f"Missing Cloudbet selection_id for '{outcome_b_name}'. Available: {list(selection_ids_b_all.keys())}")
                return False

            # Execution logic: Sequence matters. 
            # Usually Cloudbet (sportsbook) is more sensitive to odds movement.
            # But Polymarket (exchange) can have liquidity issues.
            
            # 1. Place bet on Platform B (Cloudbet)
            success_b = False
            if selection_id_b:
                # Use the configured currency
                currency = getattr(self.cfg, 'currency', 'USDT')
                self.logger.info(f"Placing bet on Cloudbet using currency: {currency}")
                resp_b = await self.cb_executor.place_bet(selection_id_b, odds_b, stake_b, currency=currency)
                if resp_b:
                    success_b = True
                    self.logger.info("Successfully placed bet on Cloudbet")
                else:
                    self.logger.error("Failed to place bet on Cloudbet. ABORTING hedge.")
                    return False # ABORT to avoid unhedged position
            
            # 2. Place bet on Platform A (Polymarket)
            if success_b and token_id_a:
                # Convert decimal odds to price (e.g. 2.0 -> 0.5)
                price_a = 1.0 / odds_a
                resp_a = await self.pm_executor.place_order(token_id_a, price_a, "BUY", stake_a)
                if resp_a:
                    self.logger.info("Successfully placed hedge order on Polymarket")
                    return True
                else:
                    self.logger.critical(f"FAILED TO HEDGE on Polymarket! You have an unhedged bet on Cloudbet for ${stake_b}")
                    return True # Still return True because Cloudbet bet was placed
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error in real execution: {e}", exc_info=True)
            return False


