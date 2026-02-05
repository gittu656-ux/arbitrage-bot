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
            return False

        self._reset_daily_counters_if_needed()

        profit_pct = opportunity.get("profit_percentage", 0.0)
        if profit_pct < self.cfg.min_profit_threshold:
            return False

        # STRICTLY Arbitrage Only (Both bets) as requested
        if opportunity.get('type') != 'arbitrage':
            # self.logger.debug("Skipping non-arbitrage opportunity (Value Edge)") 
            return False

        if self.cfg.max_bets_per_day and self._bets_today >= self.cfg.max_bets_per_day:
            self.logger.info(
                f"Autobet skipped - reached max bets per day ({self.cfg.max_bets_per_day})"
            )
            return False

        # For arbitrage, guaranteed_profit should be >= 0. We still track a
        # "loss" bucket to guard against any operational issues.
        if self.cfg.daily_loss_limit > 0 and self._loss_today <= -self.cfg.daily_loss_limit:
            self.logger.warning(
                "Autobet disabled for today - daily loss limit reached "
                f"({self._loss_today:.2f} <= -{self.cfg.daily_loss_limit:.2f})"
            )
            return False

        return True

    def autobet_opportunity(self, opportunity: Dict, db_id: int) -> None:
        """
        Attempt to place bets on both platforms, only marking as successful if both succeed.
        """
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

        self.logger.info(
            f"AUTOBET ATTEMPT: {opportunity.get('market_name')} | "
            f"{opportunity.get('platform_a')}/{opportunity.get('platform_b')} | "
            f"Stake=${total_capital:.2f} | Expected PnL=${guaranteed_profit:.2f}"
        )

        # REAL EXECUTION (OPTIONAL)
        if self.cfg.real_execution:
            import asyncio
            # Wait for execution to complete and check if successful
            asyncio.create_task(self._execute_and_record(opportunity, db_id, guaranteed_profit))
        else:
            # Simulation mode - mark as taken immediately
            self.db.mark_bet_placed(
                opportunity_id=db_id,
                realized_pnl=guaranteed_profit,
            )
            self._bets_today += 1
            if guaranteed_profit < 0:
                self._loss_today += guaranteed_profit
            self.logger.info(f"SIMULATION: Bet marked as taken (real_execution=false)")

    async def _execute_and_record(self, opportunity: Dict, db_id: int, guaranteed_profit: float):
        """Execute bets and only record if both succeed."""
        success = await self._execute_real_bets(opportunity)
        
        if success:
            # Both bets placed successfully - mark in database
            self.db.mark_bet_placed(
                opportunity_id=db_id,
                realized_pnl=guaranteed_profit,
            )
            self._bets_today += 1
            if guaranteed_profit < 0:
                self._loss_today += guaranteed_profit
            
            self.logger.info(
                f"AUTOBET SUCCESS: {opportunity.get('market_name')} | "
                f"Both bets placed | PnL=${guaranteed_profit:.2f}"
            )
        else:
            self.logger.error(
                f"AUTOBET FAILED: {opportunity.get('market_name')} | "
                f"One or both bets failed - NOT marked as taken"
            )

    async def _execute_real_bets(self, opportunity: Dict) -> bool:
        """Execute real bets on both platforms. Returns True if both succeed."""
        try:
            # Try to initialize executors if not already done
            if not self.pm_executor or not self.cb_executor:
                # This requires refactoring how AutobetEngine is initialized
                # For now, we'll try to find keys in env
                import os
                pm_key = os.getenv("POLYMARKET_PRIVATE_KEY")
                cb_key = os.getenv("CLOUDBET_API_KEY")
                
                if pm_key and not self.pm_executor:
                    self.pm_executor = PolymarketExecutor(pm_key)
                if cb_key and not self.cb_executor:
                    self.cb_executor = CloudbetExecutor(cb_key)

            if not self.pm_executor or not self.cb_executor:
                self.logger.error("Executors not initialized - missing API keys.")
                return False

            platform_a = opportunity.get('platform_a')
            platform_b = opportunity.get('platform_b')
            
            # Extract IDs and parameters
            # Platform A
            market_a_meta = opportunity.get('market_a', {}).get('metadata', {})
            outcome_a_name = opportunity.get('outcome_a', {}).get('name')
            token_id_a = market_a_meta.get('token_ids', {}).get(outcome_a_name)
            odds_a = opportunity.get('odds_a')
            stake_a = opportunity.get('bet_amount_a')

            # Platform B (Cloudbet)
            outcome_b = opportunity.get('outcome_b', {})
            outcome_b_name = outcome_b.get('name')
            odds_b = opportunity.get('odds_b')
            stake_b = opportunity.get('bet_amount_b')
            
            # Check for V3 metadata (Sports Event Matcher)
            event_id_b = outcome_b.get('event_id')
            market_url_b = outcome_b.get('market_url')
            
            # Fallback for Legacy Matcher (Regular Market Matcher)
            if not event_id_b or not market_url_b:
                market_b_meta = opportunity.get('market_b', {}).get('metadata', {})
                # Try to get selection ID and map it
                selection_id_b = market_b_meta.get('selection_ids', {}).get(outcome_b_name)
                if selection_id_b:
                    # For legacy, we might still need to construct a marketUrl or use a different endpoint
                    # But since V3 is our primary now, we prioritize event_id/market_url
                    pass

            self.logger.info(f"STARTING REAL EXECUTION for arbitrage: {opportunity.get('market_name')}")

            # Execution logic: Sequence matters. 
            # 1. Place bet on Platform B (Cloudbet)
            success_b = False
            if event_id_b and market_url_b:
                self.logger.info(f"Executing Cloudbet V3 Bet: {market_url_b} on event {event_id_b}")
                resp_b = await self.cb_executor.place_bet(
                    event_id=event_id_b, 
                    market_url=market_url_b, 
                    odds=odds_b, 
                    stake=stake_b
                )
                if resp_b:
                    success_b = True
                    self.logger.info("Successfully placed bet on Cloudbet V3")
                else:
                    self.logger.error("Failed to place bet on Cloudbet. ABORTING hedge.")
                    return False
            else:
                self.logger.error("Cloudbet execution failed: Missing event_id or market_url")
                return False

            # 2. Place bet on Platform A (Polymarket)
            success_a = False
            if success_b and token_id_a:
                # Convert decimal odds to price (e.g. 2.0 -> 0.5)
                price_a = 1.0 / odds_a
                self.logger.info(f"Executing Polymarket hedge: {outcome_a_name} @ {price_a:.4f}")
                resp_a = await self.pm_executor.place_order(token_id_a, price_a, "BUY", stake_a)
                if resp_a:
                    success_a = True
                    self.logger.info("Successfully placed hedge order on Polymarket")
                else:
                    self.logger.critical(f"FAILED TO HEDGE on Polymarket! You have an unhedged bet on Cloudbet for ${stake_b}")
                    return False
            
            # Return True only if both bets succeeded
            return success_a and success_b
            
        except Exception as e:
            self.logger.error(f"Error in real execution: {e}", exc_info=True)
            return False


