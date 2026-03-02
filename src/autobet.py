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
        self._recent_attempts = {}  # Track recent attempts to prevent loops
        
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

    def _clean_recent_attempts(self):
        """Remove old attempts from the cache (older than 1 hour)."""
        now = datetime.now().timestamp()
        # Keep attempts from last 3600 seconds
        self._recent_attempts = {
            k: v for k, v in self._recent_attempts.items() 
            if now - v < 3600
        }

    def should_autobet(self, opportunity: Dict) -> bool:
        """Apply cheap risk filters before attempting to autobet."""
        if not self.cfg.enabled:
            self.logger.info("Autobet skipped - configuration disabled (cfg.enabled=False)")
            return False

        self._reset_daily_counters_if_needed()
        self._clean_recent_attempts()

        # check for recent attempts to prevent double-betting loops
        market_name = opportunity.get("market_name")
        if market_name in self._recent_attempts:
            last_time = self._recent_attempts[market_name]
            self.logger.warning(
                f"Autobet skipped - recently attempted {market_name} at {datetime.fromtimestamp(last_time)}"
            )
            return False

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

        # Record attempt immediately to prevent race conditions or loops
        self._recent_attempts[opportunity.get("market_name")] = datetime.now().timestamp()

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
            if "bet_amount_c" in opportunity:
                opportunity["bet_amount_c"] *= scale
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
            self.logger.critical(
                f"AUTOBET FAILED: {opportunity.get('market_name')} | "
                f"One or both bets failed. "
                f"Check logs for critical errors. "
                f"Marked in memory as attempted to prevent immediate retry."
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
                proxy = os.getenv("CLOUDBET_PROXY")  # Optional proxy for Railway/cloud hosts
                
                if pm_key and not self.pm_executor:
                    self.pm_executor = PolymarketExecutor(pm_key)
                if cb_key and not self.cb_executor:
                    self.cb_executor = CloudbetExecutor(cb_key, proxy=proxy)

            if not self.pm_executor or not self.cb_executor:
                self.logger.error("Executors not initialized - missing API keys.")
                return False

            platform_a = opportunity.get('platform_a')
            platform_b = opportunity.get('platform_b')
            
            # Extract IDs and parameters
            # Platform A
            market_a_meta = opportunity.get('market_a', {}).get('metadata', {})
            outcome_a_name = opportunity.get('outcome_a', {}).get('name')
            token_ids_map = market_a_meta.get('token_ids', {})
            token_id_a = token_ids_map.get(outcome_a_name)
            
            # Fallback search if token ID missing
            if not token_id_a and outcome_a_name and token_ids_map:
                # Try simple normalization
                for name, tid in token_ids_map.items():
                    if name.lower() == outcome_a_name.lower() or outcome_a_name.lower() in name.lower():
                        token_id_a = tid
                        self.logger.info(f"Metched token ID via fallback: {name} -> {outcome_a_name}")
                        break
            
            if not token_id_a and opportunity.get('pm_outcome'):
                pm_outcome = opportunity.get('pm_outcome')
                token_id_a = token_ids_map.get(pm_outcome)
                if not token_id_a:
                    # Try case-insensitive lookup for YES/NO
                    for name, tid in token_ids_map.items():
                        if name.upper() == pm_outcome.upper():
                            token_id_a = tid
                            break
                if token_id_a:
                    self.logger.info(f"Resolved token ID via pm_outcome: {pm_outcome} -> {token_id_a}")

            if not token_id_a:
               self.logger.error(f"Missing Token ID for {outcome_a_name}. Available: {list(token_ids_map.keys())}. ABORTING.")
               return False
            
            odds_a = opportunity.get('odds_a')
            stake_a = opportunity.get('bet_amount_a')

            # Platform B (Cloudbet)
            outcome_b = opportunity.get('outcome_b', {})
            outcome_b_name = outcome_b.get('name')
            odds_b = opportunity.get('odds_b')
            stake_b = opportunity.get('bet_amount_b')
            
            # Platform C (Optional Draw for 3-way)
            outcome_c = opportunity.get('outcome_c')
            odds_c = opportunity.get('odds_c')
            stake_c = opportunity.get('bet_amount_c')
            
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

            # NEW ORDER: Polymarket (A) -> Cloudbet (B & C)
            # Prediction markets (Polymarket) are usually less liquid, so we hit them first.
            
            # 1. Place bet on Platform A (Polymarket - Main Team)
            success_a = False
            price_a = 1.0 / odds_a
            self.logger.info(f"Executing Polymarket leg first: {outcome_a_name} @ {price_a:.4f} | Stake={stake_a}")
            resp_a = await self.pm_executor.place_order(token_id_a, price_a, "BUY", stake_a)
            
            if resp_a:
                success_a = True
                self.logger.info("Successfully placed first leg on Polymarket")
            else:
                self.logger.error("Failed to place Polymarket bet. ABORTING entire arbitrage.")
                return False

            # 2. Place bet on Platform B (Cloudbet - Opposite Team)
            success_b = False
            if success_a and event_id_b and market_url_b:
                self.logger.info(f"Executing Cloudbet V3 Hedge (B): {market_url_b} | Stake={stake_b}")
                resp_b = await self.cb_executor.place_bet(
                    event_id=event_id_b, 
                    market_url=market_url_b, 
                    odds=odds_b, 
                    stake=stake_b,
                    currency=self.cfg.currency
                )
                if resp_b:
                    success_b = True
                else:
                    self.logger.critical(f"FAILED TO HEDGE on Cloudbet B! You are unhedged on Polymarket!")
                    # In a production bot, we might try to market-sell the PM position here
                    return False
            else:
                self.logger.error("Cloudbet execution failed: Missing event_id or market_url for B")
                return False

            # 3. Place bet on Platform C (Cloudbet - Draw) if needed
            success_c = True
            if success_a and success_b and outcome_c and stake_c:
                success_c = False
                event_id_c = outcome_c.get('event_id')
                market_url_c = outcome_c.get('market_url')
                if event_id_c and market_url_c:
                    self.logger.info(f"Executing Cloudbet V3 Hedge (C - Draw): {market_url_c} | Stake={stake_c}")
                    resp_c = await self.cb_executor.place_bet(
                        event_id=event_id_c, 
                        market_url=market_url_c, 
                        odds=odds_c, 
                        stake=stake_c,
                        currency=self.cfg.currency
                    )
                    if resp_c:
                        success_c = True
                    else:
                        self.logger.critical("FAILED TO HEDGE Draw on Cloudbet! You are partially unhedged!")
                        return False

            # Return True only if ALL required bets succeeded
            return success_a and success_b and success_c
            
        except Exception as e:
            self.logger.error(f"Error in real execution: {e}", exc_info=True)
            return False


