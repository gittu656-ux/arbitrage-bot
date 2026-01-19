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

from .logger import setup_logger
from .database import ArbitrageDatabase
from .config_loader import AutobetConfig, BankrollConfig


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
        Mark an opportunity as bet-taken, respecting risk limits.

        This does not talk to any external sportsbook/exchange. It simply
        records that, according to our model, we would have taken this bet.
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

        # Record in DB
        self.db.mark_bet_placed(
            opportunity_id=db_id,
            realized_pnl=guaranteed_profit,
        )

        self._bets_today += 1
        # If something went wrong and guaranteed_profit < 0, treat it as loss
        if guaranteed_profit < 0:
            self._loss_today += guaranteed_profit

        self.logger.info(
            f"AUTOBET TAKEN: {opportunity.get('market_name')} | "
            f"{opportunity.get('platform_a')}/{opportunity.get('platform_b')} | "
            f"Stake=${total_capital:.2f} | PnL=${guaranteed_profit:.2f}"
        )


