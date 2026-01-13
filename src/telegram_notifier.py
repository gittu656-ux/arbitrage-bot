"""
Telegram notification system for arbitrage alerts.

Uses python-telegram-bot 20.x with async/await.
Handles retries, errors gracefully, and never blocks the main event loop.
"""
import asyncio
from typing import Dict, Optional
from datetime import datetime
from telegram import Bot
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError
from telegram.request import HTTPXRequest

from .logger import setup_logger


class TelegramNotifier:
    """
    Sends arbitrage alerts via Telegram to multiple chat IDs/channels.
    
    Features:
    - Async/await (non-blocking)
    - Automatic retries (up to 3 attempts)
    - Graceful error handling
    - Formatted alert messages
    - Multi-recipient support (chat + channel)
    """
    
    def __init__(self, bot_token: str, chat_id: int, channel_id: Optional[int] = None):
        """
        Initialize Telegram notifier.
        
        Args:
            bot_token: Telegram bot token
            chat_id: Telegram chat ID (integer) for notifications
            channel_id: Optional Telegram channel ID for notifications
        """
        self.bot_token = bot_token
        self.chat_id = int(chat_id)  # Ensure it's an integer
        self.channel_id = int(channel_id) if channel_id else None  # Optional channel
        self.recipient_ids = [self.chat_id]  # Start with chat ID
        if self.channel_id:
            self.recipient_ids.append(self.channel_id)
        
        self.logger = setup_logger("telegram_notifier")
        # Initialize Bot with proper timeout settings
        request = HTTPXRequest(
            connect_timeout=10.0,
            read_timeout=10.0,
            write_timeout=10.0
        )
        self.bot = Bot(token=bot_token, request=request)
        self.max_retries = 3
    
    def _format_alert_message(self, opportunity: Dict) -> str:
        """
        Format arbitrage/value edge opportunity as Telegram message.
        
        Two types:
        1. ARBITRAGE: Opposite outcomes on different platforms
           - Polymarket: Team A @ odds1
           - Cloudbet: Team B @ odds2 (OPPOSITE)
        
        2. VALUE EDGE: Same outcome but better odds on one platform
           - Polymarket: Team @ odds1
           - Cloudbet: Team @ odds2 (better odds)
        
        Args:
            opportunity: Opportunity dictionary
        
        Returns:
            Formatted message string with Markdown formatting
        """
        market_name = opportunity.get('market_name', 'Unknown Market')
        profit_pct = opportunity.get('profit_percentage', 0)
        
        platform_a = opportunity.get('platform_a', 'Platform A')
        platform_b = opportunity.get('platform_b', 'Platform B')
        
        # Get outcome names for display
        # For arbitrage, outcome_a and outcome_b should be different teams (opposite outcomes)
        outcome_a_dict = opportunity.get('outcome_a', {})
        outcome_b_dict = opportunity.get('outcome_b', {})
        
        # Try to get team names from outcome dictionaries, fallback to odds keys
        if isinstance(outcome_a_dict, dict) and 'name' in outcome_a_dict:
            outcome_a = outcome_a_dict.get('name', 'YES')
            odds_a = outcome_a_dict.get('odds', opportunity.get('pm_odds', opportunity.get('odds_a', 0)))
        else:
            outcome_a = opportunity.get('team', 'YES')  # Fallback to team name
            odds_a = opportunity.get('pm_odds', opportunity.get('odds_a', 0))
        
        if isinstance(outcome_b_dict, dict) and 'name' in outcome_b_dict:
            outcome_b = outcome_b_dict.get('name', 'NO')
            odds_b = outcome_b_dict.get('odds', opportunity.get('cb_odds', opportunity.get('odds_b', 0)))
        else:
            # For arbitrage, outcome_b should be the opposite team
            # Try to get from cb_teams if available
            cb_teams = opportunity.get('cb_teams')
            team = opportunity.get('team')
            if cb_teams and isinstance(cb_teams, (tuple, list)) and len(cb_teams) >= 2 and team:
                # Calculate opposite team
                if cb_teams[0] == team:
                    outcome_b = cb_teams[1]
                elif cb_teams[1] == team:
                    outcome_b = cb_teams[0]
                else:
                    # Use the other team
                    outcome_b = cb_teams[1] if cb_teams[0] != team else cb_teams[0]
            else:
                # Last resort fallback
                outcome_b = opportunity.get('team', 'Opposite Team')
            odds_b = opportunity.get('cb_odds', opportunity.get('odds_b', 0))
        
        bet_amount_a = opportunity.get('bet_amount_a', 0)
        bet_amount_b = opportunity.get('bet_amount_b', 0)
        total_capital = opportunity.get('total_capital', 0)
        guaranteed_profit = opportunity.get('guaranteed_profit', 0)
        edge_pct = opportunity.get('edge_percentage', 0)
        better_platform = opportunity.get('better_platform', '')
        
        url_a = opportunity.get('market_a', {}).get('url', 'N/A')
        url_b = opportunity.get('market_b', {}).get('url', 'N/A')
        
        # Format platform names for display
        platform_a_display = platform_a.capitalize()
        platform_b_display = platform_b.capitalize()
        
        # Build message based on opportunity type
        opportunity_type = opportunity.get('type', 'arbitrage')
        
        if opportunity_type == 'arbitrage':
            # ARBITRAGE: Opposite outcomes
            # outcome_a and outcome_b are different teams
            message = f"""*ARBITRAGE FOUND ({profit_pct:.2f}%)*

*Market:* {market_name}

*{platform_a_display}:*
{outcome_a} @ {odds_a:.2f} - ${bet_amount_a:.2f}
{url_a}

*{platform_b_display}:*
{outcome_b} @ {odds_b:.2f} - ${bet_amount_b:.2f}
{url_b}

*Total Invested:* ${total_capital:.2f}
*Guaranteed Profit:* ${guaranteed_profit:.2f}"""
        
        else:
            # VALUE EDGE: Same outcome, different odds
            # outcome_a and outcome_b are the SAME TEAM
            better_emoji = "✓" if better_platform == platform_a else ""
            worse_emoji = "✓" if better_platform == platform_b else ""
            
            message = f"""*VALUE EDGE FOUND ({profit_pct:.2f}%)*

*Market:* {market_name}
*Team:* {outcome_a}

*{platform_a_display}:* {better_emoji if better_platform == platform_a else ""}
{outcome_a} @ {odds_a:.2f}
{url_a}

*{platform_b_display}:* {better_emoji if better_platform == platform_b else ""}
{outcome_b} @ {odds_b:.2f}
{url_b}

*Better Odds:* {better_platform.capitalize()}
*Edge:* {edge_pct:.2f}%"""
        
        return message
    
    async def send_message(self, text: str, timeout: int = 5) -> bool:
        """
        Send a text message via Telegram to all configured recipients (chat + channel).
        
        Args:
            text: Message text to send
            timeout: Maximum time to wait for send (seconds)
        
        Returns:
            True if sent successfully to all recipients, False if any failed
        """
        all_success = True
        
        for recipient_id in self.recipient_ids:
            recipient_type = "channel" if recipient_id == self.channel_id else "chat"
            
            for attempt in range(1, self.max_retries + 1):
                try:
                    self.logger.info(f"Telegram send attempt {attempt}/{self.max_retries} to {recipient_type} {recipient_id}...")
                    # Use asyncio.wait_for with timeout
                    result = await asyncio.wait_for(
                        self.bot.send_message(
                            chat_id=recipient_id,
                            text=text,
                            parse_mode='Markdown',
                            disable_web_page_preview=False
                        ),
                        timeout=timeout
                    )
                    self.logger.info(f"Telegram message sent successfully to {recipient_type} (attempt {attempt})")
                    break  # Success, move to next recipient
                    
                except RetryAfter as e:
                    # Rate limited - wait for the specified time
                    wait_time = e.retry_after
                    self.logger.warning(f"Rate limited on {recipient_type}. Waiting {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                    continue
                    
                except (TimedOut, NetworkError) as e:
                    # Network error - retry with exponential backoff
                    if attempt < self.max_retries:
                        wait_time = attempt * 0.5  # 0.5s, 1s, 1.5s
                        self.logger.warning(
                            f"Network error on attempt {attempt}/{self.max_retries} to {recipient_type}: {e}. "
                            f"Retrying in {wait_time}s..."
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        self.logger.error(f"Failed to send Telegram message to {recipient_type} {recipient_id} after {self.max_retries} attempts: {e}")
                        all_success = False
                        break
                        
                except TelegramError as e:
                    # Other Telegram errors - don't retry
                    self.logger.error(f"Telegram error on {recipient_type} {recipient_id}: {e}")
                    all_success = False
                    break
                    
                except asyncio.TimeoutError as e:
                    if attempt < self.max_retries:
                        self.logger.warning(f"Timeout on attempt {attempt}/{self.max_retries} to {recipient_type}. Retrying...")
                        await asyncio.sleep(0.5)
                        continue
                    else:
                        self.logger.error(f"Telegram send timeout to {recipient_type} {recipient_id} after {self.max_retries} attempts: {e}")
                        all_success = False
                        break
                        
                except Exception as e:
                    # Unexpected errors - log and don't retry
                    self.logger.error(f"Unexpected error sending Telegram message to {recipient_type} {recipient_id}: {e}")
                    all_success = False
                    break
        
        return all_success
    
    async def send_alert(self, opportunity: Dict, timeout: int = 5) -> bool:
        """
        Send arbitrage alert via Telegram.
        
        This is a convenience method that formats the opportunity and calls send_message().
        
        Args:
            opportunity: Arbitrage opportunity dictionary
            timeout: Maximum time to wait for send (seconds)
        
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            message = self._format_alert_message(opportunity)
            success = await self.send_message(message, timeout)
            
            if success:
                self.logger.info(f"Telegram alert sent for: {opportunity.get('market_name')}")
            else:
                self.logger.warning(f"Failed to send Telegram alert for: {opportunity.get('market_name')}")
            
            return success
            
        except Exception as e:
            # Never crash the main app
            self.logger.error(f"Error formatting/sending alert: {e}")
            return False
    
    async def send_test_message(self) -> bool:
        """
        Send a test message to verify Telegram configuration.
        
        Returns:
            True if sent successfully, False otherwise
        """
        test_text = "Telegram bot integration test - Bot is working!"
        return await self.send_message(test_text)


# Test function for standalone testing
async def test_telegram(bot_token: str, chat_id: int):
    """
    Test function to verify Telegram integration.
    
    Usage:
        import asyncio
        from telegram_notifier import test_telegram
        
        asyncio.run(test_telegram("your_token", 123456789))
    
    Args:
        bot_token: Telegram bot token
        chat_id: Telegram chat ID (integer)
    """
    notifier = TelegramNotifier(bot_token, chat_id)
    success = await notifier.send_test_message()
    
    if success:
        print("SUCCESS: Telegram test message sent successfully!")
    else:
        print("FAILED: Failed to send Telegram test message. Check logs for details.")
    
    return success

