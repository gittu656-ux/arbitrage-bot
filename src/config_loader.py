"""
Configuration loader with environment variable support.
"""
import os
from pathlib import Path
from typing import Any, Dict
import yaml
from pydantic import BaseModel, Field
from dotenv import load_dotenv


# Load environment variables
load_dotenv()


class BankrollConfig(BaseModel):
    """Bankroll configuration."""
    amount: float = Field(gt=0, description="Total capital in USD")
    kelly_fraction: float = Field(ge=0, le=1, description="Kelly multiplier (0-1)")


class ArbitrageConfig(BaseModel):
    """Arbitrage detection configuration."""
    min_profit_threshold: float = Field(gt=0, description="Minimum profit percentage")
    polling_interval: int = Field(gt=0, description="Polling interval in seconds")
    similarity_threshold: float = Field(ge=0, le=100, description="Market matching similarity threshold")


class TelegramConfig(BaseModel):
    """Telegram notification configuration."""
    bot_token: str = Field(default="", description="Telegram bot token")
    chat_id: int = Field(default=0, description="Telegram chat ID (integer)")
    channel_id: int = Field(default=0, description="Telegram channel ID (integer, optional)")


class CloudbetAPIConfig(BaseModel):
    """Cloudbet API configuration."""
    api_key: str = Field(default="", description="Cloudbet API key")
    base_url: str = "https://sports-api.cloudbet.com/pub"
    timeout: int = 10
    retry_attempts: int = 3
    retry_delay: int = 2


class PolymarketAPIConfig(BaseModel):
    """Polymarket API configuration."""
    private_key: str = Field(default="", description="Polymarket private key")
    base_url: str = "https://clob.polymarket.com"
    timeout: int = 10
    retry_attempts: int = 3
    retry_delay: int = 2


class QuietHoursConfig(BaseModel):
    """Quiet hours configuration."""
    enabled: bool = False
    start_hour: int = Field(ge=0, le=23)
    end_hour: int = Field(ge=0, le=23)


class AutobetConfig(BaseModel):
    """Autobet and risk management configuration."""
    enabled: bool = False
    # Minimum profit % required to even consider autobetting
    min_profit_threshold: float = Field(default=1.0, ge=0)
    # Maximum number of arbitrage bets per day
    max_bets_per_day: int = Field(default=20, ge=0)
    # Hard cap on total capital per bet as % of bankroll (e.g. 0.25 = 25%)
    max_stake_fraction: float = Field(default=0.25, ge=0, le=1)
    # Optional daily loss limit in USD (0 = disabled). Since arbitrage is
    # theoretically risk-free, this mainly protects against execution issues.
    daily_loss_limit: float = Field(default=0.0, ge=0)
    # Flag to enable real money betting (dangerous!)
    real_execution: bool = Field(default=False)


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = "INFO"
    file: str = "logs/arbitrage_bot.log"
    max_bytes: int = 10485760
    backup_count: int = 5


class DatabaseConfig(BaseModel):
    """Database configuration."""
    path: str = "data/arbitrage_events.db"


class APIConfig(BaseModel):
    """API configuration container."""
    cloudbet: CloudbetAPIConfig
    polymarket: PolymarketAPIConfig


class Config(BaseModel):
    """Main configuration model."""
    bankroll: BankrollConfig
    arbitrage: ArbitrageConfig
    telegram: TelegramConfig
    apis: APIConfig
    quiet_hours: QuietHoursConfig
    logging: LoggingConfig
    database: DatabaseConfig
    autobet: AutobetConfig = AutobetConfig()
    debug_api: bool = False
    use_mock_data: bool = False


def load_config(config_path: str = "config/config.yaml") -> Config:
    """
    Load configuration from YAML file with environment variable overrides.
    
    Args:
        config_path: Path to configuration YAML file
    
    Returns:
        Validated Config object
    
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If configuration is invalid
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r') as f:
        config_dict = yaml.safe_load(f)
    
    # Load environment variables for empty strings (override config with env vars)
    if 'telegram' in config_dict:
        # Always prefer environment variables if they exist
        env_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        env_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        env_channel_id = os.getenv('TELEGRAM_CHANNEL_ID')
        
        if env_bot_token:
            config_dict['telegram']['bot_token'] = env_bot_token
        if env_chat_id:
            # Convert chat_id to int if from env var
            try:
                config_dict['telegram']['chat_id'] = int(env_chat_id)
            except (ValueError, TypeError):
                # Invalid chat_id from env, will use config value
                pass
        if env_channel_id:
            # Convert channel_id to int if from env var
            try:
                config_dict['telegram']['channel_id'] = int(env_channel_id)
            except (ValueError, TypeError):
                # Invalid channel_id from env, will use config value
                pass
        
        # Ensure chat_id is an integer
        if isinstance(config_dict['telegram'].get('chat_id'), str):
            try:
                config_dict['telegram']['chat_id'] = int(config_dict['telegram']['chat_id'])
            except (ValueError, TypeError):
                config_dict['telegram']['chat_id'] = 0
        
        # Ensure channel_id is an integer
        if isinstance(config_dict['telegram'].get('channel_id'), str):
            try:
                config_dict['telegram']['channel_id'] = int(config_dict['telegram']['channel_id'])
            except (ValueError, TypeError):
                config_dict['telegram']['channel_id'] = 0
    
    if 'apis' in config_dict and 'cloudbet' in config_dict['apis']:
        env_api_key = os.getenv('CLOUDBET_API_KEY')
        if env_api_key:
            config_dict['apis']['cloudbet']['api_key'] = env_api_key
            
    if 'apis' in config_dict and 'polymarket' in config_dict['apis']:
        env_pm_key = os.getenv('POLYMARKET_PRIVATE_KEY')
        if env_pm_key:
            config_dict['apis']['polymarket']['private_key'] = env_pm_key
    
    # Handle nested API config
    if 'apis' in config_dict:
        if 'cloudbet' in config_dict['apis']:
            config_dict['apis']['cloudbet'] = CloudbetAPIConfig(**config_dict['apis']['cloudbet'])
        if 'polymarket' in config_dict['apis']:
            config_dict['apis']['polymarket'] = PolymarketAPIConfig(**config_dict['apis']['polymarket'])
        config_dict['apis'] = APIConfig(**config_dict['apis'])
    
    # Convert all nested dicts to models
    config_dict['bankroll'] = BankrollConfig(**config_dict['bankroll'])
    config_dict['arbitrage'] = ArbitrageConfig(**config_dict['arbitrage'])
    config_dict['telegram'] = TelegramConfig(**config_dict['telegram'])
    config_dict['quiet_hours'] = QuietHoursConfig(**config_dict['quiet_hours'])
    config_dict['logging'] = LoggingConfig(**config_dict['logging'])
    config_dict['database'] = DatabaseConfig(**config_dict['database'])
    # Autobet is optional – if missing, use defaults
    autobet_cfg = config_dict.get('autobet', {})
    if isinstance(autobet_cfg, dict):
        config_dict['autobet'] = AutobetConfig(**autobet_cfg)
    else:
        config_dict['autobet'] = AutobetConfig()
    
    return Config(**config_dict)

